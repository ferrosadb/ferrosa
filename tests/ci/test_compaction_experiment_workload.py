"""RED contracts for the bounded, stdlib-only compaction workload planner.

Keys are nonnegative integers; payload byte counts are positive integers.
Validation may raise TypeError or ValueError; no coercion is permitted.
Randomness checks use invariants and broad smoke bounds, never an RNG sequence.
"""

import builtins
from collections import Counter
import importlib.util
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import unittest
from unittest import mock


# Test list:
# - Exact bounded plans, round-to-even read counts, and endpoint fractions.
# - Repeatable seeded shuffling, seed variation, and uniform random keys.
# - Reject invalid counts, fractions, seeds, payload keys, and byte counts.
# - Exact bytes, key-derived prefixes, and repeat-write/read consistency.
# - Cross-process/hash-seed stability and import without cassandra-driver.

ROOT = Path(__file__).resolve().parents[2]
WORKLOAD_PATH = ROOT / "deploy/fly-compaction-explore/workload.py"


def load_workload():
    spec = importlib.util.spec_from_file_location(
        "ferrosa_compaction_experiment_workload", WORKLOAD_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


workload = load_workload()
INVALID_POSITIVE_INTS = (True, False, 0, -1, 1.0, 1.5, "1", None)
VALIDATION_ERRORS = (TypeError, ValueError)


class PhasePlanTests(unittest.TestCase):
    def test_exact_length_shape_key_bounds_and_rounded_read_count(self):
        for operations in (1, 3, 5, 7, 31, 100):
            for fraction in (0, 0.1, 0.25, 0.5, 0.9, 1):
                with self.subTest(operations=operations, fraction=fraction):
                    plan = workload.plan_phase(operations, fraction, 7, 42)
                    self.assertIsInstance(plan, list)
                    self.assertEqual(len(plan), operations)
                    for row in plan:
                        self.assertIsInstance(row, dict)
                        self.assertEqual(set(row), {"op", "key"})
                        self.assertIn(row["op"], ("read", "write"))
                        self.assertIs(type(row["key"]), int)
                        self.assertGreaterEqual(row["key"], 0)
                        self.assertLess(row["key"], 7)
                    reads = sum(row["op"] == "read" for row in plan)
                    self.assertEqual(reads, round(operations * fraction))
                    self.assertEqual(
                        sum(row["op"] == "write" for row in plan),
                        operations - reads,
                    )

    def test_single_key_allows_repeated_operations_at_key_zero(self):
        plan = workload.plan_phase(100, 0.5, 1, 0)
        self.assertEqual(len(plan), 100)
        self.assertEqual({row["key"] for row in plan}, {0})
        self.assertEqual(Counter(row["op"] for row in plan),
                         {"read": 50, "write": 50})

    def test_same_seed_repeats_despite_other_calls_and_global_random_activity(self):
        state = random.getstate()
        self.addCleanup(random.setstate, state)
        for seed in (0, -1, 42):
            with self.subTest(seed=seed):
                expected = workload.plan_phase(256, 0.375, 19, seed)
                workload.plan_phase(37, 0.8, 3, seed + 1)
                random.seed(918273)
                for _ in range(100):
                    random.random()
                self.assertEqual(
                    workload.plan_phase(256, 0.375, 19, seed), expected
                )

    def test_seed_changes_both_operation_order_and_key_choices(self):
        plans = [workload.plan_phase(256, 0.5, 31, seed)
                 for seed in (0, 1, 2, 3)]
        self.assertGreater(len({tuple(row["op"] for row in plan)
                                for plan in plans}), 1)
        self.assertGreater(len({tuple(row["key"] for row in plan)
                                for plan in plans}), 1)

    def test_mixed_plan_is_interleaved_instead_of_two_sequential_batches(self):
        for seed in (0, 7, 42):
            with self.subTest(seed=seed):
                ops = [row["op"] for row in
                       workload.plan_phase(256, 0.5, 1, seed)]
                transitions = sum(a != b for a, b in zip(ops, ops[1:]))
                self.assertGreater(transitions, 1)
                # A fixed alternating schedule is not a shuffled interleaving.
                self.assertLess(transitions, len(ops) - 1)

    def test_key_distribution_smoke_for_both_reads_and_writes(self):
        key_count = 16
        plan = workload.plan_phase(16000, 0.375, key_count, 12345)
        for op in ("read", "write"):
            with self.subTest(op=op):
                keys = [row["key"] for row in plan if row["op"] == op]
                counts = Counter(keys)
                self.assertEqual(set(counts), set(range(key_count)))
                expected = len(keys) / key_count
                # Broad tolerance: catch hot-key/skew bugs without pinning RNG.
                for key, count in counts.items():
                    with self.subTest(key=key):
                        self.assertLess(abs(count - expected), expected * 0.3)

    def test_keys_are_sampled_with_replacement_instead_of_cycled(self):
        keys = [row["key"] for row in
                workload.plan_phase(1024, 0, 8, 2026)]
        self.assertTrue(any(a == b for a, b in zip(keys, keys[1:])))
        self.assertGreater(len({(b - a) % 8
                                for a, b in zip(keys, keys[1:])}), 1)

    def test_rejects_invalid_operation_and_key_counts(self):
        for field in ("operations", "keys"):
            for value in INVALID_POSITIVE_INTS:
                with self.subTest(field=field, value=value):
                    args = dict(operations=10, read_fraction=0.5,
                                keys=5, seed=0)
                    args[field] = value
                    with self.assertRaises(VALIDATION_ERRORS):
                        workload.plan_phase(**args)

    def test_rejects_out_of_range_nonfinite_and_nonnumeric_fractions(self):
        for value in (-0.01, 1.01, float("nan"), float("inf"),
                      float("-inf"), "0.5", None, complex(0.5, 0)):
            with self.subTest(read_fraction=value):
                with self.assertRaises(VALIDATION_ERRORS):
                    workload.plan_phase(10, value, 5, 0)

    def test_rejects_noninteger_seeds_including_bools(self):
        for seed in (True, False, 0.0, 1.5, "42", b"42", None):
            with self.subTest(seed=seed):
                with self.assertRaises(VALIDATION_ERRORS):
                    workload.plan_phase(10, 0.5, 5, seed)


class PayloadTests(unittest.TestCase):
    def test_returns_bytes_of_exact_requested_length(self):
        for key in (0, 1, 257):
            for size in (1, 7, 16, 31, 32, 33, 64, 257, 4096):
                with self.subTest(key=key, value_bytes=size):
                    value = workload.value_for(key, size)
                    self.assertIsInstance(value, bytes)
                    self.assertEqual(len(value), size)

    def test_key_derived_prefix_uniqueness_smoke(self):
        # Prefixes must carry key identity, not a shared padding/header prefix.
        # This is a smoke check, not an impossible universal collision promise.
        values = [workload.value_for(key, 128) for key in range(64)]
        self.assertEqual(len({value[:16] for value in values}), 64)

    def test_repeated_writes_remain_valid_for_every_read_of_the_same_key(self):
        size = 97
        expected = {key: workload.value_for(key, size) for key in range(4)}
        stored = dict(expected)  # The runner preloads every readable key.
        plan = workload.plan_phase(512, 0.5, 4, 55)
        writes = Counter()
        reads = 0
        for row in plan:
            key = row["key"]
            if row["op"] == "write":
                stored[key] = workload.value_for(key, size)
                writes[key] += 1
                self.assertEqual(stored[key], expected[key])
            else:
                reads += 1
                self.assertEqual(stored[key], workload.value_for(key, size))
                self.assertEqual(stored[key], expected[key])
        self.assertEqual(reads, 256)
        self.assertTrue(any(count > 1 for count in writes.values()))

    def test_payload_is_stable_across_processes_and_python_hash_seeds(self):
        script = """
import importlib.util
import json
import sys
spec = importlib.util.spec_from_file_location('workload_probe', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
print(json.dumps([module.value_for(key, 97).hex() for key in (0, 1, 257)]))
"""
        expected = [workload.value_for(key, 97).hex() for key in (0, 1, 257)]
        for hash_seed in ("1", "987654"):
            with self.subTest(hash_seed=hash_seed):
                result = subprocess.run(
                    [sys.executable, "-c", script, str(WORKLOAD_PATH)],
                    env={**os.environ, "PYTHONHASHSEED": hash_seed,
                         "PYTHONDONTWRITEBYTECODE": "1"},
                    capture_output=True, text=True, timeout=10, check=True,
                )
                self.assertEqual(json.loads(result.stdout), expected)

    def test_rejects_invalid_keys(self):
        for key in (True, False, -1, 0.0, 1.5, "0", None):
            with self.subTest(key=key):
                with self.assertRaises(VALIDATION_ERRORS):
                    workload.value_for(key, 32)

    def test_rejects_invalid_byte_counts(self):
        for size in INVALID_POSITIVE_INTS:
            with self.subTest(value_bytes=size):
                with self.assertRaises(VALIDATION_ERRORS):
                    workload.value_for(0, size)


class DependencyTests(unittest.TestCase):
    def test_module_import_does_not_attempt_to_load_cassandra_driver(self):
        original_import = builtins.__import__
        attempted = []

        def without_cassandra(name, *args, **kwargs):
            if name == "cassandra" or name.startswith("cassandra."):
                attempted.append(name)
                raise ModuleNotFoundError("cassandra-driver deliberately absent")
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=without_cassandra):
            module = load_workload()
        self.assertEqual(attempted, [])
        self.assertTrue(callable(module.plan_phase))
        self.assertTrue(callable(module.value_for))


if __name__ == "__main__":
    unittest.main()
