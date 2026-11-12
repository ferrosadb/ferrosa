"""Contracts for the PR601 probe's fresh-process fan-in sweep.

CLI tests use an executable fake probe and measure orchestration only. The
fixture follows compaction_allocation_probe.rs's println! schema; its metric
values are illustrative, not recorded Rust benchmark measurements.
"""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import tempfile
import unittest


# Test list:
# - Eight unique cases control total rows or rows per input across four fan-ins.
# - Accept one probe JSON object, including blank surrounding lines.
# - Preserve the original metrics without inventing physical byte counts.
# - Reject missing fields, wrong shapes/settings, and invalid measurements.
# - Reject empty, malformed, noisy, or multiple-sample stdout.
# - Execute eight fresh processes per repetition with seeded ordering.
# - Pin provenance, retain raw evidence, and summarize validated samples.
# - Stop at the first failed sample, retaining a partial manifest.

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "ferrosa_compaction_fanin", ROOT / "deploy/fly-compaction-explore/fanin.py"
)
fanin = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fanin)

FANINS = (4, 16, 32, 64)
VALIDATION_ERRORS = (TypeError, ValueError)
CASE = {
    "name": "fixed_total_fanin4",
    "rows": 65536,
    "partitions": 1,
    "fanin": 4,
    "value_bytes": 64,
}
# Exact field names emitted by the existing Rust allocation probe.
PROBE_LINE = (
    '{"correct":true,"rows":65536,"partitions":1,"fanin":4,'
    '"value_bytes":64,"complex":false,"purge":false,"compressed":false,'
    '"repetition":0,"alloc_events":1234,"allocated_bytes":567890,'
    '"peak_live_delta_bytes":123456,"elapsed_ns":987654321}'
)


def probe_sample(**overrides):
    sample = json.loads(PROBE_LINE)
    sample.update(overrides)
    return sample


class FaninMatrixTests(unittest.TestCase):
    def test_eight_unique_named_cases_cover_both_families_and_all_fanins(self):
        cases = fanin.cases()
        self.assertEqual(len(cases), 8)
        names = set()
        pairs = set()
        for case in cases:
            with self.subTest(case=case):
                self.assertIsInstance(case, dict)
                self.assertEqual(set(case), set(CASE))
                self.assertIsInstance(case["name"], str)
                families = [family for family in
                            ("fixed_total", "fixed_per_input")
                            if family in case["name"]]
                self.assertEqual(len(families), 1)
                family = families[0]
                self.assertIn(case["fanin"], FANINS)
                self.assertRegex(case["name"],
                                 rf'(?<!\d){case["fanin"]}(?!\d)')
                for field in ("rows", "partitions", "fanin", "value_bytes"):
                    self.assertIs(type(case[field]), int)
                self.assertEqual(case["partitions"], 1)
                self.assertEqual(case["value_bytes"], 64)
                expected_rows = (262144 // case["fanin"]
                                 if family == "fixed_total" else 4096)
                self.assertEqual(case["rows"], expected_rows)
                names.add(case["name"])
                pairs.add((family, case["fanin"]))
        self.assertEqual(len(names), 8)
        self.assertEqual(pairs, {
            (family, count)
            for family in ("fixed_total", "fixed_per_input")
            for count in FANINS
        })

    def test_total_input_rows_are_fixed_only_in_fixed_total_family(self):
        cases = fanin.cases()
        totals = {
            family: {
                case["fanin"]: case["rows"] * case["partitions"] * case["fanin"]
                for case in cases if family in case["name"]
            }
            for family in ("fixed_total", "fixed_per_input")
        }
        self.assertEqual(totals["fixed_total"],
                         {4: 262144, 16: 262144, 32: 262144, 64: 262144})
        self.assertEqual(totals["fixed_per_input"],
                         {4: 16384, 16: 65536, 32: 131072, 64: 262144})


class ParseSampleTests(unittest.TestCase):
    def assert_rejected(self, sample):
        with self.assertRaises(VALIDATION_ERRORS):
            fanin.parse_sample(json.dumps(sample), dict(CASE))

    def test_accepts_original_probe_schema_without_inventing_byte_metrics(self):
        expected = json.loads(PROBE_LINE)
        for stdout in (PROBE_LINE, PROBE_LINE + "\n",
                       "\n \t\n  " + PROBE_LINE + "  \n\n"):
            with self.subTest(stdout=stdout):
                result = fanin.parse_sample(stdout, dict(CASE))
                self.assertIsInstance(result, dict)
                self.assertEqual(result, expected)
                self.assertNotIn("input_bytes", result)
                self.assertNotIn("output_bytes", result)

    def test_accepts_matching_samples_for_every_generated_case(self):
        for case in fanin.cases():
            with self.subTest(case=case):
                sample = probe_sample(**{
                    field: case[field]
                    for field in ("rows", "partitions", "fanin", "value_bytes")
                })
                self.assertEqual(fanin.parse_sample(json.dumps(sample), case),
                                 sample)

    def test_accepts_zero_allocation_metrics_and_positive_fractional_time(self):
        sample = probe_sample(elapsed_ns=0.125, alloc_events=0,
                              allocated_bytes=0, peak_live_delta_bytes=0)
        self.assertEqual(fanin.parse_sample(json.dumps(sample), dict(CASE)),
                         sample)

    def test_accepts_finite_fractional_numeric_metrics(self):
        sample = probe_sample(elapsed_ns=1.5, alloc_events=2.5,
                              allocated_bytes=3.5, peak_live_delta_bytes=4.5)
        self.assertEqual(fanin.parse_sample(json.dumps(sample), dict(CASE)),
                         sample)

    def test_rejects_each_missing_probe_field(self):
        for field in probe_sample():
            with self.subTest(field=field):
                sample = probe_sample()
                del sample[field]
                self.assert_rejected(sample)

    def test_rejects_case_mismatches_and_invalid_configuration_numbers(self):
        for field in ("rows", "partitions", "fanin", "value_bytes"):
            for value in (CASE[field] + 1, 0, -1, True, False, None,
                          str(CASE[field]), [], {}, float("nan"),
                          float("inf"), float("-inf")):
                with self.subTest(field=field, value=value):
                    self.assert_rejected(probe_sample(**{field: value}))

    def test_correct_must_be_exactly_true(self):
        for value in (False, 0, 1, 1.0, None, "true", "false", [], {}):
            with self.subTest(correct=value):
                self.assert_rejected(probe_sample(correct=value))

    def test_shape_flags_must_be_exactly_false(self):
        for field in ("complex", "purge", "compressed"):
            for value in (True, 0, 1, 0.0, None, "false", "true", [], {}):
                with self.subTest(field=field, value=value):
                    self.assert_rejected(probe_sample(**{field: value}))

    def test_rejects_nonzero_or_invalid_repetition(self):
        for value in (1, -1, 0.5, True, False, None, "0", [], {},
                      float("nan"), float("inf"), float("-inf")):
            with self.subTest(repetition=value):
                self.assert_rejected(probe_sample(repetition=value))

    def test_rejects_nonpositive_nonfinite_or_nonnumeric_elapsed_time(self):
        for value in (0, -0.0, -1, -0.125, float("nan"), float("inf"),
                      float("-inf"), True, False, None, "1", [], {}):
            with self.subTest(elapsed_ns=value):
                self.assert_rejected(probe_sample(elapsed_ns=value))

    def test_rejects_negative_nonfinite_or_nonnumeric_allocation_metrics(self):
        for field in ("alloc_events", "allocated_bytes", "peak_live_delta_bytes"):
            for value in (-1, -0.125, float("nan"), float("inf"),
                          float("-inf"), True, False, None, "0", [], {}):
                with self.subTest(field=field, value=value):
                    self.assert_rejected(probe_sample(**{field: value}))

    def test_rejects_nonobject_json(self):
        for value in (None, [], [probe_sample()], "sample", 1, 1.5, True, False):
            with self.subTest(value=value):
                self.assert_rejected(value)

    def test_rejects_empty_malformed_noisy_and_multiple_line_stdout(self):
        for stdout in (
            "", " \t\n\n", "not JSON", "{", '{"correct":true,}',
            PROBE_LINE[:-1], PROBE_LINE + " trailing text",
            PROBE_LINE + PROBE_LINE,
            PROBE_LINE + "\n" + PROBE_LINE,
            PROBE_LINE + "\n\n" + PROBE_LINE + "\n",
            "starting probe\n" + PROBE_LINE,
            PROBE_LINE + "\nfinished probe\n",
            PROBE_LINE + "\nnull\n",
            json.dumps(probe_sample(), indent=2),
        ):
            with self.subTest(stdout=stdout):
                with self.assertRaises(VALIDATION_ERRORS):
                    fanin.parse_sample(stdout, dict(CASE))


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.binary = self.base / "fakeprobe"
        self.output = self.base / "results"
        self.calls = self.base / "calls.jsonl"
        self.binary.write_text(
            f"#!{sys.executable}\n" + '''import json, os, sys, time
from pathlib import Path
calls = Path(os.environ["FAKE_CALLS"])
index = len(calls.read_text().splitlines()) if calls.exists() else 0
with calls.open("a") as log:
    log.write(json.dumps({"args": sys.argv[1:], "pid": os.getpid(),
                         "verify": os.environ.get("FERROSA_COMPACTION_VERIFY_OUTPUT")}) + "\\n")
rows, partitions, fanin, value_bytes, repetitions = map(int, sys.argv[1:])
assert repetitions == 1
sample = dict(correct=True, rows=rows, partitions=partitions, fanin=fanin,
              value_bytes=value_bytes, complex=False, purge=False,
              compressed=False, repetition=0, elapsed_ns=index + 1,
              alloc_events=index + 2, allocated_bytes=index + 3,
              peak_live_delta_bytes=index + 4)
mode = os.environ.get("FAKE_MODE", "ok") if index == 1 else "ok"
print("fake diagnostic", file=sys.stderr, flush=True)
if mode == "timeout":
    print("partial output", flush=True)
    time.sleep(10)
if mode == "incorrect":
    sample["correct"] = False
if mode == "wrongshape":
    sample["fanin"] += 1
if mode == "malformed":
    print("not json")
elif mode != "empty":
    print(json.dumps(sample))
sys.exit(7 if mode == "nonzero" else 0)
''', encoding="utf-8")
        self.binary.chmod(0o755)

    def run_cli(self, *extra, mode="ok"):
        env = dict(os.environ, FAKE_CALLS=str(self.calls), FAKE_MODE=mode,
                   FERROSA_COMPACTION_VERIFY_OUTPUT="false")
        return subprocess.run(
            [sys.executable, str(Path(SPEC.origin)), "--binary", str(self.binary),
             "--output", str(self.output), *extra],
            env=env, capture_output=True, text=True, timeout=30)

    def read_json(self, name):
        return json.loads((self.output / name).read_text())

    def test_fresh_processes_seeded_order_provenance_and_summaries(self):
        result = self.run_cli("--repetitions", "2", "--seed", "19")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        records = [json.loads(line) for line in
                   (self.output / "samples.jsonl").read_text().splitlines()]
        manifest = self.read_json("manifest.json")
        self.assertEqual(len(calls), 16)
        self.assertEqual(len({call["pid"] for call in calls}), 16)
        self.assertEqual(len(records), 16)
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["binary_path"], str(self.binary.resolve()))
        self.assertEqual(manifest["binary_sha256"],
                         hashlib.sha256(self.binary.read_bytes()).hexdigest())
        self.assertEqual(manifest["script_sha256"],
                         hashlib.sha256(Path(SPEC.origin).read_bytes()).hexdigest())
        self.assertEqual(manifest["environment"]["FERROSA_COMPACTION_VERIFY_OUTPUT"], "true")
        self.assertEqual(manifest["seed"], 19)
        self.assertEqual(manifest["timeout"], 120)
        expected = []
        rng = random.Random(19)
        for repetition in range(2):
            order = fanin.cases()
            rng.shuffle(order)
            expected.extend((repetition, case) for case in order)
        self.assertEqual(len(manifest["invocations"]), 16)
        for index, ((repetition, case), call, record, invocation) in enumerate(
                zip(expected, calls, records, manifest["invocations"])):
            self.assertEqual(call["args"], [str(case[key]) for key in
                             ("rows", "partitions", "fanin", "value_bytes")] + ["1"])
            self.assertEqual(call["verify"], "true")
            self.assertEqual(record["case"], case)
            self.assertEqual(record["repetition"], repetition)
            self.assertEqual(invocation["order"], index)
            self.assertEqual(invocation["command"], [str(self.binary)] + call["args"])
            self.assertEqual(invocation["status"], "complete")
            self.assertEqual(self.read_json(invocation["invocation_file"]), invocation)
            self.assertEqual(self.read_json(invocation["stdout"]), record["probe"])
            self.assertEqual(record["probe"]["repetition"], 0)
            self.assertEqual((self.output / invocation["stderr"]).read_text(),
                             "fake diagnostic\n")
        summaries = self.read_json("summary.json")
        self.assertEqual(set(summaries), {case["name"] for case in fanin.cases()})
        for name, summary in summaries.items():
            probes = [r["probe"] for r in records if r["case"]["name"] == name]
            self.assertEqual(summary["samples"], 2)
            for metric, reducers in {
                "elapsed_ns": {"median": statistics.median, "min": min, "max": max},
                "peak_live_delta_bytes": {"median": statistics.median, "max": max},
                "alloc_events": {"median": statistics.median},
                "allocated_bytes": {"median": statistics.median},
            }.items():
                self.assertEqual(summary[metric], {
                    label: reduce([probe[metric] for probe in probes])
                    for label, reduce in reducers.items()})
            self.assertEqual(set(summary), {"samples", "elapsed_ns", "peak_live_delta_bytes",
                                            "alloc_events", "allocated_bytes"})

    def test_defaults_run_five_repetitions(self):
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.calls.read_text().splitlines()), 40)
        manifest = self.read_json("manifest.json")
        self.assertEqual(manifest["repetitions"], 5)
        self.assertEqual(manifest["seed"], 601)

    def test_failures_stop_immediately_and_preserve_partial_evidence(self):
        for mode in ("incorrect", "wrongshape", "nonzero", "timeout", "empty", "malformed"):
            with self.subTest(mode=mode):
                self.output = self.base / mode
                self.calls = self.base / (mode + ".jsonl")
                result = self.run_cli("--repetitions", "2", "--timeout", "0.5", mode=mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(self.calls.read_text().splitlines()), 2)
                self.assertEqual(len((self.output / "samples.jsonl").read_text().splitlines()), 1)
                manifest = self.read_json("manifest.json")
                self.assertEqual(manifest["status"], "failed")
                self.assertTrue(manifest["error"])
                attempted = [i for i in manifest["invocations"] if i["status"] != "pending"]
                self.assertEqual(len(attempted), 2)
                failed = attempted[-1]
                self.assertEqual(failed["status"], "failed")
                self.assertEqual(self.read_json(failed["invocation_file"]), failed)
                self.assertTrue((self.output / failed["stdout"]).exists())
                self.assertEqual((self.output / failed["stderr"]).read_text(), "fake diagnostic\n")
                if mode == "timeout":
                    self.assertEqual((self.output / failed["stdout"]).read_text(), "partial output\n")
                if mode == "nonzero":
                    self.assertEqual(failed["returncode"], 7)

    def test_help_and_invalid_arguments_do_not_execute_probe(self):
        result = self.run_cli("--help")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--timeout", result.stdout)
        for extra in (("--repetitions", "0"), ("--repetitions", "-1"),
                      ("--repetitions", "1.5"), ("--timeout", "0"),
                      ("--timeout", "-1"), ("--timeout", "nan"),
                      ("--timeout", "inf"), ("--seed", "1.5"),
                      ("--binary", str(self.base / "missing"))):
            with self.subTest(extra=extra):
                self.assertNotEqual(self.run_cli(*extra).returncode, 0)
        self.output.mkdir()
        sentinel = self.output / "keep"
        sentinel.write_text("untouched")
        self.assertNotEqual(self.run_cli().returncode, 0)
        self.assertEqual(sentinel.read_text(), "untouched")
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
