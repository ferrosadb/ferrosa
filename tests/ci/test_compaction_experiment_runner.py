"""RED contracts for run_phase; no CQL, metrics, or deployment dependencies."""

from collections import Counter
import importlib.util
import io
import json
import math
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


# Test list: exact row identity/counts, mismatch/error evidence, bounded workers,
# fixed-rate pacing, queue-inclusive timing, preflight validation, no overwrite.
ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "deploy/fly-compaction-explore/run.py"
RATE = 10000
VALIDATION_ERRORS = (TypeError, ValueError)


def phase_plan(count):
    # Same list-of-dicts contract as workload.plan_phase. Unique keys let tests
    # associate evidence with inputs without imposing completion/output order.
    return [{"op": "read" if key % 2 else "write", "key": key}
            for key in range(count)]


class PhaseRunnerTests(unittest.TestCase):
    def setUp(self):
        name = "ferrosa_compaction_experiment_runner"
        spec = importlib.util.spec_from_file_location(name, RUNNER_PATH)
        runner = importlib.util.module_from_spec(spec)
        previous = sys.modules.get(name)
        sys.modules[name] = runner

        def restore_module():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous

        self.addCleanup(restore_module)
        spec.loader.exec_module(runner)
        self.runner = runner
        self.run_phase = runner.run_phase
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.output = self.directory / "operations.jsonl"

    def assert_evidence(self, plan, result, errors=0, mismatches=0):
        lines = self.output.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), len(plan))
        rows = [json.loads(line) for line in lines]
        self.assertEqual(sorted(row["sequence"] for row in rows), list(range(len(plan))))
        self.assertEqual(Counter((r["op"], r["key"]) for r in rows),
                         Counter((r["op"], r["key"]) for r in plan))
        for row in rows:
            self.assertGreaterEqual(row["scheduled_unix"], result["start_unix"])
            self.assertAlmostEqual(
                (row["completed_unix"] - row["scheduled_unix"]) * 1000,
                row["latency_ms"], delta=0.001,
            )
            self.assertIs(type(row["ok"]), bool)
            self.assertIs(type(row["mismatch"]), bool)
            for field in ("latency_ms", "service_ms", "dispatch_lag_ms"):
                value = row[field]
                self.assertIn(type(value), (int, float))
                self.assertTrue(math.isfinite(value), (field, value))
                self.assertGreaterEqual(value, 0)
            self.assertAlmostEqual(
                row["latency_ms"],
                row["service_ms"] + row["dispatch_lag_ms"], delta=1.0,
            )
        self.assertEqual(result["expected_operations"], len(plan))
        self.assertEqual(result["errors"], errors)
        self.assertEqual(result["mismatches"], mismatches)
        self.assertIn(type(result["elapsed_seconds"]), (int, float))
        self.assertTrue(math.isfinite(result["elapsed_seconds"]))
        self.assertGreater(result["elapsed_seconds"], 0)
        return rows

    def test_executes_every_row_once_and_persists_success(self):
        # Repeated identical rows must also execute and persist separately.
        plan = phase_plan(20) + [dict(phase_plan(1)[0]) for _ in range(3)]
        calls = []
        lock = threading.Lock()

        def execute(op, key):
            with lock:
                calls.append((op, key))
            return True

        result = self.run_phase(plan, execute, RATE, 4, self.output)
        self.assertEqual(Counter(calls),
                         Counter((r["op"], r["key"]) for r in plan))
        for row in self.assert_evidence(plan, result):
            self.assertIs(row["ok"], True)
            self.assertIs(row["mismatch"], False)

    def test_mismatch_and_exception_are_captured_without_losing_operations(self):
        plan = phase_plan(12)
        calls = []
        lock = threading.Lock()

        def execute(op, key):
            with lock:
                calls.append((op, key))
            if key == 1:
                return False
            if key == 2:
                raise RuntimeError("sentinel operation failure")
            return True

        result = self.run_phase(plan, execute, RATE, 3, self.output)
        self.assertEqual(Counter(calls),
                         Counter((r["op"], r["key"]) for r in plan))
        rows = self.assert_evidence(plan, result, errors=1, mismatches=1)
        by_key = {r["key"]: r for r in rows}
        self.assertIs(by_key[1]["ok"], False)
        self.assertIs(by_key[1]["mismatch"], True)
        self.assertIs(by_key[2]["ok"], False)
        self.assertIs(by_key[2]["mismatch"], False)
        self.assertIn("sentinel operation failure", by_key[2]["error"])
        for key in set(by_key) - {1, 2}:
            self.assertIs(by_key[key]["ok"], True)
            self.assertIs(by_key[key]["mismatch"], False)

    def test_active_callbacks_never_exceed_concurrency(self):
        plan = phase_plan(24)
        for concurrency in (1, 3):
            with self.subTest(concurrency=concurrency):
                self.output = self.directory / f"workers-{concurrency}.jsonl"
                active = peak = 0
                lock = threading.Lock()
                pause = threading.Event()

                def execute(op, key):
                    nonlocal active, peak
                    with lock:
                        active += 1
                        peak = max(peak, active)
                    try:
                        pause.wait(0.01)
                        return True
                    finally:
                        with lock:
                            active -= 1

                result = self.run_phase(plan, execute, RATE, concurrency,
                                        self.output)
                self.assertEqual(active, 0)
                self.assertGreaterEqual(peak, 1)
                self.assertLessEqual(peak, concurrency)
                self.assert_evidence(plan, result)

    def test_rate_paces_operations_instead_of_dispatching_an_immediate_burst(self):
        plan = phase_plan(2000)
        dispatches = []
        lock = threading.Lock()

        def execute(op, key):
            with lock:
                dispatches.append(time.monotonic())
            return True

        started = time.monotonic()
        result = self.run_phase(plan, execute, RATE, 4, self.output)
        elapsed = time.monotonic() - started
        # Broad lower bound only: no machine-speed or throughput upper target.
        minimum = (len(plan) - 1) / RATE * 0.9
        self.assertEqual(len(dispatches), len(plan))
        self.assertGreaterEqual(max(dispatches) - started, minimum)
        self.assertGreaterEqual(elapsed, minimum)
        self.assertGreaterEqual(result["elapsed_seconds"], minimum)
        self.assert_evidence(plan, result)

    def test_latency_includes_wait_behind_a_blocked_callback(self):
        plan = phase_plan(2)
        entered = threading.Event()
        release = threading.Event()
        outcome = queue.Queue()
        first_key = []

        def execute(op, key):
            if not first_key:
                first_key.append(key)
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("test did not release blocked callback")
            return True

        def run():
            try:
                outcome.put(self.run_phase(plan, execute, RATE, 1, self.output))
            except BaseException as error:
                outcome.put(error)

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        try:
            self.assertTrue(entered.wait(5), "scheduler never invoked execute")
            hold_start = time.monotonic()
            release.wait(0.1)
            held_ms = (time.monotonic() - hold_start) * 1000
        finally:
            release.set()
            worker.join(5)
        self.assertFalse(worker.is_alive(), "scheduler did not finish")
        result = outcome.get_nowait()
        if isinstance(result, BaseException):
            raise result
        by_key = {r["key"]: r for r in self.assert_evidence(plan, result)}
        blocked = by_key[first_key[0]]
        queued = by_key[next(key for key in by_key if key != first_key[0])]
        self.assertGreaterEqual(blocked["service_ms"], held_ms * 0.8)
        # The queued operation's schedule must remain anchored to phase start,
        # even when all workers are occupied. Queue time is not service time.
        self.assertGreaterEqual(queued["dispatch_lag_ms"], held_ms * 0.8)
        self.assertGreaterEqual(queued["latency_ms"] - queued["service_ms"],
                                held_ms * 0.8)
        self.assertGreaterEqual(result["elapsed_seconds"] * 1000,
                                held_ms * 0.8)

    def test_invalid_inputs_fail_before_output_creation_or_callback(self):
        cases = [("plan", [])]
        cases += [("rate", value) for value in
                  (0, -1, float("nan"), float("inf"), float("-inf"),
                   True, False, None, "10000")]
        cases += [("concurrency", value) for value in
                  (0, -1, 1.5, True, False, None, "2")]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                calls = []
                args = dict(plan=phase_plan(1), rate=RATE, concurrency=1,
                            output_path=self.output,
                            execute=lambda op, key: calls.append((op, key)))
                args[field] = value
                with self.assertRaises(VALIDATION_ERRORS):
                    self.run_phase(**args)
                self.assertEqual(calls, [])
                self.assertFalse(self.output.exists())
                self.assertEqual(list(self.directory.iterdir()), [])

    def test_existing_output_is_refused_without_overwrite_or_execution(self):
        original = b"existing evidence must survive\n"
        self.output.write_bytes(original)
        calls = []
        with self.assertRaises(FileExistsError):
            self.run_phase(phase_plan(2),
                           lambda op, key: calls.append((op, key)),
                           RATE, 1, self.output)
        self.assertEqual(calls, [])
        self.assertEqual(self.output.read_bytes(), original)

    def test_callback_requires_actual_bool_and_summary_has_wall_clock_bounds(self):
        plan = phase_plan(5)
        results = [1, 0, None, "true", True]
        before = time.time()
        result = self.run_phase(plan, lambda op, key: results[key], RATE, 2, self.output)
        after = time.time()
        rows = self.assert_evidence(plan, result, errors=4)
        self.assertLessEqual(before, result["start_unix"])
        self.assertLessEqual(result["start_unix"], result["end_unix"])
        self.assertLessEqual(result["end_unix"], after)
        for row in rows:
            if row["key"] != 4:
                self.assertIn("execute must return bool", row["error"])

    def test_blocked_workers_do_not_allow_unbounded_submission(self):
        real_executor = self.runner.ThreadPoolExecutor
        submitted = []
        saturated = threading.Event()
        release = threading.Event()
        outcome = queue.Queue()

        class ObservedExecutor(real_executor):
            def submit(self, *args, **kwargs):
                future = super().submit(*args, **kwargs)
                submitted.append(future)
                if len(submitted) == 3:
                    saturated.set()
                return future

        def execute(op, key):
            if not release.wait(5):
                raise TimeoutError("test did not release workers")
            return True

        def run():
            try:
                outcome.put(self.run_phase(phase_plan(100), execute, RATE, 3, self.output))
            except BaseException as exc:
                outcome.put(exc)

        with patch.object(self.runner, "ThreadPoolExecutor", ObservedExecutor):
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            try:
                self.assertTrue(saturated.wait(5))
                release.wait(0.05)
                self.assertEqual(len(submitted), 3)
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        result = outcome.get_nowait()
        if isinstance(result, BaseException):
            raise result
        self.assert_evidence(phase_plan(100), result)

    def test_worker_evidence_write_failure_propagates(self):
        class BrokenOutput(io.StringIO):
            def write(self, data):
                raise OSError("evidence disk full")

        with patch.object(self.runner, "open", return_value=BrokenOutput(), create=True):
            with self.assertRaisesRegex(OSError, "evidence disk full"):
                self.run_phase(phase_plan(5), lambda op, key: True, RATE, 2, self.output)

    def test_cli_help_without_site_packages(self):
        result = subprocess.run([sys.executable, "-B", "-S", str(RUNNER_PATH), "--help"],
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--drain-seconds", result.stdout)

    def run_mock_cli(self, failure=None):
        cassandra = ModuleType("cassandra")
        cassandra.__version__ = "3.29.2"
        cassandra.ConsistencyLevel = SimpleNamespace(QUORUM=4)
        cluster_module = ModuleType("cassandra.cluster")
        policies = ModuleType("cassandra.policies")
        policies.RoundRobinPolicy = type("RoundRobinPolicy", (), {})
        policies.FallthroughRetryPolicy = type("FallthroughRetryPolicy", (), {})
        stored = {}
        statements = []
        calls = []
        lifecycle = []

        class Session:
            def execute(self, statement, parameters=None, timeout=None):
                if isinstance(statement, str):
                    calls.append(statement)
                    if failure == "exists" and statement.startswith("CREATE KEYSPACE"):
                        raise RuntimeError("keyspace already exists")
                    return []
                calls.append((statement.query, parameters, timeout))
                table = statement.query.split()[2 if statement.query.startswith("INSERT") else 3]
                identity = (table, *parameters[:2])
                if statement.query.startswith("INSERT"):
                    if failure == "preload":
                        raise RuntimeError("write failed")
                    stored[identity] = parameters[2]
                    return []
                value = b"wrong" if failure == "mismatch" else stored[identity]
                return [SimpleNamespace(v=value)]

            def prepare(self, query):
                statement = SimpleNamespace(query=query)
                statements.append(statement)
                return statement

        session = Session()

        class Cluster:
            def connect(self):
                return session

            def shutdown(self):
                lifecycle.append("shutdown")

        class Opener:
            def open(self, url, timeout):
                lifecycle.append("scrape")
                if not url.startswith("https://"):
                    raise OSError("admin listener requires TLS")
                if failure == "collector":
                    raise OSError("scrape unavailable")
                if url.endswith("metrics"):
                    return io.BytesIO(b"ferrosa_test 1\n")
                return io.BytesIO(json.dumps([
                    {"keyspace": "cx_test", "table": "t0"},
                    {"keyspace": "other", "table": "t0"},
                ]).encode())

        output = self.directory / "cli"
        ca = self.directory / "ca.pem"
        ca.write_text("mock CA", encoding="utf-8")
        argv = ["--hosts", "fd00::1,fd00::2,fd00::3", "--ca-file", str(ca),
                "--output", str(output), "--keyspace", "cx_test", "--keys", "8",
                "--value-bytes", "16", "--tables", "2", "--rows-per-partition", "2",
                "--rate", str(RATE), "--seconds", "0.0002", "--drain-seconds", "0.02"]
        argv.extend(["--preload-rate", str(RATE / 2)])
        cluster_module.Cluster = Cluster
        context = SimpleNamespace()
        with (patch.dict(sys.modules, {"cassandra": cassandra,
                                      "cassandra.cluster": cluster_module,
                                      "cassandra.policies": policies}),
              patch.object(cluster_module, "Cluster", return_value=Cluster()) as cluster_factory,
              patch.object(self.runner.ssl, "create_default_context", return_value=context) as tls,
              patch.object(self.runner.urllib.request, "build_opener", return_value=Opener()),
              patch.object(self.runner.urllib.request, "ProxyHandler") as proxies,
              patch("builtins.print") as progress):
            status = self.runner.main(argv)
        self.assertEqual(lifecycle[-1], "shutdown")
        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, self.runner.ssl.CERT_REQUIRED)
        tls.assert_called_once_with(cafile=str(ca))
        config = cluster_factory.call_args.kwargs
        self.assertEqual(config["contact_points"], ["fd00::1", "fd00::2", "fd00::3"])
        self.assertEqual(config["protocol_version"], 4)
        self.assertIsInstance(config["load_balancing_policy"], policies.RoundRobinPolicy)
        self.assertIsInstance(config["default_retry_policy"], policies.FallthroughRetryPolicy)
        self.assertEqual(session.default_timeout, 30)
        self.assertEqual(session.default_consistency_level, 4)
        for statement in statements:
            self.assertEqual(statement.consistency_level, 4)
            self.assertIsInstance(statement.retry_policy, policies.FallthroughRetryPolicy)
        for call in progress.call_args_list:
            self.assertTrue(call.kwargs["flush"])
        if failure != "exists":
            proxies.assert_called_once_with({})
        return status, output, stored, calls

    def test_cli_runs_all_phases_through_scheduler_and_collects_filtered_evidence(self):
        status, output, stored, calls = self.run_mock_cli()
        self.assertEqual(status, 0)
        self.assertEqual(len(stored), 8)
        workload = self.runner._sibling("workload")
        for key in range(8):
            table = key % 2
            p, c = divmod(key // 2, 2)
            self.assertEqual(stored[(f"cx_test.t{table}", p, c)], workload.value_for(key, 16))
        self.assertNotIn("IF NOT EXISTS", calls[0])
        self.assertIn("'replication_factor': 3", calls[0])
        self.assertIn("SizeTieredCompactionStrategy", calls[1])
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["driver_version"], "3.29.2")
        self.assertEqual(manifest["collector_errors"], [])
        self.assertIn("python_version", manifest)
        summaries = json.loads((output / "phase-summary.json").read_text())
        self.assertEqual(set(summaries), {"preload", "mixed", "drain", "verify"})
        for phase, count in (("preload", 8), ("mixed", 2), ("drain", 2), ("verify", 8)):
            summary = summaries[phase]
            rows = [json.loads(line) for line in (output / f"{phase}.jsonl").read_text().splitlines()]
            self.assertEqual(len(rows), count)
            self.assertEqual(summary["stats"]["combined"]["count"], count)
            self.assertLessEqual(summary["start_unix"], summary["end_unix"])
            if phase == "mixed":
                expected = workload.plan_phase(2, 0.5, 8, 601)
                self.assertEqual(Counter((r["op"], r["key"]) for r in rows),
                                 Counter((r["op"], r["key"]) for r in expected))
            if phase in ("drain", "verify"):
                self.assertTrue(all(row["op"] == "read" for row in rows))
        metrics = [json.loads(line) for line in (output / "metrics.jsonl").read_text().splitlines()]
        self.assertEqual({row["node"] for row in metrics}, {"fd00::1", "fd00::2", "fd00::3"})
        for row in metrics:
            self.assertEqual(row["storage"], [{"keyspace": "cx_test", "table": "t0"}])
            self.assertEqual(row["metrics"], "ferrosa_test 1\n")

    def test_cli_aborts_failed_phase_preserving_diagnostics_without_stats(self):
        for failure, last_phase, count in (("preload", "preload", 8), ("mismatch", "mixed", 2)):
            with self.subTest(failure=failure):
                self.directory = self.directory / failure
                self.directory.mkdir()
                status, output, _, _ = self.run_mock_cli(failure)
                self.assertEqual(status, 1)
                manifest = json.loads((output / "manifest.json").read_text())
                self.assertEqual(manifest["status"], "failed")
                summaries = json.loads((output / "phase-summary.json").read_text())
                self.assertIn("diagnostic", summaries[last_phase])
                self.assertNotIn("stats", summaries[last_phase])
                self.assertEqual(len((output / f"{last_phase}.jsonl").read_text().splitlines()), count)
                self.assertFalse((output / "drain.jsonl").exists())

    def test_cli_collector_errors_fail_final_run(self):
        status, output, _, _ = self.run_mock_cli("collector")
        self.assertEqual(status, 1)
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(len(manifest["collector_errors"]), 3)
        self.assertIn("collector recorded", manifest["diagnostic"])
        self.assertTrue((output / "verify.jsonl").exists())

    def test_cli_fresh_keyspace_creation_error_is_fatal(self):
        status, output, _, _ = self.run_mock_cli("exists")
        self.assertEqual(status, 1)
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertIn("keyspace already exists", manifest["diagnostic"])
        self.assertFalse((output / "preload.jsonl").exists())

    def test_cli_preflight_rejects_unsafe_keyspaces_and_existing_directories(self):
        ca = self.directory / "ca.pem"
        ca.write_text("mock CA", encoding="utf-8")
        argv = ["--hosts", "fd00::1", "--ca-file", str(ca), "--output", str(self.directory)]
        with patch("sys.stderr", new_callable=io.StringIO):
            for keyspace in ("cx_test; DROP KEYSPACE system", "system", "cx_", "cx_valid"):
                with self.subTest(keyspace=keyspace), self.assertRaises(SystemExit) as error:
                    self.runner.main(argv + ["--keyspace", keyspace])
                self.assertEqual(error.exception.code, 2)
        self.assertEqual(list(self.directory.iterdir()), [ca])


if __name__ == "__main__":
    unittest.main()
