#!/usr/bin/env python3
"""Run a bounded, fixed-rate compaction experiment against a fresh keyspace."""

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import importlib.util
import ipaddress
import json
import math
from pathlib import Path
import platform
import re
import ssl
import threading
import time
import urllib.request


def _positive_number(value, name):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")


def run_phase(plan, execute, rate, concurrency, output_path):
    """Execute each planned operation once, writing completion-order JSONL.

    Latency is measured from the original scheduled arrival, including admission
    delay. Service time starts inside the worker. Both futures and submitted work
    are bounded by concurrency; slow service never creates an unbounded queue.
    Callback exceptions and non-bool results become error evidence. Infrastructure
    failures (including evidence writes) propagate through observed futures.
    """
    _positive_number(rate, "rate")
    if type(concurrency) is not int or concurrency <= 0:
        raise ValueError("concurrency must be a positive integer")
    if not callable(execute):
        raise TypeError("execute must be callable")
    plan = list(plan)
    if not plan:
        raise ValueError("plan must not be empty")
    for row in plan:
        if (not isinstance(row, dict) or row.get("op") not in ("read", "write")
                or type(row.get("key")) is not int or row["key"] < 0):
            raise ValueError("plan rows require read/write op and nonnegative integer key")

    admission = threading.BoundedSemaphore(concurrency)
    writing = threading.Lock()
    counts = {"errors": 0, "mismatches": 0}
    with open(output_path, "x", encoding="utf-8") as output:
        start_unix = time.time()
        start = time.monotonic()

        def worker(sequence, op, key, scheduled):
            try:
                dispatched = time.monotonic()
                record = {"sequence": sequence, "op": op, "key": key,
                          "ok": False, "mismatch": False}
                try:
                    valid = execute(op, key)
                    if type(valid) is not bool:
                        raise TypeError("execute must return bool")
                    record.update(ok=valid, mismatch=not valid)
                except Exception as exc:
                    record["error"] = f"{type(exc).__name__}: {exc}"
                completed = time.monotonic()
                record.update(latency_ms=(completed - scheduled) * 1000,
                              scheduled_unix=start_unix + (scheduled - start),
                              completed_unix=start_unix + (completed - start),
                              service_ms=(completed - dispatched) * 1000,
                              dispatch_lag_ms=(dispatched - scheduled) * 1000)
                with writing:
                    output.write(json.dumps(record, allow_nan=False) + "\n")
                    output.flush()
                    counts["errors"] += int("error" in record)
                    counts["mismatches"] += int(record["mismatch"])
            finally:
                admission.release()

        pending = set()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for index, row in enumerate(plan):
                scheduled = start + index / rate
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                # Observe completed futures, including write failures, before
                # replacing them. Never retain a future per planned operation.
                if len(pending) == concurrency:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        future.result()
                admission.acquire()
                try:
                    pending.add(executor.submit(worker, index, row["op"], row["key"], scheduled))
                except BaseException:
                    admission.release()
                    raise
            for future in pending:
                future.result()
        elapsed = time.monotonic() - start
        end_unix = time.time()
    return dict(expected_operations=len(plan), elapsed_seconds=elapsed,
                start_unix=start_unix, end_unix=end_unix, **counts)


def _sibling(name):
    # Works when this file itself was loaded by spec_from_file_location.
    spec = importlib.util.spec_from_file_location(
        f"compaction_{name}", Path(__file__).with_name(f"{name}.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True, allow_nan=False)
        output.write("\n")
    temporary.replace(path)


class Collector:
    """Collect immediately and every two seconds; preserve every scrape error."""

    def __init__(self, hosts, keyspace, output_path, ssl_context=None):
        self.hosts = hosts
        self.keyspace = keyspace
        self.output_path = output_path
        self.errors = []
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._run, name="compaction-metrics")
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=ssl_context))

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopping.set()
        self.thread.join()

    def _sample(self, host):
        record = {"time_unix": time.time(), "node": host, "metrics": "", "storage": []}
        errors = []
        for endpoint in ("metrics", "api/storage_stats"):
            try:
                with self.opener.open(f"https://[{host}]:9090/{endpoint}", timeout=5) as response:
                    body = response.read().decode("utf-8")
                if endpoint == "metrics":
                    record["metrics"] = body
                else:
                    rows = json.loads(body)
                    if not isinstance(rows, list) or any(
                            not isinstance(row, dict) or "keyspace" not in row for row in rows):
                        raise ValueError("storage_stats must be a list of objects with keyspace")
                    record["storage"] = [row for row in rows if row["keyspace"] == self.keyspace]
            except Exception as exc:
                errors.append(f"{endpoint}: {type(exc).__name__}: {exc}")
        if errors:
            record["errors"] = errors
            self.errors.append({"time_unix": record["time_unix"], "node": host, "errors": errors})
        return record

    def _run(self):
        try:
            with self.output_path.open("x", encoding="utf-8") as output:
                while True:
                    started = time.monotonic()
                    for host in self.hosts:
                        output.write(json.dumps(self._sample(host), allow_nan=False) + "\n")
                        output.flush()
                    if self.stopping.wait(max(0, 2 - (time.monotonic() - started))):
                        break
        except BaseException as exc:
            # Thread exceptions cannot be allowed to disappear at join().
            self.errors.append({"time_unix": time.time(), "error": f"{type(exc).__name__}: {exc}"})


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hosts", required=True, help="comma-separated IPv6 addresses")
    parser.add_argument("--ca-file", required=True)
    parser.add_argument("--output", required=True, help="new artifact directory (must not exist)")
    parser.add_argument("--keyspace", required=True, help="fresh cx_* identifier")
    for name, default in (("keys", 4096), ("value-bytes", 65536),
                          ("rows-per-partition", 1), ("tables", 1), ("concurrency", 32)):
        parser.add_argument(f"--{name}", type=int, default=default)
    for name, default in (("rate", 300), ("seconds", 120),
                          ("read-fraction", 0.5), ("drain-seconds", 60)):
        parser.add_argument(f"--{name}", type=float, default=default)
    parser.add_argument("--seed", type=int, default=601)
    parser.add_argument("--preload-rate", type=float,
                        help="preload writes/sec (default: measured phase rate)")
    return parser


def _validate_args(args):
    if not re.fullmatch(r"cx_[a-z0-9_]+", args.keyspace) or len(args.keyspace) > 48:
        raise ValueError("keyspace must be a lowercase cx_* identifier of at most 48 characters")
    hosts = [str(ipaddress.IPv6Address(host.strip().strip("[]")))
             for host in args.hosts.split(",")]
    for field in ("keys", "value_bytes", "rows_per_partition", "tables", "concurrency",
                  "rate", "seconds", "drain_seconds"):
        _positive_number(getattr(args, field), field)
    if args.preload_rate is not None:
        _positive_number(args.preload_rate, "preload_rate")
    if not math.isfinite(args.read_fraction) or not 0 <= args.read_fraction <= 1:
        raise ValueError("read_fraction must be finite and between 0 and 1")
    for rate, seconds in ((args.rate, args.seconds), (min(args.rate, 100), args.drain_seconds)):
        if not math.isfinite(rate * seconds) or round(rate * seconds) < 1:
            raise ValueError("each timed phase must schedule at least one operation")
    if not Path(args.ca_file).is_file():
        raise ValueError("ca-file must be an existing CA certificate file")
    return hosts


def _experiment(args, hosts, output, manifest):
    # Import only after argument parsing, so --help and run_phase need no driver.
    import cassandra
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster
    from cassandra.policies import FallthroughRetryPolicy, RoundRobinPolicy

    manifest["driver_version"] = cassandra.__version__
    _write_json(output / "manifest.json", manifest)
    if cassandra.__version__ != "3.29.2":
        raise RuntimeError("cassandra-driver==3.29.2 is required")
    workload, analyze = _sibling("workload"), _sibling("analyze")
    context = ssl.create_default_context(cafile=args.ca_file)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    cluster = Cluster(contact_points=hosts, protocol_version=4, ssl_context=context,
                      load_balancing_policy=RoundRobinPolicy(),
                      default_retry_policy=FallthroughRetryPolicy())
    collector = None
    summaries = {}
    try:
        session = cluster.connect()
        session.default_timeout = 30
        session.default_consistency_level = ConsistencyLevel.QUORUM
        session.execute(
            f"CREATE KEYSPACE {args.keyspace} WITH replication = "
            "{'class': 'SimpleStrategy', 'replication_factor': 3}")
        inserts, selects = [], []
        for table in range(args.tables):
            name = f"{args.keyspace}.t{table}"
            session.execute(f"CREATE TABLE {name} (p int, c int, v blob, PRIMARY KEY (p,c)) "
                            "WITH compaction = {'class': 'SizeTieredCompactionStrategy'}")
            insert = session.prepare(f"INSERT INTO {name} (p,c,v) VALUES (?,?,?)")
            select = session.prepare(f"SELECT v FROM {name} WHERE p=? AND c=?")
            for statement in (insert, select):
                statement.consistency_level = ConsistencyLevel.QUORUM
                statement.retry_policy = FallthroughRetryPolicy()
            inserts.append(insert)
            selects.append(select)
        values = [workload.value_for(key, args.value_bytes) for key in range(args.keys)]

        def execute(op, key):
            table = key % args.tables
            local = key // args.tables
            partition, clustering = divmod(local, args.rows_per_partition)
            if op == "write":
                session.execute(inserts[table], (partition, clustering, values[key]), timeout=30)
                return True
            rows = list(session.execute(selects[table], (partition, clustering), timeout=30))
            return len(rows) == 1 and rows[0].v == values[key]

        def phase(name, plan, rate):
            path = output / f"{name}.jsonl"
            summary = {"expected_operations": len(plan), "start_unix": time.time()}
            summaries[name] = summary
            try:
                summary.update(run_phase(plan, execute, rate, args.concurrency, path))
                if summary["errors"] or summary["mismatches"]:
                    raise RuntimeError(f"{name}: {summary['errors']} errors, "
                                       f"{summary['mismatches']} mismatches")
                with path.open(encoding="utf-8") as source:
                    summary["stats"] = analyze.summarize_phase(
                        (json.loads(line) for line in source), len(plan), summary["elapsed_seconds"])
            except BaseException as exc:
                summary["diagnostic"] = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                summary.setdefault("end_unix", time.time())
                _write_json(output / "phase-summary.json", summaries)
                print(f"{name}: operations={summary['expected_operations']} "
                      f"errors={summary.get('errors', 'unknown')} "
                      f"mismatches={summary.get('mismatches', 'unknown')} "
                      f"elapsed={summary.get('elapsed_seconds', 0):.3f}s "
                      f"{summary.get('diagnostic', 'OK')}", flush=True)

        collector = Collector(hosts, args.keyspace, output / "metrics.jsonl", context)
        collector.start()
        phase("preload", [{"op": "write", "key": key} for key in range(args.keys)],
              args.rate if args.preload_rate is None else args.preload_rate)
        phase("mixed", workload.plan_phase(round(args.rate * args.seconds), args.read_fraction,
                                           args.keys, args.seed), args.rate)
        drain_rate = min(args.rate, 100)
        phase("drain", workload.plan_phase(round(drain_rate * args.drain_seconds), 1,
                                           args.keys, args.seed), drain_rate)
        phase("verify", [{"op": "read", "key": key} for key in range(args.keys)], args.rate)
    finally:
        try:
            if collector is not None:
                collector.stop()
                manifest["collector_errors"] = collector.errors
        finally:
            cluster.shutdown()
    if collector.errors:
        raise RuntimeError(f"collector recorded {len(collector.errors)} errors; see manifest and metrics.jsonl")


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        hosts = _validate_args(args)
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=False)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    manifest = {"args": vars(args), "python_version": platform.python_version(),
                "driver_version": None, "start_unix": time.time(), "status": "running"}
    _write_json(output / "manifest.json", manifest)
    try:
        _experiment(args, hosts, output, manifest)
        manifest["status"] = "complete"
        return 0
    except BaseException as exc:
        manifest.update(status="failed", diagnostic=f"{type(exc).__name__}: {exc}")
        print(f"FAILED: {manifest['diagnostic']}", flush=True)
        if not isinstance(exc, Exception):
            raise
        return 1
    finally:
        manifest["end_unix"] = time.time()
        _write_json(output / "manifest.json", manifest)


if __name__ == "__main__":
    raise SystemExit(main())
