#!/usr/bin/env python3
"""Measure a database through ONE of its front ends, with validated results.

Correctness: every timed point read returns the expected key, bucket and payload;
counts and bounded scans must be complete before timings are accepted.
Last revised: 2026-10-24
Last changed: Added value validation, deterministic reads and read-only probing.

Four legs, one shape of measurement, so the numbers are comparable:

    cql   -> ferrosa / Apache Cassandra     (cassandra-driver)
    pg    -> ferrosa pgwire / PostgreSQL     (psycopg3)
    bolt  -> ferrosa bolt / Neo4j            (neo4j driver)

Per size it reports the three metric families the benchmark is defined on:

    insert_rows_per_s   the write rate that built this size
    count_time_s        median wall time, request to result, of the aggregate
    read_p50/p95/p99_ms point-read latency at random keys, plus a bounded scan

Why a thread pool for every leg
-------------------------------
Each engine has its natural concurrency idiom: CQL clients pipeline in-flight
requests, psycopg blocks per connection, Bolt multiplexes a session pool. Using
each engine's favourite idiom measures the idiom. So every leg runs the SAME
shape: N worker threads, one connection each, disjoint key ranges, and the
clock stops when the last write completes. That is a fair comparison and it is
stated here because it is a deliberate choice, not an oversight.

Usage:
    multimetric_bench.py --engine cql --host H --port 9042 \
        --stages 100000,1000000 --concurrency 8 --out results.json
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

PAYLOAD_LEN = 96


def make_payloads(n: int) -> list[str]:
    """Deterministic payloads so every engine writes the same bytes."""
    rng = random.Random(1234)
    alphabet = string.ascii_letters
    return ["".join(rng.choices(alphabet, k=PAYLOAD_LEN)) for _ in range(max(1, n))]


# ─────────────────────────────── adapters ────────────────────────────────


class CqlAdapter:
    """Apache Cassandra / ferrosa CQL front end."""

    label = "cql"

    def __init__(self, args):
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster

        self.args = args
        # One Cluster, one session per thread: sessions are expensive to build
        # and cheap to share.
        auth = PlainTextAuthProvider(username=args.user, password=args.password) if args.user else None
        self.cluster = Cluster(
            contact_points=[args.host],
            port=args.port,
            auth_provider=auth,
            # Explicit, not inherited: the Python client enables compression by
            # default when lz4 is importable, and a comparison where one engine
            # silently negotiates compression and the other does not is not a
            # comparison. Off here means off for both sides.
            compression=args.compression,
            connect_timeout=30,
            control_connection_timeout=30,
        )
        self.session = self.cluster.connect()
        self._local = threading.local()

    def _session(self):
        if not hasattr(self._local, "session"):
            self._local.session = self.cluster.connect()
        return self._local.session

    def ensure_schema(self):
        s = self.session
        # Fresh keyspace per run: a leg that inherits rows from an earlier run
        # reports no insert rate and measures a warm table.
        s.execute(f"DROP KEYSPACE IF EXISTS {self.args.keyspace}")
        s.execute(
            f"CREATE KEYSPACE {self.args.keyspace} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        s.execute(
            f"CREATE TABLE {self.args.keyspace}.{self.args.table} "
            "(pk int PRIMARY KEY, bucket int, payload text)"
        )
        s.execute(f"SELECT count(*) FROM {self.args.keyspace}.{self.args.table}").one()

    def _insert_stmt(self, session):
        return session.prepare(
            f"INSERT INTO {self.args.keyspace}.{self.args.table} (pk, bucket, payload) "
            "VALUES (?, ?, ?)"
        )

    def load_range(self, start, stop, payloads):
        session = self._session()
        stmt = self._insert_stmt(session)
        # Pipeline within the thread: await in waves so a worker keeps several
        # requests in flight, matching how the other legs' connections behave.
        wave = 32
        pending = []
        for pk in range(start, stop):
            pending.append(session.execute_async(stmt, (pk, pk % 64, payloads[pk % len(payloads)])))
            if len(pending) >= wave:
                for fut in pending:
                    fut.result()
                pending.clear()
        for fut in pending:
            fut.result()

    def count(self):
        return self.session.execute(
            f"SELECT count(*) FROM {self.args.keyspace}.{self.args.table}"
        ).one()[0]

    def count_seconds(self):
        began = time.perf_counter()
        self.session.execute(f"SELECT count(*) FROM {self.args.keyspace}.{self.args.table}").one()
        return time.perf_counter() - began

    def _read_stmt(self, session):
        if not hasattr(self._local, "read_stmt"):
            self._local.read_stmt = session.prepare(
                f"SELECT pk, bucket, payload FROM {self.args.keyspace}.{self.args.table} "
                "WHERE pk = ?"
            )
        return self._local.read_stmt

    def point_read(self, pk):
        session = self._session()
        return session.execute(self._read_stmt(session), (pk,)).one()

    def scan(self, limit):
        return list(
            self.session.execute(
                f"SELECT pk, bucket, payload FROM {self.args.keyspace}.{self.args.table} LIMIT {limit}"
            )
        )

    def close(self):
        self.cluster.shutdown()


class PgAdapter:
    """PostgreSQL wire front end (ferrosa pgwire / PostgreSQL)."""

    label = "pg"

    def __init__(self, args):
        self.args = args
        self._local = threading.local()
        self._dsn = (
            f"host={args.host} port={args.port} dbname={args.database} "
            f"user={args.user} password={args.password}"
        )

    def _conn(self):
        if not hasattr(self._local, "conn"):
            import psycopg

            conn = psycopg.connect(self._dsn, autocommit=True)
            self._local.conn = conn
        return self._local.conn

    def ensure_schema(self):
        conn = self._conn()
        with conn.cursor() as cur:
            # Fresh table per run: inheriting rows would report no insert rate.
            cur.execute(f"DROP TABLE IF EXISTS {self.args.table}")
            cur.execute(
                f"CREATE TABLE {self.args.table} "
                "(pk integer PRIMARY KEY, bucket integer, payload text)"
            )

    def load_range(self, start, stop, payloads):
        conn = self._conn()
        # execute_values-style batched insert: a single round trip per wave,
        # which is the idiomatic fast path for PostgreSQL writes.
        wave = 256
        with conn.cursor() as cur:
            batch = []
            for pk in range(start, stop):
                batch.append((pk, pk % 64, payloads[pk % len(payloads)]))
                if len(batch) >= wave:
                    cur.executemany(
                        f"INSERT INTO {self.args.table} (pk, bucket, payload) VALUES (%s, %s, %s)",
                        batch,
                    )
                    batch.clear()
            if batch:
                cur.executemany(
                    f"INSERT INTO {self.args.table} (pk, bucket, payload) VALUES (%s, %s, %s)",
                    batch,
                )

    def count(self):
        with self._conn().cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.args.table}")
            return cur.fetchone()[0]

    def count_seconds(self):
        began = time.perf_counter()
        with self._conn().cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.args.table}")
            cur.fetchone()
        return time.perf_counter() - began

    def point_read(self, pk):
        with self._conn().cursor() as cur:
            cur.execute(
                f"SELECT pk, bucket, payload FROM {self.args.table} WHERE pk = %s", (pk,)
            )
            return cur.fetchone()

    def scan(self, limit):
        with self._conn().cursor() as cur:
            cur.execute(f"SELECT pk, bucket, payload FROM {self.args.table} LIMIT {limit}")
            return cur.fetchall()

    def close(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()


class BoltAdapter:
    """Bolt front end (ferrosa bolt / Neo4j)."""

    label = "bolt"

    def __init__(self, args):
        from neo4j import GraphDatabase

        self.args = args
        # An IPv6 literal must be bracketed in a URI, or the driver treats the
        # last colon-separated group as a service name and DNS-fails.
        host = f"[{args.host}]" if ":" in args.host and not args.host.startswith("[") else args.host
        self.driver = GraphDatabase.driver(
            f"bolt://{host}:{args.port}",
            auth=(args.user, args.password) if args.user else None,
        )
        # Ferrosa selects a keyspace per session; Neo4j selects a database. Both
        # arrive here as the driver's `database`, and Neo4j's default (unset) must
        # stay unset or it looks for a database named after the ferrosa keyspace.
        self.database = getattr(args, "bolt_database", None)
        self._local = threading.local()

    def _session(self):
        if not hasattr(self._local, "session"):
            self._local.session = self.driver.session(database=self.database)
        return self._local.session

    def ensure_schema(self):
        # A lookup by pk needs an index to be comparable with a primary-key read
        # on the other engines; without one this measures label scans. The DDL
        # differs between Cypher implementations (Neo4j accepts IF NOT EXISTS,
        # ferrosa's engine does not), so a refusal here must not abort the leg --
        # it means we measure scans, which the read numbers will show.
        try:
            # Fresh graph per run: inheriting nodes would report no insert rate.
            with self.driver.session(database=self.database) as s:
                s.run("MATCH (n:BenchRow) DETACH DELETE n")
            with self.driver.session(database=self.database) as s:
                s.run("CREATE INDEX bench_pk IF NOT EXISTS FOR (n:BenchRow) ON (n.pk)")
            # Wait for the index to come ONLINE, else the read leg measures a
            # building index rather than a serving one.
            deadline = time.time() + 120
            while time.time() < deadline:
                with self.driver.session(database=self.database) as s:
                    rec = s.run(
                        "SHOW INDEXES YIELD name, state WHERE name = 'bench_pk' RETURN state"
                    ).single()
                if rec and rec["state"] == "ONLINE":
                    return
                time.sleep(0.5)
        except Exception as exc:  # noqa: BLE001 - index support is engine-specific
            self.index_note = f"index not established: {type(exc).__name__}"
            print(f"[bolt] {self.index_note}", file=sys.stderr, flush=True)

    def load_range(self, start, stop, payloads):
        session = self._session()
        wave = 128
        batch = []
        for pk in range(start, stop):
            batch.append({"pk": pk, "bucket": pk % 64, "payload": payloads[pk % len(payloads)]})
            if len(batch) >= wave:
                session.run(
                    "UNWIND $rows AS row CREATE (n:BenchRow {pk: row.pk, bucket: row.bucket, payload: row.payload})",
                    rows=batch,
                )
                batch.clear()
        if batch:
            session.run(
                "UNWIND $rows AS row CREATE (n:BenchRow {pk: row.pk, bucket: row.bucket, payload: row.payload})",
                rows=batch,
            )

    def count(self):
        with self.driver.session(database=self.database) as s:
            rec = s.run("MATCH (n:BenchRow) RETURN count(n) AS c").single()
        # Neo4j returns one row holding 0 when the match is empty; ferrosa's
        # engine returns no row at all. Both mean "no nodes", so a missing record
        # is 0 rather than an error.
        return rec["c"] if rec else 0

    def count_seconds(self):
        began = time.perf_counter()
        with self.driver.session(database=self.database) as s:
            s.run("MATCH (n:BenchRow) RETURN count(n) AS c").single()
        return time.perf_counter() - began

    def point_read(self, pk):
        with self.driver.session(database=self.database) as s:
            return s.run(
                "MATCH (n:BenchRow {pk: $pk}) RETURN n.pk AS pk, n.bucket AS bucket, n.payload AS payload",
                pk=pk,
            ).single()

    def scan(self, limit):
        with self.driver.session(database=self.database) as s:
            return list(s.run(f"MATCH (n:BenchRow) RETURN n.pk AS pk LIMIT {limit}"))

    def close(self):
        self.driver.close()


ADAPTERS = {"cql": CqlAdapter, "pg": PgAdapter, "bolt": BoltAdapter}


# ─────────────────────────────── measurement ─────────────────────────────


def load(adapter, start, stop, concurrency, payloads):
    """Insert [start, stop) across `concurrency` workers; return rows/sec."""
    total = stop - start
    if total <= 0:
        return None
    chunk = (total + concurrency - 1) // concurrency
    began = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = []
        for i in range(concurrency):
            lo = start + i * chunk
            hi = min(stop, lo + chunk)
            if lo < hi:
                futures.append(pool.submit(adapter.load_range, lo, hi, payloads))
        for fut in futures:
            fut.result()
    elapsed = time.perf_counter() - began
    return total / elapsed if elapsed else 0.0


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round(p / 100.0 * (len(sorted_vals) - 1))))
    return sorted_vals[idx] * 1000.0


def measure(adapter, args, stage, actual, insert_rate):
    if stage <= 0 or actual != stage:
        raise ValueError(f"row census mismatch: requested {stage}, found {actual}")
    entry = {"rows": stage, "rows_actual": actual}
    if insert_rate is not None:
        entry["insert_rows_per_s"] = insert_rate

    samples = []
    for _ in range(args.repeats):
        began = time.perf_counter()
        counted = adapter.count()
        elapsed = time.perf_counter() - began
        if counted != stage:
            raise ValueError(f"count changed during measurement: {counted} != {stage}")
        samples.append(elapsed)
    entry.update(
        count_time_s=statistics.median(samples),
        count_min_s=min(samples),
        count_max_s=max(samples),
        samples_s=samples,
    )

    latencies = []
    rng = random.Random(args.seed)
    payloads = make_payloads(512)
    for _ in range(args.read_samples):
        pk = rng.randrange(stage)
        began = time.perf_counter()
        received = adapter.point_read(pk)
        elapsed = time.perf_counter() - began
        expected = (pk, pk % 64, payloads[pk % len(payloads)])
        if received is None or tuple(received) != expected:
            raise ValueError(f"point read returned missing or incorrect values for pk={pk}")
        latencies.append(elapsed)
    latencies.sort()
    entry["read_samples"] = len(latencies)
    entry["read_p50_ms"] = percentile(latencies, 50)
    entry["read_p95_ms"] = percentile(latencies, 95)
    entry["read_p99_ms"] = percentile(latencies, 99)

    scan_limit = min(10_000, max(1, actual))
    began = time.perf_counter()
    scanned_rows = adapter.scan(scan_limit)
    scan_elapsed = time.perf_counter() - began
    scanned = len(scanned_rows)
    keys = [item[0] for item in scanned_rows]
    if scanned != scan_limit or len(set(keys)) != scanned:
        raise ValueError(f"scan incomplete or duplicated: {scanned} rows, expected {scan_limit}")
    if any(not isinstance(key, int) or not 0 <= key < stage for key in keys):
        raise ValueError("scan returned keys outside the loaded range")
    entry["scan_rows"] = scanned
    entry["scan_time_s"] = scan_elapsed
    entry["scan_rows_per_s"] = (scanned / scan_elapsed) if scan_elapsed else 0.0
    return entry


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--engine", required=True, choices=sorted(ADAPTERS))
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--keyspace", default="bench")
    p.add_argument("--database", default="postgres")
    p.add_argument(
        "--bolt-database",
        default=None,
        help="Bolt session database/keyspace (ferrosa keyspace; unset for Neo4j)",
    )
    p.add_argument("--table", default="rows")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--stages", required=True, help="cumulative row counts, ascending")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--read-samples", type=int, default=100)
    p.add_argument("--seed", type=int, default=1234)
    p.add_argument("--read-only", action="store_true", help="reuse rows; never run DDL or load")
    p.add_argument("--image-digest", default="unknown")
    p.add_argument("--revision", default="unknown")
    p.add_argument(
        "--compression",
        action="store_true",
        help="negotiate CQL compression (default off, so both engines match)",
    )
    p.add_argument("--out", required=True)
    args = p.parse_args()

    stages = [int(s) for s in args.stages.split(",") if s.strip()]
    if not stages or stages != sorted(set(stages)) or min(stages) <= 0:
        print("--stages must contain positive, strictly ascending counts", file=sys.stderr)
        return 2
    if min(args.concurrency, args.repeats, args.read_samples) <= 0:
        print("--concurrency, --repeats and --read-samples must be positive", file=sys.stderr)
        return 2

    payloads = make_payloads(512)
    adapter = ADAPTERS[args.engine](args)
    try:
        if not args.read_only:
            adapter.ensure_schema()
        results = []
        current = 0
        for stage in stages:
            already = adapter.count()
            if args.read_only and already != stage:
                raise ValueError(f"read-only census mismatch: {already} != {stage}")
            current = max(current, already)
            insert_rate = None
            if current < stage:
                print(f"[{args.engine}] loading to {stage:,} ({stage - current:,} new, "
                      f"concurrency={args.concurrency})", file=sys.stderr, flush=True)
                insert_rate = load(adapter, current, stage, args.concurrency, payloads)
                print(f"[{args.engine}]   {insert_rate:,.0f} rows/s", file=sys.stderr, flush=True)
            actual = adapter.count()
            print(f"[{args.engine}] measuring at {actual:,} rows", file=sys.stderr, flush=True)
            results.append(measure(adapter, args, stage, actual, insert_rate))

        payload = {
            "contract": "ferrosa-bench.multimetric.validated.v2",
            "workload": "insert + count(*) + point-read (one load)",
            "engine": args.engine,
            "target": f"{args.host}:{args.port}",
            "concurrency": args.concurrency,
            "seed": args.seed,
            "read_only": args.read_only,
            "image_digest": args.image_digest,
            "revision": args.revision,
            "index_note": getattr(adapter, "index_note", "not observed in read-only probe"),
            "stages": results,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print(f"wrote {args.out}", file=sys.stderr)
        return 0
    finally:
        adapter.close()


if __name__ == "__main__":
    raise SystemExit(main())
