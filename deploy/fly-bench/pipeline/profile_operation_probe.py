"""Validated warm-connection protocol comparison; private credentials stay in JSON.

Counts, complete values and uniqueness are checked outside each timed interval.
Last revised: 2026-10-26
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import faulthandler
import json
from pathlib import Path
import random
import socket
import statistics
import struct
import threading
import time
from types import SimpleNamespace

import validated_multimetric_bench as bench


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--target', required=True)
    p.add_argument('--protocol', choices=['pg', 'cql', 'sparql'], required=True)
    p.add_argument('--scope', required=True)
    p.add_argument('--rows', type=int, required=True)
    p.add_argument('--reads', type=int, default=20000)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--write-concurrency', type=int, default=8)
    p.add_argument('--allow-unsupported-count', action='store_true')
    p.add_argument('--profile', action='store_true')
    p.add_argument('--concurrency', type=int, default=8)
    p.add_argument('--seconds', type=float, default=30)
    p.add_argument('--scan-limit', type=int)
    p.add_argument('--explicit-scan-timing', action='store_true')
    p.add_argument('--diagnostic-operation', choices=['count','scan'], required=True)
    p.add_argument('--ready-file', type=Path)
    p.add_argument('--go-file', type=Path)
    a = p.parse_args()
    if a.concurrency < 1 or a.seconds <= 0 or (a.scan_limit is not None and a.scan_limit < 1):
        p.error('concurrency, seconds and scan-limit must be positive')

    faulthandler.dump_traceback_later(500, exit=True)
    cfg = json.loads(a.config.read_text())[a.target]
    settings = dict(cfg)
    settings.update(keyspace=a.scope, table=a.scope, compression=False)
    adapter = bench.ADAPTERS[a.protocol](SimpleNamespace(**settings))
    out = dict(protocol=a.protocol, target=a.target, scope=a.scope, rows=a.rows,
               payload_bytes=96, payload_variants=512, validated=False,
               diagnostic_operation=a.diagnostic_operation, started_at=time.time(), profile=a.profile, write_concurrency=a.write_concurrency, read_concurrency=a.concurrency)
    payloads = bench.make_payloads(512)

    def save():
        a.out.write_text(json.dumps(out, indent=2) + '\n')

    def validate(row, pk):
        assert row is not None and tuple(row) == (pk, pk % 64, payloads[pk % 512]), (row, pk)

    def checked_count():
        try:
            return adapter.count()
        except RuntimeError as error:
            message=str(error)
            if a.allow_unsupported_count and a.protocol=='sparql' and 'SPARQL HTTP 400' in message and 'unsupported graph pattern: Group' in message:
                out['count_support']={'status':'unsupported','reason':'HTTP400: Group aggregation is not implemented','error':message}
                return None
            raise

    def census():
        counted=checked_count()
        if counted is not None:assert counted == a.rows
        rows = adapter.scan(a.rows+1)
        keys={r[0] for r in rows};expected=set(range(a.rows))
        out['last_census']={'expected_rows':a.rows,'returned_rows':len(rows),'unique_keys':len(keys),
            'columns':list(getattr(rows[0],'_fields',())) if rows else [],
            'first_rows':[list(row) for row in rows[:3]],
            'missing_first':sorted(expected-keys)[:12],
            'unexpected_first':list(keys-expected)[:12]}
        save()
        assert len(rows) == a.rows and keys == expected, out['last_census']
        for row in rows:
            validate(row, row[0])

    try:
        tcp = []
        for _ in range(32):
            with socket.create_connection((cfg['host'], cfg['port']), timeout=10) as sock:
                raw = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 256)
                tcp.append(struct.unpack_from('=I', raw, 68)[0])
        out['kernel_syn_rtt_us_p50'] = statistics.median(tcp)
        if not a.profile:
            adapter.ensure_schema()
            # Connection creation/preparation is reported as part of load, as in
            # the published adapter harness. Point reads use warm connections.
            began = time.perf_counter()
            bench.load(adapter, 0, a.rows, a.write_concurrency, payloads)
            elapsed = time.perf_counter() - began
            out['insert_rows_s'] = a.rows / elapsed
            out['insert_seconds'] = elapsed
        census()
        if a.protocol == 'pg':
            conn = adapter._conn()
            with conn.cursor() as cur:
                if a.target=='postgres':
                    cur.execute('SELECT version()');out['server_version']=cur.fetchone()[0]
                    out['server_settings']={}
                    for setting in ('fsync','synchronous_commit','shared_buffers'):
                        cur.execute('SHOW '+setting);out['server_settings'][setting]=cur.fetchone()[0]
                    cur.execute(f'EXPLAIN SELECT pk, bucket, payload FROM {a.scope} WHERE pk = 42')
                    out['point_plan'] = [list(row) for row in cur.fetchall()]
                else:
                    out['point_plan'] = ['EXPLAIN unsupported; primary-key schema and exact-key SELECT validated']
        if a.protocol == 'cql':
            out['protocol_version'] = adapter.protocol_version
            out['compression'] = False
        rng = random.Random(1234)
        keys = [rng.randrange(a.rows) for _ in range(a.reads)]
        barrier = threading.Barrier(a.concurrency+1)
        go = threading.Event()
        gate = {}

        scan_limit = min(a.rows, a.scan_limit or a.rows)

        def scan_delivery():
            requested = scan_limit + 1 if scan_limit == a.rows else scan_limit
            if not a.explicit_scan_timing:
                began = time.perf_counter()
                rows = adapter.scan(requested)
                return rows, time.perf_counter() - began, None, None, None
            columns = 'pk, bucket, payload'
            if a.protocol == 'cql':
                from cassandra.query import SimpleStatement
                from cassandra import ConsistencyLevel
                query = SimpleStatement(f'SELECT {columns} FROM {a.scope}.{a.scope} LIMIT {requested}', fetch_size=5000, consistency_level=ConsistencyLevel.ONE)
                began = time.perf_counter()
                result = adapter.session.execute(query, timeout=30)
                first = time.perf_counter()
                rows = list(result.current_rows)
                pages = 1
                while result.has_more_pages:
                    result.fetch_next_page()
                    rows.extend(result.current_rows)
                    pages += 1
                    if pages > (scan_limit + 4999)//5000 + 4:
                        raise RuntimeError('paging failed to make bounded progress')
                ended = time.perf_counter()
                return rows, ended-began, first-began, ended-first, pages
            if a.protocol != 'pg':
                raise RuntimeError('explicit scan timing only covers PostgreSQL/CQL')
            from contextlib import closing
            query = f'SELECT {columns} FROM {a.scope} LIMIT {requested}'
            began = time.perf_counter()
            with adapter._conn().cursor() as cursor:
                with closing(cursor.stream(query, size=1)) as stream:
                    first_row = next(stream, None)
                    first = time.perf_counter()
                    rows = [] if first_row is None else [first_row]
                    rows.extend(stream)
            ended = time.perf_counter()
            return rows, ended-began, first-began, ended-first, None

        def worker(index):
            for i in range(8):
                pk = keys[(index + i) % len(keys)]
                validate(adapter.point_read(pk), pk)
            if a.diagnostic_operation == 'scan' and a.explicit_scan_timing:
                warm, _, _, _, _ = scan_delivery()
                assert len(warm) == scan_limit
                for row in warm:
                    validate(row,row[0])
            barrier.wait(timeout=60)
            assert go.wait(timeout=90)
            times = []
            first_times = []
            drain_times = []
            page_counts = []
            i = index
            while time.perf_counter() < gate['deadline'] if a.profile else i < len(keys):
                pk = keys[i % len(keys)]
                began = time.perf_counter()
                if a.diagnostic_operation == 'count':
                    counted = adapter.count()
                    duration = time.perf_counter() - began
                    assert counted == a.rows, counted
                else:
                    rows, duration, first_s, drain_s, pages = scan_delivery()
                    assert len(rows) == scan_limit and len({row[0] for row in rows}) == scan_limit
                    if scan_limit == a.rows:
                        assert {row[0] for row in rows} == set(range(a.rows))
                    for row in rows:
                        validate(row, row[0])
                    if first_s is not None:
                        first_times.append(first_s)
                        drain_times.append(drain_s)
                    if pages is not None:
                        page_counts.append(pages)
                times.append(duration)
                i += a.concurrency
            return times, time.perf_counter(), first_times, drain_times, page_counts

        with ThreadPoolExecutor(max_workers=a.concurrency) as pool:
            jobs = [pool.submit(worker, i) for i in range(a.concurrency)]
            try:
                barrier.wait(timeout=60)
            except threading.BrokenBarrierError:
                go.set()
                for job in jobs:
                    job.result()
                raise
            if a.ready_file:
                a.ready_file.touch()
                deadline = time.monotonic() + 90
                while not a.go_file.exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError('profile gate')
                    time.sleep(.02)
            out['measurement_start_wall_time'] = time.time()
            began = time.perf_counter()
            gate['deadline'] = began + a.seconds
            go.set()
            results = [j.result(timeout=300) for j in jobs]
        samples = sorted(t for result in results for t in result[0])
        elapsed = max(result[1] for result in results) - began
        out['diagnostic_' + a.diagnostic_operation] = dict(operations=len(samples), seconds=elapsed,
                            ops_s=len(samples) / elapsed,
                            p50_ms=statistics.median(samples) * 1000,
                            p99_ms=samples[min(len(samples)-1, int(.99*len(samples)))] * 1000)
        out['measurement_end_wall_time'] = time.time()
        stats = out['diagnostic_' + a.diagnostic_operation]
        stats['mean_client_queries_inflight'] = sum(samples) / elapsed
        stats['rows_per_query'] = scan_limit if a.diagnostic_operation == 'scan' else None
        if a.diagnostic_operation == 'scan':
            stats['rows_s'] = scan_limit * len(samples) / elapsed
            stats['scan_mode'] = ('pg_single_row_stream' if a.protocol == 'pg' else 'cql_explicit_5000_pages') if a.explicit_scan_timing else 'adapter_scan'
            first_samples = [t for result in results for t in result[2]]
            drain_samples = [t for result in results for t in result[3]]
            page_counts = [n for result in results for n in result[4]]
            if first_samples:
                stats['first_available_kind'] = 'first_row' if a.protocol == 'pg' else 'first_page'
                stats['first_available_p50_ms'] = statistics.median(first_samples)*1000
                stats['drain_after_first_p50_ms'] = statistics.median(drain_samples)*1000
            if page_counts:
                stats['pages_per_query_min'] = min(page_counts)
                stats['pages_per_query_max'] = max(page_counts)
        # Full value validation is outside per-query latency, but included in
        # wall throughput and mean client in-flight occupancy. This is a driver
        # workload, not a maximum engine-only bandwidth claim.
        if not a.profile:
            counts = []
            if out.get('count_support',{}).get('status')!='unsupported':
                for _ in range(5):
                    began = time.perf_counter()
                    counted = checked_count()
                    if counted is None:break
                    counts.append(time.perf_counter() - began)
                    assert counted == a.rows
            out['count_p50_ms'] = statistics.median(counts) * 1000 if len(counts)==5 else None
            began = time.perf_counter()
            scanned = adapter.scan(min(10000, a.rows))
            elapsed = time.perf_counter() - began
            assert len(scanned) == min(10000, a.rows)
            assert len({r[0] for r in scanned}) == len(scanned)
            for row in scanned:
                validate(row, row[0])
            out['scan_rows_s'] = len(scanned) / elapsed
        census()
        out['finished_at'] = time.time()
        out['validated'] = True
        save()
    finally:
        adapter.close()


if __name__ == '__main__':
    main()
