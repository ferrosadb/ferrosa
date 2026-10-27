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
    p.add_argument('--diagnostic-operation', choices=['count','scan'], required=True)
    p.add_argument('--ready-file', type=Path)
    p.add_argument('--go-file', type=Path)
    a = p.parse_args()
    if not a.profile:
        p.error("--profile is required for count/scan diagnostic loops")
    faulthandler.dump_traceback_later(500, exit=True)
    cfg = json.loads(a.config.read_text())[a.target]
    settings = dict(cfg)
    settings.update(keyspace=a.scope, table=a.scope, compression=False)
    adapter = bench.ADAPTERS[a.protocol](SimpleNamespace(**settings))
    out = dict(protocol=a.protocol, target=a.target, scope=a.scope, rows=a.rows,
               payload_bytes=96, payload_variants=512, validated=False,
               diagnostic_operation=a.diagnostic_operation, started_at=time.time(), profile=a.profile, write_concurrency=a.write_concurrency, read_concurrency=8)
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
        barrier = threading.Barrier(9)
        go = threading.Event()
        gate = {}

        def worker(index):
            for i in range(8):
                pk = keys[(index + i) % len(keys)]
                validate(adapter.point_read(pk), pk)
            barrier.wait(timeout=60)
            assert go.wait(timeout=90)
            times = []
            i = index
            while time.perf_counter() < gate['deadline'] if a.profile else i < len(keys):
                pk = keys[i % len(keys)]
                began = time.perf_counter()
                if a.diagnostic_operation == 'count':
                    counted = adapter.count()
                    duration = time.perf_counter() - began
                    assert counted == a.rows, counted
                else:
                    rows = adapter.scan(a.rows + 1)
                    duration = time.perf_counter() - began
                    assert len(rows) == a.rows and {row[0] for row in rows} == set(range(a.rows))
                    for row in rows:
                        validate(row, row[0])
                times.append(duration)
                i += 8
            return times, time.perf_counter()

        with ThreadPoolExecutor(max_workers=8) as pool:
            jobs = [pool.submit(worker, i) for i in range(8)]
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
            began = time.perf_counter()
            gate['deadline'] = began + 30
            go.set()
            results = [j.result(timeout=300) for j in jobs]
        samples = sorted(t for ts, _ in results for t in ts)
        elapsed = max(end for _, end in results) - began
        out['diagnostic_' + a.diagnostic_operation] = dict(operations=len(samples), seconds=elapsed,
                            ops_s=len(samples) / elapsed,
                            p50_ms=statistics.median(samples) * 1000,
                            p99_ms=samples[min(len(samples)-1, int(.99*len(samples)))] * 1000)
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
