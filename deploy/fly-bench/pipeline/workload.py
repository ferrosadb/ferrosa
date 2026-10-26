"""Run on the Fly runner. Credentials remain in memory and are never arguments.

Correctness: exact counts and all written rows validated outside timing windows.
Timing: client wall time; profiling legs are separate from primary comparisons.
"""
import argparse
import faulthandler
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import random
import statistics
import time
import threading
from types import SimpleNamespace

import validated_multimetric_bench as instrument


class Workload:
    def __init__(self, protocol, scope, runtime_config, target='ferrosa'):
        runtime = json.loads(runtime_config.read_text())[target]
        args = SimpleNamespace(host=runtime['host'], port=runtime[protocol + '_port'],
            database='bench' if target == 'postgres' else 'postgres',
            user=runtime['user'], password=runtime['password'],
            table='pipeline_' + scope.lower(), bolt_database=None if target == 'neo4j' else 'bench')
        self.protocol = protocol
        self.scope = 'BenchRow' if protocol == 'bolt' else scope
        self.adapter = (instrument.PgAdapter if protocol == 'pg' else instrument.BoltAdapter)(args)
        self.payloads = instrument.make_payloads(512)
        self.index_note = 'not established; query-plan attempt recorded separately'

    def setup(self):
        if self.protocol == 'pg':
            self.adapter.ensure_schema()
        else:
            raise ValueError('Bolt uses the validated seeded fixture')

    def write_range(self, start, stop, update=False):
        if self.protocol == 'pg':
            if not update:
                return self.adapter.load_range(start, stop, self.payloads)
            with self.adapter._conn().cursor() as cur:
                cur.executemany(f'UPDATE {self.adapter.args.table} SET payload=%s WHERE pk=%s',
                    [(self.payloads[pk % 512], pk) for pk in range(start, stop)])
        else:
            session = self.adapter._session()
            if not update:
                raise ValueError('Bolt write leg requires point updates')
            for pk in range(start, stop):
                session.run(f'MATCH (n:{self.scope} {{pk: $pk}}) SET n.payload = $payload',
                    pk=pk, payload=self.payloads[pk % 512]).consume()

    def load_range(self, start, stop, payloads):
        return self.write_range(start, stop)

    def count(self):
        if self.protocol == 'pg':
            return self.adapter.count()
        with self.adapter.driver.session(database=self.adapter.database) as session:
            result = session.run(f'MATCH (n:{self.scope}) RETURN count(n) AS c').single()
            return result['c'] if result else 0

    def read(self, pk):
        if self.protocol == 'pg':
            return self.adapter.point_read(pk)
        result = self.adapter._session().run(
            f'MATCH (n:{self.scope} {{pk: $pk}}) RETURN n.pk AS pk, n.bucket AS bucket, n.payload AS payload', pk=pk)
        return result.single()

    def validate_row(self, row, pk):
        if row is None or tuple(row) != (pk, pk % 64, self.payloads[pk % 512]):
            raise ValueError(f'missing or incorrect row {pk}')

    def scan_and_validate(self, rows):
        start = time.perf_counter()
        if self.protocol == 'pg':
            with self.adapter._conn().cursor() as cur:
                cur.execute(f'SELECT pk,bucket,payload FROM {self.adapter.args.table}')
                received = cur.fetchall()
        else:
            received = list(self.adapter._session().run(
                f'MATCH (n:{self.scope}) RETURN n.pk AS pk,n.bucket AS bucket,n.payload AS payload'))
        elapsed = time.perf_counter() - start
        if len(received) != rows or {r[0] for r in received} != set(range(rows)):
            raise ValueError('scan census/key-set mismatch')
        for row in received:
            self.validate_row(row, row[0])
        return {'rows': rows, 'seconds': elapsed, 'rows_per_second': rows / elapsed}

    def plan(self):
        try:
            if self.protocol == 'pg':
                with self.adapter._conn().cursor() as cur:
                    cur.execute(f'EXPLAIN SELECT pk,bucket,payload FROM {self.adapter.args.table} WHERE pk=42')
                    return [list(row) for row in cur.fetchall()]
            result = self.adapter._session().run(f'EXPLAIN MATCH (n:{self.scope} {{pk:42}}) RETURN n.pk')
            summary = result.consume()
            return repr(summary.plan)
        except Exception as error:
            return {'unsupported_or_failed': type(error).__name__, 'message': str(error)[:1000]}


def warm_reads(workload, keys, concurrency):
    """Warm every measured worker connection, then open one timed gate."""
    ready = threading.Barrier(concurrency + 1)
    go = threading.Event()
    def worker(index):
        for pk in keys[index:index + 5]:
            workload.validate_row(workload.read(pk), pk)
        ready.wait(timeout=30)
        if not go.wait(timeout=30):
            raise TimeoutError('read timing gate never opened')
        samples = []
        for pk in keys[index::concurrency]:
            began = time.perf_counter()
            row = workload.read(pk)
            samples.append(time.perf_counter() - began)
            workload.validate_row(row, pk)
        return samples
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, i) for i in range(concurrency)]
        ready.wait(timeout=30)
        start = time.perf_counter()
        go.set()
        latencies = [sample for future in futures for sample in future.result()]
        elapsed = time.perf_counter() - start
    return latencies, elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-config', type=Path, required=True,
        help='Private connection JSON; never copy this file into results')
    parser.add_argument('--arm', choices=['A', 'B', 'C', 'D', 'E'], required=True)
    parser.add_argument('--round', type=int, required=True)
    parser.add_argument('--protocol', choices=['pg', 'bolt'], required=True)
    parser.add_argument('--target', choices=['ferrosa', 'postgres', 'neo4j'], default='ferrosa')
    parser.add_argument('--revision', required=True)
    parser.add_argument('--image', required=True)
    parser.add_argument('--rows', type=int, default=5000)
    parser.add_argument('--concurrency', type=int, default=8)
    parser.add_argument('--reads', type=int, default=400)
    parser.add_argument('--writes', type=int, default=500)
    parser.add_argument('--deadline-seconds', type=int, default=240)
    parser.add_argument('--profile-seconds', type=int, default=0)
    parser.add_argument('--profile-operation', choices=['read', 'write'], default='read')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.target == 'postgres' and args.protocol != 'pg' or args.target == 'neo4j' and args.protocol != 'bolt':
        parser.error('target and wire protocol do not match')
    if min(args.rows, args.reads, args.concurrency, args.writes, args.deadline_seconds) <= 0 or min(args.reads, args.rows) < args.concurrency:
        parser.error('positive sizes and at least one read per worker are required')
    faulthandler.dump_traceback_later(args.deadline_seconds, exit=True)
    # Each timing leg owns fresh data; its later profile leg reuses that data.
    scope = f'Pipeline{args.arm}{args.round}{args.protocol.title()}'
    workload = Workload(args.protocol, scope, args.runtime_config, args.target)
    result = {k: v for k, v in vars(args).items() if k not in ('out', 'runtime_config')}
    result.update(scope=workload.scope, table=workload.adapter.args.table,
        started_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    latencies = []
    try:
        sizes = {len(payload.encode('utf-8')) for payload in workload.payloads}
        if len(sizes) != 1:
            raise ValueError('payload sizes must be uniform for this workload')
        result.update(payload_bytes=sizes.pop(), payload_variants=len(workload.payloads))
        if args.profile_seconds:
            if workload.count() != args.rows:
                raise ValueError('profile fixture census mismatch')
            deadline = time.perf_counter() + args.profile_seconds
            def worker(index):
                rng = random.Random(1234 + index)
                samples = []
                while time.perf_counter() < deadline:
                    pk = rng.randrange(index, args.rows, args.concurrency)
                    began = time.perf_counter()
                    if args.profile_operation == 'read':
                        row = workload.read(pk)
                    else:
                        workload.write_range(pk, pk + 1, update=True)
                    samples.append(time.perf_counter() - began)
                    if args.profile_operation == 'read':
                        workload.validate_row(row, pk)
                return samples
            start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                for samples in pool.map(worker, range(args.concurrency)):
                    latencies.extend(samples)
            result.update(profile_elapsed_s=time.perf_counter() - start,
                completed_operations=len(latencies))
            result['post_profile_scan'] = workload.scan_and_validate(args.rows)
        else:
            if args.protocol == 'pg':
                workload.setup()
                if workload.count() != 0:
                    raise ValueError('fresh write fixture is not empty')
                start = time.perf_counter()
                rate = instrument.load(workload, 0, args.rows, args.concurrency, workload.payloads)
                result.update(write_operation='insert', insert_rows_per_s=rate,
                    insert_elapsed_s=time.perf_counter() - start)
            else:
                if workload.count() != args.rows:
                    raise ValueError('seeded Bolt census mismatch')
                writes = min(args.rows, args.writes)
                start = time.perf_counter()
                with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                    list(pool.map(lambda pk: workload.write_range(pk, pk + 1, update=True), range(writes)))
                elapsed = time.perf_counter() - start
                result.update(write_operation='point-update', update_operations=writes,
                    update_operations_per_s=writes / elapsed, update_elapsed_s=elapsed)
            if workload.count() != args.rows:
                raise ValueError('post-write census mismatch')
            result['scan'] = workload.scan_and_validate(args.rows)
            rng = random.Random(1234)
            keys = [rng.randrange(args.rows) for _ in range(args.reads)]
            latencies, elapsed = warm_reads(workload, keys, args.concurrency)
            result.update(read_elapsed_s=elapsed, read_operations_per_s=len(keys) / elapsed,
                query_plan=workload.plan())
        latencies.sort()
        result.update(latencies_s=latencies,
            read_or_operation_p50_ms=statistics.median(latencies) * 1000,
            read_or_operation_p99_ms=instrument.percentile(latencies, 99),
            rows_after=workload.count(), validated=True)
    finally:
        workload.adapter.close()
        result['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        args.out.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
