# Measured pipeline changes

These are descriptive means of two unprofiled repetitions per arm on one Fly
LAX performance VM with 2 CPUs and 4 GiB RAM, using the same attached data
volume, native x86-64 builds and a separate client. No significance or general
database performance claim follows from two repetitions. All accepted legs
validated counts, every scanned key and row values.

PostgreSQL: 5,000 rows, 512-byte payloads, 8 workers, 400 seeded point reads.
Bolt: 20,000 existing nodes, 500 point updates, 8 workers, 400 seeded reads.
These are protocol-specific workloads; their rates are not comparable to each
other. Older native PostgreSQL/Neo4j measurements used different workloads and
cannot establish a current performance ratio.

| Change / operation | Before | After | Change |
| --- | ---: | ---: | ---: |
| Shared payload bundle: PG insert, rows/s | 1202.86 | 1108.30 | -7.86% |
| Shared payload bundle: PG read, ops/s | 31.27 | 30.33 | -3.01% |
| Shared payload bundle: Bolt update, ops/s | 8.96 | 9.14 | +1.97% |
| Shared payload bundle: Bolt read, ops/s | 9.27 | 9.52 | +2.65% |
| Singleton-history guard: PG insert, rows/s | 1151.95 | 2104.87 | +82.72% |
| Singleton-history guard: PG read, ops/s | 30.82 | 32.64 | +5.92% |

The shared payload comparison is a bundle, not single-change attribution:
`df1c2214011c7f5c1758c1bbc749de38e4217026` →
`acf6ec1f2c8e71637af47f75afa7a791fe7aba3f`, order A1/B1/B2/A2.
The history guard isolates skipping reconstruction for singleton histories:
`acf6ec1f2c8e71637af47f75afa7a791fe7aba3f` →
`d16332f238ff21433303d3b7972f7b84ba8173be`, order B3/C3/C4/B4.
All 157 installed server package versions matched within those comparisons.
Raw run values and ranges are retained in [shared-payload-results.json](shared-payload-results.json)
and [history-guard-results.json](history-guard-results.json).

Separately sampled write stacks implicated history pruning: approximately 83%
of user CPU samples before the guard versus 28% afterwards. Profiling overhead
prevents using those legs for throughput. The guard still leaves a global MVCC
critical section and traversal of unrelated history. A persistent-root MVCC
replacement needs its own controlled comparison; the table above does not
measure that replacement.

Local System-allocator probes also showed a tradeoff: shared cell payloads
reduced requested allocation bytes by 20.4% with four observers, while increasing
them by 17.6% with no observers for the measured 128-cell/64-byte workload.
Requested allocation bytes are neither peak RSS nor a Fly heap profile.

## Persistent MVCC and PostgreSQL Sync (2026-10-24)

New native PostgreSQL controls use the same client and workload as these
Ferrosa runs. Each value below is the mean of two unprofiled repetitions;
full run values and ranges are in [mvcc-sync-results.json](mvcc-sync-results.json).

| INSERT comparison | Before rows/s | After rows/s | PostgreSQL rows/s | After / PostgreSQL |
| --- | ---: | ---: | ---: | ---: |
| Persistent MVCC, A5/B5/B6/A6 | 2,098 | 2,684 | 25,279 | 10.6% |
| Commit pipelined statements at Sync, B7/C7/C8/B8 | 2,737 | 21,275 | 25,279 | 84.2% |

The MVCC change improved INSERT throughput by 27.9%. The Sync fix improved it
7.77 times: the previous path committed each Execute independently, whereas
PostgreSQL groups the implicit transaction through Sync. Regression tests also
verify all-or-nothing rollback of that group. PostgreSQL 18.6 had fsync,
synchronous_commit and full_page_writes enabled. Both servers used the same
2-performance-CPU/4-GiB service class and region; PostgreSQL used its root
filesystem and Ferrosa an attached volume, so storage was not identical.

Reads remain 34.7 ops/s after Sync versus PostgreSQL's 9,497.8. The exact-key
read fix and decoder allocation fix are later source changes and are **not**
measured by this table.

Separate 20,000-row INSERT-only software CPU captures validate every row after
the collector stops. The old MVCC history-pruning functions accounted for
111/201 user CPU samples (55.2%); neither function appears in the persistent
candidate captures. Sample counts are only 116 before Sync and 52 after Sync,
so those captures do not establish a precise remaining CPU ranking. Flame
graphs, process/run-queue samples, disk and network counters are retained in
the private evidence. These runs do not measure individual mutex wait time
or Fly heap allocation counts. Profiled throughput is excluded above.
