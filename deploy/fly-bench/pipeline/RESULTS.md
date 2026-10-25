# Measured pipeline changes

These are descriptive means of two unprofiled repetitions per arm on one Fly
LAX performance VM with 2 CPUs and 4 GiB RAM, using the same attached data
volume, native x86-64 builds and a separate client. No significance or general
database performance claim follows from two repetitions. All accepted legs
validated counts, every scanned key and row values.

PostgreSQL: 5,000 rows, 96-byte payloads, 8 workers, 400 seeded point reads.
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

In that Sync comparison, reads measured 34.7 ops/s after Sync versus PostgreSQL's 9,497.8. The exact-key
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

## Exact-key reads and decoder backing (2026-10-25)

The combined candidate `e8772cf29ec756d63278cf25d6db4836cdf1ee88`
adds safe scalar primary-key I/O and the decoder backing fix to Sync source
`ffa19340355e3c32e4671e949042485e7a2dc425`. C12/D12/D13/C13 uses the same
primary workload and client implementation, with only CLI label validation
extended to accept D. Every 5,000-row keyset/value/count check passed.

| Operation | Before | After | PostgreSQL 18.6 | After / PostgreSQL |
| --- | ---: | ---: | ---: | ---: |
| INSERT rows/s | 21,260.0 | 21,235.9 | 25,279.0 | 84.0% |
| Point reads/s | 33.3 | 2,981.3 | 9,497.8 | 31.4% |

These are means of two unprofiled repetitions, with raw run values/ranges in
[point-read-results.json](point-read-results.json). Reads improved 89.5 times;
a 3.19× gap to native PostgreSQL remains. INSERT throughput is effectively
unchanged within these run ranges. The key-read tests require the exact storage
operation and preserve late-delete snapshots, staged inserts, cluster failures
and floating signed-zero SQL equality. Allocation testing separately measured
6.25→4.25 allocations/decoded row under the unchanged 5/row budget.

The candidate is a bundle, so this is not isolated throughput attribution to
the decoder change. The same storage-placement and two-repetition limits above
apply. Separate read profiles are collected outside these timing legs.

Separate read CPU profiles used a freshly seeded fixture in the same process
and validated it after 30 seconds of reads. The candidate has 447 user CPU
samples: allocator frames appear on 49 stacks (11.0% inclusive); libc leaves
remain unattributed. No dominant CPU-spin hotspot was identified. Blocked
mutex/request time is not measured by a CPU flame graph, so the remaining
3.19× read gap still needs request/handoff/off-CPU tracing. Raw profiles and
resource counters are retained in the private evidence.

## Payload-label correction (2026-10-25)

Earlier text and result extracts incorrectly called these 512-byte payloads.
`make_payloads(512)` creates 512 variants; `PAYLOAD_LEN = 96` sets each payload
to 96 ASCII bytes. The timed workload and Ferrosa/PostgreSQL comparison bytes
are unchanged. The extracted metadata is corrected, and the client now records
actual UTF-8 byte length and variant count in every result. A contract test
checks the JSON emitted by the real client entry point. No 512-byte workload
performance claim follows from the measurements above.
