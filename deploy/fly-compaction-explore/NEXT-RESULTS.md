# Sustained compaction and read-ahead experiments — 2026-10-23

This continues [the pilot](RESULTS.md) at PR #601 revision
`5f88f8ec28e6a674c9098c9161062a7a0ec9facc`. These are diagnostic measurements,
not a Cassandra comparison or a validated capacity limit. Raw evidence:
`/home/bkearns/src/ferrosa-suite/compaction-next-evidence-20261023`.

**Priority changed after the runs:** investigate manifest retirement before
adding a tuning controller. The durable manifests contain generations consumed
by successful compactions. The wide workload also reproduces severe foreground
latency with commit-log fsync backlog alarms. No server behavior was changed in
this campaign.

## Controlled Fly workload

The isolated `ferrosa-cx601-1023` app uses three LAX performance-2 CPU / 4 GiB
nodes with 10 GB volumes and a separate generator of the same CPU/RAM size.
It runs the same pinned #601 image as the pilot. A new dedicated Tigris bucket
passed HEAD, PUT, exact GET readback, and DELETE checks; actual uploaded objects
were verified for all three nodes. TLS is verified. No baseline apps are changed.

Each case has 16,384 deterministic keys and 16 KiB values (256 MiB logical
working set), RF=3, QUORUM, STCS, no driver retries, and output verification on.
The diagnostic flush threshold is 4 MiB, versus 64 MiB in the earlier pilot.
Preload is 100 writes/s; mixed load is 600 operations/s, 50% reads, for 300 s;
drain is 100 reads/s for 90 s; final verification reads every key at 600/s.
Thus each completed case checks 221,768 operations, including 180,000 mixed
operations. Repeated writes use the same payload, so this is not an LWW/TTL test.

| Case | Achieved mixed ops/s | Mixed p99 / p99.9 | Drain p99 | Completed compactions/node | Errors/mismatches |
|---|---:|---:|---:|---:|---:|
| 16 tables, 1 worker | 599.9 | 38.06 / 61.27 ms | 9.98 ms | 71–72 | 0 / 0 |
| 16 tables, 2 workers/permits | 599.9 | 39.00 / 60.97 ms | 10.21 ms | 71–74 | 0 / 0 |
| 4 wide partitions, 1 worker | 599.9 | 1968.27 / 3562.01 ms | 17.62 ms | 39–45 | 0 / 0 |

All three cases completed: **665,304 operations**, zero request errors, zero
payload mismatches, and zero collector errors. These totals exclude the failed
setup attempt. The wide case uses one table with four partitions of 4,096 rows
each, versus 16 tables of point partitions in the other cases.

The wide result is not a clean bill of health despite its achieved average rate:
74 mixed requests had service time above 500 ms, 32 exceeded two seconds, and
the slowest write took 3.96 s. Service p99 was 124.45 ms and dispatch-lag p99 was
1915.00 ms; the primary scheduled-response measurement retains the queueing
consequence of those stalls. Six supervisor events reported commit-log fsync
backlog beyond the two-second readiness deadline; 156 slow-sync logs included
a maximum individual sync total of 1820.44 ms. These are backlog alarms, not
proof that every individual fsync exceeded two seconds. Compaction outputs
exceeded 200 MiB. Sampled RSS stayed at 300–323 MiB/node. Final verification
p99 returned to 13.65 ms; all nodes had zero failed/cancelled compactions.
This supports testing shared local-I/O admission/pacing, with a controlled A/B
needed to establish causality and choose a policy. It does not justify raising
the write-refusal deadline or changing strategy automatically.

The one-worker case has zero collector errors and zero failed/cancelled
compactions. All 16 workload tables compacted on every node. Eleven tables per
node subsequently consumed published compaction outputs, establishing repeated
generations on that subset. Log output IDs change during publication, so exact
ancestry is deliberately not reconstructed by guessing. The offline analyzer
tracks unknown ancestry and validates local promotions separately from S3
completion. At case end 70–81 SSTables remained per node; zero queued work does
not mean every table has one file or that no unselected debt exists.

One-worker sampled RSS peaked at 438–494 MiB/node. Aggregate phase timers per
node were 9.1–10.4 s for output readback, 13.1–14.3 s for local SSTable writes,
and 83.0–88.3 s awaiting S3 uploads. These timers cover different pipeline
boundaries and can overlap; do not add them or interpret `total` as including
all asynchronous finalization.

Two workers gave no observed mixed-load p99 benefit at this fixed offered rate;
this is one exploratory run per setting, not a capacity comparison or proof of
a regression. Every table compacted on every node, with published inputs reused
on 11 tables/node again. Sampled active compactions reached two. Process metrics
showed 430–466 MiB sampled RSS maxima and 451–483 MiB lifetime RSS high-water
marks, versus 441–485 and 454–497 MiB respectively with one worker. Two-worker
local-write timers totaled 21.7–23.2 s/node, verification 17.2–19.2 s/node, and
S3-await 82.5–84.6 s/node. Concurrent wall-time totals are not CPU time.

The two-worker final verification had a separate latency anomaly: 32 reads
took over 500 ms (the slowest about 984 ms), producing 687.96 ms scheduled p99,
35.63 ms service p99, and 635.58 ms dispatch-lag p99. Around
`2026-10-23T23:22:13Z`, all sampled compaction/upload queues and running counts
were zero. The surrounding four-second server execute histogram intervals had
800/801/801 requests, all at most 50 ms, averaging 2.25–2.67 ms. Retain the
client tail, but investigate generator pauses, networking, or protocol queues
outside that execute span before attributing it to compaction. All payloads
were correct. Generator CPU/GC and finer protocol timing are missing evidence.

The standalone `/proc` sampler was lost during the two-worker machine reset;
the complete Prometheus captures supplied the RSS/HWM/CPU figures above. No
two-worker physical-I/O comparison is claimed. The wide-case sampler was
uploaded again after reset. This instrumentation gap is recorded in the raw
case annotations.

An initial two-worker setup attempt failed before loading data during a rolling
reset of fresh Raft/data paths. It is excluded. Subsequent resets stop all nodes
first, use fresh cluster identities and paths, and require both `/readyz` and
a healthy three-node ring before starting the workload.

## Durable manifest discrepancy — highest priority

After each workload was complete, compare the saved durable S3 manifest with
runtime SSTable counts and the inputs of fully observed, locally promoted
compaction tasks. Capture-window filtering excludes a task started during a
later reset. The numbers match exactly:

| Case | Node | Runtime live SSTables | Durable manifest entries | Manifest references to consumed inputs |
|---|---:|---:|---:|---:|
| 16 tables, 1 worker | 1 | 70 | 298 | 228 |
| 16 tables, 1 worker | 2 | 81 | 308 | 227 |
| 16 tables, 1 worker | 3 | 78 | 273 | 195 |
| 16 tables, 2 workers | 1 | 76 | 308 | 232 |
| 16 tables, 2 workers | 2 | 84 | 296 | 212 |
| 16 tables, 2 workers | 3 | 77 | 279 | 202 |
| Wide, 1 worker | 1 | 7 | 139 | 132 |
| Wide, 1 worker | 2 | 11 | 202 | 191 |
| Wide, 1 worker | 3 | 8 | 144 | 136 |

All referenced Data.db objects existed at audit time. Input object deletion has
a one-hour grace period; retained objects alone are expected, but the manifest
publication code explicitly removes input references before scheduling deletion.
This is **not evidence of data loss**, and the responsible interleaving is not
yet isolated. A stale snapshot merge or pending-upload replay is a candidate
cause. The existing same-payload workload cannot test tombstone resurrection.

Next: reproduce concurrent flush/sync publication versus compaction retirement;
assert retired IDs stay out of durable manifests; then test post-grace deletion,
remote-only restart, and TTL/tombstone safety. Track as **t_df374fa8**. Preserve
the saved manifests, `manifest-compacted-input-overlap.json`, and per-case logs.
No input generation was observed twice in the measured compaction task inputs,
so this campaign did not demonstrate a same-input retry storm during steady
operation. Recovery behavior remains untested.

## Fresh-process probes

The unchanged Rust `compaction_allocation_probe` was built in release mode from
the pinned revision. Each subprocess checks values, ordering, tombstones, BTI
partition lookup, and sampled BTI row lookup. Reported memory is the counting
System allocator's peak live delta during compaction, **not RSS**. Input creation
and independent correctness validation are outside the compaction timer. These
local-host timings are separate from the Fly cluster measurements. Fixtures have
overlapping keys and two regular cells/row (one cell tombstoned every seventh
winning row); probe payloads are repetitive, unlike the cluster SHAKE payloads.

### Fan-in: 262,144 total input rows, 64-byte cells, 1 MiB read-ahead

Five fresh processes per cell, randomized case order per repetition:

| Inputs | Median time | Peak live delta |
|---:|---:|---:|
| 4 | 128.1 ms | 20.25 MiB |
| 16 | 126.2 ms | 56.68 MiB |
| 32 | 131.9 ms | 105.24 MiB |
| 64 | 163.9 ms | 145.05 MiB |

All 40 samples across fixed-total and fixed-per-input families passed. Fixed
total input rows does not hold output rows constant with overlapping keys, so
this does not isolate the asymptotic head-selection cost. Memory increases
strongly with input count despite bounded streaming. A disjoint-key fixture
remains necessary before deciding whether to replace linear selection with a heap.

### Randomized read-ahead tradeoff

After two exploratory window sweeps, a separate 60-process run interleaved all
12 combinations across five repetitions. Verification remained enabled. The
64-byte family fixes 262,144 total input rows; the 16 KiB family fixes 16,384.

| Cell bytes | Inputs | 1 MiB window: time / peak | 256 KiB window: time / peak | 64 KiB window: time / peak |
|---:|---:|---:|---:|---:|
| 64 | 4 | 143.6 ms / 20.2 MiB | 143.8 ms / 14.3 MiB | 168.4 ms / 12.8 MiB |
| 64 | 64 | 201.0 ms / 145.1 MiB | 171.9 ms / 106.4 MiB | 187.2 ms / 82.4 MiB |
| 16,384 | 4 | 238.8 ms / 20.4 MiB | 338.7 ms / 14.4 MiB | 841.5 ms / 12.9 MiB |
| 16,384 | 64 | 293.0 ms / 202.5 MiB | 220.0 ms / 106.7 MiB | 260.6 ms / 82.7 MiB |

A blanket small-window default is unsafe for performance: the 4-input large-cell
case slowed 3.52× at 64 KiB. At 64 inputs, 256 KiB reduced tracked memory by
47% and median time by 25% for the large-cell fixture. This supports a candidate
per-task memory budget accounting for fan-in and active workers, while keeping
large windows when headroom allows. It does not validate an adaptive controller.

### Optional output readback cost

Forty fresh processes: five randomized repetitions of 64-byte/16 KiB cells,
uncompressed/LZ4 inputs, and optional output readback on/off. Every sample still
performs independent correctness validation after the timer; mandatory component
digest verification remains enabled in the executor in both modes.

| Cell bytes | Input compression | Readback off | Readback on | Difference |
|---:|---|---:|---:|---:|
| 64 | None | 16.61 ms | 17.29 ms | 0.68 ms |
| 64 | LZ4 | 14.60 ms | 15.79 ms | 1.19 ms |
| 16,384 | None | 186.74 ms | 215.37 ms | 28.63 ms |
| 16,384 | LZ4 | 236.79 ms | 261.03 ms | 24.24 ms |

Eliminating the entire optional phase would remove roughly 9–13% of verified
runtime for these large-cell fixtures; a metadata-only verifier can save less.
Keep verification enabled and test a candidate that avoids value materialization
while preserving digest, structural/corruption checks, row counts, and cancellation.

## Follow-on order

1. **Manifest retirement and recovery** (t_df374fa8), plus the previously
   identified selector progress/cap issue (t_5f246338). Lowering task byte caps
   without fixing the min-four progress cliff can strand eligible STCS buckets.
2. **Shared local-I/O admission** (t_0e97b46b): repeat the wide fixture with
   byte pacing/resource reservations and record client tails, commit-log backlog,
   task progress, and drain. Preserve foreground durability/readiness guards.
3. **Fan-in-aware read-ahead budget**, then the **metadata-only verifier**
   (t_61ebd0fe). The measured window tradeoff rules out a blanket small default.
4. **Observability:** log input/staged/published generation links (t_ddbcc6dc),
   successful versus failed S3 work (t_b1c2eec6), and generator/protocol timing
   for the isolated readback tail (t_e00d36ad).

Automatic STCS/LCS/TWCS switching is not justified by these results. A resource
controller should first run in shadow mode against these fixtures, with an
explicit effective-strategy check and hysteresis. Five randomized repeats were
used for each probe cell; the Fly cases have one run each and require repetitions
before attributing small differences to a setting.

## Validation and cleanup

The benchmark suite passed all 111 Python tests; after the final lineage-only
refactor, its 31 tests passed again. All 220 fresh-process probe samples passed
independent correctness checks. The three completed Fly cases issued 665,304
operations with zero request errors or payload mismatches. This does not imply
healthy latency or uninterrupted server readiness; see the wide-partition results.

The dedicated Fly app `ferrosa-cx601-1023` was destroyed and its absence verified.
The dedicated bucket `ferrosa-cx601-next-20261023` had 31,346 objects removed,
was verified empty, and was destroyed. Baseline apps were left untouched.
Raw results, logs, manifests, sanitized effective configurations, and cleanup
receipts are retained in `../compaction-next-evidence-20261023` relative to the
worktree root. Server and allocation-probe Rust source remained unchanged.

## OS evidence for I/O stalls

Post-run analysis of the already-captured two-second `/proc` samples isolates the
wide mixed phase, 2026-10-23 23:26:58–23:31:58 UTC. The sampler reads CPU, I/O,
and memory PSI in that order. See `wide-mixed-os-audit.json` in the evidence folder.

| Machine | Sample span | I/O some stall | I/O full stall | Peak I/O full avg10 | Ferrosa CPU / two-vCPU capacity | Process read/write |
|---|---:|---:|---:|---:|---:|---:|
| 80d3edf6574168 | 299 s | 149.3 s | 120.6 s | 66.78% | 20.96% | 9,257 / 4,972 MiB |
| 8d027dfe0dd958 | 297 s | 137.0 s | 112.0 s | 67.15% | 20.81% | 8,921 / 4,874 MiB |
| 8d1949ae696e98 | 297 s | 128.0 s | 104.0 s | 67.46% | 20.93% | 8,601 / 4,866 MiB |

I/O full PSI means all non-idle tasks were stalled on I/O simultaneously; it is
VM-wide and does not identify a device or compaction specifically. Memory PSI
remained zero. CPU averages cannot rule out brief saturation or scheduling stalls.
Among 156 slow-sync log events, 79.90 of 89.22 cumulative seconds were in
`sync_data`; these are summed event durations across nodes, not wall-clock time.
The longest event spent 1,820.40 ms in `sync_data`, 0.026 ms in write, and
negligible time waiting for writers/the file lock. This establishes substantial
I/O stalls and supports I/O as a material bottleneck in the measured run. It does
not establish that compaction alone caused the tail, distinguish device capacity
from virtualization effects, or replace a controlled pacing/compaction A/B.
Device queue/latency counters and generator CPU/event-loop metrics were not captured.
