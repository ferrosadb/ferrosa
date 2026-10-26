# Measured pipeline changes

## Current findings

Ferrosa reaches **89.1% of PostgreSQL INSERT throughput and 90.3% of point-read
throughput** in the reverse-placement control. This supports rough end-to-end
throughput parity for the measured warm 5k-row, eight-worker workload, with two
observations per engine. Broader equivalence across larger datasets, cold reads,
sustained load and multi-node operation requires separate measurements.

| Point-read comparison | Ferrosa ops/s | PostgreSQL ops/s | PostgreSQL / Ferrosa | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Historical separate VMs | 3,313 | 10,212 | 3.08× | Unequal network paths; confounded |
| Both on original Ferrosa VM | 3,142 | 3,555 | 1.13× | Shared slower path |
| Both on original PostgreSQL VM | 9,745 | 10,798 | 1.11× | Shared faster path |

The controls demonstrate the placement effect in both directions. The reverse
control also changes storage placement and starts with fresh Ferrosa data, so
the throughput change is not attributed solely to networking or to a code fix.
Exact fixture, process, storage and repetition limits appear below.

Local lookup overhead remains: a separate single-worker loopback probe measured
**273 versus 117 microseconds p50** for full point reads and **220 versus 105
microseconds** for missing keys. The difference on a miss means returned-payload
copying alone cannot explain it. Loopback shares server CPUs with the client;
these are separate probes, not direct server spans or additive request timings.
Allocation volume, individual mutex waits and the exact host/network cause are
not established by these measurements. No current matched Neo4j comparison is
available.

## Reverse placement on PostgreSQL's original VM — 2026-10-26

The reverse control moves the **same Ferrosa binary** onto the native PostgreSQL
VM. The original Ferrosa VM is stopped during measurements. From the same runner,
kernel SYN/SYNACK RTT before database startup is **0.297 ms**
to Ferrosa and **0.315 ms** to PostgreSQL.
This tests the faster network path in the reverse direction.

| Reverse-control operation | Ferrosa | PostgreSQL 18.6 | Ferrosa / PostgreSQL |
| --- | ---: | ---: | ---: |
| INSERT, fresh 5k, paired mean rows/s | **21,104** | **23,673** | **89.1%** |
| Warm point reads, paired mean ops/s | **9,745** | **10,798** | **90.3%** |
| INSERT observed range, rows/s | 19,849–22,359 | 22,439–24,907 | Two observations |
| Read observed range, ops/s | 9,667–9,823 | 10,565–11,030 | Two observations |

The earlier roughly 22k number was INSERT rows/s, separate from point reads/s.
Primary order is PostgreSQL → Ferrosa → Ferrosa → PostgreSQL. Each leg creates a
fresh 5k table, inserts 96-byte payloads and performs 20k validated reads with
eight warmed workers. All 80k primary reads, row values, counts and key sets pass.
Both engines share the existing performance-2CPU/4096MiB/lax comparator VM and
its root filesystem; one receives timed benchmark queries at a time. Processes
are reused. Native PostgreSQL has fsync, synchronous_commit and full_page_writes
enabled. Ferrosa has a fresh isolated data directory and the original benchmark
configuration with adjusted addresses. CPU profiles run after primary timing.

This changes placement and storage fixture, including a fresh Ferrosa data
directory; the original Ferrosa VM used an attached volume and older fixture
state. It is not a new source-code speedup or a crash-recovery test. The same
client and workload are preserved. Two observations per engine are descriptive.
[Exact observations, checksums and controls](point-read-reverse-results.json).

## Network attribution and same-VM read control — 2026-10-26

The earlier **3.08× client-observed read ratio is network-confounded**. From the
same runner, kernel SYN/SYNACK RTT before PostgreSQL startup was **2.058 ms** to
the Ferrosa VM and **0.347 ms** to the native PostgreSQL VM. These are established
connection benchmarks: handshakes are not repeated for each read. The handshake
probe exposes unequal network paths that every request/response must traverse.

Moving the exact PostgreSQL 18.6 binary onto the Ferrosa VM makes both targets use
one destination IP. Kernel handshake RTT then measures **2.050 ms** for Ferrosa
and **2.006 ms** for PostgreSQL; protocol-only Sync p50 is **2.149 ms** and
**2.146 ms**, respectively. The delay follows the VM/network path. Its exact
provider-side cause is not established.

| Same-VM warm point reads | Ferrosa | PostgreSQL 18.6 | Ferrosa / PostgreSQL |
| --- | ---: | ---: | ---: |
| Remote client, eight workers, paired mean ops/s | **3,142** | **3,555** | **88.4%** |
| Remote client, observed ops/s range | 3,045–3,239 | 3,498–3,612 | PostgreSQL / Ferrosa **1.13×** |
| Loopback `SELECT 1`, one worker, p50 | 0.088 ms | 0.104 ms | Separate diagnostic |
| Loopback full point read, one worker, p50 | **0.273 ms** | **0.117 ms** | Separate diagnostic |

Primary order is Ferrosa → PostgreSQL → PostgreSQL → Ferrosa, with a fresh 5k
table and 20k validated reads per leg, eight warmed workers and 96-byte payloads.
Both services share one performance-2CPU/4096MiB/lax VM; one receives benchmark
queries at a time. Processes are reused between legs. The native binary matches
the prior comparator SHA256. Its data directory is ephemeral, so INSERT and
maximum-capacity claims are excluded. Primary timings precede CPU profiling.
Two samples per engine are descriptive, not a statistical significance result.

This is a measurement correction, not a new engine speedup. A smaller point-read
overhead remains. Loopback removes the external route but shares server CPUs
with the client. Separate probe medians cannot be summed into request spans;
the remaining queue, decode, MVCC and protocol costs need direct span timing.
[Numeric evidence, checksums and limits](point-read-network-results.json).

## Historical separate-VM four-change walk — 2026-10-26

Runtime of the historical walk: `a948ed1df48fd955bc144956b3d50546512665e2`. Fresh process, 5,000 seeded rows,
20,000 fixed seeded point reads, eight warmed workers, 96-byte payloads. Values,
keys and counts are validated. Means below use two unprofiled observations per
checkpoint, in forward 0→1→2→3→4 then reverse 4→3→2→1→0 order, bracketed by
two native PostgreSQL 18.6 controls. Separate CPU profiles are diagnostic only.

| Historical separate-VM operation | Ferrosa | PostgreSQL 18.6 | Ferrosa / PostgreSQL | PostgreSQL / Ferrosa |
| --- | ---: | ---: | ---: | ---: |
| INSERT, fresh 5k, rows/s | **22,049** | 23,578 | **93.5%** | 1.07× |
| Point reads, fresh 5k, ops/s | **3,313** | 10,212 | **32.4%** | **3.08×** |

### Ordered point-read walk

| Cumulative checkpoint | Runtime | Reads/s, paired mean (range) | Change from previous | / PostgreSQL |
| --- | --- | ---: | ---: | ---: |
| Baseline | `15d9e7f0e` | **3,048** (3,005–3,091) | — | 29.9% |
| 1. Move decoded values | `840152137` | **2,987** (2,954–3,020) | -2.0% | 29.2% |
| 2. Fuse open / first fetch | `bdd095abf` | **2,979** (2,930–3,029) | -0.2% | 29.2% |
| 3. Direct encoding / response buffer | `de0806312` | **3,239** (3,218–3,259) | +8.7% | 31.7% |
| 4. Skip replaced SQL materialization | `a948ed1df` | **3,313** (3,308–3,319) | +2.3% | 32.4% |

Total read change: **+8.7%**.
These are cumulative changes, so each row compares with the preceding checkpoint.
Two repetitions are descriptive; the ranges matter and do not establish statistical
significance. The final PostgreSQL read controls are 9,837
and 10,586 ops/s.

The native read controls drifted **+7.6%**.
To check the bundle without the cold-build gaps, an additional 0→4→4→0 run
measured baseline **2,947** versus final
**3,285** reads/s
(**+11.5%**). That confirmation is kept
separate from the intermediate walk; both comparisons remain descriptive.

INSERT paired mean changes from 23,591 to
22,049 rows/s (-6.5%).
The baseline range is 22,055–25,127;
the final range is 19,882–24,215.
Those ranges overlap; this walk does not establish an INSERT improvement.

Step 1 pins allocation identity/capacity when moving CQL values. Step 2 pins one
blocking job for opening and the first fetch. Step 3 proves zero allocations in
the preallocated common scalar encoder and batches a bounded point response at
Sync; specialized codecs retain their checked fallback. Step 4 removes throwaway
SQL row materialization while preserving physical CQL decoding and error checks.
The physical CQL payload decode and owned MVCC image copy remain; this is not a
claim that the whole read path is zero-copy or allocation-free.

A separate 400-read Fly syscall trace confirms five successful socket writes per
warmed point read became one. Including eight prepare cycles, sends change from
2,016 to 408, with the same 89,937 total response bytes and all returned values
validated. Traced timings are not included in the throughput comparison.

Both engines use the same Fly performance-2CPU/4096MiB/lax class and client.
Ferrosa uses an attached volume and PostgreSQL its root filesystem. Read CPU
profiles, process/RSS, disk/network and scheduler data were gathered separately
and retained privately. CPU samples and runqueue sums are not individual mutex
wait times; RSS is not allocated bytes. No current matched Neo4j comparison is
established. [Exact values and image identities](point-read-walk-results.json).

The preceding 100k/follow-on-5k workload is retained below with its runtime. It
has not been rerun on runtime `a948ed1df` and is not a current 100k claim.

## Previous matched workload — 2026-10-25

Ferrosa runtime for this preceding workload: `15d9e7f0e6991301d304fbf0034877b9cd514d76`.
Native comparison: PostgreSQL 18.6. These are means of two unprofiled runs per
system, with full key/value/count validation. Eight workers, 96-byte payloads
and 400 seeded point reads; the 5k fixture follows a 100k workload in the same
process.

| Previous workload operation | Ferrosa | PostgreSQL 18.6 | Ferrosa / PostgreSQL | Remaining gap |
| --- | ---: | ---: | ---: | --- |
| INSERT, 100k rows, rows/s | **24,275** | 26,392 | **92.0%** | PostgreSQL 1.09× faster |
| INSERT, 5k follow-on, rows/s | **22,842** | 24,900 | **91.7%** | PostgreSQL 1.09× faster |
| Point reads, 5k follow-on, ops/s | **2,814** | 9,855 | **28.6%** | PostgreSQL **3.50× faster** |

INSERT is within roughly 8% of PostgreSQL in these workloads. Point reads remain
the main measured PostgreSQL gap. Older intermediate-revision measurements
are retained below as history.

At that historical checkpoint, the INSERT change improved the paired 100k rate from 17,138 to 24,275
rows/s (+41.6%). The 5k before/after ranges overlap, so its smaller mean change
does not establish a speedup. Sampled peak RSS rose from 455 to 503 MiB despite
fewer allocations. Two runs do not establish statistical significance. Both
servers use the same 2-performance-CPU/4-GiB class and region; Ferrosa uses an
attached volume and PostgreSQL its root filesystem, so storage is not identical.

Neo4j: **no current matched Ferrosa-versus-Neo4j result is established here**.
The older Bolt numbers below are Ferrosa revision comparisons. Different older
native Neo4j workloads cannot establish the current product gap.

Source values/ranges: [historical INSERT and point-read results](mvcc-absence-results.json).
The sections below preserve the measured journey at their named revisions;
their before/after rates are **historical**, not current comparison-product results.

## Historical measurements

These are descriptive means of two unprofiled repetitions per arm on one Fly
LAX performance VM with 2 CPUs and 4 GiB RAM, using the same attached data
volume, native x86-64 builds and a separate client. No significance or general
database performance claim follows from two repetitions. All accepted legs
validated counts, every scanned key and row values.

Unless stated otherwise: PostgreSQL uses 5,000 rows, 96-byte payloads, 8 workers,
and 400 seeded point reads.
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
prevents using those legs for throughput. At that revision, the guard left a
global MVCC critical section and traversal of unrelated history. The persistent-root MVCC
replacement and its later comparisons are recorded below.

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
the remaining gap at that revision was 3.19×. INSERT throughput is effectively
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
mutex/request time is not measured by a CPU flame graph. Those profiles do
not establish request/handoff/off-CPU timing; the current placement-controlled results are above. Raw
profiles and resource counters are retained in the private evidence.

## Payload-label correction (2026-10-25)

Earlier text and result extracts incorrectly called these 512-byte payloads.
`make_payloads(512)` creates 512 variants; `PAYLOAD_LEN = 96` sets each payload
to 96 ASCII bytes. The timed workload and Ferrosa/PostgreSQL comparison bytes
are unchanged. The extracted metadata is corrected, and the client now records
actual UTF-8 byte length and variant count in every result. A contract test
checks the JSON emitted by the real client entry point. No 512-byte workload
performance claim follows from the measurements above.

## Shared fresh-INSERT absence (2026-10-25)

The earlier MVCC and Sync rows are successive historical checkpoints. The current
comparison is exact-key/decoder source `e8772cf29ec756d63278cf25d6db4836cdf1ee88`
against `15d9e7f0e6991301d304fbf0034877b9cd514d76`. The latter represents a newly
tracked key's original absence once, avoiding one persistent-map entry per key
per active reader. Actual physical before-images and historical snapshot rows
remain protected before storage apply. Already tracked keys skip redundant
baseline entry mutation.

Order D41/E41/P41/P42/E42/D42, two unprofiled repetitions per Ferrosa arm. Each
fresh Ferrosa process runs a 100k fixture, then a distinct 5k fixture; every
key/value/count check passed. Fresh PostgreSQL 100k controls are in that sequence.
The separate fresh PostgreSQL 5k controls also follow a validated 100k workload
in the same process. All use 96-byte payloads and 8 workers.

| INSERT fixture | Before rows/s | After rows/s | PostgreSQL 18.6 rows/s | After / PostgreSQL |
| --- | ---: | ---: | ---: | ---: |
| 100,000 rows | 17,138 | 24,275 | 26,392 | 92.0% |
| 5,000 rows after 100k | 22,038 | 22,842 | 24,900 | 91.7% |

The 100k improvement is **41.6%**, from 64.9% to 92.0% of PostgreSQL. The 5k
mean improves 3.6%, but observed ranges overlap (before 19.7–24.4k, after
21.6–24.0k); this does not establish a small-load speedup. Two repetitions do
not establish significance or general database performance. The storage
placement difference above still applies. Individual rates/ranges and profile
summaries are in [mvcc-absence-results.json](mvcc-absence-results.json).

A real-manager allocation test keeps 1 or 16 readers alive while committing
128/256 fresh keys, excluding fixture construction. At 256 keys, allocations
change from 3,543/7,913 to 2,704/2,704. Restoring only the None fanout fails this
regression; suppressing only the known-absence fallback fails a paused-apply
visibility test. Both controls were restored before verification.

Separate 200k-row software CPU captures show persistent-map frames on 534/972
stacks before (54.9%) and 225/647 after (34.8%); categories are inclusive.
Captured process CPU is 26.26s versus 17.93s, including collector gates beyond
the load interval. Sampled peak RSS **increases** from 455 to 503 MiB. Fewer
allocations do not establish lower resident memory. Disk/network counters and
summed thread run-queue samples are retained; these are not individual mutex
waits or request latency. Background I/O and network totals differ across the
captures, so their whole-process deltas are diagnostic, not precise isolated
attribution. Profiled throughput is excluded from the table.

At that checkpoint, the read gap remained open. This change targets INSERT metadata; it
does not establish bounded MVCC residency or eliminate query/protocol handoffs.
