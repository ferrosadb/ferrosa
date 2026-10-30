# Performance comparisons

## Local allocation follow-up (2026-10-28)

This follow-up is a separate local Linux ARM64 VM experiment, **not a rerun of
the product comparisons below**. Baseline source was `0b552fea9`; the timing arm
changed only CQL fragment reservation. The fixture was 5,000 memtable partitions
with 96-byte payloads, validated for complete keys and values. Three paired
trials alternated before/after order, each with one warmup and 20 queries, with
profiling disabled and no concurrent builds. Values below are medians across
trial medians; the small sample does not establish statistical significance.

| CQL scan measurement | Before | After |
| --- | ---: | ---: |
| Throughput, rows/s | 117,958 | 121,242 |
| Query median, ms | 42.39 | 41.24 |
| Process CPU seconds per 21-query batch | 0.84 | 0.47 |

Separate sampled jemalloc profiles, subtracting a matched no-query fixture and
dividing by 21 queries, attributed fragment allocations of 1,639.16 to 2.38
decimal MB/query. This is cumulative allocation traffic, not live memory or RSS.
The first Bolt empty-predicate, borrowed-parent and eligible anchor-LIMIT fixes
reduced estimated full-scan allocation from 38.46 to 20.12 MB/query; no paired
Bolt throughput result is available. Shared memtable cursors, scheduler admission,
PG inline keys and the final owned Bolt projection/serialization moves were
added later and have **not** had a completed combined performance comparison.
Do not attribute these timing numbers to that whole batch.

## Historical Fly comparison: PR #625

PostgreSQL/CQL below measure production code `2286f389ac6669d44d5cfef1e71c7facdce37c30`.
Later Rust commits change tests only (`03cf9511e`). **Bolt/SPARQL retain older
`a2709e100` results and were not rerun after the shared-value change.**

PG COUNT is 8.3× faster than before; CQL scan mean rises
12.7%. PG bounded-scan means are effectively unchanged.
These small samples do not establish maximum capacity or statistical significance.
[Every observation and its range](current-results.json) is retained.

**Quality limits:** one native PostgreSQL leg flagged **2.83% CPU steal** over
its sampled three-second interval. Its observation is retained; the native PG
comparison is provisional. Primary measurements are complete, but **five PG
CPU captures were interrupted by Fly SSH timeouts**. Three CQL scan captures
and native PG COUNT completed. All six machines were verified stopped; volumes remain.

Higher rates are better; lower milliseconds are better. Tables use means of
four per-leg rates or medians. Before/after also include the COUNT metadata
change; they do not isolate immutable payload sharing alone.

## PostgreSQL

| Operation | Before `a2709e100` | Current `2286f389a` | PostgreSQL |
| --- | ---: | ---: | ---: |
| INSERT, logical rows/s | 27,535 | 32,641 | 38,993 |
| Point reads, ops/s | 9,633 | 9,836 | 10,571 |
| Point-read p50, ms | 0.727 | 0.715 | 0.593 |
| COUNT p50, ms | 45.716 | 5.496 | 0.672 |
| Bounded scan, logical rows/s | 98,575 | 98,713 | 487,618 |

One writer, eight point readers, 5,000 rows.

## Cassandra / CQL

| Operation | Before `a2709e100` | Current `2286f389a` | Cassandra |
| --- | ---: | ---: | ---: |
| INSERT, logical rows/s | 16,400 | 16,019 | 13,795 |
| Point reads, ops/s | 3,729 | 3,784 | 3,461 |
| Point-read p50, ms | 1.811 | 1.782 | 1.945 |
| COUNT p50, ms | 6.693 | 7.010 | 75.202 |
| Bounded scan, logical rows/s | 69,816 | 78,713 | 153,079 |

Eight writers/point readers, 20,000 rows; bounded scan returns 10,000 rows. Complete key/value census passed.

## Older comparisons: not remeasured on current code

### Neo4j / Bolt — runtime `a2709e100`

| Operation | Ferrosa (older runtime) | Native |
| --- | ---: | ---: |
| INSERT, logical rows/s | 13,223 | 11,994 |
| Point reads, ops/s | 2,335 | 2,159 |
| COUNT p50, ms | 13.311 | 1.261 |
| Bounded scan, logical rows/s | 33,289 | 42,996 |

### Oxigraph / SPARQL — runtime `a2709e100`

| Operation | Ferrosa (older runtime) | Native |
| --- | ---: | ---: |
| INSERT, logical rows/s | 38,763 | 36,407 |
| Point reads, ops/s | 5,179 | 5,229 |
| COUNT p50, ms | 11.850 | 1.801 |
| Bounded scan, logical rows/s | 29,937 | 74,329 |

Bolt used 20,000 nodes, automatic Ferrosa scalar indexes and ONLINE Neo4j
RANGE indexes. SPARQL used 2,000 logical records/6,000 triples and a three-pattern
join. These scan rates are not interchangeable physical-storage bandwidths.
[Older runtime evidence](baseline-a270-results.json).

## Methodology

- Each Ferrosa/native pair runs on the **same physical Fly VM, filesystem and
  network destination**, with 2 performance vCPUs/4 GiB and the same client VM.
  Only Ferrosa's executable changes between A/B; loader/libraries are shared.
- Order: native/A/B/native/B/A, repeated twice. Each leg has an independent
  writable fixture, 96-byte payloads from 512 variants, and exact key/value/count
  validation. PostgreSQL/CQL perform 20,000 warm point reads per leg. All scans
  project `pk`, `bucket`, `payload` and fully consume their requested rows.
- Primary scan is **one serial bounded query per leg**, not sustained capacity.
  Separate 10-second one/eight-reader delivery controls use PG single-row
  streaming or explicit CQL pages of 5,000. First-row and first-page timings
  describe different units. These controls do not calibrate profiler overhead.
- Separate 30-second eight-reader CPU diagnostics use full adapter scans/counts.
  CPU sampling is 49 Hz; context-switch callchains sample every 100 events for
  five seconds. Decoding occurs after the workload stops. Files are capped and
  removed remotely only after verified download; free space is guarded at 1 GiB.
- OS counters sample CPU/steal, RSS, runqueue/wchan, PSI, process/disk I/O, network,
  TCP queues and free disk at 1 Hz. CPU stacks and context-switch event weights
  are **not elapsed mutex waits**. RSS is not allocation volume. Sampling and
  profiler overhead are uncalibrated; unavailable cgroup counters remain unknown.
- Native versions: PostgreSQL 18.6, Cassandra 5.0.9; older Neo4j 5.26.32 and
  Oxigraph 0.5.11. INSERT includes connection/preparation overhead. Short loads,
  JVM warm-up, flush/cache state and client CPU can materially affect results.

## Limits and remaining work

- PG uses **one writer**: the eight-writer load still fails with `40P01`.
  This remains an unresolved bug. Point reads are near native rates; PG and CQL
  scans retain substantial gaps. Sharing removed payload copies but did not
  close the PG scan gap. [Ownership and remaining costs](SCAN_COUNTS.md).
- Eligible PG unfiltered single-row-table `COUNT(*)` walks key/liveness metadata
  and reconciles MVCC without SQL payload rows or the row channel. Other shapes
  stream normally. COUNT remains linear metadata work, not constant time.
- Immutable scalar values retain backing storage; CQL projection/page selection
  moves rows, and PG decodes lazily. Text adoption still allocates an ownership
  handle. Storage parsing, complex-value assembly, owned ABI boundaries and
  final wire framing still have costs; this is not end-to-end zero-copy.
- These are short, warm, single-node **local-file object-store** measurements.
  Cold/S3, large data, sustained/JIT convergence, replication/HA and crash recovery
  are unmeasured. Native PG fsync/synchronous_commit are on; direct-write periodic
  policies differ, and no crash test establishes durability equivalence.
- Bolt automatic scalar columns/indexes and memoized setup are implemented;
  explicit Cypher index DDL remains unsupported. Eligible counts use liveness
  metadata. Current-code Bolt throughput has not been established by this rerun.
- SPARQL has bounded COUNT/DISTINCT and targeted SPARQL 1.2 Protocol/Results JSON
  contracts. Other aggregates and COUNT over UNION/OPTIONAL/subqueries remain
  explicit errors. RDF 1.2 persistence/graph XML, dataset overrides, optional HTTP
  QUERY and full W3C conformance remain unimplemented. Predicate scans still
  traverse the triples table. Current-code SPARQL throughput is unmeasured here.

## Evidence

- [Current observations, ranges and artifact identities](current-results.json)
- [Journey and earlier experiments](HISTORY.md)
- [COUNT/scan ownership evidence](SCAN_COUNTS.md)
- [CPU/context-switch graphs and resource summaries](profiles-current/README.md)
- [Hardware-quality windows](hardware-quality-current.json)
