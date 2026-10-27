# Current performance: PR #625

Measured 2026-10-27 UTC on code `a2709e100cb016e46b51df7b2052a468c8ef16a2`.
Each Ferrosa/native pair ran on the **same physical Fly VM, filesystem and network destination**, with matched 2-performance-vCPU/4-GiB guests. One timed workload ran at a time. These are short, warm end-to-end measurements, not maximum sustained capacity or durability equivalence.

**Current limitations:** the preserved eight-writer PostgreSQL load failed with SQLSTATE `40P01` and remains unresolved; the PostgreSQL table below is a matched **one-writer** diagnostic. Counts and scans retain significant gaps. A matched 30-second profiler control after flush settling measured unprofiled Ferrosa at 9,617–9,768 reads/s versus PostgreSQL at 10,622–10,804. Ferrosa’s profiled repetitions varied widely (3,075–8,651/s); these instrumented rates do not establish a sustained threefold read gap. Two repetitions per condition do not establish maximum capacity.

## PostgreSQL

| Operation | Ferrosa | PostgreSQL |
| --- | ---: | ---: |
| INSERT, logical rows/s | **22,127** | 38,319 |
| Point reads, ops/s | **9,150** | 9,953 |
| Point-read p50, ms | **0.765** | 0.652 |
| COUNT p50, ms | **43.967** | 0.865 |
| Bounded scan, logical rows/s | **92,742** | 420,447 |

Higher rates are better; lower milliseconds are better.
One writer, eight readers, 5,000 rows. The eight-writer diagnostic is not repeated here. INSERT observations were Ferrosa 32,183, 13,803, 13,839, 28,685 and native 60,158, 58,013, 18,783, 16,320 rows/s. The short native loads vary substantially; every observation is retained.

## Cassandra / CQL

| Operation | Ferrosa | Cassandra |
| --- | ---: | ---: |
| INSERT, logical rows/s | **16,819** | 14,365 |
| Point reads, ops/s | **3,852** | 3,526 |
| Point-read p50, ms | **1.763** | 1.914 |
| COUNT p50, ms | **7.612** | 94.776 |
| Bounded scan, logical rows/s | **68,296** | 127,761 |

Higher rates are better; lower milliseconds are better.
Eight writers/readers, 20,000 rows. Complete key/value census passed for all 20,000 rows; this run does not silently stop at 5,000.

## Neo4j / Bolt

| Operation | Ferrosa | Neo4j |
| --- | ---: | ---: |
| INSERT, logical rows/s | **13,223** | 11,994 |
| Point reads, ops/s | **2,335** | 2,159 |
| Point-read p50, ms | **0.764** | 0.967 |
| COUNT p50, ms | **13.311** | 1.261 |
| Bounded scan, logical rows/s | **33,289** | 42,996 |

Higher rates are better; lower milliseconds are better.
20,000 nodes per label. `id` is Ferrosa’s generated UUID key; `pk` is a regular scalar property. Ferrosa auto-created its scalar indexes; Neo4j’s three RANGE indexes were ONLINE and its point plan used an index seek. Ferrosa access proof reads one vertex for equality and zero vertices for COUNT. Point p50 uses one reader; throughput uses eight. Loads have two observations per product, one per label.

## Oxigraph / SPARQL

| Operation | Ferrosa | Oxigraph |
| --- | ---: | ---: |
| INSERT, logical rows/s | **38,763** | 36,407 |
| Point reads, ops/s | **5,179** | 5,229 |
| Point-read p50, ms | **1.360** | 1.333 |
| COUNT p50, ms | **11.850** | 1.801 |
| Bounded scan, logical rows/s | **29,937** | 74,329 |

Higher rates are better; lower milliseconds are better.
Eight writers/readers; 2,000 logical records, each represented by three triples. INSERT uses batches of 128 records. COUNT is now an actual server aggregate, not a client scan substitute. The benchmark uses the common SPARQL query subset; it is not a W3C conformance suite.

## Methodology and limits

- Four observations per product in native/Ferrosa/Ferrosa/native order, repeated twice; table values are means of per-leg rates/medians. Bolt seed/load rates have two observations. [Raw observations, minima and maxima](current-results.json) are retained.
- PostgreSQL/CQL use 20,000 warm point reads per leg; SPARQL uses 2,000; Bolt uses 20,000 at concurrency eight plus 500 serial reads. Read worker connections warm before the timed barrier. PostgreSQL connection creation/preparation is included in loading.
- Payloads are 96 bytes from 512 deterministic variants. Values, complete key sets and counts validate before accepting a leg. Scans return all three fields and at most 10,000 records (2,000 for SPARQL). All benchmarks are single-node and warm.
- Native versions: PostgreSQL 18.6, Cassandra 5.0.9, Neo4j 5.26.32 and Oxigraph 0.5.11. Exact source/image/binary identities are in current-results.json. Each code version uses independent writable fixtures.
- OS sampling records CPU/steal, RSS, scheduler runqueue/wchan, PSI, process I/O, disks and network at 1 Hz; CPU stacks are recorded separately at 49 Hz. RSS is not allocation volume; scheduler/wchan samples do not measure individual mutex wait durations. Guest cgroup CPU/memory-event files are unavailable and are not treated as zero.
- Hardware quality checks invalidate OOM/counter resets and flag CPU steal >=1%, exposed throttling >=1% wall or full-memory PSI >=0.1% wall. Checks cover phase and SSH-bounded primary-leg windows. The current resource collector’s own overhead is not independently calibrated.
- Client CPU can limit these driver workloads. Profiled 30-second loops are diagnostic and must not replace the unprofiled primary rates. Cold, larger, sustained, traversal, replication/HA, crash recovery and S3 recovery performance are unmeasured.
- Native PostgreSQL has fsync/synchronous_commit on. Ferrosa’s atomic PostgreSQL batch path synchronizes its commit log before publication; direct-write periodic policies differ from native defaults. No crash test establishes equivalent durability guarantees.

## Implemented changes and remaining features

- Bolt creates scalar columns/indexes on first ordinary writes and memoizes physical schema setup. Explicit Cypher index DDL is still unsupported.
- Counts over eligible Bolt shapes use live-row metadata rather than vertex JSON materialization. The resident count path now borrows its pinned partition; a 32-row, 256-column fixture reduced requested allocation bytes from 462,427 to 1,083. This is allocation evidence, not a claimed end-to-end COUNT speedup. Counts still walk metadata; flushed reads/cache contention remain costs.
- SPARQL supports bounded global/grouped COUNT and DISTINCT. Single-pattern COUNT streams into counters; joins and property paths retain their bounded evaluator. Other aggregates and COUNT over UNION/OPTIONAL/subqueries remain explicit errors.
- SPARQL 1.2 Results JSON supports recursive triple terms and directional literals for SELECT expressions, plus version negotiation for SELECT/ASK. HTTP cardinality, dataset refusal, query-form negotiation and service-base handling are tested. These contracts target the W3C Protocol draft of 2026-07-23 and JSON draft of 2026-08-13.
- RDF 1.2 persistence and graph/XML output, protocol dataset overrides, optional HTTP QUERY, strict operation-version-specific grammar validation and full W3C conformance remain unimplemented. Unsupported persistence/graph paths fail explicitly.
- PostgreSQL COUNT/scan and SPARQL multi-pattern scan rates remain gaps. SPARQL now uses borrowed-key indexing for fully bound shared-subject joins, but every predicate pattern still performs a streaming full scan. General unbound joins retain a compatibility fallback. The eight-writer PostgreSQL deadlock remains a bug, not a supported performance result.

SQL scalar aggregation now consumes and releases rows without sorting/spilling;
SQL projections move owned payload values. In the isolated SPARQL join fixture,
128 records requested 18,498,367 bytes before versus 1,916,787 after. The matched
SPARQL scan now reaches 29,937 rows/s versus the earlier candidate’s 1,816;
PostgreSQL COUNT/scan did not materially improve. [Ownership proof, CPU evidence
and remaining count work](SCAN_COUNTS.md) distinguish the measured costs.

The measured runtime is `a2709e100`. Final documentation includes only a
rustfmt layout change to the projection guard; normalized source equivalence
was checked. CPU profiles are separate eight-reader diagnostics and can cover
a different flush/cache state from serial primary counts.

## Evidence

- [Current observations and artifact identity](current-results.json)
- [Earlier candidate at c24cad554](baseline-c24-results.json)
- [Matched baseline at 5b8356172](baseline-5b835-results.json)
- [Earlier experiment journey](HISTORY.md)
- [Eighteen CPU flamegraphs and CPU/RSS/wait/disk/network summaries](profiles-current/README.md)
- [Hardware-quality windows](hardware-quality-current.json)
- [Eight matched PostgreSQL duration/profiler-control observations](pg-duration-profiler-control.json). All six Fly machines were verified stopped after the control; volumes were retained.

Hardware checks covered 84 phase/primary-leg windows with no invalid intervals
or threshold flags; maximum observed CPU steal was 0.51%. Cgroup accounting
was unavailable, so throttling and cgroup memory events remain unknown. All
six Fly machines were verified stopped after the final run; volumes remain.
