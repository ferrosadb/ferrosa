# Current performance: PR #625

Measured 2026-10-27 UTC on code `c24cad554dd61a0fb76bf385bdf0669f6638998f`.
Each Ferrosa/native pair ran on the **same physical Fly VM, filesystem and network destination**, with matched 2-performance-vCPU/4-GiB guests. One timed workload ran at a time. These are short, warm end-to-end measurements, not maximum sustained capacity or durability equivalence.

**Current limitations:** eight-writer PostgreSQL loading fails with SQLSTATE `40P01`; the PostgreSQL table below is a matched **one-writer** diagnostic. Counts and scans retain significant gaps. A separate profiled 30-second PostgreSQL read loop reached 3,312/s versus native 10,301/s; its difference from the short unprofiled table remains under investigation. Do not infer sustained PostgreSQL parity from the short runs.

## PostgreSQL

| Operation | Ferrosa | PostgreSQL |
| --- | ---: | ---: |
| INSERT, logical rows/s | **14,641** | 27,901 |
| Point reads, ops/s | **9,502** | 9,951 |
| Point-read p50, ms | **0.738** | 0.641 |
| COUNT p50, ms | **46.210** | 0.763 |
| Bounded scan, logical rows/s | **99,404** | 443,092 |

Higher rates are better; lower milliseconds are better.
One writer, eight readers, 5,000 rows. INSERT observations were Ferrosa 15,187, 14,440, 15,067, 13,871 and native 18,831, 18,863, 55,746, 18,166 rows/s. The short native loads vary substantially; every observation is retained.

## Cassandra / CQL

| Operation | Ferrosa | Cassandra |
| --- | ---: | ---: |
| INSERT, logical rows/s | **16,274** | 15,231 |
| Point reads, ops/s | **3,696** | 3,563 |
| Point-read p50, ms | **1.828** | 1.877 |
| COUNT p50, ms | **6.302** | 82.603 |
| Bounded scan, logical rows/s | **67,999** | 123,453 |

Higher rates are better; lower milliseconds are better.
Eight writers/readers, 20,000 rows. Complete key/value census passed for all 20,000 rows; this run does not silently stop at 5,000.

## Neo4j / Bolt

| Operation | Ferrosa | Neo4j |
| --- | ---: | ---: |
| INSERT, logical rows/s | **12,054** | 11,238 |
| Point reads, ops/s | **2,358** | 2,139 |
| Point-read p50, ms | **0.756** | 1.013 |
| COUNT p50, ms | **12.227** | 1.425 |
| Bounded scan, logical rows/s | **32,552** | 43,651 |

Higher rates are better; lower milliseconds are better.
20,000 nodes per label. `id` is Ferrosa’s generated UUID key; `pk` is a regular scalar property. Ferrosa auto-created its scalar indexes; Neo4j’s three RANGE indexes were ONLINE and its point plan used an index seek. Ferrosa access proof reads one vertex for equality and zero vertices for COUNT. Point p50 uses one reader; throughput uses eight. Loads have two observations per product, one per label.

## Oxigraph / SPARQL

| Operation | Ferrosa | Oxigraph |
| --- | ---: | ---: |
| INSERT, logical rows/s | **38,443** | 40,822 |
| Point reads, ops/s | **5,403** | 5,510 |
| Point-read p50, ms | **1.256** | 1.267 |
| COUNT p50, ms | **11.536** | 1.333 |
| Bounded scan, logical rows/s | **1,816** | 80,945 |

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
- PostgreSQL COUNT/scan and SPARQL multi-pattern scan rates remain major gaps. SPARQL uses nested-loop joins and predicate scans; no scan-specific CPU attribution is claimed. The eight-writer PostgreSQL deadlock remains a bug, not a supported performance result.

## Evidence

- [Current observations and artifact identity](current-results.json)
- [Matched baseline at 5b8356172](baseline-5b835-results.json)
- [Earlier experiment journey](HISTORY.md)
- CPU/profile artifacts and the profiler-control investigation are being finalized; primary timing tables above are complete.
