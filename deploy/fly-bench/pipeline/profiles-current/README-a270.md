# CPU flamegraphs and sampled resources

Runtime `a2709e100`; CPU stacks sampled at 49 Hz in separate eight-reader loops. Primary rates are measured without CPU profiling. PostgreSQL/SPARQL include separate count and complete-scan recordings with exact value validation. Client validation consumes CPU; these instrumented rates are diagnostic.

| Capture | Flamegraph | CPU samples |
| --- | --- | ---: |
| bolt-B | [SVG](bolt-B.svg) | 238 |
| bolt-B-count | [SVG](bolt-B-count.svg) | 906 |
| bolt-neo4j | [SVG (gzip)](bolt-neo4j.svg.gz) | 1,880 |
| bolt-neo4j-count | [SVG](bolt-neo4j-count.svg) | 786 |
| cql-cassandra-profile | [SVG](cql-cassandra-profile.svg) | 550 |
| cql-ferrosa_cql-profile | [SVG](cql-ferrosa_cql-profile.svg) | 194 |
| pg-ferrosa_pg-count-profile | [SVG](pg-ferrosa_pg-count-profile.svg) | 1,382 |
| pg-ferrosa_pg-profile | [SVG](pg-ferrosa_pg-profile.svg) | 450 |
| pg-ferrosa_pg-scan-profile | [SVG](pg-ferrosa_pg-scan-profile.svg) | 1,365 |
| pg-postgres-count-profile | [SVG](pg-postgres-count-profile.svg) | 1,878 |
| pg-postgres-profile | [SVG](pg-postgres-profile.svg) | 392 |
| pg-postgres-scan-profile | [SVG](pg-postgres-scan-profile.svg) | 152 |
| sparql-ferrosa_sparql-count-profile | [SVG](sparql-ferrosa_sparql-count-profile.svg) | 364 |
| sparql-ferrosa_sparql-profile | [SVG](sparql-ferrosa_sparql-profile.svg) | 409 |
| sparql-ferrosa_sparql-scan-profile | [SVG](sparql-ferrosa_sparql-scan-profile.svg) | 1,587 |
| sparql-oxigraph-count-profile | [SVG](sparql-oxigraph-count-profile.svg) | 2,575 |
| sparql-oxigraph-profile | [SVG](sparql-oxigraph-profile.svg) | 742 |
| sparql-oxigraph-scan-profile | [SVG](sparql-oxigraph-scan-profile.svg) | 2,339 |

[Detailed resource summaries](summary-a270.json) include CPU time, per-process RSS peaks, scheduler/wchan, PSI, I/O, disks and network. Gzip SVGs can be opened after `gunzip -k <file.svg.gz>`.

- CPU widths are sample counts, not elapsed request time.
- RSS is sampled per-process resident memory, not allocation volume. Do not sum process peaks/shared pages.
- Scheduler runqueue delay and wchan samples do not measure individual mutex wait.
- Resource windows include setup/census and sampling overhead; primary rate measurements exclude profiling.
- Server OS counters include all monitored colocated processes, so their CPU totals are not engine-only CPU.
- Native/JVM and shared-library frames may lack function names even when unknown-leaf percentage is zero; retain raw recordings and Neo4j JFR privately.

Primary serial counts and the longer concurrent profiles can have different flush/cache states. CPU categories overlap; samples are not elapsed wait durations.

The [earlier matched profiler control](../pg-duration-profiler-control.json) at `c24cad554` measured unprofiled 30-second PostgreSQL reads at 9,617–9,768/s versus native 10,622–10,804/s. Profiled Ferrosa varied 3,075–8,651/s. These results do not justify replacing primary rates with instrumented throughput.

Raw perf/JFR/configuration logs remain private. No empty flamegraphs are published.
