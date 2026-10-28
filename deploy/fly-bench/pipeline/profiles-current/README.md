# Current CPU captures and resource evidence

Production code `2286f389a` (B), baseline `a2709e100` (A). Primary throughput
observations are complete. **Five PostgreSQL CPU captures were interrupted
by Fly SSH control timeouts**; only completed captures below are published.

| Capture | CPU graph | CPU samples | Context-switch graph | Sampled callchains |
| --- | --- | ---: | --- | ---: |
| cql-cassandra-scan-profile | [CPU](scan144-cql-cassandra-scan-profile-cpu.svg) | 1522 | [Switches](scan144-cql-cassandra-scan-profile-switches.svg) | 796 |
| cql-ferrosa_cql-A-scan-profile | [CPU](scan144-cql-ferrosa_cql-A-scan-profile-cpu.svg) | 562 | [Switches](scan144-cql-ferrosa_cql-A-scan-profile-switches.svg) | 2185 |
| cql-ferrosa_cql-B-scan-profile | [CPU](scan144-cql-ferrosa_cql-B-scan-profile-cpu.svg) | 622 | [Switches](scan144-cql-ferrosa_cql-B-scan-profile-switches.svg) | 943 |
| pg-postgres-count-profile | [CPU](scan144-pg-postgres-count-profile-cpu.svg) | 1870 | [Switches](scan144-pg-postgres-count-profile-switches.svg) | 108 |

[Resource/CPU/TCP summaries](summary.json) · [Hardware quality](../hardware-quality-current.json)

- CPU stacks sample at 49 Hz in separate 30-second eight-reader count/full-scan loops.
- Context switches sample every 100 events for five seconds. Widths are sampled event weights, **not wait durations**.
- Recordings are decoded after the workload stops. Context files cap at 64 MiB; verified downloads precede remote cleanup.
- RSS is not allocation volume. Runqueue/wchan do not measure individual mutex waits. CPU inclusive frames overlap.
- Delivery controls use PG single-row streaming/CQL explicit paging; their rates do not replace primary fetch-all rates or calibrate profiler overhead.
- Native Java frames are mostly module-only. No empty graphs or unfinished PG captures are presented as complete.
- One native PG primary leg flagged 2.83% CPU steal over its sampled 3-second interval; the table retains it.
- Gzip graphs open after `gunzip -k file.svg.gz`. Raw recordings/configuration remain private.

[Earlier a270 profile collection](README-a270.md) is historical and is not a current-code comparison.
