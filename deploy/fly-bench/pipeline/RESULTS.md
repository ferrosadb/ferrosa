# PR #625: measurement status

PR #625 combines PostgreSQL/Bolt/SPARQL correctness fixes, automatic scalar
columns and indexes for Bolt, and CQL scan pagination that preserves the total
LIMIT. **Fresh performance comparisons on the combined code are pending.**

The tables below are historical baselines measured on 2026-10-26 using engine
`a948ed1df48fd955bc144956b3d50546512665e2`. They do not measure the combined
implementation. Cassandra and SPARQL have no accepted current-code comparison
in this record yet.

## Compared with PostgreSQL

| Operation | Ferrosa | PostgreSQL 18.6 | Ferrosa / PostgreSQL |
| --- | ---: | ---: | ---: |
| INSERT, rows/s | **21,104** | 23,673 | **89.1%** |
| Point reads, ops/s | **9,745** | 10,798 | **90.3%** |

## Compared with Neo4j over Bolt

| Operation | Ferrosa | Neo4j 5.26.32 |
| --- | ---: | ---: |
| Point read, `pk` is Ferrosa primary key, p50 | **0.761 ms** | 1.866 ms |
| Point reads, `pk` is Ferrosa primary key, ops/s | **2,336** | 1,436 |
| Point read, `pk` is regular property, p50 | **91.63 ms** | 2.50 ms |
| Count over 20k nodes (lower is better) | **133.39 ms** | 1.50 ms |
| Bounded scan, rows/s | **37,580** | 52,933 |

Lower p50 is better. Neo4j uses an ONLINE index on `pk` in both fixtures.
The fast Bolt case declares `pk` as Ferrosa's native primary key. The slow case
uses UUID `id` as the primary key and filters regular property `pk`; Ferrosa scans
while Neo4j uses an ONLINE index. These are separate schema controls. Declaring a
primary key changes uniqueness semantics. **This baseline predates the new graph secondary-index reader**, so native-key speed does not establish general
graph parity.

## Measurement scope

- PostgreSQL: fresh 5k-row tables, 20k warm point reads per leg, eight workers.
- Bolt/Cypher: 20k nodes; point p50 uses one worker, throughput uses eight;
  counts and bounded scans use one. Both fixtures use 96-byte payloads.
- Each comparator shares a 2-performance-CPU/4-GiB Fly VM and root filesystem
  with Ferrosa. Network RTT is matched; one timed workload runs at a time.
- Two observations per engine/schema. Tables show mean rates and means of
  per-leg p50s; counts show the mean of per-leg medians. Values, keys and counts
  validate. Profiling runs separately. These are descriptive small-workload
  results; larger, cold, sustained, traversal and multi-node workloads are unmeasured.

## Remaining work

The combined candidate still needs matched-placement timing and profiles, with
independent writable fixtures for each binary because WAL/RDF formats differ.
A separate PostgreSQL loopback probe also retains a local lookup gap: 273 versus
117 microseconds p50. Neither CPU samples nor RSS establish exact allocation
volume or individual mutex wait time.

## Evidence

- [PostgreSQL observations and ranges](point-read-reverse-results.json)
- [Neo4j schemas, plans and observations](graph-placement-results.json)
- [Network and loopback controls](point-read-network-results.json)
- [Detailed experiment record and historical journey](HISTORY.md)
