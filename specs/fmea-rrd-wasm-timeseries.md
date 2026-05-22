# FMEA — RRD Materialized Rollups and WASM Time-Series UDFs

> Last updated: 2026-05-20
> Status: Draft

## Scope

Functions analyzed:

- Table-extension validation and consolidator registration.
- Async materialization queueing, retries, and observability.
- Built-in min/max/avg rollups and WASM stddev rollups.
- Late-data recomputation up to a configurable window.
- Admin-only WASM UDF loading and query-time execution.
- Sensor example docs and executable examples.

## Failure Mode Table

| # | Function | Failure mode | Effect | S | Cause | O | Detection | D | RPN | Test case |
| - | -------- | ------------ | ------ | - | ----- | - | --------- | - | --- | --------- |
| FM1 | DDL validation | Invalid `consolidation.*` config accepted | Feature appears enabled but never materializes rows | 8 | validation only happens in partial paths | 6 | current tests do not assert DDL rejection | 7 | 336 | invalid extension matrix rejects missing target/functions/columns |
| FM2 | Registry lifecycle | Existing table with extensions does not register on restart | rollups stop after restart | 9 | registry only hooks create-table path | 4 | integration restart test | 5 | 180 | create table, restart, insert crossing boundary |
| FM3 | Queue durability | Process crashes after enqueue before target write | lost rollup window | 9 | in-memory queue only | 4 | crash recovery test | 6 | 216 | kill worker after enqueue, restart, verify target row |
| FM4 | Queue lag | materialization falls behind silently | dashboards/alerts read stale aggregates | 8 | no lag virtual table/alerts | 6 | virtual table + subscribe tests | 5 | 240 | block worker, assert oldest age/depth/alert |
| FM5 | Late data | stale data rewrites rollups outside correction window | historical aggregates change unexpectedly | 7 | missing `late_window` enforcement | 4 | unit/integration tests | 4 | 112 | late row older than window increments drop counter |
| FM6 | Cascade | downstream tier columns do not match upstream output | second-tier rollups fail or compute nonsense | 8 | generated `consolidation.columns` drift | 5 | cascade integration test | 5 | 200 | 1s->5m->1h min/max/avg/stddev cascade |
| FM7 | UDF identity | overloaded UDF cache collision | wrong function executes | 8 | executor keyed by name only | 4 | overload test | 4 | 128 | same name int/double functions return distinct values |
| FM8 | UDF replacement | `OR REPLACE` invalidates old cache but schema create fails | function broken or stale | 8 | wrong DDL ordering | 5 | router test | 4 | 160 | replace changes SELECT result atomically |
| FM9 | UDF loading | URL artifact changes or redirects | unreviewed code executes | 10 | hashless/mutable URL load | 3 | import tests + audit | 4 | 120 | URL requires SHA-256 and rejects mismatch |
| FM10 | UDF execution | WASM stddev burns CPU/memory | rollup queue stalls | 8 | sandbox limit gaps | 4 | resource-limit tests | 5 | 160 | fuel/memory exhaustion fails task and records error |
| FM11 | WHERE UDF | predicate not evaluated despite being accepted | wrong query results | 7 | parser gate exists without eval path | 5 | router e2e test | 4 | 140 | `WHERE is_anomaly(v) = true ALLOW FILTERING` filters rows |
| FM12 | Subscriptions | rollup/status table updates are not delivered to subscribers | operators miss new rollups or lag alerts | 7 | virtual/target table subscription path mismatch | 4 | subscription integration tests | 5 | 140 | subscribe to rollup rows and queue delta updates |
| FM13 | Example docs | docs show path/blob behavior that implementation lacks | users cannot reproduce feature | 5 | stale examples | 7 | example CI | 3 | 105 | examples run schema/data/queries and docs snippets align |

## Highest Priority Mitigations

- FM1, FM3, FM4, and FM6 block public feature claims.
- FM7-FM10 block CQL-loaded WASM as a reliable rollup function surface.
- FM12 is required because materialization is async; without observable
  subscription support, delayed rollups are operationally invisible.

## Required Test Triads

- **Queue durability**: detection via virtual table, exploitation by crashing
  after enqueue, recovery by replay/reconstructing the window.
- **WASM URL import**: detection via hash mismatch, exploitation by redirect or
  changed artifact, recovery by preserving previous function metadata.
- **Late data**: detection via stale/drop counters, exploitation with rows just
  inside/outside `late_window`, recovery by recomputing only valid windows.

## Related

- [Architecture](rrd-wasm-timeseries-architecture.md)
- [Threat model](threat-model-rrd-wasm-timeseries.md)
- [Test specification](test-specification-rrd-wasm-timeseries.md)
