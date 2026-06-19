# ferrosa-sparql

> SPARQL 1.1 Query + Update endpoint for ferrosa, built on `spargebra`/`oxrdf`,
> translating algebra trees into reads and writes against a single CQL-backed
> `rdf_triples` table.

## What this crate is

`ferrosa-sparql` is the W3C SPARQL front-end. It parses SPARQL with `spargebra`,
plans the algebra into storage operations against ferrosa's `rdf_triples` table
(`((graph, subject), predicate, object)`), executes them via the cluster write
path, and serializes results with hand-written JSON / XML / N-Triples / Turtle
encoders. The HTTP surface implements the SPARQL 1.1 Protocol over `axum`.

Its model is **one RDF graph per keyspace**: the `graph` partition-key component
is the keyspace name, and there is exactly one `rdf_triples` table per keyspace.
Operations that would address a *different* named graph (the `ADD`/`MOVE`/`COPY`
desugaring, `GRAPH <g> { … }` reads, named-graph insert/delete targets) are
**rejected fail-loud** rather than silently retargeted to the default graph.

## What's implemented

- **Query forms** — `SELECT`, `ASK`, `CONSTRUCT`, `DESCRIBE` (both
  `DESCRIBE <iri>` and `DESCRIBE ?var WHERE { … }`).
- **Graph-pattern coverage** — BGPs, `PROJECT`, `DISTINCT`/`REDUCED`,
  `SLICE` (LIMIT/OFFSET), `ORDER BY` (over arbitrary supported expressions),
  `FILTER`, `UNION` (concatenated), property paths. Multi-pattern joins run as a
  nested-loop join with binding-compatibility checks.
- **Property paths** — `+` (OneOrMore), `*` (ZeroOrMore), `?` (ZeroOrOne),
  reverse (`^`) over a single predicate, evaluated by in-memory BFS with cycle
  detection (`src/property_path.rs`).
- **FILTER** — equality/comparison, `&&`/`||`/`!`, `BOUND`, `sameTerm`, `IF`,
  arithmetic (`+ - * /`), and the functions `STR`, `UCASE`, `LCASE`, `STRLEN`,
  `ABS`, `CEIL`, `FLOOR`, `ROUND`. Unsupported FILTER sub-forms evaluate to Null.
- **Update** — `INSERT DATA`, `DELETE DATA`, `DELETE WHERE`,
  `DELETE/INSERT … WHERE`, `CLEAR`, `DROP`, `CREATE` (no-op). All deletes funnel
  through one tombstone primitive. `LOAD` and named-graph ops fail loud; the
  whole request is validated up-front for atomicity (no partial mutation).
- **RDF\* (SPARQL-star)** — quoted triples `<< s p o >>` **parse** (via the
  `sparql-12` feature) but annotation evaluation is **not implemented**: the
  planner rejects quoted triples in subject/object position fail-loud.
- **Serialization** — SPARQL Results JSON, SPARQL Results XML (W3C shape),
  N-Triples, Turtle (currently an N-Triples subset). Content negotiation via the
  `Accept` header.
- **HTTP** — `POST /sparql`, `GET /sparql?query=…`, `POST /sparql/update`,
  `GET /sparql/health`, with a 1 MiB request-body limit.

## How it works

Five-stage pipeline: **parse** (`spargebra`) → **plan**
(`planner::plan_query` → `QueryPlan` of `TripleOp`s) → **execute**
(`executor::execute`, nested-loop join, then FILTER → ORDER BY → DISTINCT →
LIMIT/OFFSET) → **shape** (ASK boolean / CONSTRUCT+DESCRIBE graph assembly in
`engine.rs`) → **serialize** (`results.rs`).

Each triple pattern is planned into the cheapest storage access it can prove:
`SubjectLookup` (point read on the partition key) when the subject is bound,
`PredicateScan`/`FullScan` (range read) otherwise, `ObjectScan` (secondary index
on `object`, falling back to range scan) when only the object is bound, or
`PropertyPath` (BFS) for closure operators.

## Public API (key entry points)

| Area | Items |
|------|-------|
| Engine | `SparqlEngine::{new, execute, execute_update}`, `SparqlConfig`, `SparqlResult`, `ConstructedTriple` |
| HTTP | `start_sparql_http`, `build_router` (internal), `SparqlHttpConfig`, `AppState` |
| Planner | `plan_query`, `plan_where`, `QueryPlan`, `TripleOp`, `GraphQueryMode`, `OrderCondition` |
| Executor | `execute`, `execute_bindings`, `extract_subject_from_key`, `clustering_component` |
| Update | `execute_update`, `UpdateResult` |
| Triple store | `rdf_triples_schema`, `triples_table_id`, `partition_key`, `RdfTriple`, `ObjectType` |
| Results | `SparqlJsonResults`, `Binding`, `SparqlAskResult`, `ResultFormat` |
| Errors | `SparqlError` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-cluster`** — `write_path::WritePath` for all reads
  (`read`, `range_read`, `index_read`) on the query/path side.
- **`ferrosa-storage`** — `engine::StorageEngine` (`write`, `register_table`) for
  the UPDATE write/tombstone path; `TableId`.
- **`ferrosa-common`** — `CellValue`, `DecoratedKey`/`PartitionKey`, schema
  types (`TableSchema`, `ColumnDefinition`), error type.
- **`ferrosa-sstable`** — `Partition`, `Row`, `LivenessInfo`, `DeletionTime`
  (the storage row shapes it reads and writes).
- **`ferrosa-index`** — `IndexKey` for the `rdf_triples_object_idx` ObjectScan.
- **`ferrosa-schema`** — `Schema` carried in `AppState`.

External: `spargebra`, `sparesults`, `oxrdf` (all with `sparql-12`/`rdf-12`),
`axum`, `tokio`, `tower-http`, `serde`/`serde_json`, `serde_urlencoded`,
`base64` (declared, currently unused), `uuid`, `tracing`.

**Called by** (crates that depend on this):

- **`ferrosa`** — the main binary mounts the SPARQL HTTP endpoint.

## Tests

110 tests: 79 in-crate unit tests (executor 21, planner 14, property_path 11,
results 11, engine 8, filter 7, plus rdf_star/update/triple_store/namespace) and
31 integration tests (`sparql_m3_completeness` 14, `sparql_update_s02_mgmt` 11,
`sparql_update_pattern_delete` 6). Coverage gaps — OPTIONAL, real UNION
semantics, auth, property-path cost — are tracked in
[specs/fmea.md](specs/fmea.md).

## Specs

- [Architecture overview](specs/overview.md) — module map, data flow, invariants
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
