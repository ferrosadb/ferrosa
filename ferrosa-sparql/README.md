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
`PredicateScan`/`FullScan` (streaming range read) otherwise, `ObjectScan`
(secondary index on `object`, falling back to a streaming range scan) when only
the object is bound, or `PropertyPath` (BFS) for closure operators. The access
path is only a *pushdown*: every constant in the triple pattern is enforced
again when a row is bound, so choosing a path on one constant never discards
another.

### Scans stream, and the bound is real

Range scans pull **one partition at a time** from
`WritePath::range_read_stream_all` — the same streaming primitive the CQL path
uses — decode each row, filter it, and bind it immediately. No intermediate
`Vec` of fetched triples exists, so the memory a scan costs is one partition
rather than the table. `SELECT * WHERE { ?s ?p ?o }` no longer materializes the
whole triple store.

`SparqlConfig::max_rows` (default `100_000`) is the engine's single row bound.
It is enforced in two places, both real:

- **at the source** — storage rows read by a scan, counted as they arrive;
- **at each operator** — solutions buffered by a pattern, counted as appended.

Crossing either returns `SparqlError::Execution`. **Nothing is silently
truncated**: a clipped result reported as complete is indistinguishable from a
correct short one, which is worse than a failure. This replaces the former
`SCAN_ROW_CAP` constant, which only logged "scan results truncated at row cap"
*after* the whole table had already been materialized and truncated nothing, and
the former `SparqlConfig::max_results` field, which nothing read.

`LIMIT` is pushed **into** the scan — the scan stops after `offset + limit`
solutions — whenever that is provably safe: exactly one triple pattern, and no
`ORDER BY`, `DISTINCT`, `FILTER`, or `CONSTRUCT`/`DESCRIBE` above it. Those
operators either block (they must see every solution before emitting one) or
drop rows after binding (so stopping early would return too few), and in each
case the scan runs to completion under the bound instead. `ASK` is planned with
`LIMIT 1`, so it terminates on the first match.

### What is and is not bounded

| Path | Status |
|------|--------|
| Single-pattern scan (`FullScan`, `PredicateScan`, `ObjectScan` fallback) | **Streamed and bounded.** One partition resident; `max_rows` enforced at the source. |
| `SubjectLookup` point read | **Bounded** by one partition, by construction. |
| `LIMIT`/`OFFSET`/`ASK` over a single pattern | **Bounded by the query**; the scan stops at `offset + limit`. |
| `ORDER BY` / `DISTINCT` buffers | **Bounded, fail-loud.** They may buffer (both are blocking) but cannot exceed `max_rows`. |
| Property-path adjacency (BFS) | **Streamed read, bounded buffer.** BFS is blocking, so the adjacency list is buffered; past `max_rows` it errors rather than traversing a partial graph. |
| Multi-pattern join intermediates | **Bounded, NOT pipelined.** The nested-loop join still rebuilds a materialized solution set per pattern, so peak memory for a multi-pattern BGP is set by the intermediates, not by the scan. They are capped at `max_rows` and fail loud past it; pipelining the join is a separate redesign (FMEA SP-6). |

## Public API (key entry points)

| Area | Items |
|------|-------|
| Engine | `SparqlEngine::{new, execute, execute_update}`, `SparqlConfig`, `SparqlResult`, `ConstructedTriple` |
| HTTP | `start_sparql_http`, `build_router` (internal), `SparqlHttpConfig`, `AppState` |
| Planner | `plan_query`, `plan_where`, `QueryPlan`, `TripleOp`, `GraphQueryMode`, `OrderCondition` |
| Executor | `execute`, `execute_bindings`, `ExecutionLimits`, `DEFAULT_MAX_ROWS`, `extract_subject_from_key`, `clustering_component` |
| Update | `execute_update`, `UpdateResult` |
| Triple store | `rdf_triples_schema`, `triples_table_id`, `partition_key`, `RdfTriple`, `ObjectType` |
| Results | `SparqlJsonResults`, `Binding`, `SparqlAskResult`, `ResultFormat` |
| Errors | `SparqlError` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-cluster`** — `write_path::WritePath` for all reads on the
  query/path side: `read` (point), `index_read` (keyed), and
  `range_read_stream_all` (streaming scan). The `Vec`-returning `range_read` is
  **not** used and a unit tripwire (`sparql_scans_never_call_a_materializing_range_read`)
  fails the build if it is reintroduced.
- **`ferrosa-storage`** — `engine::StorageEngine` (`write`, `register_table`) for
  the UPDATE write/tombstone path; `TableId`.
- **`ferrosa-common`** — `CellValue`, `DecoratedKey`/`PartitionKey`, schema
  types (`TableSchema`, `ColumnDefinition`), error type.
- **`ferrosa-sstable`** — `Partition`, `Row`, `LivenessInfo`, `DeletionTime`
  (the storage row shapes it reads and writes).
- **`ferrosa-index`** — `IndexKey` for the `rdf_triples_object_idx` ObjectScan.
- **`ferrosa-schema`** — `Schema` carried in `AppState`.

External: `spargebra`, `sparesults`, `oxrdf` (all with `sparql-12`/`rdf-12`),
`axum`, `tokio`, `futures` (`StreamExt` over the partition stream), `tower-http`,
`serde`/`serde_json`, `serde_urlencoded`, `base64` (declared, currently unused),
`uuid`, `tracing`.

**Called by** (crates that depend on this):

- **`ferrosa`** — the main binary mounts the SPARQL HTTP endpoint.

## Tests

83 in-crate unit tests plus 5 integration suites.

**Invariant suites** — these drive `executor::execute` END TO END (a real
`QueryPlan` from the real planner, a real `WritePath` over a temp
`StorageEngine`, real rows written by `INSERT DATA`) and assert what MUST be
true of a SPARQL engine, rather than describing what the code currently does:

- `tests/sparql_executor_invariants.rs` — constant-term enforcement in every
  position, read-path agreement between a point lookup and a full scan, delete
  honesty, LIMIT/OFFSET totality over any `usize`, DISTINCT exactness.
  **4 of these currently FAIL** against t_af4eb9f0; see
  [specs/fmea.md](specs/fmea.md) SP-11.
- `tests/sparql_scan_bound_invariants.rs` (10) — completeness (complete result
  or an error, never a silent truncation) and boundedness (`LIMIT n` does not
  read the whole table), asserted without timing by setting `max_rows` below the
  table size so "did this read everything?" is directly observable.

**Behavioural suites**: `sparql_m3_completeness` (14),
`sparql_update_s02_mgmt` (11), `sparql_update_pattern_delete` (6).

Coverage gaps — OPTIONAL, real UNION semantics, auth, join cost — are tracked in
[specs/fmea.md](specs/fmea.md).

## Specs

- [Architecture overview](specs/overview.md) — module map, data flow, invariants
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
