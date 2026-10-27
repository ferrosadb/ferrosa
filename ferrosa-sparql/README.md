# ferrosa-sparql

> SPARQL Query + Update endpoint with bounded COUNT and SPARQL 1.2 JSON results, built on `spargebra`/`oxrdf`,
> translating algebra trees into reads and writes against a single CQL-backed
> `rdf_triples_v2` table.

## What this crate is

`ferrosa-sparql` is the W3C SPARQL front-end. It parses SPARQL with `spargebra`,
plans the algebra into storage operations against ferrosa's `rdf_triples_v2` table
(`((graph, subject), predicate, object, object_identity)`), executes them via the cluster write
path, and serializes results with hand-written JSON / XML / N-Triples / Turtle
encoders. The HTTP surface serves GET, form POST and direct POST over `axum`, and
serves HTTPS only when `SparqlHttpConfig` carries a certificate (`[sparql]
tls_cert/tls_key`); `require_tls` without one refuses to start (t_d5d122ba). The
rustls config comes from `ferrosa_net::tls`, the process's single crypto provider.

Each keyspace has one active v2 table. Its `(graph, subject)` partition key
separates the default graph (`default`) and named graphs. Named `GRAPH <g>` reads
and same-graph inserts/deletes are supported; variable graph names and cross-graph
`ADD`/`MOVE`/`COPY` requests remain unsupported. Existing v1 datasets are refused
until explicit offline migration completes, including empty registered v1 schemas.

## What's implemented

- **Query forms** — `SELECT`, `ASK`, `CONSTRUCT`, `DESCRIBE` (both
  `DESCRIBE <iri>` and `DESCRIBE ?var WHERE { … }`).
- **Graph-pattern coverage** — BGPs, `PROJECT`, `DISTINCT`/`REDUCED`,
  `SLICE` (LIMIT/OFFSET), `ORDER BY` (over arbitrary supported expressions),
  `FILTER`, `BIND`/`Extend`, explicit `Join`, `UNION` (concatenated), property paths. Multi-pattern joins run as a
  nested-loop join with binding-compatibility checks.
- **Property paths** — `+` (OneOrMore), `*` (ZeroOrMore), `?` (ZeroOrOne),
  reverse (`^`) over a single predicate, evaluated by in-memory BFS with cycle
  detection (`src/property_path.rs`).
- **FILTER** — equality/comparison, `&&`/`||`/`!`, `BOUND`, `sameTerm`, `IF`,
  arithmetic (`+ - * /`), and the functions `STR`, `UCASE`, `LCASE`, `STRLEN`,
  `ABS`, `CEIL`, `FLOOR`, `ROUND`. Unsupported FILTER sub-forms evaluate to Null.
- **Update** — `INSERT DATA`, `DELETE DATA`, `DELETE WHERE`,
  `DELETE/INSERT … WHERE`, `CLEAR`, `DROP`, `CREATE` (no-op). All deletes funnel
  through one tombstone primitive. `LOAD` and unsupported cross-graph ops fail loud. Constant key lengths are
  prevalidated across the request; instantiated pattern keys are validated before
  applying that operation. Storage failures are not transactionally rolled back.
- **COUNT / GROUP BY** — `COUNT(*)`, `COUNT(expr)`, DISTINCT and grouped counts,
  including an empty global input returning one zero row. The common single-triple
  scan feeds counters directly, retaining group keys and requested DISTINCT state,
  rather than collecting the input solution set. Joins and paths retain their
  existing bounded evaluator. Counts still read/decode input triples; this is not
  an O(1) metadata counter. Other aggregates, COUNT over UNION/OPTIONAL/subqueries
  and blocking input operators fail explicitly.
- **SPARQL 1.2 Query Results JSON** — typed recursive triple objects and directional
  language literals (`xml:lang`, `its:dir`). SELECT expressions support `TRIPLE`
  and triple-term constants. `Accept: application/sparql-results+json; version=1.2`
  selects a versioned result for SELECT or ASK. Unversioned JSON announces 1.2 only
  when its terms require it; query-version announcements do not force a result
  version. Explicit JSON 1.1 and legacy XML cannot silently flatten RDF 1.2 terms.
- **RDF storage boundary** — persisted RDF v2 still supports scalar RDF 1.1 terms.
  RDF 1.2 triple/directional storage and graph output are unimplemented and fail
  explicitly. This is result-format support, not full RDF/SPARQL 1.2 conformance.
- **Serialization / negotiation** — SELECT/ASK use Results JSON or XML; graph
  forms use N-Triples or Turtle (an N-Triples subset). Accept quality, q=0,
  wildcards and JSON media versions are honored; incompatible requests return
  406. Default graph output is `application/n-triples`, not a custom JSON array
  labelled as SPARQL Results JSON. RDF/XML and JSON-LD graph output are unavailable.
- **HTTP** — `POST /sparql`, `GET /sparql?query=…`, `POST /sparql/update`,
  `GET /sparql/health`, with a 1 MiB request-body limit. Query/update and version
  parameters have checked cardinality. Accepted transport operation-version
  parameters are 1.1/1.2; other parameter values fail with 400. The parser uses
  its SPARQL 1.2 grammar; strict version-specific language validation is not
  implemented. The service URI is the fallback base,
  with explicit BASE taking precedence. Protocol dataset overrides
  (`default-graph-uri`, `named-graph-uri`, `using-graph-uri`,
  `using-named-graph-uri`) are currently refused with 400 before updates; they are
  never ignored. Optional HTTP QUERY support is not implemented.

The protocol/JSON contracts are pinned to the W3C Working Drafts of
[23 July 2026](https://www.w3.org/TR/2026/WD-sparql12-protocol-20260723/) and
[13 August 2026](https://www.w3.org/TR/2026/WD-sparql12-results-json-20260813/).
These focused tests do not establish full W3C conformance.

The Rust `Binding` API is a typed enum. Use its constructors and `kind()`,
`scalar_value()`, `datatype()` and `lang()` accessors. Triple terms have no scalar
string value. This replaces direct access to the former scalar struct fields;
it does not change the persisted scalar RDF v2 identity encoding.

## How it works

Five-stage pipeline: **parse** (`spargebra`) → **plan**
(`planner::plan_query` → `QueryPlan` of storage operations and ordered `PlanStep`s) → **execute**
(`executor::execute`, algebra-ordered joins/FILTER/BIND, then ORDER BY → DISTINCT →
LIMIT/OFFSET) → **shape** (ASK boolean / CONSTRUCT+DESCRIBE graph assembly in
`engine.rs`) → **serialize** (`results.rs`).

Before parsing, `nesting::check_nesting` refuses query and update text whose
`(`/`{`/`[` nesting exceeds `MAX_NESTING` (64), skipping literals, IRIs and
comments as the lexer does. `spargebra` recurses per level with no limit, so
unbounded nesting overflowed the worker stack and aborted the process
(`tests/parser_depth.rs`).

Each triple pattern is planned into the cheapest storage access it can prove:
`SubjectLookup` (point read on the partition key) when the subject is bound,
`PredicateScan`/`FullScan` (streaming range read) otherwise, `ObjectScan`
(secondary index on `object`, falling back to a streaming range scan) when only
the object is bound, or `PropertyPath` (BFS) for closure operators. The access
path is only a *pushdown*: every constant in the triple pattern is enforced
again when a row is bound, so choosing a path on one constant never discards
another.

### BIND / Extend semantics

An empty BGP produces one empty solution. Each `BIND` evaluates against its
inner solutions; dependent bindings run in algebra order. FILTERs stay at their
algebra position, and explicit JOIN operands evaluate independently before
compatibility matching. This prevents an earlier BIND or a nested FILTER from
reading variables introduced later or outside its scope. SELECT and
`execute_bindings` (the UPDATE binding path) share this evaluation.

Constants and variable aliases retain their RDF term kind, lexical form,
datatype and language. Arithmetic currently supports signed 64-bit integer
operands: `+`, `-`, `*`, unary signs, and `/`. Integer results never pass through
floating point. Integer division produces `xsd:decimal` using integer long
division, truncated toward zero after at most 18 fractional digits. Unbound
operands, type errors, integer overflow and division by zero leave only the
BIND target unbound; the solution and its other bindings survive.

This is a bounded expression subset: other BIND expression forms fail during
planning, and decimal/float/double arithmetic operands fail explicitly during
execution. Literal constants and aliases of those datatypes still work. Existing
FILTER/ORDER BY expression limitations and OPTIONAL/UNION gaps remain; this does
not claim full SPARQL expression conformance.

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
`ORDER BY`, `DISTINCT`, `FILTER`, `COUNT`, or `CONSTRUCT`/`DESCRIBE` above it. Those
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
| Planner | `plan_query`, `plan_where`, `QueryPlan`, `PlanStep`, `TripleOp`, `GraphQueryMode`, `OrderCondition` |
| Executor | `execute`, `execute_bindings`, `ExecutionLimits`, `DEFAULT_MAX_ROWS`, `extract_subject_from_key`, `clustering_component` |
| Update | `execute_update`, `UpdateResult` |
| Triple store | `rdf_triples_schema`, `triples_table_id`, `canonical_term`, `checked_partition_key`, `encode_clustering`, `decode_v2_row`; migration input: `rdf_triples_v1_schema`, `decode_v1_row` |
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
- **`ferrosa-net`** — `tls::optional_server_config` builds the HTTPS config.

External: `spargebra`, `oxrdf` (both with `sparql-12`/`rdf-12`),
`axum`, `axum-server` (`tls-rustls-no-provider`), `tokio`, `futures` (`StreamExt` over the partition stream), `tower-http`,
`serde`/`serde_json`, `serde_urlencoded`, `base64` (declared, currently unused),
`uuid`, `tracing`.

**Called by** (crates that depend on this):

- **`ferrosa`** — the main binary mounts the SPARQL HTTP endpoint.

## Tests

Unit tests and storage-backed integration suites exercise full-term identity,
legacy survivor preservation, interrupted migration, recovery refusal and restart.

**Invariant suites** — these drive `executor::execute` END TO END (a real
`QueryPlan` from the real planner, a real `WritePath` over a temp
`StorageEngine`, real rows written by `INSERT DATA`) and assert what MUST be
true of a SPARQL engine, rather than describing what the code currently does:

- `tests/sparql_executor_invariants.rs` — constant-term enforcement in every
  position, read-path agreement between a point lookup and a full scan, delete
  honesty, LIMIT/OFFSET totality over any `usize`, DISTINCT exactness.
  See [specs/fmea.md](specs/fmea.md) for historical failure context.
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

## RDF term identity and offline migration (t_16b96d45)

Fresh datasets use v2. Object identity includes kind, original lexical form,
datatype IRI and normalized language. Plain literals equal xsd:string; numeric
lexical forms, unknown datatypes and ill-typed values are preserved. Keys use a
versioned, tagged, length-checked codec. Strict reads reject malformed identities
and key/cell disagreement. Lexical object-index hits are candidates and undergo
full-term matching. Full identities survive bindings, DISTINCT, property paths,
INSERT and all DELETE/CLEAR/DROP variants.

V1 schema/directory/set-aside evidence refuses serving and writes until an
explicit `migration::OfflineMigration` completes. The local Direct API requires
operator exclusion of all old/new readers, writers, migrators and lifecycle
operations throughout every pass. It recovers storage, checks durable recovery
admission, streams surviving terms into v2, validates index coverage and exact
survivor counts, and atomically publishes a bounded cutover marker. Interrupted
copies remain unservable and can resume without resetting data. Completed reruns
never recopy v1, preventing resurrection after v2 deletes. Source and target
incarnations bind the marker; legacy data remains retained. Pair/Cluster/degraded
migration and downgrade require separate rollout work; this API does not fence
other nodes. See [the cutover contract](specs/rdf-term-identity-cutover.md).

CLEAR/DROP validates a streaming preflight before a streaming delete pass. It
does not retain a table-sized vector of triples. This preserves refusal before
mutation for preflight validation/row-budget failures, without claiming atomicity
against concurrent writers or later storage failures.
