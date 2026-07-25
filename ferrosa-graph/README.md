# ferrosa-graph

> The property-graph query engine for ferrosa: a Cypher endpoint over data
> stored in ordinary CQL tables, kept traversable by a system-managed
> adjacency index.

## What this crate is

`ferrosa-graph` lets a ferrosa keyspace be queried as a property graph without
a separate graph store. Vertices and edges live in **regular CQL tables** tagged
with `extensions["graph.type"] = "vertex" | "edge"` (plus `graph.label`,
`graph.source`, `graph.target`). Topology is made traversable by a
**per-keyspace adjacency index** — a system table `system_graph_<ks>.adjacency`
that the engine creates lazily and keeps consistent with the edge tables.

The crate parses Cypher, validates + authorizes it against the schema, plans it
into a physical traversal plan, and executes that plan against the storage
engine through the cluster write path. It exposes the same query surface over
three transports: an **HTTP/JSON** endpoint (port 7474), the **Bolt v5** wire
protocol (port 7687, Neo4j-driver compatible), and direct in-process calls from
the `ferrosa` binary.

`GraphHttpConfig` and `BoltConfig` default to loopback-only binds
(`127.0.0.1:7474` and `127.0.0.1:7687`). The `ferrosa` binary resolves the
runtime graph settings with `[graph]` TOML values taking precedence over the
matching environment variables, then supplies these config objects to the HTTP
and Bolt servers. Bolt uses the resolved Graph HTTP host with its separately
resolved port.

## What's implemented

- **Cypher parser** — lexer + recursive-descent parser (`parser/`) covering
  `MATCH` / `OPTIONAL MATCH`, `WHERE`, `WITH` pipelines, `RETURN` (DISTINCT,
  `ORDER BY`, `LIMIT`), `CREATE` / `SET` / `REMOVE` / `DELETE` / `DETACH DELETE`,
  `MERGE`, `UNWIND`, `UNION [ALL]`, `FOREACH`, correlated `CALL {}` subqueries,
  `SUBSCRIBE` / `UNSUBSCRIBE`, variable-length paths `[*min..max]`, pattern
  predicates/comprehensions, list comprehensions, and map projections.
- **Logical planner** (`planner/logical.rs`) — resolves Cypher labels to tables
  via `graph.label` extensions (case-insensitive), validates property refs, and
  performs **per-statement authorization** (`check_permission`: `Select` for
  reads, `Modify` for writes) against the `AuthContext` (threat T3).
- **Physical planner** (`planner/physical.rs`) — anchor selection + `Expand` /
  `ExpandVarLength` / `Create` / `Merge` / `Subscribe` / `Union` plans.
- **Expand executor** (`executor/expand.rs`) — anchor lookup, per-hop adjacency
  reads, property evaluation, aggregation, write clauses; honors DoS limits
  (`max_fan_out_per_hop`, `max_result_rows`, `query_timeout`).
- **Streaming entry point** (`executor/stream.rs`, `executor/expand.rs`) —
  `execute_streaming()` returns `(columns, RowStream<'a>, QueryStats)`;
  `execute()` is a thin `collect` over it, so there is one executor, not two.
  Streaming today: `Subscribe`, `Union { all: true }` (via `chain_streams`),
  `ReturnOnly`, and the **Expand projection** — one `project_state` per pull, so
  `LIMIT k` projects k states instead of projecting everything and truncating.
  `DISTINCT` composes as `dedup_stream`. SET/REMOVE consume their inner expand
  as a stream (their own output is one summary row, so it cannot stream). Every
  other variant computes the buffered `GraphResult` and is wrapped with
  `stream_from_rows`. `UNION` without `ALL` (whole-result dedup), `ORDER BY`
  (pipeline breaker), `DELETE` (two passes over the matched rows — validate,
  then tombstone), `Aggregate`, `WcoJoin`, `ExpandVarLength` and the
  virtual-table anchor are deliberately excluded. The hop loop is still fully
  materializing. See `specs/streaming-executor-design.md` §5.
- **`RETURN DISTINCT` ordering** — `DISTINCT` **without** an `ORDER BY` returns
  rows in **first-seen (expansion) order**, not sorted order. This changed
  deliberately when DISTINCT became a streaming dedup; earlier releases returned
  string-repr sorted rows. The set of rows is the same. Add an explicit
  `ORDER BY` if you need a particular order. `DISTINCT` on a variable-length
  path (`varpath.rs`) still returns sorted order. The dedup set is **unbounded**
  in memory — a high-cardinality `DISTINCT` can still exhaust it.
- **Label-agnostic expansion** (`executor/expand.rs`) — traversals may omit the
  relationship type and/or the target-node label (`(a)-[r]->(n)`, `(a)<-[r]-(n)`,
  `-[r:T]->(n)`, `-[r]->(n:L)`). When a hop lacks a plan-time edge or vertex
  table, the executor resolves it **per adjacency row**: the edge from the row's
  recorded `edge_table`, and the opposite vertex from that edge's
  `graph.source_label` / `graph.target_label` (outgoing → target, incoming →
  source). The neighbor node and relationship hydrate with real properties, just
  like a typed traversal. Requires the **edge-table endpoint-label contract**
  (below); a resolution failure is loud (`400`), never a null endpoint.
- **Variable-length paths** (`executor/varpath.rs`, `leapfrog.rs`) — BFS over
  `min..=max` hops with a visited set for cycle detection and a
  `max_var_path_visited` vertex budget (threat T13).
- **Aggregations** (`executor/aggregate.rs`) — `count`, `sum`, `avg`, `min`,
  `max`, `collect`, with `max_groups` / `max_collect_size` caps.
- **Adjacency index** (`adjacency/`) — `schema` (table layout + naming),
  `observer` (synchronous index maintenance), `reconcile` (background safety net).
- **SUBSCRIBE** (`executor/subscribe.rs`) — per-connection subscription registry
  with a tunable per-connection cap (`FERROSA_GRAPH_MAX_SUBSCRIPTIONS`, default 8).
- **Transports** — `http.rs` (axum, Basic auth, TLS, body-size limit, SSE for
  SUBSCRIBE) and `bolt/` (Bolt v5 handshake, PackStream codec, message dispatch).
- **Cluster-aware DDL** — adjacency keyspace/table creation routes through the
  same `DdlPath` regular CQL `CREATE TABLE` uses, so every replica registers the
  system table (`ClusterGraphSchemaCoordinator`); a local coordinator is the
  single-node default.

## How it works

A query flows: **parse → bind params → validate + authorize → logical plan →
physical plan → execute**. On the first query in a keyspace that touches edges,
`ensure_adjacency_storage_for_keyspace` lazily creates
`system_graph_<ks>.adjacency`, registers the `AdjacencyIndexObserver`, runs one
synchronous reconcile pass, and (if configured) starts the background
reconciliation loop.

The **adjacency-consistency invariant** is the heart of the crate: every edge
write must produce the matching OUT and IN adjacency entries. This is enforced
**synchronously** — `AdjacencyIndexObserver` is a `WriteObserver` running in
`ObserverMode::Sync`, so its derived adjacency mutations are applied in the same
write as the edge row, not asynchronously. The background **reconciler** is the
explicit, observable fallback: it scans edge tables to repair missing entries
and scans the adjacency index to tombstone orphans, covering dropped-mutation
and crash-recovery gaps. See [specs/data-flow.md](specs/data-flow.md).

### Edge-table endpoint-label contract

A graph **edge** table (`graph.type = edge`) must declare, besides its endpoint
*columns* (`graph.source` / `graph.target`), its endpoint *labels*
`graph.source_label` / `graph.target_label`, each naming an existing vertex
table's `graph.label`. This is enforced at DDL time by `ferrosa-schema`
(`registry.rs`) — creating an edge table without valid endpoint labels is
rejected — so the metadata label-agnostic expansion relies on is guaranteed
present. Typed traversals resolve the opposite vertex from the *query's* node
label; label-agnostic traversals resolve it from these *edge-table* labels.
Should an edge ever lack them (e.g. legacy data, or the referenced vertex table
was dropped), a label-agnostic expansion **fails loud** with a `400` naming the
edge and the missing key rather than returning a null endpoint.

## Public API (key entry points)

| Area | Item |
|------|------|
| Engine | `GraphEngine::new` / `new_with_coordinator`, `execute[_with_params]`, `explain`, `execute_subscribe`, `graph_schema`, `shutdown` |
| Config | `GraphConfig`, `GraphEngineConfig` (DoS limits), `GraphHttpConfig`, `BoltConfig` |
| DDL routing | `GraphSchemaCoordinator` (+ `Local` / `Cluster` impls) |
| Adjacency | `adjacency_keyspace_name`, `adjacency_table_metadata`, `AdjacencyIndexObserver`, `reconcile_once`, `spawn_reconciliation` |
| HTTP | `http::router`, `GraphHttpConfig` |
| Bolt | `bolt::server::start_bolt_server`, `BoltConfig` |
| Errors | `GraphError`, `Result` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-cluster`** — `WritePath` (read/range_read/write), `DdlPath` +
  `DdlOperation` for replicated adjacency DDL, `ConsistencyLevel`,
  `ReplicationStrategy`, `ClusterError`.
- **`ferrosa-common`** — `DecoratedKey`, `PartitionKey`, `CellValue`, `Error`.
- **`ferrosa-net`** — internode protocol types (`PeerManager`, `RpcServer`,
  `Message`). **Used only by the integration test harness**
  (`tests/graph_http_integration.rs`); it is a `[dev-dependencies]` entry, not a
  production code path of this crate.
- **`ferrosa-schema`** — `Schema`, `SchemaSnapshot`, `TableMetadata`,
  `AuthContext`, `check_permission`, `VirtualTableRegistry`.
- **`ferrosa-sstable`** — `Partition`, `Row`, `CellValue`, `LivenessInfo`,
  `DeletionTime` (the storage row shapes it reads and builds).
- **`ferrosa-storage`** — `StorageEngine`, `Mutation`, `TableId`,
  `WriteObserver` / `ObserverMode` (the observer hook).

External: `axum`/`axum-server`, `tokio`, `serde`/`serde_json`, `arc-swap`,
`parking_lot`, `indexmap`, `phf`, `blake3`, `uuid`, `base64`, `hex`, `chrono`.

**Called by** (crates that depend on this):

- **`ferrosa`** — the main binary wires the `GraphEngine`, HTTP endpoint, and
  Bolt server alongside the CQL listener.

## Tests

353 in-crate unit/`tokio` tests plus three integration suites under `tests/`
(`adjacency_replication.rs`, `graph_http_integration.rs`, `parser_proptest.rs`).
No `#[ignore]`, no `TODO`/`FIXME` markers in source. Highest coverage:
`parser/parse_impl.rs` (81), `executor/eval.rs` (47), `executor/expand.rs` (44).

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
- [Data flow](specs/data-flow.md) — MATCH expand + adjacency-consistent write
