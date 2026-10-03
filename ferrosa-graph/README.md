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
- **Owned-handle entry point** (`executor/expand.rs`) — `execute_streaming_owned(plan,
  OwnedExecCtx)` returns `(columns, RowStream<'static>, QueryStats)`. Same
  dispatch as `execute_streaming()`, but the handles are owned
  (`ArcSwap::load_full()` instead of `load()`, `Schema::virtual_tables_arc()`
  instead of `virtual_tables()`, `Arc<Schema>` instead of `&Schema`), so the row
  stream can outlive the frame that started it — which is what a transport
  needs. `GraphEngine::execute_stream_with_params()` is the engine-level form;
  `execute_with_params()` is a `collect` over it.
- **Streamed HTTP response** (`http.rs`) — `POST /graph/query` writes the JSON
  body straight from the row stream. Nothing buffers the result server-side for
  a plan the executor streams. **Scope**: this bounds the *response*, not the
  query — phase A of `Expand` still materializes the frontier, and the
  buffering plan variants above still materialize, so a high-fan-out query can
  still exhaust memory. What it can no longer do is exhaust memory on a
  *low*-fan-out query: every storage SCAN in the crate now pulls one partition
  at a time (`range_read_stream_all`), so an anchor, hop, var-length or
  reconcile scan costs one partition rather than the whole table
  (t_bc5f0e6f; guarded by `tests/graph_range_read_memory_bound.rs`). The trailing `"stats"` object is built **after** the
  last row, so `execution_ms` now covers the projection too (a larger, more
  accurate number than the buffered path reported). A failure that surfaces
  after the first chunk **aborts the body**; the client sees a `200` with a
  truncated chunked transfer, never a `4xx`/`5xx`, because the status line is
  already on the wire. Bolt (`bolt/server.rs`) still buffers — it holds results
  in the connection struct across protocol messages and emits stats before rows.
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
- **Property write encoding** (`executor/expand.rs`) — schema-less CREATE/MERGE
  accept literal values only; a map, list or computed expression returns
  `GraphError::Validation` rather than storing empty bytes (FMEA G-11).
- **Aggregations** (`executor/aggregate.rs`) — `count`, `sum`, `avg`, `min`,
  `max`, `collect`, with `max_groups` / `max_collect_size` caps.
- **Adjacency index** (`adjacency/`) — `schema` (table layout + naming),
  `observer` (synchronous index maintenance), `reconcile` (background safety net).
- **SUBSCRIBE** (`executor/subscribe.rs`) — per-connection subscription registry
  with a tunable per-connection cap (`FERROSA_GRAPH_MAX_SUBSCRIPTIONS`, default 8).
- **Transports** — `http.rs` (axum, Basic auth, TLS, body-size limit, SSE for
  SUBSCRIBE) and `bolt/` (Bolt v5 handshake, PackStream codec, message dispatch,
  TLS / `bolt+s`). Both take their rustls config from `ferrosa_net::tls` (the
  single crypto provider; axum-server uses `tls-rustls-no-provider`), both refuse
  to start when `require_tls` is set without a certificate, and the
  disabled-engine stub serves over TLS when a certificate is configured
  (t_d5d122ba). The binary feeds both from `[graph] tls_cert/tls_key/require_tls`.

  SUBSCRIBE) and `bolt/` (Bolt v5 handshake, PackStream codec, message dispatch). The PackStream decoder bounds
  List/Map/Structure nesting at `codec::MAX_NESTING_DEPTH` (128) and rejects
  declared counts the input cannot satisfy, so a pre-auth HELLO cannot overflow
  the stack or force a huge allocation (`CodecError::NestingTooDeep`).
- **Cluster-aware DDL** — adjacency keyspace/table creation routes through the
  same `DdlPath` regular CQL `CREATE TABLE` uses, so every replica registers the
  system table (`ClusterGraphSchemaCoordinator`); a local coordinator is the
  single-node default.

## How it works

A query flows: **parse → bind params → validate + authorize → logical plan →
physical plan → execute**. Every query in a keyspace that touches edges first
awaits `GraphEngine::ensure_adjacency_ready`: once per process and keyspace it
lazily creates `system_graph_<ks>.adjacency`, registers the
`AdjacencyIndexObserver`, (if configured) starts the background reconciliation
loop, and runs one complete synchronous reconcile pass — the **heal** — before
any traversal reads the index. Concurrent first queries wait on the same heal
(a `tokio::sync::OnceCell` per keyspace). A heal with any failed read, scan or
write is not marked done: the query fails with the retryable
`GraphError::Unavailable` (HTTP 503) and the next query heals again.

The **adjacency-consistency invariant** is the heart of the crate: every edge
write must produce the matching OUT and IN adjacency entries. This is enforced
**synchronously** — `AdjacencyIndexObserver` is a `WriteObserver` running in
`ObserverMode::Sync`, so its derived adjacency mutations are applied in the same
write as the edge row, not asynchronously. The background **reconciler** is the
explicit, observable fallback: it scans edge tables to repair missing entries
and scans the adjacency index to tombstone orphans, covering dropped-mutation
and crash-recovery gaps. See [specs/data-flow.md](specs/data-flow.md).

Deletion follows the same invariant. Deleting an edge tombstones its OUT and
IN entries; the observer derives tombstones from an edge tombstone, and
`DELETE r` writes them explicitly, as MERGE writes the live entries. Reads
return row tombstones, so every traversal goes through
`traversable_neighbor_id`, which skips a deleted entry
(`adjacency::schema::row_is_deleted`) and the entries of the other direction.
The reconciler derives expected entries with the observer's column
extraction, and judges each entry on every edge row of the partition that
derives it: several edges can share one entry (typed_edges keeps one row per
`edge_type` between a pair), so the entry stays live while any of them is.
It writes a missing or tombstoned entry of a live edge back at the edge's own
write time, or one microsecond past the tombstone shadowing it — never at
"now", so a delete that lands during the pass still wins — and tombstones a
live entry whose edges are all deleted. `DELETE r` likewise writes back the
entries a surviving sibling in the same partition still derives. Orphans
with no edge row at all are removed only from edge tables keyed exactly
(`graph.source`) / (`graph.target`); for any other layout (agent_memory's
`typed_edges`) an entry does not name the edge's key, so it cannot be
point-checked. Edge and vertex row readers skip row tombstones
(`row_is_deleted`), so `count(r)` and edge binding never see a deleted row in
a partition that still holds live ones.

### Deploy notes: first start after the traversal fix (330a0c29)

Builds before 330a0c29 ran a reconcile that tombstoned **every** adjacency
entry of `agent_memory.typed_edges` on every pass (its orphan check read the
edge at the raw-key position, which a composite-key table does not use), and
left live entries behind for edges they deleted. Those builds ignored
tombstones, so the damage was invisible. From 330a0c29 on, traversals honour
tombstones, so on a node carrying that damage every typed edge would vanish
from traversals until the entries were rebuilt.

What happens on the first start of this build:

1. Nothing at boot. The heal runs on the first graph query that needs the
   adjacency index in `agent_memory` (ferrosa-memory's first `TYPED_EDGE`
   lookup), on each node independently, once per process.
2. That query, and every adjacency query arriving while it runs, waits for
   one complete reconcile of every `agent_memory` edge table. Queries that
   need no adjacency (plain `RETURN`, vertex lookups) are not held.
3. The pass rewrites every entry to every replica (below), and logs one WARN
   line: `graph engine: adjacency heal repaired the index before serving
   traversals` with `entries_repaired` (tombstoned or missing here),
   `entries_removed` (live here for a deleted edge), `entries_rewritten`
   (already right here) and `elapsed_ms`. On the first start after the
   deploy expect `entries_repaired` near twice the live typed-edge count.
4. If any read or write failed, the query gets HTTP 503 / Bolt
   `Neo.TransientError.General.DatabaseUnavailable` with "being repaired;
   retry", the WARN line says `adjacency heal incomplete`, and the next query
   heals again. No partial answer is returned as complete.

**The heal rewrites every entry, on every replica.** Each node's pre-fix
background pass damaged its own replica at its own time, and replica reads do
not ship row tombstones, so a node cannot see another replica's damage. The
heal (`ReconcileMode::Rewrite`) therefore writes every entry the edge tables
imply — live entries for live edges, tombstones for deleted ones — under the
adjacency keyspace's own replication at CL ALL, stamped at the pass start
(later than every pre-fix write, earlier than any client write during the
pass). It reads only to count what was damaged. A replica that is down fails
the heal, and queries are refused retryably until it is back. Do not start
the new build while another node is down.

**Time at production scale.** Measured by
`slow::deploy_heal_at_memory_cluster_scale` (release build, single node,
production commit-log sync) on the memory cluster's shape: 102,780 entities
and 21,000 typed edges (20,055 live), every live entry tombstoned by main's
pre-fix reconcile. The first query, heal included, answered in **337–466 ms**
(three runs), and a second pass found nothing to repair (156–204 ms). In
cluster mode each entry is one CL ALL write fanned out to the three
replicas, so expect several times that, still seconds rather than minutes;
the WARN line's `elapsed_ms` reports the real figure. Run it with:

```bash
cargo test --release -p ferrosa-graph --features slow-tests \
  --test adjacency_deploy_heal -- slow:: --nocapture
```

The heal runs on every process start, not only the first after the deploy.
Restarts therefore pay the same cost, and a node never serves traversals
from an index it has not reconciled in this process.

**Verify on one node** before rolling the others:

```bash
# 1. Restart the node on the new build, then send one traversal:
curl -s -u "$FERROSA_USER:$FERROSA_PASS" http://127.0.0.1:7474/graph/query \
  -H 'content-type: application/json' \
  -d '{"keyspace":"agent_memory","query":"MATCH (a:Entity)-[r:TYPED_EDGE]->(b:Entity) RETURN count(r)"}'
# 2. The heal's WARN line names what it repaired:
#    (the node's log: StandardErrorPath in its launchd plist)
grep 'adjacency heal' "$NODE_LOG"
# 3. Metrics: repaired > 0 once, heals_completed 1, heals_failed 0, errors 0.
curl -s http://127.0.0.1:9090/metrics | grep ferrosa_graph_adjacency_
# 4. Answers: ferrosa-memory's related-entity lookup for a known entity
#    returns its typed neighbours again (empty before the heal finished).
```

`ferrosa_graph_adjacency_heals_failed_total` above 0 means a heal hit errors
and queries were refused until a later one completed; check the WARN line's
`errors` count and the storage logs. Roll the remaining nodes one at a time;
each heals its own index on its own first query.

Not repaired, and harmless to traversals: the pre-fix reconcile's phase 1 also
wrote junk `typed_edges` entries keyed by raw composite key bytes, which no
vertex id matches. The current reconcile cannot point-check that layout, so
they stay until removed separately (t_f3d48248).

Known gap, not part of the deploy hazard (t_9049eab1): a hop binds one edge
row per adjacency entry, so ferrosa-memory's `list_typed_edges_to` returns one
`edge_type` of a pair linked by two, and without `edge_type` in the pattern it
scans the whole edge table per neighbour (24 s for one vertex at the scale
above).

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

- **`ferrosa-cluster`** — `WritePath` (read / `range_read_stream_all` / write;
  the materializing `range_read` is deliberately unused here), `DdlPath` +
  `DdlOperation` for replicated adjacency DDL, `ConsistencyLevel`,
  `ReplicationStrategy`, `ClusterError`.
- **`ferrosa-common`** — `DecoratedKey`, `PartitionKey`, `CellValue`, `Error`.
- **`ferrosa-net`** — `tls::optional_server_config` / `crypto_provider` build
  the graph HTTP and Bolt TLS configs (production path). The integration test
  harness (`tests/graph_http_integration.rs`) also uses its internode types
  (`PeerManager`, `RpcServer`, `Message`).
- **`ferrosa-schema`** — `Schema`, `SchemaSnapshot`, `TableMetadata`,
  `AuthContext`, `check_permission`, `VirtualTableRegistry`.
- **`ferrosa-sstable`** — `Partition`, `Row`, `CellValue`, `LivenessInfo`,
  `DeletionTime` (the storage row shapes it reads and builds).
- **`ferrosa-storage`** — `StorageEngine`, `Mutation`, `TableId`,
  `WriteObserver` / `ObserverMode` (the observer hook).

External: `axum`/`axum-server` (`tls-rustls-no-provider`), `rustls`, `tokio-rustls`, `tokio`, `serde`/`serde_json`, `arc-swap`,
`parking_lot`, `indexmap`, `phf`, `blake3`, `uuid`, `base64`, `hex`, `chrono`.

**Called by** (crates that depend on this):

- **`ferrosa`** — the main binary wires the `GraphEngine`, HTTP endpoint, and
  Bolt server alongside the CQL listener.

## Tests

353 in-crate unit/`tokio` tests plus four integration suites under `tests/`
(`adjacency_replication.rs`, `graph_http_integration.rs`, `parser_proptest.rs`,
`listener_tls.rs` — graph HTTP and Bolt over TLS, plaintext refused,
`require_tls` without a certificate refuses to start), and
`adjacency_deploy_heal.rs` — the index damage left by the pre-330a0c29
reconcile, reproduced with main's own reconcile code, healed before the first
traversal answers, on one node and across two replicas; its
`slow::deploy_heal_at_memory_cluster_scale` (feature `slow-tests`) measures
the heal at the memory cluster's size.
No `#[ignore]`, no `TODO`/`FIXME` markers in source. Highest coverage:
`parser/parse_impl.rs` (81), `executor/eval.rs` (47), `executor/expand.rs` (44).

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
- [Data flow](specs/data-flow.md) — MATCH expand + adjacency-consistent write
