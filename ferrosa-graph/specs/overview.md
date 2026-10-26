---
crate: ferrosa-graph
status: implemented
last_updated: 2026-10-15
executive_summary: >
  The property-graph query engine for ferrosa. Parses Cypher, authorizes it
  against the schema, plans and executes traversals over data stored in ordinary
  CQL tables tagged graph.type=vertex|edge, and keeps a per-keyspace adjacency
  index (system_graph_<ks>.adjacency) consistent with the edge tables via a
  synchronous WriteObserver, with a background reconciler as the fallback. Served
  over HTTP/JSON (7474) and Bolt v5 (7687). Its transport config defaults to
  loopback-only binds; the ferrosa binary owns TOML/environment precedence.
---

# ferrosa-graph — Architecture Overview

> Updated 2026-10-15 against `origin/main` (commit `9e32e82a`, release v0.22.3).
> Documented the streaming query entry points (`execute_stream_with_params` →
> `execute_statement_streaming` → `execute_streaming_owned`/`execute_streaming`),
> streaming aggregation, and external-sort spill; refreshed the module line
> counts. See [data-flow.md](data-flow.md) for the full sequence.

## Purpose & boundary

Native-primary-key equality uses the keyed reader. Bolt's first access to an
authorized table automatically declares missing scalar BTree indexes through
`GraphSchemaCoordinator` and waits for backfill. `engine::bolt_indexes` memoizes
setup per table, using weak immutable snapshot/path identity; ordinary connections
reuse the same cache. Setup is performed before leaf writes, including buffered
UNWIND/FOREACH/CALL execution. Snapshot replacement invalidates it even when the
schema UUID stays unchanged. Cold cluster probes observe each current source
before publishing setup; subsequent reads retain fail-closed readiness checks.

The scalar reader seeks the complete finite set of encodings matching the
existing evaluator: integer/UUID, text/hex, boolean and bounded floating-point
aliases. Dense epsilon matches, unsupported predicates or unready indexes retain
the range reader. Candidates retain all predicate checks. Explicit Cypher index
DDL remains unsupported. Simple unfiltered vertex counts use row metadata without
materializing payloads; other aggregate shapes retain the general evaluator.
See the [measurement status](../../deploy/fly-bench/pipeline/RESULTS.md) and
[experiment record](../../deploy/fly-bench/pipeline/HISTORY.md).

`ferrosa-graph` turns a ferrosa keyspace into a queryable property graph without
a dedicated graph store. There is no separate graph storage engine: vertices and
edges are **plain CQL tables** distinguished only by schema extensions
(`graph.type`, `graph.label`, `graph.source`, `graph.target`). The single piece
of graph-specific persisted state is the **adjacency index**, a system table
`system_graph_<keyspace>.adjacency` that makes neighbor lookups O(adjacency-read)
instead of a full edge-table scan.

The boundary: this crate owns Cypher (parse/plan/execute), the adjacency-index
contract, and the two graph transports. It does **not** own storage durability,
replication, consensus, or the CQL value codec — it delegates all reads and
writes to `ferrosa-cluster`'s `WritePath` and the `ferrosa-storage`
`StorageEngine`, and all DDL to the same `DdlPath` regular CQL uses.

## Transport configuration

`GraphHttpConfig` defaults to `127.0.0.1:7474` and `BoltConfig` defaults to
`127.0.0.1:7687`. The `ferrosa` composition root resolves `[graph].bind` and
`[graph].bolt_port`: TOML takes precedence over the matching environment
variables, then the loopback defaults. It passes the resolved HTTP config to
this crate and constructs Bolt with the resolved Graph HTTP host plus the
resolved Bolt port.

## Module map

| Module | Responsibility |
|--------|----------------|
| `parser` (`lexer`, `token`, `ast`, `parse_impl`, ~4.5k LoC) | Cypher lexing + recursive-descent parse to `Statement` AST |
| `planner::logical` (~610 LoC) | label→table resolution (`resolve_label`, `graph.label`, case-insensitive; logical.rs:66), property validation, **per-statement authorization** (`check_permission`, logical.rs:110) |
| `planner::physical` (~2.7k LoC) | anchor selection; `plan` entry (physical.rs:282) and `plan_match` (physical.rs:1349); `Expand` / `ExpandVarLength` / `Aggregate` (physical.rs:179) / `WcoJoin` / `MergeUpsert` / `Create` / `Subscribe` / `Union` plans |
| `executor::expand` (~9.8k LoC) | streaming dispatch hub: `execute_streaming` (expand.rs:207) / `execute_streaming_owned` (expand.rs:280) → `execute_streaming_inner` (expand.rs:371); anchor lookup, per-hop adjacency reads, eval, aggregation, write clauses, DoS limits |
| `executor::aggregate` | incremental `Accumulator` framework (aggregate.rs:11) for `count`/`sum`/`avg`/`min`/`max`/`collect` — folds row by row, no materialization of the input |
| `executor::spill` | external-sort spill for `ORDER BY` (+ `LIMIT`): streams the smallest rows in order, bounds sort memory (spill.rs:303) |
| `executor::varpath` + `leapfrog` | variable-length `[*min..max]` BFS with visited-set + vertex budget |
| `executor::eval` | expression evaluation, WHERE filtering, partition→JSON |
| `executor::subscribe` | per-connection `SubscriptionRegistry` (cap enforcement) |
| `adjacency::schema` | adjacency table metadata + keyspace naming (`system_graph_<ks>`, schema.rs:49) + direction constants |
| `adjacency::observer` | `AdjacencyIndexObserver` — synchronous (`ObserverMode::Sync`, observer.rs:42) index maintenance on edge writes |
| `adjacency::reconcile` | background safety-net scan (repair missing, tombstone orphans) |
| `engine` (~3.6k LoC) | composition root: `execute_with_params` (engine.rs:929) / `execute_stream_with_params` (engine.rs:968) → `execute_statement_streaming` (engine.rs:1016); orchestrates parse→plan→exec, lazy adjacency setup, FOREACH / CALL {} expansion, DDL coordinators |
| `http` | axum HTTP/JSON endpoint (POST `/graph/query`, http.rs:1002), Basic auth, TLS, body limit, streamed JSON response body (http.rs:660), SSE for SUBSCRIBE |
| `bolt` | Bolt v5 handshake, PackStream codec (nesting depth capped at 128, counts validated against remaining input), message dispatch, TCP server |

## Data flow (summary)

**Query path:** a transport calls `GraphEngine::execute_stream_with_params`
(engine.rs:968). It binds params, then expands the two orchestration forms
first — `FOREACH` (engine.rs:976) and `CALL {}` (engine.rs:982) — each
materialized. A single statement then runs through `execute_statement_streaming`
(engine.rs:1016): lazy adjacency setup when the statement needs it
(`statement_requires_adjacency` / `ensure_adjacency_ready`, engine.rs:1022) →
`validate` (resolve labels + authorize, planner/logical.rs:145) → `plan`
(planner/physical.rs:282) → `execute_streaming_owned` (executor/expand.rs:280),
which calls straight through to `execute_streaming` → `execute_streaming_inner`
(executor/expand.rs:207, 371) — one dispatch, no second executor. The result is
`(columns, RowStream<'static>, QueryStats)`; the HTTP transport writes the row
stream straight into the response body (`Body::from_stream`, http.rs:660) and
Bolt streams records the same way. Anchor partitions are **streamed** via
`WritePath::range_read_stream_all` — one partition per pull, never the whole
table at once (t_bc5f0e6f); each hop reads `system_graph_<ks>.adjacency` to find
neighbors. **Aggregation is incremental:** `execute_aggregate`
(executor/expand.rs:6046) streams its inner plan and folds each row into per-group
`Accumulator`s, whose memory is proportional to groups (`config.max_groups`).
An inner expansion can still materialize its frontier. Eligible unfiltered
vertex counts bypass that expansion and call `WritePath::count_range_with` at
CL ONE with the keyspace replication strategy. `ORDER BY` (with
optional `LIMIT`) spills to disk through `executor::spill` when the sort would
exceed the memory budget.

**Write path:** a `CREATE`/`MERGE`/`SET`/`DELETE` plan writes the edge/vertex row
through `WritePath`. Because the `AdjacencyIndexObserver` is registered on the
edge table as a `WriteObserver(Sync)`, the storage engine applies its derived
OUT+IN adjacency mutations in the **same** write — so the index never lags a
committed edge under normal operation. See [data-flow.md](data-flow.md) for the
full sequence.

## Key invariants

**MERGE endpoint identity.** Within each path, bind the source and destination
nodes before the edge. Bound raw keys take precedence over fallback derivation;
UUID fallback keys are 16 bytes. Preserve existing raw identities and explicit,
scoped and composite encodings rather than rehashing an endpoint's properties.

1. **Adjacency-consistency invariant.** For every live edge `(src)-[label]->(tgt)`
   there exist exactly two adjacency rows: an OUT row under `src` and an IN row
   under `tgt`, both keyed by `(direction, edge_label, neighbor_id)`. This is
   maintained **synchronously** by `AdjacencyIndexObserver` (`ObserverMode::Sync`),
   so the index is consistent with a committed edge write, not eventually.
2. **Reconciler is the explicit fallback, not the primary path.** The background
   reconciler exists only to repair gaps the synchronous observer cannot cover
   (dropped mutations under backpressure, crash-recovery windows). It is
   observable (logs repaired/orphan counts) and idempotent. It is a safety net,
   never the source of truth — fail-loud philosophy: the observer is expected to
   keep the index correct; the reconciler quantifies and closes residual drift.
   **Write values are never silently emptied.** CREATE/MERGE property values
   that are not literals (maps, lists, computed expressions) return a typed
   `GraphError::Validation` on the schema-less path instead of storing empty
   bytes (FMEA G-11); jsonb storage of maps/lists arrives with D12.
3. **Adjacency clustering wire format is fixed.** Each clustering component is
   `[u16 BE length][bytes]`: `(direction:1B, edge_label:text, neighbor_id:blob)`.
   The SSTable writer's composite parser (`validate_clustering_shape`) rejects any
   other layout, so observer + reconciler + expand executor must agree byte-for-byte.
4. **Adjacency DDL goes through the cluster path.** Creating the adjacency
   keyspace/table uses `DdlPath` (the same path as CQL `CREATE TABLE`) via
   `GraphSchemaCoordinator`, so every replica registers the system table. Bypassing
   it (direct `Schema::create_*_internal`) would leave replicas unable to accept
   forwarded writes against the adjacency table.
5. **Authorization at the logical layer.** `validate` calls `check_permission`
   (Select for reads, Modify for writes) before planning — no plan executes for an
   unauthorized statement (threat T3). Observer/reconciler writes are internal
   system writes and run with system authority, not the caller's.
6. **Edge tables declare their endpoint labels.** A `graph.type = edge` table
   must carry `graph.source_label` / `graph.target_label`, each referencing an
   existing vertex table, enforced at DDL time by `ferrosa-schema`. This is the
   contract that lets **label-agnostic** expansion (`(a)-[r]->(n)` with the edge
   and/or target-node label omitted) hydrate the opposite endpoint: when a hop
   has no plan-time edge/vertex table, the expand executor resolves the edge from
   the adjacency row's recorded `edge_table` and the opposite vertex from that
   edge's endpoint label (outgoing → target, incoming → source), then hydrates
   via the same path as a typed traversal. Missing/unresolvable endpoint metadata
   is **fail-loud** (a `400`, naming the edge + missing key), never a null
   endpoint — so a mis-declared edge is exposed, not silently empty.

## Measured query-path behaviour

Measurements taken on a local development cluster on 2026-10-15 (v0.22.3,
`9e32e82a`) — stated as observed results, not as mechanism:

- `MATCH (n:Entity) RETURN count(n)` returned 105,873 in 5.88 s.
- `MATCH (n:Fold) RETURN count(n)` returned 1,784 in 0.17 s.
- An unlabelled `MATCH (n) RETURN count(n)` is rejected immediately as invalid.

Cost is roughly linear per returned row, so the dominant cost sits in the
scan/hydration path rather than in the aggregate node — consistent with
`execute_aggregate` folding rows incrementally with memory proportional to group
count, not input size (executor/expand.rs:6046). The *cause* of the per-row scan
cost is not established by these numbers; see Open Questions.

## Open Questions

- [ ] What dominates the per-row cost in the scan/hydration path (≈5.88 s for
  105,873 rows vs ≈0.17 s for 1,784)? The measurement shows linear per-row cost
  but does not attribute it — a profile of the anchor-scan/hydration path is
  required before naming a mechanism.

## Position in the dependency graph

Sits near the top of the stack: depends on `ferrosa-cluster`, `ferrosa-storage`,
`ferrosa-schema`, `ferrosa-sstable`, `ferrosa-common` (and `ferrosa-net` for the
integration test harness only). Depended on by the `ferrosa` binary, which mounts
its HTTP + Bolt servers. See the [root crate index](../../specs/crates.md) for the
full graph.

### Cell payload boundary

Graph column extraction retains immutable `CellBytes` handles for regular
cells. Adjacency repair carries those handles into new cells; visited traversal
keys remain independently owned at the key boundary.


### First-write scalar columns over Bolt

Before planning a Bolt data write, the engine derives missing columns from bound
scalar properties, checks ALTER permission, rechecks under the shared schema gate,
and applies them through the schema coordinator. Standalone and replicated paths
update storage's column layout through the existing ALTER path. Warm writes issue
no column DDL and take no schema-setup mutex. Signed numeric literals are normalized
by the parser, so inference, byte encoding and indexed equality share literal
semantics. Null, collection and computed first values require declared columns.
