---
crate: ferrosa-graph
status: implemented
last_updated: 2026-06-19
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

## Purpose & boundary

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
| `parser` (`lexer`, `token`, `ast`, `parse_impl`, ~4.4k LoC) | Cypher lexing + recursive-descent parse to `Statement` AST |
| `planner::logical` (~595 LoC) | label→table resolution, property validation, **per-statement authorization** (`check_permission`) |
| `planner::physical` (~2.7k LoC) | anchor selection; `Expand` / `ExpandVarLength` / `Create` / `Merge` / `Subscribe` / `Union` plans |
| `executor::expand` (~7.2k LoC) | anchor lookup, per-hop adjacency reads, eval, aggregation, write clauses, DoS limits |
| `executor::varpath` + `leapfrog` | variable-length `[*min..max]` BFS with visited-set + vertex budget |
| `executor::aggregate` | `count`/`sum`/`avg`/`min`/`max`/`collect` accumulators with caps |
| `executor::eval` | expression evaluation, WHERE filtering, partition→JSON |
| `executor::subscribe` | per-connection `SubscriptionRegistry` (cap enforcement) |
| `adjacency::schema` | adjacency table metadata + keyspace naming + direction constants |
| `adjacency::observer` | `AdjacencyIndexObserver` — synchronous index maintenance on edge writes |
| `adjacency::reconcile` | background safety-net scan (repair missing, tombstone orphans) |
| `engine` (~3.2k LoC) | composition root: orchestrates parse→plan→exec, lazy adjacency setup, FOREACH / CALL {} expansion, DDL coordinators |
| `http` | axum HTTP/JSON endpoint, Basic auth, TLS, body limit, SSE for SUBSCRIBE |
| `bolt` | Bolt v5 handshake, PackStream codec, message dispatch, TCP server |

## Data flow (summary)

**Query path:** `execute_with_params` → `parse` → `bind_statement_params` →
(FOREACH / CALL {} expanded by the engine) → `validate` (resolve labels +
authorize) → `plan` → `execute`. Anchor partitions are read via
`WritePath::range_read`; each hop reads `system_graph_<ks>.adjacency` to find
neighbors; results are evaluated/aggregated/sorted into a `GraphResult`.

**Write path:** a `CREATE`/`MERGE`/`SET`/`DELETE` plan writes the edge/vertex row
through `WritePath`. Because the `AdjacencyIndexObserver` is registered on the
edge table as a `WriteObserver(Sync)`, the storage engine applies its derived
OUT+IN adjacency mutations in the **same** write — so the index never lags a
committed edge under normal operation. See [data-flow.md](data-flow.md) for the
full sequence.

## Key invariants

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

## Position in the dependency graph

Sits near the top of the stack: depends on `ferrosa-cluster`, `ferrosa-storage`,
`ferrosa-schema`, `ferrosa-sstable`, `ferrosa-common` (and `ferrosa-net` for the
integration test harness only). Depended on by the `ferrosa` binary, which mounts
its HTTP + Bolt servers. See the [root crate index](../../specs/crates.md) for the
full graph.
