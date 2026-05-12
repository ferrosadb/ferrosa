# Bug: `system_graph_<ks>.adjacency` is registered only on the coordinator, not on cluster replicas

## Summary

When a keyspace with graph-annotated edge tables (e.g. `agent_memory.typed_edges`,
`agent_memory.co_occurs_with`) receives its first edge mutation, the graph
engine lazily creates the system adjacency keyspace
`system_graph_<keyspace>.adjacency` on the **coordinator node only**. The
creation goes through `Schema::create_keyspace_internal` /
`create_table_internal`, both of which mutate the local schema registry
directly without going through the cluster's DDL replication path.

The coordinator then issues the edge MERGE, the `AdjacencyIndexObserver`
fires, and a derived mutation is forwarded to the other replicas via
`MutationForward`. On those replicas, `StorageEngine::register_table` was
never called for `system_graph_<ks>.adjacency`, so the mutation is
rejected with:

```text
WARN ferrosa_cluster::coordinator: MutationForward write failed — not sending ACK
  e=invalid format: table not registered: system_graph_agent_memory.adjacency
  table=system_graph_agent_memory.adjacency
```

The coordinator's `send()` then times out waiting for replica ACKs, the
graph mutation hangs for `reqwest::Client`'s 10 s budget, and the client
sees `operation timed out`.

## Reproduction (observed in CI)

`ferrosa-memory` PR #4, `Cluster integration tests` job, test
`crates/ferrosa-memory-core::tests::cql_live::public_graph_write_round_trip_for_co_occurs_edges`:

1. 3 ferrosa nodes start (CI compose `Build` matrix).
2. `migrate` applies `ddl/*.cql` including `agent_memory.typed_edges`
   and `agent_memory.co_occurs_with` (both with `extensions = { 'graph.type': 'edge', ... }`).
3. Test client opens a `GraphClient` against `http://localhost:17474` (node1).
4. Test calls `put_co_occurs_edge` →
   `MERGE (a:Entity {...}) MERGE (b:Entity {...}) MERGE (a)-[r:CO_OCCURS_WITH {...}]->(b) SET r.strength=...`
5. node1's GraphEngine `ensure_adjacency_storage_for_keyspace("agent_memory")`
   creates `system_graph_agent_memory.adjacency` **locally only**.
6. node1 commits the MERGE, the adjacency observer produces OUT/IN mutations
   for the new edge, and `MutationForward` ships them to node2/node3.
7. node2/node3 reject because their local `StorageEngine` has no
   `system_graph_agent_memory.adjacency`.
8. node1's coordinator times out waiting for ACKs from node2/node3 →
   the HTTP request hangs ~10 s → `operation timed out` propagates to
   the test.

Confirmed in CI run 25692856993 logs (node3-1, repeated every ~15s):

```text
node3-1 | WARN ferrosa_cluster::coordinator:
  MutationForward write failed — not sending ACK
  e=invalid format: table not registered: system_graph_agent_memory.adjacency
```

## Why it slipped past CI before

The cluster integration job has been failing earlier in the pipeline for
weeks (system_schema.views regression, schema agreement, etc.). The
graph-edge write tests added in PR #4's `54356db fix: stream viz snapshots
from storage` never got past those earlier blockers, so this bug is
freshly visible only now.

## Root cause

`ferrosa-graph/src/engine.rs::ensure_adjacency_storage_for_keyspace`
(also the related startup path in `GraphEngine::new`) uses:

- `Schema::create_keyspace_internal(adj_meta)` (local-only, no Raft)
- `Schema::create_table_internal(adjacency_table_metadata(ks))` (local-only, no Raft)
- `StorageEngine::register_table(adj_schema)` (local-only)

Regular CQL DDL goes through a different path (e.g. `coordinate_ddl` in
pair mode, or `ddl_path::forward_ddl_to_leader` in Raft cluster mode),
which propagates DDL operations to all peers. The graph engine has no
handle to either coordinator and so cannot use them.

## Fix shape (proposed)

Introduce a lightweight `GraphSchemaCoordinator` trait that the graph
engine takes by `Arc<dyn ...>`:

```rust
pub trait GraphSchemaCoordinator: Send + Sync {
    fn create_keyspace_replicated(&self, ks: KeyspaceMetadata) -> Result<()>;
    fn create_table_replicated(&self, table: TableMetadata) -> Result<()>;
}
```

Production wires this to whatever coordinator the running cluster has
(pair `apply_ddl` + `replicate_ddl`, or full Raft `forward_ddl_to_leader`).
Single-node and unit tests can pass a `LocalGraphSchemaCoordinator` that
calls `create_*_internal` directly, preserving the current behaviour.

`ensure_adjacency_storage_for_keyspace` and the equivalent startup loop
in `GraphEngine::new` swap their `schema.create_*_internal` calls for
the coordinator. `StorageEngine::register_table` continues to be called
locally on each node — it's a side effect of applying the replicated
DDL, which happens during the DDL apply pipeline on every replica.

## Test plan

Failing test before fix, passing after:

- New integration test
  `ferrosa-graph/tests/adjacency_replication.rs::ensure_adjacency_storage_replicates_to_all_schemas`
  Sets up two `Schema` instances (representing two cluster nodes) plus a
  shared `RecordingCoordinator` that fans out DDL to both. Builds a
  GraphEngine wired to schema-A + the coordinator. Registers an edge
  table on **both** schemas (simulating a DDL the coordinator already
  applied). Triggers `ensure_adjacency_storage_for_keyspace` via a
  graph mutation on engine-A. **Pre-fix**: schema-B does **not** have
  `system_graph_<ks>.adjacency`. **Post-fix**: schema-B does, because
  the engine routed the registration through the coordinator.

- Existing integration tests
  `ferrosa-memory-core::tests::cql_live::public_graph_write_round_trip_for_co_occurs_edges`
  (currently times out in CI) becomes the upstream end-to-end check
  once a real Raft-backed `GraphSchemaCoordinator` is wired in.

## Status

- 2026-05-11: Bug spec written. TDD test pending. Fix pending.
