# TODO: Cluster Formation Hardcodes RF=1 and CL=ONE

**Severity:** High (blocks production multi-node correctness)
**Component:** ferrosa-cluster

## Issue

`transition_to_cluster` in `cluster.rs` hardcodes `initial_replication_factor: 1` and the write coordinator defaults to `ConsistencyLevel::One` during early cluster formation. This means:

1. Data written during and immediately after formation is only stored on one node
2. If that node fails before repair/rebalance, data is lost
3. Keyspaces created with RF=3 don't actually replicate to 3 nodes until the ring stabilizes and a repair runs

The formation path should respect the keyspace's configured replication factor and consistency level, or at minimum warn that RF=1 is temporary and trigger an immediate repair once the ring is fully formed.

## Fix

- Read the keyspace's replication strategy and factor from schema when transitioning to cluster
- Set initial RF from the keyspace config (default to 3 for SimpleStrategy, use NTS map for NetworkTopologyStrategy)
- After ring formation completes and all nodes are `NodeState::Normal`, trigger a repair to ensure data meets the configured RF
- Document that writes during formation may have reduced durability

## Related

- `specs/todo/todo-rebalance-data-streaming.md` (streaming needed to actually move data to new replicas)
- `specs/todo/todo-multi-dc-node-dc-assignment.md` (NTS requires correct DC assignment)

## Implementation Notes

**Branch:** `fix/formation-respects-rf`
**Worktree:** `.worktrees/ws1-formation-rf`
**Files changed:** `ferrosa-cluster/src/controller/cluster.rs`, `ferrosa-cluster/src/controller/tests.rs`

### Tests that prove it

All in `ferrosa-cluster/src/controller/tests.rs`:

1. `formation_rf_extracted_from_simple_strategy_keyspace` — `resolve_formation_rf` returns 3 for an RF=3 SimpleStrategy keyspace, not 1.
2. `formation_rf_defaults_to_1_with_no_user_keyspaces` — returns the provided default (1) when only system keyspaces exist.
3. `formation_rf_extracted_from_nts_keyspace` — uses `local_dc` RF when present; falls back to summing all DC RFs when local DC is unknown.
4. `formation_rf_takes_max_across_keyspaces` — returns 5 (not 3) when keyspaces have RF=3 and RF=5.
5. `formation_emits_reduced_durability_warning_counter_when_rf_less_than_configured` — `FORMATION_REDUCED_DURABILITY_WRITES` counter increments when cluster_size < configured_rf at formation time.
6. `transition_to_cluster_uses_keyspace_rf_not_hardcoded_1` — integration-style test: creates an RF=3 schema, calls `transition_to_cluster`, reads `coordinator.default_rf` from `WritePath::Cluster`, asserts it equals 3 (not 1).

### Approach

- Added pure function `resolve_formation_rf(schema, local_dc, default_rf) -> usize` in `cluster.rs` (`pub(super)`). Iterates non-system keyspaces, parses SimpleStrategy `replication_factor` or NetworkTopologyStrategy per-DC values, returns the max across all keyspaces (or `default_rf` if none).
- Added `pub static FORMATION_REDUCED_DURABILITY_WRITES: AtomicU64` counter alongside the existing bootstrap silent-failure counters.
- Replaced the hardcoded `let initial_rf = 1;` in `transition_to_cluster` with `resolve_formation_rf(&self.schema, &self.config.data_center, 3)`, plus a warning + counter increment when cluster_size < initial_rf.

### Deliberate design decision: CL stays ConsistencyLevel::One

`initial_cl` remains `ConsistencyLevel::One`. Consistency level is orthogonal to replication factor: during formation the cluster may not yet have enough live, healthy replicas to satisfy a higher CL (e.g. Quorum requires ceil((RF+1)/2) alive replicas). Setting CL=Quorum on a 2-of-3 cluster that just started forming would immediately reject writes. Operators can `ALTER KEYSPACE` or adjust CL policy after all nodes reach `Normal` state.

### What is still open

Repair after ring stabilization (so data written during the reduced-durability window gets re-replicated to the correct RF) is tracked separately in `specs/todo/todo-rebalance-data-streaming.md`.
