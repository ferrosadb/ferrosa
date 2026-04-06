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
