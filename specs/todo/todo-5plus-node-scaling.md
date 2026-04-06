# TODO: 5+ Node Cluster Formation Not Tested/Supported

**Severity:** Medium (blocks large cluster deployments)
**Component:** ferrosa-cluster

## Issue

Cluster formation has only been tested and validated with 3-node clusters. The `transition_to_cluster` path handles the pair→cluster transition for exactly 3 nodes (1 pair + 1 joiner). Scaling beyond 3 nodes relies on the `add-node` path, which currently has no data streaming (see `todo-add-node-post-formation.md`).

Specific concerns for 5+ nodes:

1. **Token distribution**: Token assignment uses simple division (`i64::MIN + i * segment_size`). With many nodes, verify tokens are evenly distributed and don't create hot spots
2. **Raft group scaling**: openraft with 5+ voters — election timeouts, log replication latency, and snapshot transfer size may need tuning
3. **Seed node selection**: UUID-based seed selection works for 3 nodes but should be validated with larger clusters
4. **Ring convergence**: Time for all nodes to agree on the ring state increases with cluster size
5. **Gossip/failure detection**: Currently Raft-based, not gossip. Raft leader handles all failure detection — verify this scales

## Fix

1. Add integration tests for 5-node and 7-node clusters
2. Validate token distribution uniformity at each cluster size
3. Load test Raft with 5, 7, 9 voters for election stability
4. Consider Raft learners (non-voting members) for read-heavy nodes beyond the consensus group
5. Add metrics for ring convergence time and Raft replication lag

## Related

- `specs/todo/todo-add-node-post-formation.md` (add-node needs streaming for this to work)
- `specs/todo/todo-rebalance-data-streaming.md` (data must move when ring changes)
- `specs/todo/todo-multi-dc-node-dc-assignment.md` (multi-DC adds another dimension to scaling)
