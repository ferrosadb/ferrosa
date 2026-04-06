# TODO: Multi-DC — Peer DC Assignment Hardcoded to Local DC

**Severity:** High (blocks multi-DC)
**Component:** ferrosa-cluster

## Issue

`transition_to_cluster` (cluster.rs:213-214) hardcodes all peers to `self.config.data_center`:

```rust
data_center: self.config.data_center.clone(),
```

If node4 in `dc2` joins a cluster seeded from `dc1`, the ring records it as `dc1`. NetworkTopologyStrategy replication, LOCAL_QUORUM, and EACH_QUORUM all break because the ring has incorrect DC metadata.

## Fix

Each node's data_center should come from its own config, propagated via the Raft state machine or ClusterInvite metadata. The ClusterInvite message should include the sender's DC, and `add-node` should carry the joining node's DC.

## Related

- `specs/todo/todo-rebalance-data-streaming.md` (data must move to new DC replicas)
