# TODO: Rebalance Data Streaming Not Implemented

**Severity:** High
**Component:** ferrosa-cluster
**Files:** `ferrosa-cluster/src/rebalance.rs:139,162`

## Issue

Token rebalancing computes which ranges need to move but does NOT stream the actual data:

```rust
// TODO: Stream data for affected ranges from source to target nodes.
```

## Impact

After adding/removing nodes, data for reassigned token ranges stays on the old node. Reads to the new owner return empty until repair runs. Cluster scaling is incomplete.

## Fix

Implement range streaming: for each reassigned range, read SSTables from source node via internode protocol, write to target node's storage engine.
