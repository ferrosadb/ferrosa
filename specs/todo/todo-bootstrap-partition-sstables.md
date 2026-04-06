# TODO: Bootstrap SSTable Partitioning by Target Node

**Severity:** Medium
**Component:** ferrosa-cluster
**File:** `ferrosa-cluster/src/controller/cluster.rs:753`

## Issue

```rust
// TODO(S4): partition SSTables by target node token range
```

During cluster bootstrap, SSTables should be split by token range so each new node only receives the data it owns. Currently all SSTables are sent without partitioning.

## Impact

Inefficient bootstrap — nodes receive data they don't own, wasting bandwidth and disk.
