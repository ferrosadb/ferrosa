# Bug: Token Ring Inconsistency Causes Data Scatter Across Nodes

**Severity:** P0 (data loss)
**Component:** ferrosa-cluster (token ring)
**Reproducer:** `tests/cluster/test_data_loss_reproduction.py::test_read_from_all_nodes`

## Issue

With RF=1, writes to the same partition key (same token) land on different
nodes depending on which coordinator handles the request. The token ring
view is inconsistent across nodes — each node thinks a different node owns
the same token range.

## Evidence

```
Wrote 100 rows to partition (tenant_id, session_id) = fixed UUIDs
RF=1 → all 100 should be on ONE node

Actual distribution:
  node1 (port 19042): 67/100
  node2 (port 19043): 67/100  
  node3 (port 19044): 33/100

Total unique across nodes: 100 (data exists but is scattered)
```

Node1 and node2 agree (67 each — likely node1 owns and node2 forwards).
Node3 thinks IT owns 33 of the 100 tokens (different ring view).

## Root Cause

Each node independently builds its token ring during `transition_to_cluster`
using `generate_deterministic_token(node_id, index)`. If the member list
or node IDs differ between nodes at transition time, they generate different
token assignments and disagree on ownership.

## Impact

- `SELECT count(*)` returns different results per node
- Data appears "lost" when reading from the "wrong" node
- With RF=1, there's no redundancy — each row exists on exactly one node
- The 67/33 split matches the production bug pattern exactly

## Fix

Token ring must be propagated through Raft consensus, not built independently.
The leader should propose the ring configuration and all followers should
adopt it identically. This ensures all nodes agree on token ownership.
