# TODO: Hints Delivered to Wrong Node After Topology Change

**Severity:** High (data loss when tokens move between nodes)
**Component:** ferrosa-cluster

## Issue

`hints/delivery.rs:54-126` — hints are keyed by `peer_id` (UUID) and delivered to that specific node. If the token ring changes while hints are pending, the hints go to the original node even though a different node now owns those tokens.

Example:
1. Node A owns token range [0, 100], fails
2. Hints stored for node A
3. Tokens rebalanced: Node B now owns [0, 100]
4. Node A recovers
5. Hints replayed TO Node A (which no longer owns [0, 100])
6. Data never reaches Node B (the current owner)

## Fix

Hints should be re-routed based on the current token ring at delivery time:
1. Before delivering a hint, compute which node CURRENTLY owns the mutation's token
2. If the current owner differs from the original peer, redirect the hint
3. Or: store hints by token range, not by peer ID
