# TODO: Add-Node After Initial Formation Lacks Data Streaming

**Severity:** High (blocks elastic scaling)
**Component:** ferrosa-cluster

## Issue

The current `add-node` path (via Raft `AddNode` proposal) assigns a token to the new node and adds it to the ring, but does NOT stream existing data to the new node. The new node joins empty and only receives new writes routed to its token range.

This means:

1. Reads for keys in the new node's range return empty/stale until a full repair runs
2. No automatic bootstrapping — Cassandra's streaming bootstrap is not implemented
3. Cluster appears to lose data from the client's perspective after adding a node

## Current Behavior

```
add-node → Raft proposal → token assigned → ring updated → node is Normal
                                                          ↑ NO data streaming
```

## Expected Behavior (Cassandra-compatible)

```
add-node → token assigned → node enters JOINING state
         → ranges computed → streaming from existing owners
         → streaming complete → node transitions to NORMAL
         → cleanup on source nodes (remove streamed ranges)
```

## Fix

1. New node should enter `NodeState::Joining` (not Normal) until bootstrap completes
2. Compute token ranges the new node will own using the ring
3. Stream SSTables for those ranges from current owners via internode protocol
4. Once streaming completes, transition to `NodeState::Normal` via Raft proposal
5. Source nodes can then compact away data they no longer own

## Related

- `specs/todo/todo-rebalance-data-streaming.md` (the streaming primitive needed here)
- `specs/todo/todo-formation-hardcoded-rf1-cl-one.md` (RF issues during formation)
