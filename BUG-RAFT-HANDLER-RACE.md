# BUG: Node2/Node3 never transition to cluster mode — ClusterInvite not received

**Status:** PARTIALLY RESOLVED
- `7b057b0` — LazyRaft fixes handler registration timing
- `ba7599a` — ClusterInvite handler triggers cluster transition on receiving nodes
- `808b72b` — ClusterInvite sent on Data lane (not Raft lane)
- `30768c0` — ClusterInvite delivered synchronously before Raft init

Supersedes the handler race analysis. The LazyRaft fix (7b057b0) addressed handler registration timing, but the actual root cause is upstream: node2 and node3 never receive or process the ClusterInvite, so they stay in pair mode forever.

## Symptom

3-node cluster via podman compose with both fixes (808b72b ClusterInvite on Data lane, 7b057b0 LazyRaft handlers). Nodes pair successfully but Raft elections fail permanently:

**Node1:**
```
INFO  mode transition: pair -> cluster (raft init spawned) node_id=1229782938247303441 peers=2
ERROR openraft: timeout after 1s when Vote N1->N2
ERROR openraft: Unreachable node: Raft lane timeout
WARN  raft leader election timed out after 30s — reverting to Pair mode
```

**Node2/Node3:**
```
INFO  mode transition: standalone → pair  role=secondary
WARN  no handler registered msg_type=RaftVote
```
(no cluster transition, no ClusterInvite receipt logged — pair mode forever)

## Root cause

Node1 transitions `pair → cluster` when node3 connects (3rd node = quorum). Node1 sends ClusterInvite on the Data lane. But **node2 and node3 never process it**:

1. Node2 is in pair mode (secondary to node1)
2. Node3 is in pair mode (it joined node1 as seed)
3. Node1 fires ClusterInvite on Data lane to both peers
4. Peers receive it but either:
   - **The `ClusterInviteHandler` is not registered** on nodes that are in pair mode (handlers only registered during cluster transition)
   - **The handler rejects it** because the node is not yet in a state that accepts cluster invites

The `no handler registered msg_type=RaftVote` on node2/node3 is a **consequence** — since they never enter cluster mode, they never register any Raft handlers (neither the old way nor the LazyRaft way).

## Observed timeline

```
T+0s   node1 starts (standalone)
T+6s   node2 connects → node1 and node2 both transition: standalone → pair
T+12s  node3 connects → node1 transitions: pair → cluster, sends ClusterInvite
       node3: standalone → pair (paired with node1, NOT cluster)
       node2: stays in pair mode
T+12s+ node1 starts Raft elections, sends RaftVote to node2/node3
       node2/node3: "no handler registered msg_type=RaftVote" (still in pair mode)
T+42s  node1: "raft leader election timed out after 30s — reverting to Pair mode"
```

## What to investigate

1. **Is `ClusterInviteHandler` registered in pair mode?** Check if the handler for `MsgType::ClusterInvite` exists on nodes in pair mode. If not, the invite is silently dropped.

2. **Does `ClusterInviteHandler` check mode before processing?** It may reject invites from nodes that aren't recognized as cluster peers.

3. **Is the invite sent to the right peer IDs?** The `fire()` call uses `peer_id` which is derived from `FERROSA_HOST_ID`. Check that the peer manager has entries for both peers at the time of sending.

4. **Is node3's pair transition blocking the invite?** Node3 connects and transitions `standalone → pair` at the same time node1 transitions `pair → cluster`. The pair transition callback on node3 may run before the ClusterInvite arrives.

## Environment

- ferrosa branch: `fix/standalone-progressive-join` (7b057b0)
- docker-compose.yml in ferrosa-memory
- Raft state wiped, fresh start
- Fails consistently on every start

## Files

- `ferrosa-cluster/src/controller/cluster.rs:70-81` — ClusterInvite send
- `ferrosa-cluster/src/controller/cluster.rs:733` — ClusterInviteHandler
- `ferrosa-cluster/src/controller/pair.rs` — pair mode handler registration
- `ferrosa-net/src/rpc/handler.rs:52-61` — dispatch drops unregistered msg types
