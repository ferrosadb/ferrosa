# BUG: 3-Node Cluster Formation Fails to Elect Raft Leader

**Severity:** P0 (cluster cannot form, all DDL blocked)
**Component:** ferrosa-cluster / controller / raft
**Reproducer:** `ferrosa-cluster/tests/cluster_formation.rs::three_node_cluster_elects_raft_leader`
**Date reported:** 2026-04-05

## Summary

A fresh 3-node Raft cluster using the progressive join pattern (standalone -> pair -> cluster) cannot elect a leader. All three nodes start elections simultaneously, split votes indefinitely, and terms increment without convergence. DDL operations are completely blocked because they require a Raft leader.

## Observed Behavior

1. Node1 starts standalone, transitions to pair when node2 connects, then to cluster when node3 connects.
2. `transition_to_cluster` is called on node1 (the seed/Primary), which spawns a background task that:
   - Sends `ClusterInvite` to all peers
   - Creates the Raft instance via `FerrosRaft::new()`
   - Calls `raft.initialize(members)` (seed only)
   - Polls for leader election with 30s timeout
3. Node2 and node3 receive `ClusterInvite` and independently call `transition_to_cluster`, each creating their own Raft instances.
4. All three nodes begin Raft elections at roughly the same time.
5. Each candidate votes for itself (term T1) but no candidate receives 2 votes (quorum for 3-node cluster).
6. Vote RPCs timeout at the `election_timeout` interval despite TCP connectivity being fine.
7. Terms increment slowly (T1 -> T19 observed in ~90s) but no leader is elected.
8. After the 30s timeout, the background task reverts the mode to Pair and restores DDL to Direct path.
9. Node3 may also be rejected with "peer not approved to join cluster" if `auto_join=false`.

### Log Evidence (Production)

```
WARN peer not approved to join cluster, ignoring  peer=<node3-uuid>
WARN raft leader election timed out after ~30s -- reverting to Pair mode
WARN schema forward: CreateKeyspace failed - net: timeout: Data lane timeout
```

## Root Causes

There are multiple interacting issues:

### RC-1: Simultaneous Elections (Vote Splitting)

All three nodes create their Raft instances at nearly the same time and immediately begin elections. With identical election timeout ranges (default: 3000-6000ms), all nodes timeout and start new elections in lockstep. Raft's randomized election timeout is designed to break this symmetry, but the window is too narrow relative to the RPC round-trip time.

**Evidence:** Each candidate increments its term but only ever receives its own vote. No candidate achieves quorum (2 of 3).

### RC-2: Vote RPC Timeouts on Raft Lane

Vote RPCs sent via `Lane::Raft` through the `PeerManager` timeout despite TCP connectivity working on `Lane::Data`. This suggests the Raft lane connections are not fully established when elections begin, or the handler registration races with the first incoming Vote RPC.

The `FerrosRaftNetworkFactory` creates `FerrosRaftNetwork` instances that send Vote messages through the `PeerManager`. If the PeerManager's connection pool for the Raft lane is not yet established for a given peer, the send fails with a timeout rather than an immediate error.

**Evidence:** Vote RPCs timeout at 3s despite `ClusterInvite` delivery (which uses `Lane::Data`) succeeding.

### RC-3: Multiple Independent `raft.initialize()` Calls

Although the code guards `raft.initialize()` behind `was_seed` (only the Primary calls it), the `ClusterInviteHandler` can trigger `transition_to_cluster` on node2 and node3 independently. If both node2 and node3 were each in Pair mode as Primary of their respective pair contexts (unlikely but possible with timing), they could each call `initialize()` with potentially different member orderings. Even when only one node initializes, the other nodes need to receive the initial membership via AppendEntries from the leader -- but no leader exists yet because elections fail.

### RC-4: Missing Raft Lane Outbound Connections

In `transition_to_cluster`, the code creates reverse outbound connections for peers the PeerManager doesn't know about. However, this connection setup happens concurrently with the Raft init task. The Raft instance may start elections before all outbound pools are established, causing the first Vote RPCs to fail.

## Impact

- **DDL completely blocked**: No CREATE KEYSPACE, CREATE TABLE, or any schema operation works.
- **Cluster never becomes operational**: The system reverts to Pair mode after the timeout, but the third node remains disconnected from the Raft consensus.
- **Data lane timeouts**: Schema forwarding fails because there is no Raft leader to propose through.

## Reproduction

### Integration Test

```bash
cargo test --package ferrosa-cluster --test cluster_formation three_node_cluster_elects_raft_leader
```

This test creates 3 nodes with real networking, follows the progressive join pattern, and asserts that a Raft leader is elected within 30 seconds. It fails consistently, demonstrating the bug.

### Manual Reproduction (Podman)

1. Start a 3-node cluster with empty raft/ directories.
2. Wait for cluster formation logs.
3. Observe repeated "raft leader election timed out" in logs.
4. Attempt `CREATE KEYSPACE` via CQL -- it will timeout.

## Proposed Fix

### Option A: Staggered Raft Initialization (Recommended)

Only the seed node calls `raft.initialize()`. Non-seed nodes should NOT create their Raft instance until they receive the first AppendEntries or Vote RPC from the seed. This serializes initialization:

1. Seed creates Raft, calls `initialize()`, becomes candidate, wins its own vote.
2. Seed sends Vote to node2 and node3.
3. Node2/node3 receive Vote, create their Raft instances (via LazyRaft), and respond.
4. Seed wins election with 2+ votes.

This requires the `LazyRaft` handlers to actually create the Raft instance on first contact rather than just waiting for it.

### Option B: Pre-Election Connection Verification

Before starting elections, verify that outbound Raft lane connections exist to all peers. Add a readiness check:

```rust
// In the background Raft init task, before raft.initialize():
for (peer_uuid, _) in &peers {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !peer_manager.has_raft_lane(*peer_uuid) {
        if Instant::now() > deadline {
            tracing::error!("Raft lane not ready for peer {peer_uuid}");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
```

### Option C: Wider Election Timeout Spread

Increase the election timeout range to reduce vote splitting probability. For example, `raft_election_timeout_min_ms: 1000, raft_election_timeout_max_ms: 5000`. This is a mitigation, not a fix -- it reduces the probability of simultaneous elections but does not eliminate it.

### Recommended Approach

Combine Option A (staggered init) with Option B (connection readiness). Option C can be applied as defense-in-depth but should not be the primary fix.

## Related

- `ferrosa-cluster/src/controller/cluster.rs` -- `transition_to_cluster()` and the background Raft init task
- `ferrosa-cluster/src/raft/network.rs` -- `FerrosRaftNetworkFactory` and Vote RPC transport
- `ferrosa-cluster/src/controller/peer_events.rs` -- `on_peer_connected` triggers `transition_to_forming`
- `specs/cluster-formation-architecture.md` -- Architecture design for progressive join
- `specs/fmea-cluster-formation.md` -- CF-T17 (membership race) was identified but the current mitigation is insufficient
- `specs/hazards-cluster-formation.md` -- Related hazard analysis

## Final Verification (2026-04-05, commit 74a33ff)

Both cluster_formation tests pass:
- `three_node_cluster_elects_raft_leader`: Leader elected in 21s, all 3 nodes agree on leader ID 72057594037927936
- `progressive_join_mode_transitions`: Mode transitions standalone→pair→cluster work correctly
- **Status: VERIFIED FIXED**
