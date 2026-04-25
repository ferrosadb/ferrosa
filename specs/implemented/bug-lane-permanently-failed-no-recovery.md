---
type: bug
priority: P1
reported-by: ferrosa-memory production cluster
implemented-by: ""
verified-by: ""
created: 2026-04-17
updated: 2026-04-18
---

# Data lane permanently fails after max reconnection attempts — no recovery without restart

## Implementation Notes

Fix landed in `ferrosa-net/src/lane_actor.rs` (commit 30a00e7). The
`LaneCommand::MarkFailed` handler no longer transitions to the terminal
`Failed` state; instead it resets state to `Reconnecting { attempt: 0,
backoff: 5s..30s }` and spawns a delayed reconnect 10s later. Unit test
`MarkFailed should reset to Reconnecting, not terminal Failed` (see
`ferrosa-net/src/lane_actor.rs:607`) guards against regression. The
`LaneState::Failed` variant is retained for the `QueryStatus` reporting
path but is no longer reachable during normal operation.


## Observed

Node1's data lane to node3 transitioned from `Reconnecting` to `Failed` after 12 reconnection attempts (~3 minutes). Once in `Failed` state, ALL subsequent sends return `LaneFailed` error permanently. The only recovery is a full node restart.

Symptoms:
- Point reads work per-node (local SSTable path)
- Coordinated reads/writes fail (any operation that fans out to the failed lane)
- Raft replication to the failed peer stops (AppendEntries flood of "lane permanently failed")
- CQL queries that require coordinator fan-out time out

## Root Cause

`ferrosa-net/src/lane_actor.rs:321-324`:
```rust
LaneCommand::MarkFailed => {
    state = LaneState::Failed;
}
```

`LaneState::Failed` is terminal — no transition back to `Reconnecting` or `Connected`. The `handle_send` function at line 349 returns `Err(LaneFailed)` for every message when in `Failed` state.

`ferrosa-net/src/reconnect.rs:69`: `MAX_RECONNECT_ATTEMPTS = 12` — after 12 attempts (30s cap per attempt, ~3 min total), `spawn_reconnect` calls `handle.mark_failed()`.

## Trigger

Bootstrap streaming saturates the Bulk lane. The alive-watcher detects the TCP connection as dead (timeout/reset). Reconnection attempts start but fail because the peer is still busy with streaming. After 12 failed attempts, the lane is permanently marked failed.

## Fix

Replace terminal `Failed` state with periodic retry. When `MarkFailed` arrives:
1. Log the failure
2. Instead of staying in `Failed` forever, schedule a delayed reconnect (e.g., 30s)
3. If the delayed reconnect succeeds, swap in the new client
4. If it fails, schedule another delayed reconnect (with backoff up to a cap)

This way a transient network issue eventually self-heals.

## Acceptance Criteria

- [ ] Lane recovers automatically after the peer becomes reachable again
- [ ] No permanent `Failed` state — always retries with backoff
- [ ] Raft replication resumes after lane recovery
- [ ] CQL coordinated queries work after lane recovery
