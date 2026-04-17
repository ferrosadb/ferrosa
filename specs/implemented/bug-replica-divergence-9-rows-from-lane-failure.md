---
type: bug
priority: P1
reported-by: ferrosa-memory production cluster observation
implemented-by: claude-code
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# 9-row replica divergence caused by permanent lane failure

## Observed

`SELECT COUNT(*) FROM entity_store` returned different results per-node:
- Node2: 13,979
- Node3: 13,970
- Node1: unavailable (CQL timeout)

A 9-row gap between replicas at rest with RF=3 means writes reached some replicas but not others.

## Root Cause

The data lane to node1 entered `LaneState::Failed` (permanent, no recovery) after bootstrap streaming exhausted the reconnection budget (12 attempts, ~3 minutes). With the lane dead:

1. `coordinate_write_with()` fans out to all replicas, but the MutationForward to node1 returns `LaneFailed` immediately
2. With CL=ONE, the write succeeds (1 ACK from another replica is enough)
3. Hints are stored for node1 but can never be delivered (lane is permanently dead)
4. Node1 misses 9 writes that occurred while its lane was down

The same lane failure also explains why node1 was at 112MB (never received streamed data) while node2/3 were at 1.5GB.

## Fix

Addressed by the lane auto-recovery fix in `ferrosa-net/src/lane_actor.rs`:
- `MarkFailed` now transitions to `Reconnecting` with a 10s delayed retry (not terminal `Failed`)
- `handle_send`/`handle_fire` auto-recover from `Failed` state on first use
- Lane will eventually reconnect and hint delivery will replay missed writes

## Acceptance Criteria

- [x] Lane auto-recovers from Failed state (no permanent failure)
- [ ] Hint delivery replays missed writes after lane recovery
- [ ] `SELECT COUNT(*)` returns same value on all replicas after recovery period
