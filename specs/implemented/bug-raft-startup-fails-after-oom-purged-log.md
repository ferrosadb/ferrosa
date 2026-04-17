---
type: bug
priority: P1
reported-by: ferrosa-memory podman cluster crash
implemented-by: claude-code
verified-by: ""
created: 2026-04-17
updated: 2026-04-17
---

# Raft startup fails after OOM kill: "expected index [0, N), got [Some(M), Some(N))"

## Observed

After an OOM kill on a ferrosa node in the ferrosa-memory test cluster, the node fails to restart with:

```
ERROR ferrosa_cluster::controller::cluster: raft initialization failed (Fatal)
  fatal=when Read LogIndex(0): Failed to get log entries,
  expected index: [0, 64), got [Some(7), Some(63))
```

The OOM kill lost the in-memory state machine (`last_applied = None`) but the sled log store retained `last_purged_log_id = 6`. Openraft tries to replay from index 0 (since `last_applied` is None) but entries 0-6 are already purged.

## Root Cause

`FerrosStateMachine::last_applied` is purely in-memory. It's recovered from snapshots during normal operation, but if the process is killed before a snapshot is taken, `last_applied` reverts to `None` on restart. The sled-backed log store correctly persists `last_purged_log_id`, creating an inconsistency.

## Fix

Added `FerrosStateMachine::recover_from_purge_point()` — called during cluster controller initialization. If `last_applied` is `None` and the log store has a purge point, `last_applied` is set to the purge point. This is safe because entries can only be purged after they've been applied and snapshotted.

Files changed:
- `ferrosa-cluster/src/raft/state_machine.rs` — `recover_from_purge_point()` method + 3 tests
- `ferrosa-cluster/src/raft/log_store.rs` — `last_purged_log_id()` public accessor
- `ferrosa-cluster/src/controller/cluster.rs` — call recovery during init

## Acceptance Criteria

- [x] Node restarts successfully after OOM kill with purged log entries
- [x] Recovery is no-op when `last_applied` is already set (normal restart)
- [x] Recovery is no-op when no purge point exists (fresh cluster)
