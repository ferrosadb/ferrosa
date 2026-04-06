# TODO: ferrosa-ctl Snapshot/Restore Commands Not Implemented

**Severity:** Medium
**Component:** ferrosa-ctl
**File:** `ferrosa-ctl/src/commands.rs:379-420`

## Issue

Four CLI commands are stubbed but not implemented:

```rust
// TODO: POST /api/snapshots   (create snapshot)
// TODO: GET /api/snapshots    (list snapshots)
// TODO: DELETE /api/snapshots/{name}  (delete snapshot)
// TODO: POST /api/restore     (restore from snapshot)
```

## Impact

Operators cannot manage PITR snapshots via the CLI tool. Must use HTTP API directly.
