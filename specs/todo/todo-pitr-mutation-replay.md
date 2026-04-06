# TODO: PITR Mutation Replay Not Implemented

**Severity:** High
**Component:** ferrosa-storage
**File:** `ferrosa-storage/src/engine.rs:1457`

## Issue

`open_from_snapshot_with_store()` has a TODO for full mutation replay from downloaded commit log segment files. Without this, point-in-time recovery can only restore to exact snapshot boundaries — not to arbitrary timestamps between snapshots.

```rust
// TODO(PITR): full mutation replay from downloaded segment files —
//   deserialize mutations, filter by _point_in_time, apply via write().
```

## Impact

PITR restore to arbitrary timestamps doesn't work. Only snapshot-boundary restores are functional. The DBaaS fork feature with `point_in_time` parameter relies on this.

## Fix

Implement commit log segment deserialization + timestamp-filtered replay in the restore path. The `SegmentReader` and timestamp filtering validation already exist in `restore/validation.rs`.
