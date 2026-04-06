# TODO: Batchlog Remote Delete Best-Effort — Causes Duplicate Replay

**Severity:** High (duplicate batch application)
**Component:** ferrosa-cluster

## Issue

`coordinator/batch.rs:138-169` — batchlog `delete_entry` on remote replicas is best-effort:

```rust
for host_id in batchlog_replicas {
    if let Err(e) = self.peer_manager.send(...).await {
        tracing::warn!("failed to send batchlog delete: {e}");  // Just logged!
    }
}
Ok(())  // Always returns Ok
```

If the remote delete fails, the batchlog entry persists on that replica. The background replay task will see the stale entry and replay the batch again, applying mutations TWICE.

## Impact

- Idempotent mutations (INSERT with same timestamp) are harmless
- Non-idempotent mutations (counter increments, conditional updates) get corrupted
- Multiple replications of the same batch waste resources

## Fix

1. Retry remote deletes with backoff
2. Or: track batch IDs that have been applied and skip duplicates during replay
3. Or: use Raft to coordinate batchlog lifecycle (ensures all replicas agree)
