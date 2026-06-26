# TODO: Batch Partial Application — Batchlog Deleted on Partial Failure

**Severity:** Critical (batch atomicity violation)
**Component:** ferrosa-cluster

## Issue

`coordinator/batch.rs:82-95` allows partial batch success then deletes the batchlog entry regardless:

```rust
let mut result = Ok(());
while let Some(res) = futs.next().await {
    if let Err(e) = res {
        result = Err(e);  // Records first error
    }
}
// Phase 3: Delete batchlog entry (even on partial failure)
self.delete_batchlog(batch_id).await?;
result  // Returns error, but mutations were applied!
```

If the client receives `Err` and retries, the first set of mutations are already applied. The client doesn't know which mutations succeeded. The batchlog is deleted so background replay can't fix it.

## Fix

On partial failure, either:
1. Do NOT delete the batchlog entry — let background replay complete the remaining mutations
2. Or: track which mutations succeeded and include that info in the error response
