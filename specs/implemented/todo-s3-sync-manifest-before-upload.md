# TODO: sync_sstables_to_s3 Updates Manifest Before Upload Completes

**Severity:** High (data loss on node restart if upload fails)
**Component:** ferrosa-storage

## Issue

`sync_sstables_to_s3()` (engine.rs:2464-2548) submits upload tasks via `upload_mgr.submit()` which is non-blocking (sends to a channel), then immediately adds the SSTable to the manifest and saves it. The upload hasn't completed yet.

```rust
// Line 2524: submit (non-blocking — just queues the upload)
upload_mgr.submit(task).await?;

// Line 2526-2536: immediately add to manifest (upload hasn't finished!)
manifest.add_sstable(table_id_str, ManifestEntry { ... });

// Line 2543: save manifest with entries that may not be in S3 yet
manifest.save_with_retry(store.as_ref(), &prefix).await?;
```

## Impact

If the S3 upload fails or the node crashes before the upload completes:
1. The manifest says the SSTable exists in S3
2. Another node downloads the manifest and tries to get the SSTable → fails
3. The local SSTable files may be evicted (cache cleanup) since the manifest says S3 has them
4. Data is lost

## Comparison

The compaction path (engine.rs:1920-1952) correctly waits for S3 confirmation via a `oneshot::channel` before updating the manifest. The sync path should do the same.

## Fix

Either:
1. Use the `on_complete` channel pattern (like compaction) to wait for each upload before adding to manifest
2. Or: batch all uploads, await all completions, then update manifest once

## Related

- `specs/todo/todo-s3-sync-manifest-metadata-placeholder.md` (token/timestamp metadata is i64::MIN/0)
