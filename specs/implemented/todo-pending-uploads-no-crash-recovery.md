# TODO: Pending Uploads Log Never Replayed on Startup

**Severity:** High (data loss after crash during S3 upload)
**Component:** ferrosa-storage

## Issue

`StorageEngine::open()` does NOT read or replay the pending-uploads.log on startup. The log is written by `poll_compactions()` before S3 uploads, but never consumed during engine initialization.

## Crash Scenario

1. Compaction output SSTable generated and swapped into local store
2. Pending-log entry fsynced
3. **CRASH** before S3 upload completes
4. On restart: pending-uploads.log has the entry, but engine ignores it
5. SSTable never reaches S3, manifest never updated
6. If local disk is ephemeral → permanent data loss

## Fix

During `StorageEngine::open()`, read `pending_entries()` and re-submit upload tasks for each entry. The upload is idempotent (S3 PUT overwrites).
