---
type: bug
priority: P1
reported-by: user
implemented-by: ""
verified-by: ""
created: 2026-04-07
updated: 2026-04-07
source: ferrosa-memory podman cluster (RustFS backend)
source-location: "ferrosa-storage/src/manifest.rs"
branch: "main"
---

# S3 manifest save requires CAS — blocks uploads to non-CAS stores like RustFS

## Description

The manifest save path (`Manifest::save` / `save_with_retry`) always uses
`PutMode::Create` or `PutMode::Update(etag)` via `put_opts`. Object stores
that do not support conditional PUT (e.g. RustFS) reject these operations
with "Operation not yet implemented", causing **every** S3 SSTable sync to
fail.

The startup probe (`probe_s3_cas` in `engine.rs:281`) correctly detects that
CAS is unsupported and logs a non-fatal warning. But the actual manifest
`save()` still uses conditional put modes unconditionally — there is no
fallback to a plain `put()`.

As a result, SSTables are uploaded to S3 (as 96-128 byte stubs — the
upload may be partial) but the manifest is never saved, so on restart
the node cannot find its SSTables in S3 and reports them as corrupted.

## Impact

- **All S3 backups are broken** when using any store without CAS support
- In the ferrosa-memory cluster, 3400 SSTable files in RustFS were 96-128
  byte stubs with no actual data
- 564+ `S3 SSTable sync failed` errors on a single node over 21 hours
- Nodes reading from S3 get "skipping corrupted SSTable" on every read

## Root Cause

`ferrosa-storage/src/manifest.rs` lines 140-158: `save()` unconditionally
uses `PutMode::Create` / `PutMode::Update` with no fallback to plain `put()`.

`ferrosa-storage/src/engine.rs` lines 281-293: `probe_s3_cas()` returns an
error but `main.rs:351` treats it as non-fatal — the engine starts but every
subsequent manifest save fails.

## Expected Behavior

When the object store does not support conditional PUT:

1. `probe_s3_cas()` detects this at startup (already works)
2. The engine stores a `cas_supported: bool` flag
3. `Manifest::save()` falls back to unconditional `put()` when CAS is not
   available — last-writer-wins, which is acceptable for single-node
   S3 prefixes and dev environments
4. A warning is logged on each unconditional save: "manifest saved without
   CAS protection — concurrent writers may overwrite each other"

CAS should be **preferred** but not **required**. The data must be uploaded
to S3 regardless of CAS support.

## Reproduction

```bash
# Start ferrosa with RustFS as the S3 backend
# Any write triggers a flush → S3 sync → manifest save → failure
podman compose up -d  # with RustFS in docker-compose.yml
# Logs show:
#   S3 CAS probe failed (non-fatal): ...
#   manifest CAS conflict on attempt 1, retrying: ... Operation not yet implemented.
#   manifest CAS conflict on attempt 2, retrying: ... Operation not yet implemented.
#   S3 SSTable sync failed e=invalid format: failed to save manifest: Operation not yet implemented.
```

## Fix

In `Manifest::save()`, accept a `cas_supported: bool` parameter (or store
it on the manifest/engine). When false, use a plain `store.put()` instead
of `store.put_opts()` with CAS mode. Thread this flag from the probe
result through to all save call sites.
