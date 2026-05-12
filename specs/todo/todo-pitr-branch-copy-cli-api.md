---
executive_summary:
  purpose: "Tracks implementation of PITR branch/copy support from the approved architecture spec."
  critical_items:
    - "Implement metadata-only branch creation from PITR snapshots."
    - "Add manifest object references before allowing cross-prefix shared SSTables."
    - "Ship storage, web API, CLI, tests, and public website examples together."
---

# TODO: Implement PITR Branch/Copy CLI and API

**Severity:** High
**Component:** storage, web API, `ferrosa-ctl`, docs
**Spec:** `specs/pitr-branch-copy-architecture.md`
**ADR:** `specs/decisions/012-pitr-branches-copy-on-write.md`

## Issue

Users need to create a writable database branch from a PITR snapshot/checkpoint.
Branch creation must be cheap: it should copy metadata and share immutable source
SSTables through explicit object references, then write divergent data into the
branch target prefix.

## Required work

1. Add manifest schema support for durable `ObjectRef` entries.
1. Add branch metadata, lineage, and branch manifest management in
   `ferrosa-storage`.
1. Extend object GC roots to include branch manifests and lineage records.
1. Add web routes under `/api/branches`.
1. Add `ferrosa-ctl branch create/list/describe/delete`.
1. Add public docs examples under `docs/database/`.
1. Add parser-backed tests that keep website examples aligned with CLI syntax.

## Acceptance criteria

- `POST /api/branches` creates a branch from a snapshot without copying source
  SSTable bytes.
- Branch writes create branch-owned objects under the target prefix.
- Branch deletion never deletes shared source SSTables still referenced by a
  snapshot, live manifest, or another branch.
- `ferrosa-ctl branch create <name> --from-snapshot <snapshot> --target-prefix
  <prefix>` sends the exact API fields documented in the spec.
- Unit tests cover storage branch creation, object-reference manifest entries,
  branch deletion, web API status codes, CLI parsing, and doc example syntax.
- Website docs explain copy-on-write branch behavior and storage-retention
  caveats without claiming deep-copy semantics.

## Notes

Existing `ferrosa-ctl restore` sends `snapshot_name` while `RestoreRequest`
expects `snapshot`; fix that drift before copying restore command patterns into
branch commands.
