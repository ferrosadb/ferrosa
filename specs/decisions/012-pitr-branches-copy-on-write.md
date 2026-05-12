---
executive_summary:
  purpose: "Records the storage semantics for PITR branches in Ferrosa."
  critical_items:
    - "PITR branches use explicit object references as the S3 symlink analogue."
    - "Branch creation is metadata-only by default; deep copies are not the default behavior."
    - "Reference-aware GC is required before branch deletion or compaction can reclaim shared objects."
---

# ADR-012: PITR Branches Use Copy-on-Write Object References

> Date: 2026-05-12
> Status: Draft

## Context

Ferrosa already implements S3-native PITR. ADR-011 defines snapshots as metadata
checkpoints: snapshot creation copies `manifest.json`, `schema.json`, and
`metadata.json`; immutable SSTable objects remain shared by reference.

Users now need writable database branches from a PITR checkpoint. S3 does not
support symlinks, so Ferrosa needs an explicit design for sharing immutable
source objects while allowing new writes to diverge into branch-owned storage.

## Decision

PITR branches are cheap copy-on-write forks. Creating a branch from a snapshot
writes branch metadata, schema, lineage, and a branch manifest. The branch
manifest initially references source SSTable objects through explicit object
references. It does not copy SSTable bytes by default.

The S3 symlink analogue is an `ObjectRef` embedded in each manifest entry. The
reference records the object-store identity, bucket or store scope, prefix, key,
etag or version when available, checksum when available, and optional owner
branch.

## Rationale

- **Fast branch creation**: metadata-only branch creation is proportional to the
  manifest size, not the data set size.
- **S3-compatible**: explicit references work with S3 and S3-compatible stores;
  they do not require symlinks, redirects, object versioning, or provider-only
  features.
- **Fail-loud integrity**: etag, version, and checksum fields let readers detect
  overwritten or stale source objects instead of silently reading wrong data.
- **Natural divergence**: new flushes and compactions write to the branch target
  prefix, so branch storage grows only as it changes.
- **Auditable lineage**: branch metadata records the source snapshot, source
  prefix, creation time, and optional point-in-time cutoff.

## Consequences

- Manifest entries need a versioned schema upgrade from local `id` entries to
  entries with durable `ObjectRef` data.
- SSTable readers and restore/branch code must dereference entries through the
  object reference instead of assuming the current prefix owns every object.
- Garbage collection must include branch manifests and snapshot manifests as
  roots before deleting any object.
- Branch deletion must delete only branch-owned unreferenced objects and branch
  metadata. It must not delete source objects that are still referenced.
- A later `--materialize` mode can deep-copy objects when operators need storage
  isolation from source lifecycle policies.

## Alternatives rejected

### Deep-copy branches by default

Deep copies are simple to reason about but turn branch creation into a bulk data
copy operation. That contradicts Ferrosa's S3-native PITR model and makes large
branches slow and expensive.

### S3 object versioning

Object versioning can preserve overwritten objects, but it is not portable to all
S3-compatible stores and does not express branch lineage or ownership in the
Ferrosa manifest.

### Zero-byte marker objects as symlinks

Marker objects would hide an application-level reference behind provider-specific
conventions. Readers would still need custom dereference logic, so the reference
belongs in the manifest where validation and GC can see it.

## Related documents

- [ADR-011: S3-Native Point-in-Time Restoration](011-s3-native-pitr.md)
- [PITR Branch/Copy Architecture](../pitr-branch-copy-architecture.md)
