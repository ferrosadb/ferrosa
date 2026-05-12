---
executive_summary:
  purpose: "Captures the requirements grill for PITR branch/copy support."
  critical_items:
    - "The user confirmed cheap object-storage forks with copy-on-write divergence."
    - "S3 symlink behavior should be implemented as explicit object references in branch manifests."
    - "No further blocking product questions remain for the architecture spec."
---

# PITR Branch/Copy Requirements Grill

This log records decisions resolved while specifying PITR branch/copy support.
Code-resolvable questions were answered from the Ferrosa repository; the only
strategic question was answered by the user on the kanban task thread.

## Decisions

### D001: Existing PITR surface is snapshot/restore oriented

- Actor: Architect
- Category: contract
- Auto-resolved: true
- Dependencies: []
- Status: confirmed
- Source: grill-me
- Decision: The repo already exposes `/api/snapshots`,
  `/api/restore/preflight`, and `/api/restore`, plus `ferrosa-ctl snapshot` and
  `ferrosa-ctl restore` commands.

### D002: Snapshots are S3 metadata checkpoints, not data copies

- Actor: Architect
- Category: data
- Auto-resolved: true
- Dependencies: []
- Status: confirmed
- Source: grill-me
- Decision: ADR-011 and `ferrosa-storage/src/snapshot/*` define snapshots as
  metadata copies of manifest/schema with immutable SSTables shared by
  reference.

### D003: The new branch feature should be CLI + web API

- Actor: Architect
- Category: arch
- Auto-resolved: true
- Dependencies: [D001]
- Status: confirmed
- Source: grill-me
- Decision: `ferrosa-ctl` snapshot/restore commands are thin HTTP clients, and
  the web module is the existing PITR control-plane boundary.

### D004: Branch creation uses cheap object-storage forks

- Actor: User
- Category: scope
- Auto-resolved: false
- Dependencies: [D002]
- Status: confirmed
- Source: grill-me
- Decision: Branches should share immutable source SSTables by reference and
  write divergent data into branch-local prefixes or buckets rather than
  deep-copying all source SSTable objects.

### D005: The S3 symlink analogue is explicit manifest `ObjectRef`

- Actor: Architect
- Category: data
- Auto-resolved: true
- Dependencies: [D004]
- Status: confirmed
- Source: grill-me
- Decision: S3 has no symlinks, and provider-specific marker objects would hide
  references from readers and GC. An object reference in each manifest entry
  makes sharing visible, auditable, and testable.

### D006: Branch GC must be reference-aware

- Actor: Architect
- Category: ops
- Auto-resolved: true
- Dependencies: [D002, D005]
- Status: confirmed
- Source: grill-me
- Decision: Existing snapshot semantics already preserve referenced SSTables.
  Branches extend the GC root set to include branch manifests and lineage
  records.

### D007: Public docs must show copy-on-write branch examples

- Actor: Architect
- Category: contract
- Auto-resolved: true
- Dependencies: [D004]
- Status: confirmed
- Source: grill-me
- Decision: The task requires docs examples that make the website. Examples
  should be operator-facing and must not imply deep-copy semantics.
