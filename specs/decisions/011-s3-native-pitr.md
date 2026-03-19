# ADR-011: S3-Native Point-in-Time Restoration

> Date: 2026-03-18
> Status: Draft

## Context

Ferrosa needs point-in-time restoration (PITR) for disaster recovery. Cassandra implements PITR via local hard-link snapshots + external commit log archiving commands. Ferrosa's S3-backed storage model enables a simpler, cloud-native approach.

Three approaches were considered:

1. **Cassandra-style (local hard links + external archiver)** — port Cassandra's model directly
1. **S3-native (metadata snapshots + built-in archiving)** — leverage S3 as the durable store
1. **S3 versioning** — use S3 object versioning for implicit time-travel

## Decision

S3-native PITR. Snapshots are metadata-only (copy manifest.json + schema.json to a snapshot prefix). Commit log segments are archived directly to S3 by a built-in tokio task. Restoration replays archived segments from the snapshot's commit log position with timestamp filtering.

## Rationale

- **No local file management**: Cassandra hard links require local disk coordination. Ferrosa's SSTables are already in S3, so snapshots don't need to copy data — only small metadata files.
- **No external dependencies**: Cassandra delegates archiving to shell commands (e.g., `rsync`, custom scripts). Ferrosa archives directly to S3 using the existing `object_store` crate, eliminating operational complexity.
- **S3 durability**: Archived segments inherit S3's 11-nines durability. No separate backup storage to manage.
- **Compatible with write-behind model**: ADR-001 already planned "commit log shipping to S3 every 5 seconds." This ADR implements that plan as part of the PITR feature.

## Consequences

- SSTable garbage collection must check all snapshot manifests before deleting objects
- Archive lag monitoring is needed — if the archiver falls behind, the PITR window has gaps
- Restore is a node-level operation (node must be offline); cluster-wide restore is out of scope
- S3 costs increase with retention window length (archived segments accumulate)

## Alternatives Rejected

- **Local hard links**: Ferrosa doesn't maintain long-lived local SSTable files (they're ephemeral cache). Hard links have no target to link to.
- **S3 versioning**: Requires S3 versioning enabled on the bucket (not portable to all S3-compatible stores). Provides no mutation-level timestamp filtering. Higher storage costs due to full-object versioning.
