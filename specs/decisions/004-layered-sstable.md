# ADR-004: Layered SSTable Format Support

> Date: 2026-03-11
> Status: Accepted

## Context

Cassandra has two SSTable formats: Big (legacy) and BTI (trie-based, default in 5.x). Ferrosa needs to read existing Cassandra data for migration and may benefit from an S3-optimized native format.

## Decision

Layered approach, implemented in phases:

1. **Phase 1**: BTI format read + write (Cassandra 5.x default, trie-based indexing)
1. **Phase 2**: Big format read (migration from older Cassandra deployments)
1. **Phase 3**: Native Ferrosa format behind a feature flag (S3-optimized)

## Rationale

- BTI has shown good performance characteristics and translates well to Rust
- Reading Big format enables migration from older Cassandra deployments
- Writing BTI first avoids the risk of designing a new format before understanding real workloads
- Feature flag allows incremental rollout of native format
- Migration path: Cassandra → Ferrosa (BTI mode) → Ferrosa (native mode, when ready)

## Consequences

- Phase 1 focuses exclusively on BTI — no multi-format complexity until Big reader is needed
- `ferrosa-sstable` crate is the second deliverable (after ferrosa-common) and must be thoroughly correct
- Abstract `ReadAt`/`WriteAt` I/O traits decouple SSTable logic from file system vs S3
- Native format design deferred until S3 access patterns are well-understood from real usage
- SSTable import tool (Phase 2) handles one-way Cassandra → Ferrosa migration
- See [SSTable Format Specification](../sstable.md) for detailed design
