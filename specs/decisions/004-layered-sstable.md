# ADR-004: Layered SSTable Format Support

> Date: 2026-03-11
> Status: Accepted

## Context

Cassandra has two SSTable formats: Big (legacy) and BTI (trie-based, default in 5.x). Ferrosa needs to read existing Cassandra data for migration and may benefit from an S3-optimized native format.

## Decision

Layered approach:

1. Read both Big and BTI formats (migration from any Cassandra version)
1. Write BTI format (good performance, trie-based indexing)
1. Future: native Ferrosa format behind a feature flag (S3-optimized)

## Rationale

- BTI has shown good performance characteristics and translates well to Rust
- Reading Big format enables migration from older Cassandra deployments
- Writing BTI first avoids the risk of designing a new format before understanding real workloads
- Feature flag allows incremental rollout of native format
- Migration path: Cassandra → Ferrosa (BTI mode) → Ferrosa (native mode, when ready)

## Consequences

- Must maintain multiple format readers (Big + BTI)
- `ferrosa-sstable` crate is the first deliverable and must be thoroughly correct
- Native format design deferred until S3 access patterns are well-understood from real usage
- SSTable import tool handles one-way Cassandra → Ferrosa migration
