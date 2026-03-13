# Ferrosa Architecture Specs

> Last updated: 2026-03-12

Architecture documentation for Ferrosa, a Rust reimplementation of Apache Cassandra with S3-backed storage.

## Implementation Status

| Crate | Status |
|-------|--------|
| ferrosa-common | Done |
| ferrosa-sstable | Done |
| ferrosa-storage | Not started |
| ferrosa-schema | Not started |
| ferrosa-cql | Not started |
| ferrosa-net | Not started |
| ferrosa-cluster | Not started |
| ferrosa (binary) | Not started |

## Specs Index

| Spec | Description | Status |
|------|-------------|--------|
| [Overview](overview.md) | High-level system overview and design principles | Approved |
| [Components](components.md) | Crate architecture, dependency graph, responsibilities | Approved |
| [Data Flow](data-flow.md) | Write path, read path, compaction, S3 lifecycle | Approved |
| [SSTable](sstable.md) | BTI format, trie encoding, I/O traits, compression, public API | Approved |
| [CQL](cql.md) | CQL native protocol v5, type system, parser, query execution | Approved |
| [Testing](testing.md) | Test infrastructure, suites, performance regression detection | Approved |

## Architecture Decision Records

| ADR | Decision | Status |
|-----|----------|--------|
| [001](decisions/001-write-behind-s3.md) | Write-behind async S3 storage model | Accepted |
| [002](decisions/002-cql-only-compat.md) | CQL client compat only, own internode protocol | Accepted |
| [003](decisions/003-raft-metadata.md) | Raft for metadata, tunable CL for data, transactions deferred | Accepted |
| [004](decisions/004-layered-sstable.md) | Layered SSTable: read Big+BTI, write BTI, future native | Accepted |
| [005](decisions/005-rust-native-crates.md) | Rust-native crates + Java as behavioral oracle | Accepted |
| [006](decisions/006-cql-architecture.md) | CQL: lock-free concurrency, hand-written parser, no ALLOW FILTERING | Accepted |

## Related Documents

- [README](../README.md) — project introduction
- [SSTable Design](../docs/superpowers/specs/2026-03-11-ferrosa-sstable-design.md) — implementation design for ferrosa-sstable
- [CQL Design](../docs/superpowers/specs/2026-03-12-ferrosa-cql-design.md) — implementation design for ferrosa-cql
