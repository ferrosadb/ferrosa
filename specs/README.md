# Ferrosa Architecture Specs

> Last updated: 2026-03-13

Architecture documentation for Ferrosa, a Rust reimplementation of Apache Cassandra with S3-backed storage.

## Implementation Status

| Crate | Status |
|-------|--------|
| ferrosa-common | Done |
| ferrosa-sstable | Done |
| ferrosa-storage | Done (core engine + observers + subscription observer + storage stats) |
| ferrosa-schema | Done (DDL, auth, audit, system KS, graph extensions, virtual tables) |
| ferrosa-cql | Done (Parts A-D + observability: virtual tables, subscriptions, prometheus) |
| ferrosa-graph | Done (Phase 1: parser, planner, executor, adjacency index, HTTP endpoint) |
| ferrosa-net | Not started |
| ferrosa-cluster | Not started |
| ferrosa-ctl | Done (CLI + TUI monitoring dashboard) |
| ferrosa (binary) | Done (single-node: CQL on 9042, optional graph on 7474, web console on 9090) |

## Specs Index

| Spec | Description | Status |
|------|-------------|--------|
| [Overview](overview.md) | High-level system overview and design principles | Approved |
| [Components](components.md) | Crate architecture, dependency graph, responsibilities | Approved |
| [Data Flow](data-flow.md) | Write path, read path, compaction, S3 lifecycle | Approved |
| [SSTable](sstable.md) | BTI format, trie encoding, I/O traits, compression, public API | Approved |
| [Storage](storage.md) | Storage engine: memtable, flush, merge, commit log, compaction, S3 upload, cache, engine composition | Approved |
| [Testing](testing.md) | Test infrastructure, suites, performance regression detection | Approved |
| [CQL](cql.md) | CQL native protocol v5, parser, query routing, prepared cache | Approved |
| [Schema](schema.md) | Schema management: metadata types, registry, auth, system keyspaces | Approved |
| [Threat Model](threat-model.md) | STRIDE threat analysis: trust boundaries, threat inventory, mitigations | Draft |
| [Threat Model — CQL B/C](threat-model-cql-bc.md) | STRIDE for CQL parser, routing, prepared cache | Approved |
| [Threat Model — Graph](threat-model-graph.md) | STRIDE for graph engine, HTTP endpoint, adjacency index | Draft |

## Architecture Decision Records

| ADR | Decision | Status |
|-----|----------|--------|
| [001](decisions/001-write-behind-s3.md) | Write-behind async S3 storage model | Accepted |
| [002](decisions/002-cql-only-compat.md) | CQL client compat only, own internode protocol | Accepted |
| [003](decisions/003-raft-metadata.md) | Raft for metadata, tunable CL for data, transactions deferred | Accepted |
| [004](decisions/004-layered-sstable.md) | Layered SSTable: read Big+BTI, write BTI, future native | Accepted |
| [005](decisions/005-rust-native-crates.md) | Rust-native crates + Java as behavioral oracle | Accepted |
| [006](decisions/006-auth-first-schema.md) | Auth-first schema design — auth baked into registry from day one | Accepted |
| [007](decisions/007-configurable-password-hashing.md) | Configurable password hashing — bcrypt default, argon2id optional | Accepted |
| [008](decisions/008-audit-first-schema.md) | Audit-first schema design — audit logging baked into registry from day one | Accepted |
| [009](decisions/009-pluggable-secrets-provider.md) | Pluggable secrets provider — env default, AWS SM/Vault/file backends | Accepted |
| [010](decisions/010-production-mode.md) | Production mode — mandatory encryption at all layers, fail-closed startup | Accepted |

## Related Documents

- [README](../README.md) — project introduction
- [SSTable Design](../docs/superpowers/specs/2026-03-11-ferrosa-sstable-design.md) — implementation design for ferrosa-sstable
- [Storage Design](../docs/superpowers/specs/2026-03-11-ferrosa-storage-design.md) — implementation design for ferrosa-storage
- [Schema Design](../docs/superpowers/specs/2026-03-12-ferrosa-schema-design.md) — implementation design for ferrosa-schema
- [CQL Design](../docs/superpowers/specs/2026-03-12-ferrosa-cql-design.md) — implementation design for ferrosa-cql
- [Graph Design](../docs/superpowers/specs/2026-03-12-ferrosa-graph-design.md) — implementation design for ferrosa-graph
- [Observability Design](../docs/superpowers/specs/2026-03-13-ferrosa-observability-design.md) — implementation design for observability (virtual tables, web dashboard, ferrosa-ctl)
