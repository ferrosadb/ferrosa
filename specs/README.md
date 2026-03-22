# Ferrosa Architecture Specs

> Last updated: 2026-03-20

Architecture documentation for Ferrosa, a Rust reimplementation of Apache Cassandra with S3-backed storage.

## Implementation Status

| Crate | Status |
|-------|--------|
| ferrosa-common | Done |
| ferrosa-sstable | Done |
| ferrosa-storage | Done (core engine + PITR + sidecar indexes + observers + virtual tables) |
| ferrosa-schema | Done (DDL, auth, audit, system KS, graph, virtual tables, UDT, index, BACKUP permission) |
| ferrosa-cql | Done (Parts A-D + observability + compression + UDT/UDF DDL + EXPLAIN + query planner) |
| ferrosa-index | Done (8 secondary index types + 2 vector index types: HNSW, IVFFlat) |
| ferrosa-udf | Done (parser, schema, DDL replication, Wasmtime compilation, router wiring; wit-bindgen invoke TODO) |
| ferrosa-graph | Done (eval, aggregations, var-length paths, SUBSCRIBE, leapfrog triejoin, Bolt v5, HTTP+auth) |
| ferrosa-net | Done (Phase 1 + reconnection, graceful drain) |
| ferrosa-cluster | Done (Phase 1-3 + UDT/index DDL replication) |
| ferrosa-ctl | Done (CLI + TUI + cluster management + snapshot/restore commands) |
| ferrosa (binary) | Done (CQL 9042, graph HTTP 7474, Bolt 7687, web 9090, Prometheus, PITR REST API) |

## Specs Index

| Spec | Description | Status |
|------|-------------|--------|
| [Status](status.md) | Development status, maturity assessment, remaining work | Living |
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
| [Graph Gap Closure](graph-gap-closure.md) | Architecture for remaining graph gaps: SUBSCRIBE, aggregations, var-length paths, Bolt | Complete |
| [Graph Gap FMEA](graph-gap-fmea.md) | Failure modes for graph gap closure (14 failure modes, 14 test cases) | Complete |
| [Graph Gap Project Plan](graph-gap-project-plan.md) | 3-sprint plan: eval+agg, varpath+subscribe, Bolt | Complete |
| [Threat Model — Net/Cluster](threat-model-net-cluster.md) | STRIDE for internode protocol, Raft, pair mode, coordinator | Draft |
| [Threat Model — Schema Replication](threat-model-schema-replication.md) | STRIDE for schema snapshot sync, DDL forwarding (T21-T28) | Draft |
| [Secondary Index Pipeline](secondary-index-pipeline.md) | Secondary index query integration: planner, sidecar persistence, vector indexes | Implemented |
| [Threat Model — Secondary Index](threat-model-secondary-index.md) | STRIDE for secondary index pipeline | Implemented |
| [FMEA — Secondary Index](fmea-secondary-index.md) | Failure modes for secondary index pipeline | Implemented |
| [Secondary Index Project Plan](project-plan-secondary-index.md) | 4-sprint plan: sidecar files, query planner, persistence, vector indexes | Complete |
| [PITR](pitr.md) | Point-in-time restoration: S3-native snapshots, commit log archiving, restore | Implemented |
| [Threat Model — PITR](threat-model-pitr.md) | STRIDE for backup/restore: archive integrity, snapshot tampering, restore safety | Implemented |
| [PITR FMEA](pitr-fmea.md) | Failure modes for PITR (16 modes, 21 test cases) | Implemented |
| [PITR Project Plan](pitr-project-plan.md) | 4-sprint plan: archiving, snapshots, restore, tooling | Complete |
| [Combined Project Plan](project-plan-combined.md) | Parallel index + PITR workstreams, execution status | Complete |
| [Accord Transactions](accord.md) | Accord architecture: component diagrams, data flow, integration map, ADRs | Draft |
| [DSM — Accord](dsm-accord.md) | DSM dependency analysis for Accord integration (fan-in/out, propagation cost) | Draft |
| [Threat Model — Accord](threat-model-accord.md) | STRIDE for Accord: 30 threats across 6 trust boundaries | Draft |
| [FMEA — Accord](fmea-accord.md) | Failure modes for Accord (19 modes, 19 test cases, 3 critical) | Draft |
| [Accord Project Plan](accord-project-plan.md) | 7-sprint plan: foundation, single-key, multi-key, 2i, electorates | Draft |
| [Accord Test Spec](accord-test-spec.md) | 6-layer test pyramid: 97 tests from unit to 24-step capstone | Draft |
| [Nightly Test Infrastructure](../superpowers/specs/2026-03-19-nightly-test-infrastructure-design.md) | Nightly test infrastructure design | Draft |
| [Cluster Phase 2 Design](../superpowers/specs/2026-03-14-cluster-phase2-design.md) | Raft consensus, token ring, coordinator pattern | Implemented |

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
| [011](decisions/011-s3-native-pitr.md) | S3-native PITR — metadata snapshots + built-in commit log archiving | Draft |

## Related Documents

- [README](../README.md) — project introduction
- [SSTable Design](../superpowers/specs/2026-03-11-ferrosa-sstable-design.md) — implementation design for ferrosa-sstable
- [Storage Design](../superpowers/specs/2026-03-11-ferrosa-storage-design.md) — implementation design for ferrosa-storage
- [Schema Design](../superpowers/specs/2026-03-12-ferrosa-schema-design.md) — implementation design for ferrosa-schema
- [CQL Design](../superpowers/specs/2026-03-12-ferrosa-cql-design.md) — implementation design for ferrosa-cql
- [Graph Design](../superpowers/specs/2026-03-12-ferrosa-graph-design.md) — implementation design for ferrosa-graph
- [Observability Design](../superpowers/specs/2026-03-13-ferrosa-observability-design.md) — implementation design for observability (virtual tables, web dashboard, ferrosa-ctl)
- [Net/Cluster Design](../superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md) — implementation design for ferrosa-net + ferrosa-cluster
- [Schema Replication Design](../superpowers/specs/2026-03-14-schema-replication-design.md) — schema snapshot sync + DDL forwarding
