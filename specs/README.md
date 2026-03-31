# Ferrosa Architecture Specs

> Last updated: 2026-03-30

Architecture documentation for Ferrosa, a Rust reimplementation of Apache Cassandra with S3-backed storage.

## Implementation Status

| Crate | Status |
|-------|--------|
| ferrosa-common | Complete (types, keys, tokens, Accord HLC/TxnId/Ballot) |
| ferrosa-sstable | Complete (read Big+BTI, write BTI) |
| ferrosa-storage | Complete (engine, commit log, compaction, S3 write-behind, cache, NVMe pinning, observers, Accord storage) |
| ferrosa-schema | Complete (DDL, auth, audit, system KS, graph, virtual tables, UDT, index, BACKUP permission) |
| ferrosa-cql | Complete (Parts A-D, observability, compression, UDT/UDF DDL, EXPLAIN, query planner, LWT, transactions, pagination, fts_match) |
| ferrosa-index | Complete (BTree, Hash, Composite, Phonetic, Filtered, Vector HNSW/IVFFlat, **FullText**) |
| ferrosa-udf | Complete (parser, schema, DDL replication, Wasmtime compilation, router wiring) |
| ferrosa-graph | Complete (eval, aggregations, var-length paths, SUBSCRIBE, leapfrog triejoin, Bolt v5, HTTP+auth) |
| ferrosa-net | Complete (Phase 1 + reconnection, graceful drain) |
| ferrosa-cluster | Complete (Phase 1-3, UDT/index DDL replication, Accord A1-A7, pair mode) |
| ferrosa-jepsen | In Progress (infrastructure complete, C4 run tasks need live cluster) |
| ferrosa-ctl | Complete (CLI + TUI + cluster management + snapshot/restore) |
| ferrosa (binary) | Complete (CQL 9042, graph HTTP 7474, Bolt 7687, web 9090, Prometheus, PITR REST API) |

---

## Architecture Specs

| Spec | Description | Status |
|------|-------------|--------|
| [Overview](overview.md) | High-level system overview and design principles | Approved |
| [Components](components.md) | Crate architecture, dependency graph, responsibilities | Approved |
| [Data Flow](data-flow.md) | Write path, read path, compaction, S3 lifecycle | Approved |
| [SSTable](sstable.md) | BTI format, trie encoding, I/O traits, compression | Approved |
| [Storage](storage.md) | Storage engine: memtable, flush, compaction, S3, cache | Approved |
| [CQL](cql.md) | CQL native protocol v5, parser, query routing, LWT, pagination | Approved |
| [Testing](testing.md) | Test infrastructure, suites, Jepsen, performance detection | Approved |
| [Cancel Safety](cancel-safety-conventions.md) | Async cancel safety conventions | Approved |

## Feature Specs

| Spec | Description | Status |
|------|-------------|--------|
| [Accord](accord.md) | Accord consensus: state machine, coordinator, conflict detection | Complete |
| [NVMe Pinning](nvme-pinning-architecture.md) | Per-table NVMe pinning: skip S3, pin in cache, max_bytes cap | Implemented |
| [Full-Text Indexing](fulltext-index-architecture.md) | Inverted index sidecars, analyzer pipeline, BM25, CQL fts_match() | Implemented |
| [Secondary Index Pipeline](secondary-index-pipeline.md) | Query integration, sidecar persistence, vector indexes | Implemented |
| [PITR](pitr.md) | Point-in-time restoration: S3-native snapshots, commit log archiving | Implemented |
| [Graph Gap Closure](graph-gap-closure.md) | SUBSCRIBE, aggregations, var-length paths, Bolt v5 | Complete |
| [UCS Compaction](ucs-compaction-architecture.md) | Unified Compaction Strategy: density-based levels, fan factor, per-table DDL | New |
| [Jepsen E2E](jepsen-e2e-test-plan.md) | Accord transaction verification: topologies, nemeses, workloads | Approved |

## Threat Models

| Spec | Scope | Status |
|------|-------|--------|
| [Threat Model](threat-model.md) | Ferrosa system-wide STRIDE | Draft |
| [TM — CQL B/C](threat-model-cql-bc.md) | CQL parser, routing, prepared cache | Approved |
| [TM — Net/Cluster](threat-model-net-cluster.md) | Internode protocol, Raft, pair mode | Draft |
| [TM — Schema Replication](threat-model-schema-replication.md) | Schema sync, DDL forwarding | Draft |
| [TM — Accord](threat-model-accord.md) | 30 threats across 6 trust boundaries | Complete |
| [TM — Secondary Index](threat-model-secondary-index.md) | Index pipeline security | Implemented |
| [TM — PITR](threat-model-pitr.md) | Backup/restore integrity | Implemented |
| [TM — Graph](threat-model-graph.md) | Graph engine, HTTP endpoint | Draft |

## Failure Mode Analysis (FMEA)

| Spec | Scope | Status |
|------|-------|--------|
| [FMEA — Accord](fmea-accord.md) | 19 failure modes, 19 test cases | Complete |
| [FMEA — Secondary Index](fmea-secondary-index.md) | Index pipeline failures | Implemented |
| [FMEA — PITR](pitr-fmea.md) | 16 modes, 21 test cases | Implemented |
| [FMEA — RRD Timeseries](fmea-rrd-timeseries.md) | Cascading time-series aggregation | Implemented |
| [FMEA — Graph](graph-gap-fmea.md) | 14 modes, 14 test cases | Complete |

## DSM Analysis

| Spec | Scope | Status |
|------|-------|--------|
| [DSM — Accord](dsm-accord.md) | Fan-in/out, propagation cost for Accord integration | Complete |
| [DSM + TM + FMEA — UCS](ucs-compaction-analysis.md) | Compaction subsystem: 15 modules, 10 STRIDE threats, 15 FMEA modes | New |

## Project Plans

### Active

| Plan | Scope | Status |
|------|-------|--------|
| [UCS Compaction](project-plan-ucs-compaction.md) | 4 sprints: metadata, UCS strategy, integration, equivalence | New |
| [Correctness Sprints](project-plan-correctness-sprints.md) | C1-C8: bugs, storage fixes, Jepsen, SSTable compat, Accord, compaction S3, drivers | C1-C7 Complete, C4/C8 remaining |
| [Unified Plan](project-plan-unified.md) | Ferrosa ecosystem roadmap: core DB, memory, dbaas, Temporal | Active |

### Completed

| Plan | Scope | Status |
|------|-------|--------|
| [NVMe + FTS](project-plan-nvme-fts.md) | NVMe pinning (NV1) + full-text indexing (FT1-FT2) | Complete |
| [Accord](accord-project-plan.md) | 7 sprints: foundation through electorate reconfiguration | Complete |
| [Secondary Index](project-plan-secondary-index.md) | 4 sprints: sidecar files, query planner, persistence, vector | Complete |
| [PITR](pitr-project-plan.md) | 4 sprints: archiving, snapshots, restore, tooling | Complete |
| [Combined (Index + PITR)](project-plan-combined.md) | Parallel workstreams execution | Complete |
| [Graph Gap](graph-gap-project-plan.md) | 3 sprints: eval+agg, varpath+subscribe, Bolt | Complete |

## Compiled Plans (Agent-Executable)

| Plan | Scope | Status |
|------|-------|--------|
| [Compiled — Correctness](compiled-project-plan.md) | C1-C8 tasks with dependency DAG, 31/32 complete | Near-Complete |
| [Compiled — NVMe + FTS](compiled-plan-nvme-fts.md) | 26 tasks across 6 batches | Complete |
| [Compiled — UCS Compaction](compiled-plan-ucs-compaction.md) | 13 work packets, 4 batches, 22 tasks | New |

## TDD Plans

| Plan | Scope | Tests | Status |
|------|-------|-------|--------|
| [TDD — C1/C2/C3/C7](tdd-plan-c1-c2-c3-c7.md) | Bug fixes, P0 storage, Jepsen infra, compaction S3 | 40 | Complete |
| [TDD — NVMe + FTS](tdd-plan-nvme-fts.md) | NVMe pinning + full-text indexing integration | 41 | Complete |
| [TDD — UCS Compaction](tdd-plan-ucs-compaction.md) | Unified Compaction Strategy: density, levels, fan factor | 22 | New |

## Test Specs (Accord)

| Spec | Layer | Status |
|------|-------|--------|
| [Accord Test Spec](accord-test-spec.md) | 6-layer pyramid: 97 tests unit to capstone | Complete |
| [Accord Test Infrastructure](accord-test-infrastructure.md) | Test framework, fixtures, mocks | Complete |
| [Accord Test Integration](accord-test-integration.md) | Sprint S3 integration tests | Complete |
| [Accord Test System](accord-test-system.md) | System-level end-to-end tests | Complete |
| [Accord Test MemIndex/2i](accord-test-memindex-2i.md) | MemIndex, transactional 2i, sidecars | Complete |
| [Accord Test Multikey](accord-test-multikey-electorate.md) | Multi-key transactions, electorate reconfig | Complete |
| [Accord Test UDF](accord-test-udf-integration.md) | UDF/UDA integration with Accord | Specification |

## Architecture Decision Records

| ADR | Decision | Status |
|-----|----------|--------|
| [001](decisions/001-write-behind-s3.md) | Write-behind async S3 storage model | Accepted |
| [002](decisions/002-cql-only-compat.md) | CQL client compat only, own internode protocol | Accepted |
| [003](decisions/003-raft-metadata.md) | Raft for metadata, tunable CL for data | Accepted |
| [004](decisions/004-layered-sstable.md) | Layered SSTable: read Big+BTI, write BTI, future native | Accepted |
| [005](decisions/005-rust-native-crates.md) | Rust-native crates + Java as behavioral oracle | Accepted |
| [006](decisions/006-auth-first-schema.md) | Auth-first schema design | Accepted |
| [006b](decisions/006-cql-architecture.md) | CQL architecture | Accepted |
| [007](decisions/007-configurable-password-hashing.md) | Configurable password hashing (bcrypt/argon2id) | Accepted |
| [008](decisions/008-audit-first-schema.md) | Audit-first schema design | Accepted |
| [009](decisions/009-pluggable-secrets-provider.md) | Pluggable secrets provider (env/AWS SM/Vault) | Accepted |
| [010](decisions/010-production-mode.md) | Production mode — mandatory encryption, fail-closed | Accepted |
| [011](decisions/011-s3-native-pitr.md) | S3-native PITR — metadata snapshots + commit log archiving | Accepted |

## Living Documents

| Doc | Purpose |
|-----|---------|
| [Status](status.md) | Development status, maturity assessment, remaining work |
