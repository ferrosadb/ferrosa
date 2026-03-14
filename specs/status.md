# Ferrosa Development Status

> Last updated: 2026-03-14
> Status: Living document

## Overview

Ferrosa is a **two-node pair-mode distributed CQL-compatible database** with graph
query support, built-in observability, and S3-backed storage. The internode transport
(ferrosa-net Phase 1) and pair mode cluster (ferrosa-cluster Phase 1) are complete,
including DDL replication, write forwarding, failover, and catch-up.

| Metric | Value |
|--------|-------|
| Crates | 10 of 10 planned |
| Source files | ~180 |
| Source LOC | ~56,700 |
| Test functions | ~1,082 |
| Integration test files | 20 |

## Maturity Assessment

```text
               Spec'd   Coded   Tested   Prod-ready
common         ██████   ██████  ██████   ████░░
sstable        ██████   ██████  ██████   ████░░
storage        ██████   ██████  █████░   ███░░░
schema         ████░░   █████░  █████░   ███░░░
cql            ██████   ██████  ██████   ████░░
graph          █████░   ██████  ████░░   ███░░░
ctl            ██████   ██████  ███░░░   ███░░░
binary         █████░   ██████  ███░░░   ██░░░░
net            █████░   █████░  ████░░   ██░░░░
cluster        █████░   ████░░  ███░░░   ██░░░░
```

## Crate Status

### ferrosa-common — Complete

- **LOC:** 1,133 (9 files) | **Tests:** 36
- **Modules:** `cell`, `data_type`, `error`, `key`, `murmur3`, `schema`, `token`
- **What's done:** Token, PartitionKey, DecoratedKey, CellValue, Murmur3 partitioner.
  Property tests via optional `test-generators` feature.
- **Remaining:** More property tests for edge cases.

### ferrosa-sstable — Complete (BTI format)

- **LOC:** 8,250 (19 files) | **Tests:** 177
- **Modules:** `bloom`, `byte_comparable`, `compression`, `data`, `io`, `marshal`,
  `partition_index`, `reader`, `row_index`, `statistics`, `toc`, `trie`, `types`,
  `varint`, `writer`
- **What's done:** Full BTI read/write. On-disk trie (16 node types, page-aware packing),
  Bloom filter, LZ4/Zstd compression, byte-comparable keys, Cassandra compat tests.
- **Remaining:**
  - [ ] Big format reader (read-only compat for existing Cassandra SSTables)
  - [ ] Native Ferrosa SSTable format (behind feature flag)
  - [ ] `sstable-dump` / `sstable-import` CLI tools

### ferrosa-storage — Mostly Complete (Parts A/B/C)

- **LOC:** 9,278 (29 files) | **Tests:** 204
- **Modules:** `cache`, `commitlog` (7 submodules), `compaction` (3 submodules),
  `engine`, `flush`, `manifest`, `memtable` (2 impls), `merge`, `observer`, `store`,
  `subscription_observer`, `upload`, `virtual_tables`
- **What's done:** Memtable (sharded BTree + skiplist), commit log (CAS-allocated
  segments, 3 sync modes, CDC, `force_sync` for catch-up), flush, merge, compaction
  (STCS strategy), S3 upload manager, manifest with etag CAS, local LRU cache,
  WriteObserver trait, SubscriptionObserver.
- **Remaining:**
  - [x] ~~Commit log replay integration~~ (merged PR #38)
  - [x] ~~Compaction execution merge I/O~~ (merged PR #38)
  - [ ] LCS and TWCS compaction strategies
  - [ ] Disk backpressure
  - [ ] `io_uring` I/O backend
  - [ ] Manifest CAS retry loop (T23 — designed, needs wiring)
  - [ ] S3 bucket policy validation at startup (T22 — verify encryption enabled)

### ferrosa-schema — Mostly Complete (Chunk A)

- **LOC:** 7,348 (27 files) | **Tests:** 204
- **Modules:** `audit` (3 submodules), `auth` (4 submodules), `convert`, `error`,
  `metadata` (3 submodules), `registry`, `secrets`, `startup`, `system` (4 submodules),
  `virtual_registry`, `virtual_table`
- **What's done:** Schema registry with `ArcSwap` lock-free snapshots, full RBAC auth
  (bcrypt/argon2), column-level permissions, rate limiting, audit logging (log + table
  sinks), system keyspace queries, VirtualTable trait + registry. Schema replication:
  `apply_snapshot()`, idempotent `*_internal()` methods for pair mode.
- **Remaining (Chunks B-F):**
  - [ ] DDL validation rules
  - [ ] System table persistence to SSTable
  - [ ] UDT (user-defined type) support
  - [ ] Role hierarchy with inheritance
  - [ ] Audit sink composition

### ferrosa-cql — Complete (Parts A-D + Compression)

- **LOC:** ~12,200 (20 files) | **Tests:** ~248 | **Largest crate**
- **Modules:** `ast`, `auth`, `bridge`, `client`, `connection`, `error`, `frame`,
  `lexer`, `parser`, `prepared`, `prometheus`, `result`, `router`, `server`,
  `subscribe`, `types`, `virtual_tables` (connections + active_queries)
- **What's done:** CQL v5 framing (16 opcodes), full type system, SASL PLAIN auth,
  LL(2) recursive-descent parser, query routing (DDL to schema, DML to storage),
  prepared statement cache (moka W-TinyLFU), ConnectionTracker/QueryTracker virtual
  tables, SUBSCRIBE/UNSUBSCRIBE extensions, Prometheus text exposition, CqlClient,
  LZ4 and Snappy frame compression with negotiation. DDL routes through `DdlPath`
  for pair mode replication.
- **Remaining:**
  - [ ] CQL TLS via rustls (T02/T03 — Critical, plaintext traffic)
  - [ ] Per-IP rate limiting for connection/query flood (T04)
  - [ ] EVENT push notifications
  - [ ] ALLOW FILTERING support
  - [ ] Logged batch atomicity
  - [ ] UDT support
  - [ ] Query tracing

### ferrosa-graph — Phase 1 Complete

- **LOC:** 5,547 (20 files) | **Tests:** 121
- **Modules:** `adjacency` (observer, reconcile, schema), `engine`, `error`,
  `executor` (expand, result), `http`, `parser` (ast, lexer, parse_impl, token),
  `planner` (logical, physical)
- **What's done:** Cypher subset parser, logical planner with label resolution +
  per-hop auth, physical planner, expand executor with resource limits, adjacency
  index with WriteObserver, background reconciliation, HTTP/JSON endpoint with
  auth, TLS, error sanitization, audit logging.
- **Future (Phases 2-3):**
  - [ ] Full adjacency reconciliation scan (T5 — stub, needs row-level verification)
  - [ ] WCO (worst-case optimal) joins
  - [ ] Leapfrog triejoin
  - [ ] Variable-length paths
  - [ ] Aggregations
  - [ ] Bolt protocol support

### ferrosa-ctl — Complete

- **LOC:** 1,047 (3 files) | **Tests:** 31
- **Modules:** `commands`, `tui`
- **What's done:** CLI admin tool (clap). Commands: `query`, `describe`, `monitor`,
  `metrics`. TUI monitor dashboard (ratatui/crossterm) with 5 panels, auto-refresh,
  keyboard navigation.
- **Remaining:**
  - [ ] Integration tests (currently unit tests only)

### ferrosa (binary) — Complete (pair-mode)

- **LOC:** ~770 (5 files) | **Tests:** ~15
- **Modules:** `web` (api, static_files)
- **What's done:** Composes all crates. CQL server on :9042, graph HTTP on :7474,
  web console on :9090. Connection + query tracker wiring, REST API for
  metrics/schema/queries/cluster management, embedded static assets via rust-embed.
  Cluster management endpoints: `GET /api/cluster/status`,
  `POST /api/cluster/promote`, `POST /api/cluster/switchover`.
- **Remaining:**
  - [ ] Graceful shutdown sequencing
  - [ ] Configuration file support (currently env vars only)

### ferrosa-net — Phase 1 Complete (PR #39)

- **LOC:** 2,383 (14 files) | **Tests:** 43 (40 unit + 3 integration)
- **Modules:** `codec`, `config`, `discovery` (seeds), `error`, `handshake`, `message`,
  `peer`, `pool`, `rpc` (handler, server, client)
- **What's done:** 12-byte binary wire protocol with 3 priority lanes (Raft/Data/Bulk),
  24 message types (including schema replication: PairSchemaSync, PairDdlForward,
  PairDdlAck), PSK-authenticated handshake (HMAC-SHA256), RPC server with connection
  limits + handshake timeout, RPC client with request-response and fire-and-forget,
  `PriorityPool` (3 TCP connections per peer), static seed discovery, `PeerManager` with
  heartbeat-based failure detection. Proptest fuzzing for message decode. No dependency
  on ferrosa-common.
- **Remaining (Phase 2):**
  - [ ] TLS via rustls for internode encryption
  - [ ] Connection reconnection and backoff
  - [ ] Graceful shutdown / drain
  - [ ] Compression (LZ4/Snappy frame-level)
  - [ ] Metrics and tracing integration
  - [ ] Zero-copy serialization (Cap'n Proto / FlatBuffers / rkyv) for wire protocol
- **Spec:** [Net/Cluster Design](../superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)
- **Threat Model:** [Net/Cluster Threats](threat-model-net-cluster.md)

### ferrosa-cluster — Phase 1 Complete (Pair Mode)

- **LOC:** 2,756 (16 files) | **Tests:** 33 (29 unit + 4 integration)
- **Modules:** `config`, `consistency`, `controller`, `ddl_path`, `error`, `mode`,
  `pair` (catchup, coordinator, ddl, handler, node, switchover), `state`, `write_path`
- **What's done:** Pair mode with full failover lifecycle:
  - `PairCoordinator` — write forwarding (secondary → primary) + replication
  - `DdlCoordinator` + `DdlPath` — DDL forwarding/replication through primary
  - `PairSchemaSyncHandler` — schema snapshot catch-up on rejoin
  - `ModeController` — runtime mode transitions (standalone → pair → degraded)
  - `WritePath::Unavailable` — degraded mode rejects writes when peer lost
  - `force_promote()` — operator promotes to standalone primary
  - `switchover()` — swap primary/secondary roles (both nodes required)
  - Reverse connection on inbound peer for bidirectional RPC
  - Auto re-pair with force-promoted role override on peer rejoin
  - Commit log `force_sync` for catch-up data replay
  - `ConsistencyLevel` with `blockFor()` + property tests
  - Docker smoke test: 5-phase lifecycle (bidirectional writes, failover,
    promotion, catch-up with schema + data, switchover)
- **Remaining (Phase 2 — Full Cluster):**
  - [ ] Raft metadata (openraft) for schema + topology
  - [ ] Token ring with Murmur3 partitioner
  - [ ] Tunable consistency levels (ONE, QUORUM, ALL)
  - [ ] Coordinator pattern for write/read fan-out
  - [ ] Hinted handoff and repair
  - [ ] Node lifecycle (join, leave, bootstrap)
  - [ ] AlterKeyspace/AlterTable DDL forwarding
  - [ ] `StorageEngine::unregister_table()` for drop replication cleanup
- **Spec:** [Net/Cluster Design](../superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)
- **Schema Replication Spec:** [Schema Replication](../superpowers/specs/2026-03-14-schema-replication-design.md)
- **Threat Models:** [Net/Cluster](threat-model-net-cluster.md), [Schema Replication](threat-model-schema-replication.md)

## Active Work in Progress

| Item | Location | State |
|------|----------|-------|
| ~~Storage replay + compaction execution~~ | ~~`.worktrees/storage-replay-compaction`~~ | Merged (PR #38) |
| ~~ferrosa-net Phase 1~~ | ~~`ferrosa-net/`~~ | Complete (PR #39) |
| ~~ferrosa-cluster Phase 1 (Pair mode)~~ | ~~`feature/pair-integration`~~ | Complete |
| ferrosa-cluster Phase 2 (Full cluster) | — | Next up |

## Path to Distributed Operation

The critical path from single-node to multi-node:

1. ~~**ferrosa-storage:** Commit log replay + compaction execution~~ (Done — PR #38)
1. ~~**ferrosa-net:** Internode transport (Phase 1)~~ (Done — PR #39)
1. ~~**ferrosa-cluster:** Pair mode — write forwarding, DDL replication, failover~~ (Done)
1. **ferrosa-schema:** System table persistence (Chunk B)
1. **ferrosa-cluster:** Raft metadata, ring topology, request routing
1. **ferrosa-cluster:** Tunable consistency levels (ONE, QUORUM, ALL)
1. **ferrosa-cluster:** Hinted handoff and repair

## Related Documents

- [Components](components.md) — crate dependency graph
- [Overview](overview.md) — system architecture
- [Architecture Design](../superpowers/specs/2026-03-11-ferrosa-architecture-design.md) — full design spec
- [Schema Replication Design](../superpowers/specs/2026-03-14-schema-replication-design.md) — DDL replication spec
- [Schema Replication Threat Model](threat-model-schema-replication.md) — STRIDE analysis (T21-T28)
