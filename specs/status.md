# Ferrosa Development Status

> Last updated: 2026-03-14
> Status: Living document

## Overview

Ferrosa is a **distributed CQL-compatible database** with graph query support,
built-in observability, and S3-backed storage. The internode transport (ferrosa-net
Phase 1), pair mode cluster (ferrosa-cluster Phase 1), and cluster building blocks
(ferrosa-cluster Phase 2) are complete: DDL replication, write forwarding, failover,
catch-up, Raft consensus, token ring, and coordinator pattern.

| Metric | Value |
|--------|-------|
| Crates | 11 of 11 planned |
| Source files | ~194 |
| Source LOC | ~68,000 |
| Test functions | ~1,370 |
| Integration test files | 22 |

## Maturity Assessment

```text
               Spec'd   Coded   Tested   Prod-ready
common         ██████   ██████  ██████   ████░░
sstable        ██████   ██████  ██████   ████░░
storage        ██████   ██████  █████░   ███░░░
schema         ████░░   █████░  █████░   ███░░░
index          █████░   █████░  ████░░   ██░░░░
cql            ██████   ██████  ██████   ████░░
graph          █████░   ██████  ████░░   ███░░░
ctl            ██████   ██████  ███░░░   ███░░░
binary         █████░   ██████  ███░░░   ███░░░
net            █████░   █████░  ████░░   ██░░░░
cluster        ██████   █████░  ████░░   ██░░░░
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
  `engine`, `flush`, `index` (tracker, scheduler, virtual_table), `manifest`,
  `memtable` (2 impls), `merge`, `observer`, `store`, `subscription_observer`,
  `upload`, `virtual_tables`
- **What's done:** Memtable (sharded BTree + skiplist), commit log (CAS-allocated
  segments, 3 sync modes, CDC, `force_sync` for catch-up), flush, merge, compaction
  (STCS strategy), S3 upload manager, manifest with etag CAS, local LRU cache,
  WriteObserver trait, SubscriptionObserver. Index support: `IndexStateTracker`,
  `IndexBuildScheduler`, index virtual table, `UploadTask::IndexFiles` variant.
- **Remaining:**
  - [x] ~~Commit log replay integration~~ (merged PR #38)
  - [x] ~~Compaction execution merge I/O~~ (merged PR #38)
  - [x] ~~Manifest CAS retry loop~~ (exponential backoff, 3 retries)
  - [x] ~~S3 bucket validation at startup~~ (list + put + delete probe)
  - [x] ~~S3 upload wiring~~ (flush → UploadManager → manifest update)
  - [x] ~~S3 cold restart bootstrap~~ (schema.json + manifest → download SSTables)
  - [x] ~~Graceful shutdown flush + S3 sync~~
  - [ ] LCS and TWCS compaction strategies
  - [ ] Disk backpressure
  - [ ] `io_uring` I/O backend

### ferrosa-schema — Mostly Complete (Chunk A)

- **LOC:** 7,348 (27 files) | **Tests:** 204
- **Modules:** `audit` (3 submodules), `auth` (4 submodules), `convert`, `error`,
  `metadata` (3 submodules), `registry`, `secrets`, `startup`, `system` (4 submodules),
  `virtual_registry`, `virtual_table`
- **What's done:** Schema registry with `ArcSwap` lock-free snapshots, full RBAC auth
  (bcrypt/argon2), column-level permissions, rate limiting, audit logging (log + table
  sinks), system keyspace queries, VirtualTable trait + registry. Schema replication:
  `apply_snapshot()`, idempotent `*_internal()` methods for pair mode. `IndexMetadata`
  for secondary index definitions, `system_schema.indexes` virtual table, cascade
  cleanup on `DROP TABLE` (removes associated indexes).
- **Remaining (Chunks B-F):**
  - [x] ~~DDL validation rules~~ (table name, PK, RF constraints)
  - [ ] System table persistence to SSTable
  - [ ] UDT (user-defined type) support
  - [ ] Role hierarchy with inheritance
  - [ ] Audit sink composition

### ferrosa-index — Phase 1 Complete (PR #44)

- **LOC:** ~5,800 (14 files) | **Tests:** 110
- **Modules:** `btree`, `hash`, `composite`, `filtered`, `phonetic` (soundex, metaphone,
  double_metaphone, caverphone), `vector` (hnsw, ivfflat)
- **What's done:** Pluggable secondary index framework with `IndexBuilder`/`IndexReader`/`IndexFactory`
  traits. 8 index types: B-tree (range scans), hash (O(1) equality), composite
  (multi-column), filtered (partial coverage), phonetic (Soundex, Metaphone, Double Metaphone,
  Caverphone), and two vector methods (HNSW, IVFFlat) for approximate nearest neighbor search.
  Distance functions (L2, cosine, inner product). Storage-attached: indexes build asynchronously
  after SSTable flush with zero write-path impact. Per-index staleness tracking, operational
  metrics via `system_views.secondary_indexes`, CQL-compatible DDL with
  `CREATE INDEX ... USING 'type'` syntax.
- **Remaining:**
  - [x] ~~Query path integration~~ (index check in SELECT path, falls through to scan until IndexReader wired)
  - [ ] `SOUNDS LIKE` / `ANN OF` CQL syntax
  - [ ] Clustered indexes
  - [ ] GPU offloading for vector operations
  - [ ] Binary serialization for index persistence
  - [ ] Compaction-triggered index rebuild
  - [ ] Distributed index coordination in cluster mode
- **Spec:** [Secondary Indexes Design](../superpowers/specs/2026-03-14-secondary-indexes-design.md)

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
  for pair mode replication. `CREATE INDEX` / `DROP INDEX` parser support and router
  integration, `resolve_index_type()` for mapping index USING clause to index factory.
- **Remaining:**
  - [x] ~~CQL TLS via rustls~~ (ring crypto provider, 10s handshake timeout)
  - [x] ~~Per-IP rate limiting~~ (IpConnectionTracker, default 64 per IP)
  - [x] ~~EVENT push notification types~~ (SchemaChange/Topology/Status + broadcast channel)
  - [x] ~~ALLOW FILTERING support~~ (full table scan + WHERE predicate post-filter)
  - [x] ~~SUBSCRIBE EVERY polling mode~~ (streaming frames, max 8 per connection)
  - [x] ~~Secondary index checks in SELECT path~~ (allows query without ALLOW FILTERING when index exists)
  - [x] ~~CQL Duration type (0x0015)~~ (zigzag vint encoding)
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
  - [x] ~~Integration tests~~ (11 tests covering connections, queries, system tables)

### ferrosa (binary) — Complete (pair-mode, production-ready)

- **LOC:** ~870 (6 files) | **Tests:** ~18
- **Modules:** `maintenance`, `web` (api, static_files)
- **What's done:** Composes all crates. CQL server on :9042, graph HTTP on :7474,
  web console on :9090. Connection + query tracker wiring, REST API for
  metrics/schema/queries/cluster management, embedded static assets via rust-embed.
  Cluster management endpoints: `GET /api/cluster/status`,
  `POST /api/cluster/promote`, `POST /api/cluster/switchover`.
  Background maintenance loop (auto-flush, compaction polling, commit log GC).
  Graceful shutdown with configurable timeout (drains in-flight requests).
  Per-connection request limiting for backpressure under load. Exponential backoff
  reconnection for internode links. Ships as .deb via `scripts/build-deb.sh` with
  systemd service unit.
- **Remaining:**
  - [x] ~~Graceful shutdown sequencing~~ (done)
  - [x] ~~Configuration file support~~ (TOML with env var override)
  - [x] ~~Flush + S3 sync on graceful shutdown~~ (zero data loss on SIGTERM)
  - [x] ~~Configurable flush interval~~ (FERROSA_FLUSH_INTERVAL_SECS)

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
  - [x] ~~TLS via rustls for internode encryption~~ (server + client, self-signed support)
  - [ ] Connection reconnection and backoff
  - [ ] Graceful shutdown / drain
  - [ ] Compression (LZ4/Snappy frame-level)
  - [ ] Metrics and tracing integration
  - [ ] Zero-copy serialization (Cap'n Proto / FlatBuffers / rkyv) for wire protocol
- **Spec:** [Net/Cluster Design](../superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)
- **Threat Model:** [Net/Cluster Threats](threat-model-net-cluster.md)

### ferrosa-cluster — Phase 2 Complete (Raft + Token Ring + Coordinator)

- **LOC:** 6,262 (25 files) | **Tests:** 91 (87 unit + 4 integration)
- **Modules:** `config`, `consistency`, `controller`, `ddl_path`, `error`, `mode`,
  `pair` (catchup, coordinator, ddl, handler, node, switchover), `state`, `write_path`,
  `raft` (mod, log_store, state_machine, network), `ring` (mod, strategy),
  `coordinator` (mod, write, read)
- **What's done:**
  - **Phase 1 (Pair mode):** `PairCoordinator` (write forwarding + replication),
    `DdlCoordinator` + `DdlPath` (DDL forwarding through primary),
    `PairSchemaSyncHandler` (schema catch-up on rejoin), `ModeController`
    (standalone → pair → degraded → cluster transitions), `force_promote()`,
    `switchover()`, auto re-pair, commit log `force_sync`, `ConsistencyLevel`
    with `blockFor()` + property tests
  - **Phase 2 (Raft consensus):** `FerrosRaftConfig` (openraft type config),
    `RaftCommand` enum (15 schema + 3 topology + 1 config variant),
    `SledLogStore` (sled-backed persistent Raft log with vote/commit persistence),
    `FerrosStateMachine` (deterministic apply with BTreeMap-based `RaftState`,
    schema + topology + token map, snapshot build/install),
    `FerrosRaftNetwork` + factory (wraps PeerManager for Raft RPC)
  - **Phase 2 (Token ring):** `TokenRing` with `BTreeMap<Token, u64>` for O(log n)
    replica lookup, clockwise walk for SimpleStrategy, vnode-aware dedup
  - **Phase 2 (Coordinator):** `ClusterCoordinator` with write/read fan-out,
    tunable consistency level enforcement (`blockFor(CL)` ACK collection),
    local-replica optimization, `WritePath::Cluster` and `DdlPath::Cluster` variants
  - **Phase 2 (Wiring):** `transition_to_cluster()` in ModeController (3rd peer
    triggers Raft group + token ring + coordinator initialization),
    `RaftClusterState` for system.peers
  - Docker smoke test: 12-phase lifecycle (pair mode phases 1-5, cluster
    formation, 3-node writes/reads, QUORUM with node failure, below-QUORUM
    rejection, recovery, DDL replication, FMEA failure modes)
- **Remaining (Phase 3 — Production Cluster):**
  - [ ] End-to-end cluster mode activation in binary (RPC handler registration)
  - [ ] Hinted handoff and repair
  - [ ] Node lifecycle (leave, decommission, bootstrap streaming)
  - [ ] NetworkTopologyStrategy (multi-DC)
  - [ ] Read repair
  - [ ] Automatic token rebalancing
  - [ ] Quorum Lease / Mencius optimizations (Paxos-Raft paper)
- **Spec:** [Cluster Phase 2 Design](../superpowers/specs/2026-03-14-cluster-phase2-design.md)
- **Phase 1 Spec:** [Net/Cluster Design](../superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)
- **Schema Replication Spec:** [Schema Replication](../superpowers/specs/2026-03-14-schema-replication-design.md)
- **Threat Models:** [Net/Cluster](threat-model-net-cluster.md), [Schema Replication](threat-model-schema-replication.md)

## Active Work in Progress

| Item | Location | State |
|------|----------|-------|
| ~~Storage replay + compaction execution~~ | ~~`.worktrees/storage-replay-compaction`~~ | Merged (PR #38) |
| ~~ferrosa-net Phase 1~~ | ~~`ferrosa-net/`~~ | Merged (PR #39) |
| ~~ferrosa-cluster Phase 1 (Pair mode)~~ | ~~`feature/pair-integration`~~ | Merged |
| ~~ferrosa-index Phase 1 (8 index types)~~ | ~~`feature/secondary-indexes-design`~~ | Merged (PR #44) |
| ~~DDL completeness (AlterKS/AlterTable, Role DDL, Index DDL)~~ | ~~`ferrosa-cluster/`~~ | Merged (PR #45) |
| ~~.deb packaging + systemd service~~ | ~~`scripts/build-deb.sh`~~ | Merged (PR #46) |
| Release workflow (GitHub Actions) | `.github/workflows/` | In progress (PR #47) |
| ~~ferrosa-cluster Phase 2 (Raft + Ring + Coordinator)~~ | ~~`feature/raft-cluster-phase2`~~ | Merged |
| Observability wiring (virtual tables + auth + WebSocket) | `feature/observability-wiring` | In progress |
| ferrosa-cluster Phase 3 (Production cluster wiring) | — | Next up |

## Path to Distributed Operation

The critical path from single-node to multi-node:

1. ~~**ferrosa-storage:** Commit log replay + compaction execution~~ (Done — PR #38)
1. ~~**ferrosa-net:** Internode transport (Phase 1)~~ (Done — PR #39)
1. ~~**ferrosa-cluster:** Pair mode — write forwarding, DDL replication, failover~~ (Done)
1. ~~**ferrosa-cluster:** Raft metadata, ring topology, coordinator pattern~~ (Done — Phase 2)
1. **ferrosa-cluster:** End-to-end cluster wiring in binary (Phase 3)
1. **ferrosa-schema:** System table persistence (Chunk B)
1. **ferrosa-cluster:** Hinted handoff and repair

## Related Documents

- [Components](components.md) — crate dependency graph
- [Overview](overview.md) — system architecture
- [Architecture Design](../superpowers/specs/2026-03-11-ferrosa-architecture-design.md) — full design spec
- [Schema Replication Design](../superpowers/specs/2026-03-14-schema-replication-design.md) — DDL replication spec
- [Secondary Indexes Design](../superpowers/specs/2026-03-14-secondary-indexes-design.md) — pluggable index framework spec
- [Schema Replication Threat Model](threat-model-schema-replication.md) — STRIDE analysis (T21-T28)
