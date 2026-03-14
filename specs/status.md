# Ferrosa Development Status

> Last updated: 2026-03-13
> Status: Living document

## Overview

Ferrosa is a fully functional **single-node CQL-compatible database** with graph query
support and built-in observability. The internode transport layer (ferrosa-net Phase 1) is
complete. The path to distributed operation requires ferrosa-cluster.

| Metric | Value |
|--------|-------|
| Crates | 9 of 10 planned |
| Source files | ~150 |
| Source LOC | ~48,300 |
| Test functions | ~1,083 |
| Integration test files | 19 |

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
net            █████░   ████░░  ████░░   ██░░░░
cluster        ░░░░░░   ░░░░░░  ░░░░░░   ░░░░░░
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
  segments, 3 sync modes, CDC), flush, merge, compaction (STCS strategy), S3 upload
  manager, manifest with etag CAS, local LRU cache, WriteObserver trait,
  SubscriptionObserver.
- **Remaining:**
  - [x] ~~Commit log replay integration~~ (merged PR #38)
  - [x] ~~Compaction execution merge I/O~~ (merged PR #38)
  - [ ] LCS and TWCS compaction strategies
  - [ ] Disk backpressure
  - [ ] `io_uring` I/O backend
  - [ ] Manifest CAS retry loop (T23 — designed, needs wiring)
  - [ ] S3 bucket policy validation at startup (T22 — verify encryption enabled)

### ferrosa-schema — Mostly Complete (Chunk A)

- **LOC:** 7,129 (27 files) | **Tests:** 199
- **Modules:** `audit` (3 submodules), `auth` (4 submodules), `convert`, `error`,
  `metadata` (3 submodules), `registry`, `secrets`, `startup`, `system` (4 submodules),
  `virtual_registry`, `virtual_table`
- **What's done:** Schema registry with `ArcSwap` lock-free snapshots, full RBAC auth
  (bcrypt/argon2), column-level permissions, rate limiting, audit logging (log + table
  sinks), system keyspace queries, VirtualTable trait + registry.
- **Remaining (Chunks B-F):**
  - [ ] DDL validation rules
  - [ ] System table persistence to SSTable
  - [ ] UDT (user-defined type) support
  - [ ] Role hierarchy with inheritance
  - [ ] Audit sink composition

### ferrosa-cql — Complete (Parts A-D + Compression)

- **LOC:** ~12,300 (20 files) | **Tests:** ~275 | **Largest crate**
- **Modules:** `ast`, `auth`, `bridge`, `client`, `connection`, `error`, `frame`,
  `lexer`, `parser`, `prepared`, `prometheus`, `result`, `router`, `server`,
  `subscribe`, `types`, `virtual_tables` (connections + active_queries)
- **What's done:** CQL v5 framing (16 opcodes), full type system, SASL PLAIN auth,
  LL(2) recursive-descent parser, query routing (DDL to schema, DML to storage),
  prepared statement cache (moka W-TinyLFU), ConnectionTracker/QueryTracker virtual
  tables, SUBSCRIBE/UNSUBSCRIBE extensions, Prometheus text exposition, CqlClient,
  LZ4 and Snappy frame compression with negotiation.
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

### ferrosa (binary) — Complete (single-node)

- **LOC:** ~870 (5 files) | **Tests:** ~15
- **Modules:** `web` (api, static_files)
- **What's done:** Composes all crates. CQL server on :9042, graph HTTP on :7474,
  web console on :9090. Connection + query tracker wiring, REST API for
  metrics/schema/queries, embedded static assets via rust-embed. Integration smoke
  tests covering server lifecycle, DDL/DML, system tables, multi-connection.
- **Remaining:**
  - [ ] Graceful shutdown sequencing
  - [ ] Configuration file support (currently env vars only)

### ferrosa-net — Phase 1 Complete (PR #39)

- **LOC:** 2,317 (14 files) | **Tests:** 43 (40 unit + 3 integration)
- **Modules:** `codec`, `config`, `discovery` (seeds), `error`, `handshake`, `message`,
  `peer`, `pool`, `rpc` (handler, server, client)
- **What's done:** 12-byte binary wire protocol with 3 priority lanes (Raft/Data/Bulk),
  21 message types, PSK-authenticated handshake (HMAC-SHA256), RPC server with connection
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
- **Spec:** [Net/Cluster Design](../docs/superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)
- **Threat Model:** [Net/Cluster Threats](threat-model-net-cluster.md)

### ferrosa-cluster — Not Started (Spec Written)

- **Purpose:** Raft metadata (openraft), token ring, tunable consistency levels,
  coordinator pattern, pair mode, hinted handoff, node lifecycle
- **Prerequisites:** ferrosa-net
- **Spec:** [Net/Cluster Design](../docs/superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)

## Active Work in Progress

| Item | Location | State |
|------|----------|-------|
| ~~Storage replay + compaction execution~~ | ~~`.worktrees/storage-replay-compaction`~~ | Merged (PR #38) |
| ~~ferrosa-net Phase 1~~ | ~~`ferrosa-net/`~~ | Complete (PR #39) |
| ferrosa-cluster | Not started | Next up |

## Path to Distributed Operation

The critical path from single-node to multi-node:

1. ~~**ferrosa-storage:** Commit log replay + compaction execution~~ (Done — PR #38)
1. **ferrosa-schema:** System table persistence (Chunk B)
1. ~~**ferrosa-net:** Internode transport (Phase 1)~~ (Done — PR #39)
1. **ferrosa-cluster:** Raft metadata, ring topology, request routing
1. **ferrosa-cluster:** Tunable consistency levels (ONE, QUORUM, ALL)
1. **ferrosa-cluster:** Hinted handoff and repair

## Related Documents

- [Components](components.md) — crate dependency graph
- [Overview](overview.md) — system architecture
- [Architecture Design](../docs/superpowers/specs/2026-03-11-ferrosa-architecture-design.md) — full design spec
