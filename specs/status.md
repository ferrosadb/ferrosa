# Ferrosa Development Status

> Last updated: 2026-03-16
> Status: Living document

## Overview

Ferrosa is a **distributed CQL-compatible database** with graph
query support, built-in observability, and S3-backed storage. The production cluster
sprint is complete with Raft consensus, coordinated reads/writes, hinted handoff,
node lifecycle (join/decommission/rebalance), reconnection, and integration tests.
Hardening and observability wiring sprints are also complete. UDT/UDF with WASM
sandboxing is complete (parser, schema, DDL replication, Wasmtime compilation, router
wiring). Secondary and vector indexes consolidated into the main branch. Graph engine
is feature-complete: Cypher parser, CREATE/SET/DELETE planner+executor, result
projection with property extraction, ORDER BY/LIMIT/DISTINCT, and full adjacency
reconciliation.

| Metric | Value |
|--------|-------|
| Crates | 12 (11 core + ferrosa-udf) |
| Source files | ~250+ |
| Source LOC | ~115,000+ |
| Test functions | ~1,650+ |
| Integration test files | 30+ |

## Maturity Assessment

```text
               Spec'd   Coded   Tested   Prod-ready
common         ██████   ██████  ██████   ████░░
sstable        ██████   ██████  ██████   ████░░
storage        ██████   ██████  █████▌   ████░░
schema         █████░   █████▌  █████░   ███░░░
index          █████░   ██████  █████░   ██░░░░
udf            █████░   █████░  ████░░   ███░░░
cql            ██████   ██████  ██████   ████░░
graph          ██████   ██████  █████░   ███░░░
ctl            ██████   ██████  ████░░   ████░░
binary         ██████   ██████  ████░░   ████░░
net            ██████   ██████  █████░   ███░░░
cluster        ██████   ██████  █████░   ███░░░
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

- **LOC:** ~10,500 (32 files) | **Tests:** ~230
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
  - [x] ~~UDT (user-defined type) support~~ — `UserTypeMetadata`, `system_schema.types` virtual table, schema registry integration
  - [ ] System table persistence to SSTable
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

### ferrosa-udf — Done (WASM UDF/UDA)

- **LOC:** ~1,800 (6 files) | **Tests:** ~68
- **Modules:** `executor`, `sandbox`, `error`, `convert`, `wit/ferrosa-udf.wit`
- **What's done:** WIT contract defining CQL value types for WASM Component Model,
  `UdfExecutor` with moka compilation cache (256 entries) and real Wasmtime
  compilation (`wasmtime::component::Component`), `SandboxConfig` with resource
  limits (16MB memory, 1M fuel, 5s timeout, 10MB binary upload limit),
  `CqlValue` to WIT `cql-value` conversion layer covering all CQL types including
  collections, UDTs, temporal types, and decimal/varint. CQL parser handles
  CREATE/DROP FUNCTION and CREATE/DROP AGGREGATE. Schema has `FunctionMetadata`,
  `AggregateMetadata`, and registry methods. `system_schema.functions` and
  `system_schema.aggregates` virtual tables. DDL replication via
  `DdlOperation::CreateFunction/DropFunction/CreateAggregate/DropAggregate` and
  corresponding `RaftCommand` variants. Router wires CREATE/DROP FUNCTION/AGGREGATE
  through `DdlPath` with permission checks. Binary initializes `UdfExecutor` at
  startup.
- **Remaining:**
  - [x] ~~Wasmtime component compilation~~ (real `wasmtime::component::Component` instantiation)
  - [x] ~~UDF execution wiring in CQL router~~ (CREATE/DROP FUNCTION routed through DdlPath)
  - [x] ~~`system_schema.functions` virtual table~~ (+ `system_schema.aggregates`)
  - [x] ~~Aggregate UDF (UDA) support~~ — CREATE/DROP AGGREGATE with state/final functions
  - [x] ~~DdlOperation::CreateFunction/DropFunction for cluster replication~~
  - [x] ~~Val encoding for all 26 CQL types~~ (recursive Val::Variant encoding, wit-bindgen not usable due to recursive type limitation)
  - [ ] Function calls in SELECT expressions (requires expression executor)
  - [ ] GRANT/REVOKE on function resources
  - [ ] Aggregate state/final function orchestration in query path

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
  - [x] ~~UDT DDL support~~ — CREATE/ALTER/DROP TYPE parsing, routing, cluster replication
  - [x] ~~UDF DDL parsing~~ — CREATE/DROP FUNCTION AST and parser support
  - [x] ~~UDT wire encoding~~ — protocol type 0x0030, type resolution in bridge
  - [x] ~~UDF execution wiring~~ (CREATE/DROP FUNCTION/AGGREGATE routed through DdlPath)
  - [ ] Logged batch atomicity
  - [ ] Query tracing

### ferrosa-graph — Feature Complete

- **LOC:** ~7,500 (20 files) | **Tests:** 139
- **Modules:** `adjacency` (observer, reconcile, schema), `engine`, `error`,
  `executor` (expand, result), `http`, `parser` (ast, lexer, parse_impl, token),
  `planner` (logical, physical)
- **What's done:** Full Cypher parser, logical planner with label resolution +
  per-hop auth, physical planner for MATCH/CREATE/SET/DELETE, expand executor with
  result projection (property extraction from rows), ORDER BY/LIMIT/DISTINCT,
  resource limits (T4), adjacency index with WriteObserver, full background
  reconciliation (edge scan + orphan cleanup), HTTP/JSON endpoint with auth, TLS,
  error sanitization, audit logging.
- **Future:**
  - [x] ~~Full adjacency reconciliation scan~~ (T5 — edge scan + orphan cleanup)
  - [x] ~~CREATE/SET/DELETE planning and execution~~
  - [x] ~~Result projection with property extraction~~
  - [x] ~~ORDER BY, LIMIT, DISTINCT~~
  - [ ] WCO (worst-case optimal) joins
  - [ ] Leapfrog triejoin
  - [ ] Variable-length paths
  - [ ] Aggregations
  - [ ] Bolt protocol support

### ferrosa-ctl — Complete

- **LOC:** ~1,400 (4 files) | **Tests:** ~45
- **Modules:** `commands`, `tui`
- **What's done:** CLI admin tool (clap). Commands: `query`, `describe`, `monitor`,
  `metrics`, `status`, `connections`, `queries`, `storage`, `topology`, `peers`.
  TUI monitor dashboard (ratatui/crossterm) with 5 panels, auto-refresh,
  keyboard navigation. Cluster management subcommands: `add-node`, `decommission`,
  `ring`, `rebalance`.
- **Remaining:**
  - [x] ~~Integration tests~~ (11 tests covering connections, queries, system tables)

### ferrosa (binary) — Complete (cluster-mode)

- **LOC:** ~1,200 (8 files) | **Tests:** ~30
- **Modules:** `maintenance`, `web` (api, static_files)
- **What's done:** Composes all crates. CQL server on :9042, graph HTTP on :7474,
  web console + cluster API on :9090, Prometheus metrics endpoint (`/metrics`).
  Connection + query tracker wiring, REST API for
  metrics/schema/queries/cluster management, embedded static assets via rust-embed.
  Cluster management endpoints: `GET /api/cluster/status`,
  `POST /api/cluster/promote`, `POST /api/cluster/switchover`,
  `POST /api/cluster/add-node`, `POST /api/cluster/decommission`,
  `GET /api/cluster/ring`, `POST /api/cluster/rebalance`.
  Background maintenance loop (auto-flush, compaction polling, commit log GC).
  Graceful shutdown with configurable timeout (drains in-flight requests).
  Internode drain on shutdown. Per-connection request limiting for backpressure
  under load. Exponential backoff reconnection for internode links. Ships as
  .deb via `scripts/build-deb.sh` with systemd service unit.
- **Remaining:**
  - [x] ~~Graceful shutdown sequencing~~ (done)
  - [x] ~~Configuration file support~~ (TOML with env var override)
  - [x] ~~Flush + S3 sync on graceful shutdown~~ (zero data loss on SIGTERM)
  - [x] ~~Configurable flush interval~~ (FERROSA_FLUSH_INTERVAL_SECS)

### ferrosa-net — Phase 1 + Reconnection Complete

- **LOC:** ~3,800 (18 files) | **Tests:** ~75 (unit + integration)
- **Modules:** `codec`, `config`, `discovery` (seeds), `error`, `handshake`, `message`,
  `peer`, `pool`, `reconnect`, `rpc` (handler, server, client)
- **What's done:** 12-byte binary wire protocol with 3 priority lanes (Raft/Data/Bulk),
  24 message types (including schema replication: PairSchemaSync, PairDdlForward,
  PairDdlAck), PSK-authenticated handshake (HMAC-SHA256), RPC server with connection
  limits + handshake timeout, RPC client with request-response and fire-and-forget,
  `PriorityPool` (3 TCP connections per peer), static seed discovery, `PeerManager` with
  heartbeat-based failure detection. Proptest fuzzing for message decode. No dependency
  on ferrosa-common.
  - [x] ~~TLS via rustls for internode encryption~~ (server + client, self-signed support)
  - [x] ~~Connection reconnection and backoff~~ (alive watch channel + exponential backoff state machine)
  - [x] ~~Graceful shutdown / drain~~ (CancellationToken-based drain)
  - [x] ~~PeerManager reconnection orchestration~~
- **Remaining:**
  - [ ] Compression (LZ4/Snappy frame-level)
  - [ ] Metrics and tracing integration
  - [ ] Zero-copy serialization (Cap'n Proto / FlatBuffers / rkyv) for wire protocol
- **Spec:** [Net/Cluster Design](../superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)
- **Threat Model:** [Net/Cluster Threats](threat-model-net-cluster.md)

### ferrosa-cluster — Phase 3 Complete (Production Cluster)

- **LOC:** ~14,500 (45+ files) | **Tests:** ~280 (unit + integration)
- **Modules:** `config`, `consistency`, `controller`, `ddl_path`, `error`, `mode`,
  `pair` (catchup, coordinator, ddl, handler, node, switchover), `state`, `write_path`,
  `raft` (mod, log_store, state_machine, network), `ring` (mod, strategy),
  `coordinator` (mod, write, read), `hint` (segment, store, delivery),
  `lifecycle` (join, decommission, streaming, rebalance)
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
  - **Phase 3 Slice 1 (Cluster wiring):** Raft RPC handlers, Raft init in
    controller, DDL via Raft, concurrent write fan-out, digest computation,
    remote quorum reads with digest
  - **Phase 3 Slice 2 (Reconnection):** Connection drop detection, reconnection
    state machine, PeerManager reconnection, graceful drain
  - **Phase 3 Slice 3 (Hinted handoff):** Hint segment I/O with CRC32, HintStore
    with eviction, coordinator hint-on-failure, hint delivery background task
  - **Phase 3 Slice 4 (Node lifecycle):** ApproveNode Raft command, streaming
    protocol, node join with approval gate, node decommission via LeaveNode,
    token rebalancing with skew-aware algorithm
  - **Phase 3 Slice 5 (Integration tests):** Docker compose (3/5-node profiles),
    3-node smoke tests (C1-C10), 5-node + lifecycle + FMEA tests
  - [x] ~~End-to-end cluster mode activation in binary~~
  - [x] ~~Hinted handoff~~
  - [x] ~~Node lifecycle (join, decommission, streaming, rebalance)~~
  - [x] ~~Read repair~~ (deferred — digest reads included)
  - [x] ~~UDF/UDA DDL replication~~ (CreateFunction/DropFunction/CreateAggregate/DropAggregate RaftCommand variants)
- **Remaining:**
  - [ ] NetworkTopologyStrategy (multi-DC)
  - [ ] Read repair (full inline repair)
  - [ ] Quorum Lease / Mencius optimizations (Paxos-Raft paper)
- **Spec:** [Cluster Phase 2 Design](../superpowers/specs/2026-03-14-cluster-phase2-design.md)
- **Phase 1 Spec:** [Net/Cluster Design](../superpowers/specs/2026-03-13-ferrosa-net-cluster-design.md)
- **Schema Replication Spec:** [Schema Replication](../superpowers/specs/2026-03-14-schema-replication-design.md)
- **Threat Models:** [Net/Cluster](threat-model-net-cluster.md), [Schema Replication](threat-model-schema-replication.md)

## Active Work in Progress

| Item | Location | State |
|------|----------|-------|
| ~~UDT/UDF with WASM sandboxing~~ | — | Done (merged) |
| ~~Secondary + vector indexes consolidated~~ | — | Done (merged) |
| ~~Release workflow (GitHub Actions)~~ | ~~`.github/workflows/`~~ | Merged (PR #47) |
| ~~Hardening sprint~~ | — | Merged (PR #55) |
| ~~Observability wiring~~ | — | Merged (PR #53) |
| ~~Production cluster (Phase 3)~~ | — | Merged (PR #57) |
| ~~Beta release v1.0.0-beta.1~~ | — | Released (PR #58, #59) |
| Beta release v1.0.0-beta.3 | — | In progress |
| NetworkTopologyStrategy (multi-DC) | — | Planned |

## Path to Distributed Operation

The critical path from single-node to multi-node:

1. ~~**ferrosa-storage:** Commit log replay + compaction execution~~ (Done — PR #38)
1. ~~**ferrosa-net:** Internode transport (Phase 1)~~ (Done — PR #39)
1. ~~**ferrosa-cluster:** Pair mode — write forwarding, DDL replication, failover~~ (Done)
1. ~~**ferrosa-cluster:** Raft metadata, ring topology, coordinator pattern~~ (Done — Phase 2)
1. ~~**ferrosa-cluster:** End-to-end cluster wiring in binary~~ (Done — Phase 3)
1. **ferrosa-schema:** System table persistence (Chunk B)
1. ~~**ferrosa-cluster:** Hinted handoff~~ (Done — Phase 3)
1. **ferrosa-cluster:** NetworkTopologyStrategy (multi-DC)
1. **Beta release**

## Related Documents

- [Components](components.md) — crate dependency graph
- [Overview](overview.md) — system architecture
- [Architecture Design](../superpowers/specs/2026-03-11-ferrosa-architecture-design.md) — full design spec
- [Schema Replication Design](../superpowers/specs/2026-03-14-schema-replication-design.md) — DDL replication spec
- [Secondary Indexes Design](../superpowers/specs/2026-03-14-secondary-indexes-design.md) — pluggable index framework spec
- [Schema Replication Threat Model](threat-model-schema-replication.md) — STRIDE analysis (T21-T28)
