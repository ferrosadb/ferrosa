# Ferrosa Development Status

> Last updated: 2026-04-02
> Status: Living document

## Overview

Ferrosa is a **distributed CQL-compatible database** with graph query support,
built-in observability, Accord consensus transactions, and S3-backed storage.

**Completed milestones (2026-04-03):**

- **Observability (O1-O6)** — Self-hosted telemetry: FerrosaTelemetryLayer with direct StorageEngine writes (no feedback loop), 25+ tracing spans across CQL/coordinator/Accord/storage/network, slow query detection with parameterized text, query fingerprint tracker (top 10k), billing metering per client, alert evaluator, table access summary, full scan reasons, internode trace context propagation (32-byte header), on-demand flame chart endpoint, 13 virtual tables in `system_observability.*`, `otel` feature flag for enterprise export.
- **S4 close all hazards** — DDL queued during Forming (not rejected), ClusterInviteHandler spawns tracked, transition guard on disconnect, RangeReadHandler truncation flag, capped unbounded collections, PairClusterState peer cache, SSTable-based streaming, BootstrapComplete RPC barrier.
- **P0 NTS fix** — `keyspace_rf()` returned RF=1 for NTS; writes went to coordinator only. Fixed: RF extraction via `ReplicationStrategy` parser, `WritePath` dispatches to `coordinate_write_nts()`, default datacenter `datacenter1`.
- **Jepsen readiness** — 4 linearizability fixes: stale read on re-fetch failure returns error, read repair awaited inline, hint store failures logged at error, clock skew configurable. Raft node_map reads through poison.
- **Cluster formation** — Progressive join (Standalone→Pair→Forming→Cluster), ClusterInvite protocol, LazyRaft handler registration, all-node bootstrap streaming, CQL broadcast propagation, system.peers tokens/native_address, connection slot leak fix.
- **Cluster correctness S1-S3** — 22 tasks: all P0/P1 hazards closed, NTS read path, hints for all failures, RowWire streaming, BroadcastResolver trait, tunable Raft config, quorum committed_cluster_size.
- **Accord transactions** — 7 sprints (A1-A7) complete: PreAccept/Accept/Commit/Execute, LWT, BEGIN TRANSACTION, cross-shard conflict detection, crash recovery, electorate reconfiguration, 2,808+ tests
- **Correctness sprints** — C1-C7 complete: BUG-021-026 fixed, P0 storage hazards closed, Jepsen infrastructure wired, SSTable Cassandra compat validated. C4 (live Jepsen) and C8 (all-drivers compat) remain.
- **NVMe table pinning** — Per-table `storage.pin = nvme` attribute: skip S3 upload, pin in local cache, max_bytes enforcement, ALTER TABLE pin/unpin transitions, Prometheus metrics
- **Full-text indexing** — Inverted index pipeline: StandardAnalyzer (lowercase + stop words + Porter stemmer), FTI sidecar files built on flush, BM25 ranked search, AND/OR/NOT/Prefix queries, CQL `fts_match()` function, compaction merge
- **Secondary + vector indexes** — BTree, Hash, Composite, Phonetic, Filtered, Vector (HNSW, IVFFlat), FullText — 11 index types with query planner integration
- **PITR** — S3-native: commit log archiving, snapshot management, point-in-time restoration, CLI tooling
- **Graph engine** — Complete: eval, aggregations, var-length paths, SUBSCRIBE, Bolt v5

| Metric | Value |
|--------|-------|
| Crates | 13 (12 core + ferrosa-jepsen) |
| Source files | ~378 |
| Source LOC | ~258,000+ |
| Test functions | ~3,499+ |
| Integration test files | 40+ |
| CQL parser coverage | 81.8% (707/864 Cassandra doc examples) |
| Index types | 11 (BTree, Hash, Composite, Phonetic, Filtered, Vector HNSW/IVFFlat, FullText) |
| SSTable fuzz cases | 9 property-based tests (1000+ inputs each) |

## Maturity Assessment

```text
               Spec'd   Coded   Tested   Prod-ready
common         ██████   ██████  ██████   ████░░
sstable        ██████   ██████  ██████   ████░░
storage        ██████   ██████  ██████   ████░░
schema         ██████   ██████  ██████   ████░░
index+fts      ██████   ██████  ██████   ████░░
udf            ██████   ██████  █████░   ███░░░
cql            ██████   ██████  ██████   ████░░
graph          ██████   ██████  ██████   ████░░
ctl            ██████   ██████  ████░░   ████░░
binary         ██████   ██████  ████░░   ████░░
net            ██████   ██████  █████░   ███░░░
cluster+accord ██████   ██████  ██████   ████░░
jepsen         ██████   █████░  ████░░   ██░░░░
```

## Crate Status

### ferrosa-common — Complete

- **LOC:** 1,500+ (12 files) | **Tests:** 50+
- **Modules:** `cell`, `data_type`, `error`, `key`, `murmur3`, `schema`, `token`, `accord`
- **What's done:** Token, PartitionKey, DecoratedKey, CellValue, Murmur3 partitioner.
  Property tests via optional `test-generators` feature. **Accord types:** `Timestamp`
  (HLC hybrid logical clock), `TxnId` (transaction identifier), `Ballot` (ballot numbers
  for consensus rounds).
- **Remaining:** More property tests for edge cases.

### ferrosa-sstable — Complete (BTI format, Cassandra-compatible)

- **LOC:** 8,500+ (19 files) | **Tests:** 190+
- **Modules:** `bloom`, `byte_comparable`, `compression`, `data`, `io`, `marshal`,
  `partition_index`, `reader`, `row_index`, `statistics`, `toc`, `trie`, `types`,
  `varint`, `writer`
- **What's done:** Full BTI read/write. On-disk trie (16 node types, page-aware packing),
  Bloom filter, LZ4/Zstd compression, byte-comparable keys, Cassandra compat tests.
  Cell serialization matches Cassandra's `Cell.Serializer` exactly for all 3 cell types
  (live, tombstone, expiring/TTL). Property-based fuzz testing for all cell types.
  Reader fuzz testing with random bytes, truncated data, and single-byte corruption
  (never panics). Cell value length guard (256MB) prevents OOM from corrupt data.
- **Recent fixes (2026-03-23):**
  - [x] Expiring cell (TTL) serialization — CELL_IS_EXPIRING flag + LDT/TTL deltas
  - [x] Capacity overflow guard for corrupt cell lengths
  - [x] Reader resilience — returns Err, never panics on malformed input
  - [x] Property-based fuzz tests (proptest): live/tombstone/expiring cell roundtrips
  - [x] Reader fuzz: random bytes, truncated, single-byte corruption
  - [x] Cassandra CQLSSTableWriter fixture generator (Java, Docker)
- **Remaining:**
  - [x] Sign-bit fix for BTI trie partition index
  - [ ] Big format reader (read-only compat for existing Cassandra SSTables)
  - [ ] Native Ferrosa SSTable format (behind feature flag)
  - [ ] `sstable-dump` / `sstable-import` CLI tools (migration tooling)

### ferrosa-storage — Complete (core engine + NVMe + compaction S3 + Accord)

- **LOC:** ~33,500 (42 files) | **Tests:** 541+
- **Modules:** `cache`, `commitlog` (7 submodules), `compaction` (4 submodules incl. UCS),
  `engine`, `flush`, `index` (tracker, scheduler, virtual_table), `manifest`,
  `memtable` (2 impls), `merge`, `observer`, `store`, `subscription_observer`,
  `upload`, `virtual_tables`, `accord` (sync_writer, write_gate, reorder_buffer,
  sidecar)
- **What's done:** Memtable (sharded BTree + skiplist), commit log (CAS-allocated
  segments, 3 sync modes, CDC, `force_sync` for catch-up), flush, merge, compaction
  (STCS strategy), S3 upload manager, manifest with etag CAS, local LRU cache,
  WriteObserver trait, SubscriptionObserver. Index support: `IndexStateTracker`,
  `IndexBuildScheduler`, index virtual table, `UploadTask::IndexFiles` variant.
  **Accord module:** `SyncWriter` (durable write-ahead for Accord commits),
  `WriteGate` (DDL drain-and-block gate), `ReorderBuffer` (dependency-ordered apply),
  `.accord` sidecar files (crash recovery replay), `DurabilityService` with
  `ExclusiveSyncPoint`.
- **Remaining:**
  - [x] ~~Commit log replay integration~~ (merged PR #38)
  - [x] ~~Compaction execution merge I/O~~ (merged PR #38)
  - [x] ~~Manifest CAS retry loop~~ (exponential backoff, 3 retries)
  - [x] ~~S3 bucket validation at startup~~ (list + put + delete probe)
  - [x] ~~S3 upload wiring~~ (flush → UploadManager → manifest update)
  - [x] ~~S3 cold restart bootstrap~~ (schema.json + manifest → download SSTables)
  - [x] ~~Graceful shutdown flush + S3 sync~~
  - [x] ~~Commit log archiving to S3~~ (PITR Sprint P-1)
  - [x] ~~SnapshotManager (create/list/delete)~~ (PITR Sprint P-2)
  - [x] ~~SSTable GC safety (snapshot manifest scanning)~~ (PITR Sprint P-2)
  - [x] ~~RestoreManager (segment continuity, timestamp filtering, node-id validation)~~ (PITR Sprint P-3)
  - [x] ~~StorageEngine::open_from_snapshot~~ (PITR Sprint P-3)
  - [x] ~~archive_status and snapshots virtual tables~~ (PITR Sprint P-4)
  - [x] ~~Snapshot TTL cleanup~~ (PITR Sprint P-4)
  - [x] ~~Sidecar index file persistence (flush, load, merge, tombstone-aware)~~ (Index Sprints I-1/I-3)
  - [x] ~~VectorMemtableIndex for ANN queries~~ (Index Sprint I-4)
  - [x] read_range includes SSTable data
  - [x] DELETE merges row-level tombstones
  - [x] ~~SSTable read resilience~~ — skip corrupt partitions with warning, never crash
  - [x] ~~S3 CAS probe~~ — detect stores without conditional put (RustFS/MinIO), fallback to unconditional writes
  - [x] ~~FMEA corruption resilience tests~~ — truncated/zero/evolved/corrupt Data.db files
  - [x] ~~UCS (Unified Compaction Strategy)~~ — density-based levels, per-table DDL config, fan factor
  - [ ] TWCS (Time-Window) compaction — future; UCS with TTL-aware levels could subsume
  - [ ] Disk backpressure
  - [ ] `io_uring` I/O backend

### ferrosa-schema — Complete

- **LOC:** ~11,700 (27 files) | **Tests:** 297
- **Modules:** `audit` (3 submodules), `auth` (4 submodules), `convert`, `error`,
  `metadata` (3 submodules), `registry`, `secrets`, `startup`, `system` (4 submodules),
  `virtual_registry`, `virtual_table`
- **What's done:** Schema registry with `ArcSwap` lock-free snapshots, full RBAC auth
  (bcrypt/argon2), column-level permissions, rate limiting, audit logging (log + table
  sinks), system keyspace queries, VirtualTable trait + registry. Schema replication:
  `apply_snapshot()`, idempotent `*_internal()` methods for pair mode. `IndexMetadata`
  for secondary index definitions, `system_schema.indexes` virtual table, cascade
  cleanup on `DROP TABLE` (removes associated indexes).
- **Completed (Chunks B-F):**
  - [x] ~~DDL validation rules~~ (table name, PK, RF constraints)
  - [x] ~~UDT (user-defined type) support~~ — `UserTypeMetadata`, `system_schema.types` virtual table, schema registry integration
  - [x] ~~BACKUP permission for snapshot operations~~ (PITR Sprint P-4)
  - [x] ~~System table persistence to SSTable~~ — system_schema.*and system_auth.* persisted via SystemTableWriter
  - [x] ~~Role hierarchy with inheritance~~ — recursive has_permission_recursive() with cycle detection, member_of in role_members
  - [x] ~~Audit sink composition~~ — CompositeSink fans out to multiple `Arc<dyn AuditSink>` (Log, SystemTable, Test)

### ferrosa-index — Complete (11 index types)

- **LOC:** ~7,500 (22 files) | **Tests:** 145+
- **Modules:** `btree`, `hash`, `composite`, `filtered`, `phonetic` (soundex, metaphone,
  double_metaphone, caverphone), `vector` (hnsw, ivfflat), `fulltext` (analyzer, builder,
  reader, query, scoring, merge, stemmer)
- **What's done:** Pluggable secondary index framework with `IndexBuilder`/`IndexReader`/`IndexFactory`
  traits. 8 index types: B-tree (range scans), hash (O(1) equality), composite
  (multi-column), filtered (partial coverage), phonetic (Soundex, Metaphone, Double Metaphone,
  Caverphone), and two vector methods (HNSW, IVFFlat) for approximate nearest neighbor search.
  Distance functions (L2, cosine, inner product). Storage-attached: indexes build asynchronously
  after SSTable flush with zero write-path impact. Per-index staleness tracking, operational
  metrics via `system_views.secondary_indexes`, CQL-compatible DDL with
  `CREATE INDEX ... USING 'type'` syntax.
- **Remaining:**
  - [x] ~~Query path integration~~ (planner + route_select, Index Sprint I-2)
  - [x] ~~IndexIntersection (multi-index WHERE)~~ (Index Sprint I-4)
  - [x] ~~Sidecar index persistence (flush/compaction/recovery)~~ (Index Sprint I-3)
  - [ ] `SOUNDS LIKE` / `ANN OF` CQL syntax
  - [ ] Clustered indexes
  - [ ] GPU offloading for vector operations
  - [ ] Compaction-triggered index rebuild
  - [ ] Distributed index coordination in cluster mode
- **Spec:** [Secondary Indexes Design](../superpowers/specs/2026-03-14-secondary-indexes-design.md)

### ferrosa-udf — Done (WASM UDF/UDA)

- **LOC:** ~2,400 (6 files) | **Tests:** 74
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
  - [x] ~~Function calls in SELECT expressions~~ — built-in (uuid, now, toTimestamp, writetime, ttl, toJson) + scalar UDFs per-row
  - [x] ~~GRANT/REVOKE on function resources~~ — FUNCTION and ALL FUNCTIONS IN KEYSPACE resources with EXECUTE permission
  - [x] ~~Aggregate state/final function orchestration~~ — execute_uda() calls state func per-row, final func once at end

### ferrosa-cql — Complete (Parts A-D + Compression + Accord + Temporal compat)

- **LOC:** ~30,400 (26 files) | **Tests:** 567+ | **Largest crate**
- **Modules:** `ast`, `auth`, `bridge`, `client`, `connection`, `error`, `frame`,
  `lexer`, `parser`, `prepared`, `prometheus`, `result`, `router`, `server`,
  `subscribe`, `types`, `virtual_tables` (connections + active_queries), `pagination`,
  `transaction`
- **What's done:** CQL v5 framing (16 opcodes), full type system, SASL PLAIN auth,
  LL(2) recursive-descent parser, query routing (DDL to schema, DML to storage),
  prepared statement cache (moka W-TinyLFU), ConnectionTracker/QueryTracker virtual
  tables, SUBSCRIBE/UNSUBSCRIBE extensions, Prometheus text exposition, CqlClient,
  LZ4 and Snappy frame compression with negotiation. DDL routes through `DdlPath`
  for pair mode replication. `CREATE INDEX` / `DROP INDEX` parser support and router
  integration, `resolve_index_type()` for mapping index USING clause to index factory.
  **Accord/LWT:** LWT INSERT IF NOT EXISTS, IF conditions on UPDATE/DELETE, Batch CAS,
  SERIAL/LOCAL_SERIAL consistency levels, `BEGIN TRANSACTION`/`COMMIT`/`ROLLBACK`
  parser and router support, read-set/write-set extraction, transaction limits,
  client retry on Accord contention. **Pagination:** Result set paging with page state.
  **Built-in functions:** `now()`, `toTimestamp()`, `TTL()`.
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
  - [x] ~~EXPLAIN statement~~ (Index Sprint I-2)
  - [x] ~~Query planner (ScanPlan: PrimaryKey/SingleIndex/IndexIntersection/FullScan)~~ (Index Sprint I-2)
  - [x] ALLOW FILTERING with full-scan post-filter
  - [x] token() function in WHERE clauses
  - [x] toJson() built-in function
  - [x] SELECT DISTINCT
  - [x] INSERT IF NOT EXISTS (LWT basic)
  - [x] Counter increment/decrement
  - [x] Collection UPDATE +/- operators
  - [x] CONTAINS / CONTAINS KEY filtering
  - [x] vector\<float, N\> CQL type (Phase 1+2)
  - [x] CQL protocol v4 compatibility
  - [x] PREPARE with pk_count metadata
  - [x] EXECUTE positional bind values
  - [x] DELETE map element syntax
  - [x] DROP without TABLE keyword
  - [x] DROP ROLE
  - [x] LWT INSERT IF NOT EXISTS
  - [x] LWT IF conditions on UPDATE/DELETE
  - [x] Batch CAS
  - [x] SERIAL/LOCAL_SERIAL consistency levels
  - [x] BEGIN TRANSACTION/COMMIT/ROLLBACK parsing
  - [x] Pagination (result set paging with page state)
  - [x] now(), toTimestamp(), TTL() built-in functions
  - [x] SUBSCRIBE dual timestamps
  - [x] ~~gocql/Temporal wire compatibility~~ (14 protocol fixes, 2026-03-23)
  - [x] ~~System table column filtering~~ (SELECT specific columns from system.local/peers)
  - [x] ~~PREPARE result metadata for SELECT/LWT~~ (column count + types)
  - [x] ~~EXECUTE bind value substitution for collections~~ (map/list/set wire format decoding)
  - [x] ~~BATCH bind value substitution~~ (was skipping all bind values)
  - [x] ~~LWT response with existing row data~~ ([applied]=false includes actual row)
  - [x] ~~USING TTL ? / USING TIMESTAMP ? bind markers~~ (parse + substitute)
  - [x] ~~Built-in functions in build_column_info~~ (toTimestamp, now, uuid, token)
  - [x] ~~CqlCodec EOF handling~~ (healthcheck probes no longer flood logs)
  - [x] ~~Parser: WITH COMPACTION={map}, ALTER TABLE WITH, SELECT AS, UPDATE IF col=?~~
  - [x] ~~Murmur3Partitioner name~~ (Cassandra-compatible, not FerrosaPartitioner)
  - [x] ~~CQL doc examples test~~ (81.8% parser coverage, 204 Cassandra .cql files)
  - [x] ~~Python wire-level CQL test harness~~
  - [ ] Logged batch atomicity
  - [ ] Query tracing

### ferrosa-graph — Complete

- **LOC:** ~13,800 (28 files) | **Tests:** 268
- **Modules:** `adjacency` (observer, reconcile, schema), `bolt` (codec, handshake,
  message, server), `engine`, `error`, `executor` (aggregate, eval, expand,
  leapfrog, result, subscribe, varpath), `http`, `parser` (ast, lexer, parse_impl,
  token), `planner` (logical, physical)
- **What's done:** Full Cypher parser, expression evaluator with NULL propagation +
  three-valued logic + 10 built-in scalar functions, aggregation framework
  (count/sum/avg/min/max/collect with GROUP BY), variable-length paths via BFS with
  cycle detection + visited budget, SUBSCRIBE/UNSUBSCRIBE with SSE streaming + delta
  mode, leapfrog triejoin for worst-case optimal cyclic pattern matching, Bolt v5
  wire protocol (PackStream codec, chunked framing, version negotiation, TCP server
  on port 7687 with auth), HTTP/JSON endpoint with auth + TLS + error sanitization,
  hop property filtering, adjacency index with WriteObserver + full reconciliation.
- **Completed:**
  - [x] ~~Full adjacency reconciliation scan~~ (T5 — edge scan + orphan cleanup)
  - [x] ~~CREATE/SET/DELETE planning and execution~~
  - [x] ~~Result projection with property extraction~~
  - [x] ~~ORDER BY, LIMIT, DISTINCT~~
  - [x] ~~Expression evaluator with built-in functions~~
  - [x] ~~Aggregation framework (count, sum, avg, min, max, collect, GROUP BY)~~
  - [x] ~~Variable-length paths~~ (`[*]`, `[*3]`, `[*1..5]` syntax with BFS)
  - [x] ~~SUBSCRIBE/UNSUBSCRIBE execution~~ (SSE streaming, delta mode, subscription registry)
  - [x] ~~Hop property filtering~~ (relationship constraint evaluation during traversal)
  - [x] ~~WCO joins / leapfrog triejoin~~ (automatic cycle detection, sorted adjacency intersection)
  - [x] ~~Bolt v5 protocol~~ (PackStream, chunked framing, TCP server on port 7687)

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
  - [x] ~~Snapshot create/list/delete CLI commands~~ (PITR Sprint P-4)
  - [x] ~~Restore CLI command~~ (PITR Sprint P-4)

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
  under load. Exponential backoff reconnection for internode links.
  IpSlotGuard for CQL server connection management + TCP keepalive.
  Ships as .deb via `scripts/build-deb.sh` with systemd service unit.
- **Remaining:**
  - [x] ~~Graceful shutdown sequencing~~ (done)
  - [x] ~~Configuration file support~~ (TOML with env var override)
  - [x] ~~Flush + S3 sync on graceful shutdown~~ (zero data loss on SIGTERM)
  - [x] ~~Configurable flush interval~~ (FERROSA_FLUSH_INTERVAL_SECS)
  - [x] ~~Snapshot/restore REST API endpoints~~ (PITR Sprint P-5)
  - [x] ~~Backup & Restore web dashboard card~~ (PITR Sprint P-5)
  - [x] ~~Archive lag indicator~~ (PITR Sprint P-5)

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

### ferrosa-cluster — Phase 3 Complete + Accord Transactions

- **LOC:** ~22,000 (65+ files) | **Tests:** ~580 (unit + integration + Jepsen)
- **Modules:** `config`, `consistency`, `controller`, `ddl_path`, `error`, `mode`,
  `pair` (catchup, coordinator, ddl, handler, node, switchover), `state`, `write_path`,
  `raft` (mod, log_store, state_machine, network), `ring` (mod, strategy),
  `coordinator` (mod, write, read), `hint` (segment, store, delivery),
  `lifecycle` (join, decommission, streaming, rebalance),
  `accord` (state_machine, coordinator, conflict_index, protocol_log, recovery,
  dep_wait, ddl_drain, cross_shard, leaseholder, durability, mem_index, electorate,
  jepsen, metrics)
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
  - **Accord Transactions (Sprints A1-A7):**
  - [x] Core types: HLC Timestamp, TxnId, Ballot, ConflictIndex, ProtocolLog
  - [x] AccordStateMachine (39 tests) — consensus state machine
  - [x] AccordCoordinator — fast path (3/4 quorum), slow path (majority), quorum formulas
  - [x] CQL Router → Accord integration for LWT and transactions
  - [x] LWT: INSERT IF NOT EXISTS, IF conditions on UPDATE/DELETE, Batch CAS
  - [x] Dependency-wait with cycle detection (DepWaitGraph)
  - [x] DDL drain-and-block (DdlDrain) — pauses Accord during schema changes
  - [x] 11 recovery scenarios, RecoveryCoordinator
  - [x] 4 property-based tests, 24-step EPaxos test
  - [x] MemIndex (BTreeMap-based conflict index)
  - [x] Leaseholder assignment — linearizable local reads
  - [x] Jepsen infrastructure: TestCluster, NemesisController, HistoryRecorder, LinearizabilityChecker
  - [x] Jepsen register test (3 workloads), bank test, write-skew test
  - [x] Crash recovery replay via `.accord` sidecar files
  - [x] DurabilityService / ExclusiveSyncPoint
  - [x] Performance baseline and regression suite
  - [x] BEGIN TRANSACTION/COMMIT/ROLLBACK — read-set/write-set extraction, transaction limits
  - [x] Cross-shard conflict detection and execution
  - [x] Client retry on Accord contention
  - [x] Electorate reconfiguration: epoch propagation, JoinElectorate 4-gate, shrink/resize, epoch transition drain
  - [x] Two-phase DDL with Accord coordination
  - [x] Full Jepsen nemesis suite, chaos minority kill
  - [x] 9 Accord observability metrics
  - [x] UDF/UDA integration with Accord (18 tests)
  - [x] Transactional secondary index reads (READ_2I, 5-layer merge)
  - [x] SUBSCRIBE dual timestamps for Accord ordering
- **Recent fixes (2026-04-02, cluster formation hardening):**
  - [x] system.peers tokens column fix
  - [x] Bootstrap streaming all-nodes fix (Phase A: schema convergence, Phase B: streaming, Phase C: leader promotes)
  - [x] LazyRaft handler registration — Raft handlers registered before async init (7b057b0)
  - [x] ClusterInvite sent on Data lane with 10-attempt retry (808b72b)
  - [x] ClusterInvite delivered synchronously before Raft init (30768c0)
  - [x] ClusterInvite triggers cluster transition on receiving nodes (ba7599a)
  - [x] FERROSA_CLUSTER_MODE config removed — progressive join is the only mode (83943a5)
- **Remaining:**
  - [ ] NetworkTopologyStrategy (multi-DC)
  - [ ] Read repair (full inline repair)
  - [ ] Quorum Lease / Mencius optimizations (Paxos-Raft paper)
- **Spec:** [Cluster Phase 2 Design](../superpowers/specs/2026-03-14-cluster-phase2-design.md)
- **Accord Spec:** [Accord Transactions](accord.md)
- **Accord Plan:** [Accord Project Plan](accord-project-plan.md)
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
| ~~Beta release v1.0.0-beta.3~~ | — | Released |
| ~~Secondary index pipeline (Sprints I-1 to I-4)~~ | — | Done (feature branch) |
| ~~PITR (Sprints P-1 to P-5)~~ | — | Done (feature branch) |
| ~~Accord Transactions (Sprints A1-A7)~~ | — | Done (merged PR #77, 2,808 tests) |
| ~~gocql/Temporal wire compatibility~~ | — | Done (18 commits, PR #78) |
| ~~SSTable expiring cell (TTL) serialization~~ | — | Done (P0 data corruption fix) |
| ~~SSTable reader fuzz testing~~ | — | Done (proptest, 9 fuzz tests) |
| ~~Cassandra Murmur3Partitioner compat~~ | — | Done |
| Cluster formation hardening (S1-S3) | fix/standalone-progressive-join | S1-S3 complete — S1: DDL blocked, pk_read, NTS DC validation, parking_lot, spawn_tracked, quorum committed_size, formation timeout. S2: hints all replicas, RowWire streaming, read_range capped, promotion delay configurable. S3: broadcast map cleanup, LazyRaft 3x retry, BroadcastResolver trait, Raft config tunable, digest Result. S4 active. |
| Temporal v1.31.0 on ferrosa | ferrosa-temporal | Running (shard acquisition WIP) |
| Beta release v1.0.0-beta.4 | — | Sprints complete |
| NetworkTopologyStrategy (multi-DC) | — | Planned |

## Path to Distributed Operation

The critical path from single-node to multi-node:

1. ~~**ferrosa-storage:** Commit log replay + compaction execution~~ (Done — PR #38)
1. ~~**ferrosa-net:** Internode transport (Phase 1)~~ (Done — PR #39)
1. ~~**ferrosa-cluster:** Pair mode — write forwarding, DDL replication, failover~~ (Done)
1. ~~**ferrosa-cluster:** Raft metadata, ring topology, coordinator pattern~~ (Done — Phase 2)
1. ~~**ferrosa-cluster:** End-to-end cluster wiring in binary~~ (Done — Phase 3)
1. ~~**ferrosa-schema:** System table persistence (Chunk B)~~ (Done — SystemTableWriter + persistence.rs)
1. ~~**ferrosa-cluster:** Hinted handoff~~ (Done — Phase 3)
1. **ferrosa-cluster:** NetworkTopologyStrategy (multi-DC)
1. **Beta release**

## Accord Transactions

Ferrosa implements the Accord consensus protocol (based on the Accord paper from
Cassandra 5.x) for distributed transactions. The implementation spans 7 sprints
(A1-A7) with 2,808 passing tests.

### Protocol Overview

Accord provides serializable transactions without a dedicated coordinator:

1. **PreAccept** — propose transaction to electorate, collect dependency sets
2. **Accept** — resolve conflicts via slow path if fast path quorum not met
3. **Commit** — record committed transaction in ProtocolLog
4. **Execute** — apply transaction after all dependencies are satisfied
5. **Apply** — write results to storage via SyncWriter

### Key Components

| Component | Purpose |
|-----------|---------|
| `AccordStateMachine` | Core consensus state machine (39 tests) |
| `AccordCoordinator` | Fast/slow path coordination with quorum formulas |
| `ConflictIndex` | Key-range conflict detection for concurrent transactions |
| `ProtocolLog` | Durable record of transaction decisions |
| `MemIndex` | BTreeMap-based in-memory conflict index |
| `RecoveryCoordinator` | 11 recovery scenarios for interrupted transactions |
| `DepWaitGraph` | Dependency-wait with cycle detection |
| `DdlDrain` | Drain-and-block gate for DDL during transactions |
| `CrossShard` | Cross-shard conflict detection and execution |
| `Leaseholder` | Leaseholder assignment for linearizable local reads |
| `DurabilityService` | ExclusiveSyncPoint for durability guarantees |
| `ReorderBuffer` | Dependency-ordered apply buffer |
| `WriteGate` | DDL drain-and-block gate |
| `SyncWriter` | Durable write-ahead for Accord commits |

### Testing Infrastructure

| Category | Tests | Description |
|----------|-------|-------------|
| Unit tests | ~400 | AccordStateMachine, coordinator, conflict index, protocol log |
| 24-step EPaxos test | 1 | Full protocol round-trip with dependency tracking |
| Property-based | 4 | QuickCheck-style tests for consensus invariants |
| Recovery scenarios | 11 | Interrupted transactions at each protocol phase |
| Jepsen register | 3 workloads | Read/write/CAS linearizability |
| Jepsen bank | 1 | Balance preservation under concurrent transfers |
| Jepsen write-skew | 1 | Serializable isolation verification |
| Chaos nemesis | Full suite | Network partition, minority kill, clock skew |
| UDF/UDA integration | 18 | WASM UDFs within Accord transactions |
| Performance regression | Suite | Baseline + automated regression detection |

### Observability

9 Accord-specific metrics exposed via the existing Prometheus endpoint:

- `ferrosa_accord_transactions_total` — total transactions processed
- `ferrosa_accord_fast_path_total` — transactions completing on fast path
- `ferrosa_accord_slow_path_total` — transactions requiring slow path
- `ferrosa_accord_contention_total` — contention events requiring retry
- `ferrosa_accord_recovery_total` — recovery coordinator invocations
- `ferrosa_accord_dep_wait_cycles` — dependency cycles detected
- `ferrosa_accord_cross_shard_total` — cross-shard transactions
- `ferrosa_accord_electorate_reconfig_total` — electorate reconfigurations
- `ferrosa_accord_apply_latency_seconds` — transaction apply latency histogram

See [Accord Specification](accord.md) and [Accord Project Plan](accord-project-plan.md)
for full details.

## Temporal Integration (2026-03-23)

Temporal v1.31.0 running on ferrosa as a Cassandra-compatible backend. This exposed
and drove fixes for 14 CQL protocol bugs, 1 P0 SSTable data corruption bug, and
established comprehensive CQL language and SSTable format testing infrastructure.

### Bugs Fixed

| # | Bug | Severity | Root Cause |
|---|-----|----------|------------|
| 1 | System table column filtering | HIGH | SELECT specific columns returned all 16 |
| 2 | PREPARE result metadata empty | HIGH | SELECT prepared statements reported 0 result columns |
| 3 | CqlCodec EOF "bytes remaining" | LOW | Healthcheck probes left partial frames |
| 4 | Table option map literals | MEDIUM | WITH COMPACTION = {map} not parsed |
| 5 | Collection bind value decoding | HIGH | Map/list/set bind values decoded as blob |
| 6 | BATCH bind values skipped | HIGH | BATCH handler didn't substitute bind markers |
| 7 | LWT PREPARE metadata | HIGH | IF NOT EXISTS reported 0 result columns |
| 8 | LWT response NULLs | HIGH | [applied]=false returned NULLs, not existing row |
| 9 | BATCH LWT void result | HIGH | BATCH with IF NOT EXISTS returned void |
| 10 | USING TTL ? not parsed | MEDIUM | Parser rejected bind markers in USING TTL |
| 11 | TTL bind value not substituted | P0 | EXECUTE didn't update using_ttl from bind values |
| 12 | [ttl] synthetic column missing | HIGH | PREPARE bind metadata didn't include TTL column |
| 13 | toTimestamp not in build_column_info | MEDIUM | Built-in function list incomplete |
| 14 | SSTable expiring cell TTL | P0 | Writer never set CELL_IS_EXPIRING or wrote TTL bytes |
| 15 | Capacity overflow panic | P0 | Corrupt cell length caused allocation panic |
| 16 | Partitioner name | LOW | Reported FerrosaPartitioner, gocql needs Murmur3 |

### Test Infrastructure Added

| Test | Type | Coverage |
|------|------|----------|
| `cassandra_cql_examples.rs` | Parser doc test | 81.8% (707/864 Cassandra .cql examples) |
| `test_cassandra_cql_examples.py` | Wire-level driver test | Python DataStax driver vs live ferrosa |
| SSTable cell roundtrip (proptest) | Property-based fuzz | 4 tests × 256+ random inputs each |
| SSTable reader fuzz (proptest) | Corruption fuzz | 5 tests × 256+ random inputs each |
| CassandraSSTableWriter (Java) | Cross-compat fixture | 5 fixture sets via CQLSSTableWriter |
| FMEA corruption resilience | Integration | 4 tests: truncated/zero/evolved/corrupt Data.db |
| Docker healthcheck fix | Infrastructure | bash /dev/tcp instead of nc |

### Temporal Status

- Schema migration: **PASS** (all 14 Cassandra versions applied)
- Server startup: **PASS** (all 4 services: frontend, history, matching, worker)
- Namespace registration: **PASS** (default namespace via admin-tools)
- Shard acquisition: **WIP** (retryable errors, LWT existing row data fix needed for UPDATE IF)
- Visibility: SQLite (Temporal dropped Cassandra visibility in v1.24)
- UI: **PASS** (<http://host:8233>)
- API: **PASS** (<http://host:7243/api/v1/system-info> returns v1.31.0)

## Related Documents

- [Components](components.md) — crate dependency graph
- [Overview](overview.md) — system architecture
- [Accord Transactions](accord.md) — Accord consensus protocol specification
- [Accord Project Plan](accord-project-plan.md) — sprint completion status
- [Architecture Design](../superpowers/specs/2026-03-11-ferrosa-architecture-design.md) — full design spec
- [Schema Replication Design](../superpowers/specs/2026-03-14-schema-replication-design.md) — DDL replication spec
- [Secondary Indexes Design](../superpowers/specs/2026-03-14-secondary-indexes-design.md) — pluggable index framework spec
- [Schema Replication Threat Model](threat-model-schema-replication.md) — STRIDE analysis (T21-T28)
