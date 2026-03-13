# Component Architecture

> Last updated: 2026-03-12
> Status: Approved

## Overview

Ferrosa is a Cargo workspace of 8 crates with a clean, acyclic dependency graph. Each crate has a single responsibility and can be tested independently.

## Dependency Graph

```mermaid
graph BT
    Common[ferrosa-common<br/>Shared types]
    SST[ferrosa-sstable<br/>SSTable read/write]
    Net[ferrosa-net<br/>Internode protocol]
    Storage[ferrosa-storage<br/>Write-behind S3 engine]
    Schema[ferrosa-schema<br/>DDL, system keyspaces]
    CQL[ferrosa-cql<br/>CQL protocol v5]
    Graph[ferrosa-graph<br/>Cypher/GQL parser]
    Cluster[ferrosa-cluster<br/>Raft, routing, CL]
    Bin[ferrosa<br/>Binary]

    SST --> Common
    Net --> Common
    Storage --> Common
    Storage --> SST
    Schema --> Common
    CQL --> Common
    CQL --> Schema
    CQL --> Storage
    Cluster --> Common
    Cluster --> Net
    Cluster --> Storage
    Bin --> CQL
    Bin --> Cluster
    Bin --> Common
```

## Components

### ferrosa-common

- **Purpose**: Shared low-level types used across all crates
- **Location**: `ferrosa-common/`
- **Dependencies**: None (leaf crate)
- **Key types**: `Token` (i64, Murmur3), `PartitionKey`, `DecoratedKey`, `CellValue` (bytes + timestamp + TTL), `Timestamp`, error types (`Error`, `Result`)
- **Boundary**: CQL-level type definitions (text, int, collections, UDTs) live in `ferrosa-cql`, not here

### ferrosa-sstable

- **Purpose**: Read and write Cassandra-compatible SSTable files (BTI format)
- **Location**: `ferrosa-sstable/`
- **Dependencies**: `ferrosa-common`, `lz4_flex`, `zstd`, `crc32fast`
- **Detailed spec**: [SSTable Format Specification](sstable.md)
- **Key interfaces**:
  - `ReadAt` — abstract positional I/O trait (file-system impl here, S3 impl in ferrosa-storage)
  - `SSTableReader<R: ReadAt>` — open and query BTI SSTables from `SSTableComponents<R>`
  - `SSTableWriter` — write BTI SSTables to in-memory `Vec<u8>` buffers (`WrittenSSTable`), caller provides `SerializationHeader`, `WriteOptions`, and pre-computed byte-comparable keys + Bloom filter hashes
- **Formats**: Phase 1: BTI (trie-based, Cassandra 5.x default) read + write. Phase 2: Big (legacy) read for migration. Phase 3: native Ferrosa format behind feature flag.
- **Components handled**: Data.db, Partitions.db (trie partition index), Rows.db (trie row index), Filter.db (Bloom filter), Statistics.db, CompressionInfo.db, TOC.txt
- **On-disk trie**: 16 node types with page-aware packing (4096-byte pages), bottom-up incremental construction, used by both partition and row indices
- **Compression**: LZ4 (default, `lz4_flex`), Zstd (`zstd`). Snappy/Deflate deferred to post-1.0.
- **Bloom filter**: Cassandra-compatible double-hashing using Murmur3 h1 + h2 from ferrosa-common
- **Standalone tools** (Phase 2): `ferrosa-sstable-dump`, `ferrosa-sstable-import`

### ferrosa-storage

- **Purpose**: Storage engine with S3 write-behind
- **Location**: `ferrosa-storage/`
- **Dependencies**: `ferrosa-common`, `ferrosa-sstable`, `arc-swap`, `parking_lot`, `crc32fast`, `object_store` (S3), `tokio`, `serde`, `serde_json`, `bytes`, `crossbeam-skiplist` (optional)
- **Status**: Parts A/B/C implemented — memtable, flush, merge, commit log, compaction, S3 upload manager, manifest, local cache, StorageEngine composition
- **Modules**:
  - `memtable/` — `Memtable` trait with two implementations: `ShardedBTreeMemtable` (64-shard `BTreeMap` with `parking_lot::RwLock`), `SkipListMemtable` (lock-free, behind `skiplist-memtable` feature flag)
  - `flush.rs` — `FlushTarget` trait, `FileFlushTarget` (disk), `InMemoryFlushTarget` (testing)
  - `merge.rs` — Read-path merge across memtable + multiple SSTables (last-write-wins)
  - `store.rs` — `TableStore`: lock-free per-table composition via `arc-swap`
  - `commitlog/` — CAS-allocated segments, CRC32-checksummed entries, configurable sync strategies (Periodic/Batch/Group), checkpoint tracking per table, CDC reader
  - `compaction/` — `SizeTieredStrategy` (STCS), `CompactionExecutor` (background `std::thread` with `mpsc`)
  - `upload/` — `UploadManager` (tokio task with bounded `mpsc`, exponential backoff retry), `ObjectStoreConfig` (12-factor env vars)
  - `manifest.rs` — S3 manifest with etag-based CAS (conditional put via `PutMode::Update`)
  - `cache.rs` — `LocalCache` with LRU eviction, pinned entries, size tracking
  - `engine.rs` — `StorageEngine` composing all components, `StorageEngineConfig` with `from_env()` for 12-factor config
- **Concurrency**:
  - *Memtable writes*: 64-shard `BTreeMap` — different partitions write in parallel; same-shard writes serialize on a per-shard `RwLock`. Alternative: `SkipListMemtable` (lock-free, `crossbeam-skiplist`, behind feature flag).
  - *Memtable flush*: Atomic swap via `arc-swap` — current memtable replaced with a fresh one; old memtable flushed to SSTable. Reads check both active and flushing memtable.
  - *SSTable reads during compaction*: `Arc`-based refcounting. Compaction creates new SSTables and atomically swaps the active set via `arc-swap`. In-flight reads hold references to old SSTables, cleaned up when last reference drops.
  - *S3 upload concurrency*: Independent tokio task observes new SSTables via bounded `mpsc` channel (backpressure), uploads without holding storage engine locks. Local files retained until S3 upload confirms.
- **Compaction strategies**: Size-Tiered (STCS) implemented; Leveled (LCS) and Time-Window (TWCS) are follow-on work
- **Follow-on work**: Compaction execution wiring (merge I/O), metadata collection from SSTables, S3 upload trigger from flush, manifest CAS loop integration, commit log recovery/replay, commit log S3 shipping, LCS/TWCS strategies, disk backpressure, grace period GC, orphan cleanup

### ferrosa-schema

- **Purpose**: Schema management, auth, audit, and system keyspaces
- **Location**: `ferrosa-schema/`
- **Dependencies**: `ferrosa-common`, `arc-swap`, `bcrypt`, `argon2`, `uuid`, `serde`, `serde_json`, `indexmap`, `tracing`, `password-hash`
- **Status**: Implemented — metadata types, schema registry with lock-free snapshots, auth (roles, permissions, RBAC, rate limiting), audit logging (composite sinks), system keyspace queries, secrets provider, production mode validation
- **Modules**:
  - `metadata/` — `KeyspaceMetadata`, `TableMetadata`, `ColumnMetadata`, replication params, caching params
  - `registry.rs` — `Schema` with `ArcSwap<SchemaSnapshot>` for lock-free reads, `AuthMethod` config
  - `auth/` — `AuthContext`, `Permission`, `Resource`, RBAC with `check_permission()`, `PasswordHasher` (bcrypt/argon2id), `AuthRateLimiter`
  - `audit/` — `AuditEvent`, `AuditSink` trait, `LogAuditSink`, `SystemTableAuditSink`, `CompositeSink`
  - `system/` — System keyspace queries: `system.local`, `system.peers`, `system_schema.keyspaces/tables/columns`, `system_auth.roles/role_members/role_permissions`
  - `secrets/` — `SecretsProvider` trait, `EnvSecretsProvider`
  - `startup.rs` — `validate_production_requirements()`, `DeploymentMode`
  - `convert.rs` — CQL-to-marshal type conversion
- **Key interfaces**: Table/keyspace definitions, CREATE/ALTER/DROP validation, system keyspace queries (`system.local`, `system.peers`, `system_schema.*`)
- **Persistence**: Schema is Raft-committed metadata (via `ferrosa-cluster`). All nodes have identical schema at the same Raft index.
- **Agreement**: Raft applied index comparison, not gossip-based version UUIDs (though UUIDs maintained for driver compat)

### ferrosa-cql

- **Purpose**: CQL native protocol v5 and query execution
- **Location**: `ferrosa-cql/`
- **Dependencies**: `ferrosa-common`, `ferrosa-schema`, `ferrosa-storage`, `tokio`, `tokio-util`, `bytes`, `futures`, `arc-swap`, `uuid`, `num-bigint`, `phf`, `md-5`, `tracing`
- **Status**: Part A implemented (protocol framing, CQL type system, TCP server, SASL PLAIN auth). Parts B-D (parser, query routing, prepared cache, security hardening) are next.
- **Implemented modules**:
  - `frame.rs` — CQL v5 binary framing, `CqlCodec` (Tokio `Encoder`/`Decoder`)
  - `types.rs` — `CqlValue` enum with encode/decode for all CQL types
  - `server.rs` — TCP listener, per-connection Tokio tasks
  - `connection.rs` — Connection handler, auth flow, request routing
  - `auth.rs` — SASL PLAIN authentication
  - `error.rs` — CQL error codes
- **Key interfaces**: Protocol framing, CQL type system with encode/decode, TCP server with auth
- **Auth**: SASL PLAIN initially, pluggable auth provider trait
- **Minimum viable subset**: STARTUP, OPTIONS, QUERY, PREPARE, EXECUTE, REGISTER, RESULT + basic types + system keyspace queries
- **Target**: All standard CQL drivers connect without modification

### ferrosa-graph

- **Purpose**: Graph query engine with Cypher/GQL support
- **Location**: `ferrosa-graph/`
- **Dependencies**: `phf`
- **Status**: Early stage — Cypher parser (lexer, parser, AST) implemented. Not yet integrated with storage or CQL layers.
- **Modules**:
  - `parser/` — `lexer.rs` (tokenizer), `parse_impl.rs` (recursive descent parser), `ast.rs` (AST types), `token.rs` (token definitions), `error.rs` (parse errors)
- **Design**: Data stored in normal CQL tables, accessed via system-managed adjacency index

### ferrosa-net

- **Purpose**: Custom internode protocol
- **Location**: `ferrosa-net/`
- **Dependencies**: `ferrosa-common`
- **Key interfaces**: Connection pooling, multiplexing, message framing, TLS (`rustls`)
- **Transport**: TCP + length-prefixed framing initially. QUIC is a research item.
- **Versioning**: Connection-level version negotiation handshake. Major version = breaking, minor = compatible.

### ferrosa-cluster

- **Purpose**: Distributed coordination
- **Location**: `ferrosa-cluster/`
- **Dependencies**: `ferrosa-common`, `ferrosa-net`, `ferrosa-storage`
- **Key interfaces**: Raft metadata consensus, node membership, token ring (Murmur3, vnodes), request routing, tunable CL, read repair, hinted handoff, anti-entropy repair
- **Raft design**: Single cluster-wide group via `openraft`. 3-5 voter nodes, rest are learners. Log on local disk + async S3 snapshots. New nodes bootstrap from S3 snapshot.
- **Research**: Accord/Tempo/EPaxos for transactions, HLC for clock sync

### ferrosa (binary)

- **Purpose**: Compose all crates into the running database
- **Location**: `ferrosa/` (workspace root binary)
- **Dependencies**: All crates
- **Key interfaces**: CLI, configuration loading, service startup/shutdown

## Build Order

```mermaid
gantt
    title Crate Build Sequence
    dateFormat X
    axisFormat %s

    section Foundation
    ferrosa-common          :done, 0, 1
    ferrosa-sstable         :done, 1, 3

    section Engine
    ferrosa-storage         :done, 3, 5
    ferrosa-schema          :done, 5, 6

    section Protocol
    ferrosa-cql             :active, 6, 8
    ferrosa-graph           :active, 6, 7
    ferrosa-net             :7, 8

    section Distributed
    ferrosa-cluster         :8, 10
    ferrosa binary          :10, 11
```

| Order | Crate | Testable Milestone |
|-------|-------|--------------------|
| 1 | ferrosa-common | Type definitions compile — **done** |
| 2 | ferrosa-sstable | Read real Cassandra SSTables, round-trip BTI — **done** |
| 3 | ferrosa-storage | Single-node writes + reads with S3 backend — **done** (core engine; follow-on items tracked) |
| 4 | ferrosa-schema | Schema registry, auth, audit, system keyspaces queryable — **done** |
| 5 | ferrosa-cql | cqlsh connects and runs basic queries — **Part A done** (framing, types, server, auth) |
| 5b | ferrosa-graph | Cypher parser produces valid ASTs — **parser done** |
| 6 | ferrosa-net | Two nodes exchange messages |
| 7 | ferrosa-cluster | 3-node cluster at QUORUM |
| 8 | ferrosa (binary) | Full database, characterization tests pass |

## Related Specs

- [Overview](overview.md) — system overview and design principles
- [Data Flow](data-flow.md) — write/read paths
- [Storage](storage.md) — storage engine details
- [CQL](cql.md) — CQL native protocol v5
