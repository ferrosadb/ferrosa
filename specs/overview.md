# System Overview

> Last updated: 2026-04-05
> Status: Approved

## Overview

Ferrosa is a distributed database that speaks CQL, Cypher, and SPARQL, backed by S3-compatible object storage for durability and ephemeral local storage for performance. It is built as independent Rust crates, informed by a deep analysis of Apache Cassandra's architecture but designed as idiomatic Rust from the ground up.

## System Diagram

```mermaid
graph TB
    subgraph Clients
        D1[CQL Drivers]
        D2[cqlsh]
        D3[ferrosa-ctl<br/>CLI + TUI Monitor]
        D4[Graph Clients<br/>Bolt / HTTP]
        D5[SPARQL Clients<br/>HTTP]
    end

    subgraph "Ferrosa Node"
        CQL[ferrosa-cql<br/>CQL Protocol v4/v5]
        Graph[ferrosa-graph<br/>Cypher + Bolt v5]
        SPARQL[ferrosa-sparql<br/>SPARQL 1.1 Query/Update]
        Schema[ferrosa-schema<br/>DDL, Auth, Audit]
        Cluster[ferrosa-cluster<br/>Raft, Routing, CL]
        Accord[Accord Consensus<br/>Transactions, LWT]
        Index[ferrosa-index<br/>Secondary + Vector Indexes]
        UDF[ferrosa-udf<br/>WASM Sandbox]
        Storage[ferrosa-storage<br/>Memtable, Cache, Compaction, PITR]
        SST[ferrosa-sstable<br/>BTI Read + Write]
        Net[ferrosa-net<br/>Internode Protocol]
        Web[Web Console<br/>Port 9090]
    end

    subgraph "Persistence"
        NVMe[Ephemeral NVMe<br/>Local Cache]
        S3[S3-Compatible Store<br/>Durable Storage]
    end

    subgraph "Other Ferrosa Nodes"
        N2[Node 2]
        N3[Node 3]
    end

    D1 & D2 -->|CQL Native Protocol v4/v5| CQL
    D3 -->|CQL + HTTP| CQL & Web
    D4 -->|Bolt v5 / HTTP| Graph
    D5 -->|HTTP SPARQL Protocol| SPARQL
    CQL --> Schema
    CQL --> Storage
    CQL --> Cluster
    CQL --> Accord
    CQL --> Index
    CQL --> UDF
    Graph --> Schema
    Graph --> Storage
    SPARQL --> Schema
    SPARQL --> Storage
    Accord --> Cluster
    Accord --> Storage
    Cluster --> Storage
    Index --> Storage
    Cluster --> Net
    Storage --> SST
    SST --> NVMe
    SST -.->|async upload| S3
    Storage --> NVMe
    Storage -.->|write-behind + PITR archive| S3
    Net <-->|Internode| N2 & N3
    Cluster -.->|Raft consensus| N2 & N3
```

## Design Principles

1. **S3 as source of truth** — nodes are ephemeral, SSTables live in object storage
1. **CQL compatibility** — existing drivers and tools work unchanged
1. **Rust-native** — idiomatic Rust with clean ownership, not a Java transliteration
1. **Cassandra's consistency model** — tunable CL (ONE, QUORUM, ALL) preserved
1. **Serializable transactions** — Accord consensus for LWT and multi-statement transactions
1. **Independent crates** — each subsystem testable and usable on its own

## Two Parallel Tracks

```mermaid
graph LR
    subgraph "Track 1: Analysis"
        A1[DSM Analysis] --> A2[Behavioral Characterization]
        A2 --> A3[What We Wouldn't Do ADR]
        A3 --> A4[SSTable Format Spec]
        A4 --> A5[CQL Protocol Spec]
    end

    subgraph "Track 2: Rust Implementation"
        R1[ferrosa-common] --> R2[ferrosa-sstable]
        R1 --> R2b[ferrosa-index]
        R2 --> R3[ferrosa-storage]
        R2b --> R3
        R3 --> R4[ferrosa-schema]
        R4 --> R5[ferrosa-cql]
        R5 --> R6[ferrosa-net]
        R6 --> R7[ferrosa-cluster]
        R3 --> R9[ferrosa-sparql]
        R7 --> R8[ferrosa binary]
        R9 --> R8
        R8 --> R9[ferrosa-ctl]
    end

    A4 -.->|feeds| R2
    A5 -.->|feeds| R5
    A2 -.->|feeds| R7
    A3 -.->|scopes| R3
```

Track 1 (Java analysis) informs Track 2 (Rust implementation). Track 1 is analysis only, not a deliverable.

**Current progress**: All 12 crates are implemented and functional. **Accord consensus transactions** are fully implemented (7 sprints, 2,808 tests): AccordStateMachine, AccordCoordinator (fast/slow path), LWT (INSERT IF NOT EXISTS, IF conditions on UPDATE/DELETE), BEGIN TRANSACTION/COMMIT/ROLLBACK, cross-shard conflict detection, Jepsen-style linearizability testing, electorate reconfiguration, crash recovery, and 9 observability metrics. The production cluster sprint is complete with Raft consensus, coordinated reads/writes, hinted handoff, node lifecycle (join/decommission/rebalance), reconnection, and integration tests. The graph engine is fully complete: Cypher parser, expression evaluator, aggregation framework, variable-length paths, SUBSCRIBE/UNSUBSCRIBE with SSE streaming, leapfrog triejoin, and Bolt v5 wire protocol. UDF/UDA with WASM sandboxing is complete and integrated with Accord transactions (18 tests). Secondary and vector indexes are consolidated with a full query planner pipeline, including transactional index reads (READ_2I, 5-layer merge). Point-in-time recovery is implemented: commit log archiving to S3, snapshot management, point-in-time restoration, CLI tooling, and web console integration. CQL driver compatibility is verified with cdrs-tokio, supporting protocol v4 and v5 negotiation. The `vector<float, N>` type enables embedding storage for AI/ML workloads. Phonetic indexes with Double Metaphone support fuzzy text search. The `system_observability` virtual tables expose live system state for monitoring. The `ferrosa` binary composes everything into a cluster-mode database with background maintenance, graceful shutdown, per-connection backpressure, PITR archiving, and exponential backoff reconnection. Available as a `.deb` package via GitHub Releases.

## Key Architectural Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Deployment | AWS-first, flag lock-in | Start concrete, stay portable |
| Storage | Write-behind async S3 | Minimal write latency, quorum mitigates loss window |
| SSTable | Phased: BTI read+write first, Big read later | Focus on modern format, migration path preserved |
| Protocol | CQL client compat, own internode | Apps work unchanged, clean internal design |
| Consensus | Raft metadata, tunable CL | Proven Rust libs + Cassandra semantics |
| Transactions | Accord consensus (serializable) | Cassandra 5.x compatible, no dedicated coordinator |
| Partitioner | Murmur3Partitioner | Cassandra SSTable compatibility |
| Driver compat | Protocol v4 + v5 negotiation | cdrs-tokio and other standard CQL drivers work out of the box |
| Embeddings | `vector<float, N>` CQL type | Native vector storage for AI/ML workloads; ANN query support via vector indexes |
| NVMe Table Pinning | Per-table `storage.pin = nvme` | Pins SSTables to local disk, bypassing S3 for sub-millisecond reads |
| Full-Text Search | Inverted index sidecars + `fts_match()` | BM25-scored full-text queries via CQL function; `CREATE INDEX ... USING 'fulltext'` |
| Compaction | STCS (default) + UCS (density-based) | UCS (Cassandra 5.0 CEP-26) subsumes LCS/STCS via fan factor; per-table DDL config |
| Phonetic Search | `SOUNDS LIKE` operator + phonetic indexes | Double Metaphone matching for fuzzy name lookups |

## AWS Lock-in Flags

Ferrosa is AWS-first but must remain portable to S3-compatible stores (MinIO, etc.):

- **S3 object metadata** (`x-amz-meta-*`): Standard across S3-compatible stores. No lock-in.
- **S3 client library**: Using `object_store` crate 0.11 (Apache Arrow project) with `aws` feature. Supports S3, MinIO (via endpoint override), GCS, Azure. Configured via `FERROSA_S3_*` environment variables in `ObjectStoreConfig`.
- **S3 conditional writes**: The manifest uses etag-based conditional put (`PutMode::Update`) for CAS. The `object_store` crate supports this on S3 and MinIO. Other S3-compatible stores may vary — flag as portability concern if expanding.

## Observability

Ferrosa includes a built-in observability system that exposes live database state through multiple interfaces without requiring external tooling for basic monitoring.

### Virtual Tables

The `system_observability` keyspace provides read-only virtual tables that are computed on demand rather than stored on disk:

| Table | Source | Contents |
|-------|--------|----------|
| `connections` | `ConnectionTracker` (ferrosa-cql) | Active CQL client connections, addresses, auth state |
| `active_queries` | `QueryTracker` (ferrosa-cql) | In-flight queries, elapsed time, bound parameters |
| `storage_stats` | `StorageStatsTable` (ferrosa-storage) | Memtable sizes, flush counts, compaction stats, cache hit rates |
| `secondary_indexes` | `SecondaryIndexesVirtualTable` (ferrosa-storage) | Per-index staleness, build status, pending SSTables, build errors |

Virtual tables are backed by the `VirtualTable` trait defined in ferrosa-schema (`virtual_table.rs`). Each implementation provides a `read_rows()` method that returns the current state as CQL rows. The `VirtualTableRegistry` (`virtual_registry.rs`) manages registration and lookup using `ArcSwap` for lock-free concurrent reads.

### Web Dashboard

The ferrosa binary hosts an Axum web server on port 9090 (`web/` module) that provides:

- **JSON API** — programmatic access to connection, query, and storage stats
- **Static file serving** — embedded HTML/JS dashboard for browser-based monitoring
- **Prometheus `/metrics` endpoint** — standard Prometheus text format for scraping by Prometheus, Grafana, Datadog, and other monitoring tools

### CQL Extensions

Two new CQL commands support real-time push-based monitoring:

- **`SUBSCRIBE`** — register for streaming updates on virtual table changes (backed by `subscribe.rs` in ferrosa-cql)
- **`UNSUBSCRIBE`** — cancel an active subscription

The `subscription_observer.rs` in ferrosa-storage bridges storage events into the subscription system, pushing updates to connected clients as state changes occur.

### ferrosa-ctl

`ferrosa-ctl` is a standalone CLI admin tool built with `ratatui` and `crossterm`. It connects to a running Ferrosa node and provides:

- **CLI mode** — issue admin queries, inspect virtual tables, check node health
- **TUI monitor mode** — live-updating terminal dashboard showing connections, active queries, and storage stats in a multi-panel layout

## Research Items

Deferred but tracked for future investigation:

| Area | Options | Status |
|------|---------|--------|
| Distributed transactions | Accord consensus | **Implemented** — 7 sprints (A1-A7), 2,808 tests, Jepsen-verified |
| Clock synchronization | HLC (Hybrid Logical Clock) | **Implemented** — HLC timestamps for Accord transaction ordering |
| Transport protocol | QUIC (`quinn` crate) | Research — better for multi-DC, built-in multiplexing |
| Native SSTable format | S3-optimized: larger blocks, content-addressed, embedded metadata | Research — behind feature flag, after BTI is solid |
| Object store abstraction | `object_store` crate (Apache Arrow) | **Adopted** — used for S3/MinIO/GCS/Azure portability |
| S3 conditional writes | Etag-based CAS via `object_store` `PutMode::Update` | **Adopted** — used in manifest for consistency |

## References

- Fleming et al., "Hunter: Using Change Point Detection to Hunt for Performance Regressions," ICPE '23. [Paper](https://dl.acm.org/doi/10.1145/3578244.3583719) | [Code](https://github.com/datastax-labs/hunter)
- DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store," SOSP '07. [Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Apache Cassandra Source](https://github.com/apache/cassandra)
- [AWS S3 Object Metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)
- [Sprites Documentation](https://docs.sprites.dev/)
- [fly.io Documentation](https://fly.io/docs/)

## Related Specs

- [Components](components.md) — crate architecture details
- [SSTable](sstable.md) — BTI format, trie encoding, I/O traits, compression
- [Data Flow](data-flow.md) — write/read paths, Accord transaction flow, and S3 lifecycle
- [Accord](accord.md) — Accord consensus protocol specification
- [Testing](testing.md) — test infrastructure and suites
