# System Overview

> Last updated: 2026-03-13
> Status: Approved

## Overview

Ferrosa is a distributed database that speaks CQL, backed by S3-compatible object storage for durability and ephemeral local storage for performance. It is built as independent Rust crates, informed by a deep analysis of Apache Cassandra's architecture but designed as idiomatic Rust from the ground up.

## System Diagram

```mermaid
graph TB
    subgraph Clients
        D1[CQL Drivers]
        D2[cqlsh]
    end

    subgraph "Ferrosa Node"
        CQL[ferrosa-cql<br/>CQL Protocol v5]
        Schema[ferrosa-schema<br/>DDL, System KS]
        Cluster[ferrosa-cluster<br/>Raft, Routing, CL]
        Storage[ferrosa-storage<br/>Memtable, Cache, Compaction]
        SST[ferrosa-sstable<br/>BTI Read + Write]
        Net[ferrosa-net<br/>Internode Protocol]
    end

    subgraph "Persistence"
        NVMe[Ephemeral NVMe<br/>Local Cache]
        S3[S3-Compatible Store<br/>Durable Storage]
    end

    subgraph "Other Ferrosa Nodes"
        N2[Node 2]
        N3[Node 3]
    end

    D1 & D2 -->|CQL Native Protocol v5| CQL
    CQL --> Schema
    CQL --> Storage
    CQL --> Cluster
    Cluster --> Storage
    Cluster --> Net
    Storage --> SST
    SST --> NVMe
    SST -.->|async upload| S3
    Storage --> NVMe
    Storage -.->|write-behind| S3
    Net <-->|Internode| N2 & N3
    Cluster -.->|Raft consensus| N2 & N3
```

## Design Principles

1. **S3 as source of truth** — nodes are ephemeral, SSTables live in object storage
1. **CQL compatibility** — existing drivers and tools work unchanged
1. **Rust-native** — idiomatic Rust with clean ownership, not a Java transliteration
1. **Cassandra's consistency model** — tunable CL (ONE, QUORUM, ALL) preserved
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
        R2 --> R3[ferrosa-storage]
        R3 --> R4[ferrosa-schema]
        R4 --> R5[ferrosa-cql]
        R5 --> R6[ferrosa-net]
        R6 --> R7[ferrosa-cluster]
        R7 --> R8[ferrosa binary]
    end

    A4 -.->|feeds| R2
    A5 -.->|feeds| R5
    A2 -.->|feeds| R7
    A3 -.->|scopes| R3
```

Track 1 (Java analysis) informs Track 2 (Rust implementation). Track 1 is analysis only, not a deliverable.

**Current progress**: All core crates are implemented. `ferrosa-common`, `ferrosa-sstable`, `ferrosa-storage`, `ferrosa-schema`, and `ferrosa-cql` (Parts A-D: protocol, parser, routing, prepared cache, security hardening) are complete. `ferrosa-graph` Phase 1 is complete (Cypher parser, planner, executor, adjacency index, HTTP endpoint with auth/TLS). The `ferrosa` binary crate composes everything into a working single-node database accepting CQL on port 9042 and optionally graph queries on port 7474. Next up: `ferrosa-net` (internode protocol) and `ferrosa-cluster` (Raft, distributed coordination).

## Key Architectural Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Deployment | AWS-first, flag lock-in | Start concrete, stay portable |
| Storage | Write-behind async S3 | Minimal write latency, quorum mitigates loss window |
| SSTable | Phased: BTI read+write first, Big read later | Focus on modern format, migration path preserved |
| Protocol | CQL client compat, own internode | Apps work unchanged, clean internal design |
| Consensus | Raft metadata, tunable CL | Proven Rust libs + Cassandra semantics |
| Partitioner | Murmur3Partitioner | Cassandra SSTable compatibility |

## AWS Lock-in Flags

Ferrosa is AWS-first but must remain portable to S3-compatible stores (MinIO, etc.):

- **S3 object metadata** (`x-amz-meta-*`): Standard across S3-compatible stores. No lock-in.
- **S3 client library**: Using `object_store` crate 0.11 (Apache Arrow project) with `aws` feature. Supports S3, MinIO (via endpoint override), GCS, Azure. Configured via `FERROSA_S3_*` environment variables in `ObjectStoreConfig`.
- **S3 conditional writes**: The manifest uses etag-based conditional put (`PutMode::Update`) for CAS. The `object_store` crate supports this on S3 and MinIO. Other S3-compatible stores may vary — flag as portability concern if expanding.

## Research Items

Deferred but tracked for future investigation:

| Area | Options | Status |
|------|---------|--------|
| Distributed transactions | Accord, Tempo, Janus, EPaxos | Research — evaluate when core is stable |
| Clock synchronization | HLC, TrueTime-like | Research — needed for cross-DC consistency |
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
- [Data Flow](data-flow.md) — write/read paths and S3 lifecycle
- [Testing](testing.md) — test infrastructure and suites
