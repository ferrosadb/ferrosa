# Ferrosa

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Status: Developer Preview](https://img.shields.io/badge/status-developer%20preview-orange)](#project-status)

A Rust reimplementation of Apache Cassandra with S3-backed storage.

Ferrosa is a distributed database that speaks the CQL protocol, enabling existing
Cassandra applications to connect without modification. Under the hood, it replaces
Cassandra's local-disk storage model with a write-behind architecture where ephemeral
local storage serves as a fast cache and S3-compatible object storage provides
durability.

> **Status: Developer Preview.** Ferrosa is under active development. APIs, on-disk
> formats, and configuration may change before a stable 1.0. Don't run it on data you
> can't lose. Please report issues — we want to hear them.

## Quick Install

```bash
curl -fsSL https://ferrosadb.com/install.sh | bash
```

The installer detects your platform (macOS arm64/x86_64, Linux x86_64/aarch64),
downloads the latest release into `~/.ferrosa/bin/`, writes a default config to
`~/.ferrosa/config/`, and offers to register a launchctl/systemd unit and set CQL
admin credentials.

To build from source instead, see [Building](#testing) below.

## Why Ferrosa?

Apache Cassandra is a proven distributed database based on the
[Amazon Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).
It works well, but carries decades of accumulated complexity: non-idiomatic Java for
performance reasons, tightly coupled subsystems, and a storage engine designed around
assumptions (cheap local disk, persistent nodes) that don't match cloud-native
infrastructure.

Ferrosa starts from a deep analysis of Cassandra's architecture — understanding what
it does, what's essential, and what's accidental complexity — then builds a clean Rust
implementation that takes advantage of modern hardware and cloud infrastructure.

### Design Principles

- **S3 as the source of truth.** Nodes are ephemeral. SSTables live in object storage.
  A new node can serve reads within seconds by fetching from S3, not hours of streaming.
- **CQL compatibility.** Existing applications, drivers, and tools work unchanged.
  Ferrosa speaks CQL native protocol v5.
- **Rust-native, not a transliteration.** Ferrosa is built as idiomatic Rust with clean
  ownership boundaries, not a line-by-line port of Java code.
- **Cassandra's consistency model.** Tunable consistency levels (ONE, QUORUM, ALL) work
  as expected. Replication factor and consistency level semantics are preserved.
- **Independent crates.** Each subsystem is a standalone Rust crate with its own tests
  and documentation. `ferrosa-sstable` can read Cassandra SSTables and is useful on
  its own for migration tooling.

## Architecture

```mermaid
graph TB
    subgraph Clients
        D1[CQL Drivers]
        D2[cqlsh]
        D3[ferrosa-ctl<br/>CLI + TUI Monitor]
        D4[Graph Clients<br/>Bolt / HTTP]
    end

    subgraph "Ferrosa Node"
        CQL[ferrosa-cql<br/>CQL Protocol v5]
        Graph[ferrosa-graph<br/>Cypher + Bolt v5]
        Schema[ferrosa-schema<br/>DDL, Auth, Audit]
        Cluster[ferrosa-cluster<br/>Raft, Routing, CL]
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

    D1 & D2 -->|CQL Native Protocol v5| CQL
    D3 -->|CQL + HTTP| CQL & Web
    D4 -->|Bolt v5 / HTTP| Graph
    CQL --> Schema
    CQL --> Storage
    CQL --> Cluster
    CQL --> Index
    CQL --> UDF
    Graph --> Schema
    Graph --> Storage
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

### Storage Model: Write-Behind Async S3

Writes go to a local commit log and memtable, then acknowledge to the client based on
the configured consistency level. SSTables are flushed to local ephemeral storage and
asynchronously uploaded to S3. The commit log is also shipped to S3 on a short interval
(configurable interval) for crash recovery.

Data durability during the async upload window is protected by:

1. **Quorum writes** — data exists on multiple replicas before acknowledgment
2. **Commit log shipping** — small, frequent uploads to S3 (seconds, not minutes)
3. **Upload priority** — freshly-flushed SSTables upload before compaction output
4. **Replica coordination** — at least one replica confirming S3 upload marks data fully durable
5. **Increased quorum (optional)** — users can set write CL=ALL or higher RF for maximum durability during migration

Reads check memtable first, then local SSTable cache, falling back to S3 on cache miss.
Bloom filters and partition indices are always cached locally.

### SSTable Compatibility

Ferrosa implements Cassandra's SSTable formats in phases:

- **BTI format** (trie-based, Cassandra 5.x default) — primary read/write format, implemented first
- **Big format** (legacy) — read support planned for migrating older Cassandra deployments

Storage I/O is abstracted behind `ReadAt`/`WriteAt` traits, enabling the same SSTable code to
read from local files or S3. A future native Ferrosa format optimized for S3 access patterns
is planned behind a feature flag.

### Cluster Coordination

- **Metadata consensus:** Raft (via `openraft`) for schema, topology, and token management
- **Data consistency:** Cassandra-compatible tunable consistency levels
- **Failure detection:** Heartbeat-based with configurable thresholds
- **Internode protocol:** Custom binary protocol over TCP with TLS

Distributed transactions (Accord-style) are a research item, not yet implemented.

## Crates

| Crate | Description |
|-------|-------------|
| `ferrosa` | Binary — composes all crates into the running database |
| `ferrosa-cluster` | Raft metadata, node membership, tunable CL, request routing, hinted handoff |
| `ferrosa-cql` | CQL native protocol v5, query parsing, execution, EXPLAIN |
| `ferrosa-graph` | Graph query engine — Cypher parser, Bolt v5, adjacency index, HTTP endpoint |
| `ferrosa-index` | Pluggable secondary indexes — B-tree, hash, composite, phonetic, vector |
| `ferrosa-udf` | WASM-sandboxed user-defined functions and aggregates |
| `ferrosa-storage` | Memtable, commit log, compaction, S3 write-behind, PITR, cache management |
| `ferrosa-schema` | Table/keyspace definitions, auth, audit, system keyspaces |
| `ferrosa-sstable` | Read/write BTI SSTables, trie indices, Bloom filter, compression |
| `ferrosa-net` | Internode protocol, connection management, priority-lane RPC |
| `ferrosa-ctl` | CLI admin tool with TUI monitoring, snapshot/restore commands |
| `ferrosa-common` | Shared types: Token, PartitionKey, DecoratedKey, CQL types |

## Testing

```bash
cargo test                        # All crates
cargo test -p ferrosa-storage     # Single crate
cargo clippy --all-targets        # Lint
cargo fmt --check                 # Format check
```

## Project Status

All 12 crates are implemented with ~115,000+ lines of Rust and ~1,650+ tests. The
production cluster sprint is complete with Raft consensus, hinted handoff, node
lifecycle management, and integration tests. Secondary indexes have a full query
planner pipeline. Point-in-time recovery is implemented with commit log archiving,
snapshot management, and timestamp-filtered restoration. See the
[architecture specs](specs/README.md) for the full specification and status.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the development
workflow, hygiene checklist, and what we accept. By contributing, you agree to the
[Code of Conduct](CODE_OF_CONDUCT.md).

Security issues should be reported privately — see [SECURITY.md](SECURITY.md).

## License

Ferrosa is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE) for details.

Copyright 2026 Ferrosa, Inc.
