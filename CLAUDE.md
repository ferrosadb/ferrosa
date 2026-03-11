# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ferrosa is a Rust reimplementation of Apache Cassandra with S3-backed storage. The repository has two tracks:

- **Rust workspace** (primary): Independent crates that compose into a distributed database
- **Cassandra submodule** (`cassandra/`): Apache Cassandra 5.1 source as a behavioral reference/oracle for Track 1 analysis

See `docs/superpowers/specs/2026-03-11-ferrosa-architecture-design.md` for the full architecture spec.

## Rust Workspace (Track 2 — Primary)

Cargo workspace with these crates (in build order):

| Crate | Purpose |
|-------|---------|
| `ferrosa-common` | Shared types: Token, PartitionKey, DecoratedKey, CellValue |
| `ferrosa-sstable` | Read Cassandra Big+BTI SSTables, write BTI format |
| `ferrosa-storage` | Memtable, commit log, compaction, S3 write-behind, cache |
| `ferrosa-schema` | Table/keyspace definitions, system keyspaces, schema evolution |
| `ferrosa-cql` | CQL native protocol v5, query parsing/execution |
| `ferrosa-net` | Custom internode protocol, TLS, connection management |
| `ferrosa-cluster` | Raft metadata, tunable CL, routing, repair, hinted handoff |
| `ferrosa` | Binary — composes all crates into the running database |

```bash
# Build
cargo build

# Test
cargo test                           # All crates
cargo test -p ferrosa-sstable        # Single crate

# Lint
cargo clippy --all-targets
cargo fmt --check
```

## Cassandra Submodule (Track 1 — Analysis Reference)

The `cassandra/` directory is a git submodule of Apache Cassandra (`git@github.com:apache/cassandra.git`). It exists for:

- DSM (Dependency Structure Matrix) analysis
- Behavioral characterization
- SSTable format reverse engineering
- CQL protocol documentation

Commands run from `cassandra/`:

```bash
cd cassandra
ant build                    # Compile
ant test                     # Unit tests
ant testsome -Dtest.name=MyTest  # Single test
ant check                    # Code checks
```

### Cassandra Architecture Reference

Source under `cassandra/src/java/org/apache/cassandra/`:

| Package | Purpose |
|---------|---------|
| `cql3/` | CQL query language |
| `db/` | Storage engine, memtable, compaction |
| `io/sstable/` | SSTable formats (Big + BTI) |
| `gms/` | Gossip protocol |
| `service/` | StorageService, StorageProxy |
| `service/accord/` | Accord consensus (5.x) |
| `tcm/` | Cluster metadata service |
| `db/commitlog/` | Commit log |
| `cache/` | Row, key, chunk caches |
| `repair/` | Anti-entropy repair |

### Checkstyle Rules (Cassandra)

- **Clock**: Use `Clock.Global`, not `System.currentTimeMillis()`
- **Executors**: Use `ExecutorFactory.Global`, not `java.util.concurrent.Executors`
- Suppress with: `// checkstyle: permit this import`

## Key Design Decisions

- **Storage**: Write-behind async S3 — local ephemeral disk as cache, S3 as durable store
- **SSTable**: Read Big+BTI, write BTI, future native format behind feature flag
- **Protocol**: CQL client compatible, own internode protocol (not Cassandra wire compat)
- **Consensus**: Raft for metadata (openraft), tunable consistency for data, transactions deferred
- **Partitioner**: Murmur3Partitioner (Cassandra compatible)
- **Target**: AWS-first, flag any lock-in for S3-compatible portability
