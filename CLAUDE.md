# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ferrosa is a Rust reimplementation of Apache Cassandra with S3-backed storage. The repository has two tracks:

- **Rust workspace** (primary): Independent crates that compose into a distributed database
- **Cassandra submodule** (`cassandra/`): Apache Cassandra 5.1 source as a behavioral reference/oracle for Track 1 analysis

See `superpowers/specs/2026-03-11-ferrosa-architecture-design.md` for the full architecture spec.

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

## Directory Layout Rules

- **`docs/`** — PUBLIC marketing site served via GitHub Pages (ferrosadb.com). Contains only HTML, CSS, SVG, and `CNAME`. **NEVER put internal specs, plans, rustdoc output, or non-public content here.** Changes to `docs/` trigger the Pages deployment workflow.
- **`superpowers/`** — Internal specs (`superpowers/specs/`) and implementation plans (`superpowers/plans/`). Not publicly served.
- **`specs/`** — Architecture specs, threat models, status docs. Not publicly served.
- **`.github/workflows/docs.yml`** — Deploys `docs/` to GitHub Pages. Must ONLY deploy from `docs/`. Never deploy rustdoc (`target/doc/`) or any generated content to Pages.

## Test Policy — Non-Negotiable Rules for All Agents

These rules apply to every agent working in this repository. They cannot be overridden by task instructions.

- **No `#[ignore]`** — Never add `#[ignore]` to a test. There are zero legitimately ignored tests in this codebase.
- **No silent returns** — Never write `if condition { return; }` in a test body to make a test appear to pass when it didn't run. A test that returns early shows as `ok` but ran nothing. This is forbidden.
- **Panic on missing infrastructure** — If a test requires infrastructure (Firecracker, Podman/Docker, cluster nodes), it must `panic!` with a clear message explaining what to set up. Example:
  ```rust
  if std::env::var("FERROSA_TEST_FIRECRACKER").is_err() {
      panic!("FERROSA_TEST_FIRECRACKER not set — run scripts/lima-fc-setup.sh first");
  }
  ```
- **Infrastructure env vars**:
  - `FERROSA_TEST_FIRECRACKER=1` — Firecracker VMs, SSH, cluster provisioning tests
  - `FERROSA_TEST_CLUSTER_NODES=<addr>` — pre-provisioned cluster
  - `FERROSA_TEST_CONTAINERS=1` — Docker/Podman compose cluster (MinIO + Cassandra compat tests)
- **Container runtime** — Use `container_runtime()` helper (auto-detects `docker` or `podman`) not hardcoded `"docker"`. macOS uses Podman Desktop.
- **Goal: `cargo test` with full infrastructure = zero failures, zero ignored.** Without infrastructure, tests fail loudly with setup instructions.

## Key Design Decisions

- **Storage**: Write-behind async S3 — local ephemeral disk as cache, S3 as durable store
- **SSTable**: Read Big+BTI, write BTI, future native format behind feature flag
- **Protocol**: CQL client compatible, own internode protocol (not Cassandra wire compat)
- **Consensus**: Raft for metadata (openraft), Accord for strict-serializable transactions (all writes routed through Accord)
- **Transactions**: See [specs/accord-project-plan.md](specs/accord-project-plan.md) for the Accord implementation plan (7 sprints, 4 phases)
- **Partitioner**: Murmur3Partitioner (Cassandra compatible)
- **Target**: AWS-first, flag any lock-in for S3-compatible portability

## Current Sprint Focus

See [specs/project-plan-correctness-sprints.md](specs/project-plan-correctness-sprints.md) for the active sprint plan: 6 sprints focused on single-DC Jepsen correctness, S3/SSTable Cassandra format validation, and Accord transaction correctness under all failure modes. Start here before taking on new work.
