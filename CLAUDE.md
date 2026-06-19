# CLAUDE.md

Guidance for Claude Code working in this repository.

## Project Overview

Ferrosa is a developer-preview Rust reimplementation of an Apache-Cassandra-shaped database with S3-backed storage. The workspace currently has 18 crates. Current work focuses on correctness evidence, cluster formation hardening, UCS compaction, and the remote index builder.

## Workspace

| Crate | Purpose |
|-------|---------|
| `ferrosa` | Main binary: CQL 9042, graph HTTP 7474, Bolt 7687, web 9090, Prometheus |
| `ferrosa-common` | Shared types: Token, PartitionKey, DecoratedKey, CellValue, Accord HLC/TxnId |
| `ferrosa-sstable` | Read Cassandra Big+BTI SSTables, write BTI format |
| `ferrosa-storage` | Memtable, commit log, compaction, S3 write-behind, cache, NVMe pinning, index pipeline |
| `ferrosa-schema` | Table/keyspace definitions, system keyspaces, DDL, auth, audit, UDT, virtual tables |
| `ferrosa-cql` | CQL native protocol v5, query parsing/execution, LWT, transactions, pagination |
| `ferrosa-index` | BTree, Hash, Composite, Phonetic, Filtered, Vector HNSW/IVFFlat, FullText |
| `ferrosa-net` | Custom internode protocol, TLS, connection management, graceful drain |
| `ferrosa-cluster` | Raft metadata (openraft), tunable CL, routing, repair, hinted handoff, Accord |
| `ferrosa-graph` | Property graph: eval, aggregations, var-length paths, SUBSCRIBE, Bolt v5, HTTP |
| `ferrosa-udf` | User-defined functions: parser, Wasmtime compilation, DDL replication |
| `ferrosa-worker` | Background task management |
| `ferrosa-sparql` | SPARQL 1.1 query endpoint |
| `ferrosa-ctl` | CLI + TUI: cluster management, snapshot/restore |
| `ferrosa-jepsen` | Distributed testing framework (Firecracker-based) |
| `ferrosa-loadgen` | Load testing: UCS compaction stress, integrity checks |
| `ferrosa-index-builder` | Standalone index builder: offloads secondary index construction from engine |

```bash
cargo build                          # Build all
cargo test                           # Test all
cargo test -p ferrosa-storage        # Single crate
cargo clippy --all-targets           # Lint (warnings are errors in CI)
cargo fmt --check                    # Format check
```

## Directory Layout

- **`docs/`** — PUBLIC marketing site (ferrosadb.com) via GitHub Pages. HTML/CSS/SVG only. **Never put specs, rustdoc, or internal content here.**
- **`specs/`** — Internal architecture specs, threat models, plans, proposed/open work, and evidence indexes. See [specs/README.md](specs/README.md).
- **`specs/proposed/`** — Design proposals and investigations, not implemented release claims.
- **`specs/todo/`** — Open work items awaiting implementation or triage.
- **`specs/in-process/`** — Only actively owned work items.
- **`specs/implemented/`** — Implementation evidence awaiting final verification/archive.
- **`specs/verified-test-plan/`** — Ambiguous items that need a live verification run before being declared fixed.
- **`specs/archive/`** — Completed plans, fixed bugs, historical analysis.
- **`specs/decisions/`** — Architecture Decision Records (ADRs).
- **`cassandra/`** — Git submodule of Apache Cassandra 5.1 (behavioral reference only).

## Key Design Decisions

- **Storage**: Write-behind async S3 — local ephemeral disk as cache, S3 as durable store
- **SSTable**: Read Big+BTI, write BTI, future native format behind feature flag
- **Protocol**: CQL client compatible, own internode protocol (not Cassandra wire compat)
- **Consensus**: Raft for metadata (openraft), Accord for strict-serializable transactions
- **Index Build**: Configurable via `FERROSA_INDEX_BACKEND` — `local` (in-process), `remote` (HTTP to ferrosa-index-builder), `off` (external builder only)
- **Partitioner**: Murmur3Partitioner (Cassandra compatible)
- **Target**: AWS-first, flag any lock-in for S3-compatible portability

## Current Focus

See [specs/project-plan-next-sprints.md](specs/project-plan-next-sprints.md) for the active sprint plan (S1-S4).

Active work areas:
- **Cluster formation**: state machine, formation protocol ([specs/cluster-formation-architecture.md](specs/cluster-formation-architecture.md))
- **UCS compaction**: unified compaction strategy ([specs/ucs-compaction-architecture.md](specs/ucs-compaction-architecture.md))
- **Remote index builder**: standalone binary, engine backend modes ([specs/remote-index-build-backend.md](specs/remote-index-build-backend.md))
- **Correctness**: open work in [specs/todo/](specs/todo/) and verification-only items in [specs/verified-test-plan/](specs/verified-test-plan/)

## Development Process

### Use `/tdd` for all implementation work

Every new feature and bug fix must follow the `/tdd` skill (red-green-refactor). Write the failing test first, make it pass, then refactor. No code lands without a test that exercises it.

### Use Rust skills and knowledge

Always apply Rust idioms and best practices. Use the language's type system to prevent bugs at compile time. Prefer `Result` over panics, ownership over reference counting, iterators over manual loops, and exhaustive matches over wildcards.

### Hygiene checklist (every change)

1. `cargo fmt` — format before anything else
2. `cargo clippy --all-targets` — zero warnings, no suppressions without justification
3. `cargo test -p <crate>` — affected crate tests pass
4. `cargo build --all-targets` — full workspace compiles clean
5. Feature branch — never commit to main directly
6. **Update the modified crate's docs** — see the rule below.

### Per-crate docs are part of "done"

Every crate carries its own `README.md` (what's implemented, how it works, its
dependencies/dependents) and a `specs/` directory (`overview.md`, `fmea.md`,
`roadmap.md`). **Changing a crate's behavior, public API, dependency set, or
known-issue/roadmap status is not done until that crate's `README.md` + `specs/`
are updated to match.** A crate's docs must reflect its *current* implementation
and dependency set — stale crate docs are treated like a failing check. The
crate-centric index lives at [`specs/crates.md`](specs/crates.md).

### CI must pass before pushing

Agents MUST verify that all CI checks will pass locally before pushing to a remote branch or creating a PR. This means running `cargo fmt --check`, `cargo clippy --all-targets`, and `cargo test` across the full workspace — not just the crate you touched. A CI failure that could have been caught locally is a wasted round-trip. Do not push and hope.

## Releases

See [specs/release-process.md](specs/release-process.md). Key rules:

- **Never hand-edit `[workspace.package] version` in `Cargo.toml` in a PR.** The nightly release automation owns it and derives the next SemVer from Conventional Commit history; a manual bump is ignored and overwritten.
- Use **Conventional Commit** subjects (`feat:`, `fix:`, `feat!:`/`BREAKING CHANGE:`) — the auto-bump level is computed from them, and sloppy subjects silently degrade a release to a `patch`.
- Releases are **tag-only** (the `main` ruleset forbids direct pushes). Nightly cuts are GitHub **prereleases** (nightly channel); a maintainer **promotes** one to **stable** via the *Promote Release to Stable* workflow.

## Test Policy

Non-negotiable rules for all agents:

- **No `#[ignore]`** — Zero legitimately ignored tests in this codebase.
- **No silent returns** — Never `if condition { return; }` in a test body.
- **Live-infra opt-in target** — Tests requiring Firecracker/Docker/cluster must be behind the crate feature `live-infra-tests`, so default verifier commands do not report missing-infra test bodies as passed.
- **Panic on missing infrastructure** — Once `live-infra-tests` is enabled, tests requiring Firecracker/Docker/cluster must `panic!` with setup instructions when the matching environment prerequisite is absent.
- **Infrastructure env vars**:
  - `FERROSA_TEST_FIRECRACKER=1` — Firecracker VMs
  - `FERROSA_TEST_CLUSTER_NODES=<addr>` — pre-provisioned cluster
  - `FERROSA_TEST_CONTAINERS=1` — Docker/Podman compose (MinIO + Cassandra compat)
- **Container runtime** — Use `container_runtime()` helper, not hardcoded `"docker"`.
- **Goal**: `cargo test` with full infrastructure = zero failures, zero ignored.
- **Local live-infra form**: enable the feature and the matching env var together, e.g. `FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-storage --features live-infra-tests compaction_end_to_end_pipeline -- --nocapture`.
