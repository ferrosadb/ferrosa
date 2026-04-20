# Design Structure Matrix — Ferrosa Workspace

> **Last refreshed:** 2026-04-20
> **Source:** `cargo metadata --format-version 1 --manifest-path /Users/bkearns/src/ferrosa/Cargo.toml`
> **Methodology:** Workspace-internal dependency graph, derived from `Cargo.toml` normal (non-dev) deps. Dev-only edges are listed separately and excluded from the layer topology so test helpers cannot mask production cycles.

## Executive Summary

| Metric | Value |
|---|---|
| Workspace crates | 17 |
| Normal-dep edges (internal) | 54 |
| Dev-only edges (internal) | 5 |
| Cycles (normal deps) | **0** |
| Cycles (including dev deps) | **0** |
| Longest chain (leaves → root) | 7 levels (L0 → L6) |
| Highest fan-in crate | `ferrosa-common` (13 dependents) |
| Highest fan-out crate | `ferrosa` binary (9 internal deps) |
| Highest LOC / high-fan-in crate | `ferrosa-storage` (39k LOC, fan-in=7) |

**Headline finding:** the workspace is a clean, acyclic DAG. No production crate has a circular dependency, and dev-dep back-edges (`ferrosa-ctl` → higher layers) do not close any cycle. Coupling is concentrated in three crates — `ferrosa-storage`, `ferrosa-schema`, and `ferrosa-sstable` — which is expected given their role as the shared substrate for everything above them.

**However, one critical dependency is invisible to `cargo`:** the ferrosa-memory MCP server writes directly to ferrosa-graph-owned tables (`typed_edges`, `folded_into`, `mentioned_in`, `co_occurs_with`, `supersedes`, `derived_edges_*`) via raw CQL `INSERT`s. This is a **schema-level coupling** that the crate graph cannot see; it creates a tight, version-coupled contract between two repositories with no compile-time enforcement. Details in the last section.

---

## Layer Topology (7 layers, leaves first)

Layer is defined as longest path from a leaf (no internal deps). `ferrosa-worker` and `ferrosa-index-builder` sit on their own sidebars — they depend on foundational crates but nothing depends on them.

```
L0  ferrosa-common         ferrosa-jepsen       ferrosa-net
L1  ferrosa-index          ferrosa-sstable      ferrosa-udf
L2  ferrosa-schema         ferrosa-worker
L3  ferrosa-storage
L4  ferrosa-cluster        ferrosa-index-builder
L5  ferrosa-cql            ferrosa-graph        ferrosa-sparql
L6  ferrosa (binary)       ferrosa-ctl          ferrosa-loadgen
```

Notes:

- `ferrosa-net` is a leaf (L0) — surprising at first glance but correct: the net crate exports the internode protocol/TLS/lane actor primitives, and `ferrosa-cluster`/`ferrosa` consume them. Net has no internal deps because it deliberately stays below the schema/storage layer.
- `ferrosa-jepsen` has zero internal edges: the Jepsen harness talks to ferrosa only through the CQL wire protocol, never as a library consumer. That isolation is a feature.
- The storage/schema/sstable layering resolves a historical ambiguity: `ferrosa-schema` depends on `ferrosa-sstable` and `ferrosa-index` (for BTI writing of system tables and index metadata), and `ferrosa-storage` then depends on all three.

## Dev-only back edges

```
ferrosa-ctl  (dev) -> [ferrosa-cluster, ferrosa-schema, ferrosa-storage, ferrosa-udf]
ferrosa-storage (dev) -> [ferrosa-common]
```

Neither edge closes a cycle; they exist so the CLI's integration tests can spin up higher-layer machinery without the CLI's production binary linking all of it.

## Cycle Check

Ran Tarjan-style DFS over the normal-deps adjacency list; zero SCCs of size > 1. Re-ran with dev-deps merged in; still zero. **All 17 crates are in singleton SCCs.**

This is stronger than Cassandra's JVM modularisation (which tolerates several cycles between `db`, `gms`, `net`, and `service`) and gives us incremental rebuild + reviewable module boundaries for free.

---

## Coupling — fan-in / fan-out / LOC

| Crate | Fan-in | Fan-out | External deps | LOC (src) | Role |
|---|---:|---:|---:|---:|---|
| `ferrosa-common` | **13** | 0 | 5 | 2,693 | Shared types — Token, PartitionKey, DecoratedKey, CellValue, Accord HLC. High fan-in is healthy; small LOC + external dep count means churn blast-radius is bounded. |
| `ferrosa-sstable` | **9** | 1 | 5 | 10,187 | SSTable read/write. Fan-in=9 is expected: every layer above storage reads SSTables. |
| `ferrosa-storage` | 7 | 4 | 22 | **39,097** | Memtable + commit log + compaction + S3 write-behind. Second-largest crate, high fan-in. This is our single biggest coupling risk — changing a storage API signature cascades through 7 direct consumers. |
| `ferrosa-schema` | 7 | 3 | 11 | 12,039 | Schema registry + auth + audit. 7 dependents reflects schema's role as metadata authority. |
| `ferrosa-index` | 7 | 1 | 6 | 7,117 | Index data structures (BTree, Hash, HNSW, Phonetic, FullText). |
| `ferrosa-cluster` | 4 | 6 | 22 | **41,734** | Raft + routing + repair + Accord. Largest crate. Fan-out=6 is where most of the dependency pressure lives. |
| `ferrosa-cql` | 3 | 7 | 25 | 34,927 | CQL protocol v5 + query execution. Highest fan-out; this crate is a "hub" that pulls most of the system together. |
| `ferrosa-udf` | 2 | 1 | 8 | 2,361 | Wasmtime UDF compilation. |
| `ferrosa-net` | 2 | 0 | 22 | 6,407 | Custom internode protocol, TLS, connection mgmt. |
| `ferrosa-graph` | 1 | 5 | 22 | 14,412 | Property graph, Cypher, Bolt, SUBSCRIBE. Consumed only by the `ferrosa` binary. |
| `ferrosa-sparql` | 1 | 6 | 12 | — | SPARQL 1.1 endpoint. |
| `ferrosa-worker` | 0 | 3 | 6 | 159 | Background-task harness. Tiny — essentially a crate-scoped glue module. |
| `ferrosa-ctl` | 0 | 1 | 11 | — | CLI + TUI. |
| `ferrosa-index-builder` | 0 | 4 | 12 | — | Standalone remote index builder binary. |
| `ferrosa-loadgen` | 0 | 5 | 14 | — | Load testing binary. |
| `ferrosa-jepsen` | 0 | 0 | 16 | — | External Jepsen harness. |
| `ferrosa` | 0 | 9 | 25 | — | Main binary — orchestrates everything. |

## Top-coupling crates — risk commentary

1. **`ferrosa-storage` (fan-in=7, 39k LOC, 22 external deps).** This is the highest blast-radius crate. Any breaking API change forces coordinated edits in `ferrosa-cluster`, `ferrosa-cql`, `ferrosa-graph`, `ferrosa-sparql`, `ferrosa-loadgen`, `ferrosa-index-builder`, `ferrosa-worker`. Mitigations already in place: write-behind boundary is async-trait'd, SSTable reader/writer is versioned. Recommendation: treat storage public API as a semver-committed surface in internal review.
2. **`ferrosa-cql` (fan-out=7, 35k LOC).** The CQL layer pulls storage + cluster + schema + sstable + index + udf + common. That's the entire application tier. `ferrosa-cql` is the natural integration hub but it is also where most bugs land because of that density. The CQL crate's 7-deep fan-out ties its build time to almost every lower layer.
3. **`ferrosa-cluster` (fan-out=6, 42k LOC).** Largest crate by LOC. Raft + routing + repair + Accord in one crate is itself a coupling hazard — splitting Accord into its own crate has been discussed and would break this tie.
4. **`ferrosa-schema` (fan-in=7).** Auth, audit, DDL, and system tables all live here per ADR-006 and ADR-008. Fan-in reflects that. Schema API is deliberately stable; churn is low.

## Interface vs implementation ratio (proxy)

Cargo metadata does not expose the pub/non-pub ratio directly, so we use **external-deps-per-internal-dep** as a rough proxy: a crate with many external deps and few internal ones is typically an "edge" crate (wire protocol, UI), while the inverse is a "hub" crate with more internal orchestration than external I/O.

| Crate | External / Internal | Classification |
|---|---:|---|
| `ferrosa-net` | 22 / 0 | Pure edge / wire |
| `ferrosa-storage` | 22 / 4 | Edge-heavy (S3, tokio, compression) |
| `ferrosa-cluster` | 22 / 6 | Balanced — both hub and edge |
| `ferrosa-cql` | 25 / 7 | Balanced hub (parser deps + driver compat) |
| `ferrosa-graph` | 22 / 5 | Edge-heavy (Bolt, HTTP, Cypher) |
| `ferrosa-common` | 5 / 0 | Pure domain types (ideal) |
| `ferrosa-sstable` | 5 / 1 | Pure core format (ideal) |
| `ferrosa-schema` | 11 / 3 | Balanced |
| `ferrosa-udf` | 8 / 1 | Wasmtime wrapper (edge) |
| `ferrosa-worker` | 6 / 3 | Pure orchestrator |

Ideal profile (low external, low internal) belongs to `ferrosa-common`, `ferrosa-sstable`, `ferrosa-worker`. They are the crates that age well.

## Dependency hotspots — recommended refactors

1. **Consider extracting `accord` from `ferrosa-cluster`.** Cluster already owns Raft, routing, repair, hinted handoff, AND Accord (strict-serializable txns). At 42k LOC this is the biggest crate in the workspace. A separate `ferrosa-accord` crate at L4 (alongside the current cluster) would shrink the cluster's public surface.
2. **`ferrosa-storage` boundary hygiene.** Seven direct dependents is the maximum fan-in we should accept for a 39k-LOC crate. Any new consumer should go through a narrower facade (e.g. routing-through-cluster), not link storage directly.
3. **`ferrosa-cql` → `ferrosa-cluster` edge.** CQL depends on cluster for coordinator routing. That is correct, but it makes CQL unable to build without the full Raft machinery. A future refactor could extract a `ferrosa-coordinator` crate at L4 that hides Raft behind a router trait.

---

## Invisible Dependencies (NOT in cargo metadata)

### `ferrosa-memory` → ferrosa-graph schema

This is the most important finding of this refresh. It is **not** in the cargo graph because `ferrosa-memory` (a separate workspace at `/Users/bkearns/src/ferrosa-memory`) does not depend on any ferrosa Rust crate. It talks to ferrosa only over CQL wire.

But it does not talk to ferrosa-graph's *Cypher executor.* It opens a raw CQL connection and writes directly to tables owned by `ferrosa-graph`:

- `agent_memory.typed_edges`
- `agent_memory.folded_into`
- `agent_memory.mentioned_in`
- `agent_memory.co_occurs_with`
- `agent_memory.supersedes`
- `agent_memory.derived_edges_*`

These tables have invariants enforced by `ferrosa-graph`'s Cypher executor (edge multiplicity, soft-delete timestamps, fold/supersede chains, index maintenance). Bypassing the executor means:

1. **The invariants are only maintained if `ferrosa-memory` reimplements them correctly.** As of 2026-04-20 there is a standing bug: `specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md`.
2. **Cargo sees zero coupling.** A breaking change to any of the above tables (renamed column, added required column, changed partition key) will compile fine, pass all ferrosa's own tests, and break `ferrosa-memory` at runtime.
3. **The schema contract has no versioning.** Regular Cassandra practice would be an intermediate view, an access-role grant, or a stored-procedure analog. None exist today.

DSM treatment: this is a **latent edge** from `ferrosa-memory` to the ferrosa-graph *schema artefact* (DDL in `ferrosa-graph/src/...` and the `agent_memory` keyspace). Treat it as if it were a hard dependency for the purposes of change management:

- Any edit to the six tables listed above must be reviewed against ferrosa-memory's write path (`ferrosa-memory/crates/ferrosa-memory-core/src/storage.rs`).
- The tracked remediation is `specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md` — once roles are enforced, the DB itself will refuse writes from non-`graph_engine` clients, turning this into a build-time-discoverable contract violation rather than a silent runtime pollution.
- A second remediation is `specs/todo/todo-extend-ferrosa-memory-graph-client-with-cypher-writes.md` — route writes through the Cypher executor via the Graph HTTP endpoint (port 7474).

Until either lands, this analysis treats `ferrosa-memory` as if it had a hard dep on `ferrosa-graph`'s L5 slot, and on the `agent_memory` keyspace DDL specifically.

### Other invisible edges worth tracking

- **`ferrosa` binary → MinIO / S3 API contract.** Compile-time sees only `aws-sdk-s3`; runtime sees a specific wire dialect (conditional PUT, CAS). MinIO compatibility issues have shown up twice (see archive).
- **`ferrosa-jepsen` → CQL wire protocol.** Jepsen is a workspace member but has zero internal deps; it only knows ferrosa via CQL. Any CQL wire change must be tested against Jepsen.
- **`ferrosa-cluster` → `openraft` snapshot format.** openraft is external but its on-disk format is our compat contract with itself across versions.

---

## Change-management implications

- **Safe to change in isolation** (low fan-in, non-hub): `ferrosa-ctl`, `ferrosa-loadgen`, `ferrosa-jepsen`, `ferrosa-worker`, `ferrosa-index-builder`. PR review can stay inside the crate.
- **Requires coordinated review** (high fan-in or high fan-out): `ferrosa-common`, `ferrosa-sstable`, `ferrosa-storage`, `ferrosa-schema`, `ferrosa-cluster`, `ferrosa-cql`. Breaking API changes need a plan for every downstream crate and a cross-workspace check for `ferrosa-memory` on the graph table subset.
- **Requires cross-repo coordination** (invisible edges): any change to the `agent_memory` keyspace DDL, the CQL wire protocol opcode set, or the openraft on-disk format.

## References

- `specs/components.md` — component inventory.
- `specs/decisions/006-auth-first-schema.md` — why ferrosa-schema sits where it does in the graph.
- `specs/decisions/008-audit-first-schema.md` — same for the audit sink surface.
- `specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md` (in the ferrosa-memory repo) — bug filing for the schema-level coupling described above.
- `specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md` — remediation.
- `specs/dsm-cluster-formation.md` — narrower DSM scoped to the formation subsystem.
