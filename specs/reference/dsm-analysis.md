# Design Structure Matrix — Ferrosa Workspace

> **Last refreshed:** 2026-10-15
> **Measured at:** `origin/main` @ `9e32e82a` (release v0.22.3)
> **Source:** `cargo metadata --format-version 1` over the workspace `Cargo.toml`.
> **Methodology:** Workspace-internal dependency graph, derived from each member's `Cargo.toml` **normal (non-dev, non-build)** deps. Dev-only edges are listed separately and excluded from the layer topology so test helpers cannot mask production cycles. `LOC (src)` is the total physical line count across every `.rs` file under a crate's `src/` (including inline `#[cfg(test)]` modules); `LOC (src, non-test)` counts only lines before the first `#[cfg(test)]` module in each file. Quoting either without its scope makes two scans non-comparable.

## Executive Summary

| Metric | Value |
|---|---|
| Workspace crates (cargo members) | 28 |
| Normal-dep edges (internal) | 111 |
| Dev-only edges (internal) | 16 |
| Cycles (normal deps) | **0** |
| Cycles (including dev deps) | **0** |
| Longest chain (leaves → root) | 11 levels (L0 → L10) |
| Highest fan-in crate | `ferrosa-common` (21 dependents) |
| Highest fan-out crate | `ferrosa` binary (16 internal deps) |
| Highest LOC crate | `ferrosa-storage` (151,109 src lines, fan-in=11) |

**Headline finding:** the workspace is a clean, acyclic DAG. No production crate has a circular dependency, and no dev-dep back-edge closes a cycle. Coupling is concentrated in the substrate crates — `ferrosa-storage`, `ferrosa-cluster`, `ferrosa-sstable`, and `ferrosa-schema` — which is expected given their role as shared infrastructure for everything above them. `ferrosa-common`, historically the leaf everyone depended on, is itself now one edge above the `ferrosa-jsonb` leaf (it carries the `CqlValue::Jsonb` variant).

**However, one critical dependency is invisible to `cargo`:** the ferrosa-memory MCP server writes directly to ferrosa-graph-owned tables (`typed_edges`, `folded_into`, `mentioned_in`, `co_occurs_with`, `supersedes`, `derived_edges_*`) via raw CQL `INSERT`s. This is a **schema-level coupling** that the crate graph cannot see; it creates a tight, version-coupled contract between two repositories with no compile-time enforcement. Details, the executor contract the writes violate, and dated live evidence are in the last section.

---

## Layer Topology (11 layers, leaves first)

Layer is defined as longest path from a leaf (no internal deps). `xtask`, `ferrosa-jepsen`, and `ferrosa-loadgen` sit on sidebars — nothing in the workspace depends on them.

```
L0  ferrosa-jsonb       ferrosa-sched        xtask
L1  ferrosa-common
L2  ferrosa-index       ferrosa-net         ferrosa-sim        ferrosa-sstable     ferrosa-udf
L3  ferrosa-cdc         ferrosa-jepsen      ferrosa-schema     ferrosa-worker
L4  ferrosa-row-bridge  ferrosa-view
L5  ferrosa-storage
L6  ferrosa-cluster     ferrosa-index-builder   ferrosa-sql
L7  ferrosa-graph       ferrosa-postgres    ferrosa-session    ferrosa-sparql
L8  ferrosa-cql
L9  ferrosa-ctl         ferrosa-flight      ferrosa-loadgen
L10 ferrosa (binary)
```

Notes:

- `ferrosa-net` is now L2, not a leaf: it depends on `ferrosa-common`. It exports the internode protocol/TLS/lane actor primitives, and `ferrosa-cluster`/`ferrosa` consume them.
- `ferrosa-common` is L1 because of its single dependency on the `ferrosa-jsonb` leaf — a real change from the earlier "leaf of shared types" reading.
- `ferrosa-jepsen` has one internal edge now (`ferrosa-jepsen` → `ferrosa-sim`) rather than zero; it still talks to ferrosa mainly through the CQL wire protocol.
- `ferrosa-sql` and `ferrosa-index-builder` sit at L6, alongside `ferrosa-cluster`; both are consumers of storage without anything depending back on them except the surfaces that use them.
- The storage/schema/sstable layering resolves a historical ambiguity: `ferrosa-schema` depends on `ferrosa-sstable` and `ferrosa-index` (for BTI writing of system tables and index metadata), and `ferrosa-storage` then depends on all three.

## Dev-only back edges

```
ferrosa           (dev) -> [ferrosa-sstable, ferrosa-storage]
ferrosa-cluster   (dev) -> [ferrosa-storage]
ferrosa-cql       (dev) -> [ferrosa-cql, ferrosa-jsonb]
ferrosa-ctl       (dev) -> [ferrosa-schema, ferrosa-session, ferrosa-storage, ferrosa-udf]
ferrosa-flight    (dev) -> [ferrosa-cql]
ferrosa-postgres  (dev) -> [ferrosa-sched]
ferrosa-sstable   (dev) -> [ferrosa-common]
ferrosa-storage   (dev) -> [ferrosa-common, ferrosa-jsonb, ferrosa-sstable, ferrosa-storage]
```

These edges exist so integration tests can spin up higher-layer machinery without the production binary linking all of it. None closes a cycle.

## Cycle Check

Ran Tarjan-style DFS over the normal-deps adjacency list: zero SCCs of size > 1. Re-ran with dev-deps merged in; still zero. **All 28 crates are in singleton SCCs.**

This is stronger than Cassandra's JVM modularisation (which tolerates several cycles between `db`, `gms`, `net`, and `service`) and gives us incremental rebuild + reviewable module boundaries for free.

---

## Coupling — fan-in / fan-out / LOC

| Crate | Fan-in | Fan-out | External deps | LOC (src) | LOC (src, non-test) | Role |
|---|---:|---:|---:|---:|---:|---|
| `ferrosa-common` | **21** | 1 | 6 | 7,694 | 4,258 | Shared types — Token, PartitionKey, DecoratedKey, CellValue, Accord HLC. Highest fan-in; small LOC means churn blast-radius is bounded. |
| `ferrosa-sstable` | **14** | 1 | 10 | 29,693 | 11,402 | SSTable read/write. High fan-in is expected — every layer above storage reads SSTables. |
| `ferrosa-schema` | **12** | 3 | 13 | 16,189 | 7,547 | Schema registry + auth + audit. Metadata authority for the workspace. |
| `ferrosa-storage` | **11** | 7 | 25 | 151,109 | 48,958 | Memtable + commit log + compaction + S3 write-behind. Largest crate and the single biggest coupling risk. |
| `ferrosa-cluster` | **8** | 7 | 19 | 125,584 | 51,028 | Raft + routing + repair + hinted handoff + Accord. Co-equal top fan-out. |
| `ferrosa-net` | **8** | 1 | 20 | 19,003 | 10,615 | Custom internode protocol, TLS, lane actors. Only internal dep is `ferrosa-common`. |
| `ferrosa-index` | 7 | 1 | 6 | 14,197 | 8,984 | Index data structures (BTree, Hash, HNSW, Phonetic, FullText). |
| `ferrosa-jsonb` | 5 | 0 | 4 | 4,721 | 4,048 | JSONB value representation. Leaf crate, now a dependency of `ferrosa-common`. |
| `ferrosa-cdc` | 4 | 2 | 1 | 285 | 179 | Bounded change-data-capture bus. |
| `ferrosa-cql` | 4 | 12 | 31 | 78,610 | 21,240 | CQL protocol v5 + query execution. Highest fan-out below the binary. |
| `ferrosa-row-bridge` | 3 | 4 | 3 | 3,332 | 2,285 | Canonical CQL row codec shared by the CQL and Postgres front-ends (decision D10). |
| `ferrosa-sched` | 3 | 0 | 1 | 3,356 | 2,012 | Shared scheduler. |
| `ferrosa-udf` | 3 | 1 | 11 | 4,546 | 3,221 | Wasmtime UDF compilation. |
| `ferrosa-session` | 2 | 6 | 1 | 451 | 179 | Connection/session state extracted for reuse across front-ends. |
| `ferrosa-flight` | 1 | 5 | 11 | 2,940 | 2,307 | Apache Arrow Flight gRPC endpoint. |
| `ferrosa-graph` | 1 | 6 | 23 | 32,769 | 21,280 | Property graph, Cypher, Bolt, adjacency index, SUBSCRIBE. Consumed only by the `ferrosa` binary. |
| `ferrosa-postgres` | 1 | 9 | 19 | 28,687 | 14,977 | PostgreSQL wire front-end (decision D3/D10). |
| `ferrosa-sim` | 1 | 1 | 2 | 3,183 | 2,369 | Deterministic simulation harness. |
| `ferrosa-sparql` | 1 | 7 | 14 | 5,872 | 4,423 | SPARQL 1.1 endpoint. |
| `ferrosa-sql` | 1 | 2 | 7 | 11,263 | 6,987 | Bespoke relational query engine backing the Postgres front-end (decision D3). |
| `ferrosa` | 0 | 16 | 26 | 21,630 | 10,775 | Main binary — orchestrates everything. |
| `ferrosa-ctl` | 0 | 5 | 16 | 8,555 | 4,963 | CLI + TUI. |
| `ferrosa-index-builder` | 0 | 4 | 10 | 1,254 | 878 | Standalone remote index builder binary. |
| `ferrosa-jepsen` | 0 | 1 | 16 | 15,032 | 8,781 | External Jepsen harness (talks CQL wire only). |
| `ferrosa-loadgen` | 0 | 5 | 12 | 5,643 | 3,731 | Load testing binary. |
| `ferrosa-view` | 0 | 1 | 2 | 988 | 545 | Conflict-free materialized-view primitives (integration in progress). |
| `ferrosa-worker` | 0 | 3 | 5 | 159 | 101 | Background-task harness. Tiny — essentially a crate-scoped glue module. |
| `xtask` | 0 | 0 | 7 | 4,551 | 2,929 | Build/dev tooling. |

## Top-coupling crates — risk commentary

1. **`ferrosa-storage` (fan-in=11, 151k src lines, 25 external deps).** This is the highest blast-radius crate. Any breaking API change forces coordinated edits across all eleven dependents — `ferrosa-cluster`, `ferrosa-cql`, `ferrosa-graph`, `ferrosa-sparql`, `ferrosa-loadgen`, `ferrosa-index-builder`, `ferrosa-worker`, `ferrosa-ctl`, `ferrosa-postgres`, `ferrosa-sql`, and `ferrosa`. Mitigations already in place: the write-behind boundary is async-trait'd and the SSTable reader/writer is versioned. Recommendation: treat the storage public API as a semver-committed surface in internal review.
2. **`ferrosa-cql` (fan-out=12, 79k src lines).** The CQL layer pulls storage + cluster + schema + sstable + index + udf + common + cdc + net + row-bridge + sched + session. That is the entire application tier. `ferrosa-cql` is the natural integration hub and also where most bugs land because of that density.
3. **`ferrosa-cluster` (fan-out=7, fan-in=8, 126k src lines).** Second-largest crate by LOC. Raft + routing + repair + hinted handoff + Accord in one crate is itself a coupling hazard — splitting Accord into its own crate has been discussed and would break this tie.
4. **`ferrosa-schema` (fan-in=12).** Auth, audit, DDL, and system tables all live here per ADR-006 and ADR-008. Schema API is deliberately stable; churn is low.

## Interface vs implementation ratio (proxy)

Cargo metadata does not expose the pub/non-pub ratio directly, so we use **external-deps-per-internal-dep** as a rough proxy: a crate with many external deps and few internal ones is typically an "edge" crate (wire protocol, UI), while the inverse is a "hub" crate with more internal orchestration than external I/O.

| Crate | External / Internal | Classification |
|---|---:|---|
| `ferrosa-net` | 20 / 1 | Pure edge / wire |
| `ferrosa-storage` | 25 / 7 | Edge-heavy (S3, tokio, compression) |
| `ferrosa-cluster` | 19 / 7 | Balanced — both hub and edge |
| `ferrosa-cql` | 31 / 12 | Balanced hub (parser deps + driver compat) |
| `ferrosa-graph` | 23 / 6 | Edge-heavy (Bolt, HTTP, Cypher, TLS) |
| `ferrosa-jsonb` | 4 / 0 | Pure value crate (ideal) |
| `ferrosa-common` | 6 / 1 | Near-pure domain types |
| `ferrosa-sstable` | 10 / 1 | Pure core format (ideal) |
| `ferrosa-schema` | 13 / 3 | Balanced |
| `ferrosa-udf` | 11 / 1 | Wasmtime wrapper (edge) |
| `ferrosa-worker` | 5 / 3 | Pure orchestrator |

Ideal profile (low external, low internal) belongs to `ferrosa-jsonb`, `ferrosa-common`, `ferrosa-sstable`, and `ferrosa-worker`. They are the crates that age well.

## Dependency hotspots — recommended refactors

1. **Consider extracting `accord` from `ferrosa-cluster`.** Cluster already owns Raft, routing, repair, hinted handoff, AND Accord (strict-serializable txns). At 126k src lines it is among the biggest crates in the workspace. A separate `ferrosa-accord` crate would shrink the cluster's public surface.
2. **`ferrosa-storage` boundary hygiene.** Eleven direct dependents is a high fan-in for a 151k-line crate. Any new consumer should go through a narrower facade (e.g. routing-through-cluster), not link storage directly.
3. **`ferrosa-cql` → `ferrosa-cluster` edge.** CQL depends on cluster for coordinator routing. That is correct, but it makes CQL unable to build without the full Raft machinery. A future refactor could extract a `ferrosa-coordinator` crate that hides Raft behind a router trait.

---

## Invisible Dependencies (NOT in cargo metadata)

### `ferrosa-memory` → ferrosa-graph schema

This is the most important finding of this analysis. It is **not** in the cargo graph because `ferrosa-memory` (a separate workspace in a sibling repository) does not depend on any ferrosa Rust crate. It talks to ferrosa only over the CQL wire.

But it does not talk to ferrosa-graph's *Cypher executor*. It opens a raw CQL connection and writes directly to tables owned by `ferrosa-graph`:

- `agent_memory.typed_edges`
- `agent_memory.folded_into`
- `agent_memory.mentioned_in`
- `agent_memory.co_occurs_with`
- `agent_memory.supersedes`
- `agent_memory.derived_edges_*`

These tables have invariants enforced by `ferrosa-graph`'s Cypher executor (edge multiplicity, soft-delete timestamps, fold/supersede chains, index maintenance). Bypassing the executor means:

1. **The invariants are only maintained if `ferrosa-memory` reimplements them correctly.** The standing bug filing lives in the ferrosa-memory repository (`specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md`).
2. **Cargo sees zero coupling.** A breaking change to any of the above tables (renamed column, added required column, changed partition key) will compile fine, pass all of ferrosa's own tests, and break `ferrosa-memory` at runtime.
3. **The schema contract has no versioning.** Regular Cassandra practice would be an intermediate view, an access-role grant, or a stored-procedure analog. None exist today.

#### Contract the coupling violates — a MERGE must set all of a table's key columns, or none

The graph engine does not merely *invite* writes through Cypher — it enforces a key-resolution contract on them. `schema_merge_requires_hidden_key_resolution` (`ferrosa-graph/src/executor/expand.rs:4956`, called from the MERGE execution path at `:4059`) decides whether a Cypher `MERGE` supplies *some but not all* of its backing table's key columns (partition key ∪ clustering key). It returns true exactly when at least one key column is set and at least one is not; when it does, the write is rejected before it reaches storage:

```text
MERGE on '<keyspace>.<table>' is missing required scoped key columns;
match existing scoped vertices or set the missing key properties explicitly
```

(`ferrosa-graph/src/executor/expand.rs:4061-4064`, returned as `GraphError::Validation`, naming the table.)

So the contract is:

- setting **every** key column is required;
- setting **none** is allowed — the executor derives the full key shape from the pattern;
- the **partial** case errors with a message naming the table.

This is the executor half of the "graph engine owns its backing tables" invariant. A Cypher write cannot silently key a row on half a primary key. But the guard lives *inside the Cypher executor* — it is not a schema constraint and not a storage-engine constraint. A raw-CQL writer that reimplements the merge by hand is outside it, which is exactly how the coupling described above does its damage.

#### Observed consequence (2026-10-15, operator's local development cluster)

Three of the six graph-owned edge families whose producing `MERGE` omitted key columns held **zero rows** — `mentioned_in` = 0, `folded_into` = 0, `supersedes` = 0 — and `temporal_events` = 0. The families whose producing write set **every** key column were populated: `co_occurs_with` = 199,039 rows and `typed_edges` = 41,021 rows. The producing writes were rejected with `missing required scoped key columns`: **106** such failures were logged before the fix and **0** after.

The contrast is the point. The empty families are not an ingestion gap — an identical write path with a complete key landed 240,060 rows across its two siblings. A half-keyed raw-CQL write is not refused by `cargo`, by the schema, or by the storage engine; it lands as a valid-looking row under a key the reader never queries, so the failure surfaces as a silently empty table rather than a compile or DDL error. This is the schema-level coupling made concrete: the same contract the Cypher executor enforces is, for the memory server, only an unenforced convention.

**Open question (not a design position):** the storage engine's server-side aggregate `SELECT count(*) ... ALLOW FILTERING` was observed to over-report against its own rows — after deleting a known row, a primary-key read and a full scan both returned empty while `count(*)` returned 1, reproducibly and on all nodes. The memory server's statistics fields use that query shape, so a reported statistic can be wrong in either direction. The aggregate's counting rule relative to tombstones and `ALLOW FILTERING` is not established here; resolving it requires a target test against the live cluster, so it is recorded as an open question rather than asserted as a defect.

DSM treatment: this is a **latent edge** from `ferrosa-memory` to the ferrosa-graph *schema artefact* (DDL in `ferrosa-graph/src/...` and the `agent_memory` keyspace). Treat it as if it were a hard dependency for the purposes of change management:

- Any edit to the six tables listed above must be reviewed against ferrosa-memory's write path (`ferrosa-memory/crates/ferrosa-memory-core/src/storage.rs`).
- The tracked remediation for DB-level enforcement is `specs/implemented/todo-enable-cql-role-auth-for-graph-table-isolation.md` (moved from `specs/todo/` to `specs/implemented/`) — once roles are enforced, the DB itself will refuse writes from non-`graph_engine` clients, turning this into a build-time-discoverable contract violation rather than a silent runtime pollution.
- A second remediation — route writes through the Cypher executor via the Graph HTTP endpoint (port 7474) — is tracked as `specs/implemented/todo-implement-public-cypher-mutations-for-client-graph-writes.md` (implemented, awaiting verification); the earlier `specs/todo/todo-extend-ferrosa-memory-graph-client-with-cypher-writes.md` is no longer present under `specs/todo/`.

Until either lands, this analysis treats `ferrosa-memory` as if it had a hard dep on `ferrosa-graph`'s L7 slot, and on the `agent_memory` keyspace DDL specifically.

### Other invisible edges worth tracking

- **`ferrosa` binary → MinIO / S3 API contract.** Compile-time sees only `aws-sdk-s3`; runtime sees a specific wire dialect (conditional PUT, CAS). MinIO compatibility issues have shown up twice (see archive).
- **`ferrosa-jepsen` → CQL wire protocol.** Jepsen is a workspace member whose only internal dep is `ferrosa-sim`; it knows ferrosa mainly via CQL. Any CQL wire change must be tested against Jepsen.
- **`ferrosa-cluster` → `openraft` snapshot format.** openraft is external but its on-disk format is our compat contract with itself across versions.

---

## Change-management implications

- **Safe to change in isolation** (low fan-in, non-hub): `ferrosa-ctl`, `ferrosa-loadgen`, `ferrosa-jepsen`, `ferrosa-worker`, `ferrosa-index-builder`, `ferrosa-view`, `ferrosa-sim`, `xtask`. PR review can stay inside the crate.
- **Requires coordinated review** (high fan-in or high fan-out): `ferrosa-common`, `ferrosa-sstable`, `ferrosa-storage`, `ferrosa-schema`, `ferrosa-cluster`, `ferrosa-net`, `ferrosa-cql`. Breaking API changes need a plan for every downstream crate and a cross-repo check for `ferrosa-memory` on the graph table subset.
- **Requires cross-repo coordination** (invisible edges): any change to the `agent_memory` keyspace DDL, the CQL wire protocol opcode set, or the openraft on-disk format.

## References

- `specs/reference/components.md` — component inventory.
- `specs/decisions/006-auth-first-schema.md` — why ferrosa-schema sits where it does in the graph.
- `specs/decisions/008-audit-first-schema.md` — same for the audit sink surface.
- `specs/todo/bug-ferrosa-memory-bypasses-graph-api-for-writes.md` (in the ferrosa-memory repository) — bug filing for the schema-level coupling described above.
- `specs/implemented/todo-enable-cql-role-auth-for-graph-table-isolation.md` — remediation.
- `specs/implemented/todo-implement-public-cypher-mutations-for-client-graph-writes.md` — remediation (public Cypher write path).
- `specs/reference/dsm-cluster-formation.md` — narrower DSM scoped to the formation subsystem.
