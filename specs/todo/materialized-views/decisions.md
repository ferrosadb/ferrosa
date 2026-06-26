---
title: Materialized Views — Decision Record (Phase 0)
status: draft
created: 2026-06-19
work_item: materialized-views
branch: feature/materialized-views
executive_summary: >
  Phase 0 grill-me decisions for the ferrosa Materialized Views feature. Four
  foundational decisions were locked: (1) a single incremental-MV engine with a
  view_kind discriminator so Postgres snapshot/REFRESH views can be added later
  without rework; (2) Accord strict-serializable base+view maintenance (stronger
  than Cassandra, no divergence) rather than Cassandra's eventually-consistent
  batchlog model; (3) full indexing of views — native 2i, FTS, and vector
  HNSW/IVFFlat indexes are all permitted on a view's own store; (4) ferrosa
  extensions to the view query are allowed now (richer WHERE predicates via the
  Filtered-index predicate evaluator, plus UDF-computed columns). The Accord and
  UDF decisions carry the two highest-risk consequences — coupling to
  ferrosa-cluster/Accord and a UDF determinism contract — and become explicit
  acceptance gates in the architecture spec.
---

# Materialized Views — Decision Record (Phase 0)

This record captures the locked decisions from the Phase 0 interrogation. Each
decision feeds forward into the architecture spec, the DSM coupling analysis,
and the work-item backlog. Decisions are deliberately *capability-maximizing* —
the chosen combination produces a materialized-view feature that is a strict
superset of Cassandra's, at the cost of more design surface that the
architecture spec must pin down.

## Ground truth at decision time

Established by codebase reconnaissance before grilling (file:line evidence in
[architecture.md](architecture.md)):

- **No MV implementation exists.** Parser rejects `CREATE/ALTER/DROP MATERIALIZED
  VIEW` (`ferrosa-cql/src/parser.rs:714`). No `ViewMetadata`; `SchemaSnapshot`
  (`ferrosa-schema/src/registry.rs:38`) has no `views` field.
- **`system_schema.views` returns zero rows** with the 10-column Cassandra-5.0
  shape (`ferrosa-cql/src/router.rs:2554`) — the shape that trips the stale
  scylla-rust-driver (P2 bug on file). Nothing populates it today.
- **A maintenance seam already exists.** `WriteObserver`
  (`ferrosa-storage/src/observer.rs`) supports `Sync`/`Async` modes; the engine
  dispatches after commit-log + memtable write (`engine.rs:5139`) and a sync
  observer can emit derived mutations that get their own commit-log + memtable
  write (`engine.rs:4709`).
- **Indexes are per-table and already layered:** inline 2i before memtable put
  (`store.rs:1208`); FTS and Vector HNSW/IVFFlat are flush-built sidecars
  (`store.rs:2237` / `store.rs:2274`) — already eventually-consistent.
- **The coordinator fans out base-table mutations only** (`write.rs:363`); view
  deltas must be computed at the replica, not the coordinator.

## Decisions

### D1 — One incremental engine, `view_kind` discriminator

**Decision:** Build the Cassandra-style incremental materialized-view engine now.
Add a `view_kind` field to `ViewMetadata` and the DDL layer
(`INCREMENTAL` | reserved `SNAPSHOT`) so a Postgres snapshot/`REFRESH MATERIALIZED
VIEW` engine can be added later with no rework to the storage observer or schema
replication. The storage maintenance path stays protocol-agnostic; the CQL and
Postgres frontends are thin DDL/DML translators on top.

**Rationale:** Cassandra MVs (incremental denormalization, strict PK rules,
auto-maintained) and Postgres MVs (arbitrary SELECT snapshot, manual `REFRESH`)
are different features that share a name. The Postgres frontend
(`feature/postgres-frontend`, a sibling worktree) will eventually need the
snapshot semantics. Committing to one engine now while reserving the
discriminator avoids both over-building and a future schema-migration.

**Consequences:** `ViewMetadata.view_kind` must be present in the very first
schema-replicated representation, or adding `SNAPSHOT` later is a breaking schema
change. → acceptance gate in architecture spec. DSM must keep both frontends off
the storage maintenance path. → [dsm-coupling.md](dsm-coupling.md).

**Alternatives rejected:** Cassandra-only (risks Postgres rework); build both
engines now (largest scope, premature — no Postgres MV requirement is concrete
yet).

### D2 — Accord strict-serializable base+view maintenance

**Decision:** Maintain base and view atomically using Accord strict-serializable
transactions. A base write and its computed view delta(s) commit in one Accord
transaction. Base and view **cannot diverge**. This is intentionally *stronger*
than Cassandra, which uses replica-local read-before-write plus an async
view-specific batchlog and tolerates documented divergence (and does not repair
views during base repair).

**Rationale:** Correctness over bug-for-bug parity. Cassandra MV divergence is a
well-known operational wart; ferrosa already has Accord for strict-serializable
transactions, so the stronger guarantee is reachable. The user explicitly chose
correctness over the Cassandra consistency contract.

**Consequences (high-risk — these are the headline design constraints):**

- **Coupling:** MV maintenance now depends on `ferrosa-cluster`'s Accord, not
  just the storage `WriteObserver`. The DSM must keep this dependency strictly
  one-directional (view engine → Accord) and must not let either frontend reach
  into Accord. → [dsm-coupling.md](dsm-coupling.md).
- **Write cost/latency:** Every base write that feeds a view becomes (or joins)
  an Accord transaction. Base tables with no views must pay **zero** Accord cost
  — the fast path must be preserved. → acceptance gate.
- **Read-before-write:** Computing a correct view delta on UPDATE/DELETE requires
  the prior base row (to issue the matching view-row delete when a view-PK column
  changes). This read must participate in the same Accord transaction.
- **Interaction with D4:** UDF-computed view columns must be evaluated
  deterministically inside the Accord transaction (see D4).

**Alternatives rejected:** Mirror Cassandra (eventual) — weaker guarantee,
documented divergence; Local-sync-first — a build-order option, not a target
model, folded into the architecture spec's phasing instead.

### D3 — Full indexing of materialized views

**Decision:** A materialized view is a real, fully-indexable table. Permit
`CREATE INDEX` (native 2i) **and** `CREATE CUSTOM INDEX` (FullText, Vector
HNSW/IVFFlat) on a view, reusing the existing per-table index machinery on the
view's own store. The view becomes a re-partitioned, independently-indexable
projection of the base.

**Rationale:** Views are already backed by a normal `TableStore`, so 2i/FTS/vector
maintenance composes for free at the storage layer. This is the feature's biggest
differentiator over Cassandra: a view can re-partition data *and* expose a vector
or full-text index over that re-partitioning.

**Consequences:** Two-stage asynchrony must be documented and tested: a base
write → (Accord, synchronous) view-row mutation → (flush-built, asynchronous) the
view's FTS/vector sidecars. The view's *rows* are strictly consistent with the
base (D2); the view's *FTS/vector sidecars* inherit the existing flush-build
eventual-consistency of those index types. This split-consistency story must be
explicit in docs. → acceptance gate.

**Alternatives rejected:** Cassandra-parity (native 2i only); no indexes on
views — both leave the differentiating capability on the table.

### D4 — Ferrosa extensions to the view query allowed now

**Decision:** Beyond strict Cassandra MV rules, allow now: (a) richer `WHERE`
predicates in the view definition, reusing the Filtered-index predicate evaluator
(`ferrosa_index::evaluate_predicate_row`); and (b) UDF-computed view columns.
Strict Cassandra rules remain the *default/validated baseline*; extensions are
opt-in grammar.

**Rationale:** ferrosa already has the predicate evaluator (used by Filtered
indexes) and Wasmtime UDFs. Exposing them in view definitions yields filtered and
derived-column views that Cassandra cannot express.

**Consequences (high-risk):**

- **UDF determinism contract (hard gate):** Under D2 (Accord strict
  serializability), a UDF used in a view definition MUST be deterministic — no
  wall-clock, no RNG, no external I/O, no nondeterministic floating-point
  reductions. A nondeterministic UDF makes the view delta unreproducible and
  silently breaks the serializability guarantee. The DDL path must **reject**
  UDFs not marked/proven deterministic for use in a view. → acceptance gate +
  FMEA failure mode.
- **Predicate re-evaluation on update:** A richer `WHERE` means an UPDATE can move
  a base row *into* or *out of* the view (predicate flip). The maintenance path
  must detect predicate-membership transitions and emit insert/delete view
  mutations accordingly — same machinery Filtered indexes already need, but now
  in the Accord transaction.

**Alternatives rejected:** Strict parity; parity + reserved grammar — both defer
capability the user wants now.

## Decision → downstream mapping

| Decision | Architecture impact | DSM impact | Backlog/risk |
|----------|--------------------|------------|--------------|
| D1 view_kind | `ViewMetadata.view_kind` in first schema rev | Frontends are translators; engine protocol-agnostic | Postgres SNAPSHOT engine = deferred epic |
| D2 Accord | View delta computed in Accord txn; zero-view fast path | view-engine → Accord, one-directional | UDF determinism; read-before-write cost |
| D3 full index | View = indexable `TableStore`; split consistency | Reuses storage index machinery, no new coupling | Two-stage async test matrix |
| D4 extensions | Predicate + UDF columns in view def | Reuses Filtered evaluator + UDF executor | UDF determinism gate; predicate-flip maintenance |

## Open questions deferred to backlog

- Postgres `SNAPSHOT`/`REFRESH` MV engine — separate epic, gated on a concrete
  postgres-frontend requirement (D1 reserves the slot).
- `system_schema.views` population + the scylla-rust-driver shape bug — must be
  resolved as part of this feature since real MVs will finally populate that
  table. Tracked as a work item.
- Anti-entropy / repair for view rows under Accord (Cassandra cannot do this;
  ferrosa can — scope it once base maintenance lands).
