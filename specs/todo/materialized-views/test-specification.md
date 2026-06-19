---
title: Materialized Views — Test Specification (7-layer)
status: draft
created: 2026-06-19
work_item: materialized-views
branch: feature/materialized-views
executive_summary: >
  Seven-layer test specification for the ferrosa materialized-view feature,
  designed before implementation so the methodology is fixed and traceable. Layers
  run unit → property → contract → integration → system → fault-injection →
  performance, each targeting specific DSM elements (E1–E14) and acceptance gates
  (G1–G7). The pure core (compute_view_delta E5, validate E4) carries the heaviest
  unit + property load because it is high-fan-out and infra-free; the Accord
  commit (E9) carries the heaviest fault-injection load because it is the sole
  high-risk integration point. Every Phase-0 decision (D1–D4) and gate maps to at
  least one named test via the traceability matrix, and the four highest-risk
  failure modes (nondeterministic UDF, transient view orphan on view-PK change,
  fast-path regression, driver-shape regression) each get a dedicated adversarial
  test. The spec also fixes the live-infra policy: cluster/Accord tests are behind
  the live-infra-tests feature and panic (not skip) when their env prerequisite is
  absent.
---

# Materialized Views — Test Specification

> Consumes [decisions.md](decisions.md) (D1–D4, gates G1–G7),
> [architecture.md](architecture.md) (state machine, phasing), and
> [dsm-proposed.md](dsm-proposed.md) (elements E1–E14, seams). The concrete file
> manifest is in [test-gen-plan.md](test-gen-plan.md).

## 1. Principles

- **Test-first.** Per repo policy (`/tdd`), each element lands red-first. This
  spec defines the red tests before code exists.
- **Push correctness down.** The pure core (E4 `validate`, E5 `compute-delta`) is
  the cheapest, fastest place to prove correctness — it gets exhaustive unit +
  property coverage with zero infra. Higher layers verify wiring, not logic.
- **Repo test policy is binding.** No `#[ignore]`, no silent `return` in a test
  body. Live-infra tests are behind the `live-infra-tests` feature and **panic**
  with setup instructions when the env prerequisite is missing (never pass).
- **Adversarial where it counts.** The four headline risks
  ([architecture.md](architecture.md) §13) each get a test that tries to *break*
  the guarantee, not just confirm the happy path.

## 2. The seven layers

### L1 — Unit (pure logic, no infra)

Target: E1, E4, E5. Fast, deterministic, run on every `cargo test`.

- **E4 validate:** every §4.2 baseline rule rejects its violation and accepts its
  satisfying case — view PK ⊇ base PK; ≤1 extra non-PK PK column; `IS NOT NULL`
  required on view-PK cols; aggregates/static/counters rejected; chained-MV
  rejected. Every §4.3 extension rule — predicate WHERE compiles; UDF computed
  column accepted only if UDF is deterministic, **rejected otherwise (G2)**;
  computed column rejected from view PK.
- **E5 compute-delta:** every row of the §6.3 state-machine table is a test —
  INSERT in/out of predicate; UPDATE non-PK col; **UPDATE view-PK col → exactly
  one delete(old)+one insert(new) (G4)**; predicate flip in/out (G6); DELETE;
  TTL/tombstone expiry → timestamped view delete. Timestamps on derived view
  cells equal the base mutation timestamp.
- **E1 metadata:** `ViewKind` serializes with `Incremental` and round-trips; the
  serialized form reserves the discriminator so adding `Snapshot` is non-breaking
  (G1 — a golden-bytes/round-trip test against a pinned encoding).

### L2 — Property-based (proptest)

Target: E5 (and E4). Generative invariants over random base mutation sequences.

- **View-as-projection invariant:** for any sequence of base mutations applied to
  a model base table, the set of view rows produced by folding `compute_view_delta`
  equals the view rows computed by re-projecting the final base state from
  scratch. (Catches missed deletes on view-PK change / predicate flip.)
- **No-orphan invariant:** at no prefix of the mutation sequence does a view row
  exist whose source base row does not satisfy the predicate / is absent.
- **Idempotent replay:** replaying the same mutation (same timestamp) is a no-op
  on the view (commit-log replay safety).
- **Determinism:** `compute_view_delta(view, prior, next)` is a pure function —
  same inputs, same output, across runs (underpins G2/D2).

### L3 — Contract (protocol & schema shape)

Target: E11, E12, E6. Verifies external contracts, not internal logic.

- **DDL contract:** `CREATE/ALTER/DROP MATERIALIZED VIEW` parse to the expected
  AST and round-trip through `ViewMetadata`; rejection messages are stable.
- **`system_schema.views` shape (G7):** the served column shape loads in the
  **actual** `ferrosadb/scylla-rust-driver` fork *and* the Python
  `cassandra-driver` — the existing P2-bug repro
  (`specs/todo/bug-system-schema-views-column-shape-breaks-scylla-driver.md`)
  passes. This is a **driver-in-the-loop** contract test, not an assertion on our
  own encoder. Also: repeated DDL triggers schema-agreement metadata fetch without
  error.
- **Schema replication contract:** a `ViewMetadata` survives a Raft DDL
  round-trip and reloads identically on a fresh replica.

### L4 — Integration (cross-crate seams)

Target: E7, E8, E10, E6 — the §5 DSM seams. Single process, real engine, no
cluster.

- **Lifecycle:** CREATE MV builds a view `TableStore`; DROP removes it; ALTER
  applies options.
- **Observer wiring (E8→E5):** a base write through the storage engine causes the
  observer to emit exactly the mutations `compute_view_delta` specifies (assert on
  the derived mutations, not just final state).
- **View indexing (E10→E7, D3):** create a 2i and a vector/FTS index on a view;
  base writes propagate to view rows (immediately) and to the view's FTS/vector
  sidecars (after flush). A test asserts the **two-tier freshness**: view row
  visible immediately, view FTS hit only after flush (G5).
- **`system_schema.views` populated:** after CREATE MV, the table returns the row
  (today it returns `&[]`).

### L5 — System (end-to-end, single node)

Target: E14 + whole stack via CQL client. `live-infra-tests` not required (local
engine).

- **Read-your-write on a view:** INSERT base, `SELECT … FROM mv` returns the
  re-partitioned row; query by the view's PK routes correctly.
- **Full operation set:** create base + MV, exercise INSERT/UPDATE/DELETE/TTL on
  base, assert view reflects each; DROP base behavior; query view via its own 2i.
- **Extension paths (D4):** a filtered view (predicate WHERE) and a UDF-computed
  -column view behave end-to-end; predicate-flip UPDATE moves a row in/out (G6).

### L6 — Fault-injection / Jepsen (Accord atomicity, crashes)

Target: E9. Behind `live-infra-tests`; `FERROSA_TEST_CLUSTER_NODES` /
`FERROSA_TEST_FIRECRACKER` per repo policy (panic if absent).

- **Atomicity (G4):** crash/abort injected between base apply and view delta —
  assert no serial point observes base-updated-but-view-stale (or vice versa).
  Under Accord (D2) there must be **no divergence** at any consistency point.
- **View-PK change atomicity:** induce the delete-old+insert-new pair under
  concurrent readers; assert no transient state shows both old and new view rows
  or neither.
- **Concurrent base writers:** racing writes to the same base row resolve to the
  same winner in base and view (timestamp reconciliation).
- **Replica failure during maintenance:** kill a replica mid-maintenance; on
  recovery base and view converge (seeds the deferred view-repair epic
  `t_f00fdaf7`).
- **Jepsen check:** a strict-serializability checker over interleaved base+view
  operations finds no violation.

### L7 — Performance / load

Target: E9 fast path, E8/E9 cost. `ferrosa-loadgen`.

- **Fast-path regression (G3 — gating):** write throughput/latency to a base
  table with **no** views must match the pre-MV baseline within noise. Asserts no
  Accord transaction is created on the no-view path. This is a **release-blocking
  benchmark**, not advisory.
- **Read-before-write cost:** measure added latency on a viewed base table under
  UPDATE/DELETE load; record as a baseline, alert on regression.
- **Two-stage async lag:** measure view-row freshness (immediate) vs view
  FTS/vector freshness (post-flush) under load; expose as metrics (G5).

## 3. Traceability matrix

Every decision and gate maps to ≥1 layer/test. (Test names are the planned
red-first stubs in [test-gen-plan.md](test-gen-plan.md).)

| Req | Statement | Layer(s) | Element |
|-----|-----------|----------|---------|
| D1 / G1 | view_kind reserved, non-breaking | L1 (round-trip), L3 (schema repl) | E1, E6 |
| D2 | Accord strict, no divergence | L6 (atomicity, jepsen) | E9 |
| D2 / G3 | zero-cost fast path, no-view tables | L7 (fast-path bench) | E9, E8 |
| D2 / G4 | view-PK change = atomic del+ins, no orphan | L1, L2 (no-orphan), L6 | E5, E9 |
| D3 | full indexing of views | L4 (view-index) | E10, E7 |
| D3 / G5 | two-tier consistency documented + observable | L4, L7 (lag metric) | E10 |
| D4 | richer WHERE + UDF columns | L1, L5 (extension paths) | E4, E5 |
| D4 / G2 | nondeterministic UDF rejected + restricted runtime | L1 (reject), L2 (determinism), L4 (runtime sandbox) | E4, E3 |
| D4 / G6 | predicate-flip UPDATE in/out | L1, L5 | E5 |
| §9 / G7 | system_schema.views shape loads in real driver | L3 (driver-in-loop) | E11 |
| §6.3 | full maintenance state machine | L1, L2 | E5 |
| Ops | CRUD + read-your-write on view | L4, L5 | E7, E14 |

## 4. Adversarial tests for the four headline risks

| Risk ([arch §13]) | Adversarial test | Layer |
|-------------------|------------------|-------|
| Nondeterministic UDF corrupts view | UDF that reads clock/RNG → rejected at DDL; if forced, restricted runtime denies the host call | L1, L4 |
| Transient view orphan on view-PK change | concurrent reader during del-old+ins-new sees neither inconsistent state | L6 |
| Fast-path regression (Accord on no-view writes) | bench fails build if no-view write latency regresses | L7 |
| Driver-shape regression (false-premise repeat) | real scylla-rust-driver fork must decode `system_schema.views` in CI | L3 |

## 5. Coverage gating

- **Unit + property (L1, L2)** run on every PR; required green. Target: 100% of
  the §6.3 state-machine table and every §4 validation rule has a case.
- **Contract + integration (L3, L4)** run on every PR (single-process, no infra
  except the driver-in-loop contract which uses the bundled fork).
- **Fault-injection + perf (L6, L7)** run in the nightly `live-infra-tests` job;
  G3 (fast-path) and G7 (driver shape) are **release-blocking**.
- A gate is "met" only when its mapped test exists and is green — no gate is
  closed on inspection alone.
