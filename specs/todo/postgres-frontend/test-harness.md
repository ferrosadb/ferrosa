---
title: Postgres Front-End — Test Harness Design
status: proposed
executive_summary: >
  Phase-9 test-harness design for the Postgres front-end. Under strict TDD (D9) the harness
  and its fixtures are built FIRST, the failing tests are generated from test-specification.md
  SECOND, and production code follows. Defines the harness components — wire-codec/property
  rigs, the differential-vs-real-Postgres oracle, the driver-matrix conformance rig, the
  unified-RBAC authz-parity rig, and load/soak — plus container/fixture/CI wiring that obeys
  the ferrosa test policy.
---

# Test Harness Design — Postgres Front-End

> Build order is normative (D9): **harness → RED tests → code → refactor.** Several test
> classes (differential vs real Postgres, driver conformance, authz parity) literally cannot
> be authored until their harness exists, so the harness is on the critical path, not a
> follow-on.

## 0. Policy compliance (non-negotiable, from ferrosa/CLAUDE.md)

- No `#[ignore]`; no silent `if cond { return; }` in test bodies.
- Live-infra tests are gated behind the `live-infra-tests` cargo feature and **`panic!` with
  setup instructions** when the matching prerequisite env var is absent (never skip-pass).
- Use the `container_runtime()` helper (not hardcoded `docker`).
- Env contract: `FERROSA_TEST_CONTAINERS=1` (Postgres/MinIO/Cassandra-compat containers),
  `FERROSA_TEST_CLUSTER_NODES=<addr>` (multi-node), `FERROSA_TEST_FIRECRACKER=1` (VMs).
- Every test authored RED first per `/tdd`.

## 1. Harness components

| # | Harness | Layer (test-spec) | Infra | Oracle / assertion |
|---|---------|-------------------|-------|--------------------|
| H1 | Wire-codec + property rig | L1 Unit, L2 Property | none (pure) | round-trip encode/decode every message type; bounded-length cap; SCRAM RFC-5802 test vectors; proptest fuzz of frames + SQL parser |
| H2 | **Differential query oracle** | L3 (centerpiece) | real PostgreSQL container + ferrosa | run identical SQL against both **under `COLLATE "C"`/`LC_COLLATE=C`**; normalize (incl. float/numeric canonical render) + compare; verdict **Match / Mismatch / OutOfScope** (Mismatch fails, OutOfScope recorded); **Postgres is the oracle**. Paired with a **restricted-query rejection oracle** (`reject_oracle`) for queries ferrosa does not support |
| H3 | Driver-matrix conformance rig | L4 | driver containers + ferrosa | per-driver shared conformance script; assert handshake/SCRAM/extended-query/introspection/txn-status |
| H4 | **Unified-RBAC authz-parity rig** | L5/§3.4 | ferrosa (+ optional cluster) | apply the SAME grant fixtures; assert identical allow/deny through the **Postgres path and the CQL path**; any divergence fails (FMEA FM-33) |
| H5 | Isolation/transaction rig | L5 | ferrosa cluster (Accord) | an explicit `BEGIN…COMMIT` block engages Accord **without any GUC** (read-your-writes inside; strict-serializable); autocommit uses eventual default; GUC forces Accord on autocommit when set; BEGIN/COMMIT/ROLLBACK status byte; documented autocommit-path staleness |
| H6 | Integration/system rig | L6 | full server over containers | driver connect-time introspection; multi-keyspace-as-schema; database-bounded joins; D8c default-db reachability |
| H7 | Load/soak rig | L7 | ferrosa + load driver | pre-auth flood; query-of-death / cartesian-join spill bounds; prepared-stmt cache pressure. Reuse `ferrosa-loadgen` patterns |

### H2 — Differential-vs-real-Postgres oracle (the most important rig)

This is the primary defense against the top FMEA risks (FM-12 wrong JOIN, FM-14 wrong
aggregate, RPN 420). Design:

- A `pg_container` fixture (e.g. `postgres:16`) brought up via `container_runtime()`; panics
  with setup instructions if `FERROSA_TEST_CONTAINERS` is unset under `live-infra-tests`. The
  container is initialized with **`LC_COLLATE=C`/`LC_CTYPE=C`** so both sides order text by
  byte value (v1 collation story below).
- A `differential!(sql, schema_fixture)` helper that: loads identical DDL+data into both
  systems, runs `sql` against each **under `COLLATE "C"`**, **normalizes** (column order per
  RowDescription, type rendering, **float/numeric canonical render**, NULL sentinel, and — for
  unordered queries — a canonical sort), then returns one of **three verdicts** rather than a
  binary pass/fail:
  - **Match** — normalized rows AND result-column types/OIDs are equal → PASS.
  - **Mismatch** — normalized results differ → hard FAIL, dumped loud.
  - **OutOfScope** — the case uses a feature ferrosa intentionally does not support, OR a
    locale/collation-dependent behavior the v1 (`COLLATE "C"`) oracle cannot compare →
    **recorded** (counted + logged with reason), **not failed**, **never a silent skip**.
- **v1 collation = `C` only (DEFERRED limitation).** The oracle runs both sides under
  `COLLATE "C"` / `LC_COLLATE=C`, removing false ordering mismatches that are collation
  differences rather than engine bugs (ferrosa has no ICU/libc collation machinery). Locale/ICU
  collation parity is explicitly **deferred** to a follow-up; non-`C` collation cases are
  verdict **OutOfScope** and the catalog reports `C`.
- **Float/numeric canonicalization (part of Match).** Float/numeric values are canonically
  rendered (`1.0` vs `1`, `-0`, scale/exponent text) before comparison so text-format
  differences do not false-trigger a Mismatch. Canonicalization normalizes *rendering* only —
  it compares decoded typed values and never rounds/drops precision (so FM-17 stays catchable).
- A growing **corpus** of SQL cases organized by feature (joins, aggregates, NULL semantics,
  ordering, numeric/typmod edges, subqueries, CTEs). Corpus entries are data, not code, so
  cases are cheap to add when a differential mismatch is found (regression capture).
- A **restricted-query rejection oracle** (`reject_oracle`) — the fail-loud counterpart that
  covers the differential oracle's structural blind spot. For every query ferrosa does NOT
  support (cross-database joins, unsupported types, `FOR UPDATE`, unsupported SQL), it asserts
  ferrosa returns a **clean typed ERROR (SQLSTATE)** and emits **NO rows** — a returned row is a
  hard failure even if it looks correct. The randomized (SQLancer-style) generator is
  constrained to ferrosa's supported grammar/types/collation; out-of-grammar queries route to
  `reject_oracle`, not the differential oracle, so the two partition the query space.
- **Fail-loud rule baked in:** when ferrosa cannot answer a query it must error, never emit
  unproven rows; the harness treats "emitted rows that differ" (Mismatch) and
  "should-have-errored / emitted rows for a restricted query" (`reject_oracle` failure) as
  distinct, both-failing outcomes.

### H4 — Unified-RBAC authz-parity rig (the D8 centerpiece)

Defends FM-33 (grant divergence) and FM-34/35 (silent widen/revoke). Design:

- A `grant_fixture` DSL describing roles, databases, keyspaces (schemas), tables, and grants
  (`CONNECT/USAGE ON DATABASE`, `USAGE ON SCHEMA`, table perms).
- A `probe_matrix` of (role, database, schema, table, action) tuples.
- For each tuple, evaluate the access decision **through the Postgres engine** and **through
  the CQL router**, against the **same** `authorize()` implementation, and assert the
  decisions are identical. A disagreement is a privilege bug → hard failure + the
  path-disagreement counter from the threat model.
- A **rollout-migration** sub-rig: snapshot effective permissions before unification, apply
  the migration, snapshot after, and assert the **diff** is exactly the intended set — no
  silent widen, no silent revoke (audited diff, fail loud).

## 2. Fixtures & seed data

- **Schema/keyspace/database/grant fixtures**: declarative builders that materialize a
  keyspace=schema layout, a database registry + keyspace↔database mapping (D8a), and grants.
- **SCRAM seed creds**: the well-known loadgen dev seed role, seeded with a SCRAM-SHA-256
  verifier (D4) so the driver matrix and differential rigs can authenticate.
- **Join/aggregate datasets**: small deterministic datasets sized to exercise hash-join,
  nested-loop, hash-aggregate, and external sort/spill thresholds.
- Determinism: no wall-clock/random in fixtures; seed any variation by case index.

## 3. CI wiring & Makefile targets

Mirror the existing ferrosa cadence (pure tests on every PR; live-infra/load on nightly,
like the current race-stress nightly job):

| Target | Runs | CI cadence |
|--------|------|------------|
| `make test-pg-unit` | H1 (pure, no infra) | every PR |
| `make test-pg-differential` | H2 over the corpus | PR (smoke subset) + nightly (full) |
| `make test-pg-drivers` | H3 driver matrix | nightly (containers) |
| `make test-pg-authz` | H4 parity + migration diff | PR (core) + nightly (full matrix) |
| `make test-pg-txn` | H5 isolation/Accord | nightly (cluster) |
| `make test-pg-system` | H6 integration | nightly |
| `make test-pg-load` | H7 flood/spill/cache | nightly |

- `cargo fmt --check`, `cargo clippy --all-targets`, and the pure layers must be green on
  every PR; live-infra layers gated by the feature + env, panicking (not skipping) when infra
  is absent so a missing-infra run can never masquerade as a pass.

## 4. Harness build order (what to stand up first)

0. **Prerequisite: `ferrosa-session` extraction (D10).** Lift the neutral `SharedState` core out
   of `ferrosa-cql` into `ferrosa-session` before any harness work; the CQL suite stays green
   (pure refactor) so the new front-ends share the core without a `postgres → cql` edge.
1. **H1** (pure) — no infra; unblocks codec/SCRAM/parser TDD immediately.
2. **H2** differential oracle — the Postgres container + `differential!` helper; unblocks all
   engine-correctness TDD (the M1 join slice depends on this).
3. **H4** authz-parity rig (core probe set) — needed once the unified `authorize()` exists.
4. **H3** driver matrix — psql + psycopg3 first (M1), remaining drivers later.
5. **H5/H6/H7** — as the corresponding features come online.

## 5. Milestone-1 minimal harness subset

To make the D6 first-JOIN slice testable, the following must exist before M1 code:

- H1 (codec round-trip, SCRAM vectors).
- H2 differential oracle with a join corpus seed.
- H3 limited to psql/libpq + psycopg3 (SCRAM, extended query, JOIN, `\d`).
- H4 minimal: CONNECT-gate + database-bounded-join authz probes; default-db `ferrosa`
  reachability (per the refreshed test-spec M1 set).

## 6. Traceability

Each rig maps to its test-spec layer (H1→L1/L2, H2→L3, H3→L4, H4→L5/§3.4, H5→L5, H6→L6,
H7→L7) and to the failure modes it defends (H2→FM-12/14; H4→FM-33/34/35; H1→DoS/SCRAM
threats; H7→query-of-death/pre-auth-flood). Detailed cases live in `test-specification.md`;
failure modes in `fmea.md`; threats in `threat-model.md`.
