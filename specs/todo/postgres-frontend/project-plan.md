---
title: Postgres Front-End — Project Plan
status: proposed
executive_summary: >
  Staged, TDD-first plan to build a Postgres wire-protocol front-end for ferrosa. Milestone 1
  is a first-JOIN-end-to-end vertical slice that front-loads the bespoke-engine risk; later
  milestones widen SQL coverage, harden the wire protocol, and complete the driver matrix.
  Priorities are seeded from fmea.md (RPN>=200 -> P1) and threat-model.md (Critical/High).
---

# Postgres Front-End — Project Plan

Everything is built TDD (red-green-refactor), per the repo policy and D7. Code starts only
after this blueprint is approved, on a feature branch (`feature/postgres-frontend`) in the
`ferrosa` repo. No other sub-repo is touched by v1.

**Execution ordering (D9, strict TDD):** within every sprint the order is **harness → RED
tests → code → refactor**. Test infrastructure and failing tests are authored *before* the
production code they cover. The Foundation sprint below stands up the harness and the
`ferrosa-session` extraction *before* any Postgres wire/engine code exists.

## Milestone gate (from D6 / D3)

**M1 = first JOIN end-to-end.** When M1 is green we hold a checkpoint to re-confirm the
bespoke-engine bet (D3) on real evidence before building the full optimizer.

## Sprint F — Foundation: harness + `ferrosa-session` extraction (precedes ALL Postgres code)

Goal: the test scaffolding and the neutral shared-state crate exist before any wire/engine
code. Nothing here ships Postgres behavior; it makes the rest TDD-able.

- **`ferrosa-session` extraction (D10):** lift `SharedState` + the protocol-agnostic write/DDL
  contract out of `ferrosa-cql` into a new `ferrosa-session` crate; refactor `ferrosa-cql` to
  consume it. **Gate: the existing CQL test suite stays green** (this is a pure refactor of
  existing code — regression-tested, no behavior change).
- **Test harness stand-up (D9, see test-harness.md):** H1 (pure codec/property/SCRAM-vector
  rig) first; then H2 differential-vs-real-Postgres oracle (Postgres container + `differential!`
  helper) with a seed join corpus; H4 authz-parity rig core; H3 limited to psql + psycopg3.
  All gated by `live-infra-tests` + `FERROSA_TEST_CONTAINERS`, panicking on missing infra.
- **Generate RED tests** from `test-specification.md` for the M1 slice — they compile and fail
  for the right reason (no implementation yet).
- Home the D8 `authorize()` + database registry + `pg_catalog` virtual tables in
  `ferrosa-schema`, pure over a metadata snapshot (D10) — interfaces + RED tests first.
  This is the single shared grant checkpoint that defuses **FM-33** (grant-check divergence,
  RPN 480, P1 dominant authz): the differential-authz RED tests drive the same grant fixtures
  through both the Postgres and CQL paths and assert identical allow/deny.

Exit criteria: CQL suite green post-extraction; M1 test set present and RED; harness brings up
the Postgres oracle + psql/psycopg3 drivers under the feature flag.

## Sprint 0 — Spine skeleton (no engine yet)

Goal: a real driver completes the handshake and SCRAM auth and gets a `ReadyForQuery`.

- New crate `ferrosa-postgres`; `PostgresServer::start_background` bound on 5432, wired into
  `ferrosa/src/main.rs` with `SharedState` clone and config/env gating.
- Message codec with **bounded max length** (mirror CQL 256 MiB cap) — *unit + property
  round-trip tests first*. StartupMessage / SSLRequest / CancelRequest special cases.
- Connection state machine `Startup → Auth → Ready`; transaction-status byte plumbing.
- SCRAM-SHA-256 server exchange (`scram.rs`) against a new `scram_sha256` verifier on the
  role store; populate the verifier on every password-set path (CQL + Postgres) — **D4**.
- Seed dev creds (the loadgen well-known seed) with a SCRAM verifier so M1 can authenticate.
- TLS / SSLRequest negotiation reusing `ferrosa-net` TLS.

P1 items addressed: FM-06 (SCRAM exchange), FM-25 (cross-protocol verifier), threat S/T pre-
auth controls, oversized-startup DoS cap.

## Sprint 1 — Catalog + single-table SELECT

Goal: `\dn`/`\d` work and a single-table `SELECT ... WHERE pk=$1` returns correct rows.

- `ferrosa-schema` virtual tables for `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
  `information_schema.*` from live metadata — **D5**.
- `catalog_queries.rs` recognizes/serves `current_schema()`, `search_path`, `SHOW`, and the
  driver connect-time introspection queries.
- CQL-type ↔ Postgres-OID mapping + text/binary encoders (`types.rs`) — *unit tests per OID*.
- `ferrosa-sql` minimal: parser for `SELECT proj FROM schema.table WHERE key=lit/param`,
  binder over catalog, `RangeScan` operator with predicate/projection pushdown to storage.
- Extended-query path enough to carry `$1` params (Parse/Bind/Describe/Execute/Sync) + portal
  manager; simple-query path.

- **Multi-database control plane (D8):** database registry + keyspace↔database mapping
  table (many-to-many); CQL `CREATE KEYSPACE` auto-registers into default db `ferrosa`;
  `pg_database` virtual table. Connection binds to a database; visible schemas = that db's
  attached keyspaces.
- **Unified RBAC (D8b):** single shared grant checkpoint consulted by BOTH the Postgres
  engine and the CQL router; `GRANT ON DATABASE`/`ON SCHEMA`. **CQL backward-compat
  migration** so existing roles keep access (fail loud on denial). See
  `todo/multi-database-control-plane.md`.

P1 items addressed: FM-10 (OID/type mapping), FM-11 (catalog-emulation gap), FM-02/FM-03
(extended-query lifecycle), FM-33 (unified-grant divergence, D8), FM-34/FM-35 (rollout
migration widen/revoke, D8).

## Sprint 2 — First JOIN (Milestone 1) 🎯

Goal: a two-table `JOIN ... WHERE pk=$1` is planned and returned correctly to psql/psycopg.

- Logical plan nodes (Scan/Filter/Project/Join) + a first physical join operator
  (`HashJoin`, bounded + spill) and `NestedLoopJoin` fallback.
- **Differential test harness vs real PostgreSQL** (container) — the centerpiece control for
  FM-12/FM-14 (silently-wrong results, top RPN 420). Same SQL → byte-compare results.
- Driver-matrix smoke for M1: libpq/psql + psycopg3 (SCRAM, extended query, JOIN, `\d`).
- **M1 checkpoint:** re-confirm bespoke vs embed with evidence (D3 gate).

P1 items addressed: FM-12 (wrong JOIN) — gating; FM-08 (planner resource bound / OOM).

## Sprint 3 — Aggregates, sort, NULL/type correctness

- `HashAggregate` (COUNT/SUM/AVG/MIN/MAX), `Sort` (external/spill), GROUP BY/HAVING,
  ORDER BY, LIMIT/OFFSET. NULL/typmod/numeric edge cases.
- Expand differential corpus heavily (NULLs, ordering, type coercions).

P1 items addressed: FM-14 (wrong aggregate), FM-15/FM-20 (encoding/NULL edge cases).

## Sprint 4 — Subqueries, CTEs, isolation semantics

- Subqueries, CTEs; finalize join ordering rules in the optimizer.
- Transaction/isolation (D11): prove an explicit `BEGIN … COMMIT` block engages Accord
  **without a GUC** (read-your-writes, strict); autocommit stays eventual; GUC can force Accord
  on autocommit. BEGIN/COMMIT/ROLLBACK status correctness; documented autocommit-path staleness.
  Wire `ferrosa.isolation` GUC (startup + SET); resolve Q1.

P1 items addressed: FM-22 (Accord opt-in silently not engaging), FM-04 (transaction-status byte).

## Sprint 5 — Wire-protocol completeness + full driver matrix

- COPY (in/out), cursors (`DECLARE`/`FETCH`), function-call, cancellation (`CancelRequest`/
  `BackendKeyData`) hardened, error/notice field completeness, `ParameterStatus`.
- Full driver conformance matrix: pgx (Go), pgjdbc (Java), node-postgres, asyncpg.
- Resolve Q2 (dbname handling), Q4 (channel binding), Q3 (legacy role migration tool).

High threats addressed: CancelRequest forgery, GUC injection, pooler GUC leakage, cross-
tenant catalog disclosure.

## Sprint 6 — Load, soak, hardening

- Pre-auth flood resistance; query-of-death / cartesian-join spill bounds; prepared-statement
  cache pressure; long-soak stability. Metrics + alerts for each threat counter.

## Priority seed (traceability)

| Source | Item | Sprint |
|--------|------|--------|
| FMEA P1 (RPN 480, DOMINANT authz) | FM-33 grant-check divergence Postgres-vs-CQL → single shared `authorize()` + differential authz | Sprint F / S1 |
| FMEA P1 (D8 rollout) | FM-34 migration silently widens, FM-35 migration silently revokes → scoped fail-loud migration + audit diff | S1 |
| FMEA P1 (RPN 420) | FM-12 wrong JOIN, FM-14 wrong aggregate | S2, S3 |
| FMEA P1 | FM-06 SCRAM exchange + FM-25 cross-protocol verifier, FM-10 OID, FM-02/03 extended-query | S0, S1 |
| FMEA P1 | FM-08 planner bound/OOM, FM-22 Accord opt-in, FM-04 txn-status byte | S2, S4 |
| Threat Critical (x5) | pre-auth flood, oversized msg, SCRAM downgrade, query-of-death, catalog disclosure | S0, S2, S5 |
| Open follow-ups | Q1 isolation GUC, Q2 dbname, Q3 migration, Q4 channel binding | S4, S5 |

## Cross-repo / dependency note

- `ferrosa-dbaas` and `ferrosa-memory` consume CQL/snapshot/schema interfaces, **not** the
  Postgres front-end, so v1 is contained to `ferrosa`. Re-check before exposing Postgres
  through the DBaaS CQL proxy layer (future work).
