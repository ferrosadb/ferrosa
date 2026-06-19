---
title: Postgres Front-End — Blueprint Index
status: proposed
executive_summary: >
  Blueprint for adding a Postgres wire-protocol front-end to ferrosa. Index of the Phase-0
  decision log and the architecture, threat-model, FMEA, test-spec, and project-plan
  artifacts produced from it. Destination: full Postgres wire protocol, driver-compatible,
  real relational SQL over a bespoke engine, explicit transactions use Accord; autocommit
  eventual, with a
  multi-database namespace and unified RBAC across Postgres and CQL (D8) and a harness-first
  strict-TDD execution order (D9). Milestone 1 is a first-JOIN-end-to-end slice.
---

# Postgres Front-End — Blueprint

A new Postgres wire-protocol listener for ferrosa, mirroring the existing CQL/Bolt/SPARQL
front-ends and sharing the same storage/schema/auth/Accord state. This directory is the
blueprint; **no implementation code exists yet** (decision D7: full blueprint first, then
code on a feature branch).

## Artifacts

| File | Phase | What it is |
|------|-------|------------|
| [`decisions.md`](decisions.md) | 0 | Eleven locked decisions (D1–D11) from the grill, with consequences and constraints |
| [`decision-tree.md`](decision-tree.md) | 0 | Dependency ordering of the decisions + four open follow-ups (Q1–Q4) |
| [`architecture.md`](architecture.md) | 1–2 | Target structure: crates, component diagram, read/write hybrid, slot-in to `main.rs` |
| [`dsm.md`](dsm.md) | 5a | Design-structure-matrix: coupling analysis + the `ferrosa-session` extraction (D10) |
| [`threat-model.md`](threat-model.md) | 3 | STRIDE over the wire/auth/engine surface; Critical/High work items |
| [`fmea.md`](fmea.md) | 4 | 40 failure modes (FM-01..FM-40) with RPN; P1 items (RPN≥200); FM-33 grant-divergence (RPN 480) co-dominant P1 with FM-12/14; differential-testing as the top control |
| [`risk-register.md`](risk-register.md) | 4 | Consolidated risk register (FMEA + threat + schedule) |
| [`test-specification.md`](test-specification.md) | 7 | 7-layer TDD test plan; differential-vs-real-Postgres centerpiece; M1 minimal test set |
| [`test-harness.md`](test-harness.md) | 9 | Harness design (codec/differential/driver/authz rigs); harness-first build order (D9) |
| [`project-plan.md`](project-plan.md) | 6 | Staged sprints (Foundation + S0–S6) with the M1 gate; priorities seeded from FMEA + threats |
| [`todo/`](todo/) | 5c | Work items for P1 failure modes, Critical threats, and open follow-ups |

## The eleven decisions at a glance

1. **D1** Consistency — autocommit eventual by default; opt into strict-serializable (Accord)
   via the `ferrosa.isolation` GUC (connection-time or `SET`). _(Refined by D11.)_
2. **D2** SQL scope — real relational (joins, subqueries, CTEs).
3. **D3** Engine — bespoke planner/optimizer/operators (no DataFusion). *Dominant risk.*
4. **D4** Auth — SCRAM-SHA-256 verifier alongside bcrypt in the shared role store.
5. **D5** Namespace — keyspace = Postgres schema. _(Superseded in part by D8.)_
6. **D6** Milestone 1 — first JOIN end-to-end over a real driver.
7. **D7** Process — finish the blueprint first, then implement on a branch.
8. **D8** Multi-database — real `database → schema(=keyspace) → table`; keyspace↔database
   mapping table (many-to-many); joins bounded to one database; **unified** db/schema grants
   gating both Postgres and CQL; unmapped keyspaces auto-land in default db `ferrosa`.
9. **D9** Process (refines D7) — strict TDD: **harness → RED tests → code → refactor**;
   the harness and failing tests are the FIRST sprint, not deferred.
10. **D10** Decoupling (from DSM) — extract a neutral `ferrosa-session` crate so
    `ferrosa-postgres` does **not** depend on `ferrosa-cql`; home `authorize()`, the database
    registry, and `pg_catalog` virtual tables in `ferrosa-schema` (pure over a metadata
    snapshot); `WritePath` stays in `ferrosa-cluster`.
11. **D11** Accord on the txn block (refines D1) — an explicit `BEGIN … COMMIT` block always
    runs on Accord (strict-serializable, read-your-writes inside) with **no GUC required**;
    autocommit stays eventual; the GUC can still force Accord on autocommit. Resolves R1/R2.

## Biggest risks (read these first)

- **Grant-check divergence Postgres-vs-CQL** (FMEA FM-33, RPN 480 — the #1 risk). Two
  enforcement paths drift → silent privilege escalation. Mitigation: a single shared
  `authorize()` in `ferrosa-schema` + differential authz tests driving the same grant fixtures
  through both paths.
- The bespoke engine returning **silently-wrong** join/aggregate results (FMEA FM-12/FM-14,
  RPN 420). Mitigation: differential testing against real PostgreSQL; fail loud over emitting
  unproven rows.
- **`ferrosa-cql` cycle-coupling** (D10/DSM): obtaining `SharedState` by depending on the
  whole `ferrosa-cql` crate is a near-cycle hazard — mitigated by the `ferrosa-session`
  extraction so no `postgres → cql` edge forms.
- The maximalist destination (full wire + real relational + bespoke) vs. "working subset
  first" — managed by the staged plan and the M1 evidence gate.

## Not yet run (deferred until code exists)

The completed analysis artifacts include the **DSM** (→ D10), [`test-harness.md`](test-harness.md),
and [`test-specification.md`](test-specification.md); under D9 the harness + RED tests are the
FIRST implementation sprint (Sprint F), **not** deferred. What genuinely remains deferred until
code exists: the correctness-hazard scan, CI/pipeline-defense, generating the spec's tests into
actual code files (Phase 8), and the compiled executable project plan — there is no code to
analyze or compile yet.
