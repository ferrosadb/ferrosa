---
title: Postgres Front-End — Open Follow-Ups & Priority Work Items
status: todo
executive_summary: >
  Deferred decisions (Q1–Q4) and the highest-priority work items (FMEA P1 / threat Critical)
  to resolve during the named sprints. Captured here and on the forge task board.
---

# Open Follow-Ups (from grill / decision-tree)

| ID | Question | Owner decision | Default leaning | Resolve in |
|----|----------|----------------|-----------------|-----------|
| Q1 | Exact `ferrosa.isolation` values; alias `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` → Accord? | D1 | Support both; alias standard form | Sprint 4 |
| Q2 | Lenient vs strict handling when client connects with dbname ≠ `ferrosa` | D8 | Strict: reject a dbname not in the registry (per D8 / threat-model S5); no coercion to a default | Sprint 5 |
| Q3 | Migration/backfill tool for legacy bcrypt-only roles to gain a SCRAM verifier | D4 | Capture at next reset; no silent backfill | Sprint 5 |
| Q4 | SCRAM channel binding (`SCRAM-SHA-256-PLUS`) under TLS | D4 | Follow-up after base SCRAM | Sprint 5 |

# P1 Failure Modes (FMEA RPN ≥ 200) → sprint

- FM-33 grant-check divergence Postgres-vs-CQL (RPN 480, DOMINANT authz) — single shared
  `authorize()` + differential authz tests; fail loud on path disagreement. (Sprint F / S1)
- FM-34 migration silently widens / FM-35 migration silently revokes access (D8 rollout) —
  scoped fail-loud migration + before/after audit diff. (S1)
- FM-12 wrong JOIN result / FM-14 wrong aggregate (RPN 420) — differential testing vs real
  Postgres; fail loud. **Gates M1.** (S2/S3)
- FM-06 SCRAM exchange / FM-25 cross-protocol verifier population. (S0)
- FM-10 OID/type mapping / FM-11 catalog-emulation gap / FM-02/FM-03 extended-query lifecycle. (S1)
- FM-08 planner resource bound / FM-22 Accord opt-in silently not engaging. (S2/S4)
- FM-04 transaction-status byte correctness. (S4)

# Critical Threats (threat-model.md) → sprint

- Pre-auth message flooding; oversized startup/message length (bounded cap). (S0)
- SCRAM downgrade/replay. (S0)
- Query-of-death planner exhaustion (bounded operators + spill). (S2)
- Cross-tenant `pg_catalog` disclosure. (S5)
