---
title: Postgres Front-End — Multi-Database Control Plane (D8)
status: todo
executive_summary: >
  Work items introduced by D8: a multi-database namespace layer (database -> schema=keyspace
  -> table), a keyspace<->database mapping table, unified database/schema RBAC gating both
  Postgres and CQL, and the CQL backward-compat migration this implies.
---

# Multi-Database Control Plane (D8)

New control-plane surface that did not exist in the original D5 single-database design.

## Work items

1. **Database registry + mapping table.** New control/system tables: a Postgres-database
   registry and a many-to-many keyspace↔database mapping (D8a). DDL broadcast must cover
   them. Drives the `pg_database` virtual table.
2. **`CREATE DATABASE` / attach-keyspace operations** on the Postgres path (and a CQL/ctl
   way to manage them); CQL `CREATE KEYSPACE` auto-registers into default db `ferrosa` (D8c).
3. **Unified grant model (D8b).** `GRANT ON DATABASE` (CONNECT/USAGE) + `GRANT ON SCHEMA`
   mapped onto existing keyspace perms, enforced at a **single shared checkpoint** consulted
   by both the Postgres engine and the CQL router. Folded into / extending `system_auth`.
4. **Database-bounded JOIN enforcement.** Planner/binder restricts visible schemas to the
   connected database's attached keyspaces; cross-database joins error clearly (D8a).
5. **`pg_database` + filtered `pg_namespace`/`pg_class`/`pg_attribute`** virtual tables
   reflecting the connected database and caller grants.
6. **CQL backward-compat migration (rollout-gating).** Existing CQL roles have keyspace/table
   perms but no database grant. Unification (D8b) must not silently revoke their access:
   either treat default db `ferrosa` as implicitly connectable for roles holding the keyspace
   perms, or auto-grant `CONNECT ON DATABASE ferrosa` during migration. **Fail loud on any
   denial; never silently widen.** This is a correctness + security gate, not cosmetic.

## Open detail (minor, decide in design)

- Once a keyspace has ≥1 explicit database attachment, does it still also appear in the
  default `ferrosa` database, or do explicit attachments replace the implicit default?
  (Lean: replace; document the rule.)

## Risk note (route to fmea.md update)

A divergence between the Postgres-path and CQL-path grant checks is a **privilege bug**
(role denied on one path but allowed on the other). Add an FMEA row: enforce once, share the
check, and test both paths against the same grant fixtures.
