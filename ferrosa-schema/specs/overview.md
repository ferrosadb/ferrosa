---
crate: ferrosa-schema
status: implemented
last_updated: 2026-06-19
executive_summary: >
  The authority for schema state in a Ferrosa node: keyspaces, tables, columns,
  roles, grants, indexes, UDTs/UDFs/UDAs, the system keyspaces, the audit
  pipeline, and startup security validation. The central Schema type holds an
  immutable SchemaSnapshot behind an ArcSwap (lock-free reads) and serialises
  mutations behind a write lock (clone-mutate-swap-bump-audit). Every
  user-facing mutation is auth-checked (ADR-006) and emits an audit event
  (ADR-008); *_internal variants bypass both for Raft/pair-mode replication.
---

# ferrosa-schema — Architecture Overview

## Purpose & boundary

`ferrosa-schema` is the **policy and metadata authority** for a node. It answers
two questions for every DDL/DML-gating operation:

1. *Is this operation permitted?* — via `check_permission` over the RBAC grant
   and role-hierarchy graph.
2. *What is the resulting schema state?* — via an atomically-swapped
   `SchemaSnapshot`.

Its boundary stops at **durability**: this crate never writes SSTables. It
publishes the column-index contract (`system::persistence`) and a
`SystemTableMutation` type so that `ferrosa-storage` can persist
`system_schema.*` / `system_auth.*` rows, and the cluster layer can replicate
schema changes through Raft. The in-memory registry is the cache; the persisted
rows are the source of truth on reboot.

## Module map

| Module | Responsibility |
|--------|----------------|
| `registry` (`src/registry.rs`, ~3.9k LoC) | `Schema`, `SchemaSnapshot`, `SchemaConfig`, `AuthMethod`; all auth-checked + `*_internal` CRUD; `apply_snapshot`; `would_create_cycle` |
| `metadata/` | `KeyspaceMetadata`, `TableMetadata`/`TableParams`/`TableFlag`, `ColumnMetadata`/`ColumnKind`/`ColumnMask`, `IndexMetadata`, `UserTypeMetadata`, `UserFunctionMetadata`, `UserAggregateMetadata` |
| `auth/` | `AuthContext`, `RoleMetadata`, `Permission`/`Resource`, recursive `check_permission`; `PasswordHasher` (bcrypt/argon2id), `PasswordPolicy`, `AuthRateLimiter`, `scram` (D4), `bootstrap` seed roles |
| `audit/` | `AuditSink` trait + `LogAuditSink`/`SystemTableAuditSink`/`CompositeSink`/`TestAuditSink`; `AuditEvent`/`AuditEventKind` |
| `system/` | Query builders for `system.local`, `system.peers(_v2)`, `system_schema.*`, `system_auth.*`; `persistence` column-index contract + `SystemTableMutation` |
| `virtual_table` + `virtual_registry` | `VirtualTable` trait for code-backed observability tables; lock-free `VirtualTableRegistry` |
| `validation` | Identifier / partition-key / replication validation for DDL |
| `startup` | `DeploymentMode`, `validate_production_requirements`, `ProductionViolation` |
| `convert` | `cql_to_marshal_type`, `TableMetadata → ferrosa_common::schema::TableSchema` |
| `secrets` | `SecretsProvider` trait + `EnvSecretsProvider` |
| `error` | `SchemaError` (`#[non_exhaustive]`), `Result` alias |

## Data flow

See [data-flow.md](data-flow.md) for the full sequence diagram. In brief:

**Auth-checked DDL** (front-end → registry): a request carries an `AuthContext`
→ `Schema::create_table` calls `check_permission(auth, Create, Keyspace(ks))`
→ rejects system keyspaces → takes `write_lock` → clones the current snapshot →
validates (`validate_table`, graph-extension rules) → inserts → bumps
`version = Uuid::new_v4()` → `ArcSwap::store` → `emit_audit_with_actor`. Readers
on other threads see either the old or new snapshot atomically; no read ever
blocks.

**Replication path** (Raft/pair-mode → registry): the leader-applied entry calls
the matching `*_internal` method, which skips `check_permission` and audit and
applies the change idempotently. `set_schema_version` is then called so all
nodes converge on one `version` UUID. Note: `apply_snapshot` and
`create_table_internal` do **not** register the table with the `StorageEngine` —
the caller must do that separately.

**Auth path** (login): `Schema::authenticate` checks the rate limiter *before*
hashing (DoS guard), verifies the password against the role's `salted_hash`
(with a decoy hash on miss to defeat timing side-channels), auto-rehashes if the
configured algorithm differs, and returns an `AuthContext`.

## Key invariants

1. **Atomic snapshot swaps.** Every mutation clones the whole `SchemaSnapshot`,
   mutates the clone, and `ArcSwap::store`s it under `write_lock`. Readers never
   observe a partially-applied DDL.
2. **Version bump per user mutation.** Auth-checked mutations set
   `version = Uuid::new_v4()` so CQL clients (cqlsh) detect schema changes.
   `*_internal` paths deliberately do **not** bump; the Raft state machine sets
   the version explicitly via `set_schema_version` for cross-node convergence.
3. **System keyspaces/tables are protected.** `is_system_keyspace` and
   `TableMetadata::is_system` reject user DDL on `system`, `system_schema`,
   `system_auth`, `system_observability`.
4. **No SCRAM verifier without cleartext.** A bcrypt/argon2 hash cannot derive a
   SCRAM verifier, so `HASHED PASSWORD` roles get `scram = None` (D4 gap — see
   FMEA SC-4).
5. **Role hierarchy is acyclic.** `would_create_cycle` rejects grants that would
   form a cycle; `check_permission` is additionally cycle-safe via a `visited`
   set.
6. **Mutations are auth-checked + audited; replication is not.** Public methods
   take `&AuthContext` and emit audit; `*_internal` methods are the only
   un-audited mutators and exist solely for trusted replication.

## Position in the dependency graph

A mid-layer crate: depends only on `ferrosa-common`, `ferrosa-index`,
`ferrosa-sstable`, and is depended on by 12 crates (`ferrosa`,
`ferrosa-cluster`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-flight`,
`ferrosa-graph`, `ferrosa-loadgen`, `ferrosa-postgres`, `ferrosa-row-bridge`,
`ferrosa-session`, `ferrosa-storage`, `ferrosa-view`). See the
[root crate index](../../specs/crates.md) for the full graph.
</content>
