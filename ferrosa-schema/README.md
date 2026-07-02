# ferrosa-schema

> The authority for schema state: keyspaces, tables, columns, roles, grants,
> indexes, UDTs/UDFs/UDAs, the system keyspaces, the audit pipeline, and
> startup security validation. Every mutation is auth-checked (ADR-006) and
> emits an audit event (ADR-008).

## What this crate is

`ferrosa-schema` owns the in-memory **schema registry** — the single
point-in-time view of all DDL and RBAC state in a Ferrosa node. The central
type is [`Schema`](src/registry.rs): it holds an immutable
[`SchemaSnapshot`](src/registry.rs) behind an `ArcSwap` so reads are lock-free,
and serialises mutations behind a `write_lock` (clone → mutate → swap → bump
version → emit audit). It is consumed by nearly every other crate in the
workspace (CQL/Postgres/Flight front-ends, the cluster Raft state machine, the
graph engine, `ferrosa-ctl`, storage).

This crate is **policy + metadata**, not storage. It decides *whether* a DDL or
auth operation is allowed and what the resulting schema state is; the durable
SSTable rows for `system_schema.*` / `system_auth.*` are written by
`ferrosa-storage` from the column-index contract this crate publishes in
`system::persistence`.

## What's implemented

- **Metadata model** ([`metadata/`](src/metadata)) — `KeyspaceMetadata`,
  `TableMetadata` (with `TableParams`, `TableFlag`, `CachingParams`, graph
  `extensions`, `is_system`), `ColumnMetadata` (`ColumnKind`,
  `ClusteringOrder`, `ColumnMask` for dynamic data masking), `IndexMetadata`,
  `UserTypeMetadata`, `UserFunctionMetadata`, `UserAggregateMetadata`.
- **Schema registry** ([`registry.rs`](src/registry.rs), ~3.9k LoC) — the
  `Schema` type with auth-checked CRUD (`create_keyspace`, `create_table`,
  `alter_table`, `drop_*`, `create_index`, `create_role`, `create_role_hashed`,
  `alter_role`, `grant`/`revoke`, `grant_role`/`revoke_role`, UDT/UDF/UDA ops)
  **plus** `*_internal` variants that bypass auth/audit for Raft/pair-mode
  replication. Both `drop_table` and `drop_table_internal` cascade over the
  dropped table's `SchemaSnapshot.indexes` entries (t_ae06e925).
  `apply_snapshot` bulk-loads a snapshot (skips system keyspaces).
- **Auth / RBAC** ([`auth/`](src/auth)) — `AuthContext`, `RoleMetadata`,
  Cassandra-style `Permission` (9 variants) and `Resource` hierarchy
  (`AllKeyspaces > Keyspace > Table`, `AllRoles > Role`),
  recursive `check_permission` with superuser bypass + role-hierarchy
  inheritance + cycle-safe traversal. Password hashing (`bcrypt` cost-12 default
  or `argon2id`) with auto-rehash on login, `PasswordPolicy` (incl.
  `iso27001()`), per-username `AuthRateLimiter`, and SCRAM-SHA-256 verifier
  derivation (`scram`, decision D4) for Postgres login.
- **System keyspaces** ([`system/`](src/system)) — query builders for
  `system.local`, `system.peers(_v2)`, `system_schema.{keyspaces,tables,columns,
  aggregates}`, `system_auth.*`, plus `persistence.rs` (column-index contract +
  `SystemTableMutation` bridging DDL to storage writes).
- **Audit** ([`audit/`](src/audit)) — `AuditSink` trait, `LogAuditSink`,
  `SystemTableAuditSink`, `CompositeSink` fan-out, `TestAuditSink`, and a typed
  `AuditEventKind` enum.
- **Virtual tables** ([`virtual_table.rs`](src/virtual_table.rs),
  [`virtual_registry.rs`](src/virtual_registry.rs)) — `VirtualTable` trait for
  live, code-backed observability tables; `VirtualTableRegistry` (lock-free,
  `ArcSwap`).
- **Validation & startup** ([`validation.rs`](src/validation.rs),
  [`startup.rs`](src/startup.rs)) — identifier/PK validation;
  `DeploymentMode` + `validate_production_requirements` security gate.
- **Storage bridge** ([`convert.rs`](src/convert.rs)) — `cql_to_marshal_type`
  and `TableMetadata → TableSchema` conversion for `ferrosa-common`.
- **Secrets** ([`secrets/`](src/secrets)) — `SecretsProvider` trait +
  `EnvSecretsProvider`.

## Public API (key entry points)

| Area | Types / functions |
|------|-------------------|
| Registry | `Schema`, `SchemaSnapshot`, `SchemaConfig`, `AuthMethod`, `is_system_keyspace` |
| Metadata | `KeyspaceMetadata`, `TableMetadata`, `ColumnMetadata`, `IndexMetadata`, `UserTypeMetadata`, `UserFunctionMetadata`, `UserAggregateMetadata` |
| Auth | `AuthContext`, `Permission`, `Resource`, `RoleMetadata`, `GrantEntry`, `check_permission`, `PasswordHasher`, `PasswordPolicy`, `AuthRateLimiter`, `ScramCredential` |
| Audit | `AuditSink`, `AuditEvent`, `AuditEventKind`, `LogAuditSink`, `SystemTableAuditSink`, `CompositeSink` |
| Virtual tables | `VirtualTable`, `VirtualTableRegistry`, `VirtualRow`, `RowPredicate` |
| System | `query_local`, `query_peers`, `query_keyspaces/tables/columns`, `query_roles/role_members/role_permissions` |
| Startup | `DeploymentMode`, `validate_production_requirements`, `ProductionViolation` |
| Error | `SchemaError`, `Result` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `CqlType`, `CellValue`, `DataType`, and
  `schema::{TableSchema, ColumnDefinition}` (the conversion target in `convert`).
- **`ferrosa-index`** — index type definitions used by `IndexMetadata`.
- **`ferrosa-sstable`** — SSTable shapes referenced by system-table persistence.

External: `arc-swap`, `bcrypt`, `argon2`, `pbkdf2`, `hmac`, `sha2`,
`password-hash`, `uuid`, `serde`/`serde_json`, `indexmap`, `rand`, `tracing`.

**Called by** (crates that depend on this):

- `ferrosa`, `ferrosa-cluster`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-flight`,
  `ferrosa-graph`, `ferrosa-loadgen`, `ferrosa-postgres`, `ferrosa-row-bridge`,
  `ferrosa-session`, `ferrosa-storage`, `ferrosa-view`.

## Tests

354 in-crate `#[test]` functions plus 19 integration tests
(`tests/{auth_integration,integration,property_tests}.rs`). Coverage is strong
for permission checks, password hashing, error display, and production
validation. Live gaps are tracked in [specs/fmea.md](specs/fmea.md).

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [FMEA / known issues](specs/fmea.md) — failure modes + real gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
- [Data flow](specs/data-flow.md) — DDL apply + auth-check sequence
</content>
</invoke>
