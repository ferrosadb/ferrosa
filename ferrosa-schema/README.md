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
  `extensions`, `is_system`; `to_storage_schema` also carries the table's
  `gc_grace_seconds` into the storage schema's `extensions` — a negative value is
  not carried, so the storage layer purges nothing for that table), `ColumnMetadata` (`ColumnKind`,
  `ClusteringOrder`, `ColumnMask` for dynamic data masking), `IndexMetadata`,
  `UserTypeMetadata`, `UserFunctionMetadata`, `UserAggregateMetadata`.
- **Schema registry** ([`registry.rs`](src/registry.rs), ~3.9k LoC) — the
  `Schema` type with auth-checked CRUD (`create_keyspace`, `create_table`,
  `alter_table`, `drop_*`, `create_index`, `create_role`, `create_role_hashed`,
  `alter_role`, `grant`/`revoke`, `grant_role`/`revoke_role`, UDT/UDF/UDA ops)
  **plus** `*_internal` variants that bypass auth/audit for Raft/pair-mode
  replication. Both `drop_table` and `drop_table_internal` cascade over the
  dropped table's `SchemaSnapshot.indexes` entries (t_ae06e925).
  `apply_snapshot` bulk-loads a snapshot (skips system keyspaces) and
  refuses the whole snapshot, before applying any of it, when a table breaks
  the jsonb placement rules. It only INSERTS what is missing; an existing
  table is kept as is. It also adopts the snapshot's `version`, so a node
  restored from its persisted `schema.json` (or brought up to a peer's snapshot)
  holds that state's version rather than the random one minted at construction
  (bug: schema_version diverges after restart, t_7dda9421).
  `replace_table_internal` overwrites a table definition
  (same validation and jsonb checks as create), which pair catch-up uses to
  apply an ALTER the receiver missed (ferrosa-cluster CL-33).
- **jsonb placement rules** ([`jsonb_rules.rs`](src/jsonb_rules.rs), T-154a,
  D3/D21) — jsonb is refused in partition and clustering keys (through
  frozen collections, tuples and UDTs) and as a set element, map key or
  vector element. One check serves `create_table[_internal]`,
  `alter_table[_internal]`, `alter_type_add_field`, `apply_snapshot` and the
  propose-side `check_create_table_jsonb` / `check_alter_table_jsonb`. Errors
  are `SchemaError::JsonbInKey` and `SchemaError::JsonbNesting`.
- **jsonb DDL capability gate — REMOVED** (T-300, D24, D32) — the interim
  capability gate was removed from the tree, not disabled. jsonb is a first-class
  data type: it ships standalone, paired and clustered with no flag, env var,
  config key or operator action, and it cannot be turned off again. The engine
  already replicates graph materialization over Raft, so a column type is not
  special; a gate that can refuse a data type is itself the defect. The surviving
  jsonb constraint is the placement rule above (T-154a). `tests/jsonb_is_first_class.rs`
  pins that jsonb DDL succeeds on every topology.
- **Auth / RBAC** ([`auth/`](src/auth)) — `AuthContext`, `RoleMetadata`,
  Cassandra-style `Permission` (9 variants) and `Resource` hierarchy
  (`AllKeyspaces > Keyspace > Table`, `AllRoles > Role`),
  recursive `check_permission` with superuser bypass + role-hierarchy
  inheritance + cycle-safe traversal. Password hashing (`bcrypt` cost-12 default
  or `argon2id`) with auto-rehash on login, `PasswordPolicy` (incl.
  `iso27001()`), per-username `AuthRateLimiter`, and SCRAM-SHA-256 verifier
  derivation (`scram`, decision D4) for Postgres login. Logins verified outside
  the registry (PostgreSQL SCRAM) go through the SAME limiter via
  `Schema::check_login_rate_limit` / `record_login_failure` / `complete_login`
  (t_e1c819ad), so a lockout is shared across protocols; `complete_login` also
  refuses `NOLOGIN` roles, whose SCRAM verifier is stored all the same.
  Fresh installs do not create the historical `cassandra/cassandra` account.
  A legacy `cassandra` superuser is created only when the secrets provider
  supplies an explicit `superuser_password` (`FERROSA_SUPERUSER_PASSWORD` for
  `EnvSecretsProvider`); ordinary installs use the seeded `ferrosa_admin` role.
  Restart recovery uses `restore_role_internal` to replace fresh-process
  bootstrap credentials with authoritative rows from `system_auth.roles`;
  rotating the built-in password also clears its must-change marker.
- **System keyspaces** ([`system/`](src/system)) — query builders for
  `system.local`, `system.peers(_v2)`, `system_schema.{keyspaces,tables,columns,
  aggregates}`, `system_auth.*`, plus `persistence.rs` (column-index contract +
  `SystemTableMutation` bridging DDL to storage writes). `system_schema.tables`
  rows carry Cassandra's `flags` (`compound`, plus `counter`/`dense`/`super`
  where they apply) and `system_schema.columns` reports `position = -1` for
  regular and static columns, as Cassandra does (t_cd650102). Without `flags`
  the python driver read every table as compact static and dropped its
  clustering key.
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

> T-022: `cql_to_marshal_type` and the aggregate-table type stringifier delegate to `ferrosa_common::cql_type::names`.

## jsonb (T-150)

`system_schema.aggregates` initcond rendering lists `CqlValue::Jsonb` with the other non-literal values (T-150). No schema behaviour changed.


### PostgreSQL table incarnation isolation (2026-10-26)

The registry exposes `lifecycle_read()` for synchronous validation plus physical apply and `lifecycle_write()` for synchronous schema/storage reconciliation. Read leases may overlap; mutation leases are exclusive and reentrant on their acquiring thread. Registry writers acquire lifecycle before the registry mutex. Callers must acquire the lifecycle fence before storage locks and must never hold either guard across an await. Table retirement, replacement, keyspace retirement and snapshot restore therefore wait for admitted PostgreSQL applies to finish. The fence is shared across the registry, so DDL can wait for applies to unrelated tables too.

Snapshot import adopts an incoming different table UUID and removes superseded index metadata. The registry owns no storage: callers must retire the old physical incarnation under the write fence before publishing this replacement. Same-UUID imports preserve the existing definition.

Storage schemas now persist the authoritative UUID in the reserved `ferrosa.table_id` extension (overwriting caller-supplied values). Restore prefers that provenance, with the local schema registry supplying legacy provenance when available. Unidentified data or malformed UUID metadata is refused before any table is retired; recoverable data is preserved for explicit repair. Empty registrations can adopt the incoming schema.

## Immutable scalar scan values

System-schema metadata uses the immutable shared text/blob variants of the common CQL runtime value type. Schema statement formatting still explicitly produces owned textual CQL literals. Schema metadata contents, encoding and persistence contracts remain unchanged.
