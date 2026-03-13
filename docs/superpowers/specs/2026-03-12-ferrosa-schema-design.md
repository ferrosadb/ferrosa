# ferrosa-schema Design Specification

> Date: 2026-03-12
> Status: Draft
> Chunk: A (of A–F)

## Goal

Build the schema management crate for Ferrosa: rich metadata types, a thread-safe schema registry, authentication and authorization with column masking, audit logging, and system keyspace responses that let CQL drivers connect and discover schema.

## Architecture

`ferrosa-schema` is a single crate with layered modules. It depends only on `ferrosa-common`. It is the authority for what keyspaces, tables, columns, and roles exist. Every mutating operation requires an `AuthContext` — auth is baked in from day one, not retrofitted. Every auth and DDL operation emits an audit event — observability is baked in from day one, not retrofitted (ADR-008).

The crate produces typed structs for system keyspace queries. The CQL layer (ferrosa-cql, future) handles wire serialization.

## Tech Stack

- `ferrosa-common` — shared types (`TableSchema`, `ColumnDefinition`, `Token`)
- `arc-swap` — lock-free snapshot reads (same pattern as ferrosa-storage)
- `uuid` — table IDs, schema versions, host IDs
- `bcrypt` — default password hashing
- `argon2` — optional stronger password hashing
- `serde`, `serde_json` — serialization for persistence
- `indexmap` — insertion-ordered column maps
- `tracing` — structured audit event logging (default sink)

## Chunk Breakdown

| Chunk | Scope |
|-------|-------|
| **A (this spec)** | Core types + Schema registry + Auth (roles, permissions, column masking, `system_auth`) + Audit logging + system keyspaces (`system.local`, `system.peers_v2`, `system_schema.keyspaces/tables/columns`) |
| **B** | DDL validation (CREATE/ALTER/DROP keyspaces and tables, type validation, auth-gated) |
| **C** | UDTs + `system_schema.types` |
| **D** | Indexes + Views + `system_schema.indexes/views` |
| **E** | Functions + Aggregates + Triggers + `system_schema.functions/aggregates/triggers` |
| **F** | Traces + Distributed (`system_traces`, `system_distributed`) |

---

## Module Structure

```
ferrosa-schema/src/
├── lib.rs                  # Public API, re-exports
├── metadata/
│   ├── mod.rs
│   ├── keyspace.rs         # KeyspaceMetadata, ReplicationParams
│   ├── table.rs            # TableMetadata, TableParams, TableId
│   └── column.rs           # ColumnMetadata, ColumnKind, ClusteringOrder, ColumnMask
├── auth/
│   ├── mod.rs
│   ├── role.rs             # RoleMetadata, RoleOption
│   ├── permission.rs       # Permission enum, Resource, GrantEntry
│   ├── password.rs         # PasswordHasher, bcrypt/argon2id, auto-upgrade
│   └── rate_limit.rs       # AuthRateLimiter, exponential backoff, account lockout
├── audit/
│   ├── mod.rs              # AuditEvent, AuditSink trait, CompositeSink, re-exports
│   ├── event.rs            # AuditEvent, AuditEventKind, AuditLogEntry
│   ├── log_sink.rs         # LogAuditSink — structured logging via tracing
│   └── table_sink.rs       # SystemTableAuditSink — in-memory ring buffer for system_auth.audit_log
├── registry.rs             # Schema: thread-safe in-memory registry via ArcSwap
├── system/
│   ├── mod.rs
│   ├── local.rs            # system.local responses
│   ├── peers.rs            # system.peers_v2 responses
│   ├── schema_tables.rs    # system_schema.keyspaces/tables/columns
│   └── auth_tables.rs      # system_auth.roles/role_members/role_permissions
├── secrets/
│   ├── mod.rs              # SecretsProvider trait, SecretsError
│   └── env.rs              # EnvSecretsProvider
├── startup.rs              # DeploymentMode, validate_production_requirements()
└── convert.rs              # TableMetadata → ferrosa_common::TableSchema
```

---

## Core Metadata Types

### KeyspaceMetadata

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyspaceMetadata {
    pub name: String,
    pub durable_writes: bool,
    pub replication: ReplicationParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationParams {
    pub strategy: String,                  // "SimpleStrategy" or "NetworkTopologyStrategy"
    pub options: HashMap<String, String>,  // e.g. {"replication_factor": "3"}
}
```

### TableMetadata

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub keyspace: String,
    pub name: String,
    pub id: Uuid,
    pub columns: IndexMap<String, ColumnMetadata>,  // insertion-ordered
    pub partition_key: Vec<String>,                  // column names, ordered
    pub clustering_key: Vec<(String, ClusteringOrder)>,
    pub params: TableParams,
    pub flags: HashSet<TableFlag>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFlag {
    Compound,
    Counter,
    Dense,
    Super,
}
```

### TableParams

Full Cassandra parameter set with sensible defaults:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableParams {
    pub bloom_filter_fp_chance: f64,           // default: 0.01
    pub caching: CachingParams,                // keys: "ALL", rows_per_partition: "NONE"
    pub comment: String,                       // default: ""
    pub compaction: HashMap<String, String>,    // strategy + options
    pub compression: HashMap<String, String>,  // algorithm + options
    pub crc_check_chance: f64,                 // default: 1.0
    pub default_time_to_live: i32,             // default: 0
    pub gc_grace_seconds: i32,                 // default: 864000 (10 days)
    pub max_index_interval: i32,               // default: 2048
    pub min_index_interval: i32,               // default: 128
    pub memtable_flush_period_in_ms: i32,      // default: 0 (disabled)
    pub speculative_retry: String,             // default: "99PERCENTILE"
    pub additional_write_policy: String,       // default: "99PERCENTILE"
    pub cdc: bool,                             // default: false
    pub read_repair: String,                   // default: "BLOCKING"
    pub allow_auto_snapshot: bool,             // default: true
    pub incremental_backups: bool,             // default: false
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachingParams {
    pub keys: String,                // "ALL" | "NONE"
    pub rows_per_partition: String,  // "NONE" | "ALL" | number
}
```

### ColumnMetadata

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub kind: ColumnKind,
    pub position: i32,                       // 0-based within its kind
    pub column_type: String,                 // CQL type string: "text", "map<text, int>"
    pub clustering_order: ClusteringOrder,
    pub mask: Option<ColumnMask>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnKind {
    PartitionKey,
    Clustering,
    Regular,
    Static,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringOrder {
    Asc,
    Desc,
    None,
}

/// Cassandra 5.x column masking — masks column values for non-privileged roles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMask {
    pub function_name: String,    // e.g. "mask_default", "mask_replace", "mask_inner"
    pub arguments: Vec<String>,   // function arguments
}
```

---

## Authentication and Authorization

### ADR Reference

See [ADR-006: Auth-First Schema Design](../../specs/decisions/006-auth-first-schema.md) and [ADR-007: Configurable Password Hashing](../../specs/decisions/007-configurable-password-hashing.md).

### Role Metadata

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleMetadata {
    pub name: String,
    pub is_superuser: bool,
    pub can_login: bool,
    pub salted_hash: Option<String>,      // "$2b$..." (bcrypt) or "$argon2id$..." (argon2id)
    pub member_of: HashSet<String>,       // parent roles (role hierarchy)
}
```

### Permissions Model

```rust
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    Create,
    Alter,
    Drop,
    Select,
    Modify,
    Authorize,
    Describe,
    Execute,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Resource {
    AllKeyspaces,
    Keyspace(String),
    Table(String, String),                        // (keyspace, table)
    AllRoles,
    Role(String),
    AllFunctions(String),                          // keyspace
    Function(String, String, Vec<String>),          // (keyspace, name, arg_types)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantEntry {
    pub role: String,
    pub resource: Resource,
    pub permissions: HashSet<Permission>,
}
```

### Password Hashing

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PasswordHasher {
    Bcrypt { cost: u32 },                                               // default
    Argon2id { memory_kib: u32, iterations: u32, parallelism: u32 },
}

impl Default for PasswordHasher {
    fn default() -> Self {
        PasswordHasher::Bcrypt { cost: 12 }
    }
}
```

- **Default**: bcrypt with cost 12
- **Configurable**: `FERROSA_AUTH_HASHER=argon2id` switches to argon2id for new hashes
- **Self-describing**: Hash strings embed the algorithm (`$2b$...` or `$argon2id$...`)
- **Auto-upgrade on login**: If configured hasher differs from stored hash algorithm, re-hash on next successful `authenticate()` call
- **Verification**: Auto-detects algorithm from hash prefix, no config needed to verify

### Password Complexity Policy

Configurable password complexity enforcement. In production mode, ISO 27001 compliant minimums are enforced.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordPolicy {
    pub min_length: usize,
    pub require_uppercase: bool,
    pub require_lowercase: bool,
    pub require_digit: bool,
    pub require_special: bool,
    pub reject_username_as_password: bool,
}
```

**Presets:**

```rust
impl PasswordPolicy {
    /// No restrictions — for development/testing only.
    pub fn permissive() -> Self;

    /// ISO 27001 Annex A.9.4.3 compliant minimum:
    /// - 12+ characters
    /// - uppercase, lowercase, digit, special character required
    /// - password != username
    pub fn iso27001() -> Self;
}
```

**ISO 27001 defaults** (used in production mode):

| Rule | Value |
|------|-------|
| Minimum length | 12 |
| Require uppercase | true |
| Require lowercase | true |
| Require digit | true |
| Require special character | true |
| Reject username as password | true |

**Enforcement points:**

- `create_role()` — validates password if provided
- `alter_role()` — validates new password if changed
- Bootstrap — `FERROSA_SUPERUSER_PASSWORD` is validated against the active policy

**Mode integration:**

- `FERROSA_MODE=production`: Uses `PasswordPolicy::iso27001()` as the minimum floor. Custom policy via env vars can only be *stricter*, not weaker.
- `FERROSA_MODE=development`: Uses `PasswordPolicy::permissive()` by default. Custom policy via env vars overrides.

**Configuration:**

| Env Var | Values | Default (dev) | Default (prod) |
|---------|--------|---------------|----------------|
| `FERROSA_AUTH_MIN_PASSWORD_LENGTH` | integer | 1 | 12 |
| `FERROSA_AUTH_REQUIRE_UPPERCASE` | `true`/`false` | false | true |
| `FERROSA_AUTH_REQUIRE_LOWERCASE` | `true`/`false` | false | true |
| `FERROSA_AUTH_REQUIRE_DIGIT` | `true`/`false` | false | true |
| `FERROSA_AUTH_REQUIRE_SPECIAL` | `true`/`false` | false | true |

**Error:**

```rust
PasswordTooWeak {
    violations: Vec<String>,  // e.g. ["must be at least 12 characters", "must contain a digit"]
}
```

### Client Certificate Authentication (Mutual TLS)

When CQL TLS is configured with mutual authentication (ferrosa-cql, future), clients present an X.509 certificate signed by a trusted CA. The schema crate defines the auth model; the CQL crate implements the TLS handshake.

```rust
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    /// Password-only authentication (default).
    Password,
    /// Certificate-only — client certificate CN maps to a Cassandra role.
    Certificate,
    /// Certificate + password — both required (two-factor).
    CertificateAndPassword,
}
```

**Certificate-to-role mapping**: The client certificate's Common Name (CN) or a Subject Alternative Name (SAN) maps to a Cassandra role name. If the role exists and `can_login` is true, authentication succeeds. No password is checked in `Certificate` mode.

**Internode mutual TLS**: All internode connections require mutual TLS in production mode. Both nodes present certificates signed by the cluster CA. The peer's certificate CN must match a known node identity (validated against `system.peers_v2`).

**Configuration** (applied by ferrosa-cql and ferrosa-net, not ferrosa-schema):

| Env Var | Values | Default |
|---------|--------|---------|
| `FERROSA_AUTH_METHOD` | `password`, `certificate`, `certificate_and_password` | `password` |
| `FERROSA_CQL_TLS_CERT` | Path to server certificate PEM | (required for TLS) |
| `FERROSA_CQL_TLS_KEY` | Path to server private key PEM | (required for TLS) |
| `FERROSA_CQL_TLS_CA` | Path to CA certificate PEM for client verification | (required for mutual TLS) |
| `FERROSA_INTERNODE_TLS_CERT` | Path to node certificate PEM | (required for internode TLS) |
| `FERROSA_INTERNODE_TLS_KEY` | Path to node private key PEM | (required for internode TLS) |
| `FERROSA_INTERNODE_TLS_CA` | Path to cluster CA PEM | (required for internode TLS) |

Chunk A defines the `AuthMethod` enum and includes it in `SchemaConfig`. The actual TLS handshake and certificate validation is implemented in ferrosa-cql and ferrosa-net.

### AuthContext

Passed to every mutating registry operation. Superusers bypass permission checks. The `authenticate()` method on `Schema` verifies credentials and returns an `AuthContext`. See Bootstrap section for the full struct definition including `must_change_password`.

### Auth Rate Limiting and Backpressure

The auth module includes a built-in rate limiter to defend against brute-force attacks and CPU-exhaustion DoS (bcrypt/argon2 are intentionally expensive). The rate limiter is keyed by username and tracks failed attempts.

```rust
pub struct AuthRateLimiter {
    /// Failed attempt records keyed by username.
    attempts: Mutex<HashMap<String, FailedAttempts>>,
    config: RateLimitConfig,
}

pub struct FailedAttempts {
    pub count: u32,
    pub first_failure: Instant,
    pub last_failure: Instant,
}

pub struct RateLimitConfig {
    pub max_attempts: u32,            // default: 5
    pub base_backoff: Duration,       // default: 1 second
    pub max_backoff: Duration,        // default: 60 seconds
    pub lockout_duration: Duration,   // default: 15 minutes
    pub window: Duration,             // default: 15 minutes — failures older than this are forgotten
}
```

**Backoff algorithm**: Exponential backoff with jitter. After `n` consecutive failures for a username, `authenticate()` returns `AuthenticationFailed` immediately (without performing password verification) if called within `min(base_backoff * 2^(n-1), max_backoff)` of the last failure. This prevents attackers from consuming CPU with repeated bcrypt/argon2 operations.

**Lockout**: After `max_attempts` failures within `window`, the account is locked for `lockout_duration`. All attempts during lockout return `AuthenticationFailed` immediately. Successful authentication resets the failure counter.

**Backpressure**: The rate limiter is checked *before* password hashing, so locked-out or throttled requests consume negligible CPU.

**Observability**: Each rejection logs at WARN level with username (not password) and failure count. Account lockout events log at ERROR level.

**Configuration**:

| Env Var | Field | Default |
|---------|-------|---------|
| `FERROSA_AUTH_MAX_ATTEMPTS` | `max_attempts` | `5` |
| `FERROSA_AUTH_BASE_BACKOFF_MS` | `base_backoff` | `1000` |
| `FERROSA_AUTH_MAX_BACKOFF_MS` | `max_backoff` | `60000` |
| `FERROSA_AUTH_LOCKOUT_SECS` | `lockout_duration` | `900` (15 min) |
| `FERROSA_AUTH_WINDOW_SECS` | `window` | `900` (15 min) |

Note: The CQL layer (ferrosa-cql, future) may add per-IP rate limiting on top of this per-username limiter, since the schema crate has no concept of source IP.

### Permission Checking

Permission resolution walks the role hierarchy:

1. Check direct grants for the role on the exact resource
1. Check grants on parent resources (table -> keyspace -> all keyspaces)
1. Check inherited grants via `member_of` roles (recursive)
1. Superusers bypass all checks

---

## Audit Logging

### ADR Reference

See [ADR-008: Audit-First Schema Design](../../specs/decisions/008-audit-first-schema.md).

### Design Principles

Audit logging follows the same "baked in from day one" pattern as authentication (ADR-006). Every auth event and every schema mutation emits an `AuditEvent` through a pluggable `AuditSink` trait. The registry holds a sink reference; there is no code path that mutates schema or processes authentication without producing an audit record.

### Event Types

```rust
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub timestamp: SystemTime,
    pub event: AuditEventKind,
    pub actor: Option<String>,        // role name; None for system-initiated (bootstrap)
    pub source: Option<SocketAddr>,   // set by CQL layer if available
    pub schema_version: Option<Uuid>, // snapshot version after mutation (None for auth events)
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize)]
pub enum AuditEventKind {
    // Authentication events
    AuthSuccess { role: String },
    AuthFailed { role: String },
    AuthThrottled { role: String },
    AuthLockedOut { role: String, failure_count: u32 },
    PasswordChanged { role: String, upgraded_algorithm: bool },

    // Keyspace DDL
    KeyspaceCreated { keyspace: String },
    KeyspaceAltered { keyspace: String },
    KeyspaceDropped { keyspace: String },

    // Table DDL
    TableCreated { keyspace: String, table: String },
    TableAltered { keyspace: String, table: String },
    TableDropped { keyspace: String, table: String },

    // Role management
    RoleCreated { role: String, is_superuser: bool },
    RoleAltered { role: String },
    RoleDropped { role: String },

    // Permission changes
    PermissionGranted { role: String, resource: Resource, permissions: HashSet<Permission> },
    PermissionRevoked { role: String, resource: Resource, permissions: HashSet<Permission> },

    // System events
    SchemaBootstrapped,
    SuperuserPasswordMustChange,
}
```

### AuditSink Trait

```rust
pub trait AuditSink: Send + Sync {
    fn emit(&self, event: &AuditEvent);
}
```

The trait is deliberately simple — one method, no error return. Audit emission must not block or fail schema operations. Implementations handle buffering, batching, and error recovery internally.

### Built-in Sinks

**Chunk A provides two sinks:**

```rust
/// Emits audit events as structured JSON via the `tracing` crate at INFO level.
/// Target: "ferrosa::audit" — operators filter/route via tracing-subscriber.
pub struct LogAuditSink;

/// Stores audit events in the in-memory audit log, queryable via
/// system_auth.audit_log. Bounded ring buffer — oldest events evicted
/// when capacity is reached.
pub struct SystemTableAuditSink {
    log: Mutex<VecDeque<AuditLogEntry>>,
    capacity: usize,  // default: 10_000
}
```

**`LogAuditSink`** provides structured JSON logging via `tracing`:

- Routes to stdout, files, or log aggregators (CloudWatch, Datadog, etc.)
- Filter with `RUST_LOG=ferrosa::audit=info`

**`SystemTableAuditSink`** stores events in `system_auth.audit_log`:

```rust
/// Row in system_auth.audit_log, queryable via CQL.
#[derive(Debug, Clone, Serialize)]
pub struct AuditLogEntry {
    pub timestamp: SystemTime,
    pub event_type: String,       // e.g. "AUTH_SUCCESS", "KEYSPACE_CREATED"
    pub actor: Option<String>,    // role name
    pub source: Option<String>,   // IP:port string
    pub resource: Option<String>, // e.g. "keyspace:production", "table:ks.users"
    pub operation: String,        // human-readable description
    pub schema_version: Option<Uuid>,
}

pub fn query_audit_log(sink: &SystemTableAuditSink) -> Vec<AuditLogEntry>;
```

The audit log is an in-memory ring buffer (not persisted to SSTable). This is appropriate for Chunk A — the log survives for the lifetime of the node and is queryable via CQL. Persistence to durable storage (S3 archival) is follow-on work.

**Default configuration**: Both sinks are active — events go to both `tracing` and the in-memory table. A `CompositeSink` wraps multiple sinks:

```rust
pub struct CompositeSink {
    sinks: Vec<Box<dyn AuditSink>>,
}
```

**Configuration:**

| Env Var | Values | Default |
|---------|--------|---------|
| `FERROSA_AUDIT_LOG_CAPACITY` | integer | `10000` |

**Future sinks (not in Chunk A):**

| Sink | Description |
|------|-------------|
| `S3AuditSink` | Batches audit events to S3 as append-only JSON-lines files for compliance archival |

### Registry Integration

The `Schema` struct holds a boxed sink:

```rust
pub struct Schema {
    inner: ArcSwap<SchemaSnapshot>,
    write_lock: Mutex<()>,
    hasher_config: PasswordHasher,
    password_policy: PasswordPolicy,
    rate_limiter: AuthRateLimiter,
    audit_sink: Box<dyn AuditSink>,
}
```

Constructor — see Public API section for the full `SchemaConfig` struct and `Schema::new()` signature.

Every registry method calls `self.audit_sink.emit(...)` after the operation completes (success or failure). For mutations, the event includes the new `schema_version`. For auth events, `schema_version` is `None`.

**Emit points** (every code path that modifies state or checks credentials):

| Method | Event on Success | Event on Failure |
|--------|-----------------|-----------------|
| `authenticate()` | `AuthSuccess` | `AuthFailed`, `AuthThrottled`, or `AuthLockedOut` |
| `create_keyspace()` | `KeyspaceCreated` | (no audit for existence errors — not security-relevant) |
| `alter_keyspace()` | `KeyspaceAltered` | — |
| `drop_keyspace()` | `KeyspaceDropped` | — |
| `create_table()` | `TableCreated` | — |
| `alter_table()` | `TableAltered` | — |
| `drop_table()` | `TableDropped` | — |
| `create_role()` | `RoleCreated` | — |
| `alter_role()` | `RoleAltered`, `PasswordChanged` (if password changed) | — |
| `drop_role()` | `RoleDropped` | — |
| `grant()` | `PermissionGranted` | — |
| `revoke()` | `PermissionRevoked` | — |

**Permission denied** events: When `check_permission()` fails, the calling method (e.g., `create_keyspace`) returns `PermissionDenied` — this is logged at WARN level by the CQL layer, not by the audit sink, to avoid duplication. The sink focuses on state-changing events and auth outcomes.

### Source Address Propagation

The schema crate has no concept of network connections. The `source: Option<SocketAddr>` field in `AuditEvent` is set by the CQL layer before calling registry methods. The registry propagates it through a thread-local or by accepting an optional `AuditContext` parameter:

```rust
/// Optional context for enriching audit events with caller metadata.
/// Set by CQL layer before calling registry methods.
#[derive(Debug, Clone, Default)]
pub struct AuditContext {
    pub source: Option<SocketAddr>,
}
```

Registry methods that accept `&AuthContext` also accept an optional `&AuditContext`. If not provided, `source` defaults to `None` (appropriate for internal/bootstrap operations).

### Testing the Audit System

```rust
/// In-memory audit sink for testing. Collects all emitted events.
pub struct TestAuditSink {
    events: Mutex<Vec<AuditEvent>>,
}

impl TestAuditSink {
    pub fn new() -> Self;
    pub fn events(&self) -> Vec<AuditEvent>;
    pub fn clear(&self);
}
```

Tests assert on the collected events:

- Every `create_*`/`alter_*`/`drop_*` produces exactly one audit event
- `authenticate()` produces `AuthSuccess` or `AuthFailed`
- Rate-limited auth produces `AuthThrottled` or `AuthLockedOut`
- Bootstrap emits `SchemaBootstrapped` and optionally `SuperuserPasswordMustChange`
- Events contain correct actor, timestamp, and schema_version

---

## Schema Registry

### Design

The `Schema` struct is defined in the Audit Logging section above (it holds the `AuditSink` alongside the other components). The snapshot it manages:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub version: Uuid,
    pub keyspaces: HashMap<String, KeyspaceMetadata>,
    pub tables: HashMap<(String, String), TableMetadata>,  // (keyspace, table)
    pub roles: HashMap<String, RoleMetadata>,
    pub grants: HashMap<String, Vec<GrantEntry>>,          // keyed by role name for O(1) lookup
}
```

- **Lock-free reads**: `ArcSwap` provides `Arc<SchemaSnapshot>` without locking
- **Serialized writes**: `write_lock: Mutex<()>` held for duration of clone-modify-swap, preventing lost updates from concurrent mutations (same pattern as `ferrosa-storage::TableStore`)
- **Copy-on-write mutations**: Clone snapshot, apply change, atomic swap
- **Schema version**: New `Uuid` generated on every mutation (drivers poll this)
- **Persistence**: Serializable via serde — Raft persistence (ferrosa-cluster) is follow-on

### Bootstrap

On first startup (`Schema::new()`), a default superuser role is created:

- **Role**: `cassandra`
- **Superuser**: `true`, **Can login**: `true`

The superuser password is determined by the `FERROSA_SUPERUSER_PASSWORD` environment variable:

- **If set**: The provided password is hashed with the configured hasher. This is the recommended production path.
- **If not set**: The password defaults to `cassandra` (matching Cassandra behavior) **and the role is marked as `password_must_change: true`**. The first successful `authenticate()` for this role logs an ERROR-level warning and returns an `AuthContext` with `must_change_password: true`. The CQL layer (future) uses this flag to reject all queries except `ALTER ROLE cassandra WITH PASSWORD = '...'`.

```rust
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub role: String,
    pub is_superuser: bool,
    pub must_change_password: bool,   // true until default password is changed
}
```

This eliminates T08 (default credentials left unchanged) — operators cannot use the system without either setting the env var or changing the password on first login.

### Result Type

```rust
pub type Result<T> = std::result::Result<T, SchemaError>;
```

### Update Types

Chunk A defines the update structs referenced by `alter_*` methods. Full DDL validation is Chunk B; Chunk A applies updates to the snapshot without deep validation beyond auth and existence checks.

```rust
pub struct KeyspaceUpdates {
    pub replication: Option<ReplicationParams>,
    pub durable_writes: Option<bool>,
}

pub struct TableUpdates {
    pub params: Option<TableParams>,
    pub add_columns: Vec<ColumnMetadata>,
    pub drop_columns: Vec<String>,
}

pub struct RoleUpdates {
    pub is_superuser: Option<bool>,
    pub can_login: Option<bool>,
    pub password: Option<String>,       // plaintext — will be hashed by registry
    pub member_of: Option<HashSet<String>>,
}
```

### Public API

```rust
pub struct SchemaConfig {
    pub hasher: PasswordHasher,
    pub password_policy: PasswordPolicy,
    pub auth_method: AuthMethod,
    pub rate_limit: RateLimitConfig,
    pub audit_sink: Box<dyn AuditSink>,
    pub secrets: Box<dyn SecretsProvider>,
    pub mode: DeploymentMode,
}

impl Schema {
    pub fn new(config: SchemaConfig) -> Result<Self>;

    // Lock-free snapshot access
    pub fn snapshot(&self) -> Arc<SchemaSnapshot>;

    // Keyspace operations (auth-gated)
    pub fn create_keyspace(&self, ks: KeyspaceMetadata, auth: &AuthContext) -> Result<()>;
    pub fn alter_keyspace(&self, name: &str, updates: KeyspaceUpdates, auth: &AuthContext) -> Result<()>;
    pub fn drop_keyspace(&self, name: &str, auth: &AuthContext) -> Result<()>;

    // Table operations (auth-gated)
    pub fn create_table(&self, table: TableMetadata, auth: &AuthContext) -> Result<()>;
    pub fn alter_table(&self, ks: &str, table: &str, updates: TableUpdates, auth: &AuthContext) -> Result<()>;
    pub fn drop_table(&self, keyspace: &str, table: &str, auth: &AuthContext) -> Result<()>;

    // Role operations (auth-gated)
    // password is accepted separately to avoid embedding plaintext in RoleMetadata
    pub fn create_role(&self, role: RoleMetadata, password: Option<&str>, auth: &AuthContext) -> Result<()>;
    pub fn alter_role(&self, name: &str, updates: RoleUpdates, auth: &AuthContext) -> Result<()>;
    pub fn drop_role(&self, name: &str, auth: &AuthContext) -> Result<()>;

    // Permission operations (auth-gated)
    pub fn grant(&self, role: &str, resource: &Resource, perms: HashSet<Permission>, auth: &AuthContext) -> Result<()>;
    pub fn revoke(&self, role: &str, resource: &Resource, perms: HashSet<Permission>, auth: &AuthContext) -> Result<()>;

    // Authentication
    pub fn authenticate(&self, username: &str, password: &str) -> Result<AuthContext>;
    pub fn check_permission(&self, auth: &AuthContext, perm: Permission, resource: &Resource) -> Result<()>;
}
```

### Error Types

```rust
#[non_exhaustive]
pub enum SchemaError {
    KeyspaceExists(String),
    KeyspaceNotFound(String),
    TableExists(String, String),
    TableNotFound(String, String),
    RoleExists(String),
    RoleNotFound(String),
    /// Authentication failed. Message intentionally vague to avoid leaking
    /// whether the role exists, can't login, or has a bad password.
    AuthenticationFailed,
    /// Rate limited — too many failed attempts. Returned without performing
    /// password verification to prevent CPU exhaustion.
    AuthenticationThrottled,
    PermissionDenied { role: String, permission: Permission, resource: Resource },
    SystemKeyspaceProtected(String),
    RoleCycleDetected(String),
    PasswordTooWeak { violations: Vec<String> },
    InvalidSchema(String),
}
```

Note: `AuthenticationFailed` is intentionally a single variant — it does not distinguish "role not found" vs. "login disabled" vs. "bad password" to prevent information leakage. Internal logging may include the sub-reason for debugging.

Permission resolution includes cycle detection: if a role hierarchy forms a cycle (A member_of B, B member_of A), the walk terminates and returns `RoleCycleDetected`. The `create_role` and `alter_role` operations also reject changes that would create a cycle.

---

## System Keyspace Responses

### system.local

```rust
pub struct LocalInfo {
    pub key: String,                      // always "local" — primary key expected by drivers
    pub cluster_name: String,
    pub data_center: String,
    pub rack: String,
    pub host_id: Uuid,
    pub broadcast_address: IpAddr,        // address other nodes use to reach this node
    pub broadcast_port: u16,
    pub listen_address: IpAddr,
    pub listen_port: u16,
    pub rpc_address: IpAddr,              // native transport address — drivers use this
    pub rpc_port: u16,                    // native transport port
    pub native_protocol_version: String,  // "5"
    pub partitioner: String,              // "org.apache.cassandra.dht.Murmur3Partitioner"
    pub release_version: String,          // Ferrosa version string
    pub cql_version: String,              // "3.4.7"
    pub schema_version: Uuid,             // from SchemaSnapshot.version
    pub tokens: Vec<String>,
    pub bootstrapped: String,             // "COMPLETED"
}

pub fn query_local(schema: &Schema, node_config: &NodeConfig) -> LocalInfo;
```

The Java driver (DataStax v4) queries `SELECT * FROM system.local WHERE key='local'` and uses `rpc_address` to determine the node's contact address. Both `key` and `rpc_address` are required for driver compatibility.

### system.peers_v2

```rust
pub struct PeerInfo {
    pub peer: IpAddr,
    pub peer_port: u16,
    pub data_center: String,
    pub rack: String,
    pub host_id: Uuid,
    pub preferred_ip: Option<IpAddr>,     // multi-DC routing hint
    pub preferred_port: Option<u16>,
    pub release_version: String,
    pub schema_version: Uuid,
    pub tokens: Vec<String>,
    pub native_address: IpAddr,
    pub native_port: u16,
}

/// Trait for cluster topology — ferrosa-cluster implements this.
/// Single-node stub returns an empty peer list.
pub trait ClusterState: Send + Sync {
    fn peers(&self) -> Vec<PeerInfo>;
}

pub fn query_peers(schema: &Schema, cluster_state: &dyn ClusterState) -> Vec<PeerInfo>;
```

Only `system.peers_v2` is provided. Older drivers that require `system.peers` (v1) are not supported — Ferrosa targets CQL protocol v5 which uses v2. This is documented but not implemented as a fallback.

### system_schema.*

```rust
// system_schema.keyspaces — one row per keyspace
pub fn query_keyspaces(snapshot: &SchemaSnapshot) -> Vec<KeyspaceRow>;

// system_schema.tables — one row per table
pub fn query_tables(snapshot: &SchemaSnapshot) -> Vec<TableRow>;

// system_schema.columns — one row per column
pub fn query_columns(snapshot: &SchemaSnapshot) -> Vec<ColumnRow>;
```

Each `*Row` struct has fields matching the exact Cassandra `system_schema` column names and types for driver compatibility.

### system_auth.*

```rust
// system_auth.roles — one row per role
// auth parameter controls visibility: non-superusers see salted_hash as None
pub fn query_roles(snapshot: &SchemaSnapshot, auth: &AuthContext) -> Vec<RoleRow>;

// system_auth.role_members — derived from RoleMetadata.member_of
pub fn query_role_members(snapshot: &SchemaSnapshot) -> Vec<RoleMemberRow>;

// system_auth.role_permissions — derived from grants
pub fn query_role_permissions(snapshot: &SchemaSnapshot) -> Vec<RolePermissionRow>;

// system_auth.audit_log — audit event history from SystemTableAuditSink
// Requires superuser. Returns most recent events first (ring buffer order).
pub fn query_audit_log(sink: &SystemTableAuditSink, auth: &AuthContext) -> Result<Vec<AuditLogEntry>>;
```

**Hash filtering (T10 mitigation)**: `query_roles()` takes `&AuthContext` and omits the `salted_hash` field for non-superuser callers. The `RoleRow.salted_hash` field is `Option<String>` — superusers see the hash, everyone else sees `None`. This matches Cassandra's behavior where `system_auth.roles` is restricted.

---

## Storage Bridge

`convert.rs` bridges `TableMetadata` to `ferrosa_common::TableSchema`:

```rust
impl TableMetadata {
    /// Convert to the storage engine's schema representation.
    ///
    /// Extracts partition key type, clustering columns, static columns,
    /// and regular columns in the format ferrosa-storage expects.
    pub fn to_storage_schema(&self) -> ferrosa_common::TableSchema;
}
```

This keeps `ferrosa_common::TableSchema` minimal (storage concerns only) while `TableMetadata` carries the full CQL-level schema.

### CQL-to-Marshal Type Mapping

The existing `ferrosa_common::TableSchema` uses Cassandra internal marshal type names (e.g., `"org.apache.cassandra.db.marshal.UTF8Type"`), while `ColumnMetadata` uses CQL type strings (e.g., `"text"`). The `convert.rs` module includes a `cql_to_marshal_type()` function that maps between these representations.

Supported types in Chunk A:

| CQL Type | Marshal Type |
|----------|-------------|
| `text`, `varchar` | `UTF8Type` |
| `int` | `Int32Type` |
| `bigint` | `LongType` |
| `boolean` | `BooleanType` |
| `float` | `FloatType` |
| `double` | `DoubleType` |
| `blob` | `BytesType` |
| `uuid` | `UUIDType` |
| `timeuuid` | `TimeUUIDType` |
| `timestamp` | `TimestampType` |
| `inet` | `InetAddressType` |
| `counter` | `CounterColumnType` |
| `ascii` | `AsciiType` |
| `varint` | `IntegerType` |
| `decimal` | `DecimalType` |
| `set<T>` | `SetType(T)` |
| `list<T>` | `ListType(T)` |
| `map<K, V>` | `MapType(K, V)` |
| `frozen<T>` | `FrozenType(T)` |

For composite partition keys, `to_storage_schema()` synthesizes a `CompositeType(...)` marshal string from the component column types. UDT type mapping is deferred to Chunk C.

---

## Secrets Management

### ADR Reference

See [ADR-009: Pluggable Secrets Provider](../../specs/decisions/009-pluggable-secrets-provider.md).

### Design Principles

Ferrosa needs secrets at startup (S3 credentials, superuser password) and at runtime (password hashing config). Hard-coding these as environment variables works for development but is inadequate for production — env vars leak through `/proc`, container inspection, and crash dumps.

The `SecretsProvider` trait abstracts secret retrieval behind a pluggable interface. Chunk A ships with an `EnvSecretsProvider` (current behavior) and defines the trait so that AWS Secrets Manager, HashiCorp Vault, and other backends can be added without changing the core.

### SecretsProvider Trait

```rust
/// Retrieves secret values by key. Implementations handle caching,
/// rotation, and backend-specific authentication internally.
pub trait SecretsProvider: Send + Sync {
    /// Retrieve a secret value by key. Returns None if the key doesn't exist.
    fn get_secret(&self, key: &str) -> Result<Option<String>, SecretsError>;
}

#[derive(Debug)]
pub enum SecretsError {
    /// Backend unavailable (network, auth, permissions)
    ProviderUnavailable(String),
    /// Key exists but access denied
    AccessDenied(String),
    /// Other backend-specific errors
    Other(String),
}
```

### Built-in Providers

**Chunk A provides one provider:**

```rust
/// Reads secrets from environment variables. Keys are uppercased and
/// prefixed with FERROSA_ (e.g., key "s3.secret_key" → FERROSA_S3_SECRET_ACCESS_KEY).
pub struct EnvSecretsProvider;
```

This preserves backward compatibility — existing env-var-based configuration works unchanged.

**Future providers (not in Chunk A):**

| Provider | Description |
|----------|-------------|
| `AwsSecretsManagerProvider` | Reads from AWS Secrets Manager. Secret ARN configured via `FERROSA_SECRETS_ARN`. Caches values with configurable TTL for rotation support. |
| `VaultSecretsProvider` | Reads from HashiCorp Vault. Vault address and auth method configured via `FERROSA_VAULT_ADDR`, `FERROSA_VAULT_TOKEN` or Kubernetes auth. |
| `FileSecretsProvider` | Reads from mounted secret files (Kubernetes secrets volume). Path configured via `FERROSA_SECRETS_DIR`. |

### Secret Keys

The following secrets are retrievable via the provider:

| Key | Used By | Env Var Equivalent |
|-----|---------|-------------------|
| `superuser_password` | Bootstrap | `FERROSA_SUPERUSER_PASSWORD` |
| `s3.access_key_id` | S3 upload | `FERROSA_S3_ACCESS_KEY_ID` |
| `s3.secret_access_key` | S3 upload | `FERROSA_S3_SECRET_ACCESS_KEY` |

The provider is consulted at startup and on-demand for rotation-aware backends. `SchemaConfig.secrets` (see Public API) holds the provider. It is used during `Schema::new()` for bootstrap password retrieval and stored for future rotation support.

### Configuration

| Env Var | Values | Default |
|---------|--------|---------|
| `FERROSA_SECRETS_PROVIDER` | `env`, `aws-secrets-manager`, `vault`, `file` | `env` |

When set to `env`, the `EnvSecretsProvider` is used (backward compatible). Other values select the corresponding provider, which reads its own configuration from a minimal set of env vars (just enough to bootstrap the connection to the secrets backend).

---

## Production Mode

### ADR Reference

See [ADR-010: Production Mode — Mandatory Encryption](../../specs/decisions/010-production-mode.md).

### Design

Ferrosa supports a `FERROSA_MODE` environment variable that controls security enforcement:

| Mode | Behavior |
|------|----------|
| `development` (default) | Permissive — allows plaintext S3 (`FERROSA_S3_ALLOW_HTTP`), unencrypted local disk, no TLS on CQL/internode. Logs warnings for each relaxation. |
| `production` | Strict — refuses to start unless all encryption requirements are met. |

**Production mode enforces at startup:**

1. **CQL TLS required**: CQL listener must have TLS configured (certificate + key). Refuses to bind without TLS.
1. **CQL mutual TLS required**: Client certificate authentication must be enabled. Clients must present a valid certificate signed by a trusted CA. This provides two-factor auth (certificate + password) or certificate-only auth depending on configuration.
1. **Internode mutual TLS required**: Internode protocol must have mutual TLS configured. Both sides present certificates signed by the cluster CA. Refuses to start cluster communication without mutual TLS.
1. **S3 HTTPS only**: `FERROSA_S3_ALLOW_HTTP=true` is rejected. S3 endpoint must use HTTPS.
1. **Encrypted local storage**: Startup check verifies data directory is on an encrypted filesystem. Refuses to start if not detected (with override `FERROSA_ALLOW_UNENCRYPTED_DISK=true` for environments where encryption is handled at a layer Ferrosa can't detect, e.g., hardware encryption).
1. **Secrets provider not `env`**: In production mode, `FERROSA_SECRETS_PROVIDER=env` logs a WARNING (not a hard block, since some locked-down environments manage env vars securely).
1. **Default superuser password forbidden**: `FERROSA_SUPERUSER_PASSWORD` must be set or the node refuses to start. No `cassandra`/`cassandra` default in production mode.
1. **Password policy floor**: Custom password policy cannot be weaker than `PasswordPolicy::iso27001()`. If any configured threshold is below the ISO 27001 minimum, the node refuses to start.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    Development,
    Production,
}

impl DeploymentMode {
    pub fn from_env() -> Self;  // reads FERROSA_MODE
}

/// Validates all production-mode invariants. Returns a list of violations.
/// Called at startup before binding any listeners.
pub fn validate_production_requirements(
    config: &SchemaConfig,
    node_config: &NodeConfig,
) -> Vec<ProductionViolation>;

#[non_exhaustive]
pub enum ProductionViolation {
    CqlTlsNotConfigured,
    CqlMutualTlsNotConfigured,     // client certificate auth required in production
    InternodeTlsNotConfigured,
    InternodeMutualTlsNotConfigured, // internode mutual TLS required in production
    S3HttpEnabled,
    UnencryptedLocalStorage { path: PathBuf },
    DefaultSuperuserPassword,
    EnvSecretsInProduction,
    PasswordPolicyBelowMinimum,     // custom policy weaker than ISO 27001 floor
}
```

**Startup behavior:**

- In `Production` mode: if `validate_production_requirements()` returns any violations, log each at ERROR level and exit with a non-zero status code. The node does not start.
- In `Development` mode: same checks run but violations are logged at WARN level. The node starts anyway.

### Configuration

| Env Var | Values | Default |
|---------|--------|---------|
| `FERROSA_MODE` | `development`, `production` | `development` |
| `FERROSA_ALLOW_UNENCRYPTED_DISK` | `true`, `false` | `false` |

### Scope

The `DeploymentMode` enum and `validate_production_requirements()` function live in `ferrosa-schema` (since it already handles bootstrap and startup configuration). The actual TLS configuration lives in `ferrosa-cql` and `ferrosa-net` — production mode validation checks that those configs are present, not that it configures TLS itself.

For Chunk A, the validation covers: S3 HTTP, local disk encryption, default superuser password, secrets provider, and password policy floor. CQL TLS, CQL mutual TLS, internode TLS, and internode mutual TLS checks are added when ferrosa-cql and ferrosa-net are implemented — the `ProductionViolation` enum is `#[non_exhaustive]` so variants can be added without breaking changes.

---

## NodeConfig

Shared configuration for system keyspace responses:

```rust
pub struct NodeConfig {
    pub cluster_name: String,
    pub data_center: String,
    pub rack: String,
    pub host_id: Uuid,
    pub listen_address: IpAddr,
    pub listen_port: u16,
    pub broadcast_address: IpAddr,  // address other nodes use to reach this node
    pub broadcast_port: u16,
    pub rpc_address: IpAddr,        // native transport (CQL) address — used by drivers
    pub rpc_port: u16,              // native transport (CQL) port
    pub tokens: Vec<String>,
}
```

Populated from environment variables following the 12-factor config pattern established in ferrosa-storage:

| Env Var | Field | Default |
|---------|-------|---------|
| `FERROSA_CLUSTER_NAME` | `cluster_name` | `"ferrosa"` |
| `FERROSA_DATA_CENTER` | `data_center` | `"dc1"` |
| `FERROSA_RACK` | `rack` | `"rack1"` |
| `FERROSA_LISTEN_ADDRESS` | `listen_address` | `127.0.0.1` |
| `FERROSA_LISTEN_PORT` | `listen_port` | `7000` |
| `FERROSA_BROADCAST_ADDRESS` | `broadcast_address` | listen_address |
| `FERROSA_BROADCAST_PORT` | `broadcast_port` | listen_port |
| `FERROSA_RPC_ADDRESS` | `rpc_address` | `0.0.0.0` |
| `FERROSA_RPC_PORT` | `rpc_port` | `9042` |

`host_id` and `tokens` are generated/assigned at startup, not configured via environment.

---

## Concurrency Model

| Operation | Mechanism |
|-----------|-----------|
| Read snapshot | Lock-free via `ArcSwap::load()` — returns `Arc<SchemaSnapshot>` |
| Mutate schema | Clone snapshot, apply change, `ArcSwap::store()` — writers serialize via internal `Mutex` |
| Authenticate (verify only) | Read-only snapshot access + bcrypt/argon2 verify (CPU-bound), no lock needed |
| Authenticate (auto-upgrade) | Acquires `write_lock` only when hash algorithm differs from config — re-hash and swap |

Writers are serialized via `write_lock` (only one schema mutation at a time) but readers never block. `authenticate()` takes the write lock only when a hash upgrade is needed, keeping the common path (verify only) lock-free. This matches Cassandra's behavior — schema changes are rare and serialized through Raft.

### System Keyspace Protection

System keyspaces (`system`, `system_schema`, `system_auth`) are protected from user modification. Any `create_table`, `alter_table`, or `drop_table` targeting a system keyspace returns `SchemaError::SystemKeyspaceProtected`. System keyspace contents are managed internally by the registry.

---

## Testing Strategy

### Unit Tests

- `metadata/` — construction, defaults, serialization round-trip, equality
- `auth/password.rs` — bcrypt hash/verify, argon2id hash/verify, auto-detect from prefix, auto-upgrade
- `auth/rate_limit.rs` — backoff calculation, lockout logic, window expiry, counter reset on success
- `auth/permission.rs` — permission checking with hierarchy, superuser bypass, resource inheritance
- `registry.rs` — CRUD operations, auth gating, version bumps, error cases (duplicates, not found, permission denied), bootstrap with `FERROSA_SUPERUSER_PASSWORD`
- `audit/event.rs` — event serialization, all variants constructible
- `auth/password.rs` — password policy validation: length, character classes, username rejection, ISO 27001 preset
- `audit/log_sink.rs` — `LogAuditSink` emits valid structured JSON via tracing
- `audit/table_sink.rs` — `SystemTableAuditSink` ring buffer capacity, eviction, query
- `secrets/env.rs` — `EnvSecretsProvider` reads correct env vars, returns `None` for missing keys
- `startup.rs` — production mode rejects S3 HTTP, default password, unencrypted disk; development mode warns
- `convert.rs` — `TableMetadata::to_storage_schema()` matches expected `TableSchema` output

### Integration Tests

- Full workflow: create role -> authenticate -> create keyspace -> create table -> query system_schema -> verify
- Permission denied: non-superuser can't create keyspace without grant
- Role hierarchy: grants inherited via `member_of`
- System keyspace correctness: `system.local` fields match node config, `system_schema.*` reflects current state
- Bootstrap: default superuser with `FERROSA_SUPERUSER_PASSWORD` set uses env password; without it, `must_change_password` is true
- Rate limiting: repeated failed auth triggers backoff; successful auth resets counter; lockout blocks all attempts
- Hash filtering: non-superuser `query_roles()` returns `None` for `salted_hash`; superuser sees real hash
- Audit completeness: every registry mutation and auth call emits exactly one audit event via `TestAuditSink`
- Audit content: events contain correct actor, timestamp, schema_version, and event-specific fields
- Production mode: validation rejects missing TLS, HTTP S3, default password, weak password policy; development mode warns only
- Secrets provider: `EnvSecretsProvider` returns correct values; unknown keys return `None`
- Password policy: weak passwords rejected by ISO 27001 policy; permissive policy allows anything; production mode rejects policy below floor
- Audit log table: `SystemTableAuditSink` stores events, `query_audit_log()` returns them, ring buffer evicts oldest when full

### Property Tests

- Round-trip: `SchemaSnapshot` -> serde_json -> `SchemaSnapshot` preserves all fields
- Permission resolution: superuser always authorized, no-grant role always denied on non-public resources
- Hash verification: hash(password) always verifies against same password, never against different password

---

## Follow-on Work (Chunks B-F)

| Chunk | What It Adds |
|-------|-------------|
| B | DDL validation — type checking, naming rules, constraint enforcement, all auth-gated |
| C | UDTs — `UserType`, type resolution, `system_schema.types` |
| D | Indexes + Views — `IndexMetadata`, `ViewMetadata`, `system_schema.indexes/views` |
| E | Functions + Aggregates + Triggers — UDFs, UDAs, `system_schema.functions/aggregates/triggers` |
| F | Traces + Distributed — `system_traces.sessions/events`, `system_distributed.repair_history/view_build_status` |
