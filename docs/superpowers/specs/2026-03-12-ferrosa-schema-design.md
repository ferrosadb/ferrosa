# ferrosa-schema Design Specification

> Date: 2026-03-12
> Status: Draft
> Chunk: A (of A–F)

## Goal

Build the schema management crate for Ferrosa: rich metadata types, a thread-safe schema registry, authentication and authorization with column masking, and system keyspace responses that let CQL drivers connect and discover schema.

## Architecture

`ferrosa-schema` is a single crate with layered modules. It depends only on `ferrosa-common`. It is the authority for what keyspaces, tables, columns, and roles exist. Every mutating operation requires an `AuthContext` — auth is baked in from day one, not retrofitted.

The crate produces typed structs for system keyspace queries. The CQL layer (ferrosa-cql, future) handles wire serialization.

## Tech Stack

- `ferrosa-common` — shared types (`TableSchema`, `ColumnDefinition`, `Token`)
- `arc-swap` — lock-free snapshot reads (same pattern as ferrosa-storage)
- `uuid` — table IDs, schema versions, host IDs
- `bcrypt` — default password hashing
- `argon2` — optional stronger password hashing
- `serde`, `serde_json` — serialization for persistence
- `indexmap` — insertion-ordered column maps

## Chunk Breakdown

| Chunk | Scope |
|-------|-------|
| **A (this spec)** | Core types + Schema registry + Auth (roles, permissions, column masking, `system_auth`) + system keyspaces (`system.local`, `system.peers_v2`, `system_schema.keyspaces/tables/columns`) |
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
│   └── password.rs         # PasswordHasher, bcrypt/argon2id, auto-upgrade
├── registry.rs             # Schema: thread-safe in-memory registry via ArcSwap
├── system/
│   ├── mod.rs
│   ├── local.rs            # system.local responses
│   ├── peers.rs            # system.peers_v2 responses
│   ├── schema_tables.rs    # system_schema.keyspaces/tables/columns
│   └── auth_tables.rs      # system_auth.roles/role_members/role_permissions
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

### AuthContext

```rust
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub role: String,
    pub is_superuser: bool,
}
```

Passed to every mutating registry operation. Superusers bypass permission checks. The `authenticate()` method on `Schema` verifies credentials and returns an `AuthContext`.

### Permission Checking

Permission resolution walks the role hierarchy:

1. Check direct grants for the role on the exact resource
1. Check grants on parent resources (table -> keyspace -> all keyspaces)
1. Check inherited grants via `member_of` roles (recursive)
1. Superusers bypass all checks

---

## Schema Registry

### Design

```rust
pub struct Schema {
    inner: ArcSwap<SchemaSnapshot>,
    write_lock: Mutex<()>,             // serializes all mutations (clone-modify-swap)
    hasher_config: PasswordHasher,
}

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
- **Password**: `cassandra` (hashed with the configured hasher)
- **Superuser**: `true`, **Can login**: `true`

This matches Cassandra's behavior. Operators should change this password immediately after first boot. The default credentials are logged as a warning at startup.

### Public API

```rust
impl Schema {
    pub fn new(hasher_config: PasswordHasher) -> Self;

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
    pub fn create_role(&self, role: RoleMetadata, auth: &AuthContext) -> Result<()>;
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
    PermissionDenied { role: String, permission: Permission, resource: Resource },
    SystemKeyspaceProtected(String),
    RoleCycleDetected(String),
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
pub fn query_roles(snapshot: &SchemaSnapshot) -> Vec<RoleRow>;

// system_auth.role_members — derived from RoleMetadata.member_of
pub fn query_role_members(snapshot: &SchemaSnapshot) -> Vec<RoleMemberRow>;

// system_auth.role_permissions — derived from grants
pub fn query_role_permissions(snapshot: &SchemaSnapshot) -> Vec<RolePermissionRow>;
```

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

Populated from environment variables (`FERROSA_CLUSTER_NAME`, `FERROSA_DATA_CENTER`, etc.) following the 12-factor config pattern established in ferrosa-storage.

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
- `auth/permission.rs` — permission checking with hierarchy, superuser bypass, resource inheritance
- `registry.rs` — CRUD operations, auth gating, version bumps, error cases (duplicates, not found, permission denied)
- `convert.rs` — `TableMetadata::to_storage_schema()` matches expected `TableSchema` output

### Integration Tests

- Full workflow: create role -> authenticate -> create keyspace -> create table -> query system_schema -> verify
- Permission denied: non-superuser can't create keyspace without grant
- Role hierarchy: grants inherited via `member_of`
- System keyspace correctness: `system.local` fields match node config, `system_schema.*` reflects current state

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
