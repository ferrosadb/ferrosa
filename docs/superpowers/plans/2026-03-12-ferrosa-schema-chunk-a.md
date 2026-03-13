# ferrosa-schema Chunk A Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the schema management crate: rich metadata types, thread-safe registry with ArcSwap, authentication/authorization, audit logging, secrets management, production mode, and system keyspace responses for CQL driver compatibility.

**Architecture:** Bottom-up build: error types → metadata types → auth → audit → secrets → registry → system keyspaces → storage bridge. Every layer independently testable. Auth-first (ADR-006): every mutation requires AuthContext. Audit-first (ADR-008): every mutation emits audit event. TDD throughout.

**Tech Stack:** Rust, `arc-swap` (lock-free reads), `bcrypt`/`argon2` (password hashing), `uuid`, `serde`/`serde_json`, `indexmap` (ordered columns), `tracing` (audit logging), `proptest` (property tests)

**Spec:** `docs/superpowers/specs/2026-03-12-ferrosa-schema-design.md`

---

## File Structure

### New files

| File | Purpose |
|------|---------|
| `ferrosa-schema/Cargo.toml` | Crate manifest with dependencies |
| `ferrosa-schema/src/lib.rs` | Public API, module declarations, re-exports |
| `ferrosa-schema/src/error.rs` | `SchemaError` enum, `Result` type alias |
| `ferrosa-schema/src/metadata/mod.rs` | Module declarations, re-exports |
| `ferrosa-schema/src/metadata/keyspace.rs` | `KeyspaceMetadata`, `ReplicationParams` |
| `ferrosa-schema/src/metadata/table.rs` | `TableMetadata`, `TableParams`, `CachingParams`, `TableFlag` |
| `ferrosa-schema/src/metadata/column.rs` | `ColumnMetadata`, `ColumnKind`, `ClusteringOrder`, `ColumnMask` |
| `ferrosa-schema/src/auth/mod.rs` | Module declarations, re-exports |
| `ferrosa-schema/src/auth/role.rs` | `RoleMetadata`, `AuthContext`, `RoleUpdates` |
| `ferrosa-schema/src/auth/permission.rs` | `Permission`, `Resource`, `GrantEntry`, permission checking |
| `ferrosa-schema/src/auth/password.rs` | `PasswordHasher`, `PasswordPolicy`, hash/verify/validate |
| `ferrosa-schema/src/auth/rate_limit.rs` | `AuthRateLimiter`, `RateLimitConfig`, `FailedAttempts` |
| `ferrosa-schema/src/audit/mod.rs` | Module declarations, `AuditSink` trait, `CompositeSink`, re-exports |
| `ferrosa-schema/src/audit/event.rs` | `AuditEvent`, `AuditEventKind`, `AuditContext`, `AuditLogEntry` |
| `ferrosa-schema/src/audit/log_sink.rs` | `LogAuditSink` — structured JSON via tracing |
| `ferrosa-schema/src/audit/table_sink.rs` | `SystemTableAuditSink` — in-memory ring buffer |
| `ferrosa-schema/src/registry.rs` | `Schema`, `SchemaSnapshot`, `SchemaConfig`, bootstrap, CRUD |
| `ferrosa-schema/src/system/mod.rs` | Module declarations, re-exports |
| `ferrosa-schema/src/system/local.rs` | `LocalInfo`, `NodeConfig`, `query_local` |
| `ferrosa-schema/src/system/peers.rs` | `PeerInfo`, `ClusterState` trait, `query_peers` |
| `ferrosa-schema/src/system/schema_tables.rs` | `query_keyspaces`, `query_tables`, `query_columns` |
| `ferrosa-schema/src/system/auth_tables.rs` | `query_roles`, `query_role_members`, `query_role_permissions` |
| `ferrosa-schema/src/secrets/mod.rs` | `SecretsProvider` trait, `SecretsError` |
| `ferrosa-schema/src/secrets/env.rs` | `EnvSecretsProvider` |
| `ferrosa-schema/src/startup.rs` | `DeploymentMode`, `ProductionViolation`, `validate_production_requirements` |
| `ferrosa-schema/src/convert.rs` | `cql_to_marshal_type`, `TableMetadata::to_storage_schema` |
| `ferrosa-schema/tests/integration.rs` | Full workflow integration tests |
| `ferrosa-schema/tests/auth_integration.rs` | Auth-focused integration tests |
| `ferrosa-schema/tests/property_tests.rs` | Property tests (round-trip, permission, hash) |

### Modified files

| File | Changes |
|------|---------|
| `Cargo.toml` (workspace root) | Add `ferrosa-schema` to workspace members |

---

## Chunk 1: Scaffolding + Error Types + Core Metadata

### Task 1: Crate Scaffolding

**Files:**

- Create: `ferrosa-schema/Cargo.toml`
- Create: `ferrosa-schema/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "ferrosa-schema"
description = "Schema management for Ferrosa: metadata, auth, audit, system keyspaces"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
arc-swap = "1.7"
bcrypt = "0.16"
argon2 = "0.5"
uuid = { version = "1", features = ["v4", "serde"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
indexmap = { version = "2", features = ["serde"] }
tracing = "0.1"
password-hash = "0.5"

[dev-dependencies]
proptest = "1"
tracing-subscriber = { version = "0.3", features = ["json"] }
```

- [ ] **Step 2: Create minimal lib.rs**

```rust
//! Schema management for the Ferrosa distributed database.
//!
//! This crate is the authority for keyspaces, tables, columns, and roles.
//! Every mutating operation requires an `AuthContext` (ADR-006).
//! Every mutation emits an audit event (ADR-008).

pub mod error;

pub use error::{SchemaError, Result};
```

- [ ] **Step 3: Add to workspace members**

Add `"ferrosa-schema"` to the `members` array in the root `Cargo.toml`.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p ferrosa-schema`
Expected: Compilation succeeds (error module doesn't exist yet, so create a stub).

- [ ] **Step 5: Commit**

```
feat(schema): scaffold ferrosa-schema crate
```

---

### Task 2: SchemaError + Result Type

**Files:**

- Create: `ferrosa-schema/src/error.rs`
- Test: `ferrosa-schema/src/error.rs` (inline tests)

- [ ] **Step 1: Write failing test for SchemaError Display**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_exists_display() {
        let err = SchemaError::KeyspaceExists("ks1".to_string());
        assert!(err.to_string().contains("ks1"));
    }

    #[test]
    fn authentication_failed_is_vague() {
        let err = SchemaError::AuthenticationFailed;
        let msg = err.to_string();
        // Must NOT reveal whether role exists, can't login, or bad password
        assert!(!msg.contains("not found"));
        assert!(!msg.contains("login"));
        assert!(!msg.contains("password"));
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<SchemaError>();
        assert_sync::<SchemaError>();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `SchemaError` not defined.

- [ ] **Step 3: Implement SchemaError**

```rust
//! Error types for ferrosa-schema.

use std::collections::HashSet;
use std::fmt;

/// Errors from schema operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum SchemaError {
    KeyspaceExists(String),
    KeyspaceNotFound(String),
    TableExists(String, String),
    TableNotFound(String, String),
    RoleExists(String),
    RoleNotFound(String),
    /// Intentionally vague — does not reveal whether role exists,
    /// can't login, or has a bad password.
    AuthenticationFailed,
    /// Rate limited — too many failed attempts.
    AuthenticationThrottled,
    PermissionDenied {
        role: String,
        permission: String,  // String initially; refactored to typed Permission/Resource in Task 7
        resource: String,
    },
    SystemKeyspaceProtected(String),
    RoleCycleDetected(String),
    PasswordTooWeak { violations: Vec<String> },
    InvalidSchema(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyspaceExists(name) => write!(f, "keyspace already exists: {name}"),
            Self::KeyspaceNotFound(name) => write!(f, "keyspace not found: {name}"),
            Self::TableExists(ks, t) => write!(f, "table already exists: {ks}.{t}"),
            Self::TableNotFound(ks, t) => write!(f, "table not found: {ks}.{t}"),
            Self::RoleExists(name) => write!(f, "role already exists: {name}"),
            Self::RoleNotFound(name) => write!(f, "role not found: {name}"),
            Self::AuthenticationFailed => write!(f, "authentication failed"),
            Self::AuthenticationThrottled => write!(f, "too many failed attempts, try again later"),
            Self::PermissionDenied { role, permission, resource } => {
                write!(f, "permission denied: {role} lacks {permission} on {resource}")
            }
            Self::SystemKeyspaceProtected(name) => {
                write!(f, "cannot modify system keyspace: {name}")
            }
            Self::RoleCycleDetected(name) => write!(f, "role cycle detected involving: {name}"),
            Self::PasswordTooWeak { violations } => {
                write!(f, "password too weak: {}", violations.join(", "))
            }
            Self::InvalidSchema(msg) => write!(f, "invalid schema: {msg}"),
        }
    }
}

impl std::error::Error for SchemaError {}

/// Result type alias for ferrosa-schema.
pub type Result<T> = std::result::Result<T, SchemaError>;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-schema`
Expected: All 3 tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add SchemaError enum and Result type alias
```

---

### Task 3: KeyspaceMetadata + ReplicationParams

**Files:**

- Create: `ferrosa-schema/src/metadata/mod.rs`
- Create: `ferrosa-schema/src/metadata/keyspace.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/metadata/keyspace.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_metadata_construction() {
        let ks = KeyspaceMetadata {
            name: "my_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "3".to_string())]),
            },
        };
        assert_eq!(ks.name, "my_ks");
        assert!(ks.durable_writes);
        assert_eq!(ks.replication.strategy, "SimpleStrategy");
    }

    #[test]
    fn keyspace_metadata_serde_roundtrip() {
        let ks = KeyspaceMetadata {
            name: "test".to_string(),
            durable_writes: false,
            replication: ReplicationParams {
                strategy: "NetworkTopologyStrategy".to_string(),
                options: HashMap::from([("dc1".to_string(), "3".to_string())]),
            },
        };
        let json = serde_json::to_string(&ks).unwrap();
        let back: KeyspaceMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(ks, back);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement KeyspaceMetadata and ReplicationParams**

In `metadata/keyspace.rs`:

```rust
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyspaceMetadata {
    pub name: String,
    pub durable_writes: bool,
    pub replication: ReplicationParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationParams {
    pub strategy: String,
    pub options: HashMap<String, String>,
}
```

In `metadata/mod.rs`:

```rust
pub mod keyspace;

pub use keyspace::{KeyspaceMetadata, ReplicationParams};
```

Update `lib.rs` to add `pub mod metadata;` and re-exports.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add KeyspaceMetadata and ReplicationParams
```

---

### Task 4: ColumnMetadata + ColumnKind + ClusteringOrder + ColumnMask

**Files:**

- Create: `ferrosa-schema/src/metadata/column.rs`
- Modify: `ferrosa-schema/src/metadata/mod.rs`
- Test: `ferrosa-schema/src/metadata/column.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_metadata_with_mask() {
        let col = ColumnMetadata {
            name: "ssn".to_string(),
            kind: ColumnKind::Regular,
            position: 0,
            column_type: "text".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: Some(ColumnMask {
                function_name: "mask_default".to_string(),
                arguments: vec![],
            }),
        };
        assert!(col.mask.is_some());
    }

    #[test]
    fn column_kind_variants() {
        assert_ne!(ColumnKind::PartitionKey, ColumnKind::Clustering);
        assert_ne!(ColumnKind::Regular, ColumnKind::Static);
    }

    #[test]
    fn column_metadata_serde_roundtrip() {
        let col = ColumnMetadata {
            name: "age".to_string(),
            kind: ColumnKind::Regular,
            position: 2,
            column_type: "int".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        };
        let json = serde_json::to_string(&col).unwrap();
        let back: ColumnMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(col, back);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement column types**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub kind: ColumnKind,
    pub position: i32,
    pub column_type: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMask {
    pub function_name: String,
    pub arguments: Vec<String>,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add ColumnMetadata, ColumnKind, ClusteringOrder, ColumnMask
```

---

### Task 5: TableParams + CachingParams + Default

**Files:**

- Create: `ferrosa-schema/src/metadata/table.rs`
- Modify: `ferrosa-schema/src/metadata/mod.rs`
- Test: `ferrosa-schema/src/metadata/table.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_params_defaults() {
        let params = TableParams::default();
        assert!((params.bloom_filter_fp_chance - 0.01).abs() < f64::EPSILON);
        assert_eq!(params.gc_grace_seconds, 864_000);
        assert_eq!(params.default_time_to_live, 0);
        assert!(!params.cdc);
    }

    #[test]
    fn caching_params_defaults() {
        let params = CachingParams::default();
        assert_eq!(params.keys, "ALL");
        assert_eq!(params.rows_per_partition, "NONE");
    }

    #[test]
    fn table_params_serde_roundtrip() {
        let params = TableParams::default();
        let json = serde_json::to_string(&params).unwrap();
        let back: TableParams = serde_json::from_str(&json).unwrap();
        assert!((back.bloom_filter_fp_chance - 0.01).abs() < f64::EPSILON);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement TableParams, CachingParams, TableFlag, TableMetadata**

Implement the full `TableParams` struct with `Default` impl matching the Cassandra defaults from the spec. Implement `CachingParams` with `Default`. Implement `TableFlag` enum. Implement `TableMetadata` struct with `IndexMap<String, ColumnMetadata>` for columns.

Key types from spec:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFlag {
    Compound,
    Counter,
    Dense,
    Super,
}
```

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub keyspace: String,
    pub name: String,
    pub id: Uuid,
    pub columns: IndexMap<String, ColumnMetadata>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<(String, ClusteringOrder)>,
    pub params: TableParams,
    pub flags: HashSet<TableFlag>,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add TableMetadata, TableParams, CachingParams, TableFlag
```

---

## Chunk 2: Auth Types + Password Hashing + Password Policy

### Task 6: RoleMetadata + AuthContext + Update Types

**Files:**

- Create: `ferrosa-schema/src/auth/mod.rs`
- Create: `ferrosa-schema/src/auth/role.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/auth/role.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_metadata_construction() {
        let role = RoleMetadata {
            name: "admin".to_string(),
            is_superuser: true,
            can_login: true,
            salted_hash: Some("$2b$12$hash".to_string()),
            member_of: HashSet::new(),
        };
        assert!(role.is_superuser);
        assert!(role.can_login);
    }

    #[test]
    fn auth_context_superuser() {
        let ctx = AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        assert!(ctx.is_superuser);
        assert!(!ctx.must_change_password);
    }

    #[test]
    fn role_updates_optional_fields() {
        let updates = RoleUpdates {
            is_superuser: None,
            can_login: Some(false),
            password: None,
            member_of: None,
        };
        assert!(updates.is_superuser.is_none());
        assert_eq!(updates.can_login, Some(false));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement RoleMetadata, AuthContext, update types**

Implement `RoleMetadata`, `AuthContext`, `KeyspaceUpdates`, `TableUpdates`, `RoleUpdates` as defined in the spec. Place update types alongside the types they modify (KeyspaceUpdates in keyspace.rs, TableUpdates in table.rs, RoleUpdates in role.rs).

**Implementation notes:**

- `RoleUpdates` must derive/implement `Default` (all fields `None`) — tests use `..Default::default()`
- Modifies `metadata/keyspace.rs` (add `KeyspaceUpdates`) and `metadata/table.rs` (add `TableUpdates`) in addition to `auth/role.rs`

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add RoleMetadata, AuthContext, update types
```

---

### Task 7: Permission + Resource + GrantEntry

**Files:**

- Create: `ferrosa-schema/src/auth/permission.rs`
- Modify: `ferrosa-schema/src/auth/mod.rs`
- Test: `ferrosa-schema/src/auth/permission.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_variants_are_distinct() {
        assert_ne!(Permission::Select, Permission::Modify);
        assert_ne!(Permission::Create, Permission::Drop);
    }

    #[test]
    fn resource_table_includes_keyspace() {
        let r = Resource::Table("ks".to_string(), "users".to_string());
        match r {
            Resource::Table(ks, t) => {
                assert_eq!(ks, "ks");
                assert_eq!(t, "users");
            }
            _ => panic!("expected Table"),
        }
    }

    #[test]
    fn grant_entry_serde_roundtrip() {
        let grant = GrantEntry {
            role: "admin".to_string(),
            resource: Resource::Keyspace("ks1".to_string()),
            permissions: HashSet::from([Permission::Select, Permission::Modify]),
        };
        let json = serde_json::to_string(&grant).unwrap();
        let back: GrantEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(grant.role, back.role);
        assert_eq!(grant.permissions.len(), 2);
    }

    #[test]
    fn resource_display_for_error_messages() {
        let r = Resource::Table("ks".to_string(), "t".to_string());
        let s = r.to_string();
        assert!(s.contains("ks"));
        assert!(s.contains("t"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement Permission, Resource, GrantEntry**

Implement as defined in the spec. Add `Display` impl for `Resource` and `Permission` (needed for `SchemaError::PermissionDenied` messages).

**Implementation notes:**

- Both `Permission` and `Resource` must be `#[non_exhaustive]` per the spec
- After this task, refactor `SchemaError::PermissionDenied` to use typed `Permission` and `Resource` instead of `String` (update the error Display impl to use their Display impls)

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add Permission, Resource, GrantEntry types
```

---

### Task 8: Password Hashing — Bcrypt + Argon2id

**Files:**

- Create: `ferrosa-schema/src/auth/password.rs`
- Modify: `ferrosa-schema/src/auth/mod.rs`
- Test: `ferrosa-schema/src/auth/password.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bcrypt_hash_and_verify() {
        let hasher = PasswordHasher::default();
        let hash = hasher.hash_password("secret123").unwrap();
        assert!(hash.starts_with("$2b$"));
        assert!(hasher.verify_password("secret123", &hash).unwrap());
    }

    #[test]
    fn bcrypt_wrong_password_fails() {
        let hasher = PasswordHasher::default();
        let hash = hasher.hash_password("correct").unwrap();
        assert!(!PasswordHasher::verify_password_any("wrong", &hash).unwrap());
    }

    #[test]
    fn argon2id_hash_and_verify() {
        let hasher = PasswordHasher::Argon2id {
            memory_kib: 19456,
            iterations: 2,
            parallelism: 1,
        };
        let hash = hasher.hash_password("secret123").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(PasswordHasher::verify_password_any("secret123", &hash).unwrap());
    }

    #[test]
    fn auto_detect_algorithm_from_prefix() {
        let bcrypt_hash = PasswordHasher::default().hash_password("pw").unwrap();
        assert!(PasswordHasher::verify_password_any("pw", &bcrypt_hash).unwrap());

        let argon_hasher = PasswordHasher::Argon2id {
            memory_kib: 19456, iterations: 2, parallelism: 1,
        };
        let argon_hash = argon_hasher.hash_password("pw").unwrap();
        assert!(PasswordHasher::verify_password_any("pw", &argon_hash).unwrap());
    }

    #[test]
    fn needs_upgrade_detects_algorithm_mismatch() {
        let bcrypt_hash = PasswordHasher::default().hash_password("pw").unwrap();
        let argon_config = PasswordHasher::Argon2id {
            memory_kib: 19456, iterations: 2, parallelism: 1,
        };
        assert!(argon_config.needs_rehash(&bcrypt_hash));
        assert!(!PasswordHasher::default().needs_rehash(&bcrypt_hash));
    }

    #[test]
    fn default_hasher_is_bcrypt_12() {
        let hasher = PasswordHasher::default();
        match hasher {
            PasswordHasher::Bcrypt { cost } => assert_eq!(cost, 12),
            _ => panic!("expected bcrypt"),
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `PasswordHasher` not defined.

- [ ] **Step 3: Implement PasswordHasher**

Implement `PasswordHasher` enum with `Bcrypt { cost }` and `Argon2id { memory_kib, iterations, parallelism }` variants. Implement:

- `hash_password(&self, password: &str) -> Result<String>` — hashes using the configured algorithm
- `verify_password_any(password: &str, hash: &str) -> Result<bool>` — auto-detects algorithm from `$2b$` or `$argon2id$` prefix
- `needs_rehash(&self, hash: &str) -> bool` — returns true if hash algorithm doesn't match configured algorithm
- `Default` impl returning `Bcrypt { cost: 12 }`

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add PasswordHasher with bcrypt and argon2id support
```

---

### Task 9: PasswordPolicy + Validation

**Files:**

- Modify: `ferrosa-schema/src/auth/password.rs`
- Test: `ferrosa-schema/src/auth/password.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn permissive_policy_accepts_anything() {
    let policy = PasswordPolicy::permissive();
    assert!(policy.validate("a", "user1").is_ok());
}

#[test]
fn iso27001_rejects_short_password() {
    let policy = PasswordPolicy::iso27001();
    let result = policy.validate("short", "user1");
    assert!(result.is_err());
    if let Err(SchemaError::PasswordTooWeak { violations }) = result {
        assert!(violations.iter().any(|v| v.contains("12 characters")));
    }
}

#[test]
fn iso27001_rejects_missing_uppercase() {
    let policy = PasswordPolicy::iso27001();
    let result = policy.validate("abcdefghij1!", "user1");
    assert!(result.is_err());
}

#[test]
fn iso27001_rejects_password_matching_username() {
    let policy = PasswordPolicy::iso27001();
    let result = policy.validate("ValidP@ss123", "ValidP@ss123");
    assert!(result.is_err());
}

#[test]
fn iso27001_accepts_strong_password() {
    let policy = PasswordPolicy::iso27001();
    assert!(policy.validate("Str0ng!P@sswd", "admin").is_ok());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `PasswordPolicy` not defined.

- [ ] **Step 3: Implement PasswordPolicy**

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

impl PasswordPolicy {
    pub fn permissive() -> Self { /* all false, min_length: 1 */ }
    pub fn iso27001() -> Self { /* all true, min_length: 12 */ }
    pub fn validate(&self, password: &str, username: &str) -> Result<()> { /* check each rule */ }
    pub fn is_at_least_as_strong_as(&self, other: &PasswordPolicy) -> bool { /* floor check */ }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add PasswordPolicy with ISO 27001 and permissive presets
```

---

### Task 10: Auth Rate Limiting

**Files:**

- Create: `ferrosa-schema/src/auth/rate_limit.rs`
- Modify: `ferrosa-schema/src/auth/mod.rs`
- Test: `ferrosa-schema/src/auth/rate_limit.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> RateLimitConfig {
        RateLimitConfig {
            max_attempts: 3,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            lockout_duration: Duration::from_secs(2),
            window: Duration::from_secs(10),
        }
    }

    #[test]
    fn first_attempt_is_allowed() {
        let limiter = AuthRateLimiter::new(test_config());
        assert!(limiter.check_rate_limit("user1").is_ok());
    }

    #[test]
    fn successful_auth_resets_counter() {
        let limiter = AuthRateLimiter::new(test_config());
        limiter.record_failure("user1");
        limiter.record_failure("user1");
        limiter.record_success("user1");
        assert!(limiter.check_rate_limit("user1").is_ok());
    }

    #[test]
    fn lockout_after_max_attempts() {
        let limiter = AuthRateLimiter::new(test_config());
        for _ in 0..3 {
            limiter.record_failure("user1");
        }
        assert!(limiter.check_rate_limit("user1").is_err());
    }

    #[test]
    fn default_config_values() {
        let config = RateLimitConfig::default();
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.base_backoff, Duration::from_secs(1));
        assert_eq!(config.max_backoff, Duration::from_secs(60));
        assert_eq!(config.lockout_duration, Duration::from_secs(900));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement AuthRateLimiter**

Implement `RateLimitConfig` (with `Default`), `FailedAttempts`, and `AuthRateLimiter` with:

- `new(config: RateLimitConfig) -> Self`
- `check_rate_limit(&self, username: &str) -> Result<()>` — returns `AuthenticationThrottled` if backoff/lockout active
- `record_failure(&self, username: &str)` — increments failure count
- `record_success(&self, username: &str)` — clears failure record

Backoff algorithm: `min(base_backoff * 2^(n-1), max_backoff)`. Lockout after `max_attempts` within `window`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add AuthRateLimiter with exponential backoff and lockout
```

---

## Chunk 3: Audit + Secrets + Production Mode

### Task 11: Audit Event Types

**Files:**

- Create: `ferrosa-schema/src/audit/mod.rs`
- Create: `ferrosa-schema/src/audit/event.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/audit/event.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_event_construction() {
        let event = AuditEvent {
            timestamp: SystemTime::now(),
            event: AuditEventKind::AuthSuccess { role: "admin".to_string() },
            actor: Some("admin".to_string()),
            source: None,
            schema_version: None,
        };
        assert!(event.actor.is_some());
        assert!(event.source.is_none());
    }

    #[test]
    fn audit_event_kind_all_variants_constructible() {
        // Ensure every variant can be constructed
        let _ = AuditEventKind::AuthSuccess { role: "r".into() };
        let _ = AuditEventKind::AuthFailed { role: "r".into() };
        let _ = AuditEventKind::AuthThrottled { role: "r".into() };
        let _ = AuditEventKind::AuthLockedOut { role: "r".into(), failure_count: 5 };
        let _ = AuditEventKind::PasswordChanged { role: "r".into(), upgraded_algorithm: false };
        let _ = AuditEventKind::KeyspaceCreated { keyspace: "ks".into() };
        let _ = AuditEventKind::KeyspaceAltered { keyspace: "ks".into() };
        let _ = AuditEventKind::KeyspaceDropped { keyspace: "ks".into() };
        let _ = AuditEventKind::TableCreated { keyspace: "ks".into(), table: "t".into() };
        let _ = AuditEventKind::TableAltered { keyspace: "ks".into(), table: "t".into() };
        let _ = AuditEventKind::TableDropped { keyspace: "ks".into(), table: "t".into() };
        let _ = AuditEventKind::RoleCreated { role: "r".into(), is_superuser: false };
        let _ = AuditEventKind::RoleAltered { role: "r".into() };
        let _ = AuditEventKind::RoleDropped { role: "r".into() };
        let _ = AuditEventKind::PermissionGranted {
            role: "r".into(),
            resource: Resource::AllKeyspaces,
            permissions: HashSet::from([Permission::Select]),
        };
        let _ = AuditEventKind::PermissionRevoked {
            role: "r".into(),
            resource: Resource::AllKeyspaces,
            permissions: HashSet::from([Permission::Select]),
        };
        let _ = AuditEventKind::SchemaBootstrapped;
        let _ = AuditEventKind::SuperuserPasswordMustChange;
    }

    #[test]
    fn audit_event_serializes_to_json() {
        let event = AuditEvent {
            timestamp: SystemTime::UNIX_EPOCH,
            event: AuditEventKind::KeyspaceCreated { keyspace: "ks1".to_string() },
            actor: Some("admin".to_string()),
            source: None,
            schema_version: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("KeyspaceCreated"));
        assert!(json.contains("ks1"));
    }

    #[test]
    fn audit_context_default_is_empty() {
        let ctx = AuditContext::default();
        assert!(ctx.source.is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement AuditEvent, AuditEventKind, AuditContext, AuditLogEntry**

Implement all types as defined in the spec. Include `PermissionGranted`/`PermissionRevoked` variants (which reference `Resource` and `Permission` from the auth module — use `HashSet<Permission>`).

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add AuditEvent, AuditEventKind, AuditContext types
```

---

### Task 12: AuditSink Trait + TestAuditSink + CompositeSink

**Files:**

- Modify: `ferrosa-schema/src/audit/mod.rs`
- Test: `ferrosa-schema/src/audit/mod.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_sink_collects_events() {
        let sink = TestAuditSink::new();
        let event = AuditEvent {
            timestamp: SystemTime::now(),
            event: AuditEventKind::SchemaBootstrapped,
            actor: None,
            source: None,
            schema_version: None,
        };
        sink.emit(&event);
        assert_eq!(sink.events().len(), 1);
    }

    #[test]
    fn test_audit_sink_clear() {
        let sink = TestAuditSink::new();
        let event = AuditEvent {
            timestamp: SystemTime::now(),
            event: AuditEventKind::SchemaBootstrapped,
            actor: None,
            source: None,
            schema_version: None,
        };
        sink.emit(&event);
        sink.clear();
        assert!(sink.events().is_empty());
    }

    #[test]
    fn composite_sink_fans_out() {
        let sink1 = Arc::new(TestAuditSink::new());
        let sink2 = Arc::new(TestAuditSink::new());
        let composite = CompositeSink::new(vec![sink1.clone(), sink2.clone()]);
        let event = AuditEvent {
            timestamp: SystemTime::now(),
            event: AuditEventKind::SchemaBootstrapped,
            actor: None,
            source: None,
            schema_version: None,
        };
        composite.emit(&event);
        assert_eq!(sink1.events().len(), 1);
        assert_eq!(sink2.events().len(), 1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `AuditSink` trait not defined.

- [ ] **Step 3: Implement AuditSink trait, TestAuditSink, CompositeSink**

```rust
pub trait AuditSink: Send + Sync {
    fn emit(&self, event: &AuditEvent);
}

pub struct TestAuditSink {
    events: Mutex<Vec<AuditEvent>>,
}

pub struct CompositeSink {
    sinks: Vec<Arc<dyn AuditSink>>,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add AuditSink trait, TestAuditSink, CompositeSink
```

---

### Task 13: LogAuditSink + SystemTableAuditSink

**Files:**

- Create: `ferrosa-schema/src/audit/log_sink.rs`
- Create: `ferrosa-schema/src/audit/table_sink.rs`
- Modify: `ferrosa-schema/src/audit/mod.rs`
- Test: inline tests in each file

- [ ] **Step 1: Write failing tests for SystemTableAuditSink**

```rust
// In table_sink.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_stores_events() {
        let sink = SystemTableAuditSink::new(100);
        let event = AuditEvent {
            timestamp: SystemTime::now(),
            event: AuditEventKind::SchemaBootstrapped,
            actor: None, source: None, schema_version: None,
        };
        sink.emit(&event);
        let entries = sink.query();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event_type, "SCHEMA_BOOTSTRAPPED");
    }

    #[test]
    fn ring_buffer_evicts_oldest_when_full() {
        let sink = SystemTableAuditSink::new(2);
        for i in 0..3 {
            let event = AuditEvent {
                timestamp: SystemTime::now(),
                event: AuditEventKind::KeyspaceCreated { keyspace: format!("ks{i}") },
                actor: None, source: None, schema_version: None,
            };
            sink.emit(&event);
        }
        let entries = sink.query();
        assert_eq!(entries.len(), 2);
        // Oldest (ks0) evicted, ks1 and ks2 remain
        assert!(entries.iter().any(|e| e.resource.as_deref() == Some("keyspace:ks2")));
    }

    #[test]
    fn query_returns_newest_first() {
        let sink = SystemTableAuditSink::new(100);
        for i in 0..3 {
            let event = AuditEvent {
                timestamp: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(i),
                event: AuditEventKind::KeyspaceCreated { keyspace: format!("ks{i}") },
                actor: None, source: None, schema_version: None,
            };
            sink.emit(&event);
        }
        let entries = sink.query();
        // Newest first
        assert!(entries[0].timestamp >= entries[1].timestamp);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement LogAuditSink and SystemTableAuditSink**

`LogAuditSink`: Emits events as structured JSON via `tracing::info!` with target `"ferrosa::audit"`.

`SystemTableAuditSink`: Bounded `VecDeque<AuditLogEntry>`, converts `AuditEvent` to `AuditLogEntry` on emit. `query()` returns entries in newest-first order.

```rust
pub struct AuditLogEntry {
    pub timestamp: SystemTime,
    pub event_type: String,
    pub actor: Option<String>,
    pub source: Option<String>,
    pub resource: Option<String>,
    pub operation: String,
    pub schema_version: Option<Uuid>,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add LogAuditSink and SystemTableAuditSink
```

---

### Task 14: SecretsProvider + EnvSecretsProvider

**Files:**

- Create: `ferrosa-schema/src/secrets/mod.rs`
- Create: `ferrosa-schema/src/secrets/env.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/secrets/env.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_provider_reads_existing_var() {
        std::env::set_var("FERROSA_SUPERUSER_PASSWORD", "test_pw_123");
        let provider = EnvSecretsProvider;
        let result = provider.get_secret("superuser_password").unwrap();
        assert_eq!(result.as_deref(), Some("test_pw_123"));
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }

    #[test]
    fn env_provider_returns_none_for_missing() {
        std::env::remove_var("FERROSA_NONEXISTENT_KEY");
        let provider = EnvSecretsProvider;
        let result = provider.get_secret("nonexistent_key").unwrap();
        assert!(result.is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `SecretsProvider` not defined.

- [ ] **Step 3: Implement SecretsProvider trait, SecretsError, EnvSecretsProvider**

```rust
// secrets/mod.rs
pub trait SecretsProvider: Send + Sync {
    fn get_secret(&self, key: &str) -> std::result::Result<Option<String>, SecretsError>;
}

#[derive(Debug)]
pub enum SecretsError {
    ProviderUnavailable(String),
    AccessDenied(String),
    Other(String),
}

// secrets/env.rs
pub struct EnvSecretsProvider;
```

`EnvSecretsProvider` maps keys to env vars: `superuser_password` → `FERROSA_SUPERUSER_PASSWORD`, `s3.access_key_id` → `FERROSA_S3_ACCESS_KEY_ID`, etc.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add SecretsProvider trait and EnvSecretsProvider
```

---

### Task 15: DeploymentMode + ProductionViolation + Validation

**Files:**

- Create: `ferrosa-schema/src/startup.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/startup.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn development_mode_is_default() {
        // Unset the env var to get default
        std::env::remove_var("FERROSA_MODE");
        assert_eq!(DeploymentMode::from_env(), DeploymentMode::Development);
    }

    #[test]
    fn production_mode_from_env() {
        std::env::set_var("FERROSA_MODE", "production");
        assert_eq!(DeploymentMode::from_env(), DeploymentMode::Production);
        std::env::remove_var("FERROSA_MODE");
    }

    #[test]
    fn production_rejects_default_superuser_password() {
        // Build a SchemaConfig with no superuser password set
        // validate_production_requirements should return DefaultSuperuserPassword
        // (exact test depends on SchemaConfig, which is Task 17)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement DeploymentMode and ProductionViolation**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    Development,
    Production,
}

impl DeploymentMode {
    pub fn from_env() -> Self {
        match std::env::var("FERROSA_MODE").as_deref() {
            Ok("production") => Self::Production,
            _ => Self::Development,
        }
    }
}

#[non_exhaustive]
#[derive(Debug)]
pub enum ProductionViolation {
    CqlTlsNotConfigured,
    CqlMutualTlsNotConfigured,
    InternodeTlsNotConfigured,
    InternodeMutualTlsNotConfigured,
    S3HttpEnabled,
    UnencryptedLocalStorage { path: std::path::PathBuf },
    DefaultSuperuserPassword,
    EnvSecretsInProduction,
    PasswordPolicyBelowMinimum,
}
```

Implement `validate_production_requirements()` as a standalone function. For Chunk A, it checks: S3 HTTP, default superuser password, secrets provider type, password policy floor. CQL/internode TLS checks are stubs returning empty (added when those crates land).

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add DeploymentMode, ProductionViolation, startup validation
```

---

## Chunk 4: Schema Registry

### Task 16: SchemaSnapshot + SchemaConfig

**Files:**

- Create: `ferrosa-schema/src/registry.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/registry.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_snapshot_is_empty_on_construction() {
        let snap = SchemaSnapshot::new();
        assert!(snap.keyspaces.is_empty());
        assert!(snap.tables.is_empty());
        assert!(snap.roles.is_empty());
        assert!(snap.grants.is_empty());
    }

    #[test]
    fn schema_snapshot_serde_roundtrip() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert("ks1".to_string(), KeyspaceMetadata {
            name: "ks1".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        });
        let json = serde_json::to_string(&snap).unwrap();
        let back: SchemaSnapshot = serde_json::from_str(&json).unwrap();
        assert!(back.keyspaces.contains_key("ks1"));
    }

    #[test]
    fn schema_config_default_builds() {
        let config = SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        };
        assert_eq!(config.mode, DeploymentMode::Development);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement SchemaSnapshot, SchemaConfig, AuthMethod**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub version: Uuid,
    pub keyspaces: HashMap<String, KeyspaceMetadata>,
    pub tables: HashMap<(String, String), TableMetadata>,
    pub roles: HashMap<String, RoleMetadata>,
    pub grants: HashMap<String, Vec<GrantEntry>>,
}

pub struct SchemaConfig {
    pub hasher: PasswordHasher,
    pub password_policy: PasswordPolicy,
    pub auth_method: AuthMethod,
    pub rate_limit: RateLimitConfig,
    pub audit_sink: Box<dyn AuditSink>,
    pub secrets: Box<dyn SecretsProvider>,
    pub mode: DeploymentMode,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    Password,
    Certificate,
    CertificateAndPassword,
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add SchemaSnapshot, SchemaConfig, AuthMethod
```

---

### Task 17: Schema Struct + Bootstrap

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`
- Test: `ferrosa-schema/src/registry.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn schema_new_creates_default_superuser() {
    let schema = test_schema();
    let snap = schema.snapshot();
    assert!(snap.roles.contains_key("cassandra"));
    let role = &snap.roles["cassandra"];
    assert!(role.is_superuser);
    assert!(role.can_login);
}

#[test]
fn schema_new_with_env_password() {
    std::env::set_var("FERROSA_SUPERUSER_PASSWORD", "TestP@ssw0rd!");
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    };
    let schema = Schema::new(config).unwrap();
    let snap = schema.snapshot();
    let role = &snap.roles["cassandra"];
    // With env password set, salted_hash should be present
    assert!(role.salted_hash.is_some());
    std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
}

#[test]
fn schema_new_without_env_password_marks_must_change() {
    std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    };
    let schema = Schema::new(config).unwrap();
    // Default password "cassandra" used, authenticate should set must_change_password
    let ctx = schema.authenticate("cassandra", "cassandra").unwrap();
    assert!(ctx.must_change_password);
}

#[test]
fn schema_snapshot_returns_arc() {
    let schema = test_schema();
    let snap1 = schema.snapshot();
    let snap2 = schema.snapshot();
    // Both point to same version
    assert_eq!(snap1.version, snap2.version);
}

#[test]
fn schema_bootstrap_emits_audit_event() {
    let sink = Arc::new(TestAuditSink::new());
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(AuditSinkRef(sink.clone())),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    };
    let _ = Schema::new(config).unwrap();
    let events = sink.events();
    assert!(events.iter().any(|e| matches!(e.event, AuditEventKind::SchemaBootstrapped)));
}

// Helper used across all registry tests
fn test_schema() -> Schema {
    std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    };
    Schema::new(config).unwrap()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `Schema` struct not defined.

- [ ] **Step 3: Implement Schema struct with bootstrap**

```rust
pub struct Schema {
    inner: ArcSwap<SchemaSnapshot>,
    write_lock: Mutex<()>,
    hasher_config: PasswordHasher,
    password_policy: PasswordPolicy,
    rate_limiter: AuthRateLimiter,
    audit_sink: Box<dyn AuditSink>,
}

impl Schema {
    pub fn new(config: SchemaConfig) -> Result<Self> { /* bootstrap superuser */ }
    pub fn snapshot(&self) -> Arc<SchemaSnapshot> { /* ArcSwap::load() */ }
}
```

Bootstrap creates the `cassandra` superuser role. If `secrets.get_secret("superuser_password")` returns a password, hash it and use it. Otherwise, use the default `cassandra` password and mark `password_must_change: true` internally (tracked via a field in the snapshot or a separate flag).

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add Schema struct with bootstrap and lock-free snapshot
```

---

### Task 18: Authentication (authenticate + rate limiter + audit)

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`
- Test: `ferrosa-schema/src/registry.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn authenticate_valid_credentials() {
    std::env::set_var("FERROSA_SUPERUSER_PASSWORD", "TestP@ss1!");
    let schema = test_schema_with_password("TestP@ss1!");
    let ctx = schema.authenticate("cassandra", "TestP@ss1!").unwrap();
    assert_eq!(ctx.role, "cassandra");
    assert!(ctx.is_superuser);
    std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
}

#[test]
fn authenticate_wrong_password() {
    let schema = test_schema();
    let result = schema.authenticate("cassandra", "wrong");
    assert!(matches!(result, Err(SchemaError::AuthenticationFailed)));
}

#[test]
fn authenticate_nonexistent_role() {
    let schema = test_schema();
    let result = schema.authenticate("nobody", "pass");
    assert!(matches!(result, Err(SchemaError::AuthenticationFailed)));
}

#[test]
fn authenticate_emits_success_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    sink.clear(); // clear bootstrap events
    let _ = schema.authenticate("cassandra", "cassandra");
    let events = sink.events();
    assert!(events.iter().any(|e| matches!(&e.event, AuditEventKind::AuthSuccess { role } if role == "cassandra")));
}

#[test]
fn authenticate_emits_failed_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    sink.clear();
    let _ = schema.authenticate("cassandra", "wrong");
    let events = sink.events();
    assert!(events.iter().any(|e| matches!(&e.event, AuditEventKind::AuthFailed { .. })));
}

#[test]
fn authenticate_rate_limited_after_failures() {
    let schema = test_schema_with_rate_limit(RateLimitConfig {
        max_attempts: 2,
        base_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
        lockout_duration: Duration::from_secs(5),
        window: Duration::from_secs(60),
    });
    // Fail twice
    let _ = schema.authenticate("cassandra", "wrong");
    let _ = schema.authenticate("cassandra", "wrong");
    // Third attempt should be throttled
    let result = schema.authenticate("cassandra", "cassandra");
    assert!(matches!(result, Err(SchemaError::AuthenticationThrottled)));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `authenticate` not defined.

- [ ] **Step 3: Implement authenticate()**

```rust
impl Schema {
    pub fn authenticate(&self, username: &str, password: &str) -> Result<AuthContext> {
        // 1. Check rate limiter BEFORE hashing
        self.rate_limiter.check_rate_limit(username)?;

        // 2. Look up role
        let snap = self.snapshot();
        let role = snap.roles.get(username);

        // 3. Verify password (constant-time-ish: always hash even if role missing)
        let (verified, role_data) = match role {
            Some(r) if r.can_login => {
                match &r.salted_hash {
                    Some(hash) => (PasswordHasher::verify_password_any(password, hash)
                        .unwrap_or(false), Some(r)),
                    None => (false, Some(r)),
                }
            }
            _ => {
                // Hash anyway to prevent timing side-channel on role existence
                let _ = self.hasher_config.hash_password(password);
                (false, None)
            }
        };

        if !verified {
            self.rate_limiter.record_failure(username);
            self.audit_sink.emit(&AuditEvent::auth_failed(username));
            return Err(SchemaError::AuthenticationFailed);
        }

        self.rate_limiter.record_success(username);

        // 4. Auto-upgrade hash if algorithm differs
        // (acquires write_lock only when needed)

        // 5. Build AuthContext
        let role_data = role_data.unwrap();
        let must_change = /* check password_must_change flag */;

        self.audit_sink.emit(&AuditEvent::auth_success(username));

        Ok(AuthContext {
            role: username.to_string(),
            is_superuser: role_data.is_superuser,
            must_change_password: must_change,
        })
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add authenticate() with rate limiting and audit
```

---

### Task 19: Permission Checking

**Files:**

- Modify: `ferrosa-schema/src/auth/permission.rs`
- Test: `ferrosa-schema/src/auth/permission.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn superuser_bypasses_all_checks() {
    let snap = snapshot_with_role("admin", true);
    let auth = AuthContext { role: "admin".to_string(), is_superuser: true, must_change_password: false };
    assert!(check_permission(&snap, &auth, Permission::Create, &Resource::AllKeyspaces).is_ok());
}

#[test]
fn no_grant_means_denied() {
    let snap = snapshot_with_role("reader", false);
    let auth = AuthContext { role: "reader".to_string(), is_superuser: false, must_change_password: false };
    let result = check_permission(&snap, &auth, Permission::Create, &Resource::Keyspace("ks".into()));
    assert!(matches!(result, Err(SchemaError::PermissionDenied { .. })));
}

#[test]
fn direct_grant_allows_access() {
    let mut snap = snapshot_with_role("writer", false);
    snap.grants.insert("writer".to_string(), vec![GrantEntry {
        role: "writer".to_string(),
        resource: Resource::Keyspace("ks1".to_string()),
        permissions: HashSet::from([Permission::Modify]),
    }]);
    let auth = AuthContext { role: "writer".to_string(), is_superuser: false, must_change_password: false };
    assert!(check_permission(&snap, &auth, Permission::Modify, &Resource::Keyspace("ks1".into())).is_ok());
}

#[test]
fn table_inherits_keyspace_grant() {
    let mut snap = snapshot_with_role("ks_admin", false);
    snap.grants.insert("ks_admin".to_string(), vec![GrantEntry {
        role: "ks_admin".to_string(),
        resource: Resource::Keyspace("ks1".to_string()),
        permissions: HashSet::from([Permission::Select]),
    }]);
    let auth = AuthContext { role: "ks_admin".to_string(), is_superuser: false, must_change_password: false };
    // Table("ks1", "t1") should inherit from Keyspace("ks1")
    assert!(check_permission(&snap, &auth, Permission::Select, &Resource::Table("ks1".into(), "t1".into())).is_ok());
}

#[test]
fn role_hierarchy_grants_inherited() {
    let mut snap = SchemaSnapshot::new();
    // parent role has the grant
    snap.roles.insert("parent".to_string(), RoleMetadata {
        name: "parent".to_string(), is_superuser: false, can_login: false,
        salted_hash: None, member_of: HashSet::new(),
    });
    snap.roles.insert("child".to_string(), RoleMetadata {
        name: "child".to_string(), is_superuser: false, can_login: true,
        salted_hash: None, member_of: HashSet::from(["parent".to_string()]),
    });
    snap.grants.insert("parent".to_string(), vec![GrantEntry {
        role: "parent".to_string(),
        resource: Resource::AllKeyspaces,
        permissions: HashSet::from([Permission::Select]),
    }]);
    let auth = AuthContext { role: "child".to_string(), is_superuser: false, must_change_password: false };
    assert!(check_permission(&snap, &auth, Permission::Select, &Resource::Keyspace("any".into())).is_ok());
}

#[test]
fn cycle_detection_prevents_infinite_loop() {
    let mut snap = SchemaSnapshot::new();
    snap.roles.insert("a".into(), RoleMetadata {
        name: "a".into(), is_superuser: false, can_login: true,
        salted_hash: None, member_of: HashSet::from(["b".into()]),
    });
    snap.roles.insert("b".into(), RoleMetadata {
        name: "b".into(), is_superuser: false, can_login: false,
        salted_hash: None, member_of: HashSet::from(["a".into()]),
    });
    let auth = AuthContext { role: "a".to_string(), is_superuser: false, must_change_password: false };
    // Should not hang — returns PermissionDenied (no matching grant found)
    let result = check_permission(&snap, &auth, Permission::Create, &Resource::AllKeyspaces);
    assert!(result.is_err());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `check_permission` not defined.

- [ ] **Step 3: Implement check_permission**

Implement as a free function `pub fn check_permission(snap: &SchemaSnapshot, auth: &AuthContext, perm: Permission, resource: &Resource) -> Result<()>` for testability. Also add a `Schema::check_permission(&self, auth, perm, resource)` method in `registry.rs` that loads the snapshot and delegates to the free function — this matches the spec's public API.

Resolution order:

1. Superuser bypass
2. Direct grants on exact resource
3. Grants on parent resources (Table → Keyspace → AllKeyspaces)
4. Inherited grants via `member_of` (recursive with visited set for cycle detection)

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add permission checking with hierarchy and cycle detection
```

---

## Chunk 5: Registry CRUD Operations

### Task 20: Keyspace CRUD (create/alter/drop)

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`
- Test: `ferrosa-schema/src/registry.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn create_keyspace_as_superuser() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let ks = KeyspaceMetadata {
        name: "my_ks".to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
        },
    };
    schema.create_keyspace(ks, &auth).unwrap();
    assert!(schema.snapshot().keyspaces.contains_key("my_ks"));
}

#[test]
fn create_keyspace_duplicate_fails() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let ks = test_keyspace("dup");
    schema.create_keyspace(ks.clone(), &auth).unwrap();
    let result = schema.create_keyspace(ks, &auth);
    assert!(matches!(result, Err(SchemaError::KeyspaceExists(_))));
}

#[test]
fn create_keyspace_bumps_version() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let v1 = schema.snapshot().version;
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    let v2 = schema.snapshot().version;
    assert_ne!(v1, v2);
}

#[test]
fn create_keyspace_emits_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    let auth = superuser_auth(&schema);
    sink.clear();
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    let events = sink.events();
    assert!(events.iter().any(|e| matches!(&e.event, AuditEventKind::KeyspaceCreated { keyspace } if keyspace == "ks1")));
}

#[test]
fn drop_keyspace_removes_it() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    schema.drop_keyspace("ks1", &auth).unwrap();
    assert!(!schema.snapshot().keyspaces.contains_key("ks1"));
}

#[test]
fn drop_keyspace_not_found() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let result = schema.drop_keyspace("nope", &auth);
    assert!(matches!(result, Err(SchemaError::KeyspaceNotFound(_))));
}

#[test]
fn alter_keyspace_updates_replication() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    let updates = KeyspaceUpdates {
        replication: Some(ReplicationParams {
            strategy: "NetworkTopologyStrategy".to_string(),
            options: HashMap::from([("dc1".to_string(), "3".to_string())]),
        }),
        durable_writes: None,
    };
    schema.alter_keyspace("ks1", updates, &auth).unwrap();
    let snap = schema.snapshot();
    assert_eq!(snap.keyspaces["ks1"].replication.strategy, "NetworkTopologyStrategy");
}

#[test]
fn system_keyspace_protected() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let result = schema.drop_keyspace("system", &auth);
    assert!(matches!(result, Err(SchemaError::SystemKeyspaceProtected(_))));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `create_keyspace` not defined.

- [ ] **Step 3: Implement keyspace CRUD**

Pattern for each mutating method:

1. Check auth (`check_permission` on snapshot)
2. Acquire `write_lock`
3. Clone snapshot
4. Apply mutation (check for duplicates, not-found, system protection)
5. Generate new version UUID
6. `ArcSwap::store()` the new snapshot
7. Emit audit event
8. Return `Ok(())`

System keyspaces to protect: `system`, `system_schema`, `system_auth`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add keyspace CRUD with auth, audit, system protection
```

---

### Task 21: Table CRUD (create/alter/drop)

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`
- Test: `ferrosa-schema/src/registry.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn create_table_in_existing_keyspace() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    let table = test_table("ks1", "users");
    schema.create_table(table, &auth).unwrap();
    assert!(schema.snapshot().tables.contains_key(&("ks1".to_string(), "users".to_string())));
}

#[test]
fn create_table_in_nonexistent_keyspace_fails() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let table = test_table("nope", "users");
    let result = schema.create_table(table, &auth);
    assert!(matches!(result, Err(SchemaError::KeyspaceNotFound(_))));
}

#[test]
fn create_table_duplicate_fails() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    schema.create_table(test_table("ks1", "t1"), &auth).unwrap();
    let result = schema.create_table(test_table("ks1", "t1"), &auth);
    assert!(matches!(result, Err(SchemaError::TableExists(_, _))));
}

#[test]
fn drop_table_removes_it() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    schema.create_table(test_table("ks1", "t1"), &auth).unwrap();
    schema.drop_table("ks1", "t1", &auth).unwrap();
    assert!(!schema.snapshot().tables.contains_key(&("ks1".to_string(), "t1".to_string())));
}

#[test]
fn create_table_emits_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    let auth = superuser_auth(&schema);
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    sink.clear();
    schema.create_table(test_table("ks1", "t1"), &auth).unwrap();
    let events = sink.events();
    assert!(events.iter().any(|e| matches!(&e.event,
        AuditEventKind::TableCreated { keyspace, table } if keyspace == "ks1" && table == "t1")));
}

#[test]
fn create_table_in_system_keyspace_blocked() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let result = schema.create_table(test_table("system", "bad"), &auth);
    assert!(matches!(result, Err(SchemaError::SystemKeyspaceProtected(_))));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `create_table` not defined.

- [ ] **Step 3: Implement table CRUD**

Same pattern as keyspace CRUD. Validates that the keyspace exists. Checks system keyspace protection. Emits table-specific audit events.

`alter_table` applies `TableUpdates`: optional `params` replacement, `add_columns` appended, `drop_columns` removed.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add table CRUD with auth, audit, system protection
```

---

### Task 22: Role CRUD (create/alter/drop with password policy)

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`
- Test: `ferrosa-schema/src/registry.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn create_role_with_password() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let role = RoleMetadata {
        name: "newuser".to_string(),
        is_superuser: false,
        can_login: true,
        salted_hash: None,
        member_of: HashSet::new(),
    };
    // create_role with a password (via RoleUpdates or separate param)
    schema.create_role(role, Some("userpass"), &auth).unwrap();
    let snap = schema.snapshot();
    assert!(snap.roles["newuser"].salted_hash.is_some());
}

#[test]
fn create_role_weak_password_rejected_by_policy() {
    let schema = test_schema_with_policy(PasswordPolicy::iso27001());
    let auth = superuser_auth(&schema);
    let role = RoleMetadata {
        name: "weak".to_string(),
        is_superuser: false,
        can_login: true,
        salted_hash: None,
        member_of: HashSet::new(),
    };
    let result = schema.create_role(role, Some("bad"), &auth);
    assert!(matches!(result, Err(SchemaError::PasswordTooWeak { .. })));
}

#[test]
fn alter_role_changes_password() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let role = RoleMetadata {
        name: "user1".to_string(),
        is_superuser: false,
        can_login: true,
        salted_hash: None,
        member_of: HashSet::new(),
    };
    schema.create_role(role, Some("OldP@ss123!"), &auth).unwrap();
    let old_hash = schema.snapshot().roles["user1"].salted_hash.clone();

    let updates = RoleUpdates {
        is_superuser: None,
        can_login: None,
        password: Some("NewP@ss456!".to_string()),
        member_of: None,
    };
    schema.alter_role("user1", updates, &auth).unwrap();
    let new_hash = schema.snapshot().roles["user1"].salted_hash.clone();
    assert_ne!(old_hash, new_hash);
}

#[test]
fn alter_role_password_change_emits_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    let auth = superuser_auth(&schema);
    let role = RoleMetadata {
        name: "user1".into(), is_superuser: false, can_login: true,
        salted_hash: None, member_of: HashSet::new(),
    };
    schema.create_role(role, Some("P@ssw0rd!!"), &auth).unwrap();
    sink.clear();
    schema.alter_role("user1", RoleUpdates {
        password: Some("NewP@ss456!".into()), ..Default::default()
    }, &auth).unwrap();
    let events = sink.events();
    assert!(events.iter().any(|e| matches!(&e.event, AuditEventKind::PasswordChanged { .. })));
}

#[test]
fn drop_role_removes_it() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let role = RoleMetadata {
        name: "temp".into(), is_superuser: false, can_login: false,
        salted_hash: None, member_of: HashSet::new(),
    };
    schema.create_role(role, None, &auth).unwrap();
    schema.drop_role("temp", &auth).unwrap();
    assert!(!schema.snapshot().roles.contains_key("temp"));
}

#[test]
fn create_role_cycle_rejected() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_role(RoleMetadata {
        name: "a".into(), is_superuser: false, can_login: false,
        salted_hash: None, member_of: HashSet::from(["b".into()]),
    }, None, &auth).unwrap();
    let result = schema.create_role(RoleMetadata {
        name: "b".into(), is_superuser: false, can_login: false,
        salted_hash: None, member_of: HashSet::from(["a".into()]),
    }, None, &auth);
    assert!(matches!(result, Err(SchemaError::RoleCycleDetected(_))));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `create_role` not defined.

- [ ] **Step 3: Implement role CRUD**

`create_role` accepts `(role: RoleMetadata, password: Option<&str>, auth: &AuthContext)`. If password provided, validates against policy and hashes it. Checks for role cycles before inserting.

**Note:** The spec defines `create_role(&self, role: RoleMetadata, auth: &AuthContext)` with two params. This plan adds a third `password: Option<&str>` param to avoid embedding plaintext passwords in `RoleMetadata.salted_hash`. This is an intentional improvement — the spec should be updated to match.

`alter_role` applies `RoleUpdates`. If `password` is `Some`, validates and hashes. Emits `PasswordChanged` audit event if password changes.

Both emit appropriate audit events.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add role CRUD with password policy and cycle detection
```

---

### Task 23: Grant/Revoke

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`
- Test: `ferrosa-schema/src/registry.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn grant_adds_permissions() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_role(test_role("reader"), None, &auth).unwrap();
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    schema.grant("reader", &Resource::Keyspace("ks1".into()),
        HashSet::from([Permission::Select]), &auth).unwrap();
    let snap = schema.snapshot();
    let grants = snap.grants.get("reader").unwrap();
    assert!(grants.iter().any(|g| g.permissions.contains(&Permission::Select)));
}

#[test]
fn revoke_removes_permissions() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    schema.create_role(test_role("writer"), None, &auth).unwrap();
    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    schema.grant("writer", &Resource::Keyspace("ks1".into()),
        HashSet::from([Permission::Select, Permission::Modify]), &auth).unwrap();
    schema.revoke("writer", &Resource::Keyspace("ks1".into()),
        HashSet::from([Permission::Modify]), &auth).unwrap();
    let snap = schema.snapshot();
    let grants = snap.grants.get("writer").unwrap();
    let ks_grant = grants.iter().find(|g| g.resource == Resource::Keyspace("ks1".into())).unwrap();
    assert!(ks_grant.permissions.contains(&Permission::Select));
    assert!(!ks_grant.permissions.contains(&Permission::Modify));
}

#[test]
fn grant_emits_audit() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    let auth = superuser_auth(&schema);
    schema.create_role(test_role("r1"), None, &auth).unwrap();
    sink.clear();
    schema.grant("r1", &Resource::AllKeyspaces,
        HashSet::from([Permission::Select]), &auth).unwrap();
    let events = sink.events();
    assert!(events.iter().any(|e| matches!(&e.event, AuditEventKind::PermissionGranted { .. })));
}

#[test]
fn grant_to_nonexistent_role_fails() {
    let schema = test_schema();
    let auth = superuser_auth(&schema);
    let result = schema.grant("nobody", &Resource::AllKeyspaces,
        HashSet::from([Permission::Select]), &auth);
    assert!(matches!(result, Err(SchemaError::RoleNotFound(_))));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — `grant` not defined.

- [ ] **Step 3: Implement grant/revoke**

Grant adds or merges permissions for a role+resource pair. Revoke removes specified permissions from the grant entry. Both emit audit events and require auth.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add grant/revoke with auth and audit
```

---

## Chunk 6: System Keyspaces + Storage Bridge

### Task 24: NodeConfig + system.local

**Files:**

- Create: `ferrosa-schema/src/system/mod.rs`
- Create: `ferrosa-schema/src/system/local.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/system/local.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_config_defaults() {
        let config = NodeConfig::default();
        assert_eq!(config.cluster_name, "ferrosa");
        assert_eq!(config.data_center, "dc1");
        assert_eq!(config.rack, "rack1");
        assert_eq!(config.rpc_port, 9042);
    }

    #[test]
    fn query_local_returns_correct_fields() {
        let schema = test_schema();
        let node_config = NodeConfig::default();
        let info = query_local(&schema, &node_config);
        assert_eq!(info.key, "local");
        assert_eq!(info.cluster_name, "ferrosa");
        assert_eq!(info.partitioner, "org.apache.cassandra.dht.Murmur3Partitioner");
        assert_eq!(info.native_protocol_version, "5");
        assert_eq!(info.schema_version, schema.snapshot().version);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement NodeConfig, LocalInfo, query_local**

Implement `NodeConfig` with `Default` impl using the defaults from the spec. Implement `LocalInfo` and `query_local()` that reads schema version from the snapshot and combines with node config.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add NodeConfig, LocalInfo, query_local for system.local
```

---

### Task 25: PeerInfo + ClusterState + system.peers_v2

**Files:**

- Create: `ferrosa-schema/src/system/peers.rs`
- Modify: `ferrosa-schema/src/system/mod.rs`
- Test: `ferrosa-schema/src/system/peers.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct EmptyCluster;
    impl ClusterState for EmptyCluster {
        fn peers(&self) -> Vec<PeerInfo> { vec![] }
    }

    #[test]
    fn single_node_returns_empty_peers() {
        let schema = test_schema();
        let peers = query_peers(&schema, &EmptyCluster);
        assert!(peers.is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement PeerInfo, ClusterState, query_peers**

```rust
pub trait ClusterState: Send + Sync {
    fn peers(&self) -> Vec<PeerInfo>;
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add PeerInfo, ClusterState trait, query_peers
```

---

### Task 26: system_schema Queries

**Files:**

- Create: `ferrosa-schema/src/system/schema_tables.rs`
- Modify: `ferrosa-schema/src/system/mod.rs`
- Test: `ferrosa-schema/src/system/schema_tables.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_keyspaces_reflects_snapshot() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert("ks1".to_string(), test_keyspace_meta("ks1"));
        let rows = query_keyspaces(&snap);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].keyspace_name, "ks1");
    }

    #[test]
    fn query_tables_includes_keyspace_and_table_name() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert("ks1".to_string(), test_keyspace_meta("ks1"));
        snap.tables.insert(("ks1".to_string(), "t1".to_string()), test_table_meta("ks1", "t1"));
        let rows = query_tables(&snap);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].keyspace_name, "ks1");
        assert_eq!(rows[0].table_name, "t1");
    }

    #[test]
    fn query_columns_lists_all_columns() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert("ks1".to_string(), test_keyspace_meta("ks1"));
        let mut table = test_table_meta("ks1", "t1");
        table.columns.insert("c1".to_string(), test_column("c1", ColumnKind::Regular));
        table.columns.insert("c2".to_string(), test_column("c2", ColumnKind::PartitionKey));
        snap.tables.insert(("ks1".to_string(), "t1".to_string()), table);
        let rows = query_columns(&snap);
        assert_eq!(rows.len(), 2);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement query_keyspaces, query_tables, query_columns**

Define `KeyspaceRow`, `TableRow`, `ColumnRow` structs matching Cassandra `system_schema` column names. Convert from `SchemaSnapshot` metadata.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add system_schema query functions
```

---

### Task 27: system_auth Queries (with hash filtering)

**Files:**

- Create: `ferrosa-schema/src/system/auth_tables.rs`
- Modify: `ferrosa-schema/src/system/mod.rs`
- Test: `ferrosa-schema/src/system/auth_tables.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_roles_superuser_sees_hash() {
        let snap = snapshot_with_hashed_role("admin", true, Some("$2b$hash".into()));
        let auth = AuthContext { role: "admin".into(), is_superuser: true, must_change_password: false };
        let rows = query_roles(&snap, &auth);
        let admin_row = rows.iter().find(|r| r.role == "admin").unwrap();
        assert!(admin_row.salted_hash.is_some());
    }

    #[test]
    fn query_roles_non_superuser_hash_filtered() {
        let snap = snapshot_with_hashed_role("user1", false, Some("$2b$hash".into()));
        let auth = AuthContext { role: "user1".into(), is_superuser: false, must_change_password: false };
        let rows = query_roles(&snap, &auth);
        for row in &rows {
            assert!(row.salted_hash.is_none(), "non-superuser should not see hashes");
        }
    }

    #[test]
    fn query_role_members_derives_from_member_of() {
        let mut snap = SchemaSnapshot::new();
        snap.roles.insert("parent".into(), RoleMetadata {
            name: "parent".into(), is_superuser: false, can_login: false,
            salted_hash: None, member_of: HashSet::new(),
        });
        snap.roles.insert("child".into(), RoleMetadata {
            name: "child".into(), is_superuser: false, can_login: true,
            salted_hash: None, member_of: HashSet::from(["parent".into()]),
        });
        let rows = query_role_members(&snap);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].role, "child");
        assert_eq!(rows[0].member, "parent");
    }

    #[test]
    fn query_audit_log_requires_superuser() {
        let sink = SystemTableAuditSink::new(100);
        let non_super = AuthContext { role: "user".into(), is_superuser: false, must_change_password: false };
        let result = query_audit_log(&sink, &non_super);
        assert!(matches!(result, Err(SchemaError::PermissionDenied { .. })));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement system_auth query functions**

- `query_roles(snap, auth)` — returns `Vec<RoleRow>`, filters `salted_hash` to `None` for non-superusers
- `query_role_members(snap)` — derives rows from `member_of` sets
- `query_role_permissions(snap)` — derives rows from grants
- `query_audit_log(sink, auth)` — requires superuser, delegates to `SystemTableAuditSink::query()`

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add system_auth queries with hash filtering
```

---

### Task 28: Storage Bridge (convert.rs)

**Files:**

- Create: `ferrosa-schema/src/convert.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: `ferrosa-schema/src/convert.rs` (inline tests)

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cql_text_to_utf8type() {
        assert_eq!(
            cql_to_marshal_type("text"),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );
    }

    #[test]
    fn cql_varchar_to_utf8type() {
        assert_eq!(
            cql_to_marshal_type("varchar"),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );
    }

    #[test]
    fn cql_int_to_int32type() {
        assert_eq!(
            cql_to_marshal_type("int"),
            "org.apache.cassandra.db.marshal.Int32Type"
        );
    }

    #[test]
    fn cql_bigint_to_longtype() {
        assert_eq!(
            cql_to_marshal_type("bigint"),
            "org.apache.cassandra.db.marshal.LongType"
        );
    }

    #[test]
    fn to_storage_schema_basic() {
        let table = TableMetadata {
            keyspace: "ks".into(),
            name: "users".into(),
            id: Uuid::new_v4(),
            columns: IndexMap::from([
                ("id".into(), ColumnMetadata {
                    name: "id".into(), kind: ColumnKind::PartitionKey,
                    position: 0, column_type: "uuid".into(),
                    clustering_order: ClusteringOrder::None, mask: None,
                }),
                ("name".into(), ColumnMetadata {
                    name: "name".into(), kind: ColumnKind::Regular,
                    position: 0, column_type: "text".into(),
                    clustering_order: ClusteringOrder::None, mask: None,
                }),
            ]),
            partition_key: vec!["id".into()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
        };
        let storage = table.to_storage_schema();
        assert_eq!(storage.keyspace, "ks");
        assert_eq!(storage.table, "users");
        assert_eq!(storage.key_type, "org.apache.cassandra.db.marshal.UUIDType");
        assert_eq!(storage.regular_columns.len(), 1);
        assert_eq!(storage.regular_columns[0].name, "name");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema`
Expected: FAIL — functions not defined.

- [ ] **Step 3: Implement cql_to_marshal_type and to_storage_schema**

`cql_to_marshal_type()` maps CQL type strings to Cassandra marshal type names per the spec's type mapping table. Handle collections (`set<T>`, `list<T>`, `map<K,V>`, `frozen<T>`) by recursively converting inner types.

`TableMetadata::to_storage_schema()` converts to `ferrosa_common::TableSchema`, extracting partition key type (composite if multiple partition key columns), clustering columns, static columns, and regular columns.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
feat(schema): add storage bridge with CQL-to-marshal type mapping
```

---

## Chunk 7: Integration + Property Tests

### Task 29: Integration Tests — Full Workflow

**Files:**

- Create: `ferrosa-schema/tests/integration.rs`
- Test: integration tests

- [ ] **Step 1: Write integration test — full workflow**

```rust
//! Full workflow integration tests for ferrosa-schema.

use ferrosa_schema::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[test]
fn full_workflow_create_role_auth_keyspace_table_query() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = Schema::new(SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(/* wrapper for sink */),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    }).unwrap();

    // Authenticate as superuser
    let auth = schema.authenticate("cassandra", "cassandra").unwrap();
    assert!(auth.is_superuser);

    // Create a keyspace
    schema.create_keyspace(KeyspaceMetadata {
        name: "production".into(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".into(),
            options: HashMap::from([("replication_factor".into(), "1".into())]),
        },
    }, &auth).unwrap();

    // Create a table
    // ... (full table with columns)

    // Verify system_schema reflects state
    let snap = schema.snapshot();
    let ks_rows = query_keyspaces(&snap);
    assert!(ks_rows.iter().any(|r| r.keyspace_name == "production"));
}
```

- [ ] **Step 2: Write integration test — permission denied**

```rust
#[test]
fn non_superuser_cannot_create_keyspace_without_grant() {
    let schema = test_schema();
    let su = superuser_auth(&schema);
    schema.create_role(test_role("limited"), Some("P@ssw0rd!!"), &su).unwrap();
    let ctx = schema.authenticate("limited", "P@ssw0rd!!").unwrap();
    let result = schema.create_keyspace(test_keyspace("ks1"), &ctx);
    assert!(matches!(result, Err(SchemaError::PermissionDenied { .. })));
}
```

- [ ] **Step 3: Write integration test — rate limiting**

```rust
#[test]
fn rate_limiting_blocks_after_failures() {
    let schema = Schema::new(SchemaConfig {
        rate_limit: RateLimitConfig {
            max_attempts: 3,
            base_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
            lockout_duration: Duration::from_secs(5),
            window: Duration::from_secs(60),
        },
        ..test_config()
    }).unwrap();

    for _ in 0..3 {
        let _ = schema.authenticate("cassandra", "wrong");
    }
    // Even with correct password, should be throttled
    let result = schema.authenticate("cassandra", "cassandra");
    assert!(matches!(result, Err(SchemaError::AuthenticationThrottled)));
}
```

- [ ] **Step 4: Write integration test — audit completeness**

```rust
#[test]
fn every_mutation_emits_audit_event() {
    let sink = Arc::new(TestAuditSink::new());
    let schema = test_schema_with_sink(sink.clone());
    let auth = superuser_auth(&schema);
    sink.clear();

    schema.create_keyspace(test_keyspace("ks1"), &auth).unwrap();
    schema.create_table(test_table("ks1", "t1"), &auth).unwrap();
    schema.create_role(test_role("r1"), None, &auth).unwrap();
    schema.grant("r1", &Resource::AllKeyspaces, HashSet::from([Permission::Select]), &auth).unwrap();
    schema.revoke("r1", &Resource::AllKeyspaces, HashSet::from([Permission::Select]), &auth).unwrap();
    schema.drop_role("r1", &auth).unwrap();
    schema.drop_table("ks1", "t1", &auth).unwrap();
    schema.drop_keyspace("ks1", &auth).unwrap();

    let events = sink.events();
    // 8 operations = 8 audit events
    assert_eq!(events.len(), 8);
}
```

- [ ] **Step 5: Run all integration tests**

Run: `cargo test -p ferrosa-schema --test integration`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```
test(schema): add integration tests for full workflow, permissions, audit
```

---

### Task 30: Integration Tests — Bootstrap + Production Mode

**Files:**

- Create: `ferrosa-schema/tests/auth_integration.rs`
- Test: integration tests

- [ ] **Step 1: Write bootstrap integration tests**

```rust
#[test]
fn bootstrap_with_env_password_no_must_change() {
    std::env::set_var("FERROSA_SUPERUSER_PASSWORD", "Str0ng!P@sswd");
    let schema = Schema::new(test_config()).unwrap();
    let ctx = schema.authenticate("cassandra", "Str0ng!P@sswd").unwrap();
    assert!(!ctx.must_change_password);
    std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
}

#[test]
fn bootstrap_without_env_password_must_change() {
    std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    let schema = Schema::new(test_config()).unwrap();
    let ctx = schema.authenticate("cassandra", "cassandra").unwrap();
    assert!(ctx.must_change_password);
}
```

- [ ] **Step 2: Write production mode validation tests**

```rust
#[test]
fn production_mode_rejects_default_password() {
    std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    let config = SchemaConfig {
        mode: DeploymentMode::Production,
        ..test_config()
    };
    // Should fail during new() or return violations
    // (exact behavior depends on implementation)
}

#[test]
fn production_mode_rejects_weak_password_policy() {
    let node_config = NodeConfig::default();
    let config = SchemaConfig {
        password_policy: PasswordPolicy::permissive(),
        mode: DeploymentMode::Production,
        ..test_config()
    };
    let violations = validate_production_requirements(&config, &node_config);
    assert!(violations.iter().any(|v| matches!(v, ProductionViolation::PasswordPolicyBelowMinimum)));
}

#[test]
fn development_mode_allows_permissive_policy() {
    let node_config = NodeConfig::default();
    let config = SchemaConfig {
        password_policy: PasswordPolicy::permissive(),
        mode: DeploymentMode::Development,
        ..test_config()
    };
    let violations = validate_production_requirements(&config, &node_config);
    // Violations are returned but not fatal in development mode
    // (the caller decides based on mode)
}
```

- [ ] **Step 3: Write hash filtering integration test**

```rust
#[test]
fn hash_filtering_integration() {
    let schema = test_schema();
    let su = superuser_auth(&schema);
    schema.create_role(RoleMetadata {
        name: "viewer".into(), is_superuser: false, can_login: true,
        salted_hash: None, member_of: HashSet::new(),
    }, Some("ViewP@ss1!"), &su).unwrap();

    let snap = schema.snapshot();

    // Superuser sees hashes
    let su_rows = query_roles(&snap, &su);
    assert!(su_rows.iter().any(|r| r.role == "viewer" && r.salted_hash.is_some()));

    // Non-superuser does not
    let viewer_auth = schema.authenticate("viewer", "ViewP@ss1!").unwrap();
    let viewer_rows = query_roles(&snap, &viewer_auth);
    for row in &viewer_rows {
        assert!(row.salted_hash.is_none());
    }
}
```

- [ ] **Step 4: Run all auth integration tests**

Run: `cargo test -p ferrosa-schema --test auth_integration`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
test(schema): add bootstrap, production mode, and hash filtering integration tests
```

---

### Task 31: Property Tests

**Files:**

- Create: `ferrosa-schema/tests/property_tests.rs`
- Test: property tests

- [ ] **Step 1: Write property tests**

```rust
use proptest::prelude::*;
use ferrosa_schema::*;

proptest! {
    #[test]
    fn schema_snapshot_serde_roundtrip(
        ks_name in "[a-z][a-z0-9_]{0,10}",
        table_name in "[a-z][a-z0-9_]{0,10}",
    ) {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert(ks_name.clone(), KeyspaceMetadata {
            name: ks_name.clone(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        });
        let json = serde_json::to_string(&snap).unwrap();
        let back: SchemaSnapshot = serde_json::from_str(&json).unwrap();
        prop_assert!(back.keyspaces.contains_key(&ks_name));
    }

    #[test]
    fn superuser_always_authorized(
        perm_idx in 0..8usize,
    ) {
        let perms = [
            Permission::Create, Permission::Alter, Permission::Drop,
            Permission::Select, Permission::Modify, Permission::Authorize,
            Permission::Describe, Permission::Execute,
        ];
        let snap = SchemaSnapshot::new();
        let auth = AuthContext {
            role: "super".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        let result = check_permission(&snap, &auth, perms[perm_idx], &Resource::AllKeyspaces);
        prop_assert!(result.is_ok());
    }

    #[test]
    fn hash_verifies_same_password(password in ".{1,50}") {
        let hasher = PasswordHasher::default();
        let hash = hasher.hash_password(&password).unwrap();
        prop_assert!(PasswordHasher::verify_password_any(&password, &hash).unwrap());
    }

    #[test]
    fn hash_rejects_different_password(
        password in ".{1,30}",
        other in ".{1,30}",
    ) {
        prop_assume!(password != other);
        let hasher = PasswordHasher::default();
        let hash = hasher.hash_password(&password).unwrap();
        prop_assert!(!PasswordHasher::verify_password_any(&other, &hash).unwrap());
    }
}
```

- [ ] **Step 2: Run property tests**

Run: `cargo test -p ferrosa-schema --test property_tests`
Expected: All tests pass.

- [ ] **Step 3: Commit**

```
test(schema): add property tests for serde roundtrip, permissions, hashing
```

---

### Task 32: Final lib.rs Re-exports + Cleanup

**Files:**

- Modify: `ferrosa-schema/src/lib.rs`

- [ ] **Step 1: Ensure all public types are re-exported from lib.rs**

Update `lib.rs` to re-export all major types:

```rust
pub mod metadata;
pub mod auth;
pub mod audit;
pub mod registry;
pub mod system;
pub mod secrets;
pub mod startup;
pub mod convert;
pub mod error;

pub use error::{SchemaError, Result};
pub use metadata::{
    KeyspaceMetadata, ReplicationParams, TableMetadata, TableParams,
    CachingParams, TableFlag, ColumnMetadata, ColumnKind, ClusteringOrder, ColumnMask,
    KeyspaceUpdates, TableUpdates,
};
pub use auth::{
    RoleMetadata, AuthContext, RoleUpdates, Permission, Resource, GrantEntry,
    PasswordHasher, PasswordPolicy, AuthRateLimiter, RateLimitConfig,
    check_permission,
};
pub use audit::{
    AuditEvent, AuditEventKind, AuditContext, AuditLogEntry,
    AuditSink, TestAuditSink, CompositeSink, LogAuditSink,
};
pub use audit::table_sink::SystemTableAuditSink;
pub use registry::{Schema, SchemaSnapshot, SchemaConfig, AuthMethod};
pub use system::{
    NodeConfig, LocalInfo, PeerInfo, ClusterState,
    query_local, query_peers, query_keyspaces, query_tables, query_columns,
    query_roles, query_role_members, query_role_permissions, query_audit_log,
};
pub use secrets::{SecretsProvider, SecretsError, EnvSecretsProvider};
pub use startup::{DeploymentMode, ProductionViolation, validate_production_requirements};
pub use convert::cql_to_marshal_type;
```

- [ ] **Step 2: Run full test suite**

Run: `cargo test -p ferrosa-schema`
Expected: All tests pass.

- [ ] **Step 3: Run clippy and fmt**

Run: `cargo clippy -p ferrosa-schema -- -D warnings && cargo fmt -p ferrosa-schema -- --check`
Expected: No warnings, no formatting issues.

- [ ] **Step 4: Commit**

```
feat(schema): finalize public API re-exports
```

---

## Summary

| Chunk | Tasks | What it builds |
|-------|-------|---------------|
| 1 | 1–5 | Scaffolding, errors, metadata types (keyspace, column, table) |
| 2 | 6–10 | Auth types, password hashing, password policy, rate limiting |
| 3 | 11–15 | Audit events/sinks, secrets provider, production mode |
| 4 | 16–19 | Schema registry, bootstrap, authentication, permission checking |
| 5 | 20–23 | Keyspace/table/role CRUD, grant/revoke |
| 6 | 24–28 | System keyspaces, storage bridge |
| 7 | 29–32 | Integration tests, property tests, cleanup |

**Total:** 32 tasks, ~160 TDD steps

**Dependencies:** Each chunk builds on the previous. Within a chunk, tasks are ordered by dependency.

---

## Implementation Notes

### Test Helpers

Tests reference helpers that must be created as needed. Define them in a shared test support module or inline in `registry.rs` tests. Key helpers:

- `test_schema()` — creates `Schema` with permissive defaults, development mode, `TestAuditSink`
- `test_schema_with_sink(sink: Arc<TestAuditSink>)` — schema with shared sink for audit verification
- `test_schema_with_rate_limit(config: RateLimitConfig)` — schema with custom rate limit config
- `test_schema_with_policy(policy: PasswordPolicy)` — schema with custom password policy
- `test_config()` — returns a `SchemaConfig` with test defaults
- `superuser_auth(schema: &Schema)` — authenticates as `cassandra` and returns the `AuthContext`
- `test_keyspace(name: &str)` — creates a `KeyspaceMetadata` with SimpleStrategy RF=1
- `test_table(ks: &str, name: &str)` — creates a `TableMetadata` with one partition key column
- `test_role(name: &str)` — creates a `RoleMetadata` with `can_login: true`, no superuser

### Environment Variable Test Safety

Tests that use `std::env::set_var`/`std::env::remove_var` must not run in parallel with each other. Options:

1. Add `serial_test = "3"` as a dev-dependency and annotate env var tests with `#[serial]`
2. Use a test mutex to serialize env var tests
3. Design the SecretsProvider to accept values directly (preferred — avoids env vars in tests entirely)

### Spec Deviations

| Deviation | Rationale |
|-----------|-----------|
| `create_role` has 3 params (adds `password: Option<&str>`) | Avoids embedding plaintext in `RoleMetadata` |
| `check_permission` is both free function and Schema method | Free function for testability; method for API compat |
| `CompositeSink` uses `Arc<dyn AuditSink>` not `Box` | Needed for shared sink in tests and multi-owner patterns |

These should be reflected back to the spec after implementation is validated.
