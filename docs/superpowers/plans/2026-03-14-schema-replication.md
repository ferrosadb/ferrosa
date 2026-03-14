# Schema Replication Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replicate DDL operations between pair mode nodes so schema catch-up and live DDL forwarding work end-to-end.

**Architecture:** Two mechanisms — snapshot-based sync for catch-up (primary sends full SchemaSnapshot to secondary on rejoin) and live DDL forwarding during normal pair operation (DDL routed through primary like writes). Three new wire messages, internal auth-bypass methods, and DdlPath/DdlCoordinator paralleling the existing WritePath/PairCoordinator.

**Tech Stack:** Rust, serde_json, ferrosa-net internode protocol, ferrosa-schema ArcSwap registry, ferrosa-storage engine, ferrosa-cql router.

**Spec:** `docs/superpowers/specs/2026-03-14-schema-replication-design.md`
**Threat Model:** `specs/threat-model-schema-replication.md`

## Review Findings (must address during implementation)

1. **Add `AlterKeyspace` and `AlterTable` variants** to `DdlOperation`, plus
   `alter_keyspace_internal` and `alter_table_internal` in Schema, and matching
   arms in all handlers. Omitted from plan code for brevity but required by spec.
2. **Add `StorageEngine::unregister_table()`** — needed for DropTable/DropKeyspace
   DDL replication. Currently does not exist.
3. **Make `is_system_keyspace()` public** — it's private in `registry.rs`. Must be
   `pub` and re-exported in `ferrosa-schema/src/lib.rs`.
4. **`SchemaSnapshot::empty()` may not exist** — construct manually in tests if needed.
5. **`PairDdlForwardHandler::apply_locally`** must propagate storage errors (not
   `let _ =`). Match `DdlCoordinator` error handling.
6. **Use `split_to` not `copy_to_bytes`** in decode arms to match existing patterns.
7. **Doc comment on `apply_snapshot`** must note that caller is responsible for
   `engine.register_table()` — the method only updates the schema registry.
8. **Add integration tests** between unit tests and smoke test (DDL replication
   primary→secondary, DDL forwarding secondary→primary, schema catch-up on rejoin).

---

## File Structure

### New files

| File | Purpose |
|------|---------|
| `ferrosa-cluster/src/pair/ddl.rs` | `DdlOperation` enum, `DdlCoordinator`, `PairDdlForwardHandler`, `PairSchemaSyncHandler` |
| `ferrosa-cluster/src/ddl_path.rs` | `DdlPath` enum (Direct/Pair/Unavailable) |

### Modified files

| File | Changes |
|------|---------|
| `ferrosa-net/src/codec.rs:72-76` | Add MsgType variants 0x45-0x47 |
| `ferrosa-net/src/codec.rs:99-103` | Add TryFrom match arms |
| `ferrosa-net/src/message.rs:105-115` | Add Message variants |
| `ferrosa-net/src/message.rs:137-141` | Add msg_type() match arms |
| `ferrosa-net/src/message.rs:146+` | Add encode/decode arms |
| `ferrosa-net/src/message.rs:361` | Update proptest range to 0x47 |
| `ferrosa-schema/src/metadata/keyspace.rs:30` | Add Serialize, Deserialize to KeyspaceUpdates |
| `ferrosa-schema/src/metadata/table.rs:135` | Add Serialize, Deserialize to TableUpdates |
| `ferrosa-schema/src/registry.rs:409+` | Add internal methods, apply_snapshot() |
| `ferrosa-cluster/src/lib.rs` | Export ddl_path, DdlPath |
| `ferrosa-cluster/src/pair/mod.rs` | Add `pub mod ddl;` |
| `ferrosa-cluster/src/controller.rs:280+` | Send schema before mutations in catch-up |
| `ferrosa-cql/src/router.rs:885-1132` | Route DDL through DdlPath |
| `ferrosa/src/main.rs:147-156` | Wire ddl_path into SharedState |
| `tests/docker-smoke.sh` | Remove dual-create workaround |

---

## Chunk 1: Wire Protocol (ferrosa-net)

### Task 1: Add wire message types

**Files:**

- Modify: `ferrosa-net/src/codec.rs:72-103`
- Modify: `ferrosa-net/src/message.rs:63-142, 146+, 361`

- [ ] **Step 1: Add MsgType variants**

In `ferrosa-net/src/codec.rs`, add after `RoleSwap = 0x44` (line 76):

```rust
    PairSchemaSync = 0x45,
    PairDdlForward = 0x46,
    PairDdlAck = 0x47,
```

Add matching arms in `TryFrom<u8>` (after line 103):

```rust
    0x45 => Ok(Self::PairSchemaSync),
    0x46 => Ok(Self::PairDdlForward),
    0x47 => Ok(Self::PairDdlAck),
```

- [ ] **Step 2: Add Message variants**

In `ferrosa-net/src/message.rs`, add after `RoleSwap` (line 115):

```rust
    /// Schema snapshot for catch-up sync (JSON-serialized SchemaSnapshot).
    PairSchemaSync(Bytes),
    /// DDL operation forwarded between pair nodes (JSON-serialized DdlOperation).
    PairDdlForward(Bytes),
    /// DDL acknowledgment.
    PairDdlAck(Bytes),
```

- [ ] **Step 3: Add msg_type() match arms**

In `msg_type()` (after line 141):

```rust
    Self::PairSchemaSync(_) => MsgType::PairSchemaSync,
    Self::PairDdlForward(_) => MsgType::PairDdlForward,
    Self::PairDdlAck(_) => MsgType::PairDdlAck,
```

- [ ] **Step 4: Add encode arms**

In `encode()`, add alongside existing opaque-body variants:

```rust
    Self::PairSchemaSync(body) => buf.put_slice(body),
    Self::PairDdlForward(body) => buf.put_slice(body),
    Self::PairDdlAck(body) => buf.put_slice(body),
```

- [ ] **Step 5: Add decode arms**

In `decode()`, add alongside existing opaque-body variants:

```rust
    MsgType::PairSchemaSync => Ok(Self::PairSchemaSync(buf.copy_to_bytes(buf.remaining()))),
    MsgType::PairDdlForward => Ok(Self::PairDdlForward(buf.copy_to_bytes(buf.remaining()))),
    MsgType::PairDdlAck => Ok(Self::PairDdlAck(buf.copy_to_bytes(buf.remaining()))),
```

- [ ] **Step 6: Update proptest range**

In `ferrosa-net/src/message.rs:361`, change:

```rust
// Old:
for msg_type_byte in 0x01..=0x44u8 {
// New:
for msg_type_byte in 0x01..=0x47u8 {
```

- [ ] **Step 7: Build and test**

Run: `cargo test -p ferrosa-net`
Expected: All tests pass including proptest decode fuzz for new types.

- [ ] **Step 8: Commit**

```bash
git add ferrosa-net/src/codec.rs ferrosa-net/src/message.rs
git commit -m "feat(net): add PairSchemaSync, PairDdlForward, PairDdlAck message types"
```

---

## Chunk 2: Schema Internal Methods (ferrosa-schema)

### Task 2: Add serde derives to update types

**Files:**

- Modify: `ferrosa-schema/src/metadata/keyspace.rs:30`
- Modify: `ferrosa-schema/src/metadata/table.rs:135`

- [ ] **Step 1: Add Serialize, Deserialize to KeyspaceUpdates**

In `ferrosa-schema/src/metadata/keyspace.rs:30`, change:

```rust
// Old:
#[derive(Debug, Clone)]
pub struct KeyspaceUpdates {
// New:
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceUpdates {
```

- [ ] **Step 2: Add Serialize, Deserialize to TableUpdates**

In `ferrosa-schema/src/metadata/table.rs:135`, change:

```rust
// Old:
#[derive(Debug, Clone)]
pub struct TableUpdates {
// New:
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableUpdates {
```

- [ ] **Step 3: Build to verify**

Run: `cargo check -p ferrosa-schema`
Expected: Compiles cleanly.

### Task 3: Add internal schema methods and apply_snapshot

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`

- [ ] **Step 4: Write test for apply_snapshot**

Add to the test module at the bottom of `ferrosa-schema/src/registry.rs`:

```rust
#[test]
fn apply_snapshot_creates_keyspaces_and_tables() {
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    };
    let schema = Schema::new(config).unwrap();
    let mut snapshot = SchemaSnapshot::empty();

    let ks = KeyspaceMetadata {
        name: "test_ks".to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
        },
    };
    snapshot.keyspaces.insert("test_ks".to_string(), ks);

    let table = TableMetadata::new_simple("test_ks", "test_tbl", vec!["id"], vec![("id", "text")]);
    snapshot.tables.insert(("test_ks".to_string(), "test_tbl".to_string()), table);

    schema.apply_snapshot(snapshot).unwrap();

    let snap = schema.snapshot();
    assert!(snap.keyspaces.contains_key("test_ks"));
    assert!(snap.tables.contains_key(&("test_ks".to_string(), "test_tbl".to_string())));
}

#[test]
fn apply_snapshot_is_idempotent() {
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    };
    let schema = Schema::new(config).unwrap();
    let mut snapshot = SchemaSnapshot::empty();

    let ks = KeyspaceMetadata {
        name: "test_ks".to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
        },
    };
    snapshot.keyspaces.insert("test_ks".to_string(), ks);

    // Apply twice — second should not error
    schema.apply_snapshot(snapshot.clone()).unwrap();
    schema.apply_snapshot(snapshot).unwrap();
}

#[test]
fn create_keyspace_internal_is_idempotent() {
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    };
    let schema = Schema::new(config).unwrap();

    let ks = KeyspaceMetadata {
        name: "test_ks".to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
        },
    };

    schema.create_keyspace_internal(ks.clone()).unwrap();
    schema.create_keyspace_internal(ks).unwrap(); // Should not error
}
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `cargo test -p ferrosa-schema -- apply_snapshot create_keyspace_internal`
Expected: FAIL — methods don't exist yet.

- [ ] **Step 6: Implement internal methods**

Add to `Schema` impl block in `ferrosa-schema/src/registry.rs`, after the
`snapshot()` method (around line 182):

```rust
    /// Bulk-load schema from a snapshot. Bypasses auth and audit.
    /// Used for pair mode catch-up. Idempotent — silently skips
    /// keyspaces/tables that already exist.
    ///
    /// Filters system keyspaces. Does NOT register tables with
    /// StorageEngine — caller must do that separately.
    pub fn apply_snapshot(&self, snapshot: SchemaSnapshot) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut current = (**self.inner.load()).clone();

        for (name, ks) in &snapshot.keyspaces {
            if is_system_keyspace(name) {
                continue;
            }
            current.keyspaces.entry(name.clone()).or_insert_with(|| ks.clone());
        }

        for ((ks, tbl), table) in &snapshot.tables {
            if is_system_keyspace(ks) {
                continue;
            }
            current.tables.entry((ks.clone(), tbl.clone())).or_insert_with(|| table.clone());
        }

        for (name, role) in &snapshot.roles {
            current.roles.entry(name.clone()).or_insert_with(|| role.clone());
        }

        for (role, grants) in &snapshot.grants {
            current.grants.entry(role.clone()).or_insert_with(|| grants.clone());
        }

        current.version = Uuid::new_v4();
        self.inner.store(Arc::new(current));
        tracing::info!("applied schema snapshot");
        Ok(())
    }

    /// Create a keyspace without auth checks. Idempotent.
    pub(crate) fn create_keyspace_internal(&self, ks: KeyspaceMetadata) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        if snap.keyspaces.contains_key(&ks.name) {
            return Ok(()); // Idempotent
        }
        snap.keyspaces.insert(ks.name.clone(), ks);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Create a table without auth checks. Idempotent.
    pub(crate) fn create_table_internal(&self, table: TableMetadata) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (table.keyspace.clone(), table.name.clone());
        if snap.tables.contains_key(&key) {
            return Ok(()); // Idempotent
        }
        snap.tables.insert(key, table);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a keyspace without auth checks. Idempotent.
    pub(crate) fn drop_keyspace_internal(&self, name: &str) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        snap.keyspaces.remove(name);
        // Also remove all tables in the keyspace
        snap.tables.retain(|(ks, _)| ks != name);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a table without auth checks. Idempotent.
    pub(crate) fn drop_table_internal(&self, keyspace: &str, table: &str) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        snap.tables.remove(&(keyspace.to_string(), table.to_string()));
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        Ok(())
    }
```

Note: `TableMetadata::new_simple` may not exist. If so, create the
TableMetadata manually in the test using the builder pattern from existing
tests.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p ferrosa-schema -- apply_snapshot create_keyspace_internal`
Expected: All PASS.

- [ ] **Step 8: Commit**

```bash
git add ferrosa-schema/
git commit -m "feat(schema): add apply_snapshot and internal DDL methods for replication"
```

---

## Chunk 3: DdlPath + DdlCoordinator (ferrosa-cluster)

### Task 4: Create DdlPath enum

**Files:**

- Create: `ferrosa-cluster/src/ddl_path.rs`
- Modify: `ferrosa-cluster/src/lib.rs`

- [ ] **Step 1: Create DdlPath**

Create `ferrosa-cluster/src/ddl_path.rs`:

```rust
//! DDL path abstraction for runtime mode transitions.
//!
//! Parallels `WritePath` — the CQL router calls `DdlPath::execute()`
//! for all DDL operations. Swapped atomically via `ArcSwap`.

use std::sync::Arc;

use ferrosa_schema::registry::SchemaSnapshot;
use ferrosa_schema::Schema;
use ferrosa_storage::engine::StorageEngine;

use crate::pair::ddl::DdlCoordinator;

/// The active DDL path. Swapped atomically via `ArcSwap` when
/// the deployment mode changes (standalone → pair → cluster).
pub enum DdlPath {
    /// Standalone: DDL applied directly to local schema + storage.
    Direct {
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
    },
    /// Pair mode: DDL routed through DdlCoordinator (primary authority).
    Pair(Arc<DdlCoordinator>),
    /// Degraded: peer lost, DDL rejected until operator promotes.
    Unavailable,
}
```

- [ ] **Step 2: Add to lib.rs**

In `ferrosa-cluster/src/lib.rs`, add:

```rust
pub mod ddl_path;
pub use ddl_path::DdlPath;
```

- [ ] **Step 3: Build to verify**

Run: `cargo check -p ferrosa-cluster`
Expected: Compiles (DdlCoordinator doesn't exist yet, will need stub).

### Task 5: Create DdlOperation and DdlCoordinator

**Files:**

- Create: `ferrosa-cluster/src/pair/ddl.rs`
- Modify: `ferrosa-cluster/src/pair/mod.rs`

- [ ] **Step 4: Create pair/ddl.rs**

Create `ferrosa-cluster/src/pair/ddl.rs`:

```rust
//! DDL coordination and forwarding for pair mode.
//!
//! `DdlCoordinator` routes DDL through the primary, mirroring how
//! `PairCoordinator` routes mutations. The primary is the single
//! DDL authority — secondary forwards, primary applies + replicates.

use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, KeyspaceUpdates};
use ferrosa_schema::metadata::table::{TableMetadata, TableUpdates};
use ferrosa_schema::Schema;
use ferrosa_storage::engine::StorageEngine;

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// A DDL operation that can be forwarded/replicated between pair nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlOperation {
    CreateKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(TableMetadata),
    DropTable { keyspace: String, table: String },
}

impl DdlOperation {
    /// Serialize to JSON bytes for wire transmission.
    pub fn to_bytes(&self) -> Result<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| ClusterError::Internal(format!("DDL serialize: {e}")))
    }

    /// Deserialize from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| ClusterError::Internal(format!("DDL deserialize: {e}")))
    }
}

/// Coordinates DDL in pair mode. Routes through primary.
pub struct DdlCoordinator {
    role: Arc<ArcSwap<PairRole>>,
    peer_host_id: Uuid,
    schema: Arc<Schema>,
    engine: Arc<StorageEngine>,
    peer_manager: Arc<PeerManager>,
}

impl DdlCoordinator {
    pub fn new(
        role: Arc<ArcSwap<PairRole>>,
        peer_host_id: Uuid,
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            role,
            peer_host_id,
            schema,
            engine,
            peer_manager,
        }
    }

    /// Route a DDL operation based on current role.
    pub async fn coordinate_ddl(&self, op: DdlOperation) -> Result<()> {
        match **self.role.load() {
            PairRole::Primary => {
                // Apply locally, then replicate to secondary.
                self.apply_ddl_locally(&op)?;
                self.replicate_ddl(&op).await?;
                Ok(())
            }
            PairRole::Secondary => {
                // Forward to primary. Primary applies + replicates back.
                self.forward_ddl(&op).await
            }
        }
    }

    /// Apply a DDL operation to local schema + storage.
    pub(crate) fn apply_ddl_locally(&self, op: &DdlOperation) -> Result<()> {
        match op {
            DdlOperation::CreateKeyspace(ks) => {
                self.schema
                    .create_keyspace_internal(ks.clone())
                    .map_err(|e| ClusterError::Internal(format!("create keyspace: {e}")))?;
            }
            DdlOperation::DropKeyspace(name) => {
                self.schema
                    .drop_keyspace_internal(name)
                    .map_err(|e| ClusterError::Internal(format!("drop keyspace: {e}")))?;
            }
            DdlOperation::CreateTable(table) => {
                self.schema
                    .create_table_internal(table.clone())
                    .map_err(|e| ClusterError::Internal(format!("create table: {e}")))?;
                let storage_schema = table.to_storage_schema();
                self.engine
                    .register_table(storage_schema)
                    .map_err(ClusterError::Storage)?;
            }
            DdlOperation::DropTable { keyspace, table } => {
                self.schema
                    .drop_table_internal(keyspace, table)
                    .map_err(|e| ClusterError::Internal(format!("drop table: {e}")))?;
            }
        }
        Ok(())
    }

    /// Send DDL to peer and wait for ack.
    async fn replicate_ddl(&self, op: &DdlOperation) -> Result<()> {
        let body = op.to_bytes()?;
        let resp = self
            .peer_manager
            .send(self.peer_host_id, Message::PairDdlForward(body), Lane::Data)
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairDdlAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairDdlAck, got {:?}",
                other.msg_type()
            ))),
        }
    }

    /// Forward DDL to primary and wait for ack.
    async fn forward_ddl(&self, op: &DdlOperation) -> Result<()> {
        let body = op.to_bytes()?;
        let resp = self
            .peer_manager
            .send(self.peer_host_id, Message::PairDdlForward(body), Lane::Data)
            .await
            .map_err(ClusterError::Net)?;

        match resp {
            Message::PairDdlAck(_) => Ok(()),
            other => Err(ClusterError::ReplicationFailed(format!(
                "expected PairDdlAck, got {:?}",
                other.msg_type()
            ))),
        }
    }
}

/// RPC handler for PairDdlForward messages.
/// Uses role-based dispatch like PairWriteForwardHandler.
pub struct PairDdlForwardHandler {
    role: Arc<ArcSwap<PairRole>>,
    schema: Arc<Schema>,
    engine: Arc<StorageEngine>,
    peer_host_id: Uuid,
    peer_manager: Arc<PeerManager>,
}

impl PairDdlForwardHandler {
    pub fn new(
        role: Arc<ArcSwap<PairRole>>,
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
        peer_host_id: Uuid,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            role,
            schema,
            engine,
            peer_host_id,
            peer_manager,
        }
    }

    fn apply_locally(&self, op: &DdlOperation) -> Result<()> {
        match op {
            DdlOperation::CreateKeyspace(ks) => {
                self.schema
                    .create_keyspace_internal(ks.clone())
                    .map_err(|e| ClusterError::Internal(format!("create keyspace: {e}")))?;
            }
            DdlOperation::DropKeyspace(name) => {
                self.schema
                    .drop_keyspace_internal(name)
                    .map_err(|e| ClusterError::Internal(format!("drop keyspace: {e}")))?;
            }
            DdlOperation::CreateTable(table) => {
                self.schema
                    .create_table_internal(table.clone())
                    .map_err(|e| ClusterError::Internal(format!("create table: {e}")))?;
                let storage_schema = table.to_storage_schema();
                let _ = self.engine.register_table(storage_schema);
            }
            DdlOperation::DropTable { keyspace, table } => {
                self.schema
                    .drop_table_internal(keyspace, table)
                    .map_err(|e| ClusterError::Internal(format!("drop table: {e}")))?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairDdlForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairDdlForward(b) => b,
            _ => return None,
        };

        let op = match DdlOperation::from_bytes(&body) {
            Ok(op) => op,
            Err(e) => {
                tracing::error!(%e, "failed to deserialize DDL operation");
                return None;
            }
        };

        if let Err(e) = self.apply_locally(&op) {
            tracing::error!(%e, "failed to apply DDL locally");
            return Some(Message::PairDdlAck(Bytes::from(
                format!("error: {e}").into_bytes(),
            )));
        }

        // If primary, replicate to secondary after applying locally.
        if **self.role.load() == PairRole::Primary {
            if let Ok(fwd_body) = op.to_bytes() {
                let _ = self
                    .peer_manager
                    .send(self.peer_host_id, Message::PairDdlForward(fwd_body), Lane::Data)
                    .await;
            }
        }

        Some(Message::PairDdlAck(Bytes::new()))
    }
}

/// RPC handler for PairSchemaSync messages (catch-up).
pub struct PairSchemaSyncHandler {
    schema: Arc<Schema>,
    engine: Arc<StorageEngine>,
}

impl PairSchemaSyncHandler {
    pub fn new(schema: Arc<Schema>, engine: Arc<StorageEngine>) -> Self {
        Self { schema, engine }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairSchemaSyncHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairSchemaSync(b) => b,
            _ => return None,
        };

        let snapshot: ferrosa_schema::registry::SchemaSnapshot = match serde_json::from_slice(&body)
        {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(%e, "failed to deserialize schema snapshot");
                return Some(Message::PairDdlAck(Bytes::from("deserialize error".as_bytes())));
            }
        };

        // Register tables with storage engine BEFORE applying snapshot.
        for table in snapshot.tables.values() {
            if ferrosa_schema::is_system_keyspace(&table.keyspace) {
                continue;
            }
            let storage_schema = table.to_storage_schema();
            if let Err(e) = self.engine.register_table(storage_schema) {
                tracing::warn!(
                    keyspace = %table.keyspace,
                    table = %table.name,
                    %e,
                    "failed to register table during schema sync"
                );
            }
        }

        if let Err(e) = self.schema.apply_snapshot(snapshot) {
            tracing::error!(%e, "failed to apply schema snapshot");
            return Some(Message::PairDdlAck(Bytes::from("apply error".as_bytes())));
        }

        tracing::info!("schema snapshot applied successfully");
        Some(Message::PairDdlAck(Bytes::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_operation_roundtrip_create_keyspace() {
        let ks = KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: std::collections::HashMap::from([(
                    "replication_factor".to_string(),
                    "1".to_string(),
                )]),
            },
        };
        let op = DdlOperation::CreateKeyspace(ks);
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, DdlOperation::CreateKeyspace(k) if k.name == "test_ks"));
    }

    #[test]
    fn ddl_operation_roundtrip_drop_table() {
        let op = DdlOperation::DropTable {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
        };
        let bytes = op.to_bytes().unwrap();
        let decoded = DdlOperation::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, DdlOperation::DropTable { keyspace, table } if keyspace == "ks" && table == "tbl"));
    }
}
```

- [ ] **Step 5: Add module to pair/mod.rs**

In `ferrosa-cluster/src/pair/mod.rs`, add:

```rust
pub mod ddl;
```

- [ ] **Step 6: Add serde_json dependency**

In `ferrosa-cluster/Cargo.toml`, add to `[dependencies]`:

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

- [ ] **Step 7: Build and test**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass including new DDL roundtrip tests.

- [ ] **Step 8: Commit**

```bash
git add ferrosa-cluster/
git commit -m "feat(cluster): add DdlPath, DdlOperation, DdlCoordinator, and DDL RPC handlers"
```

---

## Chunk 4: Controller Integration (catch-up schema sync)

### Task 6: Wire schema sync into catch-up flow

**Files:**

- Modify: `ferrosa-cluster/src/controller.rs:280+`

- [ ] **Step 1: Add schema and ddl_path to ModeController**

The `ModeController` needs access to `Schema` and must create a `DdlPath`
alongside the `WritePath`. Add `schema: Arc<Schema>` field and
`ddl_path: Arc<ArcSwap<DdlPath>>` to the struct and constructor. Update
`ModeControllerHandles` to include `ddl_path`.

- [ ] **Step 2: Send PairSchemaSync before mutations in catch-up**

In `controller.rs`, in the `if was_promoted { ... }` block, after sending
`RoleSwap` and before replaying mutations, add:

```rust
// Send schema snapshot before mutation replay.
let snap = schema.snapshot();
match serde_json::to_vec(&*snap) {
    Ok(json) => {
        if json.len() > 16 * 1024 * 1024 {
            tracing::warn!(size = json.len(), "schema snapshot exceeds 16 MiB limit");
        } else {
            match pm
                .send(
                    peer_host_id,
                    Message::PairSchemaSync(Bytes::from(json)),
                    Lane::Bulk,
                )
                .await
            {
                Ok(_) => tracing::info!("schema snapshot sent to rejoined peer"),
                Err(e) => tracing::warn!(%e, "failed to send schema snapshot"),
            }
        }
    }
    Err(e) => tracing::warn!(%e, "failed to serialize schema snapshot"),
}
```

- [ ] **Step 3: Register PairSchemaSyncHandler and PairDdlForwardHandler**

In `transition_to_pair`, after registering write/role-swap handlers, add:

```rust
let ddl_handler = Arc::new(crate::pair::ddl::PairDdlForwardHandler::new(
    role_arc.clone(),
    self.schema.clone(),
    self.storage.clone(),
    peer_host_id,
    peer_manager.clone(),
));
self.registry.register(MsgType::PairDdlForward, ddl_handler);

let schema_sync_handler = Arc::new(crate::pair::ddl::PairSchemaSyncHandler::new(
    self.schema.clone(),
    self.storage.clone(),
));
self.registry.register(MsgType::PairSchemaSync, schema_sync_handler);
```

- [ ] **Step 4: Create DdlCoordinator in transition_to_pair and swap DdlPath**

```rust
let ddl_coordinator = Arc::new(crate::pair::ddl::DdlCoordinator::new(
    role_arc.clone(),
    peer_host_id,
    self.schema.clone(),
    self.storage.clone(),
    peer_manager.clone(),
));
self.ddl_path.store(Arc::new(DdlPath::Pair(ddl_coordinator)));
```

- [ ] **Step 5: Update transition_to_degraded to set DdlPath::Unavailable**

```rust
fn transition_to_degraded(&self) {
    self.write_path.store(Arc::new(WritePath::unavailable()));
    self.ddl_path.store(Arc::new(DdlPath::Unavailable));
    // ... rest unchanged
}
```

- [ ] **Step 6: Update force_promote to set DdlPath::Direct**

```rust
pub fn force_promote(&self) -> Result<()> {
    self.write_path.store(Arc::new(WritePath::direct(self.storage.clone())));
    self.ddl_path.store(Arc::new(DdlPath::Direct {
        schema: self.schema.clone(),
        engine: self.storage.clone(),
    }));
    // ... rest unchanged
}
```

- [ ] **Step 7: Build and test**

Run: `cargo test -p ferrosa-cluster`
Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add ferrosa-cluster/
git commit -m "feat(cluster): wire schema sync into catch-up and register DDL handlers"
```

---

## Chunk 5: CQL Router Integration + Binary Wiring

### Task 7: Route DDL through DdlPath in CQL router

**Files:**

- Modify: `ferrosa-cql/src/router.rs:885-1132`
- Modify: `ferrosa/src/main.rs`

- [ ] **Step 1: Add ddl_path to SharedState**

In `ferrosa-cql/src/router.rs`, add to `SharedState`:

```rust
pub ddl_path: Arc<ArcSwap<ferrosa_cluster::DdlPath>>,
```

- [ ] **Step 2: Update route_create_keyspace to use DdlPath**

Replace the direct `schema.create_keyspace()` call with DdlPath dispatch.
When DdlPath is Pair, create a `DdlOperation::CreateKeyspace` and call
`ddl_coordinator.coordinate_ddl()`. When Direct, call schema + engine
directly (existing behavior). When Unavailable, return error.

- [ ] **Step 3: Update route_create_table to use DdlPath**

Same pattern: wrap the existing `schema.create_table()` +
`engine.register_table()` calls in DdlPath dispatch.

- [ ] **Step 4: Update route_drop_keyspace and route_drop_table**

Same pattern for drops.

- [ ] **Step 5: Wire ddl_path in main.rs**

In `ferrosa/src/main.rs`, update `SharedState` construction to include
`ddl_path: handles.ddl_path`.

- [ ] **Step 6: Build and run full test suite**

Run: `cargo test`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add ferrosa-cql/ ferrosa/ ferrosa-cluster/
git commit -m "feat(cql): route DDL through DdlPath for pair mode replication"
```

---

## Chunk 6: Smoke Test Update + Nightly CI

### Task 8: Update Docker smoke test

**Files:**

- Modify: `tests/docker-smoke.sh`

- [ ] **Step 1: Remove dual-create workaround in Phase 1**

Remove the lines that create schema on node2 separately. Create only on
node1, then verify the schema appears on node2.

- [ ] **Step 2: Remove manual re-create in Phase 4**

Remove the `CREATE KEYSPACE IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS`
workaround after node1 restart. Schema should arrive via catch-up.

- [ ] **Step 3: Make Phase 4 catch-up a hard pass/fail**

Change the SKIP to a FAIL if failover data doesn't replicate.

- [ ] **Step 4: Run the smoke test**

Run: `bash tests/docker-smoke.sh`
Expected: All 5 phases pass.

- [ ] **Step 5: Commit**

```bash
git add tests/docker-smoke.sh
git commit -m "test: update smoke test for schema replication (remove dual-create workaround)"
```

### Task 9: Add nightly CI job for Docker smoke test

**Files:**

- Modify: `.github/workflows/nightly-fuzz.yml` (or create new workflow)

- [ ] **Step 6: Add smoke-test job**

Add to the nightly workflow:

```yaml
  smoke-test:
    name: Docker Smoke Test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run Docker smoke test
        run: bash tests/docker-smoke.sh
        timeout-minutes: 10
```

- [ ] **Step 7: Commit**

```bash
git add .github/workflows/
git commit -m "ci: add Docker smoke test to nightly workflow"
```
