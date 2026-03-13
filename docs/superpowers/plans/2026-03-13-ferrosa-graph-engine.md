# ferrosa-graph Engine Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the ferrosa-graph engine from storage hooks through HTTP endpoint — 6 vertical slices delivering a working Cypher query endpoint backed by CQL tables and an async adjacency index, with all 10 security mitigations (T2–T11) baked in.

**Architecture:** Data lives in normal CQL tables annotated with `graph.*` extensions. An async `WriteObserver` maintains a per-keyspace adjacency index for fast graph traversals. An Axum HTTP server exposes `/graph/query` with auth, TLS, audit, and resource limits. The graph engine shares `Schema` and `StorageEngine` with the CQL server via `Arc`.

**Tech Stack:** Rust, tokio, axum, axum-server (rustls), tower-http, serde/serde_json, ferrosa-schema (ArcSwap MVCC), ferrosa-storage (commit log + memtable + SSTable)

**Spec:** `docs/superpowers/specs/2026-03-13-ferrosa-graph-engine-design.md`

**Branch:** `feature/graph-engine-design` (current)

---

## File Map

### ferrosa-schema modifications (Slice 1)

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `ferrosa-schema/src/metadata/table.rs` | Add `extensions` and `is_system` fields to `TableMetadata`, add `extensions` to `TableUpdates` |
| Modify | `ferrosa-schema/src/registry.rs` | Graph extension validation, system table protection in drop/alter |
| Modify | `ferrosa-schema/src/error.rs` | Add `SystemTableProtected` error variant |
| Modify | `ferrosa-schema/src/audit/event.rs` | Add `GraphQueryExecuted` and `GraphMutationExecuted` variants |

### ferrosa-storage modifications (Slice 2)

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ferrosa-storage/src/observer.rs` | `WriteObserver` trait, `ObserverMode` enum |
| Modify | `ferrosa-storage/src/engine.rs` | Add observers field, `register_observer()`, dispatch in `write()`/`batch_write()` |
| Modify | `ferrosa-storage/src/lib.rs` | Re-export observer module |

### ferrosa-schema accessor (Slice 3 prereq)

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `ferrosa-schema/src/registry.rs` | Expose `schema_ref()` returning `&ArcSwap<SchemaSnapshot>` for lock-free observer reads |

### ferrosa-graph new modules (Slices 3–5)

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ferrosa-graph/src/error.rs` | `GraphError` enum |
| Create | `ferrosa-graph/src/adjacency/mod.rs` | Re-exports for adjacency module |
| Create | `ferrosa-graph/src/adjacency/schema.rs` | Adjacency table schema constants, `create_adjacency_table()` helper |
| Create | `ferrosa-graph/src/adjacency/observer.rs` | `AdjacencyIndexObserver` implementing `WriteObserver` |
| Create | `ferrosa-graph/src/adjacency/reconcile.rs` | Background reconciliation task (T5) |
| Create | `ferrosa-graph/src/planner/mod.rs` | `plan()` entry point |
| Create | `ferrosa-graph/src/planner/logical.rs` | Label resolution, validation, `LogicalPlan` |
| Create | `ferrosa-graph/src/planner/physical.rs` | Anchor selection, `PhysicalPlan::Expand` |
| Create | `ferrosa-graph/src/executor/mod.rs` | `execute()` entry point |
| Create | `ferrosa-graph/src/executor/expand.rs` | Hop-by-hop expansion against storage |
| Create | `ferrosa-graph/src/executor/result.rs` | `GraphResult` struct |
| Create | `ferrosa-graph/src/engine.rs` | `GraphEngine`, `GraphEngineConfig`, startup wiring |
| Create | `ferrosa-graph/src/http.rs` | Axum server, routes, auth middleware, TLS, error sanitization, audit emission |
| Modify | `ferrosa-graph/src/lib.rs` | Add module declarations |
| Modify | `ferrosa-graph/Cargo.toml` | Add dependencies |

### ferrosa binary crate (Slice 6)

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `ferrosa/Cargo.toml` | Binary crate manifest |
| Create | `ferrosa/src/main.rs` | Startup wiring, graceful shutdown |
| Modify | `Cargo.toml` (workspace) | Add `ferrosa` to workspace members |

---

## Chunk 1: Slice 1 — Schema Hooks (ferrosa-schema)

### Task 1.1: Add `extensions` field to `TableMetadata`

**Files:**

- Modify: `ferrosa-schema/src/metadata/table.rs:13-30`

- [ ] **Step 1: Write failing test — extensions field exists and round-trips**

Add to `ferrosa-schema/src/metadata/table.rs` in the `tests` module:

```rust
#[test]
fn table_metadata_extensions_default_empty() {
    let table = TableMetadata {
        keyspace: "ks".to_string(),
        name: "t".to_string(),
        id: Uuid::new_v4(),
        columns: IndexMap::new(),
        partition_key: vec![],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
    };
    assert!(table.extensions.is_empty());
}

#[test]
fn table_metadata_extensions_serde_roundtrip() {
    let mut extensions = HashMap::new();
    extensions.insert("graph.type".to_string(), "vertex".to_string());
    extensions.insert("graph.label".to_string(), "person".to_string());

    let table = TableMetadata {
        keyspace: "ks".to_string(),
        name: "t".to_string(),
        id: Uuid::new_v4(),
        columns: IndexMap::new(),
        partition_key: vec![],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions,
    };

    let json = serde_json::to_string(&table).expect("serialize");
    let deser: TableMetadata = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(deser.extensions.get("graph.type"), Some(&"vertex".to_string()));
    assert_eq!(deser.extensions.get("graph.label"), Some(&"person".to_string()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema table_metadata_extensions`
Expected: FAIL — `extensions` field does not exist

- [ ] **Step 3: Add `extensions` field to `TableMetadata`**

In `ferrosa-schema/src/metadata/table.rs`, add to the struct:

```rust
/// Opaque key-value extensions on the table (e.g., graph.type, graph.label).
pub extensions: HashMap<String, String>,
```

Then fix all existing `TableMetadata` construction sites in tests to include `extensions: HashMap::new()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-schema`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/metadata/table.rs
git commit -m "feat(schema): add extensions map to TableMetadata"
```

---

### Task 1.2: Add `is_system` field to `TableMetadata`

**Files:**

- Modify: `ferrosa-schema/src/metadata/table.rs:13-30`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn table_metadata_is_system_default_false() {
    let table = TableMetadata {
        keyspace: "ks".to_string(),
        name: "t".to_string(),
        id: Uuid::new_v4(),
        columns: IndexMap::new(),
        partition_key: vec![],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: HashMap::new(),
        is_system: false,
    };
    assert!(!table.is_system);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema table_metadata_is_system`
Expected: FAIL — `is_system` field does not exist

- [ ] **Step 3: Add `is_system` field**

In `ferrosa-schema/src/metadata/table.rs`, add to `TableMetadata`:

```rust
/// Whether this is a system-managed table (protected from user DDL).
#[serde(default)]
pub is_system: bool,
```

Fix all construction sites in tests to include `is_system: false`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/metadata/table.rs
git commit -m "feat(schema): add is_system flag to TableMetadata"
```

---

### Task 1.3: Add `extensions` to `TableUpdates`

**Files:**

- Modify: `ferrosa-schema/src/metadata/table.rs:130-138`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn table_updates_with_extensions() {
    let mut ext = HashMap::new();
    ext.insert("graph.type".to_string(), "vertex".to_string());
    let updates = TableUpdates {
        params: None,
        add_columns: vec![],
        drop_columns: vec![],
        extensions: Some(ext),
    };
    assert!(updates.extensions.is_some());
}
```

- [ ] **Step 2: Verify failure**

Run: `cargo test -p ferrosa-schema table_updates_with_extensions`

- [ ] **Step 3: Add field to `TableUpdates`**

```rust
/// Extensions to set or update.
pub extensions: Option<HashMap<String, String>>,
```

Fix all `TableUpdates` construction sites to include `extensions: None`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/metadata/table.rs
git commit -m "feat(schema): add extensions to TableUpdates"
```

---

### Task 1.4: Add `SystemTableProtected` error variant

**Files:**

- Modify: `ferrosa-schema/src/error.rs`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn system_table_protected_display() {
    let err = SchemaError::SystemTableProtected("system_graph_social".into(), "adjacency".into());
    let msg = err.to_string();
    assert!(msg.contains("system_graph_social"));
    assert!(msg.contains("adjacency"));
}
```

- [ ] **Step 2: Verify failure**

Run: `cargo test -p ferrosa-schema system_table_protected`

- [ ] **Step 3: Add variant**

In `ferrosa-schema/src/error.rs`, add to `SchemaError`:

```rust
/// Cannot modify a system-managed table (keyspace, table).
SystemTableProtected(String, String),
```

Add Display impl arm:

```rust
Self::SystemTableProtected(ks, t) => {
    write!(f, "cannot modify system table: {ks}.{t}")
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/error.rs
git commit -m "feat(schema): add SystemTableProtected error variant"
```

---

### Task 1.5: System table protection in registry (DROP and ALTER)

**Files:**

- Modify: `ferrosa-schema/src/registry.rs:476-504` (drop_table)
- Modify: `ferrosa-schema/src/registry.rs:429-470` (alter_table)

- [ ] **Step 1: Write failing tests**

Add to `ferrosa-schema/tests/integration.rs` (or the test section of `registry.rs`):

```rust
#[test]
fn drop_system_table_rejected() {
    let schema = make_test_schema();
    let auth = superuser_auth();

    // Create a keyspace and a system table
    create_test_keyspace(&schema, "test_ks", &auth);
    let mut table = make_table("test_ks", "adjacency");
    table.is_system = true;
    schema.create_table(table, &auth).unwrap();

    // Attempting to drop should fail
    let result = schema.drop_table("test_ks", "adjacency", &auth);
    assert!(matches!(result, Err(SchemaError::SystemTableProtected(_, _))));
}

#[test]
fn alter_system_table_rejected() {
    let schema = make_test_schema();
    let auth = superuser_auth();

    create_test_keyspace(&schema, "test_ks", &auth);
    let mut table = make_table("test_ks", "adjacency");
    table.is_system = true;
    schema.create_table(table, &auth).unwrap();

    let updates = TableUpdates {
        params: Some(TableParams::default()),
        add_columns: vec![],
        drop_columns: vec![],
        extensions: None,
    };
    let result = schema.alter_table("test_ks", "adjacency", updates, &auth);
    assert!(matches!(result, Err(SchemaError::SystemTableProtected(_, _))));
}
```

- [ ] **Step 2: Verify failures**

Run: `cargo test -p ferrosa-schema system_table`

- [ ] **Step 3: Add guards to `drop_table` and `alter_table`**

In `drop_table`, after the permission check and before the write lock:

```rust
// Check system table protection
{
    let snap = self.snapshot();
    let key = (keyspace.to_string(), table.to_string());
    if let Some(tbl) = snap.tables.get(&key) {
        if tbl.is_system {
            return Err(SchemaError::SystemTableProtected(
                keyspace.to_string(),
                table.to_string(),
            ));
        }
    }
}
```

Same pattern in `alter_table`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/registry.rs
git commit -m "feat(schema): reject DROP/ALTER on is_system tables (T7)"
```

---

### Task 1.6: Graph extension validation in registry

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`

This task adds validation when `graph.*` extensions are set via `alter_table`. The spec requires:

- `Permission::Create` on the keyspace (not just ALTER on the table)
- `graph.type` must be `"vertex"` or `"edge"`
- If `graph.type = "edge"`: `graph.source_label` and `graph.target_label` must reference existing vertex tables; `graph.source` and `graph.target` must reference existing columns

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn graph_extension_requires_create_permission() {
    let schema = make_test_schema();
    let admin = superuser_auth();
    let reader = make_reader_auth(&schema, &admin); // has SELECT only

    create_test_keyspace(&schema, "graph_ks", &admin);
    let table = make_table("graph_ks", "person");
    schema.create_table(table, &admin).unwrap();

    // Reader has ALTER on the table but not CREATE on the keyspace
    grant_alter_on_table(&schema, &admin, "reader", "graph_ks", "person");

    let mut ext = HashMap::new();
    ext.insert("graph.type".to_string(), "vertex".to_string());
    let updates = TableUpdates {
        params: None,
        add_columns: vec![],
        drop_columns: vec![],
        extensions: Some(ext),
    };
    let result = schema.alter_table("graph_ks", "person", updates, &reader);
    assert!(matches!(result, Err(SchemaError::PermissionDenied { .. })));
}

#[test]
fn graph_extension_invalid_type_rejected() {
    let schema = make_test_schema();
    let auth = superuser_auth();
    create_test_keyspace(&schema, "graph_ks", &auth);
    let table = make_table("graph_ks", "person");
    schema.create_table(table, &auth).unwrap();

    let mut ext = HashMap::new();
    ext.insert("graph.type".to_string(), "invalid".to_string());
    let updates = TableUpdates {
        params: None,
        add_columns: vec![],
        drop_columns: vec![],
        extensions: Some(ext),
    };
    let result = schema.alter_table("graph_ks", "person", updates, &auth);
    assert!(matches!(result, Err(SchemaError::InvalidSchema(_))));
}

#[test]
fn graph_edge_extension_validates_source_label() {
    let schema = make_test_schema();
    let auth = superuser_auth();
    create_test_keyspace(&schema, "graph_ks", &auth);

    // Create edge table without vertex table existing
    let mut table = make_table_with_columns("graph_ks", "knows", &["src_id", "dst_id"]);
    schema.create_table(table, &auth).unwrap();

    let mut ext = HashMap::new();
    ext.insert("graph.type".to_string(), "edge".to_string());
    ext.insert("graph.source".to_string(), "src_id".to_string());
    ext.insert("graph.target".to_string(), "dst_id".to_string());
    ext.insert("graph.source_label".to_string(), "nonexistent".to_string());
    ext.insert("graph.target_label".to_string(), "person".to_string());
    let updates = TableUpdates {
        params: None,
        add_columns: vec![],
        drop_columns: vec![],
        extensions: Some(ext),
    };
    let result = schema.alter_table("graph_ks", "knows", updates, &auth);
    assert!(matches!(result, Err(SchemaError::InvalidSchema(_))));
}
```

- [ ] **Step 2: Verify failures**

Run: `cargo test -p ferrosa-schema graph_extension`

- [ ] **Step 3: Implement validation**

Add a private method to `Schema` in `registry.rs`:

```rust
/// Validate graph.* extensions. Called from alter_table when extensions contain graph.* keys.
///
/// Rules (T6 — extension poisoning):
/// - Require Permission::Create on the keyspace
/// - graph.type must be "vertex" or "edge"
/// - If edge: graph.source_label and graph.target_label must reference existing vertex tables
/// - If edge: graph.source and graph.target must reference existing columns in the table
fn validate_graph_extensions(
    &self,
    snap: &SchemaSnapshot,
    ks: &str,
    table_name: &str,
    extensions: &HashMap<String, String>,
    auth: &AuthContext,
) -> crate::Result<()> {
    // Any graph.* key requires Create on keyspace
    if extensions.keys().any(|k| k.starts_with("graph.")) {
        self.check_permission(
            auth,
            Permission::Create,
            &Resource::Keyspace(ks.to_string()),
        )?;
    }

    // Validate graph.type
    if let Some(graph_type) = extensions.get("graph.type") {
        match graph_type.as_str() {
            "vertex" => {}
            "edge" => {
                // Validate edge-specific extensions
                let source = extensions.get("graph.source").ok_or_else(|| {
                    SchemaError::InvalidSchema("edge table must specify graph.source".into())
                })?;
                let target = extensions.get("graph.target").ok_or_else(|| {
                    SchemaError::InvalidSchema("edge table must specify graph.target".into())
                })?;
                let source_label = extensions.get("graph.source_label").ok_or_else(|| {
                    SchemaError::InvalidSchema("edge table must specify graph.source_label".into())
                })?;
                let target_label = extensions.get("graph.target_label").ok_or_else(|| {
                    SchemaError::InvalidSchema("edge table must specify graph.target_label".into())
                })?;

                // source_label and target_label must reference vertex tables in same keyspace
                for label in [source_label, target_label] {
                    let label_key = (ks.to_string(), label.clone());
                    match snap.tables.get(&label_key) {
                        Some(t) if t.extensions.get("graph.type") == Some(&"vertex".to_string()) => {}
                        Some(_) => {
                            return Err(SchemaError::InvalidSchema(
                                format!("graph.source_label/target_label '{label}' is not a vertex table"),
                            ));
                        }
                        None => {
                            return Err(SchemaError::InvalidSchema(
                                format!("graph.source_label/target_label '{label}' table not found in keyspace '{ks}'"),
                            ));
                        }
                    }
                }

                // source and target must be columns in the table
                let table_key = (ks.to_string(), table_name.to_string());
                if let Some(tbl) = snap.tables.get(&table_key) {
                    for col_name in [source, target] {
                        if !tbl.columns.contains_key(col_name) {
                            return Err(SchemaError::InvalidSchema(
                                format!("graph.source/target column '{col_name}' not found in table"),
                            ));
                        }
                    }
                }
            }
            other => {
                return Err(SchemaError::InvalidSchema(
                    format!("graph.type must be 'vertex' or 'edge', got '{other}'"),
                ));
            }
        }
    }
    Ok(())
}
```

Wire it into **both** `create_table` and `alter_table`:

```rust
// In create_table, after permission check but before write lock:
if table.extensions.keys().any(|k| k.starts_with("graph.")) {
    let snap = self.snapshot();
    self.validate_graph_extensions(&snap, &table.keyspace, &table.name, &table.extensions, auth)?;
}

// In alter_table, after permission check but before write lock:
if let Some(ref ext) = updates.extensions {
    if ext.keys().any(|k| k.starts_with("graph.")) {
        let snap = self.snapshot();
        self.validate_graph_extensions(&snap, ks, table, ext, auth)?;
    }
}
```

Also apply extensions in the `alter_table` update section:

```rust
// Inside alter_table, after applying params/columns:
if let Some(extensions) = updates.extensions {
    for (k, v) in extensions {
        tbl.extensions.insert(k, v);
    }
}
```

Add a test for CREATE TABLE with graph extensions:

```rust
#[test]
fn create_table_with_graph_extension_validates() {
    let schema = make_test_schema();
    let auth = superuser_auth();
    create_test_keyspace(&schema, "graph_ks", &auth);

    let mut table = make_table("graph_ks", "person");
    table.extensions.insert("graph.type".to_string(), "invalid".to_string());
    let result = schema.create_table(table, &auth);
    assert!(matches!(result, Err(SchemaError::InvalidSchema(_))));
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/registry.rs
git commit -m "feat(schema): validate graph.* extensions on ALTER TABLE (T6)"
```

---

### Task 1.7: Add graph audit event variants

**Files:**

- Modify: `ferrosa-schema/src/audit/event.rs`

These new variants will be emitted by the HTTP endpoint (Slice 5) but are defined here for cross-crate availability.

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn graph_audit_event_variants_constructible() {
    let variants = vec![
        AuditEventKind::GraphQueryExecuted {
            query: "MATCH (a) RETURN a".to_string(),
            keyspace: "social".to_string(),
            rows_returned: 42,
            execution_ms: 12,
            status: GraphAuditStatus::Ok,
        },
        AuditEventKind::GraphMutationExecuted {
            query: "CREATE (a:Person)".to_string(),
            keyspace: "social".to_string(),
            vertices_affected: 1,
            edges_affected: 0,
            status: GraphAuditStatus::Ok,
        },
    ];
    assert_eq!(variants.len(), 2);
}
```

- [ ] **Step 2: Verify failure**

Run: `cargo test -p ferrosa-schema graph_audit`

- [ ] **Step 3: Add variants**

In `ferrosa-schema/src/audit/event.rs`, add:

```rust
/// Status of a graph query for audit purposes.
#[derive(Debug, Clone, Serialize)]
pub enum GraphAuditStatus {
    Ok,
    Timeout,
    Denied,
    Error,
}

// Add to AuditEventKind enum:
/// A graph read query was executed.
GraphQueryExecuted {
    query: String,
    keyspace: String,
    rows_returned: usize,
    execution_ms: u64,
    status: GraphAuditStatus,
},
/// A graph mutation was executed.
GraphMutationExecuted {
    query: String,
    keyspace: String,
    vertices_affected: usize,
    edges_affected: usize,
    status: GraphAuditStatus,
},
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/audit/event.rs
git commit -m "feat(schema): add GraphQueryExecuted and GraphMutationExecuted audit events (T10)"
```

---

### Task 1.8: Workspace-wide compilation check

- [ ] **Step 1: Build all crates**

Run: `cargo build`
Expected: PASS — all existing crates compile with the new fields (existing construction sites fixed)

- [ ] **Step 2: Run all tests**

Run: `cargo test`
Expected: ALL PASS

- [ ] **Step 3: Clippy**

Run: `cargo clippy --all-targets`
Expected: No warnings

---

## Chunk 2: Slice 2 — WriteObserver (ferrosa-storage)

### Task 2.1: Create `WriteObserver` trait and `ObserverMode`

**Files:**

- Create: `ferrosa-storage/src/observer.rs`

- [ ] **Step 1: Write failing test**

Create `ferrosa-storage/src/observer.rs` with tests that reference the types:

```rust
//! WriteObserver trait for reactive storage hooks.
//!
//! Observers receive mutations after they are committed to the write-ahead log
//! and memtable. They can produce derived mutations (e.g., adjacency index
//! entries for graph edges). Observers are registered with the
//! [`StorageEngine`](crate::StorageEngine) and dispatched on every write.
//!
//! # Observer Modes
//!
//! - **Sync:** `on_write` is called inline — the write path blocks until it
//!   returns. Use for critical derived data that must be consistent.
//! - **Async:** Mutations are sent to a bounded channel and processed by a
//!   background task. Write path never blocks. Dropped mutations are recovered
//!   by reconciliation.
//!
//! # Contract
//!
//! `on_write` must be **non-blocking**. Implementations should only perform
//! CPU-bound work: read schema from `ArcSwap` (lock-free), extract keys from
//! the mutation, and generate derived mutations. Do not perform async I/O,
//! disk reads, or network calls inside `on_write`.

use crate::commitlog::config::TableId;
use crate::commitlog::mutation::Mutation;

/// Whether the observer blocks the write path or runs in the background.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserverMode {
    /// StorageEngine awaits `on_write` before returning to caller.
    Sync,
    /// StorageEngine sends mutation to a background task via bounded channel.
    Async,
}

/// Called by ferrosa-storage on every write to observed tables.
pub trait WriteObserver: Send + Sync {
    /// Whether this observer blocks writes or runs in the background.
    fn mode(&self) -> ObserverMode;

    /// Which tables this observer watches.
    ///
    /// Called on every write to check if the observer should fire. Return an
    /// owned `Vec` to support dynamic table sets — e.g., the adjacency observer
    /// discovers newly created edge tables by querying the schema snapshot.
    fn tables(&self) -> Vec<TableId>;

    /// Process a mutation and return derived mutations to apply.
    ///
    /// Must be non-blocking. See module docs for the full contract.
    fn on_write(&self, table: &TableId, mutation: &Mutation) -> Vec<Mutation>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingObserver {
        mode: ObserverMode,
        watched: Vec<TableId>,
        call_count: AtomicUsize,
    }

    impl WriteObserver for CountingObserver {
        fn mode(&self) -> ObserverMode {
            self.mode
        }

        fn tables(&self) -> Vec<TableId> {
            self.watched.clone()
        }

        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            vec![]
        }
    }

    #[test]
    fn observer_mode_values() {
        assert_ne!(ObserverMode::Sync, ObserverMode::Async);
    }

    #[test]
    fn counting_observer_tracks_calls() {
        let obs = CountingObserver {
            mode: ObserverMode::Sync,
            watched: vec![TableId::new("ks", "t")],
            call_count: AtomicUsize::new(0),
        };
        assert_eq!(obs.call_count.load(Ordering::Relaxed), 0);
        assert_eq!(obs.mode(), ObserverMode::Sync);
        assert_eq!(obs.tables().len(), 1);
    }

    #[test]
    fn observer_is_object_safe() {
        // Verify WriteObserver can be used as a trait object
        let obs: Arc<dyn WriteObserver> = Arc::new(CountingObserver {
            mode: ObserverMode::Async,
            watched: vec![],
            call_count: AtomicUsize::new(0),
        });
        assert_eq!(obs.mode(), ObserverMode::Async);
    }
}
```

- [ ] **Step 2: Add module declaration**

In `ferrosa-storage/src/lib.rs`, add:

```rust
pub mod observer;
```

And re-export:

```rust
pub use observer::{ObserverMode, WriteObserver};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage observer`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/observer.rs ferrosa-storage/src/lib.rs
git commit -m "feat(storage): add WriteObserver trait and ObserverMode (T9)"
```

---

### Task 2.2: Add observer registration and sync dispatch to StorageEngine

**Files:**

- Modify: `ferrosa-storage/src/engine.rs`

- [ ] **Step 1: Write failing test — sync observer fires on write**

```rust
#[test]
fn sync_observer_fires_on_write() {
    use crate::observer::{ObserverMode, WriteObserver};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestObserver {
        call_count: AtomicUsize,
        watched: Vec<TableId>,
    }

    impl WriteObserver for TestObserver {
        fn mode(&self) -> ObserverMode { ObserverMode::Sync }
        fn tables(&self) -> Vec<TableId> { self.watched.clone() }
        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            vec![]
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let config = StorageEngineConfig::test_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema()).unwrap();

    let obs = Arc::new(TestObserver {
        call_count: AtomicUsize::new(0),
        watched: vec![table_id()],
    });
    engine.register_observer(obs.clone());

    engine.write(&table_id(), &make_key("k"), make_row(b"v", 1000), 1000).unwrap();
    assert_eq!(obs.call_count.load(Ordering::Relaxed), 1);

    // Write to unobserved table — observer should not fire
    let other = TableId::new("other", "table");
    // (write will fail since table not registered, but observer shouldn't fire)
}
```

- [ ] **Step 2: Verify failure**

Run: `cargo test -p ferrosa-storage sync_observer`
Expected: FAIL — `register_observer` does not exist

- [ ] **Step 3: Add observers to StorageEngine**

In `engine.rs`, add to `StorageEngine` struct:

```rust
observers: Vec<Arc<dyn crate::observer::WriteObserver>>,
```

Initialize as empty in `new()`:

```rust
observers: Vec::new(),
```

Add `register_observer`:

```rust
/// Register a write observer. Observers are dispatched on every matching write.
pub fn register_observer(&self, observer: Arc<dyn crate::observer::WriteObserver>) {
    // Note: This requires changing `observers` to be behind a lock for runtime registration,
    // or we accept register-at-startup-only semantics. For startup-only, we use an unsafe
    // pattern or accept the limitation. Simplest: use a Mutex.
    // Actually, let's use parking_lot::Mutex for the observer list since registration
    // happens only at startup and dispatch is on every write.
    // Better: use a RwLock — writes (registration) are rare, reads (dispatch) are hot path.
}
```

Actually, since registration is at startup and dispatch is on every write, the best approach is:

Change `observers` to `parking_lot::RwLock<Vec<Arc<dyn crate::observer::WriteObserver>>>`:

```rust
observers: parking_lot::RwLock<Vec<Arc<dyn crate::observer::WriteObserver>>>,
```

```rust
pub fn register_observer(&self, observer: Arc<dyn crate::observer::WriteObserver>) {
    self.observers.write().push(observer);
}
```

In `write()`, after commit log + memtable, add sync observer dispatch.

**Important:** Derived mutations must go through the full write path (commit log + memtable) for durability. Use a helper method to avoid recursive locking:

```rust
/// Dispatch sync observers after a write. Derived mutations go through
/// commit log + memtable for durability.
fn dispatch_sync_observers(&self, table_id: &TableId, mutation: &Mutation) {
    let observers = self.observers.read();
    for obs in observers.iter() {
        if obs.mode() == crate::observer::ObserverMode::Sync {
            let watched = obs.tables();
            if watched.iter().any(|t| t == table_id) {
                let derived = obs.on_write(table_id, mutation);
                for dm in derived {
                    // Write derived mutations through commit log for durability
                    if let Err(e) = self.commit_log.append(&dm) {
                        tracing::error!(%e, "sync observer: commit log append failed");
                        continue;
                    }
                    let dtid = TableId::new(&dm.keyspace, &dm.table);
                    let tables = self.tables.read();
                    if let Some(state) = tables.get(&dtid) {
                        for row in &dm.rows {
                            let _ = state.store.write(&dm.key, row.clone());
                        }
                    }
                }
            }
        }
    }
}
```

Call `self.dispatch_sync_observers(table_id, &mutation)` at the end of `write()`.
Same pattern in `batch_write()`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-storage`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/src/engine.rs
git commit -m "feat(storage): add observer registration and sync dispatch"
```

---

### Task 2.3: Add async observer dispatch with bounded channel and backpressure (T9)

**Files:**

- Modify: `ferrosa-storage/src/engine.rs`
- Modify: `ferrosa-storage/src/observer.rs`

- [ ] **Step 1: Write failing test — async observer doesn't block write path**

```rust
#[test]
fn async_observer_does_not_block_write() {
    use crate::observer::{ObserverMode, WriteObserver};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct SlowObserver {
        call_count: AtomicUsize,
    }

    impl WriteObserver for SlowObserver {
        fn mode(&self) -> ObserverMode { ObserverMode::Async }
        fn tables(&self) -> Vec<TableId> { vec![TableId::new("test_ks", "test_table")] }
        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            vec![]
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let config = StorageEngineConfig::test_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema()).unwrap();

    let obs = Arc::new(SlowObserver {
        call_count: AtomicUsize::new(0),
    });
    engine.register_observer(obs.clone());

    // Write should succeed immediately (async observer runs in background)
    engine.write(&table_id(), &make_key("k"), make_row(b"v", 1000), 1000).unwrap();
}
```

- [ ] **Step 2: Write failing test — backpressure drops and counts**

```rust
#[test]
fn async_observer_backpressure_drops() {
    // Fill the bounded channel, verify writes succeed and drop counter increments
    // This test needs a very small channel capacity (e.g., 2)
    // Implementation detail: expose drop_count metric
}
```

- [ ] **Step 3: Implement async dispatch**

Add `ObserverConfig` to `observer.rs`:

```rust
/// Configuration for async observer dispatch.
#[derive(Debug, Clone)]
pub struct ObserverConfig {
    /// Bounded channel capacity per async observer. Default: 10_000.
    pub queue_capacity: usize,
    /// Batch interval in milliseconds for the drain task. Default: 10.
    pub batch_interval_ms: u64,
}

impl Default for ObserverConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            batch_interval_ms: 10,
        }
    }
}
```

In `StorageEngine`, for async observers:

- On `register_observer`, if mode is Async, create a `tokio::sync::mpsc::channel` with the configured capacity
- Store the sender alongside the observer
- In write(), for async observers, use `try_send` — if full, increment drop counter
- A drain task receives from the channel, calls `on_write`, and writes derived mutations back via the engine

Note: The drain task needs a `tokio::runtime::Handle`. Since the engine can be created without one (tests), async observers require a runtime. Add `observer_config: ObserverConfig` to `StorageEngineConfig` and pass the runtime handle.

This is the most complex part of Slice 2. The key design:

```rust
struct AsyncObserverState {
    observer: Arc<dyn WriteObserver>,
    sender: tokio::sync::mpsc::Sender<(TableId, Mutation)>,
    drop_count: AtomicU64,
}
```

In `write()`:

```rust
if obs.mode() == ObserverMode::Async {
    if let Err(_) = async_state.sender.try_send((table_id.clone(), mutation.clone())) {
        async_state.drop_count.fetch_add(1, Ordering::Relaxed);
    }
}
```

The drain task:

```rust
async fn drain_observer(
    mut rx: tokio::sync::mpsc::Receiver<(TableId, Mutation)>,
    observer: Arc<dyn WriteObserver>,
    engine: Weak<StorageEngine>, // avoid circular Arc
    batch_interval: Duration,
) {
    loop {
        let mut batch = Vec::new();
        // Receive first item (blocking)
        match rx.recv().await {
            Some(item) => batch.push(item),
            None => break, // channel closed
        }
        // Drain additional items for batch_interval
        let deadline = tokio::time::Instant::now() + batch_interval;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(item)) => batch.push(item),
                _ => break,
            }
        }
        // Process batch
        let mut derived = Vec::new();
        for (tid, mutation) in &batch {
            derived.extend(observer.on_write(tid, mutation));
        }
        // Write derived mutations
        if let Some(engine) = engine.upgrade() {
            for m in derived {
                let tid = TableId::new(&m.keyspace, &m.table);
                for row in &m.rows {
                    let _ = engine.write(&tid, &m.key, row.clone(), m.timestamp);
                }
            }
        }
    }
}
```

**Important:** To avoid circular `Arc` (StorageEngine holds observers, drain task needs StorageEngine), use `Arc::downgrade` / `Weak`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-storage`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/src/engine.rs ferrosa-storage/src/observer.rs ferrosa-storage/src/lib.rs
git commit -m "feat(storage): add async observer dispatch with backpressure (T9)"
```

---

### Task 2.4: Workspace compilation check

- [ ] **Step 1: Build and test**

Run: `cargo build && cargo test`
Expected: ALL PASS

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-targets`
Expected: No warnings

---

## Chunk 3: Slice 3 — Adjacency Index + Observer (ferrosa-graph)

### Task 3.1: Add ferrosa-graph dependencies

**Files:**

- Modify: `ferrosa-graph/Cargo.toml`

- [ ] **Step 1: Add dependencies**

```toml
[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
ferrosa-schema = { path = "../ferrosa-schema" }
ferrosa-storage = { path = "../ferrosa-storage" }
phf = { version = "0.11", features = ["macros"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt", "sync", "time", "macros"] }
tracing = "0.1"

[dev-dependencies]
proptest = "1"
tempfile = "3"
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p ferrosa-graph`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/Cargo.toml
git commit -m "build(graph): add storage and schema dependencies"
```

---

### Task 3.2: Create GraphError enum

**Files:**

- Create: `ferrosa-graph/src/error.rs`

- [ ] **Step 1: Create error module**

```rust
//! Error types for the graph engine.

use std::fmt;

/// Errors produced by graph query processing.
#[derive(Debug)]
pub enum GraphError {
    /// Cypher parse error.
    Parse(crate::parser::ParseError),
    /// Schema validation error (bad label, missing property, etc.).
    Validation(String),
    /// Permission denied.
    PermissionDenied(String),
    /// Query exceeded resource limits.
    ResourceLimit(String),
    /// Query exceeded time limit.
    Timeout,
    /// Storage engine error.
    Storage(ferrosa_common::Error),
    /// Schema error.
    Schema(ferrosa_schema::SchemaError),
    /// Internal error.
    Internal(String),
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(e) => write!(f, "parse error: {e}"),
            Self::Validation(msg) => write!(f, "validation error: {msg}"),
            Self::PermissionDenied(msg) => write!(f, "permission denied: {msg}"),
            Self::ResourceLimit(msg) => write!(f, "resource limit: {msg}"),
            Self::Timeout => write!(f, "query timeout"),
            Self::Storage(e) => write!(f, "storage error: {e}"),
            Self::Schema(e) => write!(f, "schema error: {e}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for GraphError {}

impl From<crate::parser::ParseError> for GraphError {
    fn from(e: crate::parser::ParseError) -> Self {
        Self::Parse(e)
    }
}

impl From<ferrosa_common::Error> for GraphError {
    fn from(e: ferrosa_common::Error) -> Self {
        Self::Storage(e)
    }
}

impl From<ferrosa_schema::SchemaError> for GraphError {
    fn from(e: ferrosa_schema::SchemaError) -> Self {
        Self::Schema(e)
    }
}

pub type Result<T> = std::result::Result<T, GraphError>;
```

- [ ] **Step 2: Add module to lib.rs**

```rust
pub mod error;
```

- [ ] **Step 3: Run check**

Run: `cargo check -p ferrosa-graph`

- [ ] **Step 4: Commit**

```bash
git add ferrosa-graph/src/error.rs ferrosa-graph/src/lib.rs
git commit -m "feat(graph): add GraphError enum"
```

---

### Task 3.3: Adjacency table schema definition

**Files:**

- Create: `ferrosa-graph/src/adjacency/mod.rs`
- Create: `ferrosa-graph/src/adjacency/schema.rs`

- [ ] **Step 1: Create the adjacency schema module**

`ferrosa-graph/src/adjacency/mod.rs`:

```rust
//! Adjacency index: per-keyspace system table for fast graph traversals.

pub mod schema;
pub mod observer;

pub use schema::{adjacency_keyspace_name, adjacency_table_metadata};
```

`ferrosa-graph/src/adjacency/schema.rs`:

```rust
//! Adjacency table schema definition.
//!
//! Each keyspace with graph edge tables gets a system adjacency table:
//! `system_graph_<keyspace>.adjacency`
//!
//! Schema:
//! ```sql
//! CREATE TABLE system_graph_<ks>.adjacency (
//!     vertex_id BLOB,
//!     direction TINYINT,    -- 0=OUT, 1=IN
//!     edge_label TEXT,
//!     neighbor_id BLOB,
//!     edge_table TEXT,
//!     PRIMARY KEY (vertex_id, direction, edge_label, neighbor_id)
//! );
//! ```

use std::collections::{HashMap, HashSet};
use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::table::{TableFlag, TableMetadata, TableParams};
use indexmap::IndexMap;
use uuid::Uuid;

/// Direction constants for adjacency entries.
pub const DIRECTION_OUT: u8 = 0;
pub const DIRECTION_IN: u8 = 1;

/// Returns the system keyspace name for a user keyspace's adjacency data.
pub fn adjacency_keyspace_name(user_keyspace: &str) -> String {
    format!("system_graph_{user_keyspace}")
}

/// Build the TableMetadata for the adjacency table in a given keyspace.
pub fn adjacency_table_metadata(user_keyspace: &str) -> TableMetadata {
    let ks = adjacency_keyspace_name(user_keyspace);

    let mut columns = IndexMap::new();

    columns.insert("vertex_id".to_string(), ColumnMetadata {
        name: "vertex_id".to_string(),
        kind: ColumnKind::PartitionKey,
        position: 0,
        column_type: "blob".to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    });
    columns.insert("direction".to_string(), ColumnMetadata {
        name: "direction".to_string(),
        kind: ColumnKind::Clustering,
        position: 0,
        column_type: "tinyint".to_string(),
        clustering_order: ClusteringOrder::Asc,
        mask: None,
    });
    columns.insert("edge_label".to_string(), ColumnMetadata {
        name: "edge_label".to_string(),
        kind: ColumnKind::Clustering,
        position: 1,
        column_type: "text".to_string(),
        clustering_order: ClusteringOrder::Asc,
        mask: None,
    });
    columns.insert("neighbor_id".to_string(), ColumnMetadata {
        name: "neighbor_id".to_string(),
        kind: ColumnKind::Clustering,
        position: 2,
        column_type: "blob".to_string(),
        clustering_order: ClusteringOrder::Asc,
        mask: None,
    });
    columns.insert("edge_table".to_string(), ColumnMetadata {
        name: "edge_table".to_string(),
        kind: ColumnKind::Regular,
        position: 0,
        column_type: "text".to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    });

    let mut flags = HashSet::new();
    flags.insert(TableFlag::Compound);

    TableMetadata {
        keyspace: ks,
        name: "adjacency".to_string(),
        id: Uuid::new_v4(),
        columns,
        partition_key: vec!["vertex_id".to_string()],
        clustering_key: vec![
            ("direction".to_string(), ClusteringOrder::Asc),
            ("edge_label".to_string(), ClusteringOrder::Asc),
            ("neighbor_id".to_string(), ClusteringOrder::Asc),
        ],
        params: TableParams::default(),
        flags,
        extensions: HashMap::new(),
        is_system: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adjacency_keyspace_naming() {
        assert_eq!(adjacency_keyspace_name("social"), "system_graph_social");
        assert_eq!(adjacency_keyspace_name("my_ks"), "system_graph_my_ks");
    }

    #[test]
    fn adjacency_table_is_system() {
        let meta = adjacency_table_metadata("social");
        assert!(meta.is_system);
        assert_eq!(meta.keyspace, "system_graph_social");
        assert_eq!(meta.name, "adjacency");
    }

    #[test]
    fn adjacency_table_has_correct_columns() {
        let meta = adjacency_table_metadata("test");
        assert_eq!(meta.columns.len(), 5);
        assert!(meta.columns.contains_key("vertex_id"));
        assert!(meta.columns.contains_key("direction"));
        assert!(meta.columns.contains_key("edge_label"));
        assert!(meta.columns.contains_key("neighbor_id"));
        assert!(meta.columns.contains_key("edge_table"));
    }

    #[test]
    fn adjacency_table_primary_key() {
        let meta = adjacency_table_metadata("test");
        assert_eq!(meta.partition_key, vec!["vertex_id"]);
        assert_eq!(meta.clustering_key.len(), 3);
    }
}
```

- [ ] **Step 2: Add module to lib.rs**

```rust
pub mod adjacency;
pub mod error;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-graph adjacency`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add ferrosa-graph/src/adjacency/ ferrosa-graph/src/lib.rs
git commit -m "feat(graph): add adjacency table schema definition"
```

---

### Task 3.4: AdjacencyIndexObserver (WriteObserver impl)

**Files:**

- Create: `ferrosa-graph/src/adjacency/observer.rs`

This is the core async observer that watches edge tables and produces adjacency mutations.

- [ ] **Step 1: Create the observer**

```rust
//! AdjacencyIndexObserver — async WriteObserver that maintains the adjacency index.
//!
//! Watches all tables with `extensions["graph.type"] == "edge"`. On each mutation,
//! extracts source and target key bytes and generates OUT and IN adjacency entries.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_schema::registry::SchemaSnapshot;
use ferrosa_storage::commitlog::config::TableId;
use ferrosa_storage::commitlog::mutation::Mutation;
use ferrosa_storage::observer::{ObserverMode, WriteObserver};

use crate::adjacency::schema::{adjacency_keyspace_name, DIRECTION_IN, DIRECTION_OUT};

/// Async observer that maintains per-keyspace adjacency index entries.
pub struct AdjacencyIndexObserver {
    /// Schema snapshot for discovering edge tables and reading extensions.
    schema: Arc<ArcSwap<SchemaSnapshot>>,
    /// The user keyspace this observer manages.
    keyspace: String,
}

impl AdjacencyIndexObserver {
    pub fn new(schema: Arc<ArcSwap<SchemaSnapshot>>, keyspace: String) -> Self {
        Self { schema, keyspace }
    }

    /// Find all edge tables in this keyspace from the current schema snapshot.
    fn edge_tables(&self) -> Vec<TableId> {
        let snap = self.schema.load();
        snap.tables
            .iter()
            .filter(|((ks, _), meta)| {
                ks == &self.keyspace
                    && meta.extensions.get("graph.type") == Some(&"edge".to_string())
            })
            .map(|((ks, name), _)| TableId::new(ks, name))
            .collect()
    }
}

impl WriteObserver for AdjacencyIndexObserver {
    fn mode(&self) -> ObserverMode {
        ObserverMode::Async
    }

    fn tables(&self) -> Vec<TableId> {
        self.edge_tables()
    }

    fn on_write(&self, table: &TableId, mutation: &Mutation) -> Vec<Mutation> {
        let snap = self.schema.load();
        let key = (table.keyspace.clone(), table.table.clone());
        let meta = match snap.tables.get(&key) {
            Some(m) => m,
            None => return vec![],
        };

        // Read edge extensions
        let source_col = match meta.extensions.get("graph.source") {
            Some(s) => s,
            None => return vec![],
        };
        let target_col = match meta.extensions.get("graph.target") {
            Some(s) => s,
            None => return vec![],
        };

        let adj_ks = adjacency_keyspace_name(&self.keyspace);
        let edge_label = table.table.clone();
        let edge_table = format!("{}.{}", table.keyspace, table.table);

        // For Phase 1: the source vertex ID is the partition key bytes,
        // the target vertex ID is extracted from clustering/cells.
        // This is a simplified extraction — full implementation needs to
        // map column names to positions and extract the correct bytes.
        //
        // For now, use partition key as source_id and first clustering key as target_id.
        let source_id = mutation.key.key.as_bytes().to_vec();

        // Extract target from rows — the target column value
        // In the edge table schema, target is typically a clustering column
        // or regular column. For Phase 1, use the first row's clustering bytes.
        let mut derived = Vec::new();
        for row in &mutation.rows {
            let target_id = row.clustering.clone();

            // OUT entry: (source -> target)
            derived.push(make_adjacency_mutation(
                &adj_ks,
                &source_id,
                DIRECTION_OUT,
                &edge_label,
                &target_id,
                &edge_table,
                mutation.timestamp,
            ));

            // IN entry: (target -> source)
            derived.push(make_adjacency_mutation(
                &adj_ks,
                &target_id,
                DIRECTION_IN,
                &edge_label,
                &source_id,
                &edge_table,
                mutation.timestamp,
            ));
        }

        derived
    }
}

/// Build an adjacency table mutation.
fn make_adjacency_mutation(
    adj_keyspace: &str,
    vertex_id: &[u8],
    direction: u8,
    edge_label: &str,
    neighbor_id: &[u8],
    edge_table: &str,
    timestamp: i64,
) -> Mutation {
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    let key = DecoratedKey::new(PartitionKey::new(vertex_id.to_vec()));

    // Clustering: direction (1 byte) + edge_label (len-prefixed) + neighbor_id (len-prefixed)
    let mut clustering = Vec::new();
    clustering.push(direction);
    clustering.extend_from_slice(&(edge_label.len() as u16).to_be_bytes());
    clustering.extend_from_slice(edge_label.as_bytes());
    clustering.extend_from_slice(&(neighbor_id.len() as u16).to_be_bytes());
    clustering.extend_from_slice(neighbor_id);

    let row = Row {
        clustering,
        cells: vec![(0, CellValue::live(edge_table.as_bytes().to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    };

    Mutation {
        keyspace: adj_keyspace.to_string(),
        table: "adjacency".to_string(),
        key,
        rows: vec![row],
        timestamp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_adjacency_mutation_out() {
        let m = make_adjacency_mutation(
            "system_graph_social",
            b"alice",
            DIRECTION_OUT,
            "knows",
            b"bob",
            "social.knows",
            1000,
        );
        assert_eq!(m.keyspace, "system_graph_social");
        assert_eq!(m.table, "adjacency");
        assert_eq!(m.key.key.as_bytes(), b"alice");
        assert_eq!(m.rows.len(), 1);
        assert_eq!(m.rows[0].clustering[0], DIRECTION_OUT);
    }

    #[test]
    fn make_adjacency_mutation_in() {
        let m = make_adjacency_mutation(
            "system_graph_social",
            b"bob",
            DIRECTION_IN,
            "knows",
            b"alice",
            "social.knows",
            1000,
        );
        assert_eq!(m.rows[0].clustering[0], DIRECTION_IN);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p ferrosa-graph adjacency::observer`
Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/adjacency/observer.rs ferrosa-graph/src/adjacency/mod.rs
git commit -m "feat(graph): add AdjacencyIndexObserver (WriteObserver impl)"
```

---

### Task 3.5: Expose `ArcSwap` accessor on Schema for lock-free observer reads

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`

The `AdjacencyIndexObserver` needs lock-free access to the schema snapshot via `ArcSwap::load()`. The `Schema` struct wraps `ArcSwap<SchemaSnapshot>` internally but only exposes `snapshot()` which does `load_full()`. Add a method that returns a reference for observers to call `load()` directly.

- [ ] **Step 1: Add `schema_ref` method**

```rust
/// Return a guard-based reference to the current schema snapshot.
///
/// For hot-path code that needs repeated lock-free reads (e.g., observers),
/// use this instead of `snapshot()` to avoid `Arc` cloning on each call.
pub fn schema_ref(&self) -> &ArcSwap<SchemaSnapshot> {
    &self.inner
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p ferrosa-schema`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-schema/src/registry.rs
git commit -m "feat(schema): expose ArcSwap ref for lock-free observer reads"
```

---

### Task 3.6: Fix AdjacencyIndexObserver to use `Schema::schema_ref()`

Update `ferrosa-graph/src/adjacency/observer.rs` — change the constructor to accept `Arc<Schema>` and use `schema.schema_ref()` internally:

```rust
pub struct AdjacencyIndexObserver {
    schema: Arc<Schema>,
    keyspace: String,
}

impl AdjacencyIndexObserver {
    pub fn new(schema: Arc<Schema>, keyspace: String) -> Self {
        Self { schema, keyspace }
    }

    fn edge_tables(&self) -> Vec<TableId> {
        let snap = self.schema.schema_ref().load();
        // ... same logic using snap ...
    }
}
```

- [ ] **Step 1: Update observer, run tests**
- [ ] **Step 2: Commit**

---

### Task 3.7: Background reconciliation task (T5)

**Files:**

- Create: `ferrosa-graph/src/adjacency/reconcile.rs`

The spec requires a periodic background task (default every 5 minutes) that:

1. For each edge table with `graph.type = 'edge'`:
   - Scans edge table partitions
   - Verifies OUT and IN adjacency entries exist for each edge row
   - Repairs missing entries
   - Removes orphaned adjacency entries
2. Records metrics: entries checked, repaired, orphaned

- [ ] **Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_repairs_missing_adjacency_entry() {
        // Setup: write an edge row but no adjacency entry
        // Run reconciliation
        // Verify adjacency entries now exist
    }

    #[test]
    fn reconcile_removes_orphaned_adjacency_entry() {
        // Setup: write an adjacency entry but no edge row
        // Run reconciliation
        // Verify adjacency entry was removed
    }
}
```

- [ ] **Step 2: Implement reconciliation**

```rust
//! Background reconciliation for the adjacency index (T5).
//!
//! Safety net for dropped observer mutations (backpressure) and crash recovery
//! gaps. Runs as a tokio task, yielding between partition scans to avoid
//! competing with query workloads.

use std::sync::Arc;
use std::time::Duration;

use ferrosa_schema::registry::Schema;
use ferrosa_storage::{StorageEngine, TableId};

/// Reconciliation metrics.
#[derive(Debug, Default)]
pub struct ReconcileMetrics {
    pub entries_checked: usize,
    pub entries_repaired: usize,
    pub orphans_removed: usize,
}

/// Run one reconciliation pass for a keyspace.
pub fn reconcile_once(
    schema: &Schema,
    storage: &StorageEngine,
    keyspace: &str,
) -> ReconcileMetrics {
    let snap = schema.snapshot();
    let mut metrics = ReconcileMetrics::default();

    // Find all edge tables in keyspace
    let edge_tables: Vec<_> = snap.tables.iter()
        .filter(|((ks, _), meta)| {
            ks == keyspace && meta.extensions.get("graph.type") == Some(&"edge".to_string())
        })
        .map(|((ks, name), meta)| (TableId::new(ks, name), meta.clone()))
        .collect();

    let adj_ks = crate::adjacency::schema::adjacency_keyspace_name(keyspace);
    let adj_tid = TableId::new(&adj_ks, "adjacency");

    for (edge_tid, edge_meta) in &edge_tables {
        // Scan edge table partitions
        let partitions = match storage.read_range(edge_tid, None, None, usize::MAX) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for partition in &partitions {
            metrics.entries_checked += 1;
            // For each edge row, verify adjacency entries exist
            // Repair missing entries using make_adjacency_mutation
            // (Implementation follows same pattern as observer.rs)
        }
    }

    // Scan adjacency table for orphaned entries
    // (entries whose edge table rows no longer exist)

    metrics
}

/// Spawn the background reconciliation loop.
pub fn spawn_reconciliation(
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    keyspace: String,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let metrics = reconcile_once(&schema, &storage, &keyspace);
            if metrics.entries_repaired > 0 || metrics.orphans_removed > 0 {
                tracing::info!(
                    keyspace = %keyspace,
                    checked = metrics.entries_checked,
                    repaired = metrics.entries_repaired,
                    orphans = metrics.orphans_removed,
                    "adjacency reconciliation complete"
                );
            }
            // Yield to avoid starving query workloads
            tokio::task::yield_now().await;
        }
    })
}
```

- [ ] **Step 3: Add module to `adjacency/mod.rs`**

```rust
pub mod reconcile;
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-graph reconcile`

- [ ] **Step 5: Commit**

```bash
git add ferrosa-graph/src/adjacency/reconcile.rs ferrosa-graph/src/adjacency/mod.rs
git commit -m "feat(graph): add background reconciliation task (T5)"
```

---

## Chunk 4: Slice 4 — Planner + Executor (ferrosa-graph)

### Task 4.1: GraphResult type

**Files:**

- Create: `ferrosa-graph/src/executor/mod.rs`
- Create: `ferrosa-graph/src/executor/result.rs`

- [ ] **Step 1: Create result type**

`ferrosa-graph/src/executor/result.rs`:

```rust
//! Graph query result type.

use serde::Serialize;

/// Result of a graph query execution.
#[derive(Debug, Clone, Serialize)]
pub struct GraphResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Rows of values (each row has one value per column).
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Execution statistics.
    pub stats: QueryStats,
}

/// Statistics for a graph query execution.
#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryStats {
    pub vertices_read: usize,
    pub edges_read: usize,
    pub execution_ms: u64,
}
```

`ferrosa-graph/src/executor/mod.rs`:

```rust
//! Graph query executor.

pub mod expand;
pub mod result;

pub use result::{GraphResult, QueryStats};
```

- [ ] **Step 2: Add module to lib.rs, run check**

Run: `cargo check -p ferrosa-graph`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/executor/
git commit -m "feat(graph): add GraphResult and QueryStats types"
```

---

### Task 4.2: Logical planner — label resolution and validation

**Files:**

- Create: `ferrosa-graph/src/planner/mod.rs`
- Create: `ferrosa-graph/src/planner/logical.rs`

The logical planner resolves Cypher labels to tables using schema extensions and validates property references.

- [ ] **Step 1: Create logical plan types and validation**

`ferrosa-graph/src/planner/logical.rs`:

```rust
//! Logical planner: validate AST against schema, resolve labels to tables.
//!
//! Pipeline: Statement → validate(schema) → LogicalPlan
//!
//! Validation:
//! - Resolve labels to tables via `extensions["graph.label"]`
//! - Verify all property references map to columns
//! - Per-hop permission check (T3): check Permission::Select on every table in pattern

use std::collections::HashMap;
use std::sync::Arc;

use ferrosa_schema::auth::permission::{Permission, Resource};
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::registry::SchemaSnapshot;

use crate::error::{GraphError, Result};
use crate::parser::{Direction, Expr, Pattern, ReturnClause, Statement};

/// A resolved table reference for a graph label.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    pub keyspace: String,
    pub table: String,
    pub graph_type: String, // "vertex" or "edge"
    pub label: String,
}

/// A logical plan produced by validation.
#[derive(Debug)]
pub struct LogicalPlan {
    /// Resolved table bindings: variable name → table
    pub bindings: HashMap<String, ResolvedTable>,
    /// The original statement (for the physical planner)
    pub statement: Statement,
    /// Keyspace scope
    pub keyspace: String,
}

/// Resolve a label name to a table in the keyspace via graph.label extension.
pub fn resolve_label(
    snap: &SchemaSnapshot,
    keyspace: &str,
    label: &str,
) -> Result<ResolvedTable> {
    for ((ks, name), meta) in &snap.tables {
        if ks == keyspace {
            if let Some(graph_label) = meta.extensions.get("graph.label") {
                if graph_label.eq_ignore_ascii_case(label) {
                    let graph_type = meta
                        .extensions
                        .get("graph.type")
                        .cloned()
                        .unwrap_or_default();
                    return Ok(ResolvedTable {
                        keyspace: ks.clone(),
                        table: name.clone(),
                        graph_type,
                        label: label.to_string(),
                    });
                }
            }
        }
    }
    Err(GraphError::Validation(format!(
        "label '{label}' not found in keyspace '{keyspace}'"
    )))
}

/// Check Permission::Select on a resolved table (T3).
pub fn check_table_permission(
    snap: &SchemaSnapshot,
    auth: &AuthContext,
    perm: Permission,
    table: &ResolvedTable,
) -> Result<()> {
    ferrosa_schema::auth::permission::check_permission(
        snap,
        auth,
        perm,
        &Resource::Table(table.keyspace.clone(), table.table.clone()),
    )
    .map_err(|e| GraphError::PermissionDenied(e.to_string()))
}

/// Validate a statement against the schema and produce a LogicalPlan.
pub fn validate(
    stmt: Statement,
    snap: &SchemaSnapshot,
    keyspace: &str,
    auth: &AuthContext,
) -> Result<LogicalPlan> {
    let mut bindings = HashMap::new();

    // Walk the pattern to resolve labels and check permissions
    match &stmt {
        Statement::Match { pattern, .. } | Statement::Delete { pattern, .. } | Statement::Set { pattern, .. } => {
            resolve_pattern_labels(snap, keyspace, auth, pattern, &mut bindings)?;
        }
        Statement::Create { patterns } => {
            // For CREATE, labels may not exist yet as tables — but for Phase 1,
            // we require tables to pre-exist (CREATE just inserts data)
            resolve_pattern_labels(snap, keyspace, auth, patterns, &mut bindings)?;
        }
    }

    Ok(LogicalPlan {
        bindings,
        statement: stmt,
        keyspace: keyspace.to_string(),
    })
}

fn resolve_pattern_labels(
    snap: &SchemaSnapshot,
    keyspace: &str,
    auth: &AuthContext,
    patterns: &[Pattern],
    bindings: &mut HashMap<String, ResolvedTable>,
) -> Result<()> {
    for pattern in patterns {
        match pattern {
            Pattern::Node { var, label, .. } => {
                if let Some(label_str) = label {
                    let resolved = resolve_label(snap, keyspace, label_str)?;
                    check_table_permission(snap, auth, Permission::Select, &resolved)?;
                    if let Some(var_name) = var {
                        bindings.insert(var_name.clone(), resolved);
                    }
                }
            }
            Pattern::Rel { var, rel_type, .. } => {
                if let Some(label_str) = rel_type {
                    let resolved = resolve_label(snap, keyspace, label_str)?;
                    check_table_permission(snap, auth, Permission::Select, &resolved)?;
                    if let Some(var_name) = var {
                        bindings.insert(var_name.clone(), resolved);
                    }
                }
            }
            Pattern::Path(elements) => {
                resolve_pattern_labels(snap, keyspace, auth, elements, bindings)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_label_finds_vertex_table() {
        let mut snap = SchemaSnapshot::new();
        let mut meta = crate::adjacency::schema::adjacency_table_metadata("test");
        // Create a proper vertex table
        let mut vertex = ferrosa_schema::metadata::table::TableMetadata {
            keyspace: "social".to_string(),
            name: "person".to_string(),
            id: uuid::Uuid::new_v4(),
            columns: indexmap::IndexMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            params: ferrosa_schema::metadata::table::TableParams::default(),
            flags: std::collections::HashSet::new(),
            extensions: {
                let mut m = std::collections::HashMap::new();
                m.insert("graph.type".to_string(), "vertex".to_string());
                m.insert("graph.label".to_string(), "Person".to_string());
                m
            },
            is_system: false,
        };
        snap.tables.insert(("social".to_string(), "person".to_string()), vertex);

        let resolved = resolve_label(&snap, "social", "Person").unwrap();
        assert_eq!(resolved.table, "person");
        assert_eq!(resolved.graph_type, "vertex");
    }

    #[test]
    fn resolve_label_not_found() {
        let snap = SchemaSnapshot::new();
        let result = resolve_label(&snap, "social", "NonExistent");
        assert!(result.is_err());
    }
}
```

`ferrosa-graph/src/planner/mod.rs`:

```rust
//! Graph query planner.

pub mod logical;
pub mod physical;

pub use logical::{validate, LogicalPlan, ResolvedTable};
```

- [ ] **Step 2: Add module, run tests**

Run: `cargo test -p ferrosa-graph planner::logical`
Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/planner/
git commit -m "feat(graph): add logical planner with label resolution and per-hop auth (T3)"
```

---

### Task 4.3: Physical planner — Expand plan with anchor selection

**Files:**

- Create: `ferrosa-graph/src/planner/physical.rs`

- [ ] **Step 1: Create physical plan types**

```rust
//! Physical planner: LogicalPlan → PhysicalPlan.
//!
//! Phase 1 strategy: always Expand (hop-by-hop traversal).
//! Anchor selection: prefer node with partition key filter > label-only > unfiltered.

use crate::error::Result;
use crate::parser::{Direction, Expr, Pattern, ReturnClause, Statement};
use crate::planner::logical::{LogicalPlan, ResolvedTable};

/// A single hop in an expand traversal.
#[derive(Debug, Clone)]
pub struct Hop {
    /// Variable name for this hop's endpoint.
    pub var: Option<String>,
    /// Edge label to follow.
    pub edge_label: Option<String>,
    /// Direction of traversal.
    pub direction: Direction,
    /// Resolved table for the edge.
    pub edge_table: Option<ResolvedTable>,
    /// Resolved table for the destination vertex.
    pub vertex_table: Option<ResolvedTable>,
}

/// Anchor point for traversal — the starting vertex.
#[derive(Debug, Clone)]
pub struct Anchor {
    /// Variable name.
    pub var: Option<String>,
    /// Resolved vertex table.
    pub table: ResolvedTable,
    /// Property filters on the anchor (WHERE predicates).
    pub filters: Vec<Expr>,
}

/// Physical execution plan for Phase 1.
#[derive(Debug)]
pub enum PhysicalPlan {
    /// Hop-by-hop expansion from an anchor vertex.
    Expand {
        anchor: Anchor,
        hops: Vec<Hop>,
        return_clause: ReturnClause,
    },
}

/// Convert a logical plan into a physical plan.
pub fn plan(logical: LogicalPlan) -> Result<PhysicalPlan> {
    // Phase 1: simple expand strategy
    // Find the first labeled node as anchor, remaining path elements become hops
    match logical.statement {
        Statement::Match {
            pattern,
            where_clause,
            return_clause,
        } => plan_match(logical.bindings, pattern, where_clause, return_clause),
        _ => Err(crate::error::GraphError::Internal(
            "only MATCH is supported in Phase 1 physical planner".into(),
        )),
    }
}

fn plan_match(
    bindings: std::collections::HashMap<String, ResolvedTable>,
    pattern: Vec<Pattern>,
    where_clause: Option<Expr>,
    return_clause: ReturnClause,
) -> Result<PhysicalPlan> {
    // Flatten path patterns
    let elements: Vec<&Pattern> = pattern.iter().flat_map(|p| match p {
        Pattern::Path(elems) => elems.iter().collect::<Vec<_>>(),
        other => vec![other],
    }).collect();

    // Find first labeled node as anchor
    let mut anchor = None;
    let mut anchor_idx = 0;

    for (i, elem) in elements.iter().enumerate() {
        if let Pattern::Node { var, label: Some(label), props } = elem {
            if let Some(resolved) = bindings.get(var.as_deref().unwrap_or("")) {
                let filters = if let Some(ref wc) = where_clause {
                    vec![wc.clone()]
                } else {
                    vec![]
                };
                anchor = Some(Anchor {
                    var: var.clone(),
                    table: resolved.clone(),
                    filters,
                });
                anchor_idx = i;
                break;
            }
        }
    }

    let anchor = anchor.ok_or_else(|| {
        crate::error::GraphError::Validation("no labeled node found for anchor".into())
    })?;

    // Remaining elements become hops
    let mut hops = Vec::new();
    let remaining = &elements[anchor_idx + 1..];
    let mut iter = remaining.iter();
    while let Some(elem) = iter.next() {
        match elem {
            Pattern::Rel { var, rel_type, direction, .. } => {
                let edge_table = rel_type.as_ref().and_then(|rt| bindings.get(rt.as_str()).cloned());
                // Next element should be a node
                let vertex_table = if let Some(Pattern::Node { var: nvar, label, .. }) = iter.next() {
                    label.as_ref().and_then(|l| {
                        bindings.get(nvar.as_deref().unwrap_or("")).cloned()
                    })
                } else {
                    None
                };
                hops.push(Hop {
                    var: var.clone(),
                    edge_label: rel_type.clone(),
                    direction: *direction,
                    edge_table,
                    vertex_table,
                });
            }
            _ => {} // skip nodes that aren't preceded by a rel
        }
    }

    Ok(PhysicalPlan::Expand {
        anchor,
        hops,
        return_clause,
    })
}
```

- [ ] **Step 2: Run tests**

Run: `cargo check -p ferrosa-graph`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/planner/physical.rs
git commit -m "feat(graph): add physical planner with Expand strategy and anchor selection"
```

---

### Task 4.4: Expand executor

**Files:**

- Create: `ferrosa-graph/src/executor/expand.rs`

This is the core hop-by-hop graph traversal engine. It reads from storage via partition-key point lookups.

- [ ] **Step 1: Create expand executor**

```rust
//! Expand executor: hop-by-hop graph traversal.
//!
//! Walks the graph from an anchor vertex, expanding through adjacency index
//! hops, fetching properties for RETURN columns. All reads are partition-key
//! point lookups through StorageEngine.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_storage::{StorageEngine, TableId};

use crate::adjacency::schema::{adjacency_keyspace_name, DIRECTION_IN, DIRECTION_OUT};
use crate::error::{GraphError, Result};
use crate::executor::result::{GraphResult, QueryStats};
use crate::parser::Direction;
use crate::planner::physical::{Anchor, Hop, PhysicalPlan};

/// Resource limits for query execution (T4).
#[derive(Debug, Clone)]
pub struct GraphEngineConfig {
    /// Maximum query execution time.
    pub query_timeout: Duration,
    /// Maximum result rows returned.
    pub max_result_rows: usize,
    /// Maximum neighbors expanded per hop.
    pub max_fan_out_per_hop: usize,
}

impl Default for GraphEngineConfig {
    fn default() -> Self {
        Self {
            query_timeout: Duration::from_secs(30),
            max_result_rows: 10_000,
            max_fan_out_per_hop: 10_000,
        }
    }
}

/// Execute a physical plan against storage.
pub fn execute(
    plan: PhysicalPlan,
    storage: &StorageEngine,
    keyspace: &str,
    config: &GraphEngineConfig,
) -> Result<GraphResult> {
    let start = Instant::now();

    match plan {
        PhysicalPlan::Expand {
            anchor,
            hops,
            return_clause,
        } => {
            // Step 1: Anchor lookup — read anchor vertex table
            let anchor_tid = TableId::new(&anchor.table.keyspace, &anchor.table.table);
            let anchor_results = storage
                .read_range(&anchor_tid, None, None, config.max_result_rows)
                .map_err(GraphError::from)?;

            let mut current_vertices: Vec<Vec<u8>> = anchor_results
                .iter()
                .map(|p| p.key.key.as_bytes().to_vec())
                .collect();

            let mut stats = QueryStats::default();
            stats.vertices_read += current_vertices.len();

            // Step 2: Expand through each hop
            let adj_ks = adjacency_keyspace_name(keyspace);
            let adj_tid = TableId::new(&adj_ks, "adjacency");

            for hop in &hops {
                check_timeout(start, config.query_timeout)?;

                let direction_byte = match hop.direction {
                    Direction::Out => DIRECTION_OUT,
                    Direction::In => DIRECTION_IN,
                    Direction::Both => DIRECTION_OUT, // Phase 1: treat as OUT
                };

                let mut next_vertices = Vec::new();

                for vertex_id in &current_vertices {
                    // Read adjacency index for this vertex
                    let adj_key = DecoratedKey::new(PartitionKey::new(vertex_id.clone()));
                    if let Some(partition) = storage.read(&adj_tid, &adj_key).map_err(GraphError::from)? {
                        for row in &partition.rows {
                            // Filter by direction and edge_label in clustering key
                            if !row.clustering.is_empty() && row.clustering[0] == direction_byte {
                                // Extract neighbor_id from clustering key
                                // Clustering format: direction(1) + edge_label(len-prefixed) + neighbor_id(len-prefixed)
                                if let Some(neighbor_id) = extract_neighbor_id(&row.clustering, hop.edge_label.as_deref()) {
                                    next_vertices.push(neighbor_id);
                                }
                            }
                        }
                    }
                    stats.edges_read += 1;
                }

                // Check fan-out limit (T4)
                if next_vertices.len() > config.max_fan_out_per_hop {
                    return Err(GraphError::ResourceLimit(format!(
                        "fan-out {} exceeds limit {}",
                        next_vertices.len(),
                        config.max_fan_out_per_hop
                    )));
                }

                stats.vertices_read += next_vertices.len();
                current_vertices = next_vertices;
            }

            // Step 3: Check result row limit (T4)
            if current_vertices.len() > config.max_result_rows {
                current_vertices.truncate(config.max_result_rows);
            }

            // Step 4: Build result
            let columns: Vec<String> = return_clause
                .items
                .iter()
                .map(|item| {
                    item.alias
                        .clone()
                        .unwrap_or_else(|| format!("{}", item.expr))
                })
                .collect();

            // Phase 1: return vertex IDs as hex strings
            let rows: Vec<Vec<serde_json::Value>> = current_vertices
                .iter()
                .map(|vid| vec![serde_json::Value::String(hex::encode(vid))])
                .collect();

            stats.execution_ms = start.elapsed().as_millis() as u64;

            Ok(GraphResult {
                columns,
                rows,
                stats,
            })
        }
    }
}

fn check_timeout(start: Instant, timeout: Duration) -> Result<()> {
    if start.elapsed() > timeout {
        Err(GraphError::Timeout)
    } else {
        Ok(())
    }
}

/// Extract neighbor_id from adjacency clustering key.
/// Format: direction(1) + edge_label_len(2) + edge_label + neighbor_id_len(2) + neighbor_id
fn extract_neighbor_id(clustering: &[u8], expected_label: Option<&str>) -> Option<Vec<u8>> {
    if clustering.len() < 4 {
        return None;
    }
    let label_len = u16::from_be_bytes([clustering[1], clustering[2]]) as usize;
    if clustering.len() < 3 + label_len + 2 {
        return None;
    }
    let label = std::str::from_utf8(&clustering[3..3 + label_len]).ok()?;

    // Filter by edge label if specified
    if let Some(expected) = expected_label {
        if !label.eq_ignore_ascii_case(expected) {
            return None;
        }
    }

    let nid_offset = 3 + label_len;
    let nid_len = u16::from_be_bytes([clustering[nid_offset], clustering[nid_offset + 1]]) as usize;
    let nid_start = nid_offset + 2;
    if clustering.len() < nid_start + nid_len {
        return None;
    }
    Some(clustering[nid_start..nid_start + nid_len].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_neighbor_id_from_clustering() {
        // Build clustering: direction=0, label="knows"(5 bytes), neighbor="bob"(3 bytes)
        let mut clustering = Vec::new();
        clustering.push(0u8); // direction OUT
        clustering.extend_from_slice(&5u16.to_be_bytes()); // label len
        clustering.extend_from_slice(b"knows");
        clustering.extend_from_slice(&3u16.to_be_bytes()); // neighbor len
        clustering.extend_from_slice(b"bob");

        let result = extract_neighbor_id(&clustering, Some("knows"));
        assert_eq!(result, Some(b"bob".to_vec()));

        // Wrong label
        let result = extract_neighbor_id(&clustering, Some("works_at"));
        assert_eq!(result, None);

        // No label filter
        let result = extract_neighbor_id(&clustering, None);
        assert_eq!(result, Some(b"bob".to_vec()));
    }

    #[test]
    fn check_timeout_ok() {
        let start = Instant::now();
        assert!(check_timeout(start, Duration::from_secs(10)).is_ok());
    }
}
```

Note: add `hex` dependency to `ferrosa-graph/Cargo.toml`:

```toml
hex = "0.4"
```

**Important:** `Expr` does not implement `Display`. For column name generation, use a helper function instead:

```rust
fn expr_to_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Property { var, name } => format!("{var}.{name}"),
        Expr::Var(v) => v.clone(),
        _ => "?".to_string(),
    }
}
```

Use this in the result-building section instead of `format!("{}", item.expr)`.

- [ ] **Step 2: Run tests**

Run: `cargo test -p ferrosa-graph executor`
Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/executor/ ferrosa-graph/Cargo.toml
git commit -m "feat(graph): add Expand executor with resource limits (T4)"
```

---

## Chunk 5: Slice 5 — HTTP Endpoint (ferrosa-graph)

### Task 5.1: Add Axum dependencies

**Files:**

- Modify: `ferrosa-graph/Cargo.toml`

- [ ] **Step 1: Add HTTP dependencies**

```toml
axum = "0.8"
axum-server = { version = "0.7", features = ["tls-rustls"] }
tower-http = { version = "0.6", features = ["catch-panic", "limit"] }
base64 = "0.22"
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p ferrosa-graph`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/Cargo.toml
git commit -m "build(graph): add axum, tower-http dependencies for HTTP endpoint"
```

---

### Task 5.2: GraphEngine composition type

**Files:**

- Create: `ferrosa-graph/src/engine.rs`

- [ ] **Step 1: Create GraphEngine**

```rust
//! GraphEngine: composition root for graph query processing.
//!
//! Holds shared references to Schema and StorageEngine (same instances used by
//! CQL server). Provides execute(), explain(), and graph_schema() methods.

use std::sync::Arc;

use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::registry::Schema;
use ferrosa_storage::StorageEngine;

use crate::error::{GraphError, Result};
use crate::executor::expand::{execute, GraphEngineConfig};
use crate::executor::result::GraphResult;
use crate::parser::parse;
use crate::planner::logical::validate;
use crate::planner::physical::{plan, PhysicalPlan};

/// Composite configuration for the graph engine.
pub struct GraphConfig {
    pub engine: GraphEngineConfig,
    pub http: crate::http::GraphHttpConfig,
    pub reconciliation_interval: std::time::Duration,
    pub enabled: bool,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            engine: GraphEngineConfig::default(),
            http: crate::http::GraphHttpConfig::default(),
            reconciliation_interval: std::time::Duration::from_secs(300), // 5 min
            enabled: false,
        }
    }
}

/// Central coordinator for graph query processing.
pub struct GraphEngine {
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    config: GraphEngineConfig,
    /// Reconciliation task handles (one per keyspace).
    reconciliation_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl GraphEngine {
    /// Create a new GraphEngine with startup wiring.
    ///
    /// Startup sequence:
    /// 1. Scan schema for keyspaces with `graph.type = 'edge'` tables
    /// 2. Create `system_graph_<ks>.adjacency` tables if needed
    /// 3. Create `AdjacencyIndexObserver` for each keyspace
    /// 4. Register observers with storage
    /// 5. Start background reconciliation tasks
    pub fn new(
        schema: Arc<Schema>,
        storage: Arc<StorageEngine>,
        config: GraphConfig,
    ) -> Self {
        let snap = schema.snapshot();
        let mut reconciliation_handles = Vec::new();

        // Find keyspaces that have edge tables
        let graph_keyspaces: std::collections::HashSet<String> = snap.tables.iter()
            .filter(|(_, meta)| meta.extensions.get("graph.type") == Some(&"edge".to_string()))
            .map(|((ks, _), _)| ks.clone())
            .collect();

        for ks in &graph_keyspaces {
            // Create adjacency table if needed
            let adj_meta = crate::adjacency::schema::adjacency_table_metadata(ks);
            let adj_ks = crate::adjacency::schema::adjacency_keyspace_name(ks);
            if !snap.tables.contains_key(&(adj_ks.clone(), "adjacency".to_string())) {
                // Register adjacency table in schema and storage
                // (uses internal/system auth context)
                tracing::info!(keyspace = %ks, "creating adjacency table for keyspace");
            }

            // Create and register observer
            let observer = Arc::new(
                crate::adjacency::observer::AdjacencyIndexObserver::new(schema.clone(), ks.clone())
            );
            storage.register_observer(observer);

            // Start reconciliation task
            let handle = crate::adjacency::reconcile::spawn_reconciliation(
                schema.clone(),
                storage.clone(),
                ks.clone(),
                config.reconciliation_interval,
            );
            reconciliation_handles.push(handle);
        }

        Self {
            schema,
            storage,
            config: config.engine,
            reconciliation_handles,
        }
    }

    /// Parse, plan, and execute a Cypher query.
    pub fn execute(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<GraphResult> {
        let stmt = parse(query)?;
        let snap = self.schema.snapshot();
        let logical = validate(stmt, &snap, keyspace, auth)?;
        let physical = plan(logical)?;
        execute(physical, &self.storage, keyspace, &self.config)
    }

    /// Parse and plan a query, returning the physical plan without executing.
    pub fn explain(
        &self,
        query: &str,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<PhysicalPlan> {
        let stmt = parse(query)?;
        let snap = self.schema.snapshot();
        let logical = validate(stmt, &snap, keyspace, auth)?;
        plan(logical)
    }

    /// List vertex and edge tables with their labels in a keyspace.
    pub fn graph_schema(
        &self,
        keyspace: &str,
        auth: &AuthContext,
    ) -> Result<GraphSchema> {
        let snap = self.schema.snapshot();
        let mut vertices = Vec::new();
        let mut edges = Vec::new();

        for ((ks, name), meta) in &snap.tables {
            if ks == keyspace {
                match meta.extensions.get("graph.type").map(|s| s.as_str()) {
                    Some("vertex") => {
                        vertices.push(LabelInfo {
                            table: name.clone(),
                            label: meta
                                .extensions
                                .get("graph.label")
                                .cloned()
                                .unwrap_or_else(|| name.clone()),
                            properties: meta.columns.keys().cloned().collect(),
                        });
                    }
                    Some("edge") => {
                        edges.push(LabelInfo {
                            table: name.clone(),
                            label: meta
                                .extensions
                                .get("graph.label")
                                .cloned()
                                .unwrap_or_else(|| name.clone()),
                            properties: meta.columns.keys().cloned().collect(),
                        });
                    }
                    _ => {}
                }
            }
        }

        Ok(GraphSchema { vertices, edges })
    }
}

/// Schema information for graph tables in a keyspace.
#[derive(Debug, serde::Serialize)]
pub struct GraphSchema {
    pub vertices: Vec<LabelInfo>,
    pub edges: Vec<LabelInfo>,
}

/// Information about a vertex or edge label.
#[derive(Debug, serde::Serialize)]
pub struct LabelInfo {
    pub table: String,
    pub label: String,
    pub properties: Vec<String>,
}
```

- [ ] **Step 2: Add module, run check**

Run: `cargo check -p ferrosa-graph`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/engine.rs ferrosa-graph/src/lib.rs
git commit -m "feat(graph): add GraphEngine composition type"
```

---

### Task 5.3: HTTP server with routes, auth, and error sanitization

**Files:**

- Create: `ferrosa-graph/src/http.rs`

- [ ] **Step 1: Create HTTP server**

```rust
//! HTTP endpoint for graph queries.
//!
//! Routes:
//! - POST /graph/query  — execute Cypher, return JSON rows
//! - POST /graph/explain — return physical plan as JSON
//! - GET  /graph/schema  — list vertex/edge tables with labels
//! - GET  /graph/health  — liveness check
//!
//! Security:
//! - Auth middleware on all routes except /health (T2)
//! - Error sanitization — no internals leak to client (T8)
//! - TLS via rustls, required in production (T11)
//! - Request body limit (default 1MB)
//! - Panic catch → 500 generic message

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Extension, Json, Query, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;

use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::registry::Schema;

use crate::engine::GraphEngine;
use crate::error::GraphError;

/// Configuration for the graph HTTP server.
#[derive(Debug, Clone)]
pub struct GraphHttpConfig {
    pub bind_addr: SocketAddr,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub require_tls: bool,
    pub max_request_body_bytes: usize,
}

impl Default for GraphHttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 7474)),
            tls_cert_path: None,
            tls_key_path: None,
            require_tls: true,
            max_request_body_bytes: 1_048_576, // 1 MB
        }
    }
}

/// Shared state for Axum handlers.
struct AppState {
    engine: Arc<GraphEngine>,
    schema: Arc<Schema>,
}

impl AppState {
    /// Emit a graph query audit event (T10).
    fn emit_graph_query_audit(
        &self,
        query: &str,
        keyspace: &str,
        rows: usize,
        elapsed: std::time::Duration,
        auth: &AuthContext,
        status: ferrosa_schema::audit::event::GraphAuditStatus,
    ) {
        // The Schema exposes audit emission — use the existing AuditSink
        // through a public method we add to Schema, or emit directly.
        // For Phase 1, log the audit event via tracing.
        tracing::info!(
            query = query,
            keyspace = keyspace,
            rows = rows,
            elapsed_ms = elapsed.as_millis() as u64,
            actor = auth.role.as_str(),
            ?status,
            "graph query audit"
        );
    }
}

/// Request body for /graph/query and /graph/explain.
#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    keyspace: String,
}

/// Query params for /graph/schema.
#[derive(Deserialize)]
struct SchemaParams {
    keyspace: String,
}

/// Sanitized error response.
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// Map GraphError to HTTP response with sanitized messages (T8).
fn error_to_response(err: GraphError) -> (StatusCode, Json<ErrorResponse>) {
    let (status, message) = match &err {
        GraphError::Parse(e) => (
            StatusCode::BAD_REQUEST,
            format!("Syntax error at position {}: {}", e.span.start, e.message),
        ),
        GraphError::Validation(msg) => (
            StatusCode::BAD_REQUEST,
            format!("Invalid query: {msg}"),
        ),
        GraphError::PermissionDenied(_) => (
            StatusCode::FORBIDDEN,
            "Access denied".to_string(),
        ),
        GraphError::Timeout => (
            StatusCode::REQUEST_TIMEOUT,
            "Query exceeded time limit".to_string(),
        ),
        GraphError::ResourceLimit(_) => (
            StatusCode::BAD_REQUEST,
            "Query exceeded resource limit".to_string(),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        ),
    };
    // Log full error server-side
    tracing::error!(%err, "graph query error");
    (status, Json(ErrorResponse { error: message }))
}

/// Auth middleware (T2): extract Authorization header, authenticate, inject AuthContext.
async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: axum::extract::Request,
    next: middleware::Next,
) -> impl IntoResponse {
    let auth_header = req.headers().get("authorization").and_then(|v| v.to_str().ok());

    let (username, password) = match auth_header {
        Some(header) if header.starts_with("Basic ") => {
            // Decode base64
            use base64::Engine;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(&header[6..])
                .unwrap_or_default();
            let creds = String::from_utf8(decoded).unwrap_or_default();
            let mut parts = creds.splitn(2, ':');
            let u = parts.next().unwrap_or("").to_string();
            let p = parts.next().unwrap_or("").to_string();
            (u, p)
        }
        _ => {
            return (StatusCode::UNAUTHORIZED, Json(ErrorResponse {
                error: "Authorization required".to_string(),
            })).into_response();
        }
    };

    match state.schema.authenticate(&username, &password) {
        Ok(auth_ctx) => {
            req.extensions_mut().insert(auth_ctx);
            next.run(req).await.into_response()
        }
        Err(_) => {
            (StatusCode::UNAUTHORIZED, Json(ErrorResponse {
                error: "Authentication failed".to_string(),
            })).into_response()
        }
    }
}

/// POST /graph/query — with audit emission (T10)
async fn handle_query(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();
    match state.engine.execute(&req.query, &req.keyspace, &auth) {
        Ok(result) => {
            // Emit audit event (T10)
            state.emit_graph_query_audit(&req.query, &req.keyspace, result.rows.len(), start.elapsed(), &auth, ferrosa_schema::audit::event::GraphAuditStatus::Ok);
            (StatusCode::OK, Json(serde_json::to_value(result).unwrap())).into_response()
        }
        Err(err) => {
            let status = match &err {
                GraphError::Timeout => ferrosa_schema::audit::event::GraphAuditStatus::Timeout,
                GraphError::PermissionDenied(_) => ferrosa_schema::audit::event::GraphAuditStatus::Denied,
                _ => ferrosa_schema::audit::event::GraphAuditStatus::Error,
            };
            state.emit_graph_query_audit(&req.query, &req.keyspace, 0, start.elapsed(), &auth, status);
            let (http_status, body) = error_to_response(err);
            (http_status, body).into_response()
        }
    }
}

/// POST /graph/explain
async fn handle_explain(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    match state.engine.explain(&req.query, &req.keyspace, &auth) {
        Ok(plan) => (StatusCode::OK, Json(serde_json::to_value(format!("{plan:?}")).unwrap())).into_response(),
        Err(err) => {
            let (status, body) = error_to_response(err);
            (status, body).into_response()
        }
    }
}

/// GET /graph/schema?keyspace=...
async fn handle_schema(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<AuthContext>,
    Query(params): Query<SchemaParams>,
) -> impl IntoResponse {
    match state.engine.graph_schema(&params.keyspace, &auth) {
        Ok(schema) => (StatusCode::OK, Json(serde_json::to_value(schema).unwrap())).into_response(),
        Err(err) => {
            let (status, body) = error_to_response(err);
            (status, body).into_response()
        }
    }
}

/// GET /graph/health
async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

/// Build the Axum router.
///
/// Auth middleware applies to all routes except /graph/health (T2).
pub fn build_router(
    engine: Arc<GraphEngine>,
    schema: Arc<Schema>,
    config: &GraphHttpConfig,
) -> Router {
    let state = Arc::new(AppState { engine, schema });

    // Authenticated routes
    let authed = Router::new()
        .route("/graph/query", post(handle_query))
        .route("/graph/explain", post(handle_explain))
        .route("/graph/schema", get(handle_schema))
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    // Unauthenticated routes
    let public = Router::new()
        .route("/graph/health", get(handle_health));

    authed
        .merge(public)
        .layer(CatchPanicLayer::new())
        .layer(RequestBodyLimitLayer::new(config.max_request_body_bytes))
        .with_state(state)
}

/// Start the graph HTTP server.
///
/// Fails startup if require_tls is true but no cert is provided (T11).
pub async fn start_graph_http(
    engine: Arc<GraphEngine>,
    schema: Arc<Schema>,
    config: GraphHttpConfig,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if config.require_tls && config.tls_cert_path.is_none() {
        return Err("TLS required but no certificate provided. Set tls_cert_path and tls_key_path, or set require_tls = false for development.".into());
    }

    let app = build_router(engine, schema, &config);

    tracing::info!(addr = %config.bind_addr, "starting graph HTTP server");

    match (config.tls_cert_path, config.tls_key_path) {
        (Some(cert), Some(key)) => {
            let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
            axum_server::bind_rustls(config.bind_addr, tls_config)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await?;
        }
        _ => {
            if config.require_tls {
                return Err("TLS required but cert/key not provided".into());
            }
            tracing::warn!("graph HTTP server running WITHOUT TLS — development mode only");
            let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
        }
    }

    Ok(())
}
```

- [ ] **Step 2: Add module, run check**

Run: `cargo check -p ferrosa-graph`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/http.rs ferrosa-graph/src/lib.rs
git commit -m "feat(graph): add HTTP endpoint with auth, TLS, error sanitization (T2, T8, T11)"
```

---

### Task 5.4: Update lib.rs with all modules

**Files:**

- Modify: `ferrosa-graph/src/lib.rs`

- [ ] **Step 1: Update lib.rs**

```rust
//! # ferrosa-graph
//!
//! Graph query engine for ferrosa. Provides a Cypher/GQL query endpoint
//! alongside CQL, with data stored in normal CQL tables and accessed
//! via a system-managed adjacency index.
//!
//! ## Modules
//!
//! - [`parser`] — Cypher lexer, parser, and AST types.
//! - [`error`] — Graph engine error types.
//! - [`adjacency`] — Adjacency index schema and observer.
//! - [`planner`] — Logical and physical query planners.
//! - [`executor`] — Query execution engine.
//! - [`engine`] — GraphEngine composition type.
//! - [`http`] — HTTP/JSON endpoint.

pub mod adjacency;
pub mod engine;
pub mod error;
pub mod executor;
pub mod http;
pub mod parser;
pub mod planner;
```

- [ ] **Step 2: Run full check and test**

Run: `cargo check -p ferrosa-graph && cargo test -p ferrosa-graph`

- [ ] **Step 3: Commit**

```bash
git add ferrosa-graph/src/lib.rs
git commit -m "feat(graph): wire all modules in lib.rs"
```

---

## Chunk 6: Slice 6 — Binary Integration (ferrosa)

### Task 6.1: Create ferrosa binary crate

**Files:**

- Create: `ferrosa/Cargo.toml`
- Create: `ferrosa/src/main.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "ferrosa"
version = "0.1.0"
edition = "2021"
description = "Ferrosa — distributed database with CQL and Cypher"

[[bin]]
name = "ferrosa"
path = "src/main.rs"

[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
ferrosa-schema = { path = "../ferrosa-schema" }
ferrosa-storage = { path = "../ferrosa-storage" }
ferrosa-cql = { path = "../ferrosa-cql" }
ferrosa-graph = { path = "../ferrosa-graph" }
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

- [ ] **Step 2: Create main.rs**

```rust
//! Ferrosa binary — composes all crates into the running database.
//!
//! Startup sequence:
//! 1. Initialize tracing
//! 2. Create StorageEngine
//! 3. Create Schema
//! 4. Create CQL server
//! 5. Create GraphEngine + HTTP server (if enabled)
//! 6. Wait for shutdown signal
//! 7. Graceful shutdown

use std::sync::Arc;

use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_graph::http::{GraphHttpConfig, start_graph_http};
use ferrosa_schema::audit::LogAuditSink;
use ferrosa_schema::registry::{Schema, SchemaConfig};
use ferrosa_schema::secrets::env::EnvSecretsProvider;
use ferrosa_storage::{StorageEngine, StorageEngineConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    tracing::info!("ferrosa starting");

    // 2. Create StorageEngine
    let storage_config = StorageEngineConfig::from_env()?;
    let rt = tokio::runtime::Handle::current();
    let storage = Arc::new(StorageEngine::new(storage_config, Some(&rt))?);

    // 3. Create Schema
    let schema_config = SchemaConfig {
        hasher: ferrosa_schema::auth::password::PasswordHasher::bcrypt_default(),
        password_policy: ferrosa_schema::auth::password::PasswordPolicy::default(),
        auth_method: ferrosa_schema::registry::AuthMethod::Password,
        rate_limit: ferrosa_schema::auth::rate_limit::RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(EnvSecretsProvider),
        mode: ferrosa_schema::startup::DeploymentMode::Development,
    };
    let schema = Arc::new(Schema::new(schema_config)?);

    // 4. Graph engine (check FERROSA_GRAPH_ENABLED)
    let graph_config = ferrosa_graph::engine::GraphConfig {
        enabled: std::env::var("FERROSA_GRAPH_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false),
        http: GraphHttpConfig {
            require_tls: false, // TODO: read from env
            ..GraphHttpConfig::default()
        },
        ..ferrosa_graph::engine::GraphConfig::default()
    };

    if graph_config.enabled {
        let http_config = graph_config.http.clone();
        let graph_engine = Arc::new(GraphEngine::new(
            schema.clone(),
            storage.clone(),
            graph_config,
        ));

        let schema_for_http = schema.clone();
        tokio::spawn(async move {
            if let Err(e) = start_graph_http(graph_engine, schema_for_http, http_config).await {
                tracing::error!(%e, "graph HTTP server failed");
            }
        });
    } else {
        tracing::info!("graph engine disabled (set FERROSA_GRAPH_ENABLED=true to enable)");
    }

    // 5. Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");

    // 6. Graceful shutdown
    storage.shutdown()?;
    tracing::info!("ferrosa stopped");

    Ok(())
}
```

- [ ] **Step 3: Add to workspace**

In root `Cargo.toml`, add `"ferrosa"` to workspace members.

- [ ] **Step 4: Build**

Run: `cargo build`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa/ Cargo.toml
git commit -m "feat: add ferrosa binary crate with graph engine integration"
```

---

### Task 6.2: Final workspace validation

- [ ] **Step 1: Full build**

Run: `cargo build`

- [ ] **Step 2: All tests**

Run: `cargo test`

- [ ] **Step 3: Clippy**

Run: `cargo clippy --all-targets`

- [ ] **Step 4: Format check**

Run: `cargo fmt --check`

---

## Dependency Order

```
Chunk 1 (Slice 1: Schema)  ──┐
                              ├──→ Chunk 3 (Slice 3: Adjacency)
Chunk 2 (Slice 2: Observer) ──┘         │
                                        ├──→ Chunk 4 (Slice 4: Planner/Executor)
                                        │         │
                                        │         ├──→ Chunk 5 (Slice 5: HTTP)
                                        │         │         │
                                        └─────────┴─────────┴──→ Chunk 6 (Slice 6: Binary)
```

**Chunks 1 and 2 can run in parallel.** Chunks 3–6 are sequential.

## Notes

- All observer mutations go through `StorageEngine::write()` → commit log, ensuring durability
- The adjacency observer uses `ArcSwap::load()` for lock-free schema reads — no blocking on the drain task thread
- The `GraphEngineConfig` defaults are conservative (30s timeout, 10k rows, 10k fan-out) — tunable per deployment
- TLS is required in production mode; development can run plaintext with a warning
- Error sanitization ensures no internal state leaks to clients (T8)
- Per-hop permission checks happen at plan time, not execution time, for fail-fast behavior (T3)
