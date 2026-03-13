# Ferrosa Observability Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a virtual-table-based observability system with four view layers (CQL SUBSCRIBE, Prometheus, CLI, web console) and change-driven reactive queries.

**Architecture:** All observability data modeled as virtual tables in `system_observability` keyspace. A `VirtualTable` trait in `ferrosa-schema` provides the single data abstraction. SUBSCRIBE extends CQL and graph queries with change-driven (via WriteObserver) and polling modes, with full or delta result delivery.

**Tech Stack:** Rust, tokio, axum (web), ratatui (TUI), rust-embed (static assets), prometheus text exposition format

**Spec:** `docs/superpowers/specs/2026-03-13-ferrosa-observability-design.md`

---

## File Structure

### New files

| File | Responsibility |
|------|----------------|
| `ferrosa-common/src/data_type.rs` | `DataType` enum for CQL-level type system |
| `ferrosa-schema/src/virtual_table.rs` | `VirtualTable` trait, `VirtualRow`, `RowPredicate`, `VirtualColumnDef`, `SubscriptionMode` |
| `ferrosa-schema/src/virtual_registry.rs` | `VirtualTableRegistry` — `Arc<dyn VirtualTable>` keyed by `(keyspace, table)` |
| `ferrosa-schema/src/observability/mod.rs` | `system_observability` keyspace registration |
| `ferrosa-cql/src/virtual_tables/mod.rs` | `ConnectionsTable`, `ActiveQueriesTable` implementations |
| `ferrosa-cql/src/subscribe.rs` | `SubscriptionState`, streaming response logic |
| `ferrosa-cql/src/client.rs` | Thin CQL client module (codec reuse, handshake, auth) |
| `ferrosa-cql/src/prometheus.rs` | Prometheus `/metrics` endpoint |
| `ferrosa-storage/src/virtual_tables.rs` | `StorageStatsTable` implementation |
| `ferrosa-storage/src/subscription_observer.rs` | `SubscriptionObserver` — change-driven SUBSCRIBE via WriteObserver |
| `ferrosa-ctl/Cargo.toml` | New crate manifest |
| `ferrosa-ctl/src/main.rs` | CLI entry point |
| `ferrosa-ctl/src/commands.rs` | Subcommand implementations |
| `ferrosa-ctl/src/tui.rs` | ratatui TUI monitor mode |
| `ferrosa/src/web/mod.rs` | Web interface wiring |
| `ferrosa/src/web/api.rs` | JSON API routes reading from virtual tables |
| `ferrosa/src/web/static_files.rs` | rust-embed static file serving |

### Modified files

| File | Changes |
|------|---------|
| `Cargo.toml` (workspace) | Add `ferrosa-ctl` member |
| `ferrosa-common/src/lib.rs` | Re-export `DataType` |
| `ferrosa-schema/src/lib.rs` | Re-export virtual table types |
| `ferrosa-schema/src/registry.rs` | Add `VirtualTableRegistry` field to `Schema` |
| `ferrosa-storage/src/observer.rs` | Add `watches_table()` method to `WriteObserver` trait |
| `ferrosa-storage/src/engine.rs` | Use `watches_table()` in dispatch |
| `ferrosa-storage/src/lib.rs` | Re-export new modules |
| `ferrosa-cql/src/ast.rs` | Add `Subscribe`, `Unsubscribe` variants to `Statement` |
| `ferrosa-cql/src/parser.rs` | Parse `SUBSCRIBE`, `UNSUBSCRIBE`, `EVERY`, `DELTA` |
| `ferrosa-cql/src/lexer.rs` | Add new tokens |
| `ferrosa-cql/src/router.rs` | Route virtual table SELECT through registry, route SUBSCRIBE |
| `ferrosa-cql/src/connection.rs` | Manage subscription lifecycle, streaming frames, `ConnectionTracker` |
| `ferrosa-cql/src/server.rs` | Pass `ConnectionTracker` to connection handler |
| `ferrosa-cql/src/frame.rs` | Add `STREAMING` flag (0x10) |
| `ferrosa-cql/src/result.rs` | Encode streaming ROWS with STREAMING flag |
| `ferrosa-cql/src/lib.rs` | Re-export new modules |
| `ferrosa-graph/src/parser/ast.rs` | Add `Subscribe`, `Unsubscribe` to graph AST |
| `ferrosa-graph/src/parser/parse_impl.rs` | Parse SUBSCRIBE prefix |
| `ferrosa-graph/src/parser/lexer.rs` | Add new tokens |
| `ferrosa-graph/src/parser/token.rs` | Add new token types |
| `ferrosa-graph/src/planner/logical.rs` | Produce table dependency set |
| `ferrosa-graph/src/executor/expand.rs` | Handle virtual table sources |
| `ferrosa/src/main.rs` | Wire up web server, Prometheus, virtual table registration |
| `ferrosa/Cargo.toml` | Add rust-embed, mime_guess |

---

## Chunk 1: Foundation — DataType + VirtualTable Trait + Registry

### Task 1: Add `DataType` enum to `ferrosa-common`

**Files:**

- Create: `ferrosa-common/src/data_type.rs`
- Modify: `ferrosa-common/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

In `ferrosa-common/src/data_type.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_display() {
        assert_eq!(DataType::Text.to_string(), "text");
        assert_eq!(DataType::BigInt.to_string(), "bigint");
        assert_eq!(DataType::Uuid.to_string(), "uuid");
        assert_eq!(DataType::Timestamp.to_string(), "timestamp");
    }

    #[test]
    fn data_type_is_numeric() {
        assert!(DataType::Int.is_numeric());
        assert!(DataType::BigInt.is_numeric());
        assert!(DataType::Double.is_numeric());
        assert!(!DataType::Text.is_numeric());
        assert!(!DataType::Uuid.is_numeric());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-common data_type`
Expected: FAIL — module does not exist

- [ ] **Step 3: Write minimal implementation**

In `ferrosa-common/src/data_type.rs`:

```rust
use std::fmt;

/// CQL-level data type descriptors for column definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DataType {
    Text,
    Int,
    BigInt,
    Double,
    Boolean,
    Uuid,
    Timestamp,
    Blob,
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        matches!(self, Self::Int | Self::BigInt | Self::Double)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Text => "text",
            Self::Int => "int",
            Self::BigInt => "bigint",
            Self::Double => "double",
            Self::Boolean => "boolean",
            Self::Uuid => "uuid",
            Self::Timestamp => "timestamp",
            Self::Blob => "blob",
        };
        f.write_str(s)
    }
}
```

- [ ] **Step 4: Add re-export in `ferrosa-common/src/lib.rs`**

Add `pub mod data_type;` and `pub use data_type::DataType;`

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-common data_type -v`
Expected: 2 tests PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-common/src/data_type.rs ferrosa-common/src/lib.rs
git commit -m "feat(common): add DataType enum for CQL-level type descriptors"
```

---

### Task 2: Add `VirtualTable` trait and supporting types

**Files:**

- Create: `ferrosa-schema/src/virtual_table.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

In `ferrosa-schema/src/virtual_table.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DataType};
    use std::time::Duration;

    struct TestTable;

    impl VirtualTable for TestTable {
        fn name(&self) -> &str { "test_table" }
        fn keyspace(&self) -> &str { "system_observability" }
        fn columns(&self) -> &[VirtualColumnDef] { &[] }
        fn primary_key_columns(&self) -> &[usize] { &[0] }
        fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
            vec![VirtualRow { cells: vec![] }]
        }
        fn subscription_mode(&self) -> SubscriptionMode {
            SubscriptionMode::Pollable
        }
    }

    #[test]
    fn virtual_table_trait_object_safety() {
        let table: Box<dyn VirtualTable> = Box::new(TestTable);
        assert_eq!(table.name(), "test_table");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.read(None).len(), 1);
    }

    #[test]
    fn subscription_mode_variants() {
        assert!(matches!(SubscriptionMode::Pollable, SubscriptionMode::Pollable));
        let dm = SubscriptionMode::DemandDriven {
            default_interval: Duration::from_secs(5),
        };
        assert!(matches!(dm, SubscriptionMode::DemandDriven { .. }));
    }

    #[test]
    fn row_predicate_conjunction() {
        let pred = RowPredicate {
            filters: vec![
                ColumnFilter {
                    column: "keyspace".into(),
                    op: PredicateOp::Eq,
                    value: CellValue::new_for_test(b"system"),
                },
                ColumnFilter {
                    column: "size".into(),
                    op: PredicateOp::Gt,
                    value: CellValue::new_for_test(&100i64.to_be_bytes()),
                },
            ],
        };
        assert_eq!(pred.filters.len(), 2);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema virtual_table`
Expected: FAIL — module does not exist

- [ ] **Step 3: Write implementation**

In `ferrosa-schema/src/virtual_table.rs`:

```rust
use ferrosa_common::{CellValue, DataType};
use std::time::Duration;

/// A virtual table backed by live code instead of SSTables.
pub trait VirtualTable: Send + Sync {
    fn name(&self) -> &str;
    fn keyspace(&self) -> &str;
    fn columns(&self) -> &[VirtualColumnDef];
    fn primary_key_columns(&self) -> &[usize];
    fn read(&self, predicate: Option<&RowPredicate>) -> Vec<VirtualRow>;
    fn subscription_mode(&self) -> SubscriptionMode;
}

pub struct VirtualRow {
    pub cells: Vec<CellValue>,
}

#[derive(Debug, Clone)]
pub struct VirtualColumnDef {
    pub name: String,
    pub data_type: DataType,
}

pub struct RowPredicate {
    pub filters: Vec<ColumnFilter>,
}

pub struct ColumnFilter {
    pub column: String,
    pub op: PredicateOp,
    pub value: CellValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    Eq, Gt, Lt, Gte, Lte,
}

#[derive(Debug, Clone)]
pub enum SubscriptionMode {
    Pollable,
    DemandDriven { default_interval: Duration },
    None,
}
```

- [ ] **Step 4: Add re-export in `ferrosa-schema/src/lib.rs`**

Add `pub mod virtual_table;` and re-export key types.

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-schema virtual_table -v`
Expected: 3 tests PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-schema/src/virtual_table.rs ferrosa-schema/src/lib.rs
git commit -m "feat(schema): add VirtualTable trait and supporting types"
```

---

### Task 3: Add `VirtualTableRegistry`

**Files:**

- Create: `ferrosa-schema/src/virtual_registry.rs`
- Modify: `ferrosa-schema/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

In `ferrosa-schema/src/virtual_registry.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_table::*;
    use std::sync::Arc;

    struct StubTable { name: &'static str }

    impl VirtualTable for StubTable {
        fn name(&self) -> &str { self.name }
        fn keyspace(&self) -> &str { "system_observability" }
        fn columns(&self) -> &[VirtualColumnDef] { &[] }
        fn primary_key_columns(&self) -> &[usize] { &[] }
        fn read(&self, _: Option<&RowPredicate>) -> Vec<VirtualRow> { vec![] }
        fn subscription_mode(&self) -> SubscriptionMode { SubscriptionMode::Pollable }
    }

    #[test]
    fn register_and_lookup() {
        let registry = VirtualTableRegistry::new();
        let table = Arc::new(StubTable { name: "connections" });
        registry.register(table);
        let found = registry.get("system_observability", "connections");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "connections");
    }

    #[test]
    fn lookup_missing_returns_none() {
        let registry = VirtualTableRegistry::new();
        assert!(registry.get("system_observability", "nonexistent").is_none());
    }

    #[test]
    fn list_tables_in_keyspace() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(StubTable { name: "connections" }));
        registry.register(Arc::new(StubTable { name: "storage_stats" }));
        let tables = registry.list("system_observability");
        assert_eq!(tables.len(), 2);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema virtual_registry`
Expected: FAIL

- [ ] **Step 3: Write implementation**

In `ferrosa-schema/src/virtual_registry.rs`:

```rust
use crate::virtual_table::VirtualTable;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

type TableKey = (String, String);

pub struct VirtualTableRegistry {
    tables: ArcSwap<HashMap<TableKey, Arc<dyn VirtualTable>>>,
}

impl VirtualTableRegistry {
    pub fn new() -> Self {
        Self { tables: ArcSwap::new(Arc::new(HashMap::new())) }
    }

    pub fn register(&self, table: Arc<dyn VirtualTable>) {
        let key = (table.keyspace().to_string(), table.name().to_string());
        let mut new_map = (*self.tables.load()).clone();
        new_map.insert(key, table);
        self.tables.store(Arc::new(new_map));
    }

    pub fn get(&self, keyspace: &str, table_name: &str) -> Option<Arc<dyn VirtualTable>> {
        let guard = self.tables.load();
        guard.get(&(keyspace.to_string(), table_name.to_string())).cloned()
    }

    pub fn list(&self, keyspace: &str) -> Vec<Arc<dyn VirtualTable>> {
        let guard = self.tables.load();
        guard.iter()
            .filter(|((ks, _), _)| ks == keyspace)
            .map(|(_, table)| table.clone())
            .collect()
    }
}

impl Default for VirtualTableRegistry {
    fn default() -> Self { Self::new() }
}
```

- [ ] **Step 4: Re-export in `ferrosa-schema/src/lib.rs`**

- [ ] **Step 5: Run tests**

Run: `cargo test -p ferrosa-schema virtual_registry -v`
Expected: 3 tests PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-schema/src/virtual_registry.rs ferrosa-schema/src/lib.rs
git commit -m "feat(schema): add VirtualTableRegistry with lock-free reads"
```

---

### Task 4: Wire `VirtualTableRegistry` into `Schema`

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

Add to `ferrosa-schema/src/registry.rs` test module:

```rust
#[test]
fn schema_exposes_virtual_table_registry() {
    let schema = Schema::new_for_test();
    let registry = schema.virtual_tables();
    assert!(registry.get("system_observability", "anything").is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-schema schema_exposes_virtual_table_registry`
Expected: FAIL — `virtual_tables()` does not exist

- [ ] **Step 3: Add `VirtualTableRegistry` field and accessor**

Add `virtual_table_registry: Arc<VirtualTableRegistry>` field to `Schema`, initialize in constructors, add `pub fn virtual_tables(&self) -> &VirtualTableRegistry`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-schema`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-schema/src/registry.rs
git commit -m "feat(schema): wire VirtualTableRegistry into Schema"
```

---

### Task 5: Route virtual table SELECT through registry in CQL router

**Files:**

- Modify: `ferrosa-cql/src/router.rs`
- Test: router test module

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn select_from_virtual_table_returns_rows() {
    let state = test_state();
    let stub = Arc::new(StubVirtualTable::new("connections", vec![
        VirtualRow { cells: vec![CellValue::new_for_test(b"127.0.0.1")] },
    ]));
    state.schema.virtual_tables().register(stub);

    let stmt = parse("SELECT * FROM system_observability.connections").unwrap();
    let ctx = test_request_context();
    let result = route(&state, &ctx, stmt).await;
    assert!(matches!(result, RouteResult::Rows { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-cql select_from_virtual_table`
Expected: FAIL

- [ ] **Step 3: Add virtual table check in `route_select`**

Before storage lookup, check `state.schema.virtual_tables().get(&keyspace, &table_name)`. If found, call `read()` and encode as CQL ROWS.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/router.rs
git commit -m "feat(cql): route SELECT on virtual tables through registry"
```

---

## Chunk 2: Virtual Table Implementations

### Task 6: Implement `ConnectionsTable`

**Files:**

- Create: `ferrosa-cql/src/virtual_tables/mod.rs`
- Create: `ferrosa-cql/src/virtual_tables/connections.rs`
- Modify: `ferrosa-cql/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connections_table_metadata() {
        let tracker = Arc::new(ConnectionTracker::new());
        let table = ConnectionsTable::new(tracker);
        assert_eq!(table.name(), "connections");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 7);
    }

    #[test]
    fn connections_table_reads_active() {
        let tracker = Arc::new(ConnectionTracker::new());
        tracker.register("127.0.0.1", 9042, "admin");
        tracker.register("10.0.1.5", 9042, "app_user");
        let table = ConnectionsTable::new(tracker);
        assert_eq!(table.read(None).len(), 2);
    }

    #[test]
    fn connections_table_is_pollable() {
        let tracker = Arc::new(ConnectionTracker::new());
        let table = ConnectionsTable::new(tracker);
        assert!(matches!(table.subscription_mode(), SubscriptionMode::Pollable));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-cql connections_table`
Expected: FAIL

- [ ] **Step 3: Implement `ConnectionTracker` and `ConnectionsTable`**

`ConnectionTracker` holds a concurrent map of active connections. `ConnectionsTable` implements `VirtualTable`, reading from the tracker. Columns: peer_address, peer_port, state, username, idle_seconds, requests_served, protocol_version.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql connections_table -v`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/virtual_tables/ ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add ConnectionsTable virtual table"
```

---

### Task 7: Wire `ConnectionTracker` into connection handler

**Files:**

- Modify: `ferrosa-cql/src/connection.rs`
- Modify: `ferrosa-cql/src/server.rs`
- Test: connection tests

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn connection_handler_registers_with_tracker() {
    let tracker = Arc::new(ConnectionTracker::new());
    // set up test connection with tracker
    // verify tracker.active_count() == 1 during connection
    // verify tracker.active_count() == 0 after disconnect
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Pass `Arc<ConnectionTracker>` to `handle_connection()`**

Register on connect, update state on auth, deregister on disconnect (Drop guard).

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/connection.rs ferrosa-cql/src/server.rs
git commit -m "feat(cql): wire ConnectionTracker into connection handler"
```

---

### Task 8: Implement `StorageStatsTable`

**Files:**

- Create: `ferrosa-storage/src/virtual_tables.rs`
- Modify: `ferrosa-storage/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_stats_table_metadata() {
        let engine = test_storage_engine();
        let table = StorageStatsTable::new(engine);
        assert_eq!(table.name(), "storage_stats");
        assert_eq!(table.columns().len(), 9);
    }

    #[test]
    fn storage_stats_returns_per_table_rows() {
        let engine = test_storage_engine();
        let table = StorageStatsTable::new(engine);
        let rows = table.read(None);
        assert!(!rows.is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-storage storage_stats`
Expected: FAIL

- [ ] **Step 3: Implement `StorageStatsTable`**

Reads from `StorageEngine` internals: iterates table stores, collects memtable sizes, SSTable counts, S3 object stats. Columns: keyspace, table_name, memtable_size_bytes, memtable_count, sstable_count, sstable_size_bytes, s3_object_count, s3_bytes, pending_compactions.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-storage storage_stats -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/src/virtual_tables.rs ferrosa-storage/src/lib.rs
git commit -m "feat(storage): add StorageStatsTable virtual table"
```

---

### Task 9: Implement `ActiveQueriesTable`

**Files:**

- Create: `ferrosa-cql/src/virtual_tables/active_queries.rs`
- Modify: `ferrosa-cql/src/virtual_tables/mod.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_queries_table_metadata() {
        let tracker = Arc::new(QueryTracker::new());
        let table = ActiveQueriesTable::new(tracker);
        assert_eq!(table.name(), "active_queries");
        assert_eq!(table.columns().len(), 8);
    }

    #[test]
    fn tracks_query_lifecycle() {
        let tracker = Arc::new(QueryTracker::new());
        let id = tracker.begin("SELECT * FROM users", "test_ks", "10.0.0.1", "admin");
        assert_eq!(tracker.active_count(), 1);

        let table = ActiveQueriesTable::new(tracker.clone());
        assert_eq!(table.read(None).len(), 1);

        tracker.complete(id);
        assert_eq!(table.read(None).len(), 0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-cql active_queries`
Expected: FAIL

- [ ] **Step 3: Implement `QueryTracker` and `ActiveQueriesTable`**

`QueryTracker` holds a concurrent map of active queries (query_id, client_address, username, query_text, keyspace, start_time, state). Columns match the spec schema.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql active_queries -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/virtual_tables/active_queries.rs ferrosa-cql/src/virtual_tables/mod.rs
git commit -m "feat(cql): add ActiveQueriesTable virtual table"
```

---

### Task 10: Wire `QueryTracker` into CQL router

**Files:**

- Modify: `ferrosa-cql/src/router.rs`
- Test: router tests

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn router_tracks_active_queries() {
    let state = test_state();
    let tracker = state.query_tracker.clone();
    assert_eq!(tracker.active_count(), 0);
    let stmt = parse("SELECT * FROM system.local").unwrap();
    let ctx = test_request_context();
    let _ = route(&state, &ctx, stmt).await;
    assert_eq!(tracker.total_executed(), 1);
}
```

- [ ] **Step 2-4: Implement, test**

Add `query_tracker: Arc<QueryTracker>` to `SharedState`. Call `tracker.begin()` at start of `route()`, `tracker.complete()` at end (Drop guard).

Run: `cargo test -p ferrosa-cql`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/router.rs
git commit -m "feat(cql): wire QueryTracker into router"
```

---

## Chunk 3: SUBSCRIBE Parsing + Wire Protocol

### Task 11: Add SUBSCRIBE/UNSUBSCRIBE tokens to CQL lexer

**Files:**

- Modify: `ferrosa-cql/src/lexer.rs`
- Test: lexer tests

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn lexer_recognizes_subscribe_keywords() {
    let tokens = tokenize("SUBSCRIBE SELECT * FROM t EVERY 5s DELTA").unwrap();
    assert!(tokens.iter().any(|t| matches!(t, Token::Subscribe)));
    assert!(tokens.iter().any(|t| matches!(t, Token::Every)));
    assert!(tokens.iter().any(|t| matches!(t, Token::Delta)));
}

#[test]
fn lexer_recognizes_unsubscribe() {
    let tokens = tokenize("UNSUBSCRIBE").unwrap();
    assert!(tokens.iter().any(|t| matches!(t, Token::Unsubscribe)));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-cql lexer_recognizes_subscribe`
Expected: FAIL

- [ ] **Step 3: Add token variants**

Add `Subscribe`, `Unsubscribe`, `Every`, `Delta` to `Token` enum and keyword matching.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql lexer -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/lexer.rs
git commit -m "feat(cql): add SUBSCRIBE/UNSUBSCRIBE/EVERY/DELTA tokens"
```

---

### Task 12: Add SUBSCRIBE/UNSUBSCRIBE to CQL AST

**Files:**

- Modify: `ferrosa-cql/src/ast.rs`
- Test: inline

- [ ] **Step 1: Add AST variants**

```rust
Subscribe {
    inner: Box<Statement>,       // must be Select
    interval: Option<Duration>,  // None = change-driven
    delta: bool,
},
Unsubscribe {
    stream_id: Option<u16>,
},
```

- [ ] **Step 2: Write test, run, verify**

- [ ] **Step 3: Commit**

```bash
git add ferrosa-cql/src/ast.rs
git commit -m "feat(cql): add Subscribe/Unsubscribe AST variants"
```

---

### Task 13: Parse SUBSCRIBE/UNSUBSCRIBE in CQL parser

**Files:**

- Modify: `ferrosa-cql/src/parser.rs`
- Test: parser tests

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn parse_subscribe_select() {
    let stmt = parse("SUBSCRIBE SELECT * FROM users WHERE active = true").unwrap();
    match stmt {
        Statement::Subscribe { inner, interval, delta } => {
            assert!(interval.is_none());
            assert!(!delta);
            assert!(matches!(*inner, Statement::Select { .. }));
        }
        _ => panic!("expected Subscribe"),
    }
}

#[test]
fn parse_subscribe_with_every() {
    let stmt = parse("SUBSCRIBE SELECT * FROM t EVERY 5s").unwrap();
    match stmt {
        Statement::Subscribe { interval, .. } =>
            assert_eq!(interval, Some(Duration::from_secs(5))),
        _ => panic!("expected Subscribe"),
    }
}

#[test]
fn parse_subscribe_with_delta() {
    let stmt = parse("SUBSCRIBE SELECT * FROM t DELTA").unwrap();
    match stmt {
        Statement::Subscribe { delta, .. } => assert!(delta),
        _ => panic!("expected Subscribe"),
    }
}

#[test]
fn parse_subscribe_every_and_delta() {
    let stmt = parse("SUBSCRIBE SELECT * FROM t EVERY 1s DELTA").unwrap();
    match stmt {
        Statement::Subscribe { interval, delta, .. } => {
            assert_eq!(interval, Some(Duration::from_secs(1)));
            assert!(delta);
        }
        _ => panic!("expected Subscribe"),
    }
}

#[test]
fn parse_unsubscribe_all() {
    let stmt = parse("UNSUBSCRIBE").unwrap();
    assert!(matches!(stmt, Statement::Unsubscribe { stream_id: None }));
}

#[test]
fn parse_subscribe_rejects_non_select() {
    assert!(parse("SUBSCRIBE INSERT INTO t (a) VALUES (1)").is_err());
}

#[test]
fn parse_subscribe_enforces_min_interval() {
    assert!(parse("SUBSCRIBE SELECT * FROM t EVERY 100ms").is_err());
}
```

- [ ] **Step 2: Run test to verify they fail**

Run: `cargo test -p ferrosa-cql parse_subscribe`
Expected: FAIL

- [ ] **Step 3: Implement parser logic**

Check for `Token::Subscribe` at top of `parse_statement()`. Parse inner SELECT, then optional `EVERY <duration>` (enforce 500ms floor), then optional `DELTA`. Add `parse_duration()` helper for `5s`, `1s`, `500ms` etc.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/parser.rs
git commit -m "feat(cql): parse SUBSCRIBE/UNSUBSCRIBE with EVERY and DELTA modifiers"
```

---

### Task 14: Add STREAMING flag to CQL frame

**Files:**

- Modify: `ferrosa-cql/src/frame.rs`
- Modify: `ferrosa-cql/src/result.rs`
- Test: frame tests

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn streaming_flag_set_in_subscribe_response() {
    let frame = CqlFrame::rows_streaming(stream_id, &column_specs, &rows);
    assert!(frame.flags & STREAMING_FLAG != 0);
}

#[test]
fn streaming_flag_absent_in_normal_response() {
    let frame = CqlFrame::rows(stream_id, &column_specs, &rows);
    assert!(frame.flags & STREAMING_FLAG == 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Add `STREAMING_FLAG` constant and `rows_streaming()` constructor**

```rust
pub const STREAMING_FLAG: u8 = 0x10;
```

- [ ] **Step 4: Run tests, commit**

```bash
git add ferrosa-cql/src/frame.rs ferrosa-cql/src/result.rs
git commit -m "feat(cql): add STREAMING flag (0x10) for SUBSCRIBE responses"
```

---

### Task 15: Add SUBSCRIBE/UNSUBSCRIBE to graph parser

**Files:**

- Modify: `ferrosa-graph/src/parser/ast.rs`
- Modify: `ferrosa-graph/src/parser/lexer.rs`
- Modify: `ferrosa-graph/src/parser/parse_impl.rs`
- Modify: `ferrosa-graph/src/parser/token.rs`
- Test: parser tests

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn parse_graph_subscribe_match() {
    let stmt = parse_graph("SUBSCRIBE MATCH (u:User)-[:FOLLOWS]->(f) RETURN u, f").unwrap();
    match stmt {
        GraphStatement::Subscribe { interval, delta, .. } => {
            assert!(interval.is_none());
            assert!(!delta);
        }
        _ => panic!("expected Subscribe"),
    }
}

#[test]
fn parse_graph_subscribe_every_delta() {
    let stmt = parse_graph("SUBSCRIBE MATCH (u:User) RETURN u EVERY 5s DELTA").unwrap();
    match stmt {
        GraphStatement::Subscribe { interval, delta, .. } => {
            assert_eq!(interval, Some(Duration::from_secs(5)));
            assert!(delta);
        }
        _ => panic!("expected Subscribe"),
    }
}

#[test]
fn parse_graph_subscribe_rejects_create() {
    assert!(parse_graph("SUBSCRIBE CREATE (u:User {name: 'test'})").is_err());
}

#[test]
fn existing_match_still_parses() {
    let stmt = parse_graph("MATCH (u:User) RETURN u").unwrap();
    assert!(matches!(stmt, GraphStatement::Query(_)));
}
```

- [ ] **Step 2: Run test to verify they fail**

Run: `cargo test -p ferrosa-graph parse_graph_subscribe`
Expected: FAIL

- [ ] **Step 3: Implement**

Add tokens to `token.rs` and `lexer.rs`. Add `Subscribe`/`Unsubscribe` variants to graph AST with `inner: Box<MatchQuery>`. Parse in `parse_impl.rs` — check for `Subscribe` token, parse inner MATCH, then EVERY/DELTA.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-graph`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-graph/src/parser/
git commit -m "feat(graph): add SUBSCRIBE/UNSUBSCRIBE parsing with EVERY and DELTA"
```

---

### Task 16: Graph planner produces table dependency set

**Files:**

- Modify: `ferrosa-graph/src/planner/logical.rs`
- Test: planner tests

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn planner_returns_table_dependencies() {
    let schema = test_graph_schema();
    let query = parse_match("MATCH (u:User)-[:FOLLOWS]->(f) RETURN u, f").unwrap();
    let plan = validate(&schema, &query).unwrap();
    let deps = plan.table_dependencies();
    assert!(deps.len() >= 2); // user table + follows edge table
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement**

Collect `TableId`s during label resolution into a `Vec<TableId>`. Expose via `table_dependencies()`.

- [ ] **Step 4: Run tests, commit**

```bash
git add ferrosa-graph/src/planner/logical.rs
git commit -m "feat(graph): expose table dependency set from logical planner"
```

---

## Chunk 4: WriteObserver Extension + SubscriptionObserver

### Task 17: Add `watches_table()` to `WriteObserver` trait

**Files:**

- Modify: `ferrosa-storage/src/observer.rs`
- Modify: `ferrosa-storage/src/engine.rs`
- Test: observer tests

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn default_watches_table_delegates_to_tables() {
    struct TestObs;
    impl WriteObserver for TestObs {
        fn mode(&self) -> ObserverMode { ObserverMode::Sync }
        fn tables(&self) -> Vec<TableId> { vec![TableId::new("ks", "tbl")] }
        fn on_write(&self, _: &TableId, _: &Mutation) -> Vec<Mutation> { vec![] }
    }
    let obs = TestObs;
    assert!(obs.watches_table(&TableId::new("ks", "tbl")));
    assert!(!obs.watches_table(&TableId::new("ks", "other")));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-storage default_watches_table`
Expected: FAIL

- [ ] **Step 3: Add default method**

```rust
fn watches_table(&self, table: &TableId) -> bool {
    self.tables().contains(table)
}
```

Update `StorageEngine` dispatch to call `watches_table()` instead of `tables().contains()`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-storage`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/src/observer.rs ferrosa-storage/src/engine.rs
git commit -m "feat(storage): add watches_table() to WriteObserver for dynamic table sets"
```

---

### Task 18: Implement `SubscriptionObserver`

**Files:**

- Create: `ferrosa-storage/src/subscription_observer.rs`
- Modify: `ferrosa-storage/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_deregister_subscription() {
        let obs = SubscriptionObserver::new(ObserverConfig::test_config());
        let sub_id = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
            predicate_columns: vec!["active".into()],
        });
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        obs.deregister(sub_id);
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
    }

    #[test]
    fn watches_table_is_dynamic() {
        let obs = SubscriptionObserver::new(ObserverConfig::test_config());
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
        let id = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
            predicate_columns: vec![],
        });
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        assert!(!obs.watches_table(&TableId::new("ks", "orders")));
        obs.deregister(id);
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
    }

    #[test]
    fn on_write_returns_empty() {
        let obs = SubscriptionObserver::new(ObserverConfig::test_config());
        obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
            predicate_columns: vec![],
        });
        let mutation = Mutation::test_mutation("ks", "users");
        let derived = obs.on_write(&TableId::new("ks", "users"), &mutation);
        assert!(derived.is_empty());
    }

    #[test]
    fn multiple_subscriptions_same_table() {
        let obs = SubscriptionObserver::new(ObserverConfig::test_config());
        let id1 = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
            predicate_columns: vec!["active".into()],
        });
        let id2 = obs.register(SubscriptionFilter {
            tables: vec![TableId::new("ks", "users")],
            predicate_columns: vec!["role".into()],
        });
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        obs.deregister(id1);
        assert!(obs.watches_table(&TableId::new("ks", "users")));
        obs.deregister(id2);
        assert!(!obs.watches_table(&TableId::new("ks", "users")));
    }
}
```

- [ ] **Step 2: Run test to verify they fail**

Run: `cargo test -p ferrosa-storage subscription_observer`
Expected: FAIL

- [ ] **Step 3: Implement**

`SubscriptionObserver` uses a `DashMap` (or `parking_lot::RwLock<HashMap>`) for subscriptions and a ref-counted `DashMap<TableId, usize>` for fast `watches_table()` lookups. Implements `WriteObserver` with `ObserverMode::Async`. `on_write()` returns empty — notification happens via the async channel.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-storage subscription_observer -v`
Expected: 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/src/subscription_observer.rs ferrosa-storage/src/lib.rs
git commit -m "feat(storage): add SubscriptionObserver for change-driven SUBSCRIBE"
```

---

## Chunk 5: SUBSCRIBE Execution + Streaming

### Task 19: Implement subscription lifecycle in CQL connection handler

**Files:**

- Create: `ferrosa-cql/src/subscribe.rs`
- Modify: `ferrosa-cql/src/connection.rs`
- Modify: `ferrosa-cql/src/router.rs`
- Modify: `ferrosa-cql/src/lib.rs`
- Test: inline and integration tests

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_state_tracks_active() {
        let mut state = SubscriptionState::new(8);
        let handle = SubscriptionHandle::test(1);
        assert!(state.add(handle).is_ok());
        assert_eq!(state.active_count(), 1);
    }

    #[test]
    fn subscription_state_enforces_max() {
        let mut state = SubscriptionState::new(2);
        state.add(SubscriptionHandle::test(1)).unwrap();
        state.add(SubscriptionHandle::test(2)).unwrap();
        assert!(state.add(SubscriptionHandle::test(3)).is_err());
    }

    #[test]
    fn unsubscribe_by_stream_id() {
        let mut state = SubscriptionState::new(8);
        state.add(SubscriptionHandle::test(42)).unwrap();
        state.cancel(Some(42));
        assert_eq!(state.active_count(), 0);
    }

    #[test]
    fn unsubscribe_all() {
        let mut state = SubscriptionState::new(8);
        state.add(SubscriptionHandle::test(1)).unwrap();
        state.add(SubscriptionHandle::test(2)).unwrap();
        state.cancel(None);
        assert_eq!(state.active_count(), 0);
    }
}
```

- [ ] **Step 2: Run test to verify they fail**

- [ ] **Step 3: Implement `SubscriptionState` and wire into connection handler**

`SubscriptionState` manages per-connection subscriptions. Each subscription spawns a tokio task (polling: timer loop; change-driven: channel wait). Tasks send ROWS frames with STREAMING flag. `UNSUBSCRIBE` cancels via `CancellationToken`. Disconnect cancels all.

- [ ] **Step 4: Run tests**

Run: `cargo test -p ferrosa-cql`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/subscribe.rs ferrosa-cql/src/connection.rs ferrosa-cql/src/router.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): implement SUBSCRIBE execution with streaming frames"
```

---

### Task 20: Graph executor reads from virtual tables

**Files:**

- Modify: `ferrosa-graph/src/executor/expand.rs`
- Test: executor tests

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn executor_reads_from_virtual_table() {
    let schema = test_schema_with_virtual_tables();
    let engine = test_graph_engine(schema);
    let result = engine.execute("MATCH (h:Host) RETURN h.address").await.unwrap();
    assert!(!result.rows.is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Check VirtualTableRegistry in expansion loop**

When resolving a table source, check `VirtualTableRegistry`. If virtual, call `read()` instead of storage.

- [ ] **Step 4: Run tests, commit**

```bash
git add ferrosa-graph/src/executor/expand.rs
git commit -m "feat(graph): read from virtual tables in executor expansion"
```

---

## Chunk 6: Prometheus Exporter

### Task 21: Implement Prometheus `/metrics` endpoint

**Files:**

- Create: `ferrosa-cql/src/prometheus.rs`
- Modify: `ferrosa-cql/src/lib.rs`
- Modify: `ferrosa/src/main.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_gauge_metric() {
        let line = format_metric("ferrosa_connections_active", &[("state", "ready")], 42.0);
        assert_eq!(line, "ferrosa_connections_active{state=\"ready\"} 42\n");
    }

    #[test]
    fn virtual_table_to_prometheus() {
        let registry = VirtualTableRegistry::new();
        registry.register(Arc::new(StubTable::with_rows("connections", /* ... */)));
        let output = render_metrics(&registry);
        assert!(output.contains("ferrosa_connections_"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement**

Axum handler on `:9091`. Iterates `VirtualTableRegistry`, calls `read(None)`, converts to Prometheus text exposition. Naming: `ferrosa_<table>_<column>`.

- [ ] **Step 4: Run tests, commit**

```bash
git add ferrosa-cql/src/prometheus.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add Prometheus /metrics endpoint for virtual tables"
```

---

## Chunk 7: CQL Client Module + ferrosa-ctl

### Task 22: Add thin CQL client module

**Files:**

- Create: `ferrosa-cql/src/client.rs`
- Modify: `ferrosa-cql/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn client_connects_and_handshakes() {
    let server = test_cql_server().await;
    let mut client = CqlClient::connect(server.addr()).await.unwrap();
    assert!(client.is_ready());
}

#[tokio::test]
async fn client_executes_select() {
    let server = test_cql_server().await;
    let mut client = CqlClient::connect(server.addr()).await.unwrap();
    let result = client.query("SELECT * FROM system.local").await.unwrap();
    assert!(!result.rows.is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Implement `CqlClient`**

Reuses `CqlCodec`. Implements `connect()`, `authenticate()`, `query()`, `subscribe()` (returns stream), `close()`.

- [ ] **Step 4: Run tests, commit**

```bash
git add ferrosa-cql/src/client.rs ferrosa-cql/src/lib.rs
git commit -m "feat(cql): add thin CQL client module for ferrosa-ctl"
```

---

### Task 23: Create `ferrosa-ctl` crate with subcommands

**Files:**

- Create: `ferrosa-ctl/Cargo.toml`
- Create: `ferrosa-ctl/src/main.rs`
- Create: `ferrosa-ctl/src/commands.rs`
- Modify: `Cargo.toml` (workspace)

- [ ] **Step 1: Create crate scaffold**

```toml
# ferrosa-ctl/Cargo.toml
[package]
name = "ferrosa-ctl"
version = "0.1.0"
edition = "2021"

[dependencies]
ferrosa-cql = { path = "../ferrosa-cql" }
clap = { version = "4", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
tabled = "0.17"
serde_json = "1"
```

- [ ] **Step 2: Implement CLI with clap**

```rust
#[derive(Parser)]
#[command(name = "ferrosa-ctl")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:9042")]
    host: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Status,
    Connections { #[arg(long)] sort: Option<String> },
    Queries { #[arg(long)] long_running: bool },
    Storage,
    Topology,
    Peers,
    Monitor { #[arg(long)] panel: Option<String> },
}
```

- [ ] **Step 3: Implement subcommands**

Each connects via `CqlClient`, issues SELECT against `system_observability.*`, formats output.

- [ ] **Step 4: Add to workspace, build**

Run: `cargo build -p ferrosa-ctl`
Expected: Compiles

- [ ] **Step 5: Commit**

```bash
git add ferrosa-ctl/ Cargo.toml
git commit -m "feat(ctl): create ferrosa-ctl crate with CLI subcommands"
```

---

### Task 24: Add TUI monitor mode

**Files:**

- Create: `ferrosa-ctl/src/tui.rs`
- Modify: `ferrosa-ctl/Cargo.toml`
- Modify: `ferrosa-ctl/src/main.rs`

- [ ] **Step 1: Add dependencies**

```toml
ratatui = "0.29"
crossterm = "0.28"
```

- [ ] **Step 2: Implement TUI**

Panels for connections, active queries, storage stats, cluster peers, host metrics. Uses SUBSCRIBE for live updates. Keyboard navigation (q=quit, Tab=switch panels).

- [ ] **Step 3: Build, commit**

```bash
git add ferrosa-ctl/
git commit -m "feat(ctl): add ratatui TUI monitor mode"
```

---

## Chunk 8: Web Interface — Phase 1

### Task 25: Set up web interface skeleton

**Files:**

- Create: `ferrosa/src/web/mod.rs`
- Create: `ferrosa/src/web/api.rs`
- Create: `ferrosa/src/web/static_files.rs`
- Modify: `ferrosa/Cargo.toml`
- Modify: `ferrosa/src/main.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write API tests**

```rust
#[tokio::test]
async fn api_returns_connections_json() {
    let app = test_app();
    let resp = app.get("/api/connections").await;
    assert_eq!(resp.status(), 200);
    assert!(resp.json::<serde_json::Value>().await.is_array());
}

#[tokio::test]
async fn api_returns_storage_stats_json() {
    let app = test_app();
    let resp = app.get("/api/storage_stats").await;
    assert_eq!(resp.status(), 200);
}
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement API routes**

Axum router on `:9090`. Routes map 1:1 to virtual tables: `/api/connections`, `/api/storage_stats`, `/api/active_queries`, `/api/cluster_peers`, `/api/cluster_topology`. Each reads from `VirtualTableRegistry` and returns JSON.

- [ ] **Step 4: Implement static file serving with rust-embed**

```rust
#[derive(RustEmbed)]
#[folder = "web/dist/"]
struct Assets;
```

SPA fallback: serve `index.html` for unknown paths.

- [ ] **Step 5: Wire into main.rs on separate port**

- [ ] **Step 6: Run tests, commit**

```bash
git add ferrosa/src/web/ ferrosa/Cargo.toml ferrosa/src/main.rs
git commit -m "feat: add web interface skeleton with JSON API"
```

---

### Task 26: Create minimal frontend app

**Files:**

- Create: `ferrosa/web/` directory

- [ ] **Step 1: Create minimal HTML/JS app**

Single `index.html` + bundled JS that fetches from JSON API and renders tables. Auto-refreshes every 5 seconds. Shows: cluster overview, connections, storage stats, active queries.

- [ ] **Step 2: Build and verify**

Run: `cargo build -p ferrosa`
Expected: Compiles with embedded assets

- [ ] **Step 3: Commit**

```bash
git add ferrosa/web/
git commit -m "feat: add minimal web dashboard frontend (Phase 1)"
```

---

## Deferred Work

The following items are deferred to future plans when prerequisite crates exist:

| Item | Depends On | Reason |
|------|-----------|--------|
| `cluster_peers` virtual table | `ferrosa-cluster` | Crate not yet created |
| `cluster_topology` virtual table | `ferrosa-cluster` | Crate not yet created |
| `host_metrics` virtual table (DemandDriven) | `ferrosa-net`, `ferrosa-cluster` | Requires internode protocol |
| `SubscriptionManager` | `ferrosa-net` | Crate not yet created |
| Demand-driven internode collection | `ferrosa-net` | Requires internode protocol |
| Delta mode result comparison | Basic SUBSCRIBE | Can be added after SUBSCRIBE works end-to-end |
| Schema change subscription cancellation | Basic SUBSCRIBE | Can be added after SUBSCRIBE works |
| Web Phase 2 (WebSocket live monitoring) | SUBSCRIBE infrastructure | Depends on streaming being complete |
| Web Phase 3 (Operations Console) | Phase 2 | Incremental |
| Web Phase 4 (Advanced) | Phase 3 | Incremental |
