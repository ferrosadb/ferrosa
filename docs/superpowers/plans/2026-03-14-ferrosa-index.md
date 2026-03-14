# Secondary Index Framework Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `ferrosa-index` crate with pluggable secondary index types, async build pipeline, staleness tracking, CQL DDL/query integration, and multi-node DDL coordination.

**Architecture:** New `ferrosa-index` crate defines `IndexBuilder`/`IndexReader`/`IndexFactory` traits and core types. Index implementations (B-tree, hash, composite, phonetic, filtered, vector HNSW/IVFFlat) are modules within the crate. `ferrosa-schema` gains `IndexMetadata` and registry methods. `ferrosa-cql` gains `CREATE INDEX`/`DROP INDEX` parsing and routing. `ferrosa-storage` gains `IndexBuildScheduler` and `IndexStateTracker` for async background builds. `ferrosa-cluster` gains `CreateIndex`/`DropIndex` DDL operation variants.

**Tech Stack:** Rust, serde, bitflags, uuid, arc-swap, mpsc channels

**Spec:** `docs/superpowers/specs/2026-03-14-secondary-indexes-design.md`

**Prerequisites:**

- **`feature/pair-integration` branch must be merged** before Tasks 9-10 can execute. These tasks modify `DdlOperation`, `WireSchemaSnapshot`, and `apply_ddl_locally()` which exist only on that branch. If executing before merge, implement Task 9 with `DdlPath::Direct` only and defer Task 10 entirely.
- The `_internal` schema methods (`create_keyspace_internal`, `create_table_internal`, etc.) also come from pair-integration. Task 5's `create_index_internal`/`drop_index_internal` follow this same write_lock + clone-swap-store pattern.

**Scope note — Query path deferred:** This plan covers index creation, building, and staleness tracking. The query path integration (ScanPlan, IndexLookup, SOUNDS LIKE, ANN OF, index-accelerated SELECT) is a follow-up plan. Indexes built by this plan can be tested directly via IndexReader but are not yet wired into the CQL SELECT query path.

---

## File Structure

### New Crate: `ferrosa-index`

| File | Responsibility |
|------|---------------|
| `ferrosa-index/Cargo.toml` | Crate manifest — depends on ferrosa-common, serde, bitflags |
| `ferrosa-index/src/lib.rs` | Core types (IndexType, IndexKey, RowPosition, IndexFiles, IndexConfig, IndexCapabilities, FilterPredicate, FilterOp, IndexFileMeta), traits (IndexBuilder, IndexReader, IndexFactory), re-exports |
| `ferrosa-index/src/btree.rs` | B-tree index — BTreeIndexFactory, BTreeBuilder, BTreeReader |
| `ferrosa-index/src/hash.rs` | Hash index — HashIndexFactory, HashBuilder, HashReader |
| `ferrosa-index/src/composite.rs` | Composite multi-column index — CompositeIndexFactory, CompositeBuilder, CompositeReader |
| `ferrosa-index/src/phonetic/mod.rs` | Phonetic index factory + PhoneticEncoder trait |
| `ferrosa-index/src/phonetic/soundex.rs` | Soundex algorithm |
| `ferrosa-index/src/phonetic/metaphone.rs` | Metaphone algorithm |
| `ferrosa-index/src/phonetic/double_metaphone.rs` | Double Metaphone algorithm |
| `ferrosa-index/src/phonetic/caverphone.rs` | Caverphone algorithm |
| `ferrosa-index/src/filtered.rs` | Filtered index wrapper — delegates to inner factory, filters rows by predicate |
| `ferrosa-index/src/vector/mod.rs` | Vector types (DistanceMetric, VectorMethod), distance functions, dimension constants |
| `ferrosa-index/src/vector/hnsw.rs` | HNSW graph index — HnswFactory, HnswBuilder, HnswReader |
| `ferrosa-index/src/vector/ivfflat.rs` | IVFFlat index — IvfFlatFactory, IvfFlatBuilder, IvfFlatReader |

### Modified Files

| File | Changes |
|------|---------|
| `Cargo.toml` | Add `ferrosa-index` to workspace members |
| `ferrosa-schema/Cargo.toml` | Add ferrosa-index dependency |
| `ferrosa-schema/src/metadata/mod.rs` | Add `pub mod index;` export |
| `ferrosa-schema/src/metadata/index.rs` | NEW: IndexMetadata struct |
| `ferrosa-schema/src/registry.rs` | Add indexes to SchemaSnapshot, add create/drop_index methods |
| `ferrosa-schema/src/system/mod.rs` | Add `pub mod index_tables;` |
| `ferrosa-schema/src/system/index_tables.rs` | NEW: system_schema.indexes virtual table |
| `ferrosa-cql/Cargo.toml` | Add ferrosa-index dependency (for IndexType in AST) |
| `ferrosa-cql/src/ast.rs` | Add CreateIndexStatement, DropIndexStatement, Statement variants |
| `ferrosa-cql/src/parser.rs` | Add parse_create_index(), parse_drop_index() |
| `ferrosa-cql/src/router.rs` | Add route_create_index(), route_drop_index() |
| `ferrosa-storage/Cargo.toml` | Add ferrosa-index dependency |
| `ferrosa-storage/src/engine.rs` | Add IndexBuildScheduler field, notify on flush/compaction |
| `ferrosa-storage/src/upload/manager.rs` | Add UploadTask::IndexFiles variant |
| `ferrosa-cluster/src/pair/ddl.rs` | Add CreateIndex/DropIndex to DdlOperation, handle in apply_ddl_locally |

### Test Files

| File | Tests |
|------|-------|
| `ferrosa-index/src/lib.rs` | Unit tests for core types (IndexKey, RowPosition serialization) |
| `ferrosa-index/src/btree.rs` | Unit tests for B-tree build + lookup + range |
| `ferrosa-index/src/hash.rs` | Unit tests for hash build + lookup |
| `ferrosa-index/src/composite.rs` | Unit tests for composite build + lookup + range |
| `ferrosa-index/src/phonetic/soundex.rs` | Unit tests for Soundex encoding |
| `ferrosa-index/src/phonetic/metaphone.rs` | Unit tests for Metaphone encoding |
| `ferrosa-index/src/phonetic/double_metaphone.rs` | Unit tests for Double Metaphone encoding |
| `ferrosa-index/src/phonetic/caverphone.rs` | Unit tests for Caverphone encoding |
| `ferrosa-index/src/filtered.rs` | Unit tests for filtered build with predicate |
| `ferrosa-index/src/vector/mod.rs` | Unit tests for distance functions |
| `ferrosa-index/src/vector/hnsw.rs` | Unit tests for HNSW build + nearest-neighbor |
| `ferrosa-index/src/vector/ivfflat.rs` | Unit tests for IVFFlat build + nearest-neighbor |
| `ferrosa-cql/src/parser.rs` | Unit tests for CREATE/DROP INDEX parsing (inline #[cfg(test)]) |
| `ferrosa-schema/tests/integration.rs` | Integration tests for index schema CRUD |
| `ferrosa-storage/tests/index_integration.rs` | NEW: Integration tests for IndexBuildScheduler |

---

## Chunk 1: Crate Scaffold and Core Types

### Task 1: Create ferrosa-index crate with Cargo.toml

**Files:**

- Create: `ferrosa-index/Cargo.toml`
- Create: `ferrosa-index/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Create Cargo.toml for ferrosa-index**

```toml
[package]
name = "ferrosa-index"
version = "0.1.0"
edition = "2021"

[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bitflags = "2"
uuid = { version = "1", features = ["v4", "serde"] }
thiserror = "2"

[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Create minimal lib.rs with module declarations**

```rust
pub mod btree;
pub mod composite;
pub mod filtered;
pub mod hash;
pub mod phonetic;
pub mod vector;

// Core types and traits defined below.
```

Stub each module file with `// TODO` so the crate compiles.

- [ ] **Step 3: Add ferrosa-index to workspace members**

In root `Cargo.toml`, add `"ferrosa-index"` to the `members` array after `"ferrosa-graph"`.

- [ ] **Step 4: Verify crate compiles**

Run: `cargo build -p ferrosa-index`
Expected: compiles with no errors

- [ ] **Step 5: Commit**

```bash
git add ferrosa-index/ Cargo.toml Cargo.lock
git commit -m "feat(index): scaffold ferrosa-index crate with module stubs"
```

---

### Task 2: Define core types (IndexType, IndexKey, RowPosition, etc.)

**Files:**

- Modify: `ferrosa-index/src/lib.rs`

- [ ] **Step 1: Write tests for core type serialization roundtrip**

Add to `ferrosa-index/src/lib.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_type_serde_roundtrip() {
        let types = vec![
            IndexType::BTree,
            IndexType::Hash,
            IndexType::Composite { columns: vec!["a".into(), "b".into()] },
            IndexType::Phonetic { algorithm: PhoneticAlgorithm::Soundex },
            IndexType::Vector {
                method: VectorMethod::Hnsw { m: 16, ef_construction: 200 },
                metric: DistanceMetric::Cosine,
                dimensions: 768,
            },
            IndexType::Vector {
                method: VectorMethod::IvfFlat { lists: 100 },
                metric: DistanceMetric::L2,
                dimensions: 1536,
            },
        ];
        for t in &types {
            let json = serde_json::to_string(t).unwrap();
            let back: IndexType = serde_json::from_str(&json).unwrap();
            assert_eq!(*t, back);
        }
    }

    #[test]
    fn row_position_equality() {
        let a = RowPosition {
            partition_key: vec![1, 2, 3],
            clustering_key: vec![4, 5],
        };
        let b = a.clone();
        assert_eq!(a, b);

        let c = RowPosition {
            partition_key: vec![1, 2, 3],
            clustering_key: vec![4, 6],
        };
        assert_ne!(a, c);
    }

    #[test]
    fn index_key_variants() {
        let keys = vec![
            IndexKey::Bytes(vec![0xFF, 0x00]),
            IndexKey::Text("hello".into()),
            IndexKey::Composite(vec![vec![1, 2], vec![3, 4]]),
            IndexKey::Vector(vec![0.1, 0.2, 0.3]),
        ];
        for k in &keys {
            let json = serde_json::to_string(k).unwrap();
            let back: IndexKey = serde_json::from_str(&json).unwrap();
            assert_eq!(*k, back);
        }
    }

    #[test]
    fn index_capabilities_bitflags() {
        let caps = IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN;
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
        assert!(!caps.contains(IndexCapabilities::PHONETIC));
    }

    #[test]
    fn filter_predicate_serde() {
        let pred = FilterPredicate {
            column: "status".into(),
            op: FilterOp::Eq,
            value: b"active".to_vec(),
        };
        let json = serde_json::to_string(&pred).unwrap();
        let back: FilterPredicate = serde_json::from_str(&json).unwrap();
        assert_eq!(pred, back);
    }

    #[test]
    fn filtered_index_type_wraps_inner() {
        let filtered = IndexType::Filtered {
            predicate: FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            inner: Box::new(IndexType::BTree),
        };
        let json = serde_json::to_string(&filtered).unwrap();
        let back: IndexType = serde_json::from_str(&json).unwrap();
        assert_eq!(filtered, back);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-index`
Expected: FAIL — types not defined yet

- [ ] **Step 3: Implement all core types**

In `ferrosa-index/src/lib.rs`, add all type definitions from the spec:

- `IndexType` enum (BTree, Hash, Composite, Phonetic, Filtered, Vector)
- `VectorMethod` enum (Hnsw, IvfFlat)
- `DistanceMetric` enum (L2, Cosine, InnerProduct)
- `PhoneticAlgorithm` enum (Soundex, Metaphone, DoubleMetaphone, Caverphone)
- `RowPosition` struct (partition_key: `Vec<u8>`, clustering_key: `Vec<u8>`)
- `IndexKey` enum (Bytes, Text, Composite, Vector)
- `IndexFiles` struct (data_path, meta_path, meta)
- `IndexFileMeta` struct (index_type, index_name, row_count, build_timestamp, sstable_id, file_size, checksum)
- `IndexConfig` struct (index_type, column_positions, output_dir, sstable_prefix, index_name)
- `IndexCapabilities` bitflags (POINT_LOOKUP, RANGE_SCAN, NEAREST, PHONETIC)
- `FilterPredicate` struct (column, op, value)
- `FilterOp` enum (Eq, NotEq, Lt, Gt, LtEq, GtEq)

See spec "Core Type Definitions" section for exact field types and derives. All enums and structs should derive `PartialEq` alongside `Debug, Clone, Serialize, Deserialize` so tests can use `assert_eq!` directly. Add `use std::path::PathBuf;` for `IndexFiles` and `IndexConfig`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-index`
Expected: all 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-index/src/lib.rs
git commit -m "feat(index): add core types — IndexType, IndexKey, RowPosition, capabilities"
```

---

### Task 3: Define trait system (IndexBuilder, IndexReader, IndexFactory)

**Files:**

- Modify: `ferrosa-index/src/lib.rs`

- [ ] **Step 1: Write test that exercises trait object creation**

```rust
#[test]
fn trait_objects_are_object_safe() {
    // Verify IndexBuilder is object-safe (can be Box<dyn IndexBuilder>)
    fn _assert_builder_object_safe(_: Box<dyn IndexBuilder>) {}
    // Verify IndexReader is object-safe and Send + Sync
    fn _assert_reader_send_sync(_: Arc<dyn IndexReader>) {}
    // Verify IndexFactory is object-safe
    fn _assert_factory_object_safe(_: Box<dyn IndexFactory>) {}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ferrosa-index -- trait_objects`
Expected: FAIL — traits not defined

- [ ] **Step 3: Implement traits**

Add to `ferrosa-index/src/lib.rs`:

```rust
use ferrosa_common::CellValue;
use std::ops::Bound;
use std::sync::Arc;

/// Error type for index operations.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("index build error: {0}")]
    Build(String),
    #[error("index query error: {0}")]
    Query(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

pub type IndexResult<T> = std::result::Result<T, IndexError>;

/// Build-side: called during background index construction.
/// IMPORTANT: Implementations must check `cell.value.is_some()` for the
/// indexed column(s) and skip tombstoned cells (where value is None).
pub trait IndexBuilder: Send {
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[(u16, CellValue)],
    ) -> IndexResult<()>;

    fn finish(self: Box<Self>) -> IndexResult<IndexFiles>;
}

/// Read-side: query an index built for one SSTable.
pub trait IndexReader: Send + Sync {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>>;

    fn range(
        &self,
        start: Bound<&IndexKey>,
        end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>>;

    fn nearest(
        &self,
        query: &[f32],
        k: usize,
        ef_search: Option<u16>,
    ) -> IndexResult<Vec<(RowPosition, f32)>>;

    fn capabilities(&self) -> IndexCapabilities;
}

/// Factory: registered per IndexType, creates builders and readers.
pub trait IndexFactory: Send + Sync {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>>;
    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>>;
    fn merge(
        &self,
        readers: Vec<Box<dyn IndexReader>>,
        builder: Box<dyn IndexBuilder>,
    ) -> IndexResult<IndexFiles>;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-index`
Expected: all tests PASS

- [ ] **Step 5: Run clippy**

Run: `cargo clippy -p ferrosa-index --all-targets`
Expected: no warnings

- [ ] **Step 6: Commit**

```bash
git add ferrosa-index/src/lib.rs
git commit -m "feat(index): add IndexBuilder, IndexReader, IndexFactory traits"
```

---

## Chunk 2: Schema Integration

### Task 4: Add IndexMetadata to ferrosa-schema

**Files:**

- Create: `ferrosa-schema/src/metadata/index.rs`
- Modify: `ferrosa-schema/src/metadata/mod.rs`
- Modify: `ferrosa-schema/Cargo.toml`

- [ ] **Step 1: Add ferrosa-index dependency to ferrosa-schema**

In `ferrosa-schema/Cargo.toml`, add:

```toml
ferrosa-index = { path = "../ferrosa-index" }
```

- [ ] **Step 2: Write test for IndexMetadata serde**

In `ferrosa-schema/src/metadata/index.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_metadata_serde_roundtrip() {
        let meta = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_email".into(),
            index_type: IndexType::BTree,
            target_columns: vec!["email".into()],
            filter_predicate: None,
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta.name, back.name);
        assert_eq!(meta.target_columns, back.target_columns);
    }

    #[test]
    fn index_metadata_with_filter() {
        let meta = IndexMetadata {
            keyspace: "ks".into(),
            table: "tbl".into(),
            name: "idx_active".into(),
            index_type: IndexType::Filtered {
                predicate: FilterPredicate {
                    column: "status".into(),
                    op: FilterOp::Eq,
                    value: b"active".to_vec(),
                },
                inner: Box::new(IndexType::BTree),
            },
            target_columns: vec!["email".into()],
            filter_predicate: Some(FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            }),
            options: HashMap::new(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert!(back.filter_predicate.is_some());
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p ferrosa-schema -- index_metadata`
Expected: FAIL — module not found

- [ ] **Step 4: Implement IndexMetadata**

Create `ferrosa-schema/src/metadata/index.rs`:

```rust
use ferrosa_index::{FilterOp, FilterPredicate, IndexType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub keyspace: String,
    pub table: String,
    pub name: String,
    pub index_type: IndexType,
    pub target_columns: Vec<String>,
    pub filter_predicate: Option<FilterPredicate>,
    pub options: HashMap<String, String>,
}
```

Add `pub mod index;` and re-export to `ferrosa-schema/src/metadata/mod.rs`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-schema -- index_metadata`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-schema/Cargo.toml ferrosa-schema/src/metadata/index.rs ferrosa-schema/src/metadata/mod.rs
git commit -m "feat(schema): add IndexMetadata struct with serde support"
```

---

### Task 5: Add indexes to SchemaSnapshot and Schema registry

**Files:**

- Modify: `ferrosa-schema/src/registry.rs`

- [ ] **Step 1: Write tests for index CRUD on Schema**

Add to `ferrosa-schema/tests/integration.rs`:

```rust
#[test]
fn create_and_drop_index() {
    let schema = test_schema();
    let auth = superuser_auth();

    // Create a keyspace and table first
    let ks = KeyspaceMetadata { /* ... standard test keyspace ... */ };
    schema.create_keyspace(ks, &auth).unwrap();
    let table = test_table("test_ks", "users");
    schema.create_table(table, &auth).unwrap();

    // Create an index
    let idx = IndexMetadata {
        keyspace: "test_ks".into(),
        table: "users".into(),
        name: "idx_email".into(),
        index_type: IndexType::BTree,
        target_columns: vec!["email".into()],
        filter_predicate: None,
        options: HashMap::new(),
    };
    schema.create_index(idx.clone(), &auth).unwrap();

    // Verify index exists in snapshot
    let snap = schema.snapshot();
    assert!(snap.indexes.contains_key(&("test_ks".into(), "users".into(), "idx_email".into())));

    // Drop the index
    schema.drop_index("test_ks", "users", "idx_email", &auth).unwrap();
    let snap = schema.snapshot();
    assert!(!snap.indexes.contains_key(&("test_ks".into(), "users".into(), "idx_email".into())));
}

#[test]
fn create_index_internal_is_idempotent() {
    let schema = test_schema();
    let idx = IndexMetadata {
        keyspace: "test_ks".into(),
        table: "users".into(),
        name: "idx_email".into(),
        index_type: IndexType::BTree,
        target_columns: vec!["email".into()],
        filter_predicate: None,
        options: HashMap::new(),
    };
    // First call succeeds
    schema.create_index_internal(idx.clone()).unwrap();
    // Second call also succeeds (idempotent)
    schema.create_index_internal(idx).unwrap();
}

#[test]
fn drop_index_internal_is_idempotent() {
    let schema = test_schema();
    // Drop nonexistent index — succeeds silently
    schema.drop_index_internal("ks", "tbl", "idx").unwrap();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-schema -- index`
Expected: FAIL — no indexes field or methods

- [ ] **Step 3: Add indexes field to SchemaSnapshot**

In `ferrosa-schema/src/registry.rs`, add to `SchemaSnapshot`:

```rust
#[serde(default)]
pub indexes: HashMap<(String, String, String), IndexMetadata>,
```

Update `SchemaSnapshot::new()` to initialize `indexes: HashMap::new()`.

- [ ] **Step 4: Add create/drop index methods to Schema**

Add to the `Schema` impl block:

```rust
pub fn create_index_internal(&self, index: IndexMetadata) -> Result<()> {
    let _lock = self.write_lock.lock().unwrap();
    let mut snap = (*self.inner.load_full()).clone();
    let key = (index.keyspace.clone(), index.table.clone(), index.name.clone());
    snap.indexes.entry(key).or_insert(index);
    snap.version = Uuid::new_v4();
    self.inner.store(Arc::new(snap));
    Ok(())
}

pub fn drop_index_internal(&self, keyspace: &str, table: &str, name: &str) -> Result<()> {
    let _lock = self.write_lock.lock().unwrap();
    let mut snap = (*self.inner.load_full()).clone();
    snap.indexes.remove(&(keyspace.to_string(), table.to_string(), name.to_string()));
    snap.version = Uuid::new_v4();
    self.inner.store(Arc::new(snap));
    Ok(())
}

pub fn create_index(&self, index: IndexMetadata, auth: &AuthContext) -> Result<()> {
    let resource = Resource::Table {
        keyspace: index.keyspace.clone(),
        table: index.table.clone(),
    };
    self.check_permission(auth, Permission::Alter, &resource)?;
    self.create_index_internal(index)
}

pub fn drop_index(&self, keyspace: &str, table: &str, name: &str, auth: &AuthContext) -> Result<()> {
    let resource = Resource::Table {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
    };
    self.check_permission(auth, Permission::Alter, &resource)?;
    self.drop_index_internal(keyspace, table, name)
}
```

- [ ] **Step 5: Update apply_snapshot to include indexes**

In `Schema::apply_snapshot()`, after the tables loop, add:

```rust
for ((ks, tbl, name), index) in &snapshot.indexes {
    snap.indexes
        .entry((ks.clone(), tbl.clone(), name.clone()))
        .or_insert_with(|| index.clone());
}
```

- [ ] **Step 6: Update drop_keyspace_internal to clean up indexes**

In `Schema::drop_keyspace_internal()`, after removing tables, add:

```rust
snap.indexes.retain(|(ks, _, _), _| ks != name);
```

- [ ] **Step 7: Update drop_table_internal to clean up indexes**

In `Schema::drop_table_internal()`, add:

```rust
snap.indexes.retain(|(ks, tbl, _), _| !(ks == keyspace && tbl == table));
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cargo test -p ferrosa-schema -- index`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add ferrosa-schema/src/registry.rs ferrosa-schema/tests/integration.rs
git commit -m "feat(schema): add indexes to SchemaSnapshot with CRUD methods"
```

---

### Task 6: Add system_schema.indexes virtual table

**Files:**

- Create: `ferrosa-schema/src/system/index_tables.rs`
- Modify: `ferrosa-schema/src/system/mod.rs`

- [ ] **Step 1: Write test for virtual table output**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_table_columns() {
        let table = SystemSchemaIndexesTable::new(/* schema_ref */);
        let cols = table.columns();
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec![
            "keyspace_name", "table_name", "index_name",
            "kind", "target", "options",
        ]);
    }

    #[test]
    fn indexes_table_returns_rows() {
        // Set up schema with an index, verify read() returns it
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-schema -- indexes_table`
Expected: FAIL

- [ ] **Step 3: Implement SystemSchemaIndexesTable**

Create `ferrosa-schema/src/system/index_tables.rs` implementing the `VirtualTable` trait. The `read()` method iterates `schema_ref.load().indexes` and produces rows with columns: keyspace_name, table_name, index_name, kind (derived from IndexType), target (comma-joined column names), options (JSON-serialized map).

- [ ] **Step 4: Register the virtual table**

In `ferrosa-schema/src/system/mod.rs`, add `pub mod index_tables;`. Register `SystemSchemaIndexesTable` in the virtual table registry during `Schema::new()`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-schema -- indexes_table`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-schema/src/system/index_tables.rs ferrosa-schema/src/system/mod.rs ferrosa-schema/src/registry.rs
git commit -m "feat(schema): add system_schema.indexes virtual table"
```

---

## Chunk 3: CQL Parser and Router Integration

### Task 7: Add CreateIndex/DropIndex to CQL AST

**Files:**

- Modify: `ferrosa-cql/Cargo.toml`
- Modify: `ferrosa-cql/src/ast.rs`

- [ ] **Step 1: Add ferrosa-index dependency to ferrosa-cql**

In `ferrosa-cql/Cargo.toml`, add:

```toml
ferrosa-index = { path = "../ferrosa-index" }
```

- [ ] **Step 2: Add AST types**

In `ferrosa-cql/src/ast.rs`, add:

```rust
pub struct CreateIndexStatement {
    pub name: Option<String>,
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub using: Option<String>,       // "btree", "hash", "vector", etc.
    pub filter: Option<String>,      // raw WHERE clause text for later parsing
    pub options: Vec<(String, String)>,
    pub if_not_exists: bool,
}

pub struct DropIndexStatement {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
}
```

Add `CreateIndex(CreateIndexStatement)` and `DropIndex(DropIndexStatement)` to the `Statement` enum.

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p ferrosa-cql`
Expected: compiles (router will have unmatched arms — add `_` placeholder if needed)

- [ ] **Step 4: Commit**

```bash
git add ferrosa-cql/Cargo.toml ferrosa-cql/src/ast.rs
git commit -m "feat(cql): add CreateIndex and DropIndex AST types"
```

---

### Task 8: Implement CREATE INDEX parser

**Files:**

- Modify: `ferrosa-cql/src/parser.rs`

- [ ] **Step 1: Write parser tests**

Add to the `#[cfg(test)]` module in `ferrosa-cql/src/parser.rs`:

```rust
#[test]
fn parse_create_btree_index() {
    let stmt = parse("CREATE INDEX idx_email ON users (email) USING 'btree'").unwrap();
    match stmt {
        Statement::CreateIndex(s) => {
            assert_eq!(s.name, Some("idx_email".into()));
            assert_eq!(s.table, "users");
            assert_eq!(s.columns, vec!["email"]);
            assert_eq!(s.using, Some("btree".into()));
            assert!(!s.if_not_exists);
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_index_default_type() {
    let stmt = parse("CREATE INDEX idx_email ON users (email)").unwrap();
    match stmt {
        Statement::CreateIndex(s) => {
            assert_eq!(s.using, None); // defaults to btree at routing layer
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_vector_index_with_options() {
    let stmt = parse(
        "CREATE INDEX idx_embed ON docs (embedding) USING 'vector' \
         WITH OPTIONS = {'method': 'hnsw', 'metric': 'cosine', 'dimensions': '768'}"
    ).unwrap();
    match stmt {
        Statement::CreateIndex(s) => {
            assert_eq!(s.using, Some("vector".into()));
            assert_eq!(s.options.len(), 3);
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_composite_index() {
    let stmt = parse(
        "CREATE INDEX idx_name ON users (last_name, first_name) USING 'composite'"
    ).unwrap();
    match stmt {
        Statement::CreateIndex(s) => {
            assert_eq!(s.columns, vec!["last_name", "first_name"]);
            assert_eq!(s.using, Some("composite".into()));
        }
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_create_index_if_not_exists() {
    let stmt = parse("CREATE INDEX IF NOT EXISTS idx ON t (c) USING 'hash'").unwrap();
    match stmt {
        Statement::CreateIndex(s) => assert!(s.if_not_exists),
        _ => panic!("expected CreateIndex"),
    }
}

#[test]
fn parse_drop_index() {
    let stmt = parse("DROP INDEX idx_email").unwrap();
    match stmt {
        Statement::DropIndex(s) => {
            assert_eq!(s.name, "idx_email");
            assert!(!s.if_exists);
        }
        _ => panic!("expected DropIndex"),
    }
}

#[test]
fn parse_drop_index_if_exists() {
    let stmt = parse("DROP INDEX IF EXISTS ks.idx_email").unwrap();
    match stmt {
        Statement::DropIndex(s) => {
            assert_eq!(s.keyspace, Some("ks".into()));
            assert_eq!(s.name, "idx_email");
            assert!(s.if_exists);
        }
        _ => panic!("expected DropIndex"),
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- parse_create_index parse_drop_index`
Expected: FAIL

- [ ] **Step 3: Implement parse_create_index()**

In `ferrosa-cql/src/parser.rs`, add `parse_create_index()` method. Follow the `parse_create_table()` pattern:

1. Consume `INDEX` keyword
2. Check for `IF NOT EXISTS`
3. Parse optional index name (identifier)
4. Consume `ON`
5. Parse optional `keyspace.` prefix and table name
6. Parse `(column, ...)` list
7. Check for `USING 'type'`
8. Check for `WITH OPTIONS = {map}`
9. Return `CreateIndexStatement`

Also update `parse_create()` to check for `INDEX` keyword and dispatch to `parse_create_index()`.

- [ ] **Step 4: Implement parse_drop_index()**

Add `parse_drop_index()`. Follow the `parse_drop_table()` pattern:

1. Consume `INDEX`
2. Check for `IF EXISTS`
3. Parse optional `keyspace.` prefix and index name
4. Return `DropIndexStatement`

Update `parse_drop()` to check for `INDEX`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- parse_create_index parse_drop_index`
Expected: all 7 tests PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-cql/src/parser.rs
git commit -m "feat(cql): implement CREATE INDEX and DROP INDEX parsers"
```

---

### Task 9: Implement CQL router for CREATE/DROP INDEX

**Files:**

- Modify: `ferrosa-cql/src/router.rs`

- [ ] **Step 1: Write router unit tests**

Add to `ferrosa-cql/src/router.rs` `#[cfg(test)]` module:

```rust
#[test]
fn resolve_index_type_defaults_to_btree() {
    assert_eq!(resolve_index_type(None, &HashMap::new()).unwrap(), IndexType::BTree);
}

#[test]
fn resolve_index_type_hash() {
    assert_eq!(resolve_index_type(Some("hash"), &HashMap::new()).unwrap(), IndexType::Hash);
}

#[test]
fn resolve_index_type_vector_hnsw() {
    let opts: HashMap<String, String> = [
        ("method", "hnsw"), ("metric", "cosine"),
        ("dimensions", "768"), ("m", "16"), ("ef_construction", "200"),
    ].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let t = resolve_index_type(Some("vector"), &opts).unwrap();
    match t {
        IndexType::Vector { method: VectorMethod::Hnsw { m: 16, ef_construction: 200 }, metric: DistanceMetric::Cosine, dimensions: 768 } => {}
        _ => panic!("expected Vector HNSW, got {:?}", t),
    }
}

#[test]
fn resolve_index_type_vector_missing_dimensions_errors() {
    let opts: HashMap<String, String> = [("method", "hnsw"), ("metric", "l2")]
        .iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    assert!(resolve_index_type(Some("vector"), &opts).is_err());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-cql -- resolve_index_type`
Expected: FAIL

- [ ] **Step 3: Implement resolve_index_type() helper**

Parses `using` string + options map into `IndexType`. Handles: "btree" (default), "hash", "composite", "phonetic" (requires `algorithm` option), "vector" (requires `method`, `metric`, `dimensions`). Also parse `WHERE` clause text from `CreateIndexStatement.filter` into `FilterPredicate` here.

- [ ] **Step 4: Add route_create_index()**

Follow the `route_create_table()` pattern (lines 1015-1117):

1. Resolve keyspace from statement or current_keyspace
2. Check ALTER permission on the table resource
3. Call `resolve_index_type()` to build `IndexType` from `using` + options
4. Convert options from `Vec<(String, String)>` to `HashMap<String, String>`
5. Construct `IndexMetadata`
6. Route through `DdlPath` (Direct → `schema.create_index()`, Pair → `coordinator.coordinate_ddl(CreateIndex(...))`, Unavailable → error)
7. Return schema change result

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-cql -- resolve_index_type`
Expected: PASS

- [ ] **Step 6: Add route_drop_index()**

Follow the `route_drop_table()` pattern:

1. Resolve keyspace
2. Check ALTER permission
3. Route through `DdlPath` (Direct → `schema.drop_index()`, Pair → `coordinator.coordinate_ddl(DropIndex {...})`, Unavailable → error)
4. Return schema change result

- [ ] **Step 3: Add dispatch cases in route()**

In the main `route()` function, add match arms:

```rust
Statement::CreateIndex(s) => route_create_index(state, &ctx, s).await,
Statement::DropIndex(s) => route_drop_index(state, &ctx, s).await,
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p ferrosa-cql`
Expected: compiles

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cql/src/router.rs
git commit -m "feat(cql): route CREATE INDEX and DROP INDEX through DdlPath"
```

---

## Chunk 4: Multi-Node DDL Coordination

### Task 10: Extend DdlOperation for indexes

**Files:**

- Modify: `ferrosa-cluster/src/pair/ddl.rs`

- [ ] **Step 1: Add index variants to DdlOperation**

```rust
pub enum DdlOperation {
    CreateKeyspace(KeyspaceMetadata),
    DropKeyspace(String),
    CreateTable(Box<TableMetadata>),
    DropTable { keyspace: String, table: String },
    CreateIndex(IndexMetadata),
    DropIndex { keyspace: String, table: String, index: String },
}
```

- [ ] **Step 2: Add match arms in apply_ddl_locally()**

```rust
DdlOperation::CreateIndex(ref idx) => {
    self.schema.create_index_internal(idx.clone())?;
}
DdlOperation::DropIndex { ref keyspace, ref table, ref index } => {
    self.schema.drop_index_internal(keyspace, table, index)?;
}
```

- [ ] **Step 3: Update WireSchemaSnapshot**

Add `#[serde(default)] pub indexes: Vec<((String, String, String), IndexMetadata)>` to `WireSchemaSnapshot`.

Update `from_snapshot()` to include indexes:

```rust
indexes: snap.indexes.iter()
    .map(|(k, v)| (k.clone(), v.clone()))
    .collect(),
```

Update `into_snapshot()` to include indexes:

```rust
indexes: self.indexes.into_iter().collect(),
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p ferrosa-cluster`
Expected: compiles

- [ ] **Step 5: Commit**

```bash
git add ferrosa-cluster/src/pair/ddl.rs
git commit -m "feat(cluster): add CreateIndex/DropIndex to DdlOperation and WireSchemaSnapshot"
```

---

## Chunk 5: Index Implementations — B-tree, Hash, Composite

### Task 11: Implement B-tree index

**Files:**

- Modify: `ferrosa-index/src/btree.rs`

- [ ] **Step 1: Write tests for B-tree index**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btree_build_and_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let config = IndexConfig {
            index_type: IndexType::BTree,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            sstable_prefix: "1-bti".into(),
            index_name: "idx_email".into(),
        };

        let factory = BTreeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        // Add rows with email values
        builder.add_row(b"pk1", b"ck1", &[(0, CellValue::live(b"alice@example.com".to_vec(), 1))]).unwrap();
        builder.add_row(b"pk2", b"ck1", &[(0, CellValue::live(b"bob@example.com".to_vec(), 1))]).unwrap();
        builder.add_row(b"pk3", b"ck1", &[(0, CellValue::live(b"charlie@example.com".to_vec(), 1))]).unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Point lookup
        let results = reader.lookup(&IndexKey::Bytes(b"bob@example.com".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");

        // Range scan
        let results = reader.range(
            Bound::Included(&IndexKey::Bytes(b"alice@example.com".to_vec())),
            Bound::Included(&IndexKey::Bytes(b"bob@example.com".to_vec())),
        ).unwrap();
        assert_eq!(results.len(), 2);

        // Capabilities
        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
    }

    #[test]
    fn btree_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = IndexConfig { /* ... */ };
        let factory = BTreeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();
        let results = reader.lookup(&IndexKey::Bytes(b"anything".to_vec())).unwrap();
        assert!(results.is_empty());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-index -- btree`
Expected: FAIL

- [ ] **Step 3: Implement BTreeIndexFactory, BTreeBuilder, BTreeReader**

Use a sorted Vec of `(key_bytes, RowPosition)` as the in-memory B-tree representation. On `finish()`, serialize to a binary file format: header (entry count) + sorted entries (key_len: u32, key: [u8], partition_key_len: u32, partition_key: [u8], clustering_key_len: u32, clustering_key: [u8]). On `open_reader()`, deserialize back. `lookup()` uses binary search. `range()` uses binary search for start bound then scans forward.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-index -- btree`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-index/src/btree.rs
git commit -m "feat(index): implement B-tree index with point lookup and range scan"
```

---

### Task 12: Implement Hash index

**Files:**

- Modify: `ferrosa-index/src/hash.rs`

- [ ] **Step 1: Write tests for hash index**

Tests: build with rows, point lookup succeeds, range returns Unsupported error, capabilities only has POINT_LOOKUP.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-index -- hash`

- [ ] **Step 3: Implement HashIndexFactory, HashBuilder, HashReader**

Use a hash map serialized as: header (bucket_count, entry_count) + bucket array (offset into entries section) + entries (hash, key_len, key, position). On `open_reader()`, load and rebuild HashMap. `lookup()` does hash match. `range()` returns `IndexError::Unsupported`. `nearest()` returns `IndexError::Unsupported`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-index -- hash`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-index/src/hash.rs
git commit -m "feat(index): implement hash index with O(1) point lookup"
```

---

### Task 13: Implement Composite index

**Files:**

- Modify: `ferrosa-index/src/composite.rs`

- [ ] **Step 1: Write tests for composite index**

Tests: build with multi-column rows, lookup with full composite key, prefix range scan (e.g., all entries starting with "Smith"), capabilities has POINT_LOOKUP | RANGE_SCAN.

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement CompositeIndexFactory**

Reuse B-tree internals with concatenated column keys. `add_row()` extracts values from multiple `column_positions` and concatenates their byte-comparable encodings with length prefixes. The rest of the B-tree logic applies unchanged.

- [ ] **Step 4: Run tests to verify they pass**

- [ ] **Step 5: Commit**

```bash
git add ferrosa-index/src/composite.rs
git commit -m "feat(index): implement composite multi-column index"
```

---

## Chunk 6: Phonetic and Filtered Indexes

### Task 14: Implement phonetic encoding algorithms

**Files:**

- Create: `ferrosa-index/src/phonetic/mod.rs`
- Create: `ferrosa-index/src/phonetic/soundex.rs`
- Create: `ferrosa-index/src/phonetic/metaphone.rs`
- Create: `ferrosa-index/src/phonetic/double_metaphone.rs`
- Create: `ferrosa-index/src/phonetic/caverphone.rs`

- [ ] **Step 1: Define PhoneticEncoder trait**

In `ferrosa-index/src/phonetic/mod.rs`:

```rust
pub trait PhoneticEncoder: Send + Sync {
    fn encode(&self, input: &str) -> String;
}
```

- [ ] **Step 2: Write tests for Soundex**

```rust
#[test]
fn soundex_standard_cases() {
    let enc = SoundexEncoder;
    assert_eq!(enc.encode("Robert"), "R163");
    assert_eq!(enc.encode("Rupert"), "R163");
    assert_eq!(enc.encode("Smith"), "S530");
    assert_eq!(enc.encode("Smythe"), "S530");
    assert_eq!(enc.encode("Ashcraft"), "A261");
}
```

- [ ] **Step 3: Implement Soundex algorithm**

Standard Soundex: retain first letter, map consonants to digits (B/F/P/V→1, C/G/J/K/Q/S/X/Z→2, D/T→3, L→4, M/N→5, R→6), drop A/E/I/O/U/H/W/Y, collapse adjacent same digits, pad/truncate to 4 chars.

- [ ] **Step 4: Write tests and implement Metaphone, Double Metaphone, Caverphone**

Follow the same pattern for each algorithm. Each gets its own file with unit tests for known input/output pairs.

- [ ] **Step 5: Run all phonetic tests**

Run: `cargo test -p ferrosa-index -- phonetic`
Expected: PASS

- [ ] **Step 6: Implement PhoneticIndexFactory**

In `ferrosa-index/src/phonetic/mod.rs`: factory creates a builder that encodes each text value with the configured algorithm, then stores `(phonetic_code, RowPosition)` pairs. Reader does hash-map lookup by encoding the query string and finding matches. Capabilities: `POINT_LOOKUP | PHONETIC`.

- [ ] **Step 7: Commit**

```bash
git add ferrosa-index/src/phonetic/
git commit -m "feat(index): implement phonetic indexes with Soundex, Metaphone, Double Metaphone, Caverphone"
```

---

### Task 15: Implement Filtered index wrapper

**Files:**

- Modify: `ferrosa-index/src/filtered.rs`

- [ ] **Step 1: Write tests for filtered index**

```rust
#[test]
fn filtered_index_only_includes_matching_rows() {
    let dir = tempfile::tempdir().unwrap();
    let config = IndexConfig {
        index_type: IndexType::Filtered {
            predicate: FilterPredicate {
                column: "status".into(),
                op: FilterOp::Eq,
                value: b"active".to_vec(),
            },
            inner: Box::new(IndexType::BTree),
        },
        column_positions: vec![0, 1], // col 0 = indexed col, col 1 = status col
        output_dir: dir.path().to_path_buf(),
        sstable_prefix: "1-bti".into(),
        index_name: "idx_filtered".into(),
    };

    let factory = FilteredIndexFactory::new(
        FilterPredicate { column: "status".into(), op: FilterOp::Eq, value: b"active".to_vec() },
        1, // status column position
        Box::new(BTreeIndexFactory),
    );
    let mut builder = factory.create_builder(&config).unwrap();

    // Row with status=active — should be included
    builder.add_row(b"pk1", b"ck1", &[
        (0, CellValue::live(b"alice@example.com".to_vec(), 1)),
        (1, CellValue::live(b"active".to_vec(), 1)),
    ]).unwrap();

    // Row with status=inactive — should be excluded
    builder.add_row(b"pk2", b"ck1", &[
        (0, CellValue::live(b"bob@example.com".to_vec(), 1)),
        (1, CellValue::live(b"inactive".to_vec(), 1)),
    ]).unwrap();

    let files = builder.finish().unwrap();
    let reader = factory.open_reader(&files).unwrap();

    // Only alice should be found
    let results = reader.lookup(&IndexKey::Bytes(b"alice@example.com".to_vec())).unwrap();
    assert_eq!(results.len(), 1);

    let results = reader.lookup(&IndexKey::Bytes(b"bob@example.com".to_vec())).unwrap();
    assert!(results.is_empty());
}
```

- [ ] **Step 2: Implement FilteredIndexFactory**

`FilteredIndexFactory` wraps another `IndexFactory`. Its builder checks each row against the predicate in `add_row()` — if the filter column's value matches, it delegates to the inner builder; otherwise it skips the row. Reader and merge delegate directly to the inner factory.

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-index -- filtered`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ferrosa-index/src/filtered.rs
git commit -m "feat(index): implement filtered index wrapper with predicate evaluation"
```

---

## Chunk 7: Vector Index — Distance Functions and HNSW

### Task 16: Implement distance functions

**Files:**

- Modify: `ferrosa-index/src/vector/mod.rs`

- [ ] **Step 1: Write tests for distance metrics**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_distance_zero_for_same_vector() {
        let v = vec![1.0, 2.0, 3.0];
        assert!((l2_distance(&v, &v) - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn l2_distance_known_value() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        assert!((l2_distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_distance_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_distance_same_direction() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![2.0, 4.0, 6.0];
        assert!(cosine_distance(&a, &b) < 1e-6);
    }

    #[test]
    fn inner_product_known_value() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        // 1*4 + 2*5 + 3*6 = 32
        // negative inner product as distance (larger dot = smaller distance)
        assert!((inner_product_distance(&a, &b) - (-32.0)).abs() < 1e-6);
    }

    #[test]
    fn dimension_limits() {
        assert_eq!(VECTOR_MAX_DIMENSIONS_F32, 4096);
        assert_eq!(VECTOR_MAX_DIMENSIONS_F16, 8192);
        assert_eq!(VECTOR_PERF_WARNING_THRESHOLD, 2048);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-index -- vector`

- [ ] **Step 3: Implement distance functions and constants**

```rust
pub const VECTOR_MAX_DIMENSIONS_F32: u32 = 4096;
pub const VECTOR_MAX_DIMENSIONS_F16: u32 = 8192;
pub const VECTOR_PERF_WARNING_THRESHOLD: u32 = 2048;

pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt()
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    1.0 - dot / (norm_a * norm_b)
}

pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
}

pub fn distance(metric: &DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => inner_product_distance(a, b),
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-index -- vector`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ferrosa-index/src/vector/mod.rs
git commit -m "feat(index): implement L2, cosine, and inner product distance functions"
```

---

### Task 17: Implement HNSW index

**Files:**

- Modify: `ferrosa-index/src/vector/hnsw.rs`

- [ ] **Step 1: Write tests for HNSW**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hnsw_build_and_nearest() {
        let dir = tempfile::tempdir().unwrap();
        let config = IndexConfig {
            index_type: IndexType::Vector {
                method: VectorMethod::Hnsw { m: 4, ef_construction: 16 },
                metric: DistanceMetric::L2,
                dimensions: 3,
            },
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            sstable_prefix: "1-bti".into(),
            index_name: "idx_vec".into(),
        };

        let factory = HnswFactory::new(DistanceMetric::L2);
        let mut builder = factory.create_builder(&config).unwrap();

        // Insert vectors as CellValue (raw f32 bytes)
        let vectors: Vec<(Vec<u8>, Vec<u8>, Vec<f32>)> = vec![
            (b"pk1".to_vec(), b"ck1".to_vec(), vec![1.0, 0.0, 0.0]),
            (b"pk2".to_vec(), b"ck1".to_vec(), vec![0.0, 1.0, 0.0]),
            (b"pk3".to_vec(), b"ck1".to_vec(), vec![0.0, 0.0, 1.0]),
            (b"pk4".to_vec(), b"ck1".to_vec(), vec![0.9, 0.1, 0.0]),
        ];

        for (pk, ck, vec) in &vectors {
            let bytes: Vec<u8> = vec.iter().flat_map(|f| f.to_le_bytes()).collect();
            builder.add_row(pk, ck, &[(0, CellValue::live(bytes, 1))]).unwrap();
        }

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Query near [1.0, 0.0, 0.0] — should find pk1 as nearest
        let results = reader.nearest(&[1.0, 0.0, 0.0], 2, None).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.partition_key, b"pk1");
        assert_eq!(results[1].0.partition_key, b"pk4"); // second nearest

        // Capabilities
        assert!(reader.capabilities().contains(IndexCapabilities::NEAREST));
    }

    #[test]
    fn hnsw_empty_index() {
        // Build with no rows, nearest returns empty
    }

    #[test]
    fn hnsw_lookup_returns_unsupported() {
        // Point lookup on vector index should return Unsupported
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement HNSW data structures**

Implement the core HNSW graph:

- `HnswGraph` struct: layers (Vec of adjacency lists), entry point, vectors, positions
- Layer assignment: `l = floor(-ln(rand()) * mL)` where `mL = 1.0 / ln(m as f64)`
- Insert: greedy search from top layer, connect to M nearest neighbors at each layer
- Search: greedy descent from entry point, use `ef_search` candidates at base layer
- Serialization: write graph layers + vectors + row positions to .db file
- Deserialization: read back from .db file

- [ ] **Step 4: Implement HnswFactory, HnswBuilder, HnswReader**

- `HnswBuilder::add_row()` — extract vector bytes from cell, parse as `Vec<f32>`, insert into in-memory graph
- `HnswBuilder::finish()` — serialize graph to .db file, write .meta file
- `HnswReader` — load graph from .db file, implement `nearest()` as beam search
- `lookup()` and `range()` return `IndexError::Unsupported`

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-index -- hnsw`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-index/src/vector/hnsw.rs
git commit -m "feat(index): implement HNSW vector index with graph construction and ANN search"
```

---

### Task 18: Implement IVFFlat index

**Files:**

- Modify: `ferrosa-index/src/vector/ivfflat.rs`

- [ ] **Step 1: Write tests for IVFFlat**

Tests: build with vectors, k-means produces expected clusters, nearest-neighbor search finds correct results, empty index works, lookup/range return Unsupported.

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement k-means clustering**

Simple k-means: random centroid initialization, iterative assignment + recomputation, configurable `lists` parameter for number of clusters.

- [ ] **Step 4: Implement IvfFlatFactory, IvfFlatBuilder, IvfFlatReader**

- Builder: accumulates all vectors, runs k-means on `finish()`, writes centroids + inverted lists
- Reader: loads centroids, searches nearest `probes` clusters, brute-force within selected clusters
- `merge()`: recomputes centroids across all input readers' data for better quality

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-index -- ivfflat`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-index/src/vector/ivfflat.rs
git commit -m "feat(index): implement IVFFlat vector index with k-means clustering"
```

---

## Chunk 8: Async Build Pipeline and Staleness Tracking

### Task 19: Add IndexBuildScheduler to ferrosa-storage

**Files:**

- Create: `ferrosa-storage/src/index/mod.rs`
- Create: `ferrosa-storage/src/index/scheduler.rs`
- Create: `ferrosa-storage/src/index/tracker.rs`
- Modify: `ferrosa-storage/Cargo.toml`
- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Add ferrosa-index dependency to ferrosa-storage**

In `ferrosa-storage/Cargo.toml`:

```toml
ferrosa-index = { path = "../ferrosa-index" }
```

- [ ] **Step 2: Add `pub mod index;` to `ferrosa-storage/src/lib.rs`**

- [ ] **Step 3: Write tests for IndexStateTracker**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_starts_empty() {
        let tracker = IndexStateTracker::new();
        assert!(tracker.get_state("ks", "tbl", "idx").is_none());
    }

    #[test]
    fn tracker_register_and_mark_pending() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx_email");
        tracker.mark_pending("ks", "tbl", "idx_email", "sstable-1", 1024);

        let state = tracker.get_state("ks", "tbl", "idx_email").unwrap();
        assert_eq!(state.pending_sstables.len(), 1);
        assert_eq!(state.pending_bytes, 1024);
        assert!(matches!(state.status, IndexStatus::Stale { .. }));
    }

    #[test]
    fn tracker_mark_indexed_transitions_to_current() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-1", 512);
        tracker.mark_indexed("ks", "tbl", "idx", "sst-1");

        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(state.pending_sstables.is_empty());
        assert!(matches!(state.status, IndexStatus::Current));
    }

    #[test]
    fn tracker_remove_index() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        tracker.remove_index("ks", "tbl", "idx");
        assert!(tracker.get_state("ks", "tbl", "idx").is_none());
    }

    #[test]
    fn tracker_get_coverage() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-1", 512);
        tracker.mark_pending("ks", "tbl", "idx", "sst-2", 256);
        tracker.mark_indexed("ks", "tbl", "idx", "sst-1");

        let (indexed, unindexed) = tracker.get_coverage("ks", "tbl", "idx");
        assert_eq!(indexed.len(), 1);
        assert!(indexed.contains("sst-1"));
        assert_eq!(unindexed.len(), 1);
        assert!(unindexed.contains("sst-2"));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

- [ ] **Step 4: Implement IndexStateTracker**

In `ferrosa-storage/src/index/tracker.rs`:

- Internal state: `RwLock<HashMap<(String, String, String), IndexState>>`
- Methods: `register_index()`, `remove_index()`, `mark_pending()`, `mark_indexed()`, `mark_failed()`, `get_state()`, `get_coverage()`, `all_states()` (for virtual table)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage -- tracker`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-storage/src/index/ ferrosa-storage/Cargo.toml ferrosa-storage/src/lib.rs
git commit -m "feat(storage): add IndexStateTracker for per-index staleness tracking"
```

---

### Task 20: Implement IndexBuildScheduler

**Files:**

- Modify: `ferrosa-storage/src/index/scheduler.rs`

- [ ] **Step 1: Write tests for scheduler**

```rust
#[test]
fn scheduler_processes_build_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let scheduler = IndexBuildScheduler::new(2, tracker.clone()); // 2 worker threads

    // Submit a build job for a B-tree index
    scheduler.submit(IndexBuildJob {
        sstable_id: "sst-1".into(),
        index_name: "idx_email".into(),
        index_type: IndexType::BTree,
        table: ("ks".into(), "tbl".into()),
        priority: BuildPriority::Normal,
        enqueued_at: Instant::now(),
    }).unwrap();

    // Wait for completion — poll tracker with short sleeps
    let start = Instant::now();
    loop {
        if let Some(state) = tracker.get_state("ks", "tbl", "idx_email") {
            if state.indexed_sstables.contains("sst-1") {
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("index build did not complete within 5 seconds");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let state = tracker.get_state("ks", "tbl", "idx_email").unwrap();
    assert!(matches!(state.status, IndexStatus::Current));
}
```

- [ ] **Step 2: Implement IndexBuildScheduler**

- Constructor takes an `IndexFactoryRegistry` (`HashMap<String, Box<dyn IndexFactory>>`) mapping index type names to factories, plus a `tokio::runtime::Handle` for async S3 upload bridging
- Constructor spawns N worker threads, each receiving from `mpsc::Receiver<IndexBuildJob>`
- Workers: open SSTable reader, look up factory from registry, create builder, iterate rows (skipping tombstones), finish, update tracker. For S3 upload, use `handle.block_on(upload_manager.submit(...))` to bridge the sync worker thread to the async upload path.
- `submit()` sends job through channel
- `shutdown()` sends shutdown signal, joins threads

- [ ] **Step 3: Run tests to verify they pass**

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/index/scheduler.rs
git commit -m "feat(storage): add IndexBuildScheduler with channel-based worker pool"
```

---

### Task 21: Wire scheduler into StorageEngine flush/compaction

**Files:**

- Modify: `ferrosa-storage/src/engine.rs`
- Modify: `ferrosa-storage/src/upload/manager.rs`

- [ ] **Step 1: Add UploadTask::IndexFiles variant**

In `ferrosa-storage/src/upload/manager.rs`:

```rust
pub enum UploadTask {
    SSTable { table_id: String, sstable_id: String, files: Vec<(String, Bytes)> },
    IndexFiles { table_id: String, sstable_id: String, files: Vec<(String, Bytes)> },
    Shutdown,
}
```

Handle `IndexFiles` the same way as `SSTable` in the upload worker.

- [ ] **Step 2: Add IndexBuildScheduler to StorageEngine**

Add `index_scheduler: Option<IndexBuildScheduler>` field to `StorageEngine`. Initialize it in `new()` based on config. After each flush and compaction completion, query the schema for secondary indexes on the affected table and submit build jobs.

- [ ] **Step 3: Verify it compiles and existing tests pass**

Run: `cargo test -p ferrosa-storage`
Expected: all existing tests PASS (scheduler is None when not configured)

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/engine.rs ferrosa-storage/src/upload/manager.rs
git commit -m "feat(storage): wire IndexBuildScheduler into flush and compaction paths"
```

---

## Chunk 9: Operational Metrics Virtual Table and Integration Tests

### Task 22: Add system_views.secondary_indexes virtual table

**Files:**

- Create: `ferrosa-storage/src/index/virtual_table.rs`

- [ ] **Step 1: Implement SecondaryIndexesVirtualTable**

Implement `VirtualTable` trait for `SecondaryIndexesVirtualTable`. The `read()` method queries `IndexStateTracker::all_states()` and produces rows with columns: keyspace_name, table_name, index_name, index_type, status, indexed_sstable_count, pending_sstable_count, pending_bytes, lag_seconds, last_build_ms, total_builds, build_errors, disk_size.

- [ ] **Step 2: Write tests**

Verify column definitions match spec, verify empty tracker returns no rows, verify rows match tracker state.

- [ ] **Step 3: Run tests**

Run: `cargo test -p ferrosa-storage -- secondary_indexes_virtual`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add ferrosa-storage/src/index/virtual_table.rs
git commit -m "feat(storage): add system_views.secondary_indexes virtual table"
```

---

### Task 23: End-to-end integration test

**Files:**

- Create: `ferrosa-storage/tests/index_integration.rs`

- [ ] **Step 1: Write integration test**

Test the full lifecycle:

1. Create a `StorageEngine` with test config
2. Create a schema with a table
3. Register a B-tree index via schema
4. Write some rows to the table
5. Flush the memtable
6. Wait for index build to complete (poll tracker)
7. Verify index files exist on disk
8. Open the index reader and verify lookups return correct results
9. Drop the index
10. Verify index files are cleaned up

- [ ] **Step 2: Run integration test**

Run: `cargo test -p ferrosa-storage --test index_integration`
Expected: PASS

- [ ] **Step 3: Run all workspace tests**

Run: `cargo test`
Expected: all tests PASS

- [ ] **Step 4: Run clippy on entire workspace**

Run: `cargo clippy --all-targets`
Expected: no warnings

- [ ] **Step 5: Commit**

```bash
git add ferrosa-storage/tests/index_integration.rs
git commit -m "test(storage): add end-to-end integration test for secondary index lifecycle"
```

---

## Dependency Order

Tasks must be executed in this order due to crate dependencies:

```
Task 1 (scaffold) → Task 2 (types) → Task 3 (traits)
    → Task 4 (IndexMetadata) → Task 5 (SchemaSnapshot) → Task 6 (virtual table)
    → Task 7 (AST) → Task 8 (parser) → Task 9 (router)
    → Task 10 (DDL coordination)
    → Tasks 11-13 (B-tree, hash, composite) [parallel]
    → Tasks 14-15 (phonetic, filtered) [parallel]
    → Task 16 (distance functions) → Task 17 (HNSW) → Task 18 (IVFFlat)
    → Task 19 (tracker) → Task 20 (scheduler) → Task 21 (engine wiring)
    → Task 22 (metrics virtual table) → Task 23 (integration test)
```

Tasks 11-15 can be parallelized after Task 10.
Tasks 16-18 are sequential (distance functions → HNSW → IVFFlat).
Tasks 19-23 are sequential (tracker → scheduler → wiring → virtual table → integration).
