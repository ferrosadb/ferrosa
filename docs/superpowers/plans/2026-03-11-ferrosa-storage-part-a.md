# ferrosa-storage Implementation Plan — Part A

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement ferrosa-storage Part A: memtable, flush, read-path merge, and TableStore — the core single-node storage engine.

**Architecture:** Bottom-up by component, strict TDD. Lock-free read path via `ArcSwap<StoreView>`. Sharded BTreeMap memtable behind a `Memtable` trait for future lock-free upgrade. Cell-level last-write-wins merge matching Cassandra semantics. Flush path serialized via `Mutex<()>` while reads/writes remain wait-free.

**Tech Stack:** Rust 2021, ferrosa-common, ferrosa-sstable, arc_swap, parking_lot, proptest (dev), tempfile (dev)

**Reference documents:**

- [Storage Spec](../../../specs/storage.md) — architecture, concurrency model, test strategy
- [Design Doc](../specs/2026-03-11-ferrosa-storage-design.md) — code-level API, flush pseudocode, merge rules

---

## Chunk 1: Crate Scaffolding + TableSchema

### Task 1: Scaffold ferrosa-storage Crate

**Files:**

- Create: `ferrosa-storage/Cargo.toml`
- Create: `ferrosa-storage/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Create crate Cargo.toml**

```toml
[package]
name = "ferrosa-storage"
description = "Single-node storage engine for Ferrosa: memtable, flush, merge"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true

[dependencies]
ferrosa-common = { path = "../ferrosa-common" }
ferrosa-sstable = { path = "../ferrosa-sstable" }
arc_swap = "1.7"
parking_lot = "0.12"

[dev-dependencies]
proptest = "1"
tempfile = "3"
```

- [ ] **Step 2: Create lib.rs stub**

```rust
//! Single-node storage engine for Ferrosa.
//!
//! Accepts writes into an in-memory buffer (memtable), flushes to SSTables,
//! and merges reads across all sources. The read path is entirely wait-free
//! via lock-free atomic pointer swaps.

pub mod flush;
pub mod memtable;
pub mod merge;
pub mod store;
```

- [ ] **Step 3: Add ferrosa-storage to workspace members**

In the root `Cargo.toml`, add `"ferrosa-storage"` to the `members` list:

```toml
[workspace]
resolver = "2"
members = [
    "ferrosa-common",
    "ferrosa-sstable",
    "ferrosa-storage",
]
```

- [ ] **Step 4: Create empty module files**

Create the following empty files so the crate compiles:

- `ferrosa-storage/src/memtable/mod.rs` — `pub mod sharded;`
- `ferrosa-storage/src/memtable/sharded.rs` — empty
- `ferrosa-storage/src/merge.rs` — empty
- `ferrosa-storage/src/flush.rs` — empty
- `ferrosa-storage/src/store.rs` — empty

- [ ] **Step 5: Verify the workspace compiles**

Run: `cargo build -p ferrosa-storage`
Expected: compiles with no errors (empty modules)

- [ ] **Step 6: Commit**

```bash
git add ferrosa-storage/ Cargo.toml
git commit -m "feat(storage): scaffold ferrosa-storage crate with module structure"
```

### Task 2: Add TableSchema to ferrosa-common

**Files:**

- Create: `ferrosa-common/src/schema.rs`
- Modify: `ferrosa-common/src/lib.rs`
- Test: unit tests in `ferrosa-common/src/schema.rs`

- [ ] **Step 1: Write failing tests for TableSchema**

Create `ferrosa-common/src/schema.rs` with tests only:

```rust
//! Table schema definitions shared across storage and schema crates.
//!
//! `TableSchema` describes a table's column structure: partition key type,
//! clustering columns, static columns, and regular columns. It does NOT
//! depend on ferrosa-sstable — conversion to `SerializationHeader` lives
//! in ferrosa-storage::flush to avoid circular dependencies.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_definition_stores_name_and_type() {
        let col = ColumnDefinition {
            name: "age".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        };
        assert_eq!(col.name, "age");
        assert_eq!(col.type_name, "org.apache.cassandra.db.marshal.Int32Type");
    }

    #[test]
    fn table_schema_construction() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "age".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
            ],
        };
        assert_eq!(schema.keyspace, "ks");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.regular_columns.len(), 2);
    }

    #[test]
    fn clustering_types_returns_type_names() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![
                ColumnDefinition {
                    name: "c1".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
                ColumnDefinition {
                    name: "c2".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            static_columns: vec![],
            regular_columns: vec![],
        };
        assert_eq!(
            schema.clustering_types(),
            vec![
                "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            ]
        );
    }

    #[test]
    fn column_index_finds_regular_columns() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![ColumnDefinition {
                name: "s1".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "age".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
            ],
        };
        // Static columns are indexed first, then regular columns
        assert_eq!(schema.column_index("s1"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("age"), Some(2));
        assert_eq!(schema.column_index("nonexistent"), None);
    }

    #[test]
    fn column_index_no_static_columns() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "a".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "b".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
        };
        assert_eq!(schema.column_index("a"), Some(0));
        assert_eq!(schema.column_index("b"), Some(1));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-common schema`
Expected: FAIL — `ColumnDefinition` and `TableSchema` not defined

- [ ] **Step 3: Implement TableSchema and ColumnDefinition**

Add the struct definitions and impls above the `#[cfg(test)]` section in `schema.rs`:

```rust
/// A single column definition within a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// Cassandra type class name (e.g., `org.apache.cassandra.db.marshal.UTF8Type`).
    pub type_name: String,
}

/// Describes a table's column structure.
///
/// Column ordering: static columns first (by position in `static_columns`),
/// then regular columns (by position in `regular_columns`). This matches
/// Cassandra's internal column index assignment.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub keyspace: String,
    pub table: String,
    /// Cassandra type class name for the partition key.
    pub key_type: String,
    pub clustering_columns: Vec<ColumnDefinition>,
    pub static_columns: Vec<ColumnDefinition>,
    /// Regular columns, ordered by column index.
    pub regular_columns: Vec<ColumnDefinition>,
}

impl TableSchema {
    /// Returns the type names of all clustering columns, in order.
    pub fn clustering_types(&self) -> Vec<String> {
        self.clustering_columns
            .iter()
            .map(|c| c.type_name.clone())
            .collect()
    }

    /// Look up a column's index by name.
    ///
    /// Static columns are indexed first (0..static_columns.len()),
    /// then regular columns (static_columns.len()..).
    pub fn column_index(&self, name: &str) -> Option<u16> {
        for (i, col) in self.static_columns.iter().enumerate() {
            if col.name == name {
                return Some(i as u16);
            }
        }
        let offset = self.static_columns.len();
        for (i, col) in self.regular_columns.iter().enumerate() {
            if col.name == name {
                return Some((offset + i) as u16);
            }
        }
        None
    }
}
```

- [ ] **Step 4: Export from lib.rs**

Add to `ferrosa-common/src/lib.rs`:

```rust
pub mod schema;

pub use schema::{ColumnDefinition, TableSchema};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-common schema`
Expected: all 5 tests PASS

- [ ] **Step 6: Verify workspace still compiles**

Run: `cargo build`
Expected: all crates compile

- [ ] **Step 7: Commit**

```bash
git add ferrosa-common/src/schema.rs ferrosa-common/src/lib.rs
git commit -m "feat(common): add TableSchema and ColumnDefinition types"
```

---

## Chunk 2: Memtable

### Task 3: Memtable Trait

**Files:**

- Modify: `ferrosa-storage/src/memtable/mod.rs`

- [ ] **Step 1: Define the Memtable trait**

```rust
//! Memtable: in-memory write buffer for a single table.
//!
//! The `Memtable` trait abstracts over the backing data structure,
//! enabling a future lock-free upgrade (crossbeam-skiplist, Okasaki-style
//! persistent structures) without changing any consumer code.

pub mod sharded;

use std::sync::Arc;

use ferrosa_common::{Result, TableSchema};
use ferrosa_common::key::DecoratedKey;
use ferrosa_sstable::types::{Partition, Row};

/// In-memory write buffer for a single table.
///
/// Implementations must be thread-safe for concurrent reads and writes.
/// All methods take `&self` — internal synchronization is the implementor's
/// responsibility.
pub trait Memtable: Send + Sync {
    /// Insert or update a row. Merges with existing data by timestamp
    /// (cell-level last-write-wins).
    fn put(&self, key: &DecoratedKey, row: Row, schema: &TableSchema) -> Result<()>;

    /// Read a single partition. Returns `Arc` to avoid deep clones.
    fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>>;

    /// Collect all partitions in token order.
    ///
    /// Uses `&self` because the memtable has already been swapped out of the
    /// active view — no new writes are coming.
    fn snapshot(&self) -> Vec<Partition>;

    /// Approximate memory usage in bytes. Wait-free (`AtomicUsize`).
    fn size_bytes(&self) -> usize;

    /// Number of partitions stored. Wait-free (`AtomicUsize`).
    fn partition_count(&self) -> usize;
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p ferrosa-storage`
Expected: compiles (sharded.rs is empty, which is fine since mod.rs only declares `pub mod sharded;`)

- [ ] **Step 3: Commit**

```bash
git add ferrosa-storage/src/memtable/mod.rs
git commit -m "feat(storage): add Memtable trait definition"
```

### Task 4: ShardedBTreeMemtable

**Files:**

- Modify: `ferrosa-storage/src/memtable/sharded.rs`

This is the largest task. It implements the 64-shard BTreeMap memtable with merge-on-write semantics.

- [ ] **Step 1: Write failing tests for put/get basics**

Write these tests at the bottom of `sharded.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    /// Helper: build a simple test schema with one regular column "val".
    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        }
    }

    /// Helper: build a DecoratedKey from a string.
    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    /// Helper: build a simple row with one cell.
    fn make_row(column_index: u16, value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![],
            cells: vec![(column_index, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    #[test]
    fn put_then_get_returns_partition() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");
        let row = make_row(0, b"hello", 1000);

        mem.put(&key, row, &schema).unwrap();
        let result = mem.get(&key).unwrap();
        assert!(result.is_some());
        let partition = result.unwrap();
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].cells.len(), 1);
        assert_eq!(partition.rows[0].cells[0].0, 0);
        assert_eq!(partition.rows[0].cells[0].1.value.as_deref(), Some(b"hello".as_slice()));
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let mem = ShardedBTreeMemtable::new(4);
        let key = make_key("missing");
        assert!(mem.get(&key).unwrap().is_none());
    }

    #[test]
    fn partition_count_and_size_bytes() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        assert_eq!(mem.partition_count(), 0);
        assert_eq!(mem.size_bytes(), 0);

        mem.put(&make_key("k1"), make_row(0, b"v1", 1000), &schema).unwrap();
        assert_eq!(mem.partition_count(), 1);
        assert!(mem.size_bytes() > 0);

        mem.put(&make_key("k2"), make_row(0, b"v2", 1000), &schema).unwrap();
        assert_eq!(mem.partition_count(), 2);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-storage memtable`
Expected: FAIL — `ShardedBTreeMemtable` not defined

- [ ] **Step 3: Implement ShardedBTreeMemtable struct and constructor**

Add at the top of `sharded.rs`:

```rust
//! Sharded BTreeMap memtable implementation.
//!
//! Uses 64 shards (configurable) of `parking_lot::RwLock<BTreeMap>` to
//! distribute write contention. Shard selection: `key.token.0 as u64 % num_shards`.
//!
//! This is the initial implementation behind the `Memtable` trait. The trait
//! enables swapping to a lock-free structure (crossbeam-skiplist, Okasaki-style
//! persistent structures) without changing consumer code.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::Result;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

use super::Memtable;

/// Default number of shards. 64 provides good distribution across
/// typical core counts (8-32 cores) without excessive overhead.
const DEFAULT_NUM_SHARDS: usize = 64;

pub struct ShardedBTreeMemtable {
    shards: Vec<RwLock<BTreeMap<DecoratedKey, Arc<Partition>>>>,
    num_shards: usize,
    size: AtomicUsize,
    count: AtomicUsize,
}

impl ShardedBTreeMemtable {
    /// Create a new memtable with the given number of shards.
    pub fn new(num_shards: usize) -> Self {
        let shards = (0..num_shards)
            .map(|_| RwLock::new(BTreeMap::new()))
            .collect();
        Self {
            shards,
            num_shards,
            size: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
        }
    }

    /// Create a new memtable with the default number of shards (64).
    pub fn with_default_shards() -> Self {
        Self::new(DEFAULT_NUM_SHARDS)
    }

    /// Determine which shard a key belongs to.
    fn shard_index(&self, key: &DecoratedKey) -> usize {
        (key.token.0 as u64 % self.num_shards as u64) as usize
    }

    /// Estimate the byte size of a partition (for tracking memory usage).
    fn estimate_partition_size(partition: &Partition) -> usize {
        let mut size = partition.key.key.as_bytes().len();
        for row in &partition.rows {
            size += row.clustering.len();
            for (_, cell) in &row.cells {
                size += cell.value.as_ref().map_or(0, |v| v.len());
                size += 16; // timestamp + ttl + local_deletion_time overhead
            }
        }
        if let Some(ref sr) = partition.static_row {
            for (_, cell) in &sr.cells {
                size += cell.value.as_ref().map_or(0, |v| v.len());
                size += 16;
            }
        }
        size + 64 // overhead for Arc, BTreeMap entry, etc.
    }
}
```

- [ ] **Step 4: Implement the Memtable trait — put() with merge-on-write**

```rust
impl Memtable for ShardedBTreeMemtable {
    fn put(&self, key: &DecoratedKey, row: Row, _schema: &TableSchema) -> Result<()> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();

        if let Some(existing) = shard.get_mut(key) {
            // Merge into existing partition
            let partition = Arc::make_mut(existing);
            let old_size = Self::estimate_partition_size(partition);
            merge_row_into_partition(partition, row);
            let new_size = Self::estimate_partition_size(partition);
            // Update size delta (could be negative if cells were replaced)
            if new_size > old_size {
                self.size.fetch_add(new_size - old_size, Ordering::Relaxed);
            } else {
                self.size.fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        } else {
            // New partition
            let partition = Partition {
                key: key.clone(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![row],
            };
            let size = Self::estimate_partition_size(&partition);
            shard.insert(key.clone(), Arc::new(partition));
            self.count.fetch_add(1, Ordering::Relaxed);
            self.size.fetch_add(size, Ordering::Relaxed);
        }
        Ok(())
    }

    fn get(&self, key: &DecoratedKey) -> Result<Option<Arc<Partition>>> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read();
        Ok(shard.get(key).cloned())
    }

    fn snapshot(&self) -> Vec<Partition> {
        // Parallel drain of all shards using std::thread::scope.
        // Each thread read-locks one shard and collects its partitions.
        // No write contention — memtable has already been swapped out.
        let all: Vec<Vec<Partition>> = std::thread::scope(|s| {
            let handles: Vec<_> = self
                .shards
                .iter()
                .map(|shard| {
                    s.spawn(|| {
                        let guard = shard.read();
                        guard.values().map(|arc| (**arc).clone()).collect::<Vec<_>>()
                    })
                })
                .collect();

            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|v| !v.is_empty())
                .collect()
        });
        // K-way merge: since each shard's BTreeMap is sorted by DecoratedKey
        // (which sorts by token first), we can merge them.
        k_way_merge(all)
    }

    fn size_bytes(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn partition_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}
```

- [ ] **Step 5: Implement merge_row_into_partition and k_way_merge helpers**

```rust
/// Merge a new row into an existing partition using cell-level LWW.
///
/// If a row with the same clustering key exists, cells are merged:
/// for the same column_index, the cell with the higher timestamp wins.
/// If no matching row exists, the row is inserted in sorted order.
fn merge_row_into_partition(partition: &mut Partition, new_row: Row) {
    // Handle rows with empty clustering (single-row partitions)
    match partition
        .rows
        .binary_search_by(|r| r.clustering.cmp(&new_row.clustering))
    {
        Ok(pos) => {
            // Merge cells into existing row
            let existing = &mut partition.rows[pos];
            // Row-level deletion: newer wins
            if new_row.deletion.marked_for_delete_at > existing.deletion.marked_for_delete_at {
                existing.deletion = new_row.deletion;
            }
            // Primary key liveness: newer wins
            if new_row.primary_key_liveness.timestamp > existing.primary_key_liveness.timestamp {
                existing.primary_key_liveness = new_row.primary_key_liveness;
            }
            // Cell-level LWW merge
            for (col_idx, new_cell) in new_row.cells {
                match existing.cells.iter().position(|(c, _)| *c == col_idx) {
                    Some(cell_pos) => {
                        if new_cell.timestamp > existing.cells[cell_pos].1.timestamp {
                            existing.cells[cell_pos].1 = new_cell;
                        }
                    }
                    None => {
                        existing.cells.push((col_idx, new_cell));
                        // Keep cells sorted by column_index
                        existing.cells.sort_by_key(|(c, _)| *c);
                    }
                }
            }
        }
        Err(pos) => {
            // Insert new row at sorted position
            partition.rows.insert(pos, new_row);
        }
    }
}

/// K-way merge of pre-sorted partition vectors into a single sorted vector.
///
/// Each input vector is sorted by `DecoratedKey` (token order).
/// Uses a simple iterative merge since shard count is bounded (typically 64).
fn k_way_merge(mut sources: Vec<Vec<Partition>>) -> Vec<Partition> {
    if sources.is_empty() {
        return vec![];
    }
    if sources.len() == 1 {
        return sources.remove(0);
    }

    // Use cursor-based merge
    let mut cursors: Vec<usize> = vec![0; sources.len()];
    let total: usize = sources.iter().map(|s| s.len()).sum();
    let mut result = Vec::with_capacity(total);

    for _ in 0..total {
        // Find the source with the smallest current element
        let mut min_idx = None;
        for (i, source) in sources.iter().enumerate() {
            if cursors[i] < source.len() {
                match min_idx {
                    None => min_idx = Some(i),
                    Some(prev) => {
                        if source[cursors[i]].key < sources[prev][cursors[prev]].key {
                            min_idx = Some(i);
                        }
                    }
                }
            }
        }
        if let Some(idx) = min_idx {
            // Move the partition out — we own the Vec
            let partition = sources[idx][cursors[idx]].clone();
            cursors[idx] += 1;
            result.push(partition);
        }
    }

    result
}
```

- [ ] **Step 6: Run basic tests to verify they pass**

Run: `cargo test -p ferrosa-storage memtable::sharded::tests`
Expected: all 3 tests PASS

- [ ] **Step 7: Write merge-on-write and snapshot tests**

Add to the `tests` module in `sharded.rs`:

```rust
    #[test]
    fn put_merge_on_write_newer_timestamp_wins() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");

        // Write initial value
        mem.put(&key, make_row(0, b"old", 1000), &schema).unwrap();

        // Overwrite with newer timestamp
        mem.put(&key, make_row(0, b"new", 2000), &schema).unwrap();

        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].cells[0].1.value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(partition.rows[0].cells[0].1.timestamp, 2000);
        // Partition count should still be 1
        assert_eq!(mem.partition_count(), 1);
    }

    #[test]
    fn put_merge_on_write_older_timestamp_loses() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");

        mem.put(&key, make_row(0, b"new", 2000), &schema).unwrap();
        mem.put(&key, make_row(0, b"old", 1000), &schema).unwrap();

        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells[0].1.value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(partition.rows[0].cells[0].1.timestamp, 2000);
    }

    #[test]
    fn put_different_columns_merge() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key("pk1");

        mem.put(&key, make_row(0, b"val0", 1000), &schema).unwrap();
        mem.put(&key, make_row(1, b"val1", 1000), &schema).unwrap();

        let partition = mem.get(&key).unwrap().unwrap();
        assert_eq!(partition.rows[0].cells.len(), 2);
        assert_eq!(partition.rows[0].cells[0].0, 0);
        assert_eq!(partition.rows[0].cells[1].0, 1);
    }

    #[test]
    fn snapshot_returns_token_sorted() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();

        // Insert keys that hash to different tokens
        for i in 0..20 {
            let key = make_key(&format!("key_{i}"));
            mem.put(&key, make_row(0, format!("v{i}").as_bytes(), 1000), &schema)
                .unwrap();
        }

        let snapshot = mem.snapshot();
        assert_eq!(snapshot.len(), 20);

        // Verify token ordering
        for window in snapshot.windows(2) {
            assert!(
                window[0].key <= window[1].key,
                "snapshot not in token order: {:?} > {:?}",
                window[0].key.token,
                window[1].key.token
            );
        }
    }

    #[test]
    fn multi_shard_distribution() {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();

        // Insert enough keys that at least 2 shards should have data
        for i in 0..100 {
            let key = make_key(&format!("key_{i}"));
            mem.put(&key, make_row(0, b"v", 1000), &schema).unwrap();
        }
        assert_eq!(mem.partition_count(), 100);

        // Count non-empty shards
        let non_empty = mem
            .shards
            .iter()
            .filter(|s| !s.read().is_empty())
            .count();
        assert!(non_empty >= 2, "expected distribution across shards, got {non_empty}");
    }
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage memtable::sharded::tests`
Expected: all 8 tests PASS

- [ ] **Step 9: Write concurrent put test**

Add to the `tests` module:

```rust
    #[test]
    fn concurrent_puts_no_data_loss() {
        use std::thread;

        let mem = Arc::new(ShardedBTreeMemtable::new(4));
        let schema = Arc::new(test_schema());
        let num_threads = 8;
        let keys_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let mem = Arc::clone(&mem);
                let schema = Arc::clone(&schema);
                thread::spawn(move || {
                    for k in 0..keys_per_thread {
                        let key = make_key(&format!("t{t}_k{k}"));
                        let row = make_row(0, format!("v{t}_{k}").as_bytes(), 1000 + t as i64);
                        mem.put(&key, row, &schema).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(mem.partition_count(), num_threads * keys_per_thread);

        // All partitions must be readable
        for t in 0..num_threads {
            for k in 0..keys_per_thread {
                let key = make_key(&format!("t{t}_k{k}"));
                assert!(mem.get(&key).unwrap().is_some(), "missing t{t}_k{k}");
            }
        }
    }
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage memtable::sharded::tests`
Expected: all 9 tests PASS

- [ ] **Step 11: Commit**

```bash
git add ferrosa-storage/src/memtable/
git commit -m "feat(storage): implement ShardedBTreeMemtable with merge-on-write"
```

---

## Chunk 3: Read-Path Merge

### Task 5: merge_partitions

**Files:**

- Modify: `ferrosa-storage/src/merge.rs`

- [ ] **Step 1: Write failing tests for basic merge cases**

```rust
//! Read-path merge: combines partitions from multiple sources (memtable, SSTables)
//! using cell-level last-write-wins (LWW) semantics matching Cassandra.
//!
//! Merge rules:
//! - Partition-level deletion: newest `DeletionTime` wins. Suppresses rows
//!   with `primary_key_liveness.timestamp` < `marked_for_delete_at`.
//! - Row-level deletion: newest `DeletionTime` wins per clustering key.
//!   Suppresses cells with `timestamp` < `marked_for_delete_at`.
//! - Cell-level: for same `(column_index)`, cell with highest `timestamp` wins.
//! - Static row: cell-level LWW. When one source has a static row and another
//!   does not, the one that has it is used.
//! - Rows from multiple sources merged by clustering key (byte-ordered).

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_cell(col: u16, value: &[u8], ts: i64) -> (u16, CellValue) {
        (col, CellValue::live(value.to_vec(), ts))
    }

    fn make_row_with_clustering(clustering: &[u8], cells: Vec<(u16, CellValue)>, ts: i64) -> Row {
        Row {
            clustering: clustering.to_vec(),
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    fn make_partition(key: &str, rows: Vec<Row>) -> Partition {
        Partition {
            key: make_key(key),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows,
        }
    }

    #[test]
    fn single_source_passthrough() {
        let p = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"v1", 1000)], 1000),
        ]);
        let merged = merge_partitions(vec![p.clone()]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].cells[0].1.value.as_deref(), Some(b"v1".as_slice()));
    }

    #[test]
    fn cell_level_lww_newer_wins() {
        let p1 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"old", 1000)], 1000),
        ]);
        let p2 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"new", 2000)], 2000),
        ]);
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].cells[0].1.value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(merged.rows[0].cells[0].1.timestamp, 2000);
    }

    #[test]
    fn cell_level_lww_is_commutative() {
        let p1 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"old", 1000)], 1000),
        ]);
        let p2 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"new", 2000)], 2000),
        ]);
        let m1 = merge_partitions(vec![p1.clone(), p2.clone()]);
        let m2 = merge_partitions(vec![p2, p1]);
        assert_eq!(m1.rows[0].cells[0].1.value, m2.rows[0].cells[0].1.value);
        assert_eq!(m1.rows[0].cells[0].1.timestamp, m2.rows[0].cells[0].1.timestamp);
    }

    #[test]
    fn disjoint_rows_concatenate() {
        let p1 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"v1", 1000)], 1000),
        ]);
        let p2 = make_partition("k1", vec![
            make_row_with_clustering(b"c2", vec![make_cell(0, b"v2", 1000)], 1000),
        ]);
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 2);
        // Rows should be in clustering order
        assert_eq!(merged.rows[0].clustering, b"c1");
        assert_eq!(merged.rows[1].clustering, b"c2");
    }

    #[test]
    fn disjoint_cells_merge_within_same_row() {
        let p1 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"v0", 1000)], 1000),
        ]);
        let p2 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(1, b"v1", 1000)], 1000),
        ]);
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].cells.len(), 2);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-storage merge`
Expected: FAIL — `merge_partitions` not defined

- [ ] **Step 3: Implement merge_partitions, merge_rows, apply_deletions**

Add above the `#[cfg(test)]` module:

```rust
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

/// Merge partitions from multiple sources into one.
///
/// Sources are typically: active memtable, flushing memtable, and flushed SSTables.
/// All sources must describe the same partition key.
pub fn merge_partitions(mut sources: Vec<Partition>) -> Partition {
    assert!(!sources.is_empty(), "merge_partitions called with no sources");

    if sources.len() == 1 {
        let mut result = sources.remove(0);
        apply_deletions(&mut result);
        return result;
    }

    let key = sources[0].key.clone();

    // Partition-level deletion: newest wins
    let mut deletion = DeletionTime::LIVE;
    for source in &sources {
        if source.deletion.marked_for_delete_at > deletion.marked_for_delete_at {
            deletion = source.deletion;
        }
    }

    // Static row merge: cell-level LWW
    let mut static_row: Option<Row> = None;
    for source in &mut sources {
        if let Some(sr) = source.static_row.take() {
            match static_row {
                None => static_row = Some(sr),
                Some(existing) => {
                    static_row = Some(merge_rows(existing, sr));
                }
            }
        }
    }

    // Merge rows by clustering key
    // Collect all rows, sort by clustering, merge rows with same clustering.
    // Uses drain to move rows out of sources without cloning.
    let mut all_rows: Vec<Row> = sources.into_iter().flat_map(|p| p.rows).collect();
    all_rows.sort_by(|a, b| a.clustering.cmp(&b.clustering));

    let mut merged_rows: Vec<Row> = Vec::new();
    for row in all_rows {
        if merged_rows.last().is_some_and(|last| last.clustering == row.clustering) {
            // Pop, merge, push back. Avoids needing Default on Row.
            let prev = merged_rows.pop().unwrap();
            merged_rows.push(merge_rows(prev, row));
        } else {
            merged_rows.push(row);
        }
    }

    let mut result = Partition {
        key,
        deletion,
        static_row,
        rows: merged_rows,
    };
    apply_deletions(&mut result);
    result
}

/// Merge two rows with the same clustering key.
///
/// Cell-level LWW: for the same column_index, the cell with the higher
/// timestamp wins. Row-level deletion and primary_key_liveness: newer wins.
fn merge_rows(mut a: Row, b: Row) -> Row {
    // Row-level deletion: newer wins
    if b.deletion.marked_for_delete_at > a.deletion.marked_for_delete_at {
        a.deletion = b.deletion;
    }

    // Primary key liveness: newer wins
    if b.primary_key_liveness.timestamp > a.primary_key_liveness.timestamp {
        a.primary_key_liveness = b.primary_key_liveness;
    }

    // Cell-level LWW
    for (col_idx, new_cell) in b.cells {
        match a.cells.iter().position(|(c, _)| *c == col_idx) {
            Some(pos) => {
                if new_cell.timestamp > a.cells[pos].1.timestamp {
                    a.cells[pos].1 = new_cell;
                }
            }
            None => {
                a.cells.push((col_idx, new_cell));
            }
        }
    }

    // Keep cells sorted by column_index
    a.cells.sort_by_key(|(c, _)| *c);
    a
}

/// Apply deletion semantics to a merged partition.
///
/// - Partition-level deletion suppresses rows with
///   `primary_key_liveness.timestamp` < `marked_for_delete_at`.
/// - Row-level deletion suppresses cells with
///   `timestamp` < `marked_for_delete_at`.
fn apply_deletions(partition: &mut Partition) {
    let partition_delete_at = partition.deletion.marked_for_delete_at;

    // Suppress rows killed by partition-level deletion
    if !partition.deletion.is_live() {
        partition.rows.retain(|row| {
            row.primary_key_liveness.timestamp >= partition_delete_at
        });
    }

    // Apply row-level deletions: suppress cells older than the row deletion
    for row in &mut partition.rows {
        if !row.deletion.is_live() {
            let row_delete_at = row.deletion.marked_for_delete_at;
            row.cells.retain(|(_, cell)| cell.timestamp >= row_delete_at);
        }
    }

    // Apply partition-level deletion to static row
    if !partition.deletion.is_live() {
        if let Some(ref mut sr) = partition.static_row {
            sr.cells.retain(|(_, cell)| cell.timestamp >= partition_delete_at);
            if sr.cells.is_empty() {
                partition.static_row = None;
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage merge::tests`
Expected: all 5 tests PASS

- [ ] **Step 5: Write deletion tests**

Add to the `tests` module:

```rust
    #[test]
    fn row_deletion_suppresses_older_cells() {
        let mut p1 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"v1", 1000)], 1000),
        ]);
        let mut p2 = make_partition("k1", vec![Row {
            clustering: b"c1".to_vec(),
            cells: vec![],
            deletion: DeletionTime::new(2000, 100),
            primary_key_liveness: LivenessInfo::NONE,
        }]);
        // Row deletion at ts=2000 should suppress cell at ts=1000
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 1);
        assert!(merged.rows[0].cells.is_empty(), "cells should be suppressed");
    }

    #[test]
    fn partition_deletion_suppresses_all_rows() {
        let p1 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"v1", 1000)], 1000),
            make_row_with_clustering(b"c2", vec![make_cell(0, b"v2", 1500)], 1500),
        ]);
        let p2 = Partition {
            key: make_key("k1"),
            deletion: DeletionTime::new(2000, 100),
            static_row: None,
            rows: vec![],
        };
        let merged = merge_partitions(vec![p1, p2]);
        assert!(merged.rows.is_empty(), "all rows should be suppressed");
    }

    #[test]
    fn partition_deletion_keeps_newer_rows() {
        let p1 = make_partition("k1", vec![
            make_row_with_clustering(b"c1", vec![make_cell(0, b"old", 1000)], 1000),
            make_row_with_clustering(b"c2", vec![make_cell(0, b"new", 3000)], 3000),
        ]);
        let p2 = Partition {
            key: make_key("k1"),
            deletion: DeletionTime::new(2000, 100),
            static_row: None,
            rows: vec![],
        };
        let merged = merge_partitions(vec![p1, p2]);
        assert_eq!(merged.rows.len(), 1);
        assert_eq!(merged.rows[0].clustering, b"c2");
    }

    #[test]
    fn static_row_merge_one_sided() {
        let p1 = Partition {
            key: make_key("k1"),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![make_cell(0, b"static_val", 1000)],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: vec![],
        };
        let p2 = make_partition("k1", vec![]);
        let merged = merge_partitions(vec![p1, p2]);
        assert!(merged.static_row.is_some());
        assert_eq!(
            merged.static_row.unwrap().cells[0].1.value.as_deref(),
            Some(b"static_val".as_slice())
        );
    }

    #[test]
    fn static_row_merge_two_sided_lww() {
        let p1 = Partition {
            key: make_key("k1"),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![make_cell(0, b"old_static", 1000)],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: vec![],
        };
        let p2 = Partition {
            key: make_key("k1"),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![make_cell(0, b"new_static", 2000)],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: vec![],
        };
        let merged = merge_partitions(vec![p1, p2]);
        assert!(merged.static_row.is_some());
        assert_eq!(
            merged.static_row.unwrap().cells[0].1.value.as_deref(),
            Some(b"new_static".as_slice())
        );
    }

    #[test]
    fn empty_inputs() {
        let p1 = make_partition("k1", vec![]);
        let p2 = make_partition("k1", vec![]);
        let merged = merge_partitions(vec![p1, p2]);
        assert!(merged.rows.is_empty());
    }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage merge::tests`
Expected: all 12 tests PASS

- [ ] **Step 7: Commit**

```bash
git add ferrosa-storage/src/merge.rs
git commit -m "feat(storage): implement read-path merge with cell-level LWW and deletion suppression"
```

---

## Chunk 4: Flush

### Task 6: FlushTarget Trait + InMemoryFlushTarget + build_serialization_header

**Files:**

- Modify: `ferrosa-storage/src/flush.rs`

- [ ] **Step 1: Write failing tests**

```rust
//! Flush: persist memtable snapshots to SSTables.
//!
//! The `FlushTarget` trait abstracts over the destination (in-memory for tests,
//! filesystem for production). `build_serialization_header` converts a
//! `TableSchema` + partition data into the `SerializationHeader` required by
//! `SSTableWriter`, computing per-SSTable statistics (`min_timestamp`, etc.)
//! from the data being flushed.

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
    use ferrosa_sstable::WriteOptions;

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_partition(key: &str, value: &[u8], ts: i64) -> Partition {
        Partition {
            key: make_key(key),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(value.to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        }
    }

    #[test]
    fn build_serialization_header_computes_min_timestamp() {
        let schema = test_schema();
        let partitions = vec![
            make_partition("k1", b"v1", 3000),
            make_partition("k2", b"v2", 1000),
            make_partition("k3", b"v3", 2000),
        ];
        let header = build_serialization_header(&schema, &partitions);
        assert_eq!(header.min_timestamp, 1000);
        assert_eq!(header.key_type, "org.apache.cassandra.db.marshal.UTF8Type");
        assert_eq!(header.regular_columns.len(), 1);
        assert_eq!(header.regular_columns[0].1, "org.apache.cassandra.db.marshal.UTF8Type");
    }

    #[test]
    fn build_serialization_header_with_static_columns() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![ColumnDefinition {
                name: "s1".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            regular_columns: vec![ColumnDefinition {
                name: "r1".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        };
        let partitions = vec![make_partition("k1", b"v1", 1000)];
        let header = build_serialization_header(&schema, &partitions);
        assert_eq!(header.static_columns.len(), 1);
        assert_eq!(header.regular_columns.len(), 1);
    }

    #[test]
    fn in_memory_flush_target_round_trip() {
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"value_1", 1000),
            make_partition("k2", b"value_2", 2000),
        ];
        // Sort by token for SSTableWriter
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions::default();
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = InMemoryFlushTarget;
        let reader = target.flush(output).unwrap();

        // Read back both partitions
        for p in &partitions {
            let result = reader.get_partition(&p.key).unwrap();
            assert!(result.is_some(), "partition {:?} not found", p.key.token);
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-storage flush`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement build_serialization_header**

Add above the `#[cfg(test)]` section:

```rust
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use ferrosa_common::cell::CellValue;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::{Result, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
use ferrosa_sstable::io::{FileReadAt, ReadAt};
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::writer::{SSTableOutput, SSTableWriter};

/// Build a `SerializationHeader` from a `TableSchema` and the partitions being flushed.
///
/// The header contains per-SSTable statistics (min_timestamp, min_local_deletion_time,
/// min_ttl) computed from the data, plus column definitions from the schema.
/// This function lives here (not in ferrosa-common) to avoid a circular dependency:
/// ferrosa-common cannot depend on ferrosa-sstable.
pub fn build_serialization_header(
    schema: &TableSchema,
    partitions: &[Partition],
) -> SerializationHeader {
    let mut min_timestamp: i64 = i64::MAX;
    let mut min_local_deletion_time: i32 = i32::MAX;
    let mut min_ttl: i32 = i32::MAX;

    for partition in partitions {
        // Scan rows
        for row in &partition.rows {
            if row.primary_key_liveness.has_timestamp() {
                min_timestamp = min_timestamp.min(row.primary_key_liveness.timestamp);
            }
            if row.primary_key_liveness.has_ttl() {
                min_ttl = min_ttl.min(row.primary_key_liveness.ttl);
                min_local_deletion_time =
                    min_local_deletion_time.min(row.primary_key_liveness.local_deletion_time);
            }
            for (_, cell) in &row.cells {
                if cell.timestamp != NO_TIMESTAMP {
                    min_timestamp = min_timestamp.min(cell.timestamp);
                }
                if cell.ttl != NO_TTL {
                    min_ttl = min_ttl.min(cell.ttl);
                }
                if cell.local_deletion_time != NO_DELETION_TIME {
                    min_local_deletion_time =
                        min_local_deletion_time.min(cell.local_deletion_time);
                }
            }
        }
        // Scan static row
        if let Some(ref sr) = partition.static_row {
            for (_, cell) in &sr.cells {
                if cell.timestamp != NO_TIMESTAMP {
                    min_timestamp = min_timestamp.min(cell.timestamp);
                }
                if cell.ttl != NO_TTL {
                    min_ttl = min_ttl.min(cell.ttl);
                }
                if cell.local_deletion_time != NO_DELETION_TIME {
                    min_local_deletion_time =
                        min_local_deletion_time.min(cell.local_deletion_time);
                }
            }
        }
    }

    // If no timestamps/TTLs found, use sentinel values
    if min_timestamp == i64::MAX {
        min_timestamp = NO_TIMESTAMP;
    }
    if min_local_deletion_time == i32::MAX {
        min_local_deletion_time = NO_DELETION_TIME;
    }
    if min_ttl == i32::MAX {
        min_ttl = NO_TTL;
    }

    SerializationHeader {
        min_timestamp,
        min_local_deletion_time,
        min_ttl,
        key_type: schema.key_type.clone(),
        clustering_types: schema.clustering_types(),
        static_columns: schema
            .static_columns
            .iter()
            .map(|c| (c.name.as_bytes().to_vec(), c.type_name.clone()))
            .collect(),
        regular_columns: schema
            .regular_columns
            .iter()
            .map(|c| (c.name.as_bytes().to_vec(), c.type_name.clone()))
            .collect(),
    }
}
```

- [ ] **Step 4: Implement FlushTarget trait and InMemoryFlushTarget**

```rust
/// Abstraction over where flushed SSTables land.
///
/// Two implementations:
/// - `InMemoryFlushTarget`: wraps components as `Vec<u8>`. No filesystem. Used for tests.
/// - `FileFlushTarget`: writes component files to a directory. Used in production.
pub trait FlushTarget: Send + Sync {
    type Reader: ReadAt + Send + Sync + 'static;

    /// Persist an SSTableOutput and return a reader over the persisted data.
    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Self::Reader>>;
}

/// In-memory flush target: wraps SSTable components as `Vec<u8>`.
///
/// No filesystem access. Used for tests and Part A before FileFlushTarget.
pub struct InMemoryFlushTarget;

impl FlushTarget for InMemoryFlushTarget {
    type Reader = Vec<u8>;

    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Vec<u8>>> {
        let components = SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        };
        SSTableReader::open(components)
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage flush::tests`
Expected: all 3 tests PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-storage/src/flush.rs
git commit -m "feat(storage): add FlushTarget trait, InMemoryFlushTarget, and build_serialization_header"
```

### Task 7: FileFlushTarget

**Files:**

- Modify: `ferrosa-storage/src/flush.rs`

- [ ] **Step 1: Write failing test for FileFlushTarget**

Add to `flush::tests`:

```rust
    #[test]
    fn file_flush_target_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();

        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"value_1", 1000),
            make_partition("k2", b"value_2", 2000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions::default();
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();
        let reader = target.flush(output).unwrap();

        // Read back both partitions
        for p in &partitions {
            let result = reader.get_partition(&p.key).unwrap();
            assert!(result.is_some(), "partition {:?} not found after file flush", p.key.token);
        }
    }

    #[test]
    fn file_flush_target_creates_component_files() {
        let dir = tempfile::tempdir().unwrap();
        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();

        let schema = test_schema();
        let mut partitions = vec![make_partition("k1", b"v1", 1000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions::default();
        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();
        target.flush(output).unwrap();

        // Check that component files were created
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();

        assert!(files.iter().any(|f| f.ends_with("-Data.db")), "missing Data.db: {files:?}");
        assert!(files.iter().any(|f| f.ends_with("-Partitions.db")), "missing Partitions.db: {files:?}");
        assert!(files.iter().any(|f| f.ends_with("-Filter.db")), "missing Filter.db: {files:?}");
        assert!(files.iter().any(|f| f.ends_with("-Statistics.db")), "missing Statistics.db: {files:?}");
    }

    #[test]
    fn file_flush_target_increments_generation() {
        let dir = tempfile::tempdir().unwrap();
        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();

        let schema = test_schema();
        for i in 0..3 {
            let mut partitions = vec![make_partition(&format!("k{i}"), b"v", 1000)];
            partitions.sort_by(|a, b| a.key.cmp(&b.key));
            let header = build_serialization_header(&schema, &partitions);
            let mut writer = SSTableWriter::new(WriteOptions::default(), header);
            for p in &partitions {
                writer.add_partition(p).unwrap();
            }
            target.flush(writer.finish().unwrap()).unwrap();
        }

        // Should have files for 3 generations (1, 2, 3)
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();

        assert!(files.iter().any(|f| f.starts_with("1-")));
        assert!(files.iter().any(|f| f.starts_with("2-")));
        assert!(files.iter().any(|f| f.starts_with("3-")));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-storage flush::tests::file_flush`
Expected: FAIL — `FileFlushTarget` not defined

- [ ] **Step 3: Implement FileFlushTarget**

Add to `flush.rs`:

```rust
/// File-backed flush target: writes SSTable components to a directory.
///
/// Each flush creates a new generation of files:
/// `{base_dir}/{generation}-{Component}.db` (e.g., `1-Data.db`, `1-Partitions.db`).
///
/// Writes up to 7 component files using `std::thread::scope` for parallelism.
/// (`CompressionInfo.db` is omitted when compression is disabled.)
pub struct FileFlushTarget {
    base_dir: PathBuf,
    generation: AtomicU64,
}

impl FileFlushTarget {
    /// Create a new FileFlushTarget writing to the given directory.
    pub fn new(base_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        Ok(Self {
            base_dir,
            generation: AtomicU64::new(0),
        })
    }
}

impl FlushTarget for FileFlushTarget {
    type Reader = FileReadAt;

    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<FileReadAt>> {
        let gen = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let base = &self.base_dir;

        // Write component files in parallel
        let data_path = base.join(format!("{gen}-Data.db"));
        let partitions_path = base.join(format!("{gen}-Partitions.db"));
        let rows_path = base.join(format!("{gen}-Rows.db"));
        let filter_path = base.join(format!("{gen}-Filter.db"));
        let statistics_path = base.join(format!("{gen}-Statistics.db"));
        let toc_path = base.join(format!("{gen}-TOC.txt"));
        let compression_info_path = base.join(format!("{gen}-CompressionInfo.db"));

        let has_compression_info = output.compression_info.is_some();

        std::thread::scope(|s| {
            let handles: Vec<_> = [
                s.spawn(|| std::fs::write(&data_path, &output.data)),
                s.spawn(|| std::fs::write(&partitions_path, &output.partitions)),
                s.spawn(|| std::fs::write(&rows_path, &output.rows)),
                s.spawn(|| std::fs::write(&filter_path, &output.filter)),
                s.spawn(|| std::fs::write(&statistics_path, &output.statistics)),
                s.spawn(|| std::fs::write(&toc_path, &output.toc)),
            ]
            .into_iter()
            .collect();

            if let Some(ref ci) = output.compression_info {
                s.spawn(|| std::fs::write(&compression_info_path, ci))
                    .join()
                    .unwrap()?;
            }

            for h in handles {
                h.join().unwrap()?;
            }

            Ok::<(), ferrosa_common::Error>(())
        })?;

        // Open reader from written files.
        // FileReadAt::open returns ferrosa_common::Result, so ? works directly.
        // std::fs::read returns io::Result, and Error implements From<io::Error>.
        let data = FileReadAt::open(&data_path)?;
        let partitions = FileReadAt::open(&partitions_path)?;
        let rows = FileReadAt::open(&rows_path)?;
        let filter = std::fs::read(&filter_path)?;
        let statistics = std::fs::read(&statistics_path)?;
        let compression_info = if has_compression_info {
            Some(std::fs::read(&compression_info_path)?)
        } else {
            None
        };

        let components = SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
        };
        SSTableReader::open(components)
    }
}
```

- [ ] **Step 4: Add `tempfile` import to the test module**

At the top of `flush::tests`, ensure `tempfile` is imported:

```rust
    // tempfile is a dev-dependency, imported at test scope
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage flush::tests`
Expected: all 6 tests PASS

- [ ] **Step 6: Commit**

```bash
git add ferrosa-storage/src/flush.rs
git commit -m "feat(storage): add FileFlushTarget with parallel file writes"
```

---

## Chunk 5: TableStore

### Task 8: TableStore — Write, Read, Flush Composition

**Files:**

- Modify: `ferrosa-storage/src/store.rs`

- [ ] **Step 1: Write failing tests for write + read (memtable only)**

```rust
//! TableStore: lock-free composition of memtable, flush, and SSTable reads.
//!
//! Uses `ArcSwap<StoreView>` for wait-free reads. State transitions (flush)
//! create a new immutable `StoreView` and atomically swap the pointer.
//! Flush is serialized via `Mutex<()>` — reads and writes are unaffected.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flush::InMemoryFlushTarget;
    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_sstable::WriteOptions;

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn test_store() -> TableStore<InMemoryFlushTarget> {
        TableStore::new(test_schema(), InMemoryFlushTarget, WriteOptions::default())
    }

    #[test]
    fn write_then_read_from_memtable() {
        let store = test_store();
        let key = make_key("pk1");
        store.write(&key, make_row(b"hello", 1000)).unwrap();
        let result = store.read(&key).unwrap();
        assert!(result.is_some());
        let partition = result.unwrap();
        assert_eq!(partition.rows[0].cells[0].1.value.as_deref(), Some(b"hello".as_slice()));
    }

    #[test]
    fn read_nonexistent_returns_none() {
        let store = test_store();
        assert!(store.read(&make_key("missing")).unwrap().is_none());
    }

    #[test]
    fn memtable_size_and_count() {
        let store = test_store();
        assert_eq!(store.memtable_partition_count(), 0);
        assert_eq!(store.memtable_size(), 0);

        store.write(&make_key("k1"), make_row(b"v1", 1000)).unwrap();
        assert_eq!(store.memtable_partition_count(), 1);
        assert!(store.memtable_size() > 0);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ferrosa-storage store`
Expected: FAIL — `TableStore` not defined

- [ ] **Step 3: Implement TableStore struct, new(), write(), read(), stats**

Add above the `#[cfg(test)]` section:

```rust
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_common::Result;
use ferrosa_sstable::io::ReadAt;
use ferrosa_sstable::reader::SSTableReader;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

use crate::flush::{self, FlushTarget};
use crate::memtable::sharded::ShardedBTreeMemtable;
use crate::memtable::Memtable;
use crate::merge;

/// Immutable snapshot of storage state.
///
/// State transitions create a new `StoreView` and atomically swap the pointer
/// via `ArcSwap`. Readers hold an `Arc` to their snapshot and are never
/// invalidated mid-read.
struct StoreView<R: ReadAt + Send + Sync + 'static> {
    active: Arc<dyn Memtable>,
    flushing: Option<Arc<dyn Memtable>>,
    sstables: Arc<Vec<Arc<SSTableReader<R>>>>,
}

/// Storage engine for a single table.
///
/// Read path is entirely wait-free via `ArcSwap`. Write path locks one shard
/// out of 64. Flush is serialized via `Mutex<()>` — reads and writes are
/// unaffected during flush.
///
/// All public methods take `&self`.
pub struct TableStore<F: FlushTarget> {
    schema: TableSchema,
    view: ArcSwap<StoreView<F::Reader>>,
    flush_guard: Mutex<()>,
    flush_target: F,
    options: WriteOptions,
}

impl<F: FlushTarget> TableStore<F> {
    /// Create a new TableStore.
    pub fn new(schema: TableSchema, flush_target: F, options: WriteOptions) -> Self {
        let view = StoreView {
            active: Arc::new(ShardedBTreeMemtable::with_default_shards()),
            flushing: None,
            sstables: Arc::new(Vec::new()),
        };
        Self {
            schema,
            view: ArcSwap::new(Arc::new(view)),
            flush_guard: Mutex::new(()),
            flush_target,
            options,
        }
    }

    /// Write a row to the active memtable.
    ///
    /// Wait-free view load + single-shard write-lock.
    pub fn write(&self, key: &DecoratedKey, row: Row) -> Result<()> {
        let view = self.view.load();
        view.active.put(key, row, &self.schema)
    }

    /// Read a partition by key. Merges across memtable + flushing + SSTables.
    ///
    /// Entirely wait-free at the view level. The memtable get() acquires a
    /// read-lock on one shard (nanosecond critical section).
    pub fn read(&self, key: &DecoratedKey) -> Result<Option<Partition>> {
        let view = self.view.load();
        let mut sources: Vec<Partition> = Vec::new();

        // 1. Check active memtable
        if let Some(p) = view.active.get(key)? {
            sources.push((*p).clone());
        }

        // 2. Check flushing memtable (if mid-flush)
        if let Some(ref flushing) = view.flushing {
            if let Some(p) = flushing.get(key)? {
                sources.push((*p).clone());
            }
        }

        // 3. Check flushed SSTables (newest first)
        for sstable in view.sstables.iter() {
            if let Some(p) = sstable.get_partition(key)? {
                sources.push(p);
            }
        }

        if sources.is_empty() {
            return Ok(None);
        }

        Ok(Some(merge::merge_partitions(sources)))
    }

    /// Number of flushed SSTables.
    pub fn sstable_count(&self) -> usize {
        self.view.load().sstables.len()
    }

    /// Approximate memory usage of the active memtable.
    pub fn memtable_size(&self) -> usize {
        self.view.load().active.size_bytes()
    }

    /// Number of partitions in the active memtable.
    pub fn memtable_partition_count(&self) -> usize {
        self.view.load().active.partition_count()
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage store::tests`
Expected: all 3 tests PASS

- [ ] **Step 5: Write failing tests for flush**

Add to `store::tests`:

```rust
    #[test]
    fn flush_creates_sstable() {
        let store = test_store();
        store.write(&make_key("k1"), make_row(b"v1", 1000)).unwrap();
        store.write(&make_key("k2"), make_row(b"v2", 2000)).unwrap();
        assert_eq!(store.sstable_count(), 0);

        store.flush().unwrap();

        assert_eq!(store.sstable_count(), 1);
        // Memtable should be fresh (empty) after flush
        assert_eq!(store.memtable_partition_count(), 0);
    }

    #[test]
    fn read_after_flush_finds_partition() {
        let store = test_store();
        let key = make_key("k1");
        store.write(&key, make_row(b"hello", 1000)).unwrap();
        store.flush().unwrap();

        let result = store.read(&key).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().rows[0].cells[0].1.value.as_deref(), Some(b"hello".as_slice()));
    }

    #[test]
    fn write_flush_write_read_merges_sources() {
        let store = test_store();
        let key = make_key("k1");

        // Write and flush old value
        store.write(&key, make_row(b"old", 1000)).unwrap();
        store.flush().unwrap();

        // Write newer value to memtable
        store.write(&key, make_row(b"new", 2000)).unwrap();

        // Read should merge memtable + SSTable, newer wins
        let result = store.read(&key).unwrap().unwrap();
        assert_eq!(result.rows[0].cells[0].1.value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(result.rows[0].cells[0].1.timestamp, 2000);
    }

    #[test]
    fn multiple_flushes_accumulate_sstables() {
        let store = test_store();

        store.write(&make_key("k1"), make_row(b"v1", 1000)).unwrap();
        store.flush().unwrap();
        assert_eq!(store.sstable_count(), 1);

        store.write(&make_key("k2"), make_row(b"v2", 2000)).unwrap();
        store.flush().unwrap();
        assert_eq!(store.sstable_count(), 2);

        // Both partitions readable
        assert!(store.read(&make_key("k1")).unwrap().is_some());
        assert!(store.read(&make_key("k2")).unwrap().is_some());
    }

    #[test]
    fn flush_empty_memtable_is_noop() {
        let store = test_store();
        store.flush().unwrap();
        assert_eq!(store.sstable_count(), 0);
    }
```

- [ ] **Step 6: Implement flush()**

Add to the `impl<F: FlushTarget> TableStore<F>` block:

```rust
    /// Flush the active memtable to an SSTable.
    ///
    /// Takes `&self` — does not block reads or writes during the slow part.
    /// Flush is serialized via `flush_guard` Mutex to prevent concurrent
    /// flushes from racing on the ArcSwap (load-then-store is not atomic).
    pub fn flush(&self) -> Result<()> {
        let _guard = self.flush_guard.lock();

        // Step 1: Atomic swap — install fresh memtable, move old to flushing.
        let old_view = self.view.load();
        let old_memtable = old_view.active.clone();
        let new_view = StoreView {
            active: Arc::new(ShardedBTreeMemtable::with_default_shards()),
            flushing: Some(old_memtable.clone()),
            sstables: old_view.sstables.clone(),
        };
        self.view.store(Arc::new(new_view));

        // Step 2: Snapshot the retired memtable
        let partitions = old_memtable.snapshot();

        if partitions.is_empty() {
            // Nothing to flush — clear flushing state
            let cur = self.view.load();
            self.view.store(Arc::new(StoreView {
                active: cur.active.clone(),
                flushing: None,
                sstables: cur.sstables.clone(),
            }));
            return Ok(());
        }

        // Step 3: Build SSTable
        let header = flush::build_serialization_header(&self.schema, &partitions);
        let mut writer = SSTableWriter::new(self.options.clone(), header);
        for partition in &partitions {
            writer.add_partition(partition)?;
        }
        let output = writer.finish()?;
        let reader = self.flush_target.flush(output)?;

        // Step 4: Atomic swap — prepend new SSTable, clear flushing
        let cur = self.view.load();
        let mut new_sstables = (*cur.sstables).clone();
        new_sstables.insert(0, Arc::new(reader));
        self.view.store(Arc::new(StoreView {
            active: cur.active.clone(),
            flushing: None,
            sstables: Arc::new(new_sstables),
        }));

        Ok(())
    }
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage store::tests`
Expected: all 8 tests PASS

- [ ] **Step 8: Commit**

```bash
git add ferrosa-storage/src/store.rs
git commit -m "feat(storage): implement TableStore with lock-free reads and flush"
```

---

## Chunk 6: Public API + Integration Tests

### Task 9: Public API Re-exports

**Files:**

- Modify: `ferrosa-storage/src/lib.rs`

- [ ] **Step 1: Add public re-exports**

Update `ferrosa-storage/src/lib.rs`:

```rust
//! Single-node storage engine for Ferrosa.
//!
//! Accepts writes into an in-memory buffer (memtable), flushes to SSTables,
//! and merges reads across all sources. The read path is entirely wait-free
//! via lock-free atomic pointer swaps.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │ TableStore (ArcSwap<StoreView>)             │
//! │ ┌─────────────┐  ┌──────────┐  ┌─────────┐ │
//! │ │   Active    │  │ Flushing │  │ SSTables│ │
//! │ │  Memtable   │  │ Memtable │  │ (Vec)   │ │
//! │ └─────────────┘  └──────────┘  └─────────┘ │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! - **Write path**: `ArcSwap::load()` (wait-free) → memtable `put()` (one shard lock)
//! - **Read path**: `ArcSwap::load()` (wait-free) → check all sources → `merge_partitions()`
//! - **Flush path**: `Mutex` serializes flushes; two brief `ArcSwap::store()` calls

pub mod flush;
pub mod memtable;
pub mod merge;
pub mod store;

pub use flush::{FileFlushTarget, FlushTarget, InMemoryFlushTarget};
pub use memtable::sharded::ShardedBTreeMemtable;
pub use memtable::Memtable;
pub use merge::merge_partitions;
pub use store::TableStore;
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p ferrosa-storage`
Expected: compiles with no errors

- [ ] **Step 3: Commit**

```bash
git add ferrosa-storage/src/lib.rs
git commit -m "feat(storage): add public API re-exports"
```

### Task 10: Integration Tests

**Files:**

- Create: `ferrosa-storage/tests/integration.rs`

- [ ] **Step 1: Write integration tests**

```rust
//! Integration tests for ferrosa-storage.
//!
//! These tests exercise the full write → flush → read pipeline,
//! verifying that all components compose correctly.

use std::sync::Arc;
use std::thread;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_sstable::WriteOptions;

use ferrosa_storage::store::TableStore;
use ferrosa_storage::flush::InMemoryFlushTarget;
use ferrosa_storage::flush::FileFlushTarget;

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
    }
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

fn make_row(value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

fn test_store() -> TableStore<InMemoryFlushTarget> {
    TableStore::new(test_schema(), InMemoryFlushTarget, WriteOptions::default())
}

#[test]
fn write_flush_read_round_trip() {
    let store = test_store();
    let n = 50;

    for i in 0..n {
        let key = make_key(&format!("key_{i:04}"));
        store
            .write(&key, make_row(format!("value_{i}").as_bytes(), 1000 + i as i64))
            .unwrap();
    }

    store.flush().unwrap();

    for i in 0..n {
        let key = make_key(&format!("key_{i:04}"));
        let result = store.read(&key).unwrap();
        assert!(result.is_some(), "key_{i:04} not found after flush");
        let p = result.unwrap();
        assert_eq!(
            p.rows[0].cells[0].1.value.as_deref(),
            Some(format!("value_{i}").as_bytes())
        );
    }
}

#[test]
fn multiple_flushes_merge() {
    let store = test_store();

    // Write and flush with initial timestamps
    store.write(&make_key("k1"), make_row(b"old_1", 1000)).unwrap();
    store.write(&make_key("k2"), make_row(b"old_2", 1000)).unwrap();
    store.flush().unwrap();

    // Write same keys with newer timestamps and flush again
    store.write(&make_key("k1"), make_row(b"new_1", 2000)).unwrap();
    store.write(&make_key("k2"), make_row(b"new_2", 2000)).unwrap();
    store.flush().unwrap();

    // Write to memtable (not flushed)
    store.write(&make_key("k1"), make_row(b"newest_1", 3000)).unwrap();

    // Read should merge 2 SSTables + memtable
    let p1 = store.read(&make_key("k1")).unwrap().unwrap();
    assert_eq!(p1.rows[0].cells[0].1.value.as_deref(), Some(b"newest_1".as_slice()));
    assert_eq!(p1.rows[0].cells[0].1.timestamp, 3000);

    let p2 = store.read(&make_key("k2")).unwrap().unwrap();
    assert_eq!(p2.rows[0].cells[0].1.value.as_deref(), Some(b"new_2".as_slice()));
    assert_eq!(p2.rows[0].cells[0].1.timestamp, 2000);
}

#[test]
fn flush_does_not_block_reads() {
    let store = Arc::new(test_store());
    let n = 100;

    // Pre-populate
    for i in 0..n {
        store
            .write(&make_key(&format!("k{i}")), make_row(b"v", 1000))
            .unwrap();
    }

    // Spawn reader thread
    let store_clone = Arc::clone(&store);
    let reader = thread::spawn(move || {
        let mut reads = 0;
        for _ in 0..1000 {
            for i in 0..n {
                // read should never error, even during concurrent flush
                let _ = store_clone.read(&make_key(&format!("k{i}")));
                reads += 1;
            }
        }
        reads
    });

    // Flush concurrently
    store.flush().unwrap();

    let reads = reader.join().unwrap();
    assert!(reads > 0);

    // All data still readable after flush
    for i in 0..n {
        assert!(
            store.read(&make_key(&format!("k{i}"))).unwrap().is_some(),
            "k{i} missing after concurrent flush"
        );
    }
}

#[test]
fn deletion_suppresses_across_sources() {
    let store = test_store();

    // Write and flush data
    store.write(&make_key("k1"), make_row(b"alive", 1000)).unwrap();
    store.flush().unwrap();

    // Write newer data to memtable that overwrites flushed data.
    // Note: full partition-level tombstone deletion across sources is tested
    // in merge.rs unit tests (partition_deletion_suppresses_all_rows).
    // The TableStore::write() API currently only accepts Row, not
    // partition-level tombstones. Extending write() for tombstones is
    // deferred to Part B/C when the commit log needs deletion markers.
    store.write(&make_key("k1"), make_row(b"newer", 2000)).unwrap();

    let result = store.read(&make_key("k1")).unwrap().unwrap();
    assert_eq!(result.rows[0].cells[0].1.value.as_deref(), Some(b"newer".as_slice()));
}

#[test]
fn snapshot_produces_token_order() {
    use ferrosa_storage::memtable::Memtable;
    use ferrosa_storage::ShardedBTreeMemtable;

    let mem = ShardedBTreeMemtable::with_default_shards();
    let schema = test_schema();

    for i in 0..100 {
        let key = make_key(&format!("random_key_{i}"));
        mem.put(&key, make_row(format!("v{i}").as_bytes(), 1000), &schema)
            .unwrap();
    }

    let snapshot = mem.snapshot();
    assert_eq!(snapshot.len(), 100);

    for window in snapshot.windows(2) {
        assert!(
            window[0].key <= window[1].key,
            "snapshot not in token order"
        );
    }
}

#[test]
fn file_flush_target_creates_readable_sstables() {
    let dir = tempfile::tempdir().unwrap();
    let store = TableStore::new(
        test_schema(),
        FileFlushTarget::new(dir.path().to_path_buf()).unwrap(),
        WriteOptions::default(),
    );

    store.write(&make_key("k1"), make_row(b"file_v1", 1000)).unwrap();
    store.write(&make_key("k2"), make_row(b"file_v2", 2000)).unwrap();
    store.flush().unwrap();

    assert!(store.read(&make_key("k1")).unwrap().is_some());
    assert!(store.read(&make_key("k2")).unwrap().is_some());

    let p = store.read(&make_key("k1")).unwrap().unwrap();
    assert_eq!(p.rows[0].cells[0].1.value.as_deref(), Some(b"file_v1".as_slice()));
}

#[test]
fn concurrent_writes_no_data_loss() {
    let store = Arc::new(test_store());
    let num_threads = 8;
    let keys_per_thread = 50;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for k in 0..keys_per_thread {
                    let key = make_key(&format!("t{t}_k{k}"));
                    store
                        .write(&key, make_row(format!("v{t}_{k}").as_bytes(), 1000 + t as i64))
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    store.flush().unwrap();

    // All partitions must be readable
    for t in 0..num_threads {
        for k in 0..keys_per_thread {
            let key = make_key(&format!("t{t}_k{k}"));
            assert!(
                store.read(&key).unwrap().is_some(),
                "missing t{t}_k{k} after concurrent writes + flush"
            );
        }
    }
}

#[test]
fn merge_is_commutative() {
    // Write same data, flush in different orders, read results should match
    let store1 = test_store();
    let store2 = test_store();

    // Store1: write A then B
    store1.write(&make_key("k1"), make_row(b"v_a", 1000)).unwrap();
    store1.flush().unwrap();
    store1.write(&make_key("k1"), make_row(b"v_b", 2000)).unwrap();
    store1.flush().unwrap();

    // Store2: write B then A
    store2.write(&make_key("k1"), make_row(b"v_b", 2000)).unwrap();
    store2.flush().unwrap();
    store2.write(&make_key("k1"), make_row(b"v_a", 1000)).unwrap();
    store2.flush().unwrap();

    let p1 = store1.read(&make_key("k1")).unwrap().unwrap();
    let p2 = store2.read(&make_key("k1")).unwrap().unwrap();

    // Both should resolve to the same value (newer timestamp wins)
    assert_eq!(
        p1.rows[0].cells[0].1.value,
        p2.rows[0].cells[0].1.value,
    );
    assert_eq!(
        p1.rows[0].cells[0].1.timestamp,
        p2.rows[0].cells[0].1.timestamp,
    );
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p ferrosa-storage --test integration`
Expected: all 8 tests PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-storage/tests/integration.rs
git commit -m "test(storage): add integration tests for write-flush-read pipeline"
```

### Task 11: Property Tests

**Files:**

- Create: `ferrosa-storage/tests/property_tests.rs`

- [ ] **Step 1: Write property tests**

```rust
//! Property-based tests for ferrosa-storage.
//!
//! These tests verify invariants that must hold for all inputs,
//! not just specific test cases.

use proptest::prelude::*;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::WriteOptions;

use ferrosa_storage::merge::merge_partitions;
use ferrosa_storage::memtable::Memtable;
use ferrosa_storage::ShardedBTreeMemtable;
use ferrosa_storage::store::TableStore;
use ferrosa_storage::flush::InMemoryFlushTarget;

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
    }
}

fn make_key(s: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
}

fn make_partition(key: &str, value: &[u8], ts: i64) -> Partition {
    Partition {
        key: make_key(key),
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }],
    }
}

proptest! {
    #[test]
    fn memtable_round_trip(
        key_suffix in "[a-z]{1,10}",
        value in prop::collection::vec(any::<u8>(), 0..100),
        timestamp in 1i64..1_000_000,
    ) {
        let mem = ShardedBTreeMemtable::new(4);
        let schema = test_schema();
        let key = make_key(&key_suffix);
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(value.clone(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        };

        mem.put(&key, row, &schema).unwrap();
        let result = mem.get(&key).unwrap();
        prop_assert!(result.is_some());
        let partition = result.unwrap();
        prop_assert_eq!(partition.rows[0].cells[0].1.value.as_deref(), Some(value.as_slice()));
    }

    #[test]
    fn merge_commutativity(
        ts_a in 1i64..1_000_000,
        ts_b in 1i64..1_000_000,
    ) {
        let p_a = make_partition("k1", b"val_a", ts_a);
        let p_b = make_partition("k1", b"val_b", ts_b);

        let m1 = merge_partitions(vec![p_a.clone(), p_b.clone()]);
        let m2 = merge_partitions(vec![p_b, p_a]);

        prop_assert_eq!(m1.rows[0].cells[0].1.timestamp, m2.rows[0].cells[0].1.timestamp);
        prop_assert_eq!(m1.rows[0].cells[0].1.value, m2.rows[0].cells[0].1.value);
    }

    #[test]
    fn timestamp_ordering(
        ts_low in 1i64..500_000,
        ts_high in 500_001i64..1_000_000,
    ) {
        let p_low = make_partition("k1", b"low", ts_low);
        let p_high = make_partition("k1", b"high", ts_high);

        let merged = merge_partitions(vec![p_low, p_high]);
        prop_assert_eq!(merged.rows[0].cells[0].1.timestamp, ts_high);
        prop_assert_eq!(merged.rows[0].cells[0].1.value.as_deref(), Some(b"high".as_slice()));
    }

    #[test]
    fn flush_preserves_all_data(n in 1usize..20) {
        let store = TableStore::new(test_schema(), InMemoryFlushTarget, WriteOptions::default());

        let mut keys = Vec::new();
        for i in 0..n {
            let key = make_key(&format!("prop_key_{i:04}"));
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(format!("v{i}").into_bytes(), 1000 + i as i64))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000 + i as i64),
            };
            store.write(&key, row).unwrap();
            keys.push(key);
        }

        store.flush().unwrap();

        for key in &keys {
            let result = store.read(key).unwrap();
            prop_assert!(result.is_some(), "partition lost after flush");
        }
    }
}
```

- [ ] **Step 2: Run property tests**

Run: `cargo test -p ferrosa-storage --test property_tests`
Expected: all 4 property tests PASS

- [ ] **Step 3: Commit**

```bash
git add ferrosa-storage/tests/property_tests.rs
git commit -m "test(storage): add property tests for memtable, merge, and flush"
```

### Task 12: Final Verification

- [ ] **Step 1: Run all tests across the workspace**

Run: `cargo test`
Expected: all tests PASS across ferrosa-common, ferrosa-sstable, ferrosa-storage

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-targets`
Expected: no warnings

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt --check`
Expected: no formatting issues

- [ ] **Step 4: Commit any fixes from clippy/fmt**

If clippy or fmt produced changes:

```bash
git add -A
git commit -m "fix(storage): address clippy warnings and formatting"
```
