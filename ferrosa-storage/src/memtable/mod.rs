//! Memtable: in-memory write buffer for a single table.
//!
//! The `Memtable` trait abstracts over the backing data structure,
//! enabling a future lock-free upgrade (crossbeam-skiplist, Okasaki-style
//! persistent structures) without changing any consumer code.

pub mod eager_index;
pub mod index;
pub mod mem_index;
pub mod sharded;
#[cfg(feature = "skiplist-memtable")]
pub mod skiplist;

use std::sync::Arc;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::{validate_cell_bytes, validate_clustering_shape};
use ferrosa_common::{Error, Result, TableSchema};
use ferrosa_sstable::types::{DeletionTime, Partition, Row};

/// Fail-loud guard: validate every cell in `row` against the column's
/// declared fixed-width type. Returns `Err(Error::InvalidData(_))` on
/// the first mismatch.
///
/// Static columns are indexed first (`0..static_columns.len()`), then
/// regular columns (`static_columns.len()..`). Cells whose `col_idx` is
/// out of range are tolerated (they may be system-internal columns)
/// rather than rejected at this layer.
///
/// Empty / `None` cell values bypass the check (NULL markers and
/// tombstones do not carry length-bound payloads).
///
/// See specs/in-process/bug-memtable-flush-wedge-truncated-timeuuid-
/// from-now-function.md for the bug this guards against.
pub(crate) fn validate_row_against_schema(row: &Row, schema: &TableSchema) -> Result<()> {
    // Clustering shape: production wedge was an 8-byte clustering on a
    // TimeUUID-clustered table. Catching this at the memtable boundary
    // prevents the row from reaching the commit log and the SSTable
    // writer's Gate A.
    //
    // Exception: a pure tombstone Row (empty clustering, no cells,
    // non-LIVE deletion) is the in-memory representation of a
    // partition-level DELETE (`DELETE FROM t WHERE pk = ?`). Such a
    // marker has no clustering by construction and must be allowed
    // through even on a clustered table — it carries no payload that
    // the strict-shape check is protecting.
    let is_partition_tombstone =
        row.clustering.is_empty() && row.cells.is_empty() && row.deletion != DeletionTime::LIVE;
    if !is_partition_tombstone {
        if let Err(reason) = validate_clustering_shape(&schema.clustering_columns, &row.clustering)
        {
            return Err(Error::InvalidData(format!(
                "{}.{} (clustering): {}",
                schema.keyspace, schema.table, reason
            )));
        }
    }

    let static_count = schema.static_columns.len();
    for (col_idx, cell) in &row.cells {
        let bytes = match &cell.value {
            Some(v) => v,
            None => continue,
        };
        let idx = *col_idx as usize;
        let column = if idx < static_count {
            &schema.static_columns[idx]
        } else if idx - static_count < schema.regular_columns.len() {
            &schema.regular_columns[idx - static_count]
        } else {
            continue;
        };
        if let Err(reason) = validate_cell_bytes(&column.type_name, bytes) {
            return Err(Error::InvalidData(format!(
                "{}.{} (column \"{}\", index {}): {}",
                schema.keyspace, schema.table, column.name, col_idx, reason
            )));
        }
    }
    Ok(())
}

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
pub mod vector_index;
