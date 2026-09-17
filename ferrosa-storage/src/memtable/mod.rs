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
use ferrosa_common::{CellValue, Error, Result, TableSchema};
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
/// True when `row` is the in-memory marker for a partition-level `DELETE`
/// (`DELETE FROM t WHERE pk = ?`): empty clustering, no cells, and a non-LIVE
/// deletion. Such a marker carries a partition tombstone, not a clustered row,
/// and must be lifted into [`Partition::deletion`] rather than stored as a row
/// (otherwise it suppresses nothing on read). A genuine clustered row always
/// has cells or a LIVE primary-key liveness, so this predicate never matches
/// real data.
pub(crate) fn is_partition_tombstone(row: &Row) -> bool {
    row.clustering.is_empty() && row.cells.is_empty() && row.deletion != DeletionTime::LIVE
}

#[derive(Clone, Copy)]
enum RawCollectionKind {
    List,
    Set,
    Map,
}

fn raw_collection_kind(type_name: &str) -> Option<RawCollectionKind> {
    let head = type_name.split('(').next()?.rsplit('.').next()?.trim();
    match head {
        "ListType" => Some(RawCollectionKind::List),
        "SetType" => Some(RawCollectionKind::Set),
        "MapType" => Some(RawCollectionKind::Map),
        _ => None,
    }
}

fn take_collection_value(bytes: &[u8], pos: &mut usize) -> Result<Vec<u8>> {
    let len_bytes = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| Error::InvalidData("truncated collection element length".into()))?;
    *pos += 4;
    let len = i32::from_be_bytes(len_bytes.try_into().expect("four-byte slice"));
    let len = usize::try_from(len)
        .map_err(|_| Error::InvalidData("negative collection element length".into()))?;
    let value = bytes
        .get(*pos..*pos + len)
        .ok_or_else(|| Error::InvalidData("truncated collection element value".into()))?
        .to_vec();
    *pos += len;
    Ok(value)
}

fn expand_legacy_collection_cell(
    kind: RawCollectionKind,
    blob: &CellValue,
) -> Result<Vec<CellValue>> {
    let bytes = blob
        .value
        .as_deref()
        .ok_or_else(|| Error::InvalidData("live collection blob has no value".into()))?;
    let count_bytes = bytes
        .get(..4)
        .ok_or_else(|| Error::InvalidData("truncated collection element count".into()))?;
    let count = i32::from_be_bytes(count_bytes.try_into().expect("four-byte slice"));
    let count = usize::try_from(count)
        .map_err(|_| Error::InvalidData("negative collection element count".into()))?;
    let minimum_entry_bytes = match kind {
        RawCollectionKind::List | RawCollectionKind::Set => 4,
        RawCollectionKind::Map => 8,
    };
    if count > bytes.len().saturating_sub(4) / minimum_entry_bytes {
        return Err(Error::InvalidData(format!(
            "collection element count {count} exceeds the encoded byte length"
        )));
    }
    if matches!(kind, RawCollectionKind::List) && count > u16::MAX as usize + 1 {
        return Err(Error::InvalidData(format!(
            "list element count {count} exceeds the cell-path sequence space"
        )));
    }
    let mut pos = 4;
    let mut cells = Vec::with_capacity(count + 1);

    // A whole-collection assignment is a deletion immediately before its new
    // elements. The one-microsecond offset leaves those replacement elements
    // live while shadowing older path-keyed cells.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    cells.push(CellValue::tombstone(
        blob.timestamp.saturating_sub(1),
        i32::try_from(now_secs).unwrap_or(i32::MAX),
    ));

    for seq in 0..count {
        let (path, value) = match kind {
            RawCollectionKind::List => (
                ferrosa_row_bridge::collection::list_cell_path(blob.timestamp, seq as u16),
                take_collection_value(bytes, &mut pos)?,
            ),
            RawCollectionKind::Set => (take_collection_value(bytes, &mut pos)?, Vec::new()),
            RawCollectionKind::Map => {
                let key = take_collection_value(bytes, &mut pos)?;
                let value = take_collection_value(bytes, &mut pos)?;
                (key, value)
            }
        };
        cells.push(CellValue {
            value: Some(value),
            timestamp: blob.timestamp,
            ttl: blob.ttl,
            local_deletion_time: blob.local_deletion_time,
            path: Some(path),
        });
    }
    if pos != bytes.len() {
        return Err(Error::InvalidData(format!(
            "collection blob has {} trailing bytes",
            bytes.len() - pos
        )));
    }
    Ok(cells)
}

/// Convert legacy whole-value collection cells when either side of a row merge
/// already uses element paths. Keeping one representation prevents a live
/// pathless cell from reaching an SSTable marked for complex collections.
pub(crate) fn normalize_collection_rows_for_merge(
    existing: &mut Row,
    incoming: &mut Row,
    schema: &TableSchema,
) -> Result<()> {
    let complex_columns: std::collections::HashSet<u16> = existing
        .cells
        .iter()
        .chain(&incoming.cells)
        .filter_map(|(idx, cell)| cell.path.is_some().then_some(*idx))
        .collect();
    if complex_columns.is_empty() {
        return Ok(());
    }
    let has_legacy_blob = existing
        .cells
        .iter()
        .chain(&incoming.cells)
        .any(|(idx, cell)| {
            complex_columns.contains(idx) && cell.path.is_none() && !cell.is_tombstone()
        });
    if !has_legacy_blob {
        return Ok(());
    }

    // Parse every blob before changing either row, so a malformed incoming
    // value cannot partially rewrite the existing row before rejection.
    let plan = |row: &Row| -> Result<std::collections::HashMap<u16, Vec<CellValue>>> {
        let mut replacements = std::collections::HashMap::new();
        for (idx, cell) in &row.cells {
            if complex_columns.contains(idx) && cell.path.is_none() && !cell.is_tombstone() {
                let column = schema.regular_columns.get(*idx as usize).ok_or_else(|| {
                    Error::InvalidData(format!(
                        "collection cell column index {idx} is outside the regular schema"
                    ))
                })?;
                let kind = raw_collection_kind(&column.type_name).ok_or_else(|| {
                    Error::InvalidData(format!(
                        "path-bearing cell for non-collection column {}",
                        column.name
                    ))
                })?;
                replacements.insert(*idx, expand_legacy_collection_cell(kind, cell)?);
            }
        }
        Ok(replacements)
    };
    let mut existing_replacements = plan(existing)?;
    let mut incoming_replacements = plan(incoming)?;

    let apply =
        |row: &mut Row, replacements: &mut std::collections::HashMap<u16, Vec<CellValue>>| {
            let cells = std::mem::take(&mut row.cells);
            let replacement_len: usize = replacements.values().map(Vec::len).sum();
            let mut normalized = Vec::with_capacity(cells.len() + replacement_len);
            for (idx, cell) in cells {
                if cell.path.is_none() && !cell.is_tombstone() {
                    if let Some(elements) = replacements.remove(&idx) {
                        normalized.extend(elements.into_iter().map(|element| (idx, element)));
                        continue;
                    }
                }
                normalized.push((idx, cell));
            }
            normalized.sort_by(|(a_idx, a), (b_idx, b)| (a_idx, &a.path).cmp(&(b_idx, &b.path)));
            row.cells = normalized;
        };
    apply(existing, &mut existing_replacements);
    apply(incoming, &mut incoming_replacements);
    Ok(())
}

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
    let is_partition_tombstone = is_partition_tombstone(row);
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

    /// Collect at most `limit` partitions in token order within the optional
    /// range. Implementations should avoid cloning/materializing partitions
    /// after the requested window is full.
    fn snapshot_range_limited(
        &self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> Vec<Partition> {
        self.snapshot()
            .into_iter()
            .filter(|p| start.is_none_or(|s| p.key >= *s) && end.is_none_or(|e| p.key <= *e))
            .take(limit)
            .collect()
    }

    /// Lazy iterator yielding every partition in token order within
    /// the optional `[start, end]` bounds. The iterator must NOT
    /// pre-materialize partitions — `next()` should produce exactly
    /// one clone at a time so memtable scans contribute O(1) memory
    /// to upstream consumers like the streaming range-read handler
    /// (ADR-020).
    ///
    /// The default impl falls back to `snapshot_range_limited` for
    /// backings that haven't been upgraded yet; production backings
    /// (Skiplist, Sharded) override with a truly lazy implementation.
    fn range_iter<'a>(
        &'a self,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
    ) -> Box<dyn Iterator<Item = Partition> + Send + 'a> {
        Box::new(
            self.snapshot_range_limited(start, end, usize::MAX)
                .into_iter(),
        )
    }

    /// Approximate memory usage in bytes. Wait-free (`AtomicUsize`).
    fn size_bytes(&self) -> usize;

    /// Number of partitions stored. Wait-free (`AtomicUsize`).
    fn partition_count(&self) -> usize;
}
pub mod vector_index;
