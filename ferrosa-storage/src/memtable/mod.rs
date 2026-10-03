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

fn take_collection_value<'a>(bytes: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let len_bytes = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| Error::InvalidData("truncated collection element length".into()))?;
    *pos += 4;
    let len = i32::from_be_bytes(len_bytes.try_into().expect("four-byte slice"));
    let len = usize::try_from(len)
        .map_err(|_| Error::InvalidData("negative collection element length".into()))?;
    let value = bytes
        .get(*pos..*pos + len)
        .ok_or_else(|| Error::InvalidData("truncated collection element value".into()))?;
    *pos += len;
    Ok(value)
}

/// Walk a whole-value collection blob (CQL v4+ wire encoding), handing each
/// entry to `emit` as `(seq, first, second)`: a list element's value as
/// `first`, a set element as `first`, a map entry as `(key, value)`. The
/// single parser behind [`expand_legacy_collection_cell`] and
/// [`validate_legacy_collection_blobs`], so the write-time check accepts
/// exactly what a later flush can expand.
fn walk_collection_blob<'a>(
    kind: RawCollectionKind,
    bytes: &'a [u8],
    mut emit: impl FnMut(usize, &'a [u8], &'a [u8]),
) -> Result<usize> {
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
    for seq in 0..count {
        let first = take_collection_value(bytes, &mut pos)?;
        let second = match kind {
            RawCollectionKind::Map => take_collection_value(bytes, &mut pos)?,
            RawCollectionKind::List | RawCollectionKind::Set => &[],
        };
        emit(seq, first, second);
    }
    if pos != bytes.len() {
        return Err(Error::InvalidData(format!(
            "collection blob has {} trailing bytes",
            bytes.len() - pos
        )));
    }
    Ok(count)
}

fn expand_legacy_collection_cell(
    kind: RawCollectionKind,
    blob: &CellValue,
) -> Result<Vec<CellValue>> {
    let bytes = blob
        .value
        .as_deref()
        .ok_or_else(|| Error::InvalidData("live collection blob has no value".into()))?;
    let mut elements = Vec::new();
    walk_collection_blob(kind, bytes, |seq, first, second| {
        let (path, value) = match kind {
            RawCollectionKind::List => (
                ferrosa_row_bridge::collection::list_cell_path(blob.timestamp, seq as u16),
                first.to_vec(),
            ),
            RawCollectionKind::Set => (first.to_vec(), Vec::new()),
            RawCollectionKind::Map => (first.to_vec(), second.to_vec()),
        };
        elements.push(CellValue {
            value: Some(value),
            timestamp: blob.timestamp,
            ttl: blob.ttl,
            local_deletion_time: blob.local_deletion_time,
            path: Some(path),
        });
    })?;

    // A whole-collection assignment is a deletion immediately before its new
    // elements. The one-microsecond offset leaves those replacement elements
    // live while shadowing older path-keyed cells.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut cells = Vec::with_capacity(elements.len() + 1);
    cells.push(CellValue::tombstone(
        blob.timestamp.saturating_sub(1),
        i32::try_from(now_secs).unwrap_or(i32::MAX),
    ));
    cells.extend(elements);
    Ok(cells)
}

/// Refuse a row whose whole-value cell on a non-frozen collection column does
/// not parse as that collection. The row is not changed: storage keeps the
/// whole-value form until a complex-framed flush or compaction expands it
/// ([`expand_legacy_collection_blobs`]), and a value that cannot be expanded
/// there would fail every flush of the table. The error names the table,
/// column and timestamp.
pub(crate) fn validate_legacy_collection_blobs(row: &Row, schema: &TableSchema) -> Result<()> {
    for (idx, cell) in &row.cells {
        if cell.path.is_some() || cell.is_tombstone() {
            continue;
        }
        let (Some(kind), Some(bytes)) = (collection_kind_at(schema, *idx), cell.value.as_deref())
        else {
            continue;
        };
        if let Err(e) = walk_collection_blob(kind, bytes, |_, _, _| {}) {
            let column = schema
                .column_at_ordinal(*idx)
                .map_or("<unknown>", |c| c.name.as_str());
            return Err(Error::InvalidData(format!(
                "{}.{} (column \"{column}\", index {idx}, ts {}): value is not a well-formed \
                 collection: {e}",
                schema.keyspace, schema.table, cell.timestamp
            )));
        }
    }
    Ok(())
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
                let column = schema.column_at_ordinal(*idx).ok_or_else(|| {
                    Error::InvalidData(format!(
                        "collection cell column index {idx} is outside the table schema"
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

/// Collection kind of the column at cell index `idx` (statics first, then
/// regulars, per `TableSchema`'s contract), or `None` for a non-collection,
/// frozen or out-of-range column.
fn collection_kind_at(schema: &TableSchema, idx: u16) -> Option<RawCollectionKind> {
    let idx = idx as usize;
    let static_count = schema.static_columns.len();
    let column = if idx < static_count {
        schema.static_columns.get(idx)
    } else {
        schema.regular_columns.get(idx - static_count)
    }?;
    raw_collection_kind(&column.type_name)
}

/// Expand every legacy whole-value collection cell (live, `path == None`, on a
/// non-frozen list/set/map column) into a deletion sentinel plus per-element
/// cells, in place. On `Err` the row's cells are part-consumed: the caller
/// must discard the row.
///
/// Commit-log segments written before collection writes were normalised can
/// still carry such cells; replay expands them here. Rows that reach a flush
/// or compaction unexpanded are handled by
/// [`expand_collection_blobs_for_writer`].
pub(crate) fn expand_legacy_collection_blobs(row: &mut Row, schema: &TableSchema) -> Result<()> {
    expand_row_collection_blobs(row, |idx| collection_kind_at(schema, idx))
}

/// The expansion behind [`expand_legacy_collection_blobs`] and
/// [`expand_collection_blobs_for_writer`]; `kind_at` maps a cell's column
/// index to its collection kind under the caller's indexing convention.
fn expand_row_collection_blobs(
    row: &mut Row,
    kind_at: impl Fn(u16) -> Option<RawCollectionKind>,
) -> Result<()> {
    let is_blob = |cell: &CellValue| cell.path.is_none() && !cell.is_tombstone();
    if !row
        .cells
        .iter()
        .any(|(idx, cell)| is_blob(cell) && kind_at(*idx).is_some())
    {
        return Ok(());
    }
    // Move the cells rather than cloning them: `cell.clone()` copies row data
    // (P0 OOM audit, rule `clone-on-row-data`). Taking the vec is safe because
    // every caller owns the row it hands in and discards it on `Err`, so a
    // part-consumed row never reaches storage.
    let original = std::mem::take(&mut row.cells);
    let mut cells = Vec::with_capacity(original.len() + 1);
    for (idx, cell) in original {
        match kind_at(idx).filter(|_| is_blob(&cell)) {
            Some(kind) => cells.extend(
                expand_legacy_collection_cell(kind, &cell)?
                    .into_iter()
                    .map(|element| (idx, element)),
            ),
            None => cells.push((idx, cell)),
        }
    }
    cells.sort_by(|(a_idx, a), (b_idx, b)| (a_idx, &a.path).cmp(&(b_idx, &b.path)));
    row.cells = cells;
    Ok(())
}

/// Collection kind of a writer header column (`(name, type)` pairs indexed
/// from 0), or `None` for a non-collection, frozen or out-of-range column.
fn header_collection_kind(columns: &[(Vec<u8>, String)], idx: u16) -> Option<RawCollectionKind> {
    columns
        .get(usize::from(idx))
        .and_then(|(_, type_name)| raw_collection_kind(type_name))
}

/// True when `partition` holds a live path-less cell on a non-frozen
/// collection column of `header`: a whole-value cell that
/// [`expand_collection_blobs_for_writer`] must expand for a complex-framed
/// output.
pub(crate) fn partition_has_collection_blob(
    partition: &Partition,
    header: &ferrosa_sstable::statistics::SerializationHeader,
) -> bool {
    let has_blob = |row: &Row, columns: &[(Vec<u8>, String)]| {
        row.cells.iter().any(|(idx, cell)| {
            cell.path.is_none()
                && !cell.is_tombstone()
                && header_collection_kind(columns, *idx).is_some()
        })
    };
    partition
        .static_row
        .as_ref()
        .is_some_and(|row| has_blob(row, &header.static_columns))
        || partition
            .rows
            .iter()
            .any(|row| has_blob(row, &header.regular_columns))
}

/// Lower `header`'s minimum timestamp and local deletion time so the
/// collection-deletion sentinel that [`expand_collection_blobs_for_writer`]
/// mints (timestamp `blob.timestamp - 1`, local deletion time = now) stays
/// inside the bounds the header advertises. A header built before expansion
/// can sit one microsecond above that sentinel, which the writer's delta
/// encoding and the compaction validator both refuse. Lowering a minimum is
/// always safe; it only widens the delta base.
pub(crate) fn widen_header_for_blob_sentinels(
    header: &mut ferrosa_sstable::statistics::SerializationHeader,
) {
    if header.min_timestamp != ferrosa_common::NO_TIMESTAMP {
        header.min_timestamp = header.min_timestamp.saturating_sub(1);
    }
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let now = i32::try_from(now_secs).unwrap_or(i32::MAX);
    header.min_local_deletion_time = header.min_local_deletion_time.min(now);
}

/// Make `partition` writable under `header`. A complex-framed SSTable
/// (`header.complex_collections`) refuses a live path-less cell on a
/// non-frozen collection column, yet storage legitimately holds whole-value
/// cells beside element cells: a blob written by a non-CQL or mixed-version
/// producer, a legacy simple-framed SSTable compacted with a complex one, or
/// a blob partition flushed beside an element write on another partition.
/// Each such cell is expanded here into the collection-deletion sentinel plus
/// its elements, using the writer's own column indexing (statics and regulars
/// each from 0). A simple-framed header, or a partition with no such cell, is
/// returned borrowed and untouched.
///
/// This is the single fix for both writer refusals of 2026-10-03 (node2's
/// `storage-flush`, node1's compaction). A blob that does not parse fails
/// with an error naming the partition key.
pub(crate) fn expand_collection_blobs_for_writer<'a>(
    partition: &'a Partition,
    header: &ferrosa_sstable::statistics::SerializationHeader,
) -> Result<std::borrow::Cow<'a, Partition>> {
    use std::borrow::Cow;
    if !header.complex_collections {
        return Ok(Cow::Borrowed(partition));
    }
    if !partition_has_collection_blob(partition, header) {
        return Ok(Cow::Borrowed(partition));
    }

    let mut owned = partition.clone();
    let expanded = owned
        .static_row
        .as_mut()
        .map_or(Ok(()), |row| {
            expand_row_collection_blobs(row, |idx| {
                header_collection_kind(&header.static_columns, idx)
            })
        })
        .and_then(|()| {
            owned.rows.iter_mut().try_for_each(|row| {
                expand_row_collection_blobs(row, |idx| {
                    header_collection_kind(&header.regular_columns, idx)
                })
            })
        });
    expanded.map_err(|e| {
        Error::InvalidData(format!(
            "whole-value collection cell in partition key={:?} cannot be expanded for a \
             complex-framed SSTable: {e}",
            String::from_utf8_lossy(partition.key.key.as_bytes())
        ))
    })?;
    Ok(Cow::Owned(owned))
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

    for (col_idx, cell) in &row.cells {
        let bytes = match &cell.value {
            Some(v) => v,
            None => continue,
        };
        let Some(column) = schema.column_at_ordinal(*col_idx) else {
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

    /// The smallest timestamp (microseconds) of any cell, liveness or deletion ever
    /// written to this memtable, or `i64::MAX` when nothing has been written.
    ///
    /// Compaction reads it to decide whether a tombstone may be purged: unflushed
    /// data older than the tombstone would be resurrected. The default is
    /// `i64::MIN` ("unknown, older than everything") so an implementation that does
    /// not track timestamps blocks purging instead of enabling it unsafely.
    fn min_timestamp(&self) -> i64 {
        i64::MIN
    }
}

/// The smallest timestamp a row carries: its cells, primary-key liveness and
/// deletion marker. `i64::MAX` for a row with none of them.
pub(crate) fn row_min_timestamp(row: &Row) -> i64 {
    let cells = row.cells.iter().map(|(_, cell)| cell.timestamp);
    let liveness = row
        .primary_key_liveness
        .has_timestamp()
        .then_some(row.primary_key_liveness.timestamp);
    let deletion = (!row.deletion.is_live()).then_some(row.deletion.marked_for_delete_at);
    cells
        .chain(liveness)
        .chain(deletion)
        .min()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod min_timestamp_tests {
    use super::*;
    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::LivenessInfo;

    fn row(cells: Vec<i64>, liveness: Option<i64>, deletion: Option<i64>) -> Row {
        Row {
            clustering: vec![],
            cells: cells
                .into_iter()
                .map(|ts| (0, CellValue::live(b"v".to_vec(), ts)))
                .collect(),
            deletion: deletion.map_or(DeletionTime::LIVE, |ts| DeletionTime::new(ts, 1)),
            primary_key_liveness: liveness.map_or(LivenessInfo::NONE, LivenessInfo::with_timestamp),
        }
    }

    #[test]
    fn row_min_timestamp_covers_cells_liveness_and_deletion() {
        assert_eq!(row_min_timestamp(&row(vec![], None, None)), i64::MAX);
        assert_eq!(row_min_timestamp(&row(vec![50, 30], None, None)), 30);
        assert_eq!(row_min_timestamp(&row(vec![50], Some(20), None)), 20);
        assert_eq!(row_min_timestamp(&row(vec![50], Some(20), Some(10))), 10);
        assert_eq!(row_min_timestamp(&row(vec![], None, Some(7))), 7);
    }
}
pub mod vector_index;
