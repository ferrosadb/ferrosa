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
pub mod stacked;

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

/// True when `row` is the in-memory marker for a partition's STATIC row: empty
/// clustering, at least one cell, on a table that declares clustering columns.
/// A clustered table's real rows always carry clustering bytes (rejected
/// otherwise by `validate_clustering_shape`), so the marker cannot collide
/// with one; on a table without clustering columns an empty-clustering row is
/// its single regular row and statics do not exist. The marker is lifted into
/// [`Partition::static_row`] instead of being stored as a clustered row, which
/// is how a static row reaches the memtable and the commit log through the
/// ordinary row write path (row streaming, repair, read repair). Its cells
/// must all be static ordinals (`0..static_columns.len()`).
pub(crate) fn is_static_row_marker(row: &Row, schema: &TableSchema) -> bool {
    !schema.clustering_columns.is_empty() && row.clustering.is_empty() && !row.cells.is_empty()
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

/// A non-frozen collection column's declared type: its kind plus the full
/// marshal type string, from which the element types are read so a blob's
/// elements can be checked against them.
#[derive(Clone, Copy)]
struct CollectionType<'a> {
    kind: RawCollectionKind,
    type_name: &'a str,
}

fn collection_type(type_name: &str) -> Option<CollectionType<'_>> {
    raw_collection_kind(type_name).map(|kind| CollectionType { kind, type_name })
}

/// Refuse an element whose bytes cannot be a value of `element_type`: a
/// fixed-width type with the wrong length, or text that is not valid
/// UTF-8/ASCII. This is what separates a genuine whole-value collection from
/// stray bytes that only happen to frame as one (for example a path-dropped
/// element cell). It cannot separate them when the element type is
/// variable-width and unconstrained (blob, varint, decimal), or when the bytes
/// are exactly `00 00 00 00`, the encoding of an EMPTY collection — those
/// still expand, and the expansion is counted and logged.
fn check_collection_element(element_type: &str, bytes: &[u8]) -> std::result::Result<(), String> {
    ferrosa_common::schema::validate_cell_bytes(element_type, bytes)?;
    let simple = element_type.rsplit('.').next().unwrap_or(element_type);
    match simple {
        "UTF8Type" => std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|e| format!("{element_type} element is not UTF-8: {e}")),
        "AsciiType" if !bytes.is_ascii() => Err(format!("{element_type} element is not ASCII")),
        _ => Ok(()),
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
    collection: CollectionType<'_>,
    bytes: &'a [u8],
    mut emit: impl FnMut(usize, &'a [u8], &'a [u8]),
) -> Result<usize> {
    let kind = collection.kind;
    // Element types: list/set element, or map key then value. A type string
    // that does not name them is itself malformed.
    let missing = || {
        Error::InvalidData(format!(
            "collection type {} does not name its element types",
            collection.type_name
        ))
    };
    let first_type = match kind {
        RawCollectionKind::Map => {
            ferrosa_sstable::marshal::collection_key_type(collection.type_name)
        }
        RawCollectionKind::List | RawCollectionKind::Set => {
            ferrosa_sstable::marshal::collection_value_type(collection.type_name)
        }
    }
    .ok_or_else(missing)?;
    let second_type = match kind {
        RawCollectionKind::Map => Some(
            ferrosa_sstable::marshal::collection_value_type(collection.type_name)
                .ok_or_else(missing)?,
        ),
        RawCollectionKind::List | RawCollectionKind::Set => None,
    };
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
        check_collection_element(first_type, first)
            .map_err(|e| Error::InvalidData(format!("collection element {seq}: {e}")))?;
        let second = match second_type {
            Some(value_type) => {
                let value = take_collection_value(bytes, &mut pos)?;
                check_collection_element(value_type, value)
                    .map_err(|e| Error::InvalidData(format!("collection value {seq}: {e}")))?;
                value
            }
            None => &[],
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
    collection: CollectionType<'_>,
    blob: &CellValue,
) -> Result<Vec<CellValue>> {
    let kind = collection.kind;
    let bytes = blob
        .value
        .as_deref()
        .ok_or_else(|| Error::InvalidData("live collection blob has no value".into()))?;
    let mut elements = Vec::new();
    walk_collection_blob(collection, bytes, |seq, first, second| {
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
        let (Some(collection), Some(bytes)) =
            (collection_type_at(schema, *idx), cell.value.as_deref())
        else {
            continue;
        };
        if let Err(e) = walk_collection_blob(collection, bytes, |_, _, _| {}) {
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
                let collection = collection_type(&column.type_name).ok_or_else(|| {
                    Error::InvalidData(format!(
                        "path-bearing cell for non-collection column {}",
                        column.name
                    ))
                })?;
                replacements.insert(*idx, expand_legacy_collection_cell(collection, cell)?);
            }
        }
        Ok(replacements)
    };
    let mut existing_replacements = plan(existing)?;
    let mut incoming_replacements = plan(incoming)?;
    let expanded: Vec<u16> = existing_replacements
        .keys()
        .chain(incoming_replacements.keys())
        .copied()
        .collect();
    record_blob_expansions(
        &schema_table_label(schema),
        schema_column_names(schema, &expanded),
    );

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
fn collection_type_at(schema: &TableSchema, idx: u16) -> Option<CollectionType<'_>> {
    let idx = idx as usize;
    let static_count = schema.static_columns.len();
    let column = if idx < static_count {
        schema.static_columns.get(idx)
    } else {
        schema.regular_columns.get(idx - static_count)
    }?;
    collection_type(&column.type_name)
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
    let expanded = expand_row_collection_blobs(row, |idx| collection_type_at(schema, idx))?;
    record_blob_expansions(
        &schema_table_label(schema),
        schema_column_names(schema, &expanded),
    );
    Ok(())
}

/// The expansion behind [`expand_legacy_collection_blobs`] and
/// [`expand_collection_blobs_for_writer`]; `type_at` maps a cell's column
/// index to its collection type under the caller's indexing convention.
/// Returns the column index of every cell it expanded, so callers can
/// account for each rewrite.
fn expand_row_collection_blobs<'s>(
    row: &mut Row,
    type_at: impl Fn(u16) -> Option<CollectionType<'s>>,
) -> Result<Vec<u16>> {
    let is_blob = |cell: &CellValue| cell.path.is_none() && !cell.is_tombstone();
    if !row
        .cells
        .iter()
        .any(|(idx, cell)| is_blob(cell) && type_at(*idx).is_some())
    {
        return Ok(Vec::new());
    }
    let mut expanded = Vec::new();
    // Move the cells rather than cloning them: `cell.clone()` copies row data
    // (P0 OOM audit, rule `clone-on-row-data`). Taking the vec is safe because
    // every caller owns the row it hands in and discards it on `Err`, so a
    // part-consumed row never reaches storage.
    let original = std::mem::take(&mut row.cells);
    let mut cells = Vec::with_capacity(original.len() + 1);
    for (idx, cell) in original {
        match type_at(idx).filter(|_| is_blob(&cell)) {
            Some(collection) => {
                cells.extend(
                    expand_legacy_collection_cell(collection, &cell)?
                        .into_iter()
                        .map(|element| (idx, element)),
                );
                expanded.push(idx);
            }
            None => cells.push((idx, cell)),
        }
    }
    cells.sort_by(|(a_idx, a), (b_idx, b)| (a_idx, &a.path).cmp(&(b_idx, &b.path)));
    row.cells = cells;
    Ok(expanded)
}

/// Collection type of a writer header column (`(name, type)` pairs indexed
/// from 0), or `None` for a non-collection, frozen or out-of-range column.
fn header_collection_type(columns: &[(Vec<u8>, String)], idx: u16) -> Option<CollectionType<'_>> {
    columns
        .get(usize::from(idx))
        .and_then(|(_, type_name)| collection_type(type_name))
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
                && header_collection_type(columns, *idx).is_some()
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
/// This rewrites stored data, so it is never silent: every expanded cell is
/// counted in `ferrosa_storage_collection_blob_expansions_total{table}`, and
/// the first expansion of each (table, column) in this process logs a WARN
/// naming both. A blob that does not parse, or whose elements are not values
/// of the declared element types, fails with an error naming the table and
/// partition key instead of being expanded.
pub(crate) fn expand_collection_blobs_for_writer<'a>(
    partition: &'a Partition,
    header: &ferrosa_sstable::statistics::SerializationHeader,
    table: &str,
) -> Result<std::borrow::Cow<'a, Partition>> {
    use std::borrow::Cow;
    if !header.complex_collections {
        return Ok(Cow::Borrowed(partition));
    }
    if !partition_has_collection_blob(partition, header) {
        return Ok(Cow::Borrowed(partition));
    }

    let mut owned = partition.clone();
    let result = (|| -> Result<(Vec<u16>, Vec<u16>)> {
        let static_indices = match owned.static_row.as_mut() {
            Some(row) => expand_row_collection_blobs(row, |idx| {
                header_collection_type(&header.static_columns, idx)
            })?,
            None => Vec::new(),
        };
        let mut regular_indices = Vec::new();
        for row in owned.rows.iter_mut() {
            regular_indices.extend(expand_row_collection_blobs(row, |idx| {
                header_collection_type(&header.regular_columns, idx)
            })?);
        }
        Ok((static_indices, regular_indices))
    })();
    let (static_indices, regular_indices) = result.map_err(|e| {
        Error::InvalidData(format!(
            "{table}: whole-value collection cell in partition key={:?} cannot be expanded \
             for a complex-framed SSTable: {e}",
            String::from_utf8_lossy(partition.key.key.as_bytes())
        ))
    })?;
    record_blob_expansions(
        table,
        header_column_names(&header.static_columns, &static_indices).chain(header_column_names(
            &header.regular_columns,
            &regular_indices,
        )),
    );
    Ok(Cow::Owned(owned))
}

/// Count whole-value collection cells rewritten into elements and WARN on
/// the first one per (table, column) in this process. Every expansion site
/// (writer boundary, commit-log replay, memtable merge) reports here, so no
/// rewrite of stored data is silent.
fn record_blob_expansions(table: &str, column_names: impl Iterator<Item = String>) {
    let mut count = 0u64;
    for column in column_names {
        count += 1;
        if crate::metrics::first_collection_blob_expansion(table, &column) {
            tracing::warn!(
                table,
                column = %column,
                "storage: expanding whole-value collection cells into elements (legacy or \
                 non-CQL write); further expansions for this column are counted in \
                 ferrosa_storage_collection_blob_expansions_total only"
            );
        }
    }
    if count > 0 {
        crate::metrics::add_collection_blob_expansions(table, count);
    }
}

/// Column names, from a writer header's column list, for expanded indices.
fn header_column_names<'c>(
    columns: &'c [(Vec<u8>, String)],
    indices: &'c [u16],
) -> impl Iterator<Item = String> + 'c {
    indices.iter().map(move |idx| {
        columns.get(usize::from(*idx)).map_or_else(
            || format!("<index {idx}>"),
            |(name, _)| String::from_utf8_lossy(name).into_owned(),
        )
    })
}

/// Column names, from a table schema (statics first), for expanded indices.
fn schema_column_names<'c>(
    schema: &'c TableSchema,
    indices: &'c [u16],
) -> impl Iterator<Item = String> + 'c {
    indices.iter().map(move |idx| {
        schema
            .column_at_ordinal(*idx)
            .map_or_else(|| format!("<index {idx}>"), |c| c.name.clone())
    })
}

fn schema_table_label(schema: &TableSchema) -> String {
    format!("{}.{}", schema.keyspace, schema.table)
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
    if is_static_row_marker(row, schema) {
        let static_count = schema.static_columns.len();
        if let Some((idx, _)) = row
            .cells
            .iter()
            .find(|(idx, _)| usize::from(*idx) >= static_count)
        {
            return Err(Error::InvalidData(format!(
                "{}.{} (static row): cell index {idx} is not a static column \
                 (table has {static_count} static column(s)); an empty-clustering \
                 row on a clustered table is the static row",
                schema.keyspace, schema.table
            )));
        }
        if row.deletion != DeletionTime::LIVE || row.primary_key_liveness.has_timestamp() {
            return Err(Error::InvalidData(format!(
                "{}.{} (static row): a static row carries cells only, got row deletion \
                 {:?} and liveness {:?}",
                schema.keyspace, schema.table, row.deletion, row.primary_key_liveness
            )));
        }
    } else if !is_partition_tombstone {
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
