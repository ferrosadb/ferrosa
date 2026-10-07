//! `Partition` -> row decomposition and partition/clustering key decoders.
//!
//! Moved verbatim (behaviour-identical) from `ferrosa-cql::bridge` so the CQL
//! and Postgres front-ends share one column-ordering / tombstone-skipping path.
//! Duplicating this logic would risk silently-divergent row ordering — the top
//! FMEA risk for the SQL front-end — so it lives here once.

use std::ops::ControlFlow;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::{CellValue, CqlType, CqlValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

use crate::codec::{decode_value, encode_value};
use crate::{RowBridgeError, RowDecodeError};

/// A decoded row paired with its raw clustering-key bytes.
pub type ClusteredRow = (Vec<u8>, Vec<Option<CqlValue>>);

/// Convert a storage `Partition` back to result rows for CQL RESULT encoding.
///
/// Each returned row is a `Vec<Option<CqlValue>>` with one entry per column
/// in `column_names`. Tombstone rows and cells are represented as `None`.
///
/// # Errors
/// [`RowDecodeError`] when a stored key or cell cannot be decoded. A corrupt
/// value is never returned as `None`.
pub fn partition_to_rows(
    partition: &ferrosa_sstable::types::Partition,
    column_names: &[String],
    column_types: &[CqlType],
    pk_columns: &[usize],
    ck_columns: &[usize],
) -> Result<Vec<Vec<Option<CqlValue>>>, RowDecodeError> {
    let pk_set: std::collections::HashSet<usize> = pk_columns.iter().copied().collect();
    let ck_set: std::collections::HashSet<usize> = ck_columns.iter().copied().collect();
    let storage_to_table: Vec<usize> = (0..column_names.len())
        .filter(|i| !pk_set.contains(i) && !ck_set.contains(i))
        .collect();

    partition_to_rows_with_storage_mapping(
        partition,
        column_names,
        column_types,
        pk_columns,
        ck_columns,
        &storage_to_table,
    )
}

/// True if a `local_deletion_time` (seconds since epoch) has passed.
/// `i32::MAX` is the "no expiry" sentinel and never expires.
pub fn ldt_is_expired(local_deletion_time: i32, now_secs: i32) -> bool {
    // i32::MAX is the "no expiry" sentinel (NO_DELETION_TIME).
    local_deletion_time != i32::MAX && now_secs >= local_deletion_time
}

/// True if a cell still holds a live value at `now_secs` — neither a tombstone
/// nor an expired TTL cell.
pub fn cell_is_live(cell: &CellValue, now_secs: i32) -> bool {
    !(cell.is_tombstone()
        || cell.is_expiring() && ldt_is_expired(cell.local_deletion_time, now_secs))
}

/// Convert a storage `Partition` using an explicit storage-index to table-index
/// map. New schemas use Cassandra column-name order for storage cells, which can
/// differ from original CQL declaration order.
pub fn partition_to_rows_with_storage_mapping(
    partition: &ferrosa_sstable::types::Partition,
    column_names: &[String],
    column_types: &[CqlType],
    pk_columns: &[usize],
    ck_columns: &[usize],
    storage_to_table: &[usize],
) -> Result<Vec<Vec<Option<CqlValue>>>, RowDecodeError> {
    let mut result = Vec::new();
    visit_partition_rows_with_clustering(
        partition,
        column_names,
        column_types,
        pk_columns,
        ck_columns,
        storage_to_table,
        |_clustering, row| {
            result.push(row);
            ControlFlow::Continue(())
        },
    )?;
    Ok(result)
}

/// Like [`partition_to_rows_with_storage_mapping`] but pairs each produced
/// output row with the raw clustering-key bytes of the source row.
///
/// The coordinator-side paging cursor needs the clustering bytes of the last
/// row emitted on a page to resume mid-partition without skipping or
/// duplicating rows. Tombstone/TTL skipping logic lives here once so the
/// paired and unpaired variants stay byte-identical.
pub fn partition_to_rows_with_clustering(
    partition: &ferrosa_sstable::types::Partition,
    column_names: &[String],
    column_types: &[CqlType],
    pk_columns: &[usize],
    ck_columns: &[usize],
    storage_to_table: &[usize],
) -> Result<Vec<ClusteredRow>, RowDecodeError> {
    let mut result = Vec::new();
    visit_partition_rows_with_clustering(
        partition,
        column_names,
        column_types,
        pk_columns,
        ck_columns,
        storage_to_table,
        |clustering, row| {
            result.push((clustering.to_vec(), row));
            ControlFlow::Continue(())
        },
    )?;
    Ok(result)
}

/// Visit surviving rows in a partition one at a time, paired with borrowed
/// clustering-key bytes.
///
/// Callers that page or filter streams can move each decoded row directly to
/// the next stage and stop early at a page boundary. The returned row owns its
/// decoded CQL values, but the clustering bytes are borrowed from the source
/// row so callers only copy them if they must persist an owned cursor.
pub fn visit_partition_rows_with_clustering<F>(
    partition: &ferrosa_sstable::types::Partition,
    column_names: &[String],
    column_types: &[CqlType],
    pk_columns: &[usize],
    ck_columns: &[usize],
    storage_to_table: &[usize],
    mut visit: F,
) -> Result<(), RowDecodeError>
where
    F: FnMut(&[u8], Vec<Option<CqlValue>>) -> ControlFlow<()>,
{
    let now_secs = read_now_secs();
    let pk_values = decode_pk(&partition.key, pk_columns.len());
    let decode_context = RowDecodeContext {
        pk_values: &pk_values,
        column_names,
        column_types,
        pk_columns,
        ck_columns,
        storage_to_table,
        now_secs,
        partition_key: partition.key.key.as_bytes(),
        static_row: partition.static_row.as_ref(),
    };

    for row in &partition.rows {
        if !row_is_visible(row, now_secs) {
            continue;
        }

        let output_row = decode_output_row(row, &decode_context)?;
        if let ControlFlow::Break(()) = visit(row.clustering.as_slice(), output_row) {
            break;
        }
    }
    Ok(())
}

/// Consume surviving rows in a partition one at a time, moving clustering-key
/// bytes into the visitor.
///
/// Streaming read paths use this variant for page cursors: the output row is
/// moved to the page buffer, and the clustering key of the accepted cursor row
/// is moved rather than cloned from the source row. The returned partition key
/// is the original owned key allocation from the consumed partition, so callers
/// that need an owned paging cursor can move it without copying.
pub fn consume_partition_rows_with_clustering<F>(
    partition: Partition,
    column_names: &[String],
    column_types: &[CqlType],
    pk_columns: &[usize],
    ck_columns: &[usize],
    storage_to_table: &[usize],
    mut visit: F,
) -> Result<Vec<u8>, RowDecodeError>
where
    F: FnMut(&[u8], Vec<u8>, Vec<Option<CqlValue>>) -> ControlFlow<()>,
{
    let Partition {
        key,
        rows,
        static_row,
        ..
    } = partition;
    let now_secs = read_now_secs();
    let pk_values = decode_pk(&key, pk_columns.len());
    let decode_context = RowDecodeContext {
        pk_values: &pk_values,
        column_names,
        column_types,
        pk_columns,
        ck_columns,
        storage_to_table,
        now_secs,
        partition_key: key.key.as_bytes(),
        static_row: static_row.as_ref(),
    };

    {
        let pk_bytes = key.key.as_bytes();
        for row in rows {
            if !row_is_visible(&row, now_secs) {
                continue;
            }

            let output_row = decode_output_row(&row, &decode_context)?;
            if let ControlFlow::Break(()) = visit(pk_bytes, row.clustering, output_row) {
                break;
            }
        }
    }

    Ok(key.key.into_bytes())
}

fn read_now_secs() -> i32 {
    i32::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    )
    .unwrap_or(i32::MAX)
}

fn row_is_visible(row: &Row, now_secs: i32) -> bool {
    // TTL expiry: a row whose primary-key liveness was written with a TTL and
    // has expired is gone — unless some cell is still live (a later non-TTL
    // UPDATE can resurrect it). Mirrors Cassandra semantics.
    let pkl = &row.primary_key_liveness;
    let liveness_expired = pkl.has_ttl() && ldt_is_expired(pkl.local_deletion_time, now_secs);
    if liveness_expired {
        let any_live_cell = row.cells.iter().any(|(_, c)| cell_is_live(c, now_secs));
        if !any_live_cell {
            return false;
        }
    }

    // Skip tombstone rows — but only if no newer mutation supersedes the
    // tombstone. In Cassandra semantics, an UPDATE or INSERT after a DELETE
    // resurrects the row: the primary_key_liveness timestamp or cell timestamps
    // may be newer than the row-level deletion.
    if !row.deletion.is_live() {
        let del_ts = row.deletion.marked_for_delete_at;
        let liveness_supersedes = row.primary_key_liveness.timestamp > del_ts;
        let any_cell_supersedes = row.cells.iter().any(|(_, cell)| cell.timestamp > del_ts);
        if !liveness_supersedes && !any_cell_supersedes {
            return false;
        }
    }

    true
}

struct RowDecodeContext<'a> {
    pk_values: &'a [Vec<u8>],
    column_names: &'a [String],
    column_types: &'a [CqlType],
    pk_columns: &'a [usize],
    ck_columns: &'a [usize],
    storage_to_table: &'a [usize],
    now_secs: i32,
    partition_key: &'a [u8],
    /// The partition's static row. Its cells (static ordinals) are part of
    /// every clustered row a reader sees (Cassandra semantics); a flush moves
    /// static cells here from the clustered rows they were written with.
    static_row: Option<&'a Row>,
}

impl RowDecodeContext<'_> {
    fn column_name(&self, idx: usize) -> &str {
        self.column_names
            .get(idx)
            .map(String::as_str)
            .unwrap_or("?")
    }

    fn corrupt(&self, idx: usize, reason: impl std::fmt::Display) -> RowDecodeError {
        RowDecodeError::new(
            self.column_name(idx),
            self.partition_key,
            reason.to_string(),
        )
    }
}

/// Decode a primary-key or clustering value into `output_row[col_idx]`.
/// A stored key component that does not decode is corruption, not NULL.
fn decode_key_component(
    context: &RowDecodeContext<'_>,
    col_idx: usize,
    bytes: Option<&Vec<u8>>,
    output_row: &mut [Option<CqlValue>],
) -> Result<(), RowDecodeError> {
    let (Some(ty), Some(bytes)) = (context.column_types.get(col_idx), bytes) else {
        return Ok(());
    };
    let val = decode_value(ty, bytes).map_err(|e| {
        context
            .corrupt(col_idx, &e)
            .with_jsonb_fault(e.jsonb_fault().cloned())
    })?;
    output_row[col_idx] = Some(val);
    Ok(())
}

fn decode_output_row(
    row: &Row,
    context: &RowDecodeContext<'_>,
) -> Result<Vec<Option<CqlValue>>, RowDecodeError> {
    let mut output_row: Vec<Option<CqlValue>> = vec![None; context.column_names.len()];

    // Fill PK columns.
    for (i, &col_idx) in context.pk_columns.iter().enumerate() {
        decode_key_component(context, col_idx, context.pk_values.get(i), &mut output_row)?;
    }

    // Fill CK columns.
    let ck_values = decode_clustering(&row.clustering, context.ck_columns.len());
    for (i, &col_idx) in context.ck_columns.iter().enumerate() {
        decode_key_component(context, col_idx, ck_values.get(i), &mut output_row)?;
    }

    // Fill regular/static columns from cells. Cell indices are in storage
    // column space (0-based within static+regular columns); translate to
    // full-table column index via the mapping built above.
    //
    // Group cells by table column: a simple (scalar) column has one cell
    // (path = None); a complex (collection) column has many cells sharing one
    // storage index, distinguished by a per-element path (set element / map key
    // / list TimeUUID). Complex columns are reconciled per path (CRDT LWW so a
    // removed element cannot resurrect) and assembled back into the whole
    // collection value — not decoded cell-by-cell as if each were the column.
    let mut cells_by_col: std::collections::BTreeMap<usize, Vec<&CellValue>> =
        std::collections::BTreeMap::new();
    let overlaid;
    let cells: &mut dyn Iterator<Item = (&u16, &CellValue)> = match context.static_row {
        Some(static_row) if !static_row.cells.is_empty() => {
            overlaid = overlay_static_cells(static_row, row);
            &mut overlaid.iter().map(|((idx, _), cell)| (idx, *cell))
        }
        _ => &mut row.cells.iter().map(|(idx, cell)| (idx, cell)),
    };
    for (col_index, cell) in cells {
        let storage_idx = *col_index as usize;
        let table_idx = match context.storage_to_table.get(storage_idx) {
            Some(&idx) => idx,
            None => continue,
        };
        if table_idx < context.column_types.len() {
            cells_by_col.entry(table_idx).or_default().push(cell);
        }
    }

    for (table_idx, cells) in cells_by_col {
        // A cell that cannot be assembled fails the read (RB-Tcf7ca2cc); it is
        // never presented to the client as NULL.
        output_row[table_idx] = crate::collection::assemble_column_cells(
            &context.column_types[table_idx],
            &cells,
            context.now_secs,
        )
        .map_err(|e| {
            tracing::error!(
                column = context.column_name(table_idx),
                error = %e,
                "corrupt cell: failing the read",
            );
            context
                .corrupt(table_idx, &e)
                .with_jsonb_fault(e.jsonb.clone())
        })?;
    }

    Ok(output_row)
}

/// `row`'s cells overlaid on the partition's static cells, keyed by
/// `(ordinal, path)`: where both hold the same cell, the storage engine's
/// last-write-wins picks it. Borrows every cell; nothing is copied.
pub fn overlay_static_cells<'a>(
    static_row: &'a Row,
    row: &'a Row,
) -> std::collections::BTreeMap<(u16, Option<&'a [u8]>), &'a CellValue> {
    let mut cells: std::collections::BTreeMap<(u16, Option<&'a [u8]>), &'a CellValue> =
        std::collections::BTreeMap::new();
    for (idx, cell) in static_row.cells.iter().chain(row.cells.iter()) {
        let key = (*idx, cell.path.as_deref());
        let winner = match cells.get(&key) {
            Some(existing) => ferrosa_common::reconcile_ref(existing, cell),
            None => cell,
        };
        cells.insert(key, winner);
    }
    cells
}

/// Decompose a storage `Partition` into raw per-column byte slices, invoking
/// `emit` once per surviving row. Same tombstone/TTL skipping as
/// [`partition_to_rows_with_clustering`], but yields borrowed bytes rather than
/// decoded `CqlValue`s (zero-copy re-ingest / salvage path).
pub fn write_partition_raw_rows_with_storage_mapping<F>(
    partition: &ferrosa_sstable::types::Partition,
    column_count: usize,
    pk_columns: &[usize],
    ck_columns: &[usize],
    storage_to_table: &[usize],
    mut emit: F,
) where
    F: FnMut(&[Option<&[u8]>]),
{
    let now_secs = i32::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    )
    .unwrap_or(i32::MAX);

    let pk_values = decode_pk(&partition.key, pk_columns.len());

    for row in &partition.rows {
        let pkl = &row.primary_key_liveness;
        let liveness_expired = pkl.has_ttl() && ldt_is_expired(pkl.local_deletion_time, now_secs);
        if liveness_expired {
            let any_live_cell = row.cells.iter().any(|(_, c)| cell_is_live(c, now_secs));
            if !any_live_cell {
                continue;
            }
        }

        if !row.deletion.is_live() {
            let del_ts = row.deletion.marked_for_delete_at;
            let liveness_supersedes = row.primary_key_liveness.timestamp > del_ts;
            let any_cell_supersedes = row.cells.iter().any(|(_, cell)| cell.timestamp > del_ts);
            if !liveness_supersedes && !any_cell_supersedes {
                continue;
            }
        }

        let ck_values = decode_clustering(&row.clustering, ck_columns.len());
        let mut output_row: Vec<Option<&[u8]>> = vec![None; column_count];

        for (i, &col_idx) in pk_columns.iter().enumerate() {
            if col_idx < column_count {
                output_row[col_idx] = pk_values.get(i).map(Vec::as_slice);
            }
        }

        for (i, &col_idx) in ck_columns.iter().enumerate() {
            if col_idx < column_count {
                output_row[col_idx] = ck_values.get(i).map(Vec::as_slice);
            }
        }

        for (col_index, cell) in &row.cells {
            let storage_idx = *col_index as usize;
            let table_idx = match storage_to_table.get(storage_idx) {
                Some(&idx) => idx,
                None => continue,
            };
            if table_idx >= column_count {
                continue;
            }
            if !cell_is_live(cell, now_secs) {
                output_row[table_idx] = None;
            } else {
                output_row[table_idx] = cell.value.as_deref();
            }
        }

        emit(&output_row);
    }
}

/// True when a projected column's stored bytes may be emitted to the client
/// **verbatim** — i.e. the column is a plain scalar (or key) whose single stored
/// cell already is its CQL wire encoding.
///
/// A collection, tuple, UDT or vector column is **not** raw-emittable: its value
/// is assembled from many per-element cells (CRDT last-write-wins per path), so
/// emitting one stored cell as if it were the column would be wrong. Such a
/// column is a projection barrier — the caller must fall back to the decoding
/// path for the whole query rather than mix raw and assembled columns.
pub fn projected_column_raw_encodable(ty: &CqlType) -> bool {
    !matches!(
        ty,
        CqlType::List(_)
            | CqlType::Map(_, _)
            | CqlType::Set(_)
            | CqlType::Tuple(_)
            | CqlType::Udt { .. }
            | CqlType::Vector(_, _)
    )
}

/// Map each projected result column to its index in `column_names` (the full
/// table column order), or `None` for a function call (`writetime(c)`, `ttl(c)`,
/// `count(*)`, …) that no stored column supplies.
///
/// Mirrors the index mapping `select_columns` builds, so a caller may use it to
/// emit the projected columns directly instead of materializing every column and
/// copying the projection back out.
pub fn column_projection(column_names: &[String], selected: &[String]) -> Vec<Option<usize>> {
    selected
        .iter()
        .map(|name| column_names.iter().position(|n| n == name))
        .collect()
}

/// Decompose a storage `Partition` into the requested columns' raw byte slices,
/// invoking `emit` once per surviving row.
///
/// Same tombstone/TTL (and static-row) skipping as
/// [`write_partition_raw_rows_with_storage_mapping`], but each emitted row holds
/// only the columns listed in `projected` (indices into the full table column
/// order), in that order. Every index in `projected` must be a raw-encodable
/// column (see [`projected_column_raw_encodable`]); the caller is responsible for
/// rejecting a projection that contains a function call or an assembled
/// (collection/UDT) column before calling.
///
/// This is the projection-aware form of the raw path: a query that selects a
/// strict subset of a wide table emits only those columns' stored wire bytes,
/// rather than decoding every column into `CqlValue` and copying the projection
/// back out.
pub fn write_partition_raw_rows_projected<F>(
    partition: &ferrosa_sstable::types::Partition,
    column_count: usize,
    pk_columns: &[usize],
    ck_columns: &[usize],
    storage_to_table: &[usize],
    projected: &[usize],
    mut emit: F,
) where
    F: FnMut(&[Option<&[u8]>]),
{
    let now_secs = read_now_secs();
    let pk_values = decode_pk(&partition.key, pk_columns.len());
    // Each emitted row is a projection of the row's stored cells. `full` is a
    // scratch view of the current row's columns (borrowed slices only — the cell
    // bytes are never copied); `out` is the projected view handed to `emit`. Both
    // are rebuilt per row so they can borrow that row's clustering-key bytes.
    for row in &partition.rows {
        if !row_is_visible(row, now_secs) {
            continue;
        }

        let ck_values = decode_clustering(&row.clustering, ck_columns.len());
        let mut full: Vec<Option<&[u8]>> = vec![None; column_count];

        for (i, &col_idx) in pk_columns.iter().enumerate() {
            if col_idx < column_count {
                full[col_idx] = pk_values.get(i).map(Vec::as_slice);
            }
        }

        for (i, &col_idx) in ck_columns.iter().enumerate() {
            if col_idx < column_count {
                full[col_idx] = ck_values.get(i).map(Vec::as_slice);
            }
        }

        for (col_index, cell) in &row.cells {
            let storage_idx = *col_index as usize;
            let table_idx = match storage_to_table.get(storage_idx) {
                Some(&idx) => idx,
                None => continue,
            };
            if table_idx >= column_count {
                continue;
            }
            full[table_idx] = if cell_is_live(cell, now_secs) {
                cell.value.as_deref()
            } else {
                None
            };
        }

        let out: Vec<Option<&[u8]>> = projected.iter().map(|&table_idx| full[table_idx]).collect();
        emit(&out);
    }
}

/// Decode partition-key bytes into component byte slices.
///
/// Single PK: the whole key is the single component.
/// Composite: `[2-byte len][value bytes][0x00]` per component.
pub fn decode_pk(dk: &DecoratedKey, num_components: usize) -> Vec<Vec<u8>> {
    let bytes = dk.key.as_bytes();
    if num_components <= 1 {
        return vec![bytes.to_vec()];
    }
    // Composite: [2-byte len][value bytes][0x00] per component
    let mut components = Vec::with_capacity(num_components);
    let mut pos = 0;
    while pos + 2 <= bytes.len() && components.len() < num_components {
        let len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        let end = pos + len;
        if end > bytes.len() {
            break;
        }
        components.push(bytes[pos..end].to_vec());
        pos = end;
        // Skip the 0x00 separator
        if pos < bytes.len() && bytes[pos] == 0x00 {
            pos += 1;
        }
    }
    components
}

/// Decode clustering key bytes into component byte slices.
///
/// Single CK: the whole byte slice is the single component.
/// Multiple: `[2-byte len][value bytes]` per component.
///
/// Public so offline tooling (e.g. ferrosa-ctl salvage re-ingest) can split a
/// stored clustering key back into the per-column values for a prepared INSERT.
pub fn decode_clustering(bytes: &[u8], num_components: usize) -> Vec<Vec<u8>> {
    if bytes.is_empty() || num_components == 0 {
        return vec![];
    }
    if num_components == 1 {
        return vec![bytes.to_vec()];
    }
    let mut components = Vec::with_capacity(num_components);
    let mut pos = 0;
    while pos + 2 <= bytes.len() && components.len() < num_components {
        let len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        let end = pos + len;
        if end > bytes.len() {
            break;
        }
        components.push(bytes[pos..end].to_vec());
        pos = end;
    }
    components
}

// ---------------------------------------------------------------------------
// Write-direction row assembly — the single canonical encoder shared by the
// CQL front-end (ferrosa-cql re-exports these) and the Postgres front-end.
// ---------------------------------------------------------------------------

/// Build a partition's [`DecoratedKey`] from its partition-key column values: a
/// single component encoded bare, a composite as `[2-byte len][bytes][0x00]`
/// per component (the engine's key format). `pk_types` is accepted for
/// signature stability; encoding is value-driven.
pub fn build_decorated_key(
    pk_values: &[CqlValue],
    _pk_types: &[CqlType],
) -> Result<DecoratedKey, RowBridgeError> {
    if pk_values.is_empty() {
        return Err(RowBridgeError::invalid(
            "partition key must have at least one column".to_string(),
        ));
    }
    let bytes = if pk_values.len() == 1 {
        encode_value(&pk_values[0])
    } else {
        let mut buf = Vec::new();
        for val in pk_values {
            let encoded = encode_value(val);
            let len = u16::try_from(encoded.len())
                .map_err(|_| RowBridgeError::invalid("partition key component too large"))?;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(&encoded);
            buf.push(0x00);
        }
        buf
    };
    Ok(DecoratedKey::new(PartitionKey::new(bytes)))
}

/// Encode clustering-column values into the engine's clustering-key bytes: a
/// single value bare, multiple length-prefixed and concatenated.
pub fn encode_clustering(values: &[CqlValue]) -> Vec<u8> {
    if values.is_empty() {
        return vec![];
    }
    if values.len() == 1 {
        return encode_value(&values[0]);
    }
    let mut buf = Vec::new();
    for val in values {
        let encoded = encode_value(val);
        let len = (encoded.len() as u16).to_be_bytes();
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&encoded);
    }
    buf
}

/// Build a storage [`Row`] from non-key column values + clustering values.
///
/// - `column_values`: `(storage_column_index, value)` for non-key columns.
/// - `clustering_values`: clustering-column values.
/// - `timestamp`: write timestamp (microseconds); `ttl`: optional seconds.
///
/// An explicit `Null` emits a cell tombstone (Cassandra delete semantics), not a
/// live empty cell. Cells are sorted by column index — the SSTable reader reads
/// them in index order, so out-of-order cells corrupt reads.
pub fn build_row(
    column_values: &[(u16, CqlValue)],
    clustering_values: &[CqlValue],
    timestamp: i64,
    ttl: Option<i32>,
) -> Row {
    let clustering = encode_clustering(clustering_values);
    let mut cells: Vec<(u16, CellValue)> = column_values
        .iter()
        .map(|(idx, val)| {
            if matches!(val, CqlValue::Null) {
                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let local_deletion_time = i32::try_from(now_secs).unwrap_or(i32::MAX);
                return (*idx, CellValue::tombstone(timestamp, local_deletion_time));
            }
            let encoded = encode_value(val);
            let cell = match ttl {
                Some(ttl_secs) => {
                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let local_deletion_time =
                        i32::try_from(now_secs.saturating_add(ttl_secs as u64)).unwrap_or(i32::MAX);
                    CellValue::expiring(encoded, timestamp, ttl_secs, local_deletion_time)
                }
                None => CellValue::live(encoded, timestamp),
            };
            (*idx, cell)
        })
        .collect();
    cells.sort_by_key(|(idx, _)| *idx);

    let primary_key_liveness = match ttl {
        Some(ttl_secs) => {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let local_deletion_time =
                i32::try_from(now_secs.saturating_add(ttl_secs as u64)).unwrap_or(i32::MAX);
            LivenessInfo::with_ttl(timestamp, ttl_secs, local_deletion_time)
        }
        None => LivenessInfo::with_timestamp(timestamp),
    };

    Row {
        clustering,
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness,
    }
}

/// Build a storage [`Row`] representing a deletion. Empty `delete_columns` is a
/// row-level deletion (a partition/row tombstone); a non-empty list tombstones
/// each named column. `clustering_values` locate the row; `timestamp` is micros.
pub fn build_delete_row(
    delete_columns: &[u16],
    clustering_values: &[CqlValue],
    timestamp: i64,
) -> Row {
    let clustering = encode_clustering(clustering_values);

    // System clock: the one allowed unwrap.
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;

    if delete_columns.is_empty() {
        Row {
            clustering,
            cells: vec![],
            deletion: DeletionTime::new(timestamp, now_secs),
            primary_key_liveness: LivenessInfo::NONE,
        }
    } else {
        // Column-level deletion: tombstone each specified column. Cells MUST be
        // sorted by column index — same requirement as build_row.
        let mut cells: Vec<(u16, CellValue)> = delete_columns
            .iter()
            .map(|&idx| {
                let ldt = i32::try_from(now_secs).unwrap_or(i32::MAX);
                (idx, CellValue::tombstone(timestamp, ldt))
            })
            .collect();
        cells.sort_by_key(|(idx, _)| *idx);

        Row {
            clustering,
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_value;
    use crate::collection::{build_collection_cells, CollectionOp};

    fn single_row_partition(cells: Vec<(u16, CellValue)>) -> Partition {
        let dk = DecoratedKey::new(PartitionKey::new(encode_value(&CqlValue::Int(1))));
        let row = Row {
            clustering: vec![],
            cells,
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        Partition {
            key: dk,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![row],
        }
    }

    /// A partition's static row is part of every clustered row a reader sees;
    /// where a row also holds the static cell, the newer one wins.
    #[test]
    fn static_row_cells_are_overlaid_on_every_row() {
        let mut partition = single_row_partition(vec![(1, CellValue::live(b"a1".to_vec(), 10))]);
        let mut second = partition.rows[0].clone();
        second.clustering = vec![1];
        second.cells = vec![
            (0, CellValue::live(b"row-old".to_vec(), 5)),
            (1, CellValue::live(b"a2".to_vec(), 10)),
        ];
        partition.rows[0].clustering = vec![0];
        partition.rows.push(second);
        partition.static_row = Some(Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"static".to_vec(), 20))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        });
        let rows = partition_to_rows(
            &partition,
            &["id".into(), "s".into(), "a".into()],
            &[CqlType::Int, CqlType::Blob, CqlType::Blob],
            &[0],
            &[],
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row[1], Some(CqlValue::Blob(b"static".to_vec())), "{rows:?}");
        }
        assert_eq!(rows[0][2], Some(CqlValue::Blob(b"a1".to_vec())));
        assert_eq!(rows[1][2], Some(CqlValue::Blob(b"a2".to_vec())));
    }

    /// The primary SELECT read path assembles a complex `list` column from its
    /// per-element cells (many cells at one storage index, each path-keyed) in
    /// append order — rather than decoding each element cell as the whole column.
    #[test]
    fn primary_read_path_assembles_complex_list_in_append_order() {
        let mut cells: Vec<(u16, CellValue)> = Vec::new();
        for c in build_collection_cells(
            CollectionOp::Add,
            &CqlValue::List(vec![CqlValue::Int(10)]),
            100,
        )
        .unwrap()
        {
            cells.push((0, c));
        }
        for c in build_collection_cells(
            CollectionOp::Add,
            &CqlValue::List(vec![CqlValue::Int(20)]),
            200,
        )
        .unwrap()
        {
            cells.push((0, c));
        }
        let partition = single_row_partition(cells);
        let rows = partition_to_rows(
            &partition,
            &["id".into(), "v".into()],
            &[CqlType::Int, CqlType::List(Box::new(CqlType::Int))],
            &[0],
            &[],
        )
        .unwrap();
        assert_eq!(
            rows[0][1],
            Some(CqlValue::List(vec![CqlValue::Int(10), CqlValue::Int(20)])),
        );
    }

    /// A set with a removal tombstone: the read path reconciles per path (LWW),
    /// so the removed element does not resurrect. Cells reaching the read path
    /// are already one-per-path (the merge reconciled `a`'s live+tombstone into a
    /// tombstone), so the input here is `b` live + `a` tombstoned.
    #[test]
    fn primary_read_path_reconciles_set_removal() {
        let mut cells: Vec<(u16, CellValue)> = Vec::new();
        for c in build_collection_cells(
            CollectionOp::Add,
            &CqlValue::Set(vec![CqlValue::Text("b".into())]),
            100,
        )
        .unwrap()
        {
            cells.push((0, c));
        }
        for c in build_collection_cells(
            CollectionOp::Sub,
            &CqlValue::Set(vec![CqlValue::Text("a".into())]),
            200,
        )
        .unwrap()
        {
            cells.push((0, c));
        }
        let partition = single_row_partition(cells);
        let rows = partition_to_rows(
            &partition,
            &["id".into(), "v".into()],
            &[CqlType::Int, CqlType::Set(Box::new(CqlType::Varchar))],
            &[0],
            &[],
        )
        .unwrap();
        assert_eq!(
            rows[0][1],
            Some(CqlValue::Set(vec![CqlValue::Text("b".into())]))
        );
    }

    /// Backward compatibility: a legacy single-cell whole-value collection
    /// (path = None) still decodes as the whole value.
    #[test]
    fn primary_read_path_legacy_whole_value_collection() {
        let whole = encode_value(&CqlValue::List(vec![CqlValue::Int(7), CqlValue::Int(8)]));
        let partition = single_row_partition(vec![(0, CellValue::live(whole, 100))]);
        let rows = partition_to_rows(
            &partition,
            &["id".into(), "v".into()],
            &[CqlType::Int, CqlType::List(Box::new(CqlType::Int))],
            &[0],
            &[],
        )
        .unwrap();
        assert_eq!(
            rows[0][1],
            Some(CqlValue::List(vec![CqlValue::Int(7), CqlValue::Int(8)])),
        );
    }

    /// FM RB-Tcf7ca2cc: a corrupt simple cell fails the read with a typed error
    /// naming the column and partition key. It is never returned as NULL.
    #[test]
    fn corrupt_simple_cell_fails_the_read() {
        // An INT cell must be 4 bytes; 2 bytes is corrupt.
        let partition = single_row_partition(vec![(0, CellValue::live(vec![0, 1], 100))]);
        let err = partition_to_rows(
            &partition,
            &["id".into(), "v".into()],
            &[CqlType::Int, CqlType::Int],
            &[0],
            &[],
        )
        .unwrap_err();
        assert_eq!(err.column(), "v");
        assert_eq!(
            err.partition_key(),
            encode_value(&CqlValue::Int(1)).as_slice()
        );
        assert!(err.to_string().contains("column v"), "{err}");
        assert_eq!(err.in_table("ks.t").table(), Some("ks.t"));
    }

    /// RB-T151-04: a corrupt jsonb cell (simple, or inside a collection) fails
    /// the read carrying the typed fault and the column; never NULL.
    #[test]
    fn corrupt_jsonb_cells_fail_the_read_with_the_typed_fault() {
        let bad = vec![0xf2u8, 1, 2];
        let mut list = 1i32.to_be_bytes().to_vec();
        list.extend_from_slice(&(bad.len() as i32).to_be_bytes());
        list.extend_from_slice(&bad);
        let cases = [
            (CqlType::Jsonb, bad.clone()),
            (CqlType::List(Box::new(CqlType::Jsonb)), list),
        ];
        for (ty, bytes) in cases {
            let partition = single_row_partition(vec![(0, CellValue::live(bytes, 100))]);
            let err = partition_to_rows(
                &partition,
                &["id".into(), "v".into()],
                &[CqlType::Int, ty.clone()],
                &[0],
                &[],
            )
            .unwrap_err();
            assert_eq!(err.column(), "v", "{ty:?}");
            assert!(
                matches!(
                    err.jsonb_fault(),
                    Some(crate::JsonbFault::UnknownEnvelope { byte: 0xf2, .. })
                ),
                "{ty:?}: {err}"
            );
        }
    }

    /// The streaming visitor stops at the corrupt row and reports the error.
    #[test]
    fn corrupt_simple_cell_fails_the_streaming_visitor() {
        let partition = single_row_partition(vec![(0, CellValue::live(vec![0, 1], 100))]);
        let mut visited = 0;
        let result = visit_partition_rows_with_clustering(
            &partition,
            &["id".into(), "v".into()],
            &[CqlType::Int, CqlType::Int],
            &[0],
            &[],
            &[1],
            |_, _| {
                visited += 1;
                ControlFlow::Continue(())
            },
        );
        assert!(result.is_err());
        assert_eq!(visited, 0, "a corrupt row must not reach the visitor");
    }

    /// The consuming variant reports the same error.
    #[test]
    fn corrupt_simple_cell_fails_the_consuming_visitor() {
        let partition = single_row_partition(vec![(0, CellValue::live(vec![0, 1], 100))]);
        let result = consume_partition_rows_with_clustering(
            partition,
            &["id".into(), "v".into()],
            &[CqlType::Int, CqlType::Int],
            &[0],
            &[],
            &[1],
            |_, _, _| ControlFlow::Continue(()),
        );
        assert_eq!(result.unwrap_err().column(), "v");
    }

    /// An uncorrupted read is unchanged.
    #[test]
    fn uncorrupted_simple_cell_reads_unchanged() {
        let partition = single_row_partition(vec![(
            0,
            CellValue::live(encode_value(&CqlValue::Int(9)), 100),
        )]);
        let rows = partition_to_rows(
            &partition,
            &["id".into(), "v".into()],
            &[CqlType::Int, CqlType::Int],
            &[0],
            &[],
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![vec![Some(CqlValue::Int(1)), Some(CqlValue::Int(9))]]
        );
    }

    /// The projection fast path emits only the requested columns' stored bytes,
    /// in the requested order, and a tombstoned cell reads back as NULL.
    #[test]
    fn projected_raw_rows_emit_only_the_projection_in_order() {
        let partition = single_row_partition(vec![
            (0, CellValue::live(encode_value(&CqlValue::Int(11)), 100)),
            (1, CellValue::tombstone(200, i32::MAX)),
        ]);
        // Full table order is ["id", "a", "b"]; the partition key is "id" (index
        // 0), so storage cells 0 and 1 map to table columns 1 and 2.
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        write_partition_raw_rows_projected(
            &partition,
            3,
            &[0],
            &[],
            &[1usize, 2],
            &[2, 1],
            |row| rows.push(row.iter().map(|c| c.map(<[u8]>::to_vec)).collect()),
        );
        assert_eq!(rows.len(), 1);
        // Requested order [b, a]: b is tombstoned (NULL), a is live.
        assert_eq!(rows[0][0], None, "{rows:?}");
        assert_eq!(
            rows[0][1],
            Some(encode_value(&CqlValue::Int(11))),
            "{rows:?}"
        );
    }

    /// A one-column projection emits a one-cell row, not the full table width.
    #[test]
    fn projected_raw_rows_emit_a_narrower_row_than_the_table() {
        let partition = single_row_partition(vec![
            (0, CellValue::live(encode_value(&CqlValue::Int(11)), 100)),
            (1, CellValue::live(encode_value(&CqlValue::Int(22)), 100)),
        ]);
        let mut width = None;
        write_partition_raw_rows_projected(&partition, 3, &[0], &[], &[1usize, 2], &[2], |row| {
            width = Some(row.len())
        });
        assert_eq!(width, Some(1), "a one-column projection must emit one cell");
    }

    /// An assembled (collection / UDT / vector / tuple) column is a projection
    /// barrier: its value is not the bytes of a single stored cell.
    #[test]
    fn assembled_column_types_are_not_raw_encodable() {
        assert!(projected_column_raw_encodable(&CqlType::Int));
        assert!(projected_column_raw_encodable(&CqlType::Varchar));
        assert!(projected_column_raw_encodable(&CqlType::Timestamp));
        assert!(projected_column_raw_encodable(&CqlType::Blob));
        assert!(!projected_column_raw_encodable(&CqlType::List(Box::new(
            CqlType::Int
        ))));
        assert!(!projected_column_raw_encodable(&CqlType::Set(Box::new(
            CqlType::Int
        ))));
        assert!(!projected_column_raw_encodable(&CqlType::Map(
            Box::new(CqlType::Varchar),
            Box::new(CqlType::Int)
        )));
        assert!(!projected_column_raw_encodable(&CqlType::Tuple(vec![
            CqlType::Int
        ])));
        assert!(!projected_column_raw_encodable(&CqlType::Vector(
            Box::new(CqlType::Float),
            3
        )));
        assert!(!projected_column_raw_encodable(&CqlType::Udt {
            keyspace: "ks".into(),
            name: "t".into(),
            fields: vec![],
        }));
    }

    /// `column_projection` maps each projected name to its table column index and
    /// leaves a function-call column (`count`, `writetime`) unmapped.
    #[test]
    fn column_projection_maps_names_and_leaves_function_calls_unmapped() {
        let all: Vec<String> = ["id", "a", "b"].iter().map(|s| s.to_string()).collect();
        let selected: Vec<String> = ["b", "count", "id"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            column_projection(&all, &selected),
            vec![Some(2), None, Some(0)]
        );
    }
}
