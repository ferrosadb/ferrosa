//! `Partition` -> row decomposition and partition/clustering key decoders.
//!
//! Moved verbatim (behaviour-identical) from `ferrosa-cql::bridge` so the CQL
//! and Postgres front-ends share one column-ordering / tombstone-skipping path.
//! Duplicating this logic would risk silently-divergent row ordering — the top
//! FMEA risk for the SQL front-end — so it lives here once.

use std::time::{SystemTime, UNIX_EPOCH};

use ferrosa_common::{CellValue, CqlType, CqlValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

use crate::codec::{decode_value, encode_value};
use crate::RowBridgeError;

/// Convert a storage `Partition` back to result rows for CQL RESULT encoding.
///
/// Each returned row is a `Vec<Option<CqlValue>>` with one entry per column
/// in `column_names`. Tombstone rows and cells are represented as `None`.
pub fn partition_to_rows(
    partition: &ferrosa_sstable::types::Partition,
    column_names: &[String],
    column_types: &[CqlType],
    pk_columns: &[usize],
    ck_columns: &[usize],
) -> Vec<Vec<Option<CqlValue>>> {
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
) -> Vec<Vec<Option<CqlValue>>> {
    partition_to_rows_with_clustering(
        partition,
        column_names,
        column_types,
        pk_columns,
        ck_columns,
        storage_to_table,
    )
    .into_iter()
    .map(|(_clustering, row)| row)
    .collect()
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
) -> Vec<(Vec<u8>, Vec<Option<CqlValue>>)> {
    let mut result = Vec::new();

    // Wall-clock seconds for TTL expiry, evaluated once per call. Expiry is
    // applied at read time because compaction does not purge expired cells.
    let now_secs = i32::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    )
    .unwrap_or(i32::MAX);

    // Pre-decode PK values from the partition key
    let pk_values = decode_pk(&partition.key, pk_columns.len());

    for row in &partition.rows {
        // TTL expiry: a row whose primary-key liveness was written with a TTL
        // and has expired is gone — unless some cell is still live (a later
        // non-TTL UPDATE can resurrect it). Mirrors Cassandra semantics.
        let pkl = &row.primary_key_liveness;
        let liveness_expired = pkl.has_ttl() && ldt_is_expired(pkl.local_deletion_time, now_secs);
        if liveness_expired {
            let any_live_cell = row.cells.iter().any(|(_, c)| cell_is_live(c, now_secs));
            if !any_live_cell {
                continue;
            }
        }

        // Skip tombstone rows — but only if no newer mutation supersedes the
        // tombstone. In Cassandra semantics, an UPDATE or INSERT after a
        // DELETE resurrects the row: the primary_key_liveness timestamp or
        // cell timestamps may be newer than the row-level deletion.
        if !row.deletion.is_live() {
            let del_ts = row.deletion.marked_for_delete_at;
            let liveness_supersedes = row.primary_key_liveness.timestamp > del_ts;
            let any_cell_supersedes = row.cells.iter().any(|(_, cell)| cell.timestamp > del_ts);
            if !liveness_supersedes && !any_cell_supersedes {
                continue;
            }
        }

        let mut output_row: Vec<Option<CqlValue>> = vec![None; column_names.len()];

        // Fill PK columns
        for (i, &col_idx) in pk_columns.iter().enumerate() {
            if col_idx < column_types.len() {
                if let Some(bytes) = pk_values.get(i) {
                    if let Ok(val) = decode_value(&column_types[col_idx], bytes) {
                        output_row[col_idx] = Some(val);
                    }
                }
            }
        }

        // Fill CK columns
        let ck_values = decode_clustering(&row.clustering, ck_columns.len());
        for (i, &col_idx) in ck_columns.iter().enumerate() {
            if col_idx < column_types.len() {
                if let Some(bytes) = ck_values.get(i) {
                    if let Ok(val) = decode_value(&column_types[col_idx], bytes) {
                        output_row[col_idx] = Some(val);
                    }
                }
            }
        }

        // Fill regular/static columns from cells.
        //
        // Cell indices are in storage column space (0-based within
        // static+regular columns).  Translate to full-table column index
        // via the mapping built above.
        for (col_index, cell) in &row.cells {
            let storage_idx = *col_index as usize;
            let table_idx = match storage_to_table.get(storage_idx) {
                Some(&idx) => idx,
                None => continue, // out-of-range storage index — skip
            };
            if table_idx < column_types.len() {
                if !cell_is_live(cell, now_secs) {
                    // Tombstone or expired TTL cell → reads as null.
                    output_row[table_idx] = None;
                } else if let Some(ref value_bytes) = cell.value {
                    if let Ok(val) = decode_value(&column_types[table_idx], value_bytes) {
                        output_row[table_idx] = Some(val);
                    }
                }
            }
        }

        result.push((row.clustering.clone(), output_row));
    }

    result
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
        return Err(RowBridgeError(
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
                .map_err(|_| RowBridgeError("partition key component too large".to_string()))?;
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
