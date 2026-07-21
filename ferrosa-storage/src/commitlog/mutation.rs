//! Mutation type and self-describing binary serialization for the commit log.
//!
//! A [`Mutation`] groups one or more row writes to a single table. Unlike
//! SSTable row format (which uses delta-encoding against a
//! `SerializationHeader`), commit log entries must be self-describing — each
//! entry carries its own keyspace, table, key, and full row data.
//!
//! The serialization is hand-rolled, writing directly into a pre-sized buffer
//! with no intermediate allocations. Call [`Mutation::serialized_size()`] to
//! compute the exact byte count, allocate a buffer, then
//! [`Mutation::serialize_into()`] to write.
//!
//! # Binary Layout
//!
//! ```text
//! Mutation: mutation_id:[u8;16]
//!         | keyspace_len:u16 | keyspace | table_len:u16 | table
//!         | key_len:u16 | key_bytes | token:i64 | timestamp:i64
//!         | row_count:u16 | rows...
//!
//! Row: clustering_len:u16 | clustering
//!    | deletion_marked_for_delete_at:i64 | deletion_local_deletion_time:u32
//!    | liveness_timestamp:i64 | liveness_ttl:i32 | liveness_local_deletion_time:i32
//!    | cell_count:u16 | cells...
//!
//! Cell: column_index:u16 | timestamp:i64 | ttl:i32 | local_deletion_time:i32
//!     | value_len:i32 (-1=tombstone) | value
//!     | [path_len:u16 | path]   -- iff the 0x8000 bit of column_index is set
//! ```
//!
//! The high bit (`0x8000`) of `column_index` flags a **complex-column cell path**
//! (a collection element identity — timeuuid/element/key). The next bit
//! (`0x4000`) is a **transient Accord list-path rebind flag**: the coordinator
//! sets it on a non-frozen `list` append cell so the replica's apply phase
//! rewrites the cell's path from the coordinator-local wall clock to the
//! Accord-agreed execution timestamp (see
//! [`Mutation::deserialize_from_rebinding_list_paths`]). It is consumed at
//! decode time and NEVER reaches the SSTable — the stored column index is always
//! the clean low-14-bit value. Real column indices are `< 0x4000`, so simple
//! cells and old segments (which never set either bit) are byte-for-byte
//! identical to the pre-path format — no version bump required.
//!
//! All multi-byte integers are big-endian.
//!
//! # Backward Compatibility
//!
//! The `mutation_id` field (16 bytes) was added in format v2.  Old segments
//! that lack it are detected by the commit-log reader, which fills
//! `mutation_id` with all-zeros.  A zero `mutation_id` is **never** used for
//! deduplication — mutations with a zero id are always re-applied on replay.

use ferrosa_common::accord::Timestamp as AccordTimestamp;
use ferrosa_common::{
    accord_list_cell_path, list_path_element_seq, CellValue, DecoratedKey, PartitionKey, Token,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use uuid::Uuid;

/// A mutation: one or more row writes targeting a single table.
#[derive(Debug, Clone)]
pub struct Mutation {
    /// Globally-unique mutation identifier (UUID v4 stored as 16 raw bytes).
    ///
    /// Generated at write time via [`Mutation::new`].  Used during commit-log
    /// replay to deduplicate mutations that were written more than once (e.g.
    /// because a crash occurred during a previous replay).
    ///
    /// A zero value (`[0u8; 16]`) is the legacy sentinel for mutations read
    /// from old commit-log segments that pre-date this field.  Zero ids are
    /// **not** deduplicated — they are always re-applied.
    pub mutation_id: [u8; 16],
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Partition key (with cached token).
    pub key: DecoratedKey,
    /// Rows to write.
    pub rows: Vec<Row>,
    /// Mutation timestamp (microseconds since epoch).
    pub timestamp: i64,
}

impl Mutation {
    /// Creates a new mutation with a freshly-generated UUID.
    pub fn new(
        keyspace: String,
        table: String,
        key: DecoratedKey,
        rows: Vec<Row>,
        timestamp: i64,
    ) -> Self {
        Self {
            mutation_id: Uuid::new_v4().into_bytes(),
            keyspace,
            table,
            key,
            rows,
            timestamp,
        }
    }

    /// Returns `true` if this mutation carries the legacy zero `mutation_id`.
    ///
    /// Zero ids must never be used for deduplication: they are always
    /// re-applied during replay for backward compatibility with old segments.
    pub fn has_legacy_id(&self) -> bool {
        self.mutation_id == [0u8; 16]
    }
}

/// Errors that can occur during deserialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationError {
    /// Buffer was truncated — not enough bytes to read a field.
    Truncated {
        /// What field was being read.
        field: &'static str,
        /// How many bytes were needed.
        needed: usize,
        /// How many bytes were available.
        available: usize,
    },
    /// A string field contained invalid UTF-8.
    InvalidUtf8 {
        /// Which field.
        field: &'static str,
    },
    /// A cell carried the Accord list-path rebind flag (`0x4000`) but did not
    /// have a 16-byte v1-TimeUUID path to rebind — the coordinator only sets the
    /// flag on `list` append cells, which always have a 16-byte path, so this is
    /// a corrupt/forged payload. Fail loud rather than silently mis-rebind.
    InvalidRebindListPath {
        /// The actual path length found (not 16).
        len: usize,
    },
}

impl std::fmt::Display for MutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MutationError::Truncated {
                field,
                needed,
                available,
            } => write!(
                f,
                "truncated buffer reading {field}: needed {needed} bytes, got {available}"
            ),
            MutationError::InvalidUtf8 { field } => {
                write!(f, "invalid UTF-8 in field {field}")
            }
            MutationError::InvalidRebindListPath { len } => write!(
                f,
                "cell has the Accord list-path rebind flag but a {len}-byte path (expected a \
                 16-byte v1 TimeUUID)"
            ),
        }
    }
}

impl std::error::Error for MutationError {}

type Result<T> = std::result::Result<T, MutationError>;

// ---------------------------------------------------------------------------
// Helper: cursor for reading from a byte slice
// ---------------------------------------------------------------------------

struct ReadCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ReadCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_bytes(&mut self, n: usize, field: &'static str) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(MutationError::Truncated {
                field,
                needed: n,
                available: self.remaining(),
            });
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u16(&mut self, field: &'static str) -> Result<u16> {
        let bytes = self.read_bytes(2, field)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self, field: &'static str) -> Result<i32> {
        let bytes = self.read_bytes(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u32(&mut self, field: &'static str) -> Result<u32> {
        let bytes = self.read_bytes(4, field)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self, field: &'static str) -> Result<i64> {
        let bytes = self.read_bytes(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_string(&mut self, field: &'static str) -> Result<String> {
        let len = self.read_u16(field)? as usize;
        let bytes = self.read_bytes(len, field)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| MutationError::InvalidUtf8 { field })
    }

    fn read_byte_vec(&mut self, field: &'static str) -> Result<Vec<u8>> {
        let len = self.read_u16(field)? as usize;
        let bytes = self.read_bytes(len, field)?;
        Ok(bytes.to_vec())
    }
}

// ---------------------------------------------------------------------------
// Helper: write cursor into a mutable byte slice
// ---------------------------------------------------------------------------

struct WriteCursor<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> WriteCursor<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn write_bytes(&mut self, data: &[u8]) {
        self.buf[self.pos..self.pos + data.len()].copy_from_slice(data);
        self.pos += data.len();
    }

    fn write_u16(&mut self, v: u16) {
        self.write_bytes(&v.to_be_bytes());
    }

    fn write_i32(&mut self, v: i32) {
        self.write_bytes(&v.to_be_bytes());
    }

    fn write_u32(&mut self, v: u32) {
        self.write_bytes(&v.to_be_bytes());
    }

    fn write_i64(&mut self, v: i64) {
        self.write_bytes(&v.to_be_bytes());
    }

    fn write_string(&mut self, s: &str) {
        self.write_u16(s.len() as u16);
        self.write_bytes(s.as_bytes());
    }

    fn write_byte_vec(&mut self, data: &[u8]) {
        self.write_u16(data.len() as u16);
        self.write_bytes(data);
    }
}

// ---------------------------------------------------------------------------
// Size computation helpers
// ---------------------------------------------------------------------------

/// Size of a length-prefixed string: 2-byte u16 length + string bytes.
fn string_size(s: &str) -> usize {
    2 + s.len()
}

/// Size of a length-prefixed byte array: 2-byte u16 length + data bytes.
fn byte_vec_size(data: &[u8]) -> usize {
    2 + data.len()
}

/// High bit of a cell's `column_index`, set to signal that a complex-column cell
/// **path** follows the value. Real column indices are `< 0x8000`, and a simple
/// (scalar) cell — the only kind pre-dating this and the only kind produced until
/// the write path emits complex cells — never sets it, so old commit-log segments
/// and simple cells serialize byte-for-byte as before. Only path-bearing cells use
/// the extended encoding; no format-version bump is needed.
const CELL_HAS_PATH_FLAG: u16 = 0x8000;
/// Second-highest bit of a cell's `column_index`, set by the coordinator to mark
/// a non-frozen `list` append cell whose path must be **rebound** from the
/// coordinator-local wall clock to the Accord-agreed execution timestamp at apply
/// time (see [`Mutation::deserialize_from_rebinding_list_paths`]). Transient: it
/// is stripped at decode and never persisted — the stored column index is always
/// the clean low-14-bit value. Only set on the Accord write path, so old segments
/// and the AP/logged-batch paths never carry it. Public so the CQL coordinator
/// (`ferrosa-cql`) sets exactly this bit when materializing an Accord list append
/// — one source of truth for the flag value across the serialize/deserialize seam.
pub const CELL_REBIND_LIST_PATH_FLAG: u16 = 0x4000;
/// Mask for the real column index (the low 14 bits) once both high flags are
/// stripped. Real column indices are `< 0x4000` (16384) — far beyond any table.
const CELL_COLUMN_INDEX_MASK: u16 = 0x3FFF;

/// Size of a single serialized cell.
fn cell_size(cell: &CellValue) -> usize {
    // column_index:u16 + timestamp:i64 + ttl:i32 + local_deletion_time:i32 + value_len:i32
    let header = 2 + 8 + 4 + 4 + 4; // = 22
    let value_bytes = match &cell.value {
        Some(v) => v.len(),
        None => 0, // tombstone: value_len = -1, no bytes follow
    };
    // Complex-cell path (when present): path_len:u16 + path bytes.
    let path_bytes = cell.path.as_ref().map_or(0, |p| 2 + p.len());
    header + value_bytes + path_bytes
}

/// Size of a single serialized row.
fn row_size(row: &Row) -> usize {
    let mut size = 0;
    // clustering_len:u16 + clustering bytes
    size += byte_vec_size(&row.clustering);
    // deletion: marked_for_delete_at:i64 + local_deletion_time:u32
    size += 8 + 4;
    // liveness: timestamp:i64 + ttl:i32 + local_deletion_time:i32
    size += 8 + 4 + 4;
    // cell_count:u16
    size += 2;
    // cells
    for (_, cell) in &row.cells {
        size += cell_size(cell);
    }
    size
}

impl Mutation {
    /// Computes the exact serialized byte count for this mutation.
    ///
    /// Use this to allocate a buffer before calling [`serialize_into()`](Self::serialize_into).
    pub fn serialized_size(&self) -> usize {
        let mut size = 0;
        // mutation_id: [u8; 16]
        size += 16;
        // keyspace: len:u16 + bytes
        size += string_size(&self.keyspace);
        // table: len:u16 + bytes
        size += string_size(&self.table);
        // key: len:u16 + bytes
        size += byte_vec_size(self.key.key.as_bytes());
        // token: i64
        size += 8;
        // timestamp: i64
        size += 8;
        // row_count: u16
        size += 2;
        // rows
        for row in &self.rows {
            size += row_size(row);
        }
        size
    }

    /// Computes the serialized size for a single-row mutation without first
    /// constructing an owned [`Mutation`].
    pub fn serialized_size_for_single_row(
        keyspace: &str,
        table: &str,
        key: &DecoratedKey,
        row: &Row,
    ) -> usize {
        16 + string_size(keyspace)
            + string_size(table)
            + byte_vec_size(key.key.as_bytes())
            + 8
            + 8
            + 2
            + row_size(row)
    }

    /// Serializes this mutation into a pre-sized buffer.
    ///
    /// # Panics
    ///
    /// Panics if `buf.len() < self.serialized_size()`.
    pub fn serialize_into(&self, buf: &mut [u8]) {
        assert!(
            buf.len() >= self.serialized_size(),
            "buffer too small: got {}, need {}",
            buf.len(),
            self.serialized_size()
        );

        let mut w = WriteCursor::new(buf);

        // mutation_id: 16 raw bytes (UUID)
        w.write_bytes(&self.mutation_id);

        // Mutation header
        w.write_string(&self.keyspace);
        w.write_string(&self.table);
        w.write_byte_vec(self.key.key.as_bytes());
        w.write_i64(self.key.token.0);
        w.write_i64(self.timestamp);
        w.write_u16(self.rows.len() as u16);

        // Rows
        for row in &self.rows {
            Self::serialize_row(&mut w, row);
        }
    }

    /// Serializes a single-row mutation into a pre-sized buffer without
    /// cloning the row into an owned [`Mutation`] first.
    ///
    /// # Panics
    ///
    /// Panics if `buf.len()` is too small for the serialized mutation.
    pub fn serialize_single_row_into(
        mutation_id: [u8; 16],
        keyspace: &str,
        table: &str,
        key: &DecoratedKey,
        row: &Row,
        timestamp: i64,
        buf: &mut [u8],
    ) {
        let required = Self::serialized_size_for_single_row(keyspace, table, key, row);
        assert!(
            buf.len() >= required,
            "buffer too small: got {}, need {}",
            buf.len(),
            required
        );

        let mut w = WriteCursor::new(buf);
        w.write_bytes(&mutation_id);
        w.write_string(keyspace);
        w.write_string(table);
        w.write_byte_vec(key.key.as_bytes());
        w.write_i64(key.token.0);
        w.write_i64(timestamp);
        w.write_u16(1);
        Self::serialize_row(&mut w, row);
    }

    fn serialize_row(w: &mut WriteCursor<'_>, row: &Row) {
        // Clustering key
        w.write_byte_vec(&row.clustering);

        // Row deletion
        w.write_i64(row.deletion.marked_for_delete_at);
        w.write_u32(row.deletion.local_deletion_time);

        // Primary key liveness
        w.write_i64(row.primary_key_liveness.timestamp);
        w.write_i32(row.primary_key_liveness.ttl);
        w.write_i32(row.primary_key_liveness.local_deletion_time);

        // Cells
        w.write_u16(row.cells.len() as u16);
        for (column_index, cell) in &row.cells {
            Self::serialize_cell(w, *column_index, cell);
        }
    }

    fn serialize_cell(w: &mut WriteCursor<'_>, column_index: u16, cell: &CellValue) {
        // The caller may pre-set the transient rebind flag (0x4000) on the column
        // index for an Accord `list` append cell; pass it through so the replica's
        // apply phase can rebind the path. The path flag (0x8000) is derived from
        // the cell, never passed in — fail loud if a real index collides with it.
        let rebind = column_index & CELL_REBIND_LIST_PATH_FLAG;
        assert!(
            column_index & !(CELL_REBIND_LIST_PATH_FLAG | CELL_COLUMN_INDEX_MASK) == 0,
            "column index {} collides with a reserved cell flag (>= 0x8000, or >= 0x4000 without \
             being the rebind flag)",
            column_index & CELL_COLUMN_INDEX_MASK
        );
        let tagged = (column_index & CELL_COLUMN_INDEX_MASK)
            | rebind
            | if cell.path.is_some() {
                CELL_HAS_PATH_FLAG
            } else {
                0
            };
        w.write_u16(tagged);
        w.write_i64(cell.timestamp);
        w.write_i32(cell.ttl);
        w.write_i32(cell.local_deletion_time);

        match &cell.value {
            None => {
                // Tombstone: value_len = -1
                w.write_i32(-1);
            }
            Some(v) => {
                w.write_i32(v.len() as i32);
                w.write_bytes(v);
            }
        }

        // Complex-column cell path (path_len:u16 | path bytes), only when present.
        if let Some(path) = &cell.path {
            w.write_byte_vec(path);
        }
    }

    /// Deserializes a mutation from a byte slice.
    ///
    /// Returns an error if the buffer is truncated or contains invalid UTF-8
    /// in string fields. Any transient Accord list-path rebind flag (`0x4000`)
    /// on a cell is **stripped** (the clean column index is kept) but the path is
    /// left as-is — use [`deserialize_from_rebinding_list_paths`] on the apply
    /// path to actually rebind. In practice only the Accord apply path ever sees
    /// a flagged payload, so this default decode never encounters the flag.
    ///
    /// [`deserialize_from_rebinding_list_paths`]: Self::deserialize_from_rebinding_list_paths
    pub fn deserialize_from(buf: &[u8]) -> Result<Self> {
        Self::deserialize_inner(buf, None)
    }

    /// Deserialize a mutation, rebinding every Accord `list` append cell (flagged
    /// with `0x4000`) to the agreed execution timestamp `t`.
    ///
    /// The coordinator stamps a list append cell's path with its own wall clock
    /// *before* consensus picks `t`, which would order concurrent appends by
    /// coordinator clock rather than the Accord total order (non-strict-
    /// serializable, t_68f226b5). This rewrites each flagged cell's path to
    /// [`accord_list_cell_path(&t, element_seq)`], preserving the element's
    /// within-append order (`element_seq` recovered from the pre-consensus path's
    /// clock_seq) while making the primary order the Accord `t`. Non-flagged cells
    /// (scalars, `set`/`map` elements, non-frozen UDT fields) are untouched. The
    /// stripped-clean column index is stored, so the flag never reaches the
    /// SSTable. Fails loud if a flagged cell lacks a 16-byte path.
    pub fn deserialize_from_rebinding_list_paths(buf: &[u8], t: AccordTimestamp) -> Result<Self> {
        Self::deserialize_inner(buf, Some(t))
    }

    fn deserialize_inner(buf: &[u8], rebind_t: Option<AccordTimestamp>) -> Result<Self> {
        let mut r = ReadCursor::new(buf);

        // mutation_id: 16 raw bytes (UUID v4, or all-zeros for legacy entries)
        let id_bytes = r.read_bytes(16, "mutation_id")?;
        let mut mutation_id = [0u8; 16];
        mutation_id.copy_from_slice(id_bytes);

        let keyspace = r.read_string("keyspace")?;
        let table = r.read_string("table")?;
        let key_bytes = r.read_byte_vec("key")?;
        let token_val = r.read_i64("token")?;
        let timestamp = r.read_i64("timestamp")?;
        let row_count = r.read_u16("row_count")? as usize;

        let key = DecoratedKey {
            token: Token(token_val),
            key: PartitionKey::new(key_bytes),
        };

        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            rows.push(Self::deserialize_row(&mut r, rebind_t)?);
        }

        Ok(Mutation {
            mutation_id,
            keyspace,
            table,
            key,
            rows,
            timestamp,
        })
    }

    fn deserialize_row(r: &mut ReadCursor<'_>, rebind_t: Option<AccordTimestamp>) -> Result<Row> {
        let clustering = r.read_byte_vec("clustering")?;

        let marked_for_delete_at = r.read_i64("deletion.marked_for_delete_at")?;
        let deletion_ldt = r.read_u32("deletion.local_deletion_time")?;
        let deletion = DeletionTime::new(marked_for_delete_at, deletion_ldt);

        let liveness_ts = r.read_i64("liveness.timestamp")?;
        let liveness_ttl = r.read_i32("liveness.ttl")?;
        let liveness_ldt = r.read_i32("liveness.local_deletion_time")?;
        let primary_key_liveness = LivenessInfo {
            timestamp: liveness_ts,
            ttl: liveness_ttl,
            local_deletion_time: liveness_ldt,
        };

        let cell_count = r.read_u16("cell_count")? as usize;
        let mut cells = Vec::with_capacity(cell_count);
        for _ in 0..cell_count {
            cells.push(Self::deserialize_cell(r, rebind_t)?);
        }

        Ok(Row {
            clustering,
            cells,
            deletion,
            primary_key_liveness,
        })
    }

    fn deserialize_cell(
        r: &mut ReadCursor<'_>,
        rebind_t: Option<AccordTimestamp>,
    ) -> Result<(u16, CellValue)> {
        let tagged = r.read_u16("cell.column_index")?;
        let has_path = tagged & CELL_HAS_PATH_FLAG != 0;
        let needs_rebind = tagged & CELL_REBIND_LIST_PATH_FLAG != 0;
        // The clean column index — both transient flags stripped, so the stored
        // index (and thus the SSTable) never carries them.
        let column_index = tagged & CELL_COLUMN_INDEX_MASK;
        let timestamp = r.read_i64("cell.timestamp")?;
        let ttl = r.read_i32("cell.ttl")?;
        let local_deletion_time = r.read_i32("cell.local_deletion_time")?;
        let value_len = r.read_i32("cell.value_len")?;

        let value = if value_len == -1 {
            None
        } else {
            let len = value_len as usize;
            let bytes = r.read_bytes(len, "cell.value")?;
            Some(bytes.to_vec())
        };

        // A complex-column cell path follows the value iff the flag was set. Old
        // segments and simple cells never set it, so they decode with path == None.
        let mut path = if has_path {
            Some(r.read_byte_vec("cell.path")?)
        } else {
            None
        };

        // Accord list-path rebind: when this cell was flagged AND a rebind
        // timestamp was supplied (the apply path), rewrite the coordinator-clock
        // list path to the agreed execution timestamp, preserving the element's
        // within-append order via the original path's clock_seq. A flagged cell
        // must have a 16-byte v1-TimeUUID path — fail loud otherwise.
        if needs_rebind {
            if let Some(t) = rebind_t {
                let old = path.as_deref().unwrap_or_default();
                let element_seq = list_path_element_seq(old)
                    .ok_or(MutationError::InvalidRebindListPath { len: old.len() })?;
                path = Some(accord_list_cell_path(&t, element_seq));
            }
        }

        Ok((
            column_index,
            CellValue {
                value,
                timestamp,
                ttl,
                local_deletion_time,
                path,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    /// Helper to create a simple mutation for testing.
    fn simple_mutation() -> Mutation {
        Mutation {
            mutation_id: [1u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 42_000,
        }
    }

    fn assert_mutations_equal(a: &Mutation, b: &Mutation) {
        assert_eq!(a.mutation_id, b.mutation_id);
        assert_eq!(a.keyspace, b.keyspace);
        assert_eq!(a.table, b.table);
        assert_eq!(a.key, b.key);
        assert_eq!(a.timestamp, b.timestamp);
        assert_eq!(a.rows.len(), b.rows.len());
        for (ra, rb) in a.rows.iter().zip(b.rows.iter()) {
            assert_eq!(ra.clustering, rb.clustering);
            assert_eq!(ra.deletion, rb.deletion);
            assert_eq!(ra.primary_key_liveness, rb.primary_key_liveness);
            assert_eq!(ra.cells.len(), rb.cells.len());
            for (ca, cb) in ra.cells.iter().zip(rb.cells.iter()) {
                assert_eq!(ca, cb);
            }
        }
    }

    #[test]
    fn round_trip_simple() {
        let m = simple_mutation();
        let size = m.serialized_size();
        let mut buf = vec![0u8; size];
        m.serialize_into(&mut buf);
        let m2 = Mutation::deserialize_from(&buf).unwrap();
        assert_mutations_equal(&m, &m2);
    }

    #[test]
    fn round_trip_tombstone() {
        let m = Mutation {
            mutation_id: [2u8; 16],
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk".to_vec())),
            rows: vec![Row {
                clustering: vec![10],
                cells: vec![(0, CellValue::tombstone(5000, 1_700_000_000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(5000),
            }],
            timestamp: 5000,
        };

        let size = m.serialized_size();
        let mut buf = vec![0u8; size];
        m.serialize_into(&mut buf);

        // Verify the tombstone encodes value_len = -1
        let m2 = Mutation::deserialize_from(&buf).unwrap();
        assert!(m2.rows[0].cells[0].1.is_tombstone());
        assert!(m2.rows[0].cells[0].1.value.is_none());
        assert_mutations_equal(&m, &m2);
    }

    #[test]
    fn round_trip_expiring() {
        let m = Mutation {
            mutation_id: [3u8; 16],
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk".to_vec())),
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(
                    3,
                    CellValue::expiring(b"temp".to_vec(), 2000, 3600, 1_700_003_600),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(2000),
            }],
            timestamp: 2000,
        };

        let size = m.serialized_size();
        let mut buf = vec![0u8; size];
        m.serialize_into(&mut buf);
        let m2 = Mutation::deserialize_from(&buf).unwrap();
        assert_mutations_equal(&m, &m2);

        let cell = &m2.rows[0].cells[0].1;
        assert!(cell.is_expiring());
        assert_eq!(cell.ttl, 3600);
        assert_eq!(cell.local_deletion_time, 1_700_003_600);
    }

    #[test]
    fn round_trip_empty_rows() {
        let m = Mutation {
            mutation_id: [4u8; 16],
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"key".to_vec())),
            rows: vec![],
            timestamp: 100,
        };

        let size = m.serialized_size();
        let mut buf = vec![0u8; size];
        m.serialize_into(&mut buf);
        let m2 = Mutation::deserialize_from(&buf).unwrap();
        assert_mutations_equal(&m, &m2);
        assert!(m2.rows.is_empty());
    }

    #[test]
    fn round_trip_multiple_rows() {
        let m = Mutation {
            mutation_id: [5u8; 16],
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"multi".to_vec())),
            rows: vec![
                Row {
                    clustering: vec![0, 0, 1],
                    cells: vec![
                        (0, CellValue::live(b"a".to_vec(), 100)),
                        (1, CellValue::live(b"b".to_vec(), 100)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(100),
                },
                Row {
                    clustering: vec![0, 0, 2],
                    cells: vec![(0, CellValue::tombstone(200, 1_700_000_000))],
                    deletion: DeletionTime::new(200, 1_700_000_000),
                    primary_key_liveness: LivenessInfo::with_timestamp(200),
                },
                Row {
                    clustering: vec![0, 0, 3],
                    cells: vec![],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::NONE,
                },
            ],
            timestamp: 200,
        };

        let size = m.serialized_size();
        let mut buf = vec![0u8; size];
        m.serialize_into(&mut buf);
        let m2 = Mutation::deserialize_from(&buf).unwrap();
        assert_mutations_equal(&m, &m2);
        assert_eq!(m2.rows.len(), 3);
    }

    #[test]
    fn serialized_size_matches_actual() {
        let m = simple_mutation();
        let size = m.serialized_size();
        let mut buf = vec![0u8; size];
        m.serialize_into(&mut buf);

        // Verify exact size: serialize_into uses exactly `size` bytes.
        // If serialize_into wrote fewer bytes, the trailing bytes would be zeros.
        // If it needs more, it panics. We also verify by deserializing from
        // exactly that slice — the cursor should consume all bytes.
        let m2 = Mutation::deserialize_from(&buf).unwrap();
        assert_eq!(m2.serialized_size(), size);
    }

    #[test]
    fn deserialize_truncated_fails() {
        let m = simple_mutation();
        let size = m.serialized_size();
        let mut buf = vec![0u8; size];
        m.serialize_into(&mut buf);

        // Truncate at various points — all should fail
        for truncate_at in [0, 1, 2, 5, 10, size / 2, size - 1] {
            let result = Mutation::deserialize_from(&buf[..truncate_at]);
            assert!(
                result.is_err(),
                "expected error for truncation at {truncate_at}, got Ok"
            );
        }
    }

    /// A complex-column cell (with a path) round-trips through the commit-log /
    /// Accord-wire encoding — value AND path preserved — for both live and
    /// tombstone cells.
    #[test]
    fn complex_cell_path_round_trips() {
        let mut m = simple_mutation();
        m.rows[0].cells = vec![
            (
                3,
                CellValue::live(b"elem".to_vec(), 1000).with_path(b"timeuuid-path".to_vec()),
            ),
            (
                3,
                CellValue::tombstone(1001, 1_700_000_000).with_path(b"removed-elem".to_vec()),
            ),
        ];
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        let back = Mutation::deserialize_from(&buf).unwrap();
        assert_eq!(
            back.rows[0].cells, m.rows[0].cells,
            "value + path round-trip"
        );
        assert_eq!(
            back.rows[0].cells[0].1.path.as_deref(),
            Some(b"timeuuid-path".as_slice())
        );
    }

    /// Backward-compat: a simple cell (path == None) serializes with no extra bytes
    /// and never sets the path flag — byte-for-byte identical to the pre-path
    /// format, so old commit-log segments and simple cells are unaffected. The path
    /// encoding adds exactly `u16 len + bytes`.
    #[test]
    fn simple_cell_bytes_unchanged_complex_adds_exactly_the_path() {
        let simple = CellValue::live(b"v".to_vec(), 100);
        let complex = simple.clone().with_path(b"pathXYZ".to_vec());
        assert_eq!(cell_size(&simple), 22 + 1, "simple cell has no path bytes");
        assert_eq!(
            cell_size(&complex),
            cell_size(&simple) + 2 + 7,
            "path adds only its u16 length + bytes"
        );

        // A path==None cell decodes back to path==None (no flag consumed).
        let m = simple_mutation();
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        let back = Mutation::deserialize_from(&buf).unwrap();
        assert!(back.rows[0].cells.iter().all(|(_, c)| c.path.is_none()));
    }

    // -----------------------------------------------------------------------
    // Accord list-path rebind flag (0x4000) — t_68f226b5.
    // -----------------------------------------------------------------------

    use ferrosa_common::accord::Timestamp as AccordTimestamp;

    const REBIND_FLAG: u16 = 0x4000;

    fn accord_ts(time: u64, seq: u32, node: u64) -> AccordTimestamp {
        AccordTimestamp {
            epoch: 0,
            time,
            seq,
            node,
        }
    }

    /// Serialize a one-row mutation from raw `(tagged_col_idx, cell)` tuples so a
    /// test can pre-set the 0x4000 rebind flag exactly as the coordinator would.
    fn mutation_with_cells(cells: Vec<(u16, CellValue)>) -> Vec<u8> {
        let m = Mutation {
            mutation_id: [7u8; 16],
            keyspace: "ks".into(),
            table: "t".into(),
            key: DecoratedKey::new(PartitionKey::new(b"pk".to_vec())),
            rows: vec![Row {
                clustering: vec![],
                cells,
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 1000,
        };
        let mut buf = vec![0u8; m.serialized_size()];
        m.serialize_into(&mut buf);
        buf
    }

    /// The DEFAULT decode strips the rebind flag into a clean column index and
    /// leaves the (coordinator-clock) path untouched — so any non-apply consumer
    /// (commit-log replay, read-vote) is unaffected and the flag never reaches the
    /// SSTable via a polluted column index.
    #[test]
    fn default_deserialize_strips_rebind_flag_keeps_path() {
        let coord_path = ferrosa_common::accord_list_cell_path(&accord_ts(999, 0, 0), 5);
        let buf = mutation_with_cells(vec![(
            3 | REBIND_FLAG,
            CellValue::live(b"elem".to_vec(), 1000).with_path(coord_path.clone()),
        )]);
        let back = Mutation::deserialize_from(&buf).unwrap();
        let (idx, cell) = &back.rows[0].cells[0];
        assert_eq!(*idx, 3, "rebind flag stripped from the stored column index");
        assert_eq!(
            cell.path.as_deref(),
            Some(coord_path.as_slice()),
            "default decode leaves the path unchanged"
        );
    }

    /// The apply decode rebinds a flagged `list` cell's path to the Accord
    /// execution timestamp, preserving the element_seq baked into the original
    /// path; a NON-flagged sibling cell (a `set` element) is left untouched.
    #[test]
    fn rebinding_deserialize_rewrites_only_flagged_list_path() {
        let element_seq = 3u16;
        // Coordinator-clock list path (wrong time, but carries element_seq).
        let coord_path = ferrosa_common::accord_list_cell_path(&accord_ts(999, 0, 0), element_seq);
        // A set element path — arbitrary 16 bytes, NOT flagged; must survive.
        let set_path = vec![0xABu8; 16];
        let buf = mutation_with_cells(vec![
            (
                2 | REBIND_FLAG,
                CellValue::live(b"L".to_vec(), 1000).with_path(coord_path),
            ),
            (
                5,
                CellValue::live(Vec::new(), 1000).with_path(set_path.clone()),
            ),
        ]);

        let t = accord_ts(4242, 7, 9);
        let back = Mutation::deserialize_from_rebinding_list_paths(&buf, t).unwrap();
        let cells = &back.rows[0].cells;

        let (list_idx, list_cell) = cells.iter().find(|(i, _)| *i == 2).unwrap();
        assert_eq!(*list_idx, 2, "clean column index stored");
        assert_eq!(
            list_cell.path.as_deref(),
            Some(ferrosa_common::accord_list_cell_path(&t, element_seq).as_slice()),
            "flagged list path rebound to the Accord execution ts, element_seq preserved"
        );

        let (_, set_cell) = cells.iter().find(|(i, _)| *i == 5).unwrap();
        assert_eq!(
            set_cell.path.as_deref(),
            Some(set_path.as_slice()),
            "a non-flagged set element path must NOT be rebound"
        );
    }

    /// Two flagged appends within one write keep their within-append order
    /// (element_seq 0 before 1) and both take the Accord `t.time` as primary
    /// order after rebind.
    #[test]
    fn rebinding_preserves_within_append_element_order() {
        let buf = mutation_with_cells(vec![
            (
                2 | REBIND_FLAG,
                CellValue::live(b"a".to_vec(), 1000).with_path(
                    ferrosa_common::accord_list_cell_path(&accord_ts(999, 0, 0), 0),
                ),
            ),
            (
                2 | REBIND_FLAG,
                CellValue::live(b"b".to_vec(), 1000).with_path(
                    ferrosa_common::accord_list_cell_path(&accord_ts(999, 0, 0), 1),
                ),
            ),
        ]);
        let t = accord_ts(5000, 1, 2);
        let back = Mutation::deserialize_from_rebinding_list_paths(&buf, t).unwrap();
        let seqs: Vec<u16> = back.rows[0]
            .cells
            .iter()
            .map(|(_, c)| {
                ferrosa_common::list_path_element_seq(c.path.as_deref().unwrap()).unwrap()
            })
            .collect();
        assert_eq!(seqs, vec![0, 1], "element order preserved across rebind");
        for (_, c) in &back.rows[0].cells {
            let p = c.path.as_deref().unwrap();
            assert_eq!(
                p,
                ferrosa_common::accord_list_cell_path(
                    &t,
                    ferrosa_common::list_path_element_seq(p).unwrap()
                )
                .as_slice(),
                "every rebound path carries the Accord t"
            );
        }
    }

    /// A flagged cell whose path is not a 16-byte v1 TimeUUID is corrupt — fail
    /// loud rather than silently mis-rebind (never fake success).
    #[test]
    fn rebinding_flagged_cell_without_16_byte_path_fails_loud() {
        let buf = mutation_with_cells(vec![(
            2 | REBIND_FLAG,
            CellValue::live(b"x".to_vec(), 1000).with_path(vec![0u8; 8]),
        )]);
        let err = Mutation::deserialize_from_rebinding_list_paths(&buf, accord_ts(1, 0, 0))
            .expect_err("a flagged cell with a non-16-byte path must fail loud");
        assert!(matches!(
            err,
            MutationError::InvalidRebindListPath { len: 8 }
        ));
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use ferrosa_common::test_generators::{arb_cell_value, arb_decorated_key};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use proptest::prelude::*;

    fn arb_row() -> impl Strategy<Value = Row> {
        (
            prop::collection::vec(any::<u8>(), 0..32),
            prop::collection::vec((0u16..64, arb_cell_value()), 0..16),
            prop_oneof![
                Just(DeletionTime::LIVE),
                (1i64..1_000_000, 1u32..100_000).prop_map(|(ts, ldt)| DeletionTime::new(ts, ldt)),
            ],
            1i64..1_000_000,
        )
            .prop_map(|(clustering, mut cells, deletion, ts)| {
                cells.sort_by_key(|(idx, _)| *idx);
                cells.dedup_by_key(|(idx, _)| *idx);
                Row {
                    clustering,
                    cells,
                    deletion,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }
            })
    }

    fn arb_mutation() -> impl Strategy<Value = Mutation> {
        (
            "[a-z]{1,8}",
            "[a-z]{1,8}",
            arb_decorated_key(),
            prop::collection::vec(arb_row(), 0..8),
            1i64..1_000_000,
            prop::collection::vec(any::<u8>(), 16..=16),
        )
            .prop_map(|(keyspace, table, key, rows, timestamp, id_vec)| {
                let mut mutation_id = [0u8; 16];
                mutation_id.copy_from_slice(&id_vec);
                // Ensure non-zero id to avoid the legacy-sentinel path in tests.
                if mutation_id == [0u8; 16] {
                    mutation_id[0] = 1;
                }
                Mutation {
                    mutation_id,
                    keyspace,
                    table,
                    key,
                    rows,
                    timestamp,
                }
            })
    }

    /// A cell that MAY carry a complex-column path. Scoped to this module's
    /// commit-log round-trip — the shared `arb_cell_value` stays path-free because
    /// SSTable round-trip proptests don't yet support complex cells.
    fn arb_cell_value_maybe_path() -> impl Strategy<Value = CellValue> {
        (
            arb_cell_value(),
            prop::option::of(prop::collection::vec(any::<u8>(), 1..24)),
        )
            .prop_map(|(cell, path)| match path {
                Some(p) => cell.with_path(p),
                None => cell,
            })
    }

    /// Like `arb_row` but with per-element cells: cells are de-duplicated and sorted
    /// by `(col_idx, path)` — the same key the memtable merges on — so a column can
    /// carry many element cells with distinct paths.
    fn arb_row_with_paths() -> impl Strategy<Value = Row> {
        (
            prop::collection::vec(any::<u8>(), 0..32),
            prop::collection::vec((0u16..64, arb_cell_value_maybe_path()), 0..16),
            prop_oneof![
                Just(DeletionTime::LIVE),
                (1i64..1_000_000, 1u32..100_000).prop_map(|(ts, ldt)| DeletionTime::new(ts, ldt)),
            ],
            1i64..1_000_000,
        )
            .prop_map(|(clustering, mut cells, deletion, ts)| {
                cells.sort_by(|(a_idx, a), (b_idx, b)| (*a_idx, &a.path).cmp(&(*b_idx, &b.path)));
                cells.dedup_by(|(a_idx, a), (b_idx, b)| a_idx == b_idx && a.path == b.path);
                Row {
                    clustering,
                    cells,
                    deletion,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }
            })
    }

    fn arb_mutation_with_paths() -> impl Strategy<Value = Mutation> {
        (
            "[a-z]{1,8}",
            "[a-z]{1,8}",
            arb_decorated_key(),
            prop::collection::vec(arb_row_with_paths(), 0..8),
            1i64..1_000_000,
            prop::collection::vec(any::<u8>(), 16..=16),
        )
            .prop_map(|(keyspace, table, key, rows, timestamp, id_vec)| {
                let mut mutation_id = [0u8; 16];
                mutation_id.copy_from_slice(&id_vec);
                if mutation_id == [0u8; 16] {
                    mutation_id[0] = 1;
                }
                Mutation {
                    mutation_id,
                    keyspace,
                    table,
                    key,
                    rows,
                    timestamp,
                }
            })
    }

    fn assert_round_trips(mutation: &Mutation) {
        let size = mutation.serialized_size();
        let mut buf = vec![0u8; size];
        mutation.serialize_into(&mut buf);
        let deserialized = Mutation::deserialize_from(&buf).unwrap();
        assert_eq!(mutation.mutation_id, deserialized.mutation_id);
        assert_eq!(mutation.keyspace, deserialized.keyspace);
        assert_eq!(mutation.table, deserialized.table);
        assert_eq!(mutation.key, deserialized.key);
        assert_eq!(mutation.rows.len(), deserialized.rows.len());
        assert_eq!(mutation.timestamp, deserialized.timestamp);
        for (orig, deser) in mutation.rows.iter().zip(deserialized.rows.iter()) {
            assert_eq!(orig.clustering, deser.clustering);
            assert_eq!(orig.deletion, deser.deletion);
            assert_eq!(orig.primary_key_liveness, deser.primary_key_liveness);
            // Full cell equality includes the path.
            assert_eq!(orig.cells, deser.cells);
        }
    }

    proptest! {
        #[test]
        fn serialization_round_trip(mutation in arb_mutation()) {
            assert_round_trips(&mutation);
        }

        /// Same round-trip, now with per-element complex-column cells (paths).
        #[test]
        fn serialization_round_trip_with_paths(mutation in arb_mutation_with_paths()) {
            assert_round_trips(&mutation);
        }
    }
}
