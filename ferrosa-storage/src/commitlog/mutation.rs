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
//! ```
//!
//! All multi-byte integers are big-endian.
//!
//! # Backward Compatibility
//!
//! The `mutation_id` field (16 bytes) was added in format v2.  Old segments
//! that lack it are detected by the commit-log reader, which fills
//! `mutation_id` with all-zeros.  A zero `mutation_id` is **never** used for
//! deduplication — mutations with a zero id are always re-applied on replay.

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
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

/// Size of a single serialized cell.
fn cell_size(cell: &CellValue) -> usize {
    // column_index:u16 + timestamp:i64 + ttl:i32 + local_deletion_time:i32 + value_len:i32
    let header = 2 + 8 + 4 + 4 + 4; // = 22
    let value_bytes = match &cell.value {
        Some(v) => v.len(),
        None => 0, // tombstone: value_len = -1, no bytes follow
    };
    header + value_bytes
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
        w.write_u16(column_index);
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
    }

    /// Deserializes a mutation from a byte slice.
    ///
    /// Returns an error if the buffer is truncated or contains invalid UTF-8
    /// in string fields.
    pub fn deserialize_from(buf: &[u8]) -> Result<Self> {
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
            rows.push(Self::deserialize_row(&mut r)?);
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

    fn deserialize_row(r: &mut ReadCursor<'_>) -> Result<Row> {
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
            cells.push(Self::deserialize_cell(r)?);
        }

        Ok(Row {
            clustering,
            cells,
            deletion,
            primary_key_liveness,
        })
    }

    fn deserialize_cell(r: &mut ReadCursor<'_>) -> Result<(u16, CellValue)> {
        let column_index = r.read_u16("cell.column_index")?;
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

        Ok((
            column_index,
            CellValue {
                value,
                timestamp,
                ttl,
                local_deletion_time,
                // Legacy commit-log cell format carries no cell path; complex-cell
                // paths are added to the wire format in a later increment.
                path: None,
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

    proptest! {
        #[test]
        fn serialization_round_trip(mutation in arb_mutation()) {
            let size = mutation.serialized_size();
            let mut buf = vec![0u8; size];
            mutation.serialize_into(&mut buf);
            let deserialized = Mutation::deserialize_from(&buf).unwrap();
            prop_assert_eq!(mutation.mutation_id, deserialized.mutation_id);
            prop_assert_eq!(&mutation.keyspace, &deserialized.keyspace);
            prop_assert_eq!(&mutation.table, &deserialized.table);
            prop_assert_eq!(&mutation.key, &deserialized.key);
            prop_assert_eq!(mutation.rows.len(), deserialized.rows.len());
            prop_assert_eq!(mutation.timestamp, deserialized.timestamp);
            for (orig, deser) in mutation.rows.iter().zip(deserialized.rows.iter()) {
                prop_assert_eq!(&orig.clustering, &deser.clustering);
                prop_assert_eq!(orig.cells.len(), deser.cells.len());
                prop_assert_eq!(orig.deletion, deser.deletion);
                prop_assert_eq!(orig.primary_key_liveness, deser.primary_key_liveness);
                for (oc, dc) in orig.cells.iter().zip(deser.cells.iter()) {
                    prop_assert_eq!(oc, dc);
                }
            }
        }
    }
}
