//! Data.db reader for Cassandra BTI SSTables.
//!
//! The Data.db file contains serialized partitions in token order. Each
//! partition consists of a header (key + deletion time) followed by zero or
//! more rows, terminated by an `END_OF_PARTITION` sentinel byte.
//!
//! Timestamps, TTLs, and local deletion times are **delta-encoded** against
//! the baseline values stored in the [`SerializationHeader`] from Statistics.db.
//! This dramatically reduces on-disk size when values cluster near the minimum.
//!
//! # Deferred
//!
//! - Range tombstone markers
//! - Complex columns (collections, UDTs, frozen types)
//!
//! [`SerializationHeader`]: crate::statistics::SerializationHeader

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Result};

use crate::io::ReadAt;
use crate::statistics::SerializationHeader;
use crate::types::{DeletionTime, LivenessInfo, Partition, Row};
use crate::varint;

// ---------------------------------------------------------------------------
// Row flags (bit positions in the flags byte preceding each unfiltered row)
// ---------------------------------------------------------------------------

/// Marks the end of a partition's row sequence.
const END_OF_PARTITION: u8 = 0x01;
/// Row has a non-default timestamp (delta-encoded).
const HAS_TIMESTAMP: u8 = 0x02;
/// Row has a TTL (delta-encoded).
const HAS_TTL: u8 = 0x04;
/// Row has a row-level deletion time.
const HAS_DELETION: u8 = 0x08;
/// All columns in the schema are present (no missing-column bitmap).
const HAS_ALL_COLUMNS: u8 = 0x10;
/// This row is a static row (no clustering key).
const IS_STATIC: u8 = 0x20;

// ---------------------------------------------------------------------------
// Cell flags
// ---------------------------------------------------------------------------

/// Cell inherits the row-level timestamp (no per-cell timestamp encoded).
const CELL_USE_ROW_TIMESTAMP: u8 = 0x01;
/// Cell inherits the row-level TTL.
const CELL_USE_ROW_TTL: u8 = 0x02;
/// Cell is a tombstone (deleted).
const CELL_IS_DELETED: u8 = 0x04;
/// Cell is empty (no value bytes).
const CELL_IS_EMPTY: u8 = 0x08;

// ---------------------------------------------------------------------------
// DataReader
// ---------------------------------------------------------------------------

/// Reads partitions sequentially from a Data.db file.
///
/// The reader tracks its current byte position and advances through the file
/// as partitions are read. It requires a [`SerializationHeader`] for delta
/// decoding timestamps and TTLs.
pub struct DataReader<'a, R: ReadAt> {
    reader: &'a R,
    header: &'a SerializationHeader,
    pos: u64,
}

impl<'a, R: ReadAt> DataReader<'a, R> {
    /// Create a new `DataReader` starting at `start_pos` in the data file.
    pub fn new(reader: &'a R, header: &'a SerializationHeader, start_pos: u64) -> Self {
        Self {
            reader,
            header,
            pos: start_pos,
        }
    }

    /// Read the next partition from the data file.
    ///
    /// Returns `Ok(None)` when the reader has reached EOF.
    pub fn read_partition(&mut self) -> Result<Option<Partition>> {
        let file_len = self.reader.len()?;
        if self.pos >= file_len {
            return Ok(None);
        }

        let (key_bytes, deletion) = self.read_partition_header()?;
        let key = DecoratedKey::new(PartitionKey::new(key_bytes));

        let (static_row, rows) = self.read_rows()?;

        Ok(Some(Partition {
            key,
            deletion,
            static_row,
            rows,
        }))
    }

    /// Returns the current read position in the data file.
    pub fn position(&self) -> u64 {
        self.pos
    }

    // -----------------------------------------------------------------------
    // Internal: partition header
    // -----------------------------------------------------------------------

    /// Read the partition header: key bytes + partition-level deletion time.
    fn read_partition_header(&mut self) -> Result<(Vec<u8>, DeletionTime)> {
        // Key: u16 BE length + key bytes
        let mut len_buf = [0u8; 2];
        self.reader.read_exact_at(&mut len_buf, self.pos)?;
        self.pos += 2;
        let key_len = u16::from_be_bytes(len_buf) as usize;

        let mut key_bytes = vec![0u8; key_len];
        self.reader.read_exact_at(&mut key_bytes, self.pos)?;
        self.pos += key_len as u64;

        // Deletion time: i32 local_deletion_time + i64 marked_for_delete_at
        let mut del_buf = [0u8; 12];
        self.reader.read_exact_at(&mut del_buf, self.pos)?;
        self.pos += 12;

        let local_deletion_time = i32::from_be_bytes(del_buf[0..4].try_into().unwrap());
        let marked_for_delete_at = i64::from_be_bytes(del_buf[4..12].try_into().unwrap());

        let deletion = if local_deletion_time == i32::MAX && marked_for_delete_at == i64::MIN {
            DeletionTime::LIVE
        } else {
            DeletionTime::new(marked_for_delete_at, local_deletion_time as u32)
        };

        Ok((key_bytes, deletion))
    }

    // -----------------------------------------------------------------------
    // Internal: row reading
    // -----------------------------------------------------------------------

    /// Read all rows (and optionally a static row) until `END_OF_PARTITION`.
    fn read_rows(&mut self) -> Result<(Option<Row>, Vec<Row>)> {
        let mut static_row = None;
        let mut rows = Vec::new();

        loop {
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, self.pos)?;
            self.pos += 1;
            let flags = flags_buf[0];

            if flags & END_OF_PARTITION != 0 {
                break;
            }

            let row = self.read_row(flags)?;

            if flags & IS_STATIC != 0 {
                static_row = Some(row);
            } else {
                rows.push(row);
            }
        }

        Ok((static_row, rows))
    }

    /// Read a single row given its already-consumed flags byte.
    fn read_row(&mut self, flags: u8) -> Result<Row> {
        let is_static = flags & IS_STATIC != 0;

        // Clustering key (not present for static rows)
        let clustering = if is_static {
            Vec::new()
        } else {
            let (len, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let ck_len = len as usize;
            let mut cbuf = vec![0u8; ck_len];
            self.reader.read_exact_at(&mut cbuf, self.pos)?;
            self.pos += ck_len as u64;
            cbuf
        };

        // Row body size (serialized row size varint — we skip this, it's used
        // for skipping rows during filtering but we always read fully).
        let (_row_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;

        // Previous unfiltered size (used for reverse iteration — skip).
        let (_prev_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;

        // Liveness info
        let mut liveness = LivenessInfo::NONE;
        if flags & HAS_TIMESTAMP != 0 {
            let (delta, n) = varint::read_signed_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            liveness.timestamp = self.header.min_timestamp + delta;

            if flags & HAS_TTL != 0 {
                let (ttl_delta, n) = varint::read_signed_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.ttl = self.header.min_ttl + ttl_delta as i32;

                let (ldt_delta, n) = varint::read_signed_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.local_deletion_time =
                    self.header.min_local_deletion_time + ldt_delta as i32;
            }
        }

        // Row-level deletion
        let deletion = if flags & HAS_DELETION != 0 {
            let (ts_delta, n) = varint::read_signed_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let (ldt_delta, n) = varint::read_signed_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;

            let marked_for_delete_at = self.header.min_timestamp + ts_delta;
            let local_deletion_time =
                (self.header.min_local_deletion_time + ldt_delta as i32) as u32;
            DeletionTime::new(marked_for_delete_at, local_deletion_time)
        } else {
            DeletionTime::LIVE
        };

        // Columns present bitmap (only if not HAS_ALL_COLUMNS)
        // For simplicity, when HAS_ALL_COLUMNS is not set we read a bitmap.
        let columns = if is_static {
            &self.header.static_columns
        } else {
            &self.header.regular_columns
        };
        let num_columns = columns.len();

        let present_columns: Vec<usize> = if flags & HAS_ALL_COLUMNS != 0 {
            (0..num_columns).collect()
        } else {
            // Bitmap: ceil(num_columns / 8) bytes, one bit per column (MSB first).
            // A set bit means the column is NOT present (missing).
            let bitmap_bytes = num_columns.div_ceil(8);
            let mut bitmap = vec![0u8; bitmap_bytes];
            self.reader.read_exact_at(&mut bitmap, self.pos)?;
            self.pos += bitmap_bytes as u64;

            let mut present = Vec::new();
            for i in 0..num_columns {
                let byte_idx = i / 8;
                let bit_idx = 7 - (i % 8); // MSB first
                if bitmap[byte_idx] & (1 << bit_idx) == 0 {
                    present.push(i);
                }
            }
            present
        };

        // Read cells for present columns
        let mut cells = Vec::with_capacity(present_columns.len());
        for &col_idx in &present_columns {
            let cell = self.read_cell(flags, &liveness)?;
            cells.push((col_idx as u16, cell));
        }

        Ok(Row {
            clustering,
            cells,
            deletion,
            primary_key_liveness: liveness,
        })
    }

    // -----------------------------------------------------------------------
    // Internal: cell reading
    // -----------------------------------------------------------------------

    /// Read a single cell value. The row's flags and liveness are needed for
    /// inheriting row-level timestamp/TTL.
    fn read_cell(&mut self, _row_flags: u8, row_liveness: &LivenessInfo) -> Result<CellValue> {
        let mut cell_flags_buf = [0u8; 1];
        self.reader.read_exact_at(&mut cell_flags_buf, self.pos)?;
        self.pos += 1;
        let cell_flags = cell_flags_buf[0];

        let is_deleted = cell_flags & CELL_IS_DELETED != 0;
        let is_empty = cell_flags & CELL_IS_EMPTY != 0;
        let use_row_timestamp = cell_flags & CELL_USE_ROW_TIMESTAMP != 0;
        let use_row_ttl = cell_flags & CELL_USE_ROW_TTL != 0;

        // Timestamp
        let timestamp = if use_row_timestamp {
            row_liveness.timestamp
        } else {
            let (delta, n) = varint::read_signed_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            self.header.min_timestamp + delta
        };

        // Local deletion time (for tombstones and expiring cells)
        let mut local_deletion_time = ferrosa_common::NO_DELETION_TIME;
        let mut ttl = ferrosa_common::NO_TTL;

        if is_deleted {
            // Deleted cells have a local deletion time delta
            let (ldt_delta, n) = varint::read_signed_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            local_deletion_time = self.header.min_local_deletion_time + ldt_delta as i32;
        } else if use_row_ttl {
            ttl = row_liveness.ttl;
            local_deletion_time = row_liveness.local_deletion_time;
        }

        // Value
        let value = if is_deleted || is_empty {
            None
        } else {
            let (vlen, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let vlen = vlen as usize;
            let mut vbuf = vec![0u8; vlen];
            self.reader.read_exact_at(&mut vbuf, self.pos)?;
            self.pos += vlen as u64;
            Some(vbuf)
        };

        if is_deleted {
            Ok(CellValue::tombstone(timestamp, local_deletion_time))
        } else if ttl != ferrosa_common::NO_TTL {
            Ok(CellValue::expiring(
                value.unwrap_or_default(),
                timestamp,
                ttl,
                local_deletion_time,
            ))
        } else {
            Ok(CellValue::live(value.unwrap_or_default(), timestamp))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;

    /// Helper: write a signed varint to a buffer, return bytes written.
    fn push_signed_vint(out: &mut Vec<u8>, value: i64) {
        let mut buf = [0u8; 9];
        let n = varint::write_signed_vint(&mut buf, value);
        out.extend_from_slice(&buf[..n]);
    }

    /// Helper: write an unsigned varint to a buffer, return bytes written.
    fn push_unsigned_vint(out: &mut Vec<u8>, value: u64) {
        let mut buf = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut buf, value);
        out.extend_from_slice(&buf[..n]);
    }

    /// Build a minimal serialization header for testing.
    fn test_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        }
    }

    /// Build a minimal Data.db blob for one partition with one row and one cell.
    ///
    /// Partition: key = b"pk1", live deletion
    /// Row: clustering = [0x00, 0x00, 0x00, 0x01] (int 1), timestamp delta = 42
    /// Cell: uses row timestamp, value = b"hello"
    fn build_one_partition_blob(header: &SerializationHeader) -> Vec<u8> {
        let mut data = Vec::new();

        // -- Partition header --
        // Key: u16 BE length + key bytes
        let key = b"pk1";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);

        // Deletion time: i32 local_deletion_time + i64 marked_for_delete_at (live)
        data.extend_from_slice(&i32::MAX.to_be_bytes()); // live sentinel
        data.extend_from_slice(&i64::MIN.to_be_bytes()); // live sentinel

        // -- Row --
        // Flags: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        let row_flags: u8 = HAS_TIMESTAMP | HAS_ALL_COLUMNS;
        data.push(row_flags);

        // Clustering key: varint length + bytes
        let clustering = [0x00u8, 0x00, 0x00, 0x01]; // int32 = 1
        push_unsigned_vint(&mut data, clustering.len() as u64);
        data.extend_from_slice(&clustering);

        // Row body size (we write a dummy value — the reader skips it)
        push_unsigned_vint(&mut data, 20);
        // Previous unfiltered size
        push_unsigned_vint(&mut data, 0);

        // Liveness info: timestamp delta = 42 (no TTL since HAS_TTL not set)
        let ts_delta = 42i64;
        push_signed_vint(&mut data, ts_delta);

        // Cell: use row timestamp, has value
        let cell_flags: u8 = CELL_USE_ROW_TIMESTAMP;
        data.push(cell_flags);

        // Value: varint length + bytes
        let value = b"hello";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        // -- End of partition --
        data.push(END_OF_PARTITION);

        let _ = header; // used for documentation clarity
        data
    }

    #[test]
    fn read_single_partition_one_row() {
        let header = test_header();
        let data = build_one_partition_blob(&header);
        let mut reader = DataReader::new(&data, &header, 0);

        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        // Verify key
        assert_eq!(partition.key.key.as_bytes(), b"pk1");

        // Verify partition deletion is live
        assert!(partition.deletion.is_live());

        // No static row
        assert!(partition.static_row.is_none());

        // One row
        assert_eq!(partition.rows.len(), 1);
        let row = &partition.rows[0];

        // Clustering key
        assert_eq!(row.clustering, vec![0x00, 0x00, 0x00, 0x01]);

        // Liveness: min_timestamp + delta = 1_000_000 + 42 = 1_000_042
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_042);

        // Row deletion is live
        assert!(row.deletion.is_live());

        // One cell
        assert_eq!(row.cells.len(), 1);
        let (col_idx, ref cell) = row.cells[0];
        assert_eq!(col_idx, 0);
        assert!(cell.is_live());
        assert_eq!(cell.value.as_deref(), Some(b"hello".as_slice()));
        // Cell uses row timestamp
        assert_eq!(cell.timestamp, 1_000_042);

        // No more partitions
        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_empty_partition() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header: key = b"empty"
        let key = b"empty";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());

        // Immediately end of partition
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        assert_eq!(partition.key.key.as_bytes(), b"empty");
        assert!(partition.deletion.is_live());
        assert!(partition.static_row.is_none());
        assert!(partition.rows.is_empty());

        // EOF
        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_partition_with_own_cell_timestamp() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header
        let key = b"pk2";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        let row_flags: u8 = HAS_TIMESTAMP | HAS_ALL_COLUMNS;
        data.push(row_flags);

        // Clustering
        let clustering = [0x00u8, 0x00, 0x00, 0x02];
        push_unsigned_vint(&mut data, clustering.len() as u64);
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 30);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 100
        push_signed_vint(&mut data, 100);

        // Cell: does NOT use row timestamp (flags = 0), has its own timestamp
        let cell_flags: u8 = 0;
        data.push(cell_flags);

        // Cell timestamp delta = 200
        push_signed_vint(&mut data, 200);

        // Value
        let value = b"world";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        // End of partition
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        let row = &partition.rows[0];
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_100);

        let (_, ref cell) = row.cells[0];
        // Cell has its own timestamp: min_timestamp + 200 = 1_000_200
        assert_eq!(cell.timestamp, 1_000_200);
        assert_eq!(cell.value.as_deref(), Some(b"world".as_slice()));
    }

    #[test]
    fn read_partition_with_deleted_cell() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header
        let key = b"pk3";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        let row_flags: u8 = HAS_TIMESTAMP | HAS_ALL_COLUMNS;
        data.push(row_flags);

        // Clustering
        let clustering = [0x00u8, 0x00, 0x00, 0x03];
        push_unsigned_vint(&mut data, clustering.len() as u64);
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 50
        push_signed_vint(&mut data, 50);

        // Cell: deleted, does not use row timestamp
        let cell_flags: u8 = CELL_IS_DELETED;
        data.push(cell_flags);

        // Cell timestamp delta = 60
        push_signed_vint(&mut data, 60);

        // Local deletion time delta (relative to header.min_local_deletion_time)
        // header.min_local_deletion_time = i32::MAX, delta = -2147483547
        // so actual = i32::MAX + (-2147483547) = 100
        push_signed_vint(&mut data, -2_147_483_547);

        // No value (deleted cell)

        // End of partition
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected a partition");

        let row = &partition.rows[0];
        let (_, ref cell) = row.cells[0];
        assert!(cell.is_tombstone());
        assert_eq!(cell.timestamp, 1_000_060);
        assert_eq!(cell.local_deletion_time, 100);
        assert!(cell.value.is_none());
    }

    #[test]
    fn read_two_partitions() {
        let header = test_header();
        let mut data = build_one_partition_blob(&header);

        // Second partition: key = b"pk2", empty (no rows)
        let key2 = b"pk2";
        data.extend_from_slice(&(key2.len() as u16).to_be_bytes());
        data.extend_from_slice(key2);
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());
        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);

        let p1 = reader.read_partition().unwrap().expect("partition 1");
        assert_eq!(p1.key.key.as_bytes(), b"pk1");
        assert_eq!(p1.rows.len(), 1);

        let p2 = reader.read_partition().unwrap().expect("partition 2");
        assert_eq!(p2.key.key.as_bytes(), b"pk2");
        assert!(p2.rows.is_empty());

        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_partition_with_row_deletion() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header
        let key = b"pkdel";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());

        // Row: HAS_TIMESTAMP | HAS_DELETION | HAS_ALL_COLUMNS
        let row_flags: u8 = HAS_TIMESTAMP | HAS_DELETION | HAS_ALL_COLUMNS;
        data.push(row_flags);

        // Clustering
        let clustering = [0x00u8, 0x00, 0x00, 0x07];
        push_unsigned_vint(&mut data, clustering.len() as u64);
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 30);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 10
        push_signed_vint(&mut data, 10);

        // Row deletion: timestamp delta = 20, local_deletion_time delta
        // ldt: we want local_deletion_time = 500. header.min_local_deletion_time = i32::MAX.
        // delta = 500 - i32::MAX
        push_signed_vint(&mut data, 20);
        let ldt_delta = 500i64 - i32::MAX as i64;
        push_signed_vint(&mut data, ldt_delta);

        // Cell: use row timestamp
        data.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"x";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");
        let row = &partition.rows[0];

        assert!(!row.deletion.is_live());
        assert_eq!(row.deletion.marked_for_delete_at, 1_000_020);
        assert_eq!(row.deletion.local_deletion_time, 500);
    }

    #[test]
    fn read_partition_with_missing_columns() {
        // Header with 2 regular columns
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"col_a".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"col_b".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
            ],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"pk_sparse";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());

        // Row: HAS_TIMESTAMP, no HAS_ALL_COLUMNS
        let row_flags: u8 = HAS_TIMESTAMP;
        data.push(row_flags);

        // Clustering
        let clustering = [0x00u8, 0x00, 0x00, 0x05];
        push_unsigned_vint(&mut data, clustering.len() as u64);
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 10
        push_signed_vint(&mut data, 10);

        // Missing-column bitmap: 2 columns -> 1 byte
        // We want column 0 present, column 1 missing.
        // Bitmap: bit 7 = col 0 (0 = present), bit 6 = col 1 (1 = missing)
        // = 0b01000000 = 0x40
        data.push(0x40);

        // Only column 0's cell: use row timestamp
        data.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"only_a";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        let row = &partition.rows[0];
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].0, 0); // column index 0
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"only_a".as_slice()));
    }

    #[test]
    fn eof_returns_none() {
        let header = test_header();
        let data: Vec<u8> = Vec::new();
        let mut reader = DataReader::new(&data, &header, 0);
        assert!(reader.read_partition().unwrap().is_none());
    }

    #[test]
    fn read_partition_with_delta_decoded_timestamps() {
        // Use non-trivial min_timestamp to verify delta decoding
        let header = SerializationHeader {
            min_timestamp: 1_700_000_000_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"data".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"ts_test";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering
        let clustering = [0x00u8, 0x00, 0x00, 0x0A];
        push_unsigned_vint(&mut data, clustering.len() as u64);
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 999
        push_signed_vint(&mut data, 999);

        // Cell: own timestamp, delta = 1500
        data.push(0x00); // no cell flags
        push_signed_vint(&mut data, 1500);
        let value = b"ts_value";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        let row = &partition.rows[0];
        assert_eq!(row.primary_key_liveness.timestamp, 1_700_000_000_000_999);

        let (_, ref cell) = row.cells[0];
        assert_eq!(cell.timestamp, 1_700_000_000_001_500);
    }

    #[test]
    fn position_advances_correctly() {
        let header = test_header();
        let data = build_one_partition_blob(&header);
        let data_len = data.len() as u64;

        let mut reader = DataReader::new(&data, &header, 0);
        assert_eq!(reader.position(), 0);

        let _ = reader.read_partition().unwrap();
        assert_eq!(reader.position(), data_len);
    }
}
