//! Data.db reader for Cassandra BTI SSTables.
//!
//! The Data.db file contains serialized partitions in token order. Each
//! partition consists of a header (key + deletion time) followed by zero or
//! more rows, terminated by an `END_OF_PARTITION` sentinel byte.
//!
//! Timestamps, TTLs, and local deletion times are **delta-encoded** against
//! the baseline values stored in the [`SerializationHeader`] from Statistics.db.
//! Deltas are written as **unsigned varints** (not zigzag-encoded).
//!
//! # Deferred
//!
//! - Range tombstone markers
//! - Complex columns (collections, UDTs, frozen types)
//!
//! [`SerializationHeader`]: crate::statistics::SerializationHeader

use ferrosa_common::{CellValue, DecoratedKey, Error, PartitionKey, Result};

use crate::io::ReadAt;
use crate::marshal;
use crate::statistics::SerializationHeader;
use crate::types::{DeletionTime, LivenessInfo, Partition, Row};
use crate::varint;

// ---------------------------------------------------------------------------
// Row flags (bit positions in the flags byte preceding each unfiltered row)
// Reference: UnfilteredSerializer.java lines 108-118
// ---------------------------------------------------------------------------

/// Marks the end of a partition's row sequence.
const END_OF_PARTITION: u8 = 0x01;
/// Whether the encoded unfiltered is a range tombstone marker (not a row).
const IS_MARKER: u8 = 0x02;
/// Row has a non-default timestamp (delta-encoded).
const HAS_TIMESTAMP: u8 = 0x04;
/// Row has a TTL (delta-encoded).
const HAS_TTL: u8 = 0x08;
/// Row has a row-level deletion time.
const HAS_DELETION: u8 = 0x10;
/// All columns in the schema are present (no missing-column bitmap).
const HAS_ALL_COLUMNS: u8 = 0x20;
/// Row has complex column deletion for at least one column.
#[allow(dead_code)]
const HAS_COMPLEX_DELETION: u8 = 0x40;
/// Extended flags byte follows.
const EXTENSION_FLAG: u8 = 0x80;

// ---------------------------------------------------------------------------
// Extended flags (second byte, only present if EXTENSION_FLAG is set)
// Reference: UnfilteredSerializer.java lines 120-131
// ---------------------------------------------------------------------------

/// This row is a static row (no clustering key).
const EXT_IS_STATIC: u8 = 0x01;

// ---------------------------------------------------------------------------
// Cell flags
// Reference: Cell.java lines 279-283
// ---------------------------------------------------------------------------

/// Cell is a tombstone (deleted).
const CELL_IS_DELETED: u8 = 0x01;
/// Cell is expiring (has TTL).
const CELL_IS_EXPIRING: u8 = 0x02;
/// Cell has an empty value (tombstones, counters).
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
/// Cell inherits the row-level timestamp (no per-cell timestamp encoded).
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
/// Cell inherits the row-level TTL.
const CELL_USE_ROW_TTL: u8 = 0x10;

// ---------------------------------------------------------------------------
// Partition-level DeletionTime format (Cassandra 5.x UInt format)
// Reference: DeletionTime.java lines 216-254
// ---------------------------------------------------------------------------

/// Byte marker for a live (not deleted) partition. Since real timestamps are
/// positive, the MSB of `markedForDeleteAt` is always 0 for non-live
/// deletions, so `0x80` in the first byte signals LIVE.
const DELETION_IS_LIVE: u8 = 0x80;

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
    ///
    /// Cassandra 5.x DeletionTime (UInt format):
    /// - **LIVE**: single byte `0x80`
    /// - **Non-live**: first byte is MSB of `markedForDeleteAt` (sign bit = 0),
    ///   followed by 7 more bytes to complete the i64, then 4-byte u32
    ///   `localDeletionTime`. Total: 12 bytes.
    fn read_partition_header(&mut self) -> Result<(Vec<u8>, DeletionTime)> {
        // Key: u16 BE length + key bytes
        let mut len_buf = [0u8; 2];
        self.reader.read_exact_at(&mut len_buf, self.pos)?;
        self.pos += 2;
        let key_len = u16::from_be_bytes(len_buf) as usize;

        let mut key_bytes = vec![0u8; key_len];
        self.reader.read_exact_at(&mut key_bytes, self.pos)?;
        self.pos += key_len as u64;

        // DeletionTime: read first byte to determine format
        let mut first = [0u8; 1];
        self.reader.read_exact_at(&mut first, self.pos)?;
        self.pos += 1;

        let deletion = if first[0] & DELETION_IS_LIVE != 0 {
            // LIVE — verify the byte is exactly 0x80
            if first[0] != DELETION_IS_LIVE {
                return Err(Error::InvalidData(format!(
                    "corrupted DeletionTime flags: {:#04x}",
                    first[0]
                )));
            }
            DeletionTime::LIVE
        } else {
            // Non-live: first byte is MSB of markedForDeleteAt (i64 BE).
            // Read remaining 7 bytes of the long, then 4-byte ldt.
            let mut remaining = [0u8; 11];
            self.reader.read_exact_at(&mut remaining, self.pos)?;
            self.pos += 11;

            let mut mfda_bytes = [0u8; 8];
            mfda_bytes[0] = first[0];
            mfda_bytes[1..8].copy_from_slice(&remaining[0..7]);
            let marked_for_delete_at = i64::from_be_bytes(mfda_bytes);

            let local_deletion_time = u32::from_be_bytes(remaining[7..11].try_into().unwrap());

            DeletionTime::new(marked_for_delete_at, local_deletion_time)
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

            if flags & IS_MARKER != 0 {
                return Err(Error::InvalidData(
                    "range tombstone markers not yet supported".into(),
                ));
            }

            // Extended flags byte (only present if EXTENSION_FLAG is set)
            let extended_flags = if flags & EXTENSION_FLAG != 0 {
                let mut ext_buf = [0u8; 1];
                self.reader.read_exact_at(&mut ext_buf, self.pos)?;
                self.pos += 1;
                ext_buf[0]
            } else {
                0
            };

            let is_static = extended_flags & EXT_IS_STATIC != 0;
            let row = self.read_row(flags, is_static)?;

            if is_static {
                static_row = Some(row);
            } else {
                rows.push(row);
            }
        }

        Ok((static_row, rows))
    }

    /// Read a single row given its already-consumed flags byte.
    fn read_row(&mut self, flags: u8, is_static: bool) -> Result<Row> {
        // Clustering key (not present for static rows)
        //
        // Cassandra 5.x ClusteringPrefix format:
        //   - Header varint: 2 bits per component (null/empty flags, batched per 32)
        //   - Per non-null/non-empty component:
        //     - Fixed-length types (Int32Type etc.): raw bytes, no length prefix
        //     - Variable-length types (UTF8Type etc.): varint(size) + value bytes
        //
        // Reference: ClusteringPrefix.Serializer + AbstractType.writeValue
        let clustering = if is_static {
            Vec::new()
        } else {
            let num_clustering = self.header.clustering_types.len();
            let (header_bits, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;

            let mut ck_bytes = Vec::new();
            for i in 0..num_clustering {
                let is_null = (header_bits & (1u64 << (2 * i))) != 0;
                let is_empty = (header_bits & (1u64 << (2 * i + 1))) != 0;

                if is_null || is_empty {
                    continue;
                }

                let type_name = &self.header.clustering_types[i];
                let vlen = match marshal::value_length_if_fixed(type_name) {
                    Some(fixed_len) => fixed_len,
                    None => {
                        let (len, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                        self.pos += n as u64;
                        len as usize
                    }
                };
                let mut vbuf = vec![0u8; vlen];
                self.reader.read_exact_at(&mut vbuf, self.pos)?;
                self.pos += vlen as u64;
                ck_bytes.extend_from_slice(&vbuf);
            }
            ck_bytes
        };

        // Row body size (serialized row size varint — we skip this, it's used
        // for skipping rows during filtering but we always read fully).
        let (_row_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;

        // Previous unfiltered size (used for reverse iteration — skip).
        let (_prev_size, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
        self.pos += n as u64;

        // Liveness info (unsigned varint deltas against SerializationHeader)
        let mut liveness = LivenessInfo::NONE;
        if flags & HAS_TIMESTAMP != 0 {
            let (delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            liveness.timestamp = self.header.min_timestamp.wrapping_add(delta as i64);

            if flags & HAS_TTL != 0 {
                let (ttl_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.ttl = self.header.min_ttl.wrapping_add(ttl_delta as i32);

                let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
                self.pos += n as u64;
                liveness.local_deletion_time = self
                    .header
                    .min_local_deletion_time
                    .wrapping_add(ldt_delta as i32);
            }
        }

        // Row-level deletion (unsigned varint deltas via SerializationHeader)
        let deletion = if flags & HAS_DELETION != 0 {
            let (ts_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;

            let marked_for_delete_at = self.header.min_timestamp + ts_delta as i64;
            let local_deletion_time =
                (self.header.min_local_deletion_time as i64).wrapping_add(ldt_delta as i64) as u32;
            DeletionTime::new(marked_for_delete_at, local_deletion_time)
        } else {
            DeletionTime::LIVE
        };

        // Columns present bitmap (only if not HAS_ALL_COLUMNS)
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
            // A SET bit means the column IS present (Cassandra BooleanArraySerializer).
            let bitmap_bytes = num_columns.div_ceil(8);
            let mut bitmap = vec![0u8; bitmap_bytes];
            self.reader.read_exact_at(&mut bitmap, self.pos)?;
            self.pos += bitmap_bytes as u64;

            let mut present = Vec::new();
            for i in 0..num_columns {
                let byte_idx = i / 8;
                let bit_idx = 7 - (i % 8); // MSB first
                if bitmap[byte_idx] & (1 << bit_idx) != 0 {
                    present.push(i);
                }
            }
            present
        };

        // Read cells for present columns
        let mut cells = Vec::with_capacity(present_columns.len());
        for &col_idx in &present_columns {
            let cell = self.read_cell(&liveness)?;
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
    // Reference: Cell.Serializer in Cell.java lines 377-419
    // -----------------------------------------------------------------------

    /// Read a single cell value.
    fn read_cell(&mut self, row_liveness: &LivenessInfo) -> Result<CellValue> {
        let mut cell_flags_buf = [0u8; 1];
        self.reader.read_exact_at(&mut cell_flags_buf, self.pos)?;
        self.pos += 1;
        let cell_flags = cell_flags_buf[0];

        let is_deleted = cell_flags & CELL_IS_DELETED != 0;
        let is_expiring = cell_flags & CELL_IS_EXPIRING != 0;
        let has_empty_value = cell_flags & CELL_HAS_EMPTY_VALUE != 0;
        let use_row_timestamp = cell_flags & CELL_USE_ROW_TIMESTAMP != 0;
        let use_row_ttl = cell_flags & CELL_USE_ROW_TTL != 0;

        // Timestamp (unsigned varint delta)
        let timestamp = if use_row_timestamp {
            row_liveness.timestamp
        } else {
            let (delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            self.header.min_timestamp.wrapping_add(delta as i64)
        };

        // Local deletion time (for tombstones and expiring cells)
        let local_deletion_time = if use_row_ttl {
            row_liveness.local_deletion_time
        } else if is_deleted || is_expiring {
            let (ldt_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            self.header
                .min_local_deletion_time
                .wrapping_add(ldt_delta as i32)
        } else {
            ferrosa_common::NO_DELETION_TIME
        };

        // TTL (for expiring cells only)
        let ttl = if use_row_ttl {
            row_liveness.ttl
        } else if is_expiring {
            let (ttl_delta, n) = varint::read_unsigned_vint_at(self.reader, self.pos)?;
            self.pos += n as u64;
            self.header.min_ttl.wrapping_add(ttl_delta as i32)
        } else {
            ferrosa_common::NO_TTL
        };

        // Value (absent if HAS_EMPTY_VALUE is set)
        let value = if has_empty_value {
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
        } else if is_expiring {
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

    /// Helper: write an unsigned varint to a buffer.
    fn push_unsigned_vint(out: &mut Vec<u8>, value: u64) {
        let mut buf = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut buf, value);
        out.extend_from_slice(&buf[..n]);
    }

    /// Write a LIVE DeletionTime (single byte 0x80).
    fn push_live_deletion(out: &mut Vec<u8>) {
        out.push(DELETION_IS_LIVE);
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
    fn build_one_partition_blob(_header: &SerializationHeader) -> Vec<u8> {
        let mut data = Vec::new();

        // -- Partition header --
        let key = b"pk1";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // -- Row --
        // Flags: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering key (ClusteringPrefix format): header varint + raw fixed-length bytes
        // Int32Type is fixed-length (4 bytes), so no varint length prefix.
        let clustering = [0x00u8, 0x00, 0x00, 0x01]; // int32 = 1
        push_unsigned_vint(&mut data, 0); // header: all non-null, non-empty
        data.extend_from_slice(&clustering);

        // Row body size (dummy — reader skips it)
        push_unsigned_vint(&mut data, 20);
        // Previous unfiltered size
        push_unsigned_vint(&mut data, 0);

        // Liveness info: timestamp delta = 42 (unsigned varint)
        push_unsigned_vint(&mut data, 42);

        // Cell: use row timestamp, has value
        data.push(CELL_USE_ROW_TIMESTAMP);

        // Value: varint length + bytes
        let value = b"hello";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

        // -- End of partition --
        data.push(END_OF_PARTITION);

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
        push_live_deletion(&mut data);

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
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x02];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 30);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 100 (unsigned varint)
        push_unsigned_vint(&mut data, 100);

        // Cell: does NOT use row timestamp (flags = 0), has its own timestamp
        data.push(0x00);

        // Cell timestamp delta = 200 (unsigned varint)
        push_unsigned_vint(&mut data, 200);

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
        // Use a header where min_local_deletion_time allows positive unsigned deltas
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: 50,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"pk3";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x03];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 50 (unsigned)
        push_unsigned_vint(&mut data, 50);

        // Cell: deleted + empty value, does not use row timestamp
        data.push(CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE);

        // Cell timestamp delta = 60 (unsigned)
        push_unsigned_vint(&mut data, 60);

        // Local deletion time delta = 50 (actual = min(50) + 50 = 100)
        push_unsigned_vint(&mut data, 50);

        // No value (HAS_EMPTY_VALUE set)

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
        push_live_deletion(&mut data);
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
        // Use a header where min_local_deletion_time allows positive unsigned deltas
        let header = SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: 0,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        let mut data = Vec::new();

        // Partition header
        let key = b"pkdel";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_DELETION | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_DELETION | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x07];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 30);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 10 (unsigned)
        push_unsigned_vint(&mut data, 10);

        // Row deletion: timestamp delta = 20, local_deletion_time delta = 500
        // actual mfda = 1_000_000 + 20 = 1_000_020
        // actual ldt = 0 + 500 = 500
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 500);

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
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP, no HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x05];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 10 (unsigned)
        push_unsigned_vint(&mut data, 10);

        // Missing-column bitmap: 2 columns -> 1 byte
        // We want column 0 present, column 1 missing.
        // Bitmap: bit 7 = col 0 (1 = present), bit 6 = col 1 (0 = missing)
        // = 0b10000000 = 0x80
        data.push(0x80);

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
        push_live_deletion(&mut data);

        // Row: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering (Int32Type = fixed-length, no varint prefix)
        let clustering = [0x00u8, 0x00, 0x00, 0x0A];
        push_unsigned_vint(&mut data, 0); // clustering header
        data.extend_from_slice(&clustering);

        // Row body size + prev size
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 999 (unsigned)
        push_unsigned_vint(&mut data, 999);

        // Cell: own timestamp, delta = 1500 (unsigned)
        data.push(0x00); // no cell flags
        push_unsigned_vint(&mut data, 1500);
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

    /// Verify that delta decoding with wrapping_add does not panic when
    /// min_local_deletion_time is near i32::MAX and delta causes overflow.
    #[test]
    fn delta_decode_i32_near_max_does_not_panic() {
        // Simulate the wrapping_add arithmetic used in row reading:
        //   liveness.local_deletion_time = min_local_deletion_time.wrapping_add(delta as i32)
        let min_local_deletion_time: i32 = i32::MAX - 10;
        let delta: u64 = 20;
        // This would panic with checked add; wrapping_add should not panic.
        let result = min_local_deletion_time.wrapping_add(delta as i32);
        // The result wraps around: (i32::MAX - 10) + 20 = i32::MIN + 9
        assert_eq!(result, i32::MIN + 9);
    }

    /// Verify that delta decoding with wrapping_add does not panic when
    /// min_timestamp is near i64::MAX and delta causes overflow.
    #[test]
    fn delta_decode_i64_near_max_does_not_panic() {
        // Simulate the wrapping_add arithmetic used in row/cell reading:
        //   liveness.timestamp = min_timestamp.wrapping_add(delta as i64)
        let min_timestamp: i64 = i64::MAX - 10;
        let delta: u64 = 20;
        // This would panic with checked add; wrapping_add should not panic.
        let result = min_timestamp.wrapping_add(delta as i64);
        // The result wraps around: (i64::MAX - 10) + 20 = i64::MIN + 9
        assert_eq!(result, i64::MIN + 9);
    }

    #[test]
    fn read_non_live_partition_deletion() {
        let header = test_header();
        let mut data = Vec::new();

        // Partition header: key = b"del"
        let key = b"del";
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);

        // Non-live DeletionTime: 8-byte markedForDeleteAt + 4-byte localDeletionTime
        let mfda: i64 = 1_700_000_000;
        let ldt: u32 = 1_700_000;
        data.extend_from_slice(&mfda.to_be_bytes());
        data.extend_from_slice(&ldt.to_be_bytes());

        data.push(END_OF_PARTITION);

        let mut reader = DataReader::new(&data, &header, 0);
        let partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        assert!(!partition.deletion.is_live());
        assert_eq!(partition.deletion.marked_for_delete_at, mfda);
        assert_eq!(partition.deletion.local_deletion_time, ldt);
    }
}
