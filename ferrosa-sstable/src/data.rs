//! Data.db partition deserializer.
//!
//! Reads partitions from the data file using the [`SerializationHeader`]
//! for delta decoding of timestamps and deletion times.
//!
//! # Partition layout
//!
//! ```text
//! [key_length: u16 BE] [key_bytes: key_length bytes]
//! [deletion_time: i32 local_deletion_time BE + i64 marked_for_delete_at BE]
//! [rows...]
//! [END_OF_PARTITION marker: flags byte = 0x01]
//! ```
//!
//! # Row layout
//!
//! ```text
//! [flags: u8]
//!   bit 0 (0x01): END_OF_PARTITION — signals end of partition
//!   bit 1 (0x02): HAS_CLUSTERING — clustering key follows
//!   bit 2 (0x04): HAS_TIMESTAMP — timestamp delta follows
//!   bit 3 (0x08): HAS_DELETION — row-level deletion follows
//!   bit 4 (0x10): HAS_TTL — TTL and local deletion time deltas follow
//!   bit 5 (0x20): HAS_ALL_COLUMNS — all regular columns are present
//!   (other bits reserved)
//! [clustering: varint length + bytes]  (if HAS_CLUSTERING)
//! [timestamp_delta: signed varint]     (if HAS_TIMESTAMP)
//! [deletion: i32 + i64 BE]            (if HAS_DELETION)
//! [ttl_delta: unsigned varint]         (if HAS_TTL)
//! [local_del_delta: unsigned varint]   (if HAS_TTL)
//! [cells...]
//! ```
//!
//! Reference: `UnfilteredSerializer.java`, `RowSerializer.java`

use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Result};

use crate::io::ReadAt;
use crate::statistics::SerializationHeader;
use crate::types::{DeletionTime, LivenessInfo, Partition, Row};
use crate::varint;

/// Map an on-disk i32 local_deletion_time to our u32 representation.
///
/// Cassandra uses `Integer.MAX_VALUE` (0x7FFFFFFF) as the live sentinel on
/// disk, but our [`DeletionTime`] uses `u32::MAX`. This function handles
/// the mapping.
fn map_local_deletion_time(disk_value: i32) -> u32 {
    if disk_value == i32::MAX {
        u32::MAX
    } else {
        disk_value as u32
    }
}

// Row flags bits.
const END_OF_PARTITION: u8 = 0x01;
const HAS_CLUSTERING: u8 = 0x02;
const HAS_TIMESTAMP: u8 = 0x04;
const HAS_DELETION: u8 = 0x08;
const HAS_TTL: u8 = 0x10;
const HAS_ALL_COLUMNS: u8 = 0x20;

// Cell flags bits.
const CELL_HAS_VALUE: u8 = 0x01;
const CELL_HAS_TIMESTAMP: u8 = 0x02;
const CELL_HAS_LOCAL_DELETION: u8 = 0x04;
const CELL_HAS_TTL: u8 = 0x08;
const CELL_IS_DELETED: u8 = 0x10;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x20;

/// Data.db partition reader.
///
/// Uses a [`SerializationHeader`] for delta-decoding timestamps and
/// deletion times. The reader is stateless — each `read_partition` call
/// is independent.
pub struct DataReader<R: ReadAt> {
    reader: R,
    header: SerializationHeader,
}

impl<R: ReadAt> DataReader<R> {
    /// Create a new data reader with the given backing store and
    /// serialization header (from Statistics.db).
    pub fn new(reader: R, header: SerializationHeader) -> Self {
        Self { reader, header }
    }

    /// Read one partition starting at `offset`.
    ///
    /// Returns `(partition, next_offset)` where `next_offset` is the
    /// file position immediately after the partition (start of the next
    /// partition or EOF).
    pub fn read_partition(&self, offset: u64) -> Result<(Partition, u64)> {
        let mut pos = offset;

        // 1. Read partition key: u16 BE length + key bytes.
        let mut key_len_buf = [0u8; 2];
        self.reader.read_exact_at(&mut key_len_buf, pos)?;
        let key_len = u16::from_be_bytes(key_len_buf) as usize;
        pos += 2;

        let mut key_bytes = vec![0u8; key_len];
        self.reader.read_exact_at(&mut key_bytes, pos)?;
        pos += key_len as u64;

        // 2. Read partition deletion time: i32 local_deletion_time + i64 marked_for_delete_at.
        let mut del_buf = [0u8; 12];
        self.reader.read_exact_at(&mut del_buf, pos)?;
        let ldt_raw = i32::from_be_bytes(del_buf[0..4].try_into().unwrap());
        let marked_for_delete_at = i64::from_be_bytes(del_buf[4..12].try_into().unwrap());
        pos += 12;

        let deletion = DeletionTime {
            marked_for_delete_at,
            local_deletion_time: map_local_deletion_time(ldt_raw),
        };

        // 3. Read rows until END_OF_PARTITION.
        let mut rows = Vec::new();
        loop {
            let mut flags_buf = [0u8; 1];
            self.reader.read_exact_at(&mut flags_buf, pos)?;
            let flags = flags_buf[0];
            pos += 1;

            if flags & END_OF_PARTITION != 0 {
                break;
            }

            let (row, new_pos) = self.read_row(flags, pos)?;
            rows.push(row);
            pos = new_pos;
        }

        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::new(key_bytes)),
            deletion,
            static_row: None,
            rows,
        };

        Ok((partition, pos))
    }

    /// Read one row given the already-consumed flags byte.
    fn read_row(&self, flags: u8, start_pos: u64) -> Result<(Row, u64)> {
        let mut pos = start_pos;

        // Clustering key (if present).
        let clustering = if flags & HAS_CLUSTERING != 0 {
            let (len, n) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n as u64;
            let mut clustering_bytes = vec![0u8; len as usize];
            self.reader.read_exact_at(&mut clustering_bytes, pos)?;
            pos += len;
            clustering_bytes
        } else {
            Vec::new()
        };

        // Timestamp delta (if present).
        let mut row_timestamp = self.header.min_timestamp;
        let liveness = if flags & HAS_TIMESTAMP != 0 {
            let (delta, n) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n as u64;
            row_timestamp = self.header.min_timestamp + delta as i64;
            LivenessInfo::with_timestamp(row_timestamp)
        } else {
            LivenessInfo::NONE
        };

        // Row-level deletion (if present).
        let row_deletion = if flags & HAS_DELETION != 0 {
            let mut del_buf = [0u8; 12];
            self.reader.read_exact_at(&mut del_buf, pos)?;
            let ldt_raw = i32::from_be_bytes(del_buf[0..4].try_into().unwrap());
            let mfda = i64::from_be_bytes(del_buf[4..12].try_into().unwrap());
            pos += 12;
            DeletionTime {
                marked_for_delete_at: mfda,
                local_deletion_time: map_local_deletion_time(ldt_raw),
            }
        } else {
            DeletionTime::LIVE
        };

        // TTL (if present) — read but only used for liveness info update.
        let liveness = if flags & HAS_TTL != 0 {
            let (ttl_delta, n1) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n1 as u64;
            let (ldt_delta, n2) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n2 as u64;
            let ttl = self.header.min_ttl + ttl_delta as i32;
            let ldt = self.header.min_local_deletion_time + ldt_delta as i32;
            LivenessInfo::with_ttl(liveness.timestamp, ttl, ldt)
        } else {
            liveness
        };

        // Read cells for each regular column.
        let num_columns = if flags & HAS_ALL_COLUMNS != 0 {
            self.header.regular_columns.len()
        } else {
            // Read the column count as an unsigned varint.
            let (count, n) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n as u64;
            count as usize
        };

        let mut cells = Vec::with_capacity(num_columns);
        for col_idx in 0..num_columns {
            let (cell, new_pos) = self.read_cell(row_timestamp, pos)?;
            cells.push((col_idx as u16, cell));
            pos = new_pos;
        }

        let row = Row {
            clustering,
            cells,
            deletion: row_deletion,
            primary_key_liveness: liveness,
        };

        Ok((row, pos))
    }

    /// Read a single cell value.
    fn read_cell(&self, row_timestamp: i64, start_pos: u64) -> Result<(CellValue, u64)> {
        let mut pos = start_pos;

        let mut cell_flags_buf = [0u8; 1];
        self.reader.read_exact_at(&mut cell_flags_buf, pos)?;
        let cell_flags = cell_flags_buf[0];
        pos += 1;

        // Determine timestamp.
        let timestamp = if cell_flags & CELL_USE_ROW_TIMESTAMP != 0 {
            row_timestamp
        } else if cell_flags & CELL_HAS_TIMESTAMP != 0 {
            let (delta, n) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n as u64;
            self.header.min_timestamp + delta as i64
        } else {
            row_timestamp
        };

        // TTL and local deletion time.
        let mut ttl = 0i32;
        let mut local_deletion_time = i32::MAX;

        if cell_flags & CELL_HAS_TTL != 0 {
            let (ttl_delta, n1) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n1 as u64;
            ttl = self.header.min_ttl + ttl_delta as i32;
        }

        if cell_flags & CELL_HAS_LOCAL_DELETION != 0 {
            let (ldt_delta, n1) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n1 as u64;
            local_deletion_time = self.header.min_local_deletion_time + ldt_delta as i32;
        }

        // Deleted cell (tombstone).
        if cell_flags & CELL_IS_DELETED != 0 {
            return Ok((CellValue::tombstone(timestamp, local_deletion_time), pos));
        }

        // Value (if present).
        let value = if cell_flags & CELL_HAS_VALUE != 0 {
            let (value_len, n) = varint::read_unsigned_vint_at(&self.reader, pos)?;
            pos += n as u64;
            let mut value_bytes = vec![0u8; value_len as usize];
            self.reader.read_exact_at(&mut value_bytes, pos)?;
            pos += value_len;
            Some(value_bytes)
        } else {
            None
        };

        let cell = CellValue {
            value,
            timestamp,
            ttl,
            local_deletion_time,
        };

        Ok((cell, pos))
    }
}

/// Build a partition key with its Murmur3 token.
///
/// This is a convenience for callers who have raw key bytes from Data.db
/// and need to construct a [`DecoratedKey`] with the correct token.
pub fn decorate_key(key_bytes: Vec<u8>) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(key_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::Token;

    /// Helper: build a minimal Statistics header for tests.
    fn test_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: 0,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.UTF8Type".to_string()],
            static_columns: vec![],
            regular_columns: vec![(
                b"value".to_vec(),
                "org.apache.cassandra.db.marshal.BytesType".to_string(),
            )],
        }
    }

    /// Helper: write a u16 BE value.
    fn write_u16(buf: &mut Vec<u8>, val: u16) {
        buf.extend_from_slice(&val.to_be_bytes());
    }

    /// Helper: write an i32 BE value.
    fn write_i32(buf: &mut Vec<u8>, val: i32) {
        buf.extend_from_slice(&val.to_be_bytes());
    }

    /// Helper: write an i64 BE value.
    fn write_i64(buf: &mut Vec<u8>, val: i64) {
        buf.extend_from_slice(&val.to_be_bytes());
    }

    /// Helper: write an unsigned varint.
    fn write_uvarint(buf: &mut Vec<u8>, val: u64) {
        let mut tmp = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut tmp, val);
        buf.extend_from_slice(&tmp[..n]);
    }

    /// Build a minimal Data.db partition:
    /// - Key "hello"
    /// - Live deletion time
    /// - One row with clustering "ck1", timestamp delta=42, one cell with value "world"
    /// - END_OF_PARTITION marker
    fn build_minimal_partition() -> Vec<u8> {
        let mut data = Vec::new();

        // Partition key: u16 length + bytes.
        let key = b"hello";
        write_u16(&mut data, key.len() as u16);
        data.extend_from_slice(key);

        // Deletion time: i32 local_deletion_time + i64 marked_for_delete_at.
        // LIVE = local_deletion_time=MAX_VALUE, marked_for_delete_at=MIN_VALUE.
        write_i32(&mut data, i32::MAX); // local_deletion_time (live)
        write_i64(&mut data, i64::MIN); // marked_for_delete_at (live)

        // Row 1: flags = HAS_CLUSTERING | HAS_TIMESTAMP | HAS_ALL_COLUMNS
        let flags: u8 = HAS_CLUSTERING | HAS_TIMESTAMP | HAS_ALL_COLUMNS;
        data.push(flags);

        // Clustering key: varint length + bytes.
        let ck = b"ck1";
        write_uvarint(&mut data, ck.len() as u64);
        data.extend_from_slice(ck);

        // Timestamp delta (unsigned varint): 42.
        // Actual timestamp = min_timestamp + 42 = 1_000_042.
        write_uvarint(&mut data, 42);

        // Cell: flags + value.
        // Cell flags: HAS_VALUE | USE_ROW_TIMESTAMP.
        let cell_flags: u8 = CELL_HAS_VALUE | CELL_USE_ROW_TIMESTAMP;
        data.push(cell_flags);

        // Value: varint length + bytes.
        let val = b"world";
        write_uvarint(&mut data, val.len() as u64);
        data.extend_from_slice(val);

        // END_OF_PARTITION.
        data.push(END_OF_PARTITION);

        data
    }

    #[test]
    fn read_minimal_partition() {
        let data = build_minimal_partition();
        let reader = DataReader::new(data, test_header());

        let (partition, next_offset) = reader.read_partition(0).unwrap();

        // Verify key.
        assert_eq!(partition.key.key.as_bytes(), b"hello");

        // Verify deletion time is LIVE.
        assert!(partition.deletion.is_live());

        // Verify no static row.
        assert!(partition.static_row.is_none());

        // Verify one row.
        assert_eq!(partition.rows.len(), 1);

        let row = &partition.rows[0];
        assert_eq!(row.clustering, b"ck1");
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_042);
        assert!(row.deletion.is_live());

        // Verify one cell.
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].0, 0); // column index
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"world".as_slice()));
        assert_eq!(row.cells[0].1.timestamp, 1_000_042); // uses row timestamp

        // next_offset should be at the end of the data.
        assert_eq!(next_offset, build_minimal_partition().len() as u64);
    }

    #[test]
    fn read_partition_with_deletion() {
        let mut data = Vec::new();

        // Key "del".
        write_u16(&mut data, 3);
        data.extend_from_slice(b"del");

        // Deletion: local_deletion_time=1700000000, marked_for_delete_at=999.
        write_i32(&mut data, 1_700_000_000);
        write_i64(&mut data, 999);

        // END_OF_PARTITION (no rows).
        data.push(END_OF_PARTITION);

        let reader = DataReader::new(data, test_header());
        let (partition, _) = reader.read_partition(0).unwrap();

        assert_eq!(partition.key.key.as_bytes(), b"del");
        assert!(!partition.deletion.is_live());
        assert_eq!(partition.deletion.local_deletion_time, 1_700_000_000);
        assert_eq!(partition.deletion.marked_for_delete_at, 999);
        assert!(partition.rows.is_empty());
    }

    #[test]
    fn read_partition_multiple_rows() {
        let mut data = Vec::new();

        // Key "multi".
        write_u16(&mut data, 5);
        data.extend_from_slice(b"multi");

        // Live deletion.
        write_i32(&mut data, i32::MAX);
        write_i64(&mut data, i64::MIN);

        // Row 1: clustering "a", timestamp delta=0, one cell.
        let flags1: u8 = HAS_CLUSTERING | HAS_TIMESTAMP | HAS_ALL_COLUMNS;
        data.push(flags1);
        write_uvarint(&mut data, 1); // clustering length
        data.push(b'a');
        write_uvarint(&mut data, 0); // timestamp delta

        // Cell 1: value "v1".
        data.push(CELL_HAS_VALUE | CELL_USE_ROW_TIMESTAMP);
        write_uvarint(&mut data, 2);
        data.extend_from_slice(b"v1");

        // Row 2: clustering "b", timestamp delta=100, one cell.
        let flags2: u8 = HAS_CLUSTERING | HAS_TIMESTAMP | HAS_ALL_COLUMNS;
        data.push(flags2);
        write_uvarint(&mut data, 1); // clustering length
        data.push(b'b');
        write_uvarint(&mut data, 100); // timestamp delta

        // Cell 2: value "v2".
        data.push(CELL_HAS_VALUE | CELL_USE_ROW_TIMESTAMP);
        write_uvarint(&mut data, 2);
        data.extend_from_slice(b"v2");

        // END_OF_PARTITION.
        data.push(END_OF_PARTITION);

        let reader = DataReader::new(data, test_header());
        let (partition, _) = reader.read_partition(0).unwrap();

        assert_eq!(partition.rows.len(), 2);
        assert_eq!(partition.rows[0].clustering, b"a");
        assert_eq!(partition.rows[0].primary_key_liveness.timestamp, 1_000_000);
        assert_eq!(partition.rows[1].clustering, b"b");
        assert_eq!(partition.rows[1].primary_key_liveness.timestamp, 1_000_100);
    }

    #[test]
    fn read_partition_at_offset() {
        // Put some junk before the actual partition.
        let mut data = vec![0xFF; 20];
        let partition_data = build_minimal_partition();
        let offset = data.len() as u64;
        data.extend_from_slice(&partition_data);

        let reader = DataReader::new(data, test_header());
        let (partition, _) = reader.read_partition(offset).unwrap();

        assert_eq!(partition.key.key.as_bytes(), b"hello");
        assert_eq!(partition.rows.len(), 1);
    }

    #[test]
    fn read_row_with_deletion() {
        let mut data = Vec::new();

        // Key "rd".
        write_u16(&mut data, 2);
        data.extend_from_slice(b"rd");

        // Live partition deletion.
        write_i32(&mut data, i32::MAX);
        write_i64(&mut data, i64::MIN);

        // Row with deletion: flags = HAS_DELETION | HAS_ALL_COLUMNS.
        let flags: u8 = HAS_DELETION | HAS_ALL_COLUMNS;
        data.push(flags);

        // Row deletion.
        write_i32(&mut data, 1_700_000_000);
        write_i64(&mut data, 12345);

        // Cell (tombstone).
        data.push(CELL_IS_DELETED);

        // END_OF_PARTITION.
        data.push(END_OF_PARTITION);

        let reader = DataReader::new(data, test_header());
        let (partition, _) = reader.read_partition(0).unwrap();

        assert_eq!(partition.rows.len(), 1);
        let row = &partition.rows[0];
        assert!(!row.deletion.is_live());
        assert_eq!(row.deletion.marked_for_delete_at, 12345);

        // Cell should be a tombstone.
        assert!(row.cells[0].1.is_tombstone());
    }

    #[test]
    fn read_row_with_ttl() {
        let mut data = Vec::new();

        // Key "t".
        write_u16(&mut data, 1);
        data.push(b't');

        // Live partition deletion.
        write_i32(&mut data, i32::MAX);
        write_i64(&mut data, i64::MIN);

        // Row: HAS_CLUSTERING | HAS_TIMESTAMP | HAS_TTL | HAS_ALL_COLUMNS.
        let flags: u8 = HAS_CLUSTERING | HAS_TIMESTAMP | HAS_TTL | HAS_ALL_COLUMNS;
        data.push(flags);

        // Clustering.
        write_uvarint(&mut data, 1);
        data.push(b'c');

        // Timestamp delta = 10.
        write_uvarint(&mut data, 10);

        // TTL delta = 3600, local_deletion_time delta = 1700000000.
        write_uvarint(&mut data, 3600);
        write_uvarint(&mut data, 1_700_000_000);

        // Cell with value.
        data.push(CELL_HAS_VALUE | CELL_USE_ROW_TIMESTAMP);
        write_uvarint(&mut data, 3);
        data.extend_from_slice(b"val");

        // END_OF_PARTITION.
        data.push(END_OF_PARTITION);

        let reader = DataReader::new(data, test_header());
        let (partition, _) = reader.read_partition(0).unwrap();

        let row = &partition.rows[0];
        assert!(row.primary_key_liveness.has_ttl());
        assert_eq!(row.primary_key_liveness.ttl, 3600); // min_ttl(0) + 3600
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_010);
    }

    #[test]
    fn read_cell_with_own_timestamp() {
        let mut data = Vec::new();

        // Key "ct".
        write_u16(&mut data, 2);
        data.extend_from_slice(b"ct");

        // Live partition deletion.
        write_i32(&mut data, i32::MAX);
        write_i64(&mut data, i64::MIN);

        // Row: HAS_ALL_COLUMNS only (no row-level timestamp).
        let flags: u8 = HAS_ALL_COLUMNS;
        data.push(flags);

        // Cell with its own timestamp: HAS_VALUE | HAS_TIMESTAMP.
        data.push(CELL_HAS_VALUE | CELL_HAS_TIMESTAMP);
        write_uvarint(&mut data, 500); // timestamp delta
        write_uvarint(&mut data, 2); // value length
        data.extend_from_slice(b"ok");

        // END_OF_PARTITION.
        data.push(END_OF_PARTITION);

        let reader = DataReader::new(data, test_header());
        let (partition, _) = reader.read_partition(0).unwrap();

        let cell = &partition.rows[0].cells[0].1;
        assert_eq!(cell.timestamp, 1_000_500); // min_timestamp + 500
        assert_eq!(cell.value.as_deref(), Some(b"ok".as_slice()));
    }

    #[test]
    fn read_row_with_explicit_column_count() {
        // Test a row that does NOT have HAS_ALL_COLUMNS set.
        let header = SerializationHeader {
            min_timestamp: 0,
            min_local_deletion_time: 0,
            min_ttl: 0,
            key_type: "k".to_string(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![
                (b"a".to_vec(), "T".to_string()),
                (b"b".to_vec(), "T".to_string()),
                (b"c".to_vec(), "T".to_string()),
            ],
        };

        let mut data = Vec::new();

        // Key "x".
        write_u16(&mut data, 1);
        data.push(b'x');

        // Live partition deletion.
        write_i32(&mut data, i32::MAX);
        write_i64(&mut data, i64::MIN);

        // Row: no HAS_ALL_COLUMNS -> explicit column count follows.
        data.push(0u8); // flags = 0 (no special bits except no END_OF_PARTITION)

        // Column count = 2 (only 2 of the 3 columns present).
        write_uvarint(&mut data, 2);

        // Cell 1: simple value.
        data.push(CELL_HAS_VALUE | CELL_USE_ROW_TIMESTAMP);
        write_uvarint(&mut data, 1);
        data.push(b'A');

        // Cell 2: simple value.
        data.push(CELL_HAS_VALUE | CELL_USE_ROW_TIMESTAMP);
        write_uvarint(&mut data, 1);
        data.push(b'B');

        // END_OF_PARTITION.
        data.push(END_OF_PARTITION);

        let reader = DataReader::new(data, header);
        let (partition, _) = reader.read_partition(0).unwrap();

        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].cells.len(), 2);
    }

    #[test]
    fn decorate_key_computes_token() {
        let dk = decorate_key(b"test".to_vec());
        assert_eq!(dk.key.as_bytes(), b"test");
        assert_eq!(dk.token, Token::from_key(b"test"));
    }
}
