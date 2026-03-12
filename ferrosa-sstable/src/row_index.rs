//! Row index (Rows.db) reader.
//!
//! Each partition's row index section contains metadata followed by a trie
//! of clustering key prefixes pointing to data file offsets within the partition.
//!
//! For Part B, this module implements a simple reader that extracts the
//! partition data position and promoted index length from a row index entry.
//! Full per-partition trie walking is deferred to a later phase.
//!
//! # Entry format
//!
//! At the offset pointed to by the partition index, a row index entry stores:
//!
//! ```text
//! [partition_position: unsigned varint]  — position of the partition in Data.db
//! [promoted_index_length: unsigned varint] — length of the promoted index data
//! [promoted index data: promoted_index_length bytes] — trie data (skipped for now)
//! ```
//!
//! Reference: `RowIndexReader.java`, `BtiTableWriter.java`

use ferrosa_common::Result;

use crate::io::ReadAt;
use crate::varint;

/// A parsed row index entry header.
///
/// Contains the partition's position in Data.db and the length of the
/// promoted index (per-partition clustering trie). The promoted index
/// data itself is not parsed in this phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowIndexEntry {
    /// Absolute position of the partition in Data.db.
    pub partition_position: u64,
    /// Length in bytes of the promoted index data that follows this header.
    pub promoted_index_length: u64,
}

/// Read a row index entry at the given offset.
///
/// Returns the parsed entry and the total number of bytes consumed
/// (header only, not including the promoted index data).
pub fn read_row_index_entry(reader: &impl ReadAt, offset: u64) -> Result<(RowIndexEntry, usize)> {
    let (partition_position, n1) = varint::read_unsigned_vint_at(reader, offset)?;
    let (promoted_index_length, n2) = varint::read_unsigned_vint_at(reader, offset + n1 as u64)?;

    let entry = RowIndexEntry {
        partition_position,
        promoted_index_length,
    };

    Ok((entry, n1 + n2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;

    /// Build a row index entry in memory: two unsigned varints.
    fn build_entry(partition_position: u64, promoted_index_length: u64) -> Vec<u8> {
        let mut buf = [0u8; 9];
        let mut data = Vec::new();

        let n = varint::write_unsigned_vint(&mut buf, partition_position);
        data.extend_from_slice(&buf[..n]);

        let n = varint::write_unsigned_vint(&mut buf, promoted_index_length);
        data.extend_from_slice(&buf[..n]);

        data
    }

    #[test]
    fn read_entry_small_values() {
        let data = build_entry(100, 50);
        let (entry, consumed) = read_row_index_entry(&data, 0).unwrap();

        assert_eq!(entry.partition_position, 100);
        assert_eq!(entry.promoted_index_length, 50);
        assert_eq!(consumed, 2); // both fit in 1 byte each
    }

    #[test]
    fn read_entry_large_values() {
        let data = build_entry(100_000, 50_000);
        let (entry, consumed) = read_row_index_entry(&data, 0).unwrap();

        assert_eq!(entry.partition_position, 100_000);
        assert_eq!(entry.promoted_index_length, 50_000);
        assert!(consumed > 2);
    }

    #[test]
    fn read_entry_zero_promoted_index() {
        let data = build_entry(42, 0);
        let (entry, _) = read_row_index_entry(&data, 0).unwrap();

        assert_eq!(entry.partition_position, 42);
        assert_eq!(entry.promoted_index_length, 0);
    }

    #[test]
    fn read_entry_at_offset() {
        // Put some padding before the entry.
        let mut data = vec![0xFF; 10];
        let entry_data = build_entry(500, 200);
        data.extend_from_slice(&entry_data);

        let (entry, _) = read_row_index_entry(&data, 10).unwrap();

        assert_eq!(entry.partition_position, 500);
        assert_eq!(entry.promoted_index_length, 200);
    }

    #[test]
    fn read_entry_max_single_byte_values() {
        // 127 is the max single-byte varint value
        let data = build_entry(127, 127);
        let (entry, consumed) = read_row_index_entry(&data, 0).unwrap();

        assert_eq!(entry.partition_position, 127);
        assert_eq!(entry.promoted_index_length, 127);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn read_entry_two_byte_boundary() {
        // 128 requires 2 bytes in unsigned varint
        let data = build_entry(128, 128);
        let (entry, consumed) = read_row_index_entry(&data, 0).unwrap();

        assert_eq!(entry.partition_position, 128);
        assert_eq!(entry.promoted_index_length, 128);
        assert_eq!(consumed, 4); // 2 bytes each
    }

    #[test]
    fn read_entry_very_large_position() {
        let large_pos = 1_000_000_000_000u64; // ~1 TB offset
        let data = build_entry(large_pos, 4096);
        let (entry, _) = read_row_index_entry(&data, 0).unwrap();

        assert_eq!(entry.partition_position, large_pos);
        assert_eq!(entry.promoted_index_length, 4096);
    }
}
