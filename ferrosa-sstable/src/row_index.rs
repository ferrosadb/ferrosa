//! Rows.db reader for BTI SSTables.
//!
//! The row index file contains per-partition trie structures that map
//! clustering key byte-comparable prefixes to block offsets within each
//! partition's data in Data.db.
//!
//! Each partition's row index entry has a metadata footer:
//! - Partition key (u16 BE length + bytes)
//! - Data file position (i64 BE)
//! - Trie root offset within Rows.db (i64 BE)
//! - Block count (i32 BE)
//! - Deletion time: local_deletion_time (i32 BE) + marked_for_delete_at (i64 BE)
//!
//! The trie (preceding the footer) maps clustering key byte-comparable
//! prefixes to block offsets within the partition's data.

use crate::io::ReadAt;
use crate::trie::node;
use crate::trie::walker;
use ferrosa_common::{Error, Result};

/// A single partition's row index entry.
#[derive(Debug, Clone)]
pub struct RowIndexEntry {
    /// The partition key bytes.
    pub partition_key: Vec<u8>,
    /// Position in Data.db where this partition's data begins.
    pub data_position: u64,
    /// Position of the trie root in Rows.db (for clustering key lookups).
    pub trie_root: u64,
    /// Number of data blocks for this partition.
    pub block_count: u32,
    /// Local deletion time (seconds since epoch), or `i32::MAX` if live.
    pub local_deletion_time: i32,
    /// Marked-for-delete-at timestamp (microseconds), or `i64::MIN` if live.
    pub marked_for_delete_at: i64,
}

/// Size of the fixed-length portion of a row index entry footer
/// (after the variable-length partition key):
///   data_position (8) + trie_root (8) + block_count (4) +
///   local_deletion_time (4) + marked_for_delete_at (8) = 32 bytes
const FOOTER_FIXED_SIZE: usize = 8 + 8 + 4 + 4 + 8;

/// Reader for Rows.db.
pub struct RowIndex<R: ReadAt> {
    reader: R,
    entries: Vec<RowIndexEntry>,
}

impl<R: ReadAt> RowIndex<R> {
    /// Open a row index by reading entries from the given offsets.
    ///
    /// Each offset points to the start of a row index entry's metadata footer
    /// (partition key length prefix).
    pub fn open(reader: R, entry_offsets: &[u64]) -> Result<Self> {
        let mut entries = Vec::with_capacity(entry_offsets.len());
        for &offset in entry_offsets {
            entries.push(Self::read_entry(&reader, offset)?);
        }
        Ok(Self { reader, entries })
    }

    /// Read a single row index entry at the given offset.
    ///
    /// The offset points to the start of the metadata footer:
    ///   u16 BE (partition key length) + partition key bytes + fixed fields.
    pub fn read_entry(reader: &R, offset: u64) -> Result<RowIndexEntry> {
        // Read the partition key length (u16 BE).
        let mut len_buf = [0u8; 2];
        reader.read_exact_at(&mut len_buf, offset)?;
        let pk_len = u16::from_be_bytes(len_buf) as usize;

        // Read the partition key bytes + fixed footer in one read.
        let total = pk_len + FOOTER_FIXED_SIZE;
        let mut buf = vec![0u8; total];
        reader.read_exact_at(&mut buf, offset + 2)?;

        let partition_key = buf[..pk_len].to_vec();

        let fixed = &buf[pk_len..];
        let data_position = i64::from_be_bytes(fixed[0..8].try_into().unwrap()) as u64;
        let trie_root = i64::from_be_bytes(fixed[8..16].try_into().unwrap()) as u64;
        let block_count = i32::from_be_bytes(fixed[16..20].try_into().unwrap()) as u32;
        let local_deletion_time = i32::from_be_bytes(fixed[20..24].try_into().unwrap());
        let marked_for_delete_at = i64::from_be_bytes(fixed[24..32].try_into().unwrap());

        Ok(RowIndexEntry {
            partition_key,
            data_position,
            trie_root,
            block_count,
            local_deletion_time,
            marked_for_delete_at,
        })
    }

    /// Access the parsed row index entries.
    pub fn entries(&self) -> &[RowIndexEntry] {
        &self.entries
    }

    /// Look up a clustering key in a partition's trie.
    ///
    /// Uses the trie walker to find the block offset for `clustering_key`
    /// in the trie belonging to the entry at `entry_idx`.
    ///
    /// Returns `Ok(Some(block_offset))` if found, `Ok(None)` if the
    /// clustering key is not present in the trie.
    pub fn lookup_clustering(
        &self,
        entry_idx: usize,
        clustering_key: &[u8],
    ) -> Result<Option<u64>> {
        let entry = self.entries.get(entry_idx).ok_or_else(|| {
            Error::InvalidData(format!(
                "row index entry index {entry_idx} out of range (have {})",
                self.entries.len()
            ))
        })?;

        lookup_clustering_in_entry(&self.reader, entry, clustering_key)
    }
}

/// Look up a clustering key using an already-parsed row-index entry.
pub fn lookup_clustering_in_entry<R: ReadAt>(
    reader: &R,
    entry: &RowIndexEntry,
    clustering_key: &[u8],
) -> Result<Option<u64>> {
    let result = walker::lookup(reader, entry.trie_root, clustering_key)?;

    match result {
        walker::LookupResult::Found {
            payload_pb,
            payload_bytes,
        } => {
            // Row index trie payloads encode block offsets without a hash byte
            // (pb < 8 means no hash, the value is a signed integer).
            let (_hash, offset) = node::decode_payload(payload_pb, &payload_bytes)?;
            Ok(Some(offset as u64))
        }
        walker::LookupResult::NotFound => Ok(None),
    }
}

/// Serialize a [`RowIndexEntry`] into its on-disk footer representation.
///
/// This is useful for testing round-trips and for future write support.
pub fn serialize_entry(entry: &RowIndexEntry) -> Vec<u8> {
    let pk_len = entry.partition_key.len() as u16;
    let mut buf = Vec::with_capacity(2 + entry.partition_key.len() + FOOTER_FIXED_SIZE);

    buf.extend_from_slice(&pk_len.to_be_bytes());
    buf.extend_from_slice(&entry.partition_key);
    buf.extend_from_slice(&(entry.data_position as i64).to_be_bytes());
    buf.extend_from_slice(&(entry.trie_root as i64).to_be_bytes());
    buf.extend_from_slice(&(entry.block_count as i32).to_be_bytes());
    buf.extend_from_slice(&entry.local_deletion_time.to_be_bytes());
    buf.extend_from_slice(&entry.marked_for_delete_at.to_be_bytes());

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::builder::{TrieBuilder, TriePayload};

    /// Helper: create a default (live) row index entry with the given fields.
    fn make_entry(
        partition_key: &[u8],
        data_position: u64,
        trie_root: u64,
        block_count: u32,
    ) -> RowIndexEntry {
        RowIndexEntry {
            partition_key: partition_key.to_vec(),
            data_position,
            trie_root,
            block_count,
            local_deletion_time: i32::MAX,
            marked_for_delete_at: i64::MIN,
        }
    }

    #[test]
    fn serialize_and_read_single_entry() {
        let entry = make_entry(b"my_key", 1024, 0, 4);
        let data = serialize_entry(&entry);

        let parsed = RowIndex::read_entry(&data, 0).unwrap();

        assert_eq!(parsed.partition_key, b"my_key");
        assert_eq!(parsed.data_position, 1024);
        assert_eq!(parsed.trie_root, 0);
        assert_eq!(parsed.block_count, 4);
        assert_eq!(parsed.local_deletion_time, i32::MAX);
        assert_eq!(parsed.marked_for_delete_at, i64::MIN);
    }

    #[test]
    fn serialize_and_read_with_deletion_time() {
        let entry = RowIndexEntry {
            partition_key: b"deleted".to_vec(),
            data_position: 2048,
            trie_root: 100,
            block_count: 1,
            local_deletion_time: 1700000000,
            marked_for_delete_at: 1_700_000_000_000_000,
        };
        let data = serialize_entry(&entry);

        let parsed = RowIndex::read_entry(&data, 0).unwrap();

        assert_eq!(parsed.partition_key, b"deleted");
        assert_eq!(parsed.data_position, 2048);
        assert_eq!(parsed.trie_root, 100);
        assert_eq!(parsed.block_count, 1);
        assert_eq!(parsed.local_deletion_time, 1700000000);
        assert_eq!(parsed.marked_for_delete_at, 1_700_000_000_000_000);
    }

    #[test]
    fn read_multiple_entries() {
        let entry0 = make_entry(b"pk0", 0, 0, 1);
        let entry1 = make_entry(b"pk1", 4096, 0, 2);

        let data0 = serialize_entry(&entry0);
        let data1 = serialize_entry(&entry1);

        let mut combined = Vec::new();
        combined.extend_from_slice(&data0);
        let offset1 = combined.len() as u64;
        combined.extend_from_slice(&data1);

        let row_index = RowIndex::open(combined, &[0, offset1]).unwrap();

        assert_eq!(row_index.entries().len(), 2);
        assert_eq!(row_index.entries()[0].partition_key, b"pk0");
        assert_eq!(row_index.entries()[0].data_position, 0);
        assert_eq!(row_index.entries()[0].block_count, 1);
        assert_eq!(row_index.entries()[1].partition_key, b"pk1");
        assert_eq!(row_index.entries()[1].data_position, 4096);
        assert_eq!(row_index.entries()[1].block_count, 2);
    }

    #[test]
    fn read_entry_at_nonzero_offset() {
        let entry = make_entry(b"off", 512, 0, 3);
        let serialized = serialize_entry(&entry);

        // Prepend some garbage bytes.
        let mut data = vec![0xFFu8; 100];
        data.extend_from_slice(&serialized);

        let parsed = RowIndex::read_entry(&data, 100).unwrap();
        assert_eq!(parsed.partition_key, b"off");
        assert_eq!(parsed.data_position, 512);
        assert_eq!(parsed.block_count, 3);
    }

    #[test]
    fn read_entry_empty_partition_key() {
        let entry = make_entry(b"", 0, 0, 0);
        let data = serialize_entry(&entry);

        let parsed = RowIndex::read_entry(&data, 0).unwrap();
        assert!(parsed.partition_key.is_empty());
    }

    #[test]
    fn lookup_clustering_with_trie() {
        // Build a trie that maps clustering keys to block offsets.
        // Row index tries use no hash byte (hash = None).
        let mut builder = TrieBuilder::new();
        builder
            .add(
                b"cluster_a",
                TriePayload {
                    hash: None,
                    position: 0,
                },
            )
            .unwrap();
        builder
            .add(
                b"cluster_b",
                TriePayload {
                    hash: None,
                    position: 4096,
                },
            )
            .unwrap();
        builder
            .add(
                b"cluster_c",
                TriePayload {
                    hash: None,
                    position: 8192,
                },
            )
            .unwrap();

        let (trie_data, trie_root) = builder.finish().unwrap();

        // The trie data comes first, then the row index entry footer.
        let trie_len = trie_data.len() as u64;

        let entry = make_entry(b"pk", 0, trie_root, 3);
        let entry_bytes = serialize_entry(&entry);

        let mut combined = Vec::new();
        combined.extend_from_slice(&trie_data);
        combined.extend_from_slice(&entry_bytes);

        let entry_offset = trie_len;
        let row_index = RowIndex::open(combined, &[entry_offset]).unwrap();

        // Look up each clustering key.
        assert_eq!(
            row_index.lookup_clustering(0, b"cluster_a").unwrap(),
            Some(0)
        );
        assert_eq!(
            row_index.lookup_clustering(0, b"cluster_b").unwrap(),
            Some(4096)
        );
        assert_eq!(
            row_index.lookup_clustering(0, b"cluster_c").unwrap(),
            Some(8192)
        );

        // Key not present.
        assert_eq!(row_index.lookup_clustering(0, b"cluster_d").unwrap(), None);
    }

    #[test]
    fn lookup_clustering_entry_out_of_range() {
        let entry = make_entry(b"pk", 0, 0, 1);
        let data = serialize_entry(&entry);

        let row_index = RowIndex::open(data, &[0]).unwrap();

        let err = row_index.lookup_clustering(5, b"key").unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn lookup_clustering_not_found_in_empty_trie() {
        // Build an empty trie (no keys).
        let builder = TrieBuilder::new();
        let (trie_data, _trie_root) = builder.finish().unwrap();
        assert!(trie_data.is_empty());

        // With an empty trie, trie_root is 0 and there are no nodes.
        // Build a minimal trie with a single entry that won't match.
        let mut builder2 = TrieBuilder::new();
        builder2
            .add(
                b"x",
                TriePayload {
                    hash: None,
                    position: 42,
                },
            )
            .unwrap();
        let (trie_data, trie_root) = builder2.finish().unwrap();

        let entry = make_entry(b"pk", 0, trie_root, 1);
        let entry_bytes = serialize_entry(&entry);

        let mut combined = Vec::new();
        combined.extend_from_slice(&trie_data);
        combined.extend_from_slice(&entry_bytes);

        let row_index = RowIndex::open(combined, &[trie_data.len() as u64]).unwrap();

        // "y" is not in the trie.
        assert_eq!(row_index.lookup_clustering(0, b"y").unwrap(), None);

        // "x" should be found.
        assert_eq!(row_index.lookup_clustering(0, b"x").unwrap(), Some(42));
    }

    #[test]
    fn round_trip_large_values() {
        let entry = RowIndexEntry {
            partition_key: vec![0xAB; 300],
            data_position: u64::MAX / 2,
            trie_root: 0x7FFF_FFFF_FFFF_FFFF,
            block_count: u32::MAX / 2,
            local_deletion_time: -1,
            marked_for_delete_at: i64::MAX,
        };
        let data = serialize_entry(&entry);
        let parsed = RowIndex::read_entry(&data, 0).unwrap();

        assert_eq!(parsed.partition_key.len(), 300);
        assert_eq!(parsed.data_position, entry.data_position);
        assert_eq!(parsed.trie_root, entry.trie_root);
        assert_eq!(parsed.block_count, entry.block_count);
        assert_eq!(parsed.local_deletion_time, -1);
        assert_eq!(parsed.marked_for_delete_at, i64::MAX);
    }
}
