//! Partition index (Partitions.db) reader.
//!
//! The partition index is an on-disk trie mapping byte-ordered partition key
//! prefixes to positions in the data or row index file. The trie is built by
//! [`crate::trie::builder::TrieBuilder`] and traversed by
//! [`crate::trie::walker`].
//!
//! # File layout
//!
//! ```text
//! [trie nodes ...]
//! [key_bounds: u16-length-prefixed smallest key + u16-length-prefixed largest key]
//! [footer: 3 big-endian i64s]
//!   key_bounds_offset (i64)
//!   key_count         (i64)
//!   root_pos          (i64)
//! ```
//!
//! Reference: `PartitionIndex.java`, `BtiTableWriter.java`

use ferrosa_common::{Error, Result};

use crate::io::ReadAt;
use crate::trie::walker;

/// Footer size: 3 big-endian i64 values = 24 bytes.
const FOOTER_SIZE: u64 = 24;

/// Partition index reader for Partitions.db.
///
/// Wraps a [`ReadAt`] source and provides lookup of encoded partition keys
/// in the trie, returning their position in the data or row index file.
pub struct PartitionIndex<R: ReadAt> {
    reader: R,
    root_pos: u64,
    key_count: u64,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
}

/// Result of looking up a partition key in the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionLookup {
    /// The partition has a row index entry at the given position in Rows.db.
    RowIndex { position: u64 },
    /// The partition data is directly at the given position in Data.db.
    DataDirect { position: u64 },
    /// The partition was not found in the index.
    NotFound,
}

impl<R: ReadAt> PartitionIndex<R> {
    /// Open a partition index from the given reader.
    ///
    /// Reads the footer (last 24 bytes) to obtain the root position, key
    /// count, and key bounds offset, then reads the key bounds section.
    pub fn open(reader: R) -> Result<Self> {
        let file_len = reader.len()?;
        if file_len < FOOTER_SIZE {
            return Err(Error::InvalidFormat(
                "partition index too small for footer".into(),
            ));
        }

        // Read footer: 3 big-endian i64s.
        let footer_offset = file_len - FOOTER_SIZE;
        let mut footer = [0u8; 24];
        reader.read_exact_at(&mut footer, footer_offset)?;

        let key_bounds_offset = i64::from_be_bytes(footer[0..8].try_into().unwrap());
        let key_count = i64::from_be_bytes(footer[8..16].try_into().unwrap());
        let root_pos = i64::from_be_bytes(footer[16..24].try_into().unwrap());

        if key_bounds_offset < 0 || key_count < 0 || root_pos < 0 {
            return Err(Error::InvalidFormat(
                "partition index footer has negative values".into(),
            ));
        }

        // Read key bounds: two u16-length-prefixed keys.
        let (smallest_key, bytes_read) =
            read_short_length_prefixed(&reader, key_bounds_offset as u64)?;
        let (largest_key, _) =
            read_short_length_prefixed(&reader, key_bounds_offset as u64 + bytes_read as u64)?;

        Ok(PartitionIndex {
            reader,
            root_pos: root_pos as u64,
            key_count: key_count as u64,
            smallest_key,
            largest_key,
        })
    }

    /// Look up a byte-comparable encoded key in the partition index trie.
    ///
    /// If the trie payload includes a hash byte, it is compared against
    /// `filter_hash`. If they do not match, [`PartitionLookup::NotFound`]
    /// is returned (Bloom filter rejection).
    ///
    /// The payload encodes an `idxpos` value:
    /// - If `idxpos >= 0`: the partition has a row index entry at that position
    ///   in Rows.db ([`PartitionLookup::RowIndex`]).
    /// - If `idxpos < 0`: the partition data is directly in Data.db at position
    ///   `!idxpos` (bitwise NOT) ([`PartitionLookup::DataDirect`]).
    pub fn lookup_raw(
        &self,
        encoded_key: &[u8],
        filter_hash: Option<u8>,
    ) -> Result<PartitionLookup> {
        let result = walker::lookup_payload(&self.reader, self.root_pos, encoded_key)?;

        match result {
            Some((payload_hash, idxpos)) => {
                // If both the payload and query have a hash byte, compare them.
                if let (Some(ph), Some(fh)) = (payload_hash, filter_hash) {
                    if ph != fh {
                        return Ok(PartitionLookup::NotFound);
                    }
                }

                if idxpos >= 0 {
                    Ok(PartitionLookup::RowIndex {
                        position: idxpos as u64,
                    })
                } else {
                    Ok(PartitionLookup::DataDirect {
                        position: !idxpos as u64,
                    })
                }
            }
            None => Ok(PartitionLookup::NotFound),
        }
    }

    /// Returns the number of partitions in the index.
    pub fn key_count(&self) -> u64 {
        self.key_count
    }

    /// Returns the smallest key stored in the key bounds section.
    pub fn smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }

    /// Returns the largest key stored in the key bounds section.
    pub fn largest_key(&self) -> &[u8] {
        &self.largest_key
    }
}

/// Read a u16-length-prefixed byte sequence from the reader at the given offset.
///
/// Returns `(bytes, total_bytes_consumed)` where `total_bytes_consumed`
/// includes the 2-byte length prefix.
fn read_short_length_prefixed(reader: &impl ReadAt, offset: u64) -> Result<(Vec<u8>, usize)> {
    let mut len_buf = [0u8; 2];
    reader.read_exact_at(&mut len_buf, offset)?;
    let len = u16::from_be_bytes(len_buf) as usize;

    let mut data = vec![0u8; len];
    reader.read_exact_at(&mut data, offset + 2)?;

    Ok((data, 2 + len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::builder::{TrieBuilder, TriePayload};

    /// Build a complete partition index file in memory:
    /// 1. Trie data from the builder
    /// 2. Key bounds section (u16-prefixed smallest + largest keys)
    /// 3. Footer (3 i64s: key_bounds_offset, key_count, root_pos)
    fn build_test_partition_index(
        keys: &[(&[u8], TriePayload)],
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<u8> {
        let mut builder = TrieBuilder::new();
        for (key, payload) in keys {
            builder.add(key, payload.clone()).unwrap();
        }
        let (trie_data, root_pos) = builder.finish().unwrap();

        let mut file = trie_data;

        // Key bounds section starts right after the trie data.
        let key_bounds_offset = file.len() as i64;

        // Write smallest key: u16 length + key bytes.
        file.extend_from_slice(&(smallest.len() as u16).to_be_bytes());
        file.extend_from_slice(smallest);

        // Write largest key: u16 length + key bytes.
        file.extend_from_slice(&(largest.len() as u16).to_be_bytes());
        file.extend_from_slice(largest);

        // Footer: key_bounds_offset, key_count, root_pos.
        let key_count = keys.len() as i64;
        file.extend_from_slice(&key_bounds_offset.to_be_bytes());
        file.extend_from_slice(&key_count.to_be_bytes());
        file.extend_from_slice(&(root_pos as i64).to_be_bytes());

        file
    }

    #[test]
    fn open_reads_footer_and_key_bounds() {
        let keys = vec![
            (
                b"aaa".as_slice(),
                TriePayload {
                    hash: None,
                    position: 100,
                },
            ),
            (
                b"bbb".as_slice(),
                TriePayload {
                    hash: None,
                    position: 200,
                },
            ),
            (
                b"ccc".as_slice(),
                TriePayload {
                    hash: None,
                    position: 300,
                },
            ),
        ];

        let file = build_test_partition_index(&keys, b"smallest_key", b"largest_key");
        let index = PartitionIndex::open(file).unwrap();

        assert_eq!(index.key_count(), 3);
        assert_eq!(index.smallest_key(), b"smallest_key");
        assert_eq!(index.largest_key(), b"largest_key");
    }

    #[test]
    fn lookup_existing_keys() {
        let keys = vec![
            (
                b"key_a".as_slice(),
                TriePayload {
                    hash: None,
                    position: 1000,
                },
            ),
            (
                b"key_b".as_slice(),
                TriePayload {
                    hash: None,
                    position: 2000,
                },
            ),
        ];

        let file = build_test_partition_index(&keys, b"key_a", b"key_b");
        let index = PartitionIndex::open(file).unwrap();

        // Positive idxpos -> RowIndex.
        let result = index.lookup_raw(b"key_a", None).unwrap();
        assert_eq!(result, PartitionLookup::RowIndex { position: 1000 });

        let result = index.lookup_raw(b"key_b", None).unwrap();
        assert_eq!(result, PartitionLookup::RowIndex { position: 2000 });
    }

    #[test]
    fn lookup_missing_key_returns_not_found() {
        let keys = vec![(
            b"exists".as_slice(),
            TriePayload {
                hash: None,
                position: 42,
            },
        )];

        let file = build_test_partition_index(&keys, b"exists", b"exists");
        let index = PartitionIndex::open(file).unwrap();

        let result = index.lookup_raw(b"missing", None).unwrap();
        assert_eq!(result, PartitionLookup::NotFound);
    }

    #[test]
    fn lookup_data_direct_for_negative_idxpos() {
        // Negative idxpos means DataDirect: position = !idxpos.
        // For position P in Data.db, idxpos = !P = -(P+1).
        // So for data position 500, idxpos = -501.
        let keys = vec![(
            b"direct".as_slice(),
            TriePayload {
                hash: None,
                position: -501, // !500 = -501
            },
        )];

        let file = build_test_partition_index(&keys, b"direct", b"direct");
        let index = PartitionIndex::open(file).unwrap();

        let result = index.lookup_raw(b"direct", None).unwrap();
        assert_eq!(result, PartitionLookup::DataDirect { position: 500 });
    }

    #[test]
    fn lookup_with_hash_match() {
        let keys = vec![(
            b"hashed".as_slice(),
            TriePayload {
                hash: Some(0xAB),
                position: 777,
            },
        )];

        let file = build_test_partition_index(&keys, b"hashed", b"hashed");
        let index = PartitionIndex::open(file).unwrap();

        // Hash matches -> found.
        let result = index.lookup_raw(b"hashed", Some(0xAB)).unwrap();
        assert_eq!(result, PartitionLookup::RowIndex { position: 777 });
    }

    #[test]
    fn lookup_with_hash_mismatch_returns_not_found() {
        let keys = vec![(
            b"hashed".as_slice(),
            TriePayload {
                hash: Some(0xAB),
                position: 777,
            },
        )];

        let file = build_test_partition_index(&keys, b"hashed", b"hashed");
        let index = PartitionIndex::open(file).unwrap();

        // Hash mismatch -> NotFound (Bloom filter rejection).
        let result = index.lookup_raw(b"hashed", Some(0xCD)).unwrap();
        assert_eq!(result, PartitionLookup::NotFound);
    }

    #[test]
    fn lookup_with_no_filter_hash_ignores_payload_hash() {
        let keys = vec![(
            b"hashed".as_slice(),
            TriePayload {
                hash: Some(0xAB),
                position: 777,
            },
        )];

        let file = build_test_partition_index(&keys, b"hashed", b"hashed");
        let index = PartitionIndex::open(file).unwrap();

        // No filter hash provided -> hash check is skipped.
        let result = index.lookup_raw(b"hashed", None).unwrap();
        assert_eq!(result, PartitionLookup::RowIndex { position: 777 });
    }

    #[test]
    fn open_rejects_too_small_file() {
        let tiny: Vec<u8> = vec![0; 10];
        let result = PartitionIndex::open(tiny);
        assert!(result.is_err());
    }

    #[test]
    fn read_short_length_prefixed_basic() {
        let mut data = Vec::new();
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&3u16.to_be_bytes());
        data.extend_from_slice(b"bye");

        let (bytes, consumed) = read_short_length_prefixed(&data, 0).unwrap();
        assert_eq!(bytes, b"hello");
        assert_eq!(consumed, 7);

        let (bytes2, consumed2) = read_short_length_prefixed(&data, 7).unwrap();
        assert_eq!(bytes2, b"bye");
        assert_eq!(consumed2, 5);
    }

    #[test]
    fn many_keys_lookup() {
        let mut keys: Vec<(Vec<u8>, TriePayload)> = Vec::new();
        for i in 0..50u32 {
            keys.push((
                format!("partition_{i:04}").into_bytes(),
                TriePayload {
                    hash: Some((i & 0xFF) as u8),
                    position: i as i64 * 100,
                },
            ));
        }

        let borrowed: Vec<(&[u8], TriePayload)> = keys
            .iter()
            .map(|(k, p)| (k.as_slice(), p.clone()))
            .collect();
        let file = build_test_partition_index(
            &borrowed,
            keys.first().unwrap().0.as_slice(),
            keys.last().unwrap().0.as_slice(),
        );
        let index = PartitionIndex::open(file).unwrap();

        assert_eq!(index.key_count(), 50);

        for (i, (key, _)) in keys.iter().enumerate() {
            let result = index.lookup_raw(key, Some(i as u8)).unwrap();
            assert_eq!(
                result,
                PartitionLookup::RowIndex {
                    position: i as u64 * 100
                },
                "lookup failed for key {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }
}
