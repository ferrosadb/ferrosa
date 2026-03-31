//! Partitions.db reader for the BTI partition index.
//!
//! The partition index file contains:
//! 1. A trie mapping byte-comparable encoded partition keys to payload (hash, idxpos)
//! 2. A footer (last 24 bytes): 3 big-endian i64s:
//!    - `key_bounds_offset`: offset to the key bounds section
//!    - `key_count`: number of partitions
//!    - `root_pos`: file position of the trie root node
//! 3. Key bounds section (at `key_bounds_offset`): two short-length-prefixed keys
//!    (smallest, largest)
//!
//! # Lookup
//!
//! The lookup encodes a `DecoratedKey` to its byte-comparable form, walks the
//! trie, verifies the hash byte, and interprets the idxpos:
//! - Positive idxpos: row index position (`RowIndex`)
//! - Negative idxpos: direct data pointer via bitwise NOT (`DataDirect`)
//!
//! Reference: Cassandra's `PartitionIndex.java`, `BtiTableReader.java`

use crate::io::ReadAt;
use crate::trie::node;
use crate::trie::walker;
use crate::{byte_comparable, trie::walker::LookupResult};
use ferrosa_common::{DecoratedKey, Error, Result};

/// Footer size: 3 x i64 = 24 bytes.
const FOOTER_SIZE: u64 = 24;

/// Result of a partition index lookup.
#[derive(Debug)]
pub enum PartitionLookup {
    /// Partition found; row index position for further row-level lookups.
    RowIndex { position: u64 },
    /// Partition found; direct data file position (no row index indirection).
    DataDirect { position: u64 },
    /// Partition not found in the index.
    NotFound,
}

/// Reader for a Partitions.db file.
pub struct PartitionIndex<R: ReadAt> {
    reader: R,
    root_pos: u64,
    key_count: u64,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
}

/// Read a short-length-prefixed byte array: u16 big-endian length + that many bytes.
/// Advances `pos` past the consumed bytes.
fn read_short_length_prefixed<R: ReadAt>(reader: &R, pos: &mut u64) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 2];
    reader.read_exact_at(&mut len_buf, *pos)?;
    let len = u16::from_be_bytes(len_buf) as usize;
    *pos += 2;

    let mut data = vec![0u8; len];
    reader.read_exact_at(&mut data, *pos)?;
    *pos += len as u64;

    Ok(data)
}

impl<R: ReadAt> PartitionIndex<R> {
    /// Open a partition index from a reader.
    ///
    /// Reads the footer (last 24 bytes) to extract `key_bounds_offset`,
    /// `key_count`, and `root_pos`, then reads the key bounds section.
    pub fn open(reader: R) -> Result<Self> {
        let file_len = reader.len()?;
        if file_len < FOOTER_SIZE {
            return Err(Error::InvalidFormat(
                "partition index file too small for footer".into(),
            ));
        }

        // Read footer: 3 big-endian i64s
        let footer_offset = file_len - FOOTER_SIZE;
        let mut footer_buf = [0u8; 24];
        reader.read_exact_at(&mut footer_buf, footer_offset)?;

        let key_bounds_offset = i64::from_be_bytes(footer_buf[0..8].try_into().unwrap());
        let key_count = i64::from_be_bytes(footer_buf[8..16].try_into().unwrap());
        let root_pos = i64::from_be_bytes(footer_buf[16..24].try_into().unwrap());

        // Read key bounds
        let mut bounds_pos = key_bounds_offset as u64;
        let smallest_key = read_short_length_prefixed(&reader, &mut bounds_pos)?;
        let largest_key = read_short_length_prefixed(&reader, &mut bounds_pos)?;

        Ok(PartitionIndex {
            reader,
            root_pos: root_pos as u64,
            key_count: key_count as u64,
            smallest_key,
            largest_key,
        })
    }

    /// Look up a partition key in the index.
    ///
    /// Encodes the key to byte-comparable form, walks the trie, verifies
    /// the hash byte, and interprets the idxpos.
    pub fn lookup(&self, key: &DecoratedKey) -> Result<PartitionLookup> {
        let encoded = byte_comparable::encode(key);

        match walker::lookup(&self.reader, self.root_pos, &encoded)? {
            LookupResult::Found {
                payload_pb,
                payload_bytes,
            } => {
                let (hash_opt, idxpos) = node::decode_payload(payload_pb, &payload_bytes)?;

                // Verify hash if present
                if let Some(hash) = hash_opt {
                    let (_h1, h2) = key.filter_hash();
                    let expected_hash = (h2 & 0xFF) as u8;
                    if hash != expected_hash {
                        return Ok(PartitionLookup::NotFound);
                    }
                }

                // Interpret idxpos: negative means direct data pointer (bitwise NOT)
                if idxpos < 0 {
                    Ok(PartitionLookup::DataDirect {
                        position: !idxpos as u64,
                    })
                } else {
                    Ok(PartitionLookup::RowIndex {
                        position: idxpos as u64,
                    })
                }
            }
            LookupResult::NotFound => Ok(PartitionLookup::NotFound),
        }
    }

    /// Returns the number of partitions in the index.
    pub fn key_count(&self) -> u64 {
        self.key_count
    }

    /// Returns the file size of the partition index in bytes.
    pub fn file_size(&self) -> u64 {
        self.reader.len().unwrap_or(0)
    }

    /// Returns the smallest key in the index (raw bytes, short-length-prefixed value).
    pub fn smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }

    /// Returns the largest key in the index (raw bytes, short-length-prefixed value).
    pub fn largest_key(&self) -> &[u8] {
        &self.largest_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie::builder::{TrieBuilder, TriePayload};
    use ferrosa_common::{PartitionKey, Token};

    /// Build a partition index file from a trie + key bounds + footer.
    fn build_partition_index(
        trie_data: &[u8],
        root_pos: u64,
        key_count: u64,
        smallest: &[u8],
        largest: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();

        // Trie data
        buf.extend_from_slice(trie_data);

        // Key bounds section
        let key_bounds_offset = buf.len() as i64;
        // smallest key: u16 len + bytes
        buf.extend_from_slice(&(smallest.len() as u16).to_be_bytes());
        buf.extend_from_slice(smallest);
        // largest key: u16 len + bytes
        buf.extend_from_slice(&(largest.len() as u16).to_be_bytes());
        buf.extend_from_slice(largest);

        // Footer: 3 big-endian i64s
        buf.extend_from_slice(&key_bounds_offset.to_be_bytes());
        buf.extend_from_slice(&(key_count as i64).to_be_bytes());
        buf.extend_from_slice(&(root_pos as i64).to_be_bytes());

        buf
    }

    #[test]
    fn open_reads_footer_and_key_bounds() {
        // Build a minimal trie: single leaf node with payload
        let trie_data = vec![
            0x08, // PayloadOnly, pb=8
            0xAA, // hash byte
            0x42, // idxpos = 0x42
        ];
        let root_pos = 0u64;
        let smallest = b"aaa";
        let largest = b"zzz";

        let index_data = build_partition_index(&trie_data, root_pos, 1, smallest, largest);

        let pi = PartitionIndex::open(index_data).unwrap();
        assert_eq!(pi.key_count(), 1);
        assert_eq!(pi.smallest_key(), b"aaa");
        assert_eq!(pi.largest_key(), b"zzz");
    }

    #[test]
    fn open_file_too_small() {
        let data = vec![0u8; 10]; // less than 24 bytes
        let result = PartitionIndex::open(data);
        assert!(result.is_err());
    }

    #[test]
    fn lookup_with_hand_built_trie() {
        // Build a hand-crafted trie with two entries.
        // We use the trie builder for correctness.
        let dk1 = DecoratedKey {
            token: Token(1),
            key: PartitionKey::from(b"key1".as_slice()),
        };
        let dk2 = DecoratedKey {
            token: Token(2),
            key: PartitionKey::from(b"key2".as_slice()),
        };

        let encoded1 = byte_comparable::encode(&dk1);
        let encoded2 = byte_comparable::encode(&dk2);

        let (_h1_1, h2_1) = dk1.filter_hash();
        let hash1 = (h2_1 & 0xFF) as u8;
        let (_h1_2, h2_2) = dk2.filter_hash();
        let hash2 = (h2_2 & 0xFF) as u8;

        let idxpos1: i64 = 1000;
        let idxpos2: i64 = 2000;

        // Sort encoded keys and add in order
        let mut entries = vec![
            (encoded1.clone(), hash1, idxpos1),
            (encoded2.clone(), hash2, idxpos2),
        ];
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut builder = TrieBuilder::new();
        for (encoded, hash, pos) in &entries {
            builder
                .add(
                    encoded,
                    TriePayload {
                        hash: Some(*hash),
                        position: *pos,
                    },
                )
                .unwrap();
        }
        let (trie_data, root_pos) = builder.finish().unwrap();

        let index_data = build_partition_index(&trie_data, root_pos, 2, b"key1", b"key2");

        let pi = PartitionIndex::open(index_data).unwrap();
        assert_eq!(pi.key_count(), 2);

        // Lookup dk1
        match pi.lookup(&dk1).unwrap() {
            PartitionLookup::RowIndex { position } => {
                assert_eq!(position, 1000);
            }
            other => panic!("expected RowIndex, got {:?}", other),
        }

        // Lookup dk2
        match pi.lookup(&dk2).unwrap() {
            PartitionLookup::RowIndex { position } => {
                assert_eq!(position, 2000);
            }
            other => panic!("expected RowIndex, got {:?}", other),
        }
    }

    #[test]
    fn lookup_not_found() {
        let dk = DecoratedKey {
            token: Token(1),
            key: PartitionKey::from(b"key1".as_slice()),
        };
        let encoded = byte_comparable::encode(&dk);
        let (_h1, h2) = dk.filter_hash();
        let hash = (h2 & 0xFF) as u8;

        let mut builder = TrieBuilder::new();
        builder
            .add(
                &encoded,
                TriePayload {
                    hash: Some(hash),
                    position: 100,
                },
            )
            .unwrap();
        let (trie_data, root_pos) = builder.finish().unwrap();

        let index_data = build_partition_index(&trie_data, root_pos, 1, b"key1", b"key1");
        let pi = PartitionIndex::open(index_data).unwrap();

        // Look up a key that doesn't exist
        let missing = DecoratedKey {
            token: Token(999),
            key: PartitionKey::from(b"missing".as_slice()),
        };
        match pi.lookup(&missing).unwrap() {
            PartitionLookup::NotFound => {}
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn lookup_data_direct_negative_idxpos() {
        // A negative idxpos means a direct data pointer (bitwise NOT).
        let dk = DecoratedKey {
            token: Token(42),
            key: PartitionKey::from(b"direct".as_slice()),
        };
        let encoded = byte_comparable::encode(&dk);
        let (_h1, h2) = dk.filter_hash();
        let hash = (h2 & 0xFF) as u8;

        // idxpos = -1 means data position = !(-1) = 0
        // idxpos = -101 means data position = !(-101) = 100
        let idxpos: i64 = -101;

        let mut builder = TrieBuilder::new();
        builder
            .add(
                &encoded,
                TriePayload {
                    hash: Some(hash),
                    position: idxpos,
                },
            )
            .unwrap();
        let (trie_data, root_pos) = builder.finish().unwrap();

        let index_data = build_partition_index(&trie_data, root_pos, 1, b"direct", b"direct");
        let pi = PartitionIndex::open(index_data).unwrap();

        match pi.lookup(&dk).unwrap() {
            PartitionLookup::DataDirect { position } => {
                assert_eq!(position, 100);
            }
            other => panic!("expected DataDirect, got {:?}", other),
        }
    }

    #[test]
    fn lookup_hash_mismatch_returns_not_found() {
        // Build an index with one key but a wrong hash byte. The lookup
        // should fail the hash check and return NotFound.
        let dk = DecoratedKey {
            token: Token(7),
            key: PartitionKey::from(b"hashtest".as_slice()),
        };
        let encoded = byte_comparable::encode(&dk);
        let (_h1, h2) = dk.filter_hash();
        let correct_hash = (h2 & 0xFF) as u8;
        let wrong_hash = correct_hash.wrapping_add(1);

        let mut builder = TrieBuilder::new();
        builder
            .add(
                &encoded,
                TriePayload {
                    hash: Some(wrong_hash),
                    position: 500,
                },
            )
            .unwrap();
        let (trie_data, root_pos) = builder.finish().unwrap();

        let index_data = build_partition_index(&trie_data, root_pos, 1, b"hashtest", b"hashtest");
        let pi = PartitionIndex::open(index_data).unwrap();

        match pi.lookup(&dk).unwrap() {
            PartitionLookup::NotFound => {}
            other => panic!("expected NotFound due to hash mismatch, got {:?}", other),
        }
    }

    #[test]
    fn read_short_length_prefixed_basic() {
        let mut data = Vec::new();
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&3u16.to_be_bytes());
        data.extend_from_slice(b"bye");

        let mut pos = 0u64;
        let first = read_short_length_prefixed(&data, &mut pos).unwrap();
        assert_eq!(first, b"hello");
        assert_eq!(pos, 7);

        let second = read_short_length_prefixed(&data, &mut pos).unwrap();
        assert_eq!(second, b"bye");
        assert_eq!(pos, 12);
    }

    #[test]
    fn multiple_keys_lookup() {
        // Build an index with several keys and verify all are found.
        let keys: Vec<DecoratedKey> = (0..10)
            .map(|i| DecoratedKey {
                token: Token(i * 100),
                key: PartitionKey::from(format!("part{}", i).as_bytes()),
            })
            .collect();

        // Encode and sort
        let mut entries: Vec<(Vec<u8>, u8, i64, usize)> = keys
            .iter()
            .enumerate()
            .map(|(i, dk)| {
                let encoded = byte_comparable::encode(dk);
                let (_h1, h2) = dk.filter_hash();
                let hash = (h2 & 0xFF) as u8;
                (encoded, hash, (i * 1000) as i64, i)
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut builder = TrieBuilder::new();
        for (encoded, hash, pos, _) in &entries {
            builder
                .add(
                    encoded,
                    TriePayload {
                        hash: Some(*hash),
                        position: *pos,
                    },
                )
                .unwrap();
        }
        let (trie_data, root_pos) = builder.finish().unwrap();

        let index_data =
            build_partition_index(&trie_data, root_pos, keys.len() as u64, b"part0", b"part9");
        let pi = PartitionIndex::open(index_data).unwrap();
        assert_eq!(pi.key_count(), 10);

        // Verify each key
        for (i, dk) in keys.iter().enumerate() {
            match pi.lookup(dk).unwrap() {
                PartitionLookup::RowIndex { position } => {
                    assert_eq!(position, (i * 1000) as u64, "wrong position for key {}", i);
                }
                other => panic!("expected RowIndex for key {}, got {:?}", i, other),
            }
        }
    }
}
