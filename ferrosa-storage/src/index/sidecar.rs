//! Per-SSTable sidecar index files with CRC32-validated headers.
//!
//! Wraps the existing BTree serialization format from ferrosa-index
//! with a 17-byte header containing magic bytes, version, entry count,
//! and CRC32 checksum.
//!
//! ## File format
//!
//! ```text
//! +------- Header (17 bytes) -------+
//! | magic:       b"FXSI" (4 bytes)  |
//! | version:     u8     (1 byte)    |
//! | entry_count: u64 LE (8 bytes)   |
//! | header_crc:  u32 LE (4 bytes)   |  <- CRC32 of first 13 bytes
//! +----------------------------------+
//! | body: sorted entries             |
//! |   key_len(u32) | key_bytes      |
//! |   pk_len(u32)  | pk_bytes       |
//! |   ck_len(u32)  | ck_bytes       |
//! +----------------------------------+
//! ```

use std::path::Path;

use ferrosa_index::{IndexError, IndexKey, IndexResult, RowPosition};

/// Magic bytes identifying a sidecar index file.
const SIDECAR_MAGIC: &[u8; 4] = b"FXSI";

/// Current sidecar format version.
const SIDECAR_VERSION: u8 = 1;

/// Header size: magic(4) + version(1) + entry_count(8) + crc(4) = 17.
const HEADER_SIZE: usize = 17;

/// Bytes before the CRC field: magic(4) + version(1) + entry_count(8) = 13.
const CRC_INPUT_SIZE: usize = 13;

// ── Internal entry type ──────────────────────────────────────────────────────

/// A single entry in the sidecar index, matching the BTree wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SidecarEntry {
    key: Vec<u8>,
    position: RowPosition,
}

// ── Writer ───────────────────────────────────────────────────────────────────

/// Writes a sidecar index file with CRC32-validated header.
pub struct SidecarWriter;

impl SidecarWriter {
    /// Write a sidecar index file. Entries are sorted by key before writing.
    pub fn write(path: &Path, entries: &[(IndexKey, RowPosition)]) -> IndexResult<()> {
        let mut sorted: Vec<_> = entries.to_vec();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        let entry_count = sorted.len() as u64;

        // Build header bytes (without CRC)
        let mut header = Vec::with_capacity(HEADER_SIZE);
        header.extend_from_slice(SIDECAR_MAGIC);
        header.push(SIDECAR_VERSION);
        header.extend_from_slice(&entry_count.to_le_bytes());

        assert_eq!(header.len(), CRC_INPUT_SIZE);

        // CRC32 of first 13 bytes
        let crc = crc32fast::hash(&header);
        header.extend_from_slice(&crc.to_le_bytes());

        assert_eq!(header.len(), HEADER_SIZE);

        // Serialize body entries
        let body = serialize_entries(&sorted);

        let mut file_data = header;
        file_data.extend_from_slice(&body);
        std::fs::write(path, &file_data)?;
        Ok(())
    }
}

// ── Reader ───────────────────────────────────────────────────────────────────

/// Reads a sidecar index file, validating the CRC32 header on open.
#[derive(Debug)]
pub struct SidecarReader {
    entry_count: u64,
    entries: Vec<SidecarEntry>,
}

impl SidecarReader {
    /// Construct a SidecarReader directly from entries (no disk I/O).
    /// Used during flush to create in-memory sidecar readers.
    pub fn from_entries(entries: Vec<(IndexKey, RowPosition)>) -> Self {
        let mut sorted = entries;
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let entry_count = sorted.len() as u64;
        let sidecar_entries: Vec<SidecarEntry> = sorted
            .into_iter()
            .map(|(key, pos)| SidecarEntry {
                key: key.0,
                position: pos,
            })
            .collect();
        Self {
            entry_count,
            entries: sidecar_entries,
        }
    }

    /// Open and validate a sidecar file. Returns `IndexError::Corrupt` on
    /// magic mismatch or CRC mismatch.
    pub fn open(path: &Path) -> IndexResult<Self> {
        let data = std::fs::read(path)?;
        Self::from_bytes(&data)
    }

    /// Parse a sidecar from raw bytes (useful for testing without disk).
    fn from_bytes(data: &[u8]) -> IndexResult<Self> {
        if data.len() < HEADER_SIZE {
            return Err(IndexError::Corrupt(
                "sidecar file too short for header".into(),
            ));
        }

        // Validate magic
        if &data[..4] != SIDECAR_MAGIC {
            return Err(IndexError::Corrupt("invalid sidecar magic bytes".into()));
        }

        // Validate version
        let version = data[4];
        if version != SIDECAR_VERSION {
            return Err(IndexError::Corrupt(format!(
                "unsupported sidecar version: {version}"
            )));
        }

        // Validate CRC (first 13 bytes -> 4-byte CRC at offset 13)
        let computed_crc = crc32fast::hash(&data[..CRC_INPUT_SIZE]);
        let stored_crc = u32::from_le_bytes(
            data[CRC_INPUT_SIZE..HEADER_SIZE]
                .try_into()
                .expect("4-byte slice"),
        );
        if computed_crc != stored_crc {
            return Err(IndexError::Corrupt(format!(
                "sidecar CRC mismatch: stored={stored_crc:#x}, computed={computed_crc:#x}"
            )));
        }

        let entry_count = u64::from_le_bytes(data[5..13].try_into().expect("8-byte slice"));

        let entries = deserialize_entries(&data[HEADER_SIZE..], entry_count as usize)?;

        Ok(Self {
            entry_count,
            entries,
        })
    }

    /// Number of entries in the sidecar index.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Returns all entries as `(IndexKey, RowPosition)` pairs.
    pub fn all_entries(&self) -> Vec<(IndexKey, RowPosition)> {
        self.entries
            .iter()
            .map(|e| (IndexKey(e.key.clone()), e.position.clone()))
            .collect()
    }

    /// Point lookup: returns all `RowPosition`s whose key exactly matches.
    pub fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let key_bytes = &key.0;
        let start = self
            .entries
            .partition_point(|e| e.key.as_slice() < key_bytes.as_slice());

        let mut results = Vec::new();
        for entry in &self.entries[start..] {
            if entry.key == *key_bytes {
                results.push(entry.position.clone());
            } else {
                break;
            }
        }
        Ok(results)
    }

    /// Range query: returns all `RowPosition`s for keys in `[start, end]`
    /// inclusive.
    pub fn range(&self, start: &IndexKey, end: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let start_idx = self
            .entries
            .partition_point(|e| e.key.as_slice() < start.0.as_slice());

        let mut results = Vec::new();
        for entry in &self.entries[start_idx..] {
            if entry.key.as_slice() > end.0.as_slice() {
                break;
            }
            results.push(entry.position.clone());
        }
        Ok(results)
    }
}

// ── Serialization (matches BTree entry wire format) ──────────────────────────

/// Serialize entries into the BTree body format (no entry count header --
/// the sidecar header carries that).
fn serialize_entries(entries: &[(IndexKey, RowPosition)]) -> Vec<u8> {
    let mut buf = Vec::new();

    for (key, pos) in entries {
        // key_len + key_bytes
        buf.extend_from_slice(&(key.0.len() as u32).to_le_bytes());
        buf.extend_from_slice(&key.0);

        // pk_len + pk_bytes
        buf.extend_from_slice(&(pos.partition_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&pos.partition_key);

        // ck_len + ck_bytes
        buf.extend_from_slice(&(pos.clustering_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&pos.clustering_key);
    }

    buf
}

/// Deserialize `count` entries from the BTree body format.
fn deserialize_entries(data: &[u8], count: usize) -> IndexResult<Vec<SidecarEntry>> {
    let mut entries = Vec::with_capacity(count);
    let mut offset = 0;

    for _ in 0..count {
        let key = read_length_prefixed(data, &mut offset)?;
        let pk = read_length_prefixed(data, &mut offset)?;
        let ck = read_length_prefixed(data, &mut offset)?;

        entries.push(SidecarEntry {
            key,
            position: RowPosition {
                partition_key: pk,
                clustering_key: ck,
            },
        });
    }

    Ok(entries)
}

/// Read a length-prefixed byte sequence: `u32 LE length` followed by `length` bytes.
fn read_length_prefixed(data: &[u8], offset: &mut usize) -> IndexResult<Vec<u8>> {
    if *offset + 4 > data.len() {
        return Err(IndexError::Corrupt(format!(
            "unexpected EOF at offset {offset} reading length"
        )));
    }
    let len =
        u32::from_le_bytes(data[*offset..*offset + 4].try_into().expect("4-byte slice")) as usize;
    *offset += 4;

    if *offset + len > data.len() {
        return Err(IndexError::Corrupt(format!(
            "unexpected EOF at offset {offset} reading {len} bytes"
        )));
    }
    let bytes = data[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_index::{IndexKey, RowPosition};
    use tempfile::tempdir;

    fn sample_entries() -> Vec<(IndexKey, RowPosition)> {
        vec![
            (
                IndexKey(b"alice".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: b"ck1".to_vec(),
                },
            ),
            (
                IndexKey(b"bob".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: b"ck2".to_vec(),
                },
            ),
            (
                IndexKey(b"charlie".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ]
    }

    // ── Task 3: Sidecar file format tests ─────────────────────────────────────

    #[test]
    fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sidecar");

        let entries = sample_entries();
        SidecarWriter::write(&path, &entries).unwrap();

        let reader = SidecarReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 3);

        let results = reader.lookup(&IndexKey(b"bob".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[0].clustering_key, b"ck2");
    }

    #[test]
    fn empty_sidecar_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sidecar");

        SidecarWriter::write(&path, &[]).unwrap();

        let reader = SidecarReader::open(&path).unwrap();
        assert_eq!(reader.entry_count(), 0);
        let results = reader.lookup(&IndexKey(b"anything".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn corrupt_magic_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_magic.sidecar");

        SidecarWriter::write(&path, &sample_entries()).unwrap();

        // Corrupt the magic bytes
        let mut data = std::fs::read(&path).unwrap();
        data[0] = 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = SidecarReader::open(&path);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("corrupt") || err_msg.contains("magic"),
            "expected corruption error, got: {err_msg}"
        );
    }

    #[test]
    fn corrupt_crc_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt_crc.sidecar");

        SidecarWriter::write(&path, &sample_entries()).unwrap();

        // Corrupt the entry_count field (byte 5-12) to trigger CRC mismatch
        let mut data = std::fs::read(&path).unwrap();
        data[6] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = SidecarReader::open(&path);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("corrupt") || err_msg.contains("CRC"),
            "expected CRC error, got: {err_msg}"
        );
    }

    #[test]
    fn file_too_short_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("short.sidecar");
        std::fs::write(&path, [0u8; 10]).unwrap();

        let result = SidecarReader::open(&path);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("too short"),
            "expected too-short error, got: {err_msg}"
        );
    }

    #[test]
    fn unsorted_entries_are_sorted_on_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("unsorted.sidecar");

        // Provide entries in reverse order
        let entries = vec![
            (
                IndexKey(b"zzz".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"aaa".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"mmm".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&path, &entries).unwrap();
        let reader = SidecarReader::open(&path).unwrap();

        // Range query should work correctly since entries are sorted
        let results = reader
            .range(&IndexKey(b"aaa".to_vec()), &IndexKey(b"mmm".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk1");
        assert_eq!(results[1].partition_key, b"pk2");
    }

    // ── Task 4: BTree adapter + range query tests ─────────────────────────────

    #[test]
    fn build_sidecar_from_btree_entries_and_lookup() {
        let dir = tempdir().unwrap();
        let sidecar_path = dir.path().join("from_btree.sidecar");

        // Build entries using the same row data that BTreeBuilder would process
        let entries = vec![
            (
                IndexKey(b"alpha".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: b"ck1".to_vec(),
                },
            ),
            (
                IndexKey(b"beta".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: b"ck2".to_vec(),
                },
            ),
            (
                IndexKey(b"gamma".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&sidecar_path, &entries).unwrap();
        let reader = SidecarReader::open(&sidecar_path).unwrap();

        // Point lookup
        let results = reader.lookup(&IndexKey(b"beta".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");

        // Missing key
        let results = reader.lookup(&IndexKey(b"missing".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn sidecar_range_query() {
        let dir = tempdir().unwrap();
        let sidecar_path = dir.path().join("range.sidecar");

        let entries = vec![
            (
                IndexKey(b"aaa".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"bbb".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"ccc".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"ddd".to_vec()),
                RowPosition {
                    partition_key: b"pk4".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&sidecar_path, &entries).unwrap();
        let reader = SidecarReader::open(&sidecar_path).unwrap();

        let results = reader
            .range(&IndexKey(b"bbb".to_vec()), &IndexKey(b"ccc".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[1].partition_key, b"pk3");
    }

    #[test]
    fn sidecar_range_query_full_range() {
        let dir = tempdir().unwrap();
        let sidecar_path = dir.path().join("full_range.sidecar");

        let entries = vec![
            (
                IndexKey(b"aaa".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: vec![],
                },
            ),
            (
                IndexKey(b"zzz".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&sidecar_path, &entries).unwrap();
        let reader = SidecarReader::open(&sidecar_path).unwrap();

        let results = reader
            .range(&IndexKey(b"aaa".to_vec()), &IndexKey(b"zzz".to_vec()))
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    // ── Task 4.4: CRC32 validation tests ─────────────────────────────────────

    #[test]
    fn single_bit_flip_in_header_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bitflip.sidecar");
        SidecarWriter::write(&path, &sample_entries()).unwrap();
        let mut data = std::fs::read(&path).unwrap();
        data[7] ^= 0x01; // flip one bit in entry_count
        std::fs::write(&path, &data).unwrap();
        assert!(SidecarReader::open(&path).is_err());
    }

    #[test]
    fn valid_sidecar_opens_without_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("valid.sidecar");
        SidecarWriter::write(&path, &sample_entries()).unwrap();
        assert!(SidecarReader::open(&path).is_ok());
    }

    #[test]
    fn multiple_rows_same_key_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dupes.sidecar");

        let entries = vec![
            (
                IndexKey(b"same".to_vec()),
                RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: b"ck1".to_vec(),
                },
            ),
            (
                IndexKey(b"same".to_vec()),
                RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: b"ck2".to_vec(),
                },
            ),
            (
                IndexKey(b"other".to_vec()),
                RowPosition {
                    partition_key: b"pk3".to_vec(),
                    clustering_key: vec![],
                },
            ),
        ];

        SidecarWriter::write(&path, &entries).unwrap();
        let reader = SidecarReader::open(&path).unwrap();

        let results = reader.lookup(&IndexKey(b"same".to_vec())).unwrap();
        assert_eq!(results.len(), 2);
        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"pk1".as_slice()));
        assert!(pks.contains(&b"pk2".as_slice()));
    }
}
