//! B-tree secondary index.
//!
//! Stores sorted `(key_bytes, RowPosition)` entries, supporting both point
//! lookups via binary search and range scans.
//!
//! ## File format
//!
//! ```text
//! header:  entry_count (u64 LE)
//! entries: key_len (u32 LE) | key_bytes
//!          pk_len  (u32 LE) | pk_bytes
//!          ck_len  (u32 LE) | ck_bytes
//! ```

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
};
use ferrosa_common::CellValue;
use std::ops::Bound;
use std::path::PathBuf;

/// Entry stored in the B-tree index: a key and its corresponding row position.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BTreeEntry {
    key: Vec<u8>,
    position: RowPosition,
}

// ── Factory ──────────────────────────────────────────────────────────────────

/// Factory for creating B-tree index builders and readers.
pub struct BTreeIndexFactory;

impl IndexFactory for BTreeIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let file_path = config.output_dir.join(format!("{}.btree", config.name));
        Ok(Box::new(BTreeBuilder {
            entries: Vec::new(),
            file_path,
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data.path)?;
        let entries = deserialize_entries(&data)?;
        Ok(Box::new(BTreeReader { entries }))
    }

    fn index_type(&self) -> IndexType {
        IndexType::BTree
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Accumulates rows and writes a sorted B-tree index file.
pub struct BTreeBuilder {
    entries: Vec<BTreeEntry>,
    file_path: PathBuf,
}

impl IndexBuilder for BTreeBuilder {
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[CellValue],
        column_positions: &[usize],
    ) -> IndexResult<()> {
        if column_positions.is_empty() {
            return Ok(());
        }

        let col_pos = column_positions[0];
        if col_pos >= cells.len() {
            return Err(IndexError::MissingColumn(col_pos));
        }

        let cell = &cells[col_pos];
        // Skip tombstones
        let value = match &cell.value {
            Some(v) => v.clone(),
            None => return Ok(()),
        };

        self.entries.push(BTreeEntry {
            key: value,
            position: RowPosition {
                partition_key: partition_key.to_vec(),
                clustering_key: clustering_key.to_vec(),
            },
        });

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> IndexResult<IndexFiles> {
        // Sort by key bytes for binary search
        self.entries.sort_by(|a, b| a.key.cmp(&b.key));

        let data = serialize_entries(&self.entries);
        std::fs::write(&self.file_path, &data)?;

        let size = data.len() as u64;
        Ok(IndexFiles {
            data: IndexFileMeta {
                path: self.file_path,
                size,
            },
        })
    }
}

// ── Reader ───────────────────────────────────────────────────────────────────

/// Reads a sorted B-tree index, supporting binary-search lookups and range scans.
pub struct BTreeReader {
    entries: Vec<BTreeEntry>,
}

impl IndexReader for BTreeReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let key_bytes = &key.0;
        // Find the first entry >= key via binary search
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

    fn range(
        &self,
        start: Bound<&IndexKey>,
        end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>> {
        let start_idx = match start {
            Bound::Included(key) => self
                .entries
                .partition_point(|e| e.key.as_slice() < key.0.as_slice()),
            Bound::Excluded(key) => self
                .entries
                .partition_point(|e| e.key.as_slice() <= key.0.as_slice()),
            Bound::Unbounded => 0,
        };

        let mut results = Vec::new();
        for entry in &self.entries[start_idx..] {
            let include = match end {
                Bound::Included(key) => entry.key.as_slice() <= key.0.as_slice(),
                Bound::Excluded(key) => entry.key.as_slice() < key.0.as_slice(),
                Bound::Unbounded => true,
            };
            if !include {
                break;
            }
            results.push(entry.position.clone());
        }
        Ok(results)
    }

    fn nearest(&self, _key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "nearest lookup not supported by B-tree index".to_string(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN
    }
}

// ── Serialization ────────────────────────────────────────────────────────────

fn serialize_entries(entries: &[BTreeEntry]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: entry count
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());

    for entry in entries {
        // key_len + key_bytes
        buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.key);

        // pk_len + pk_bytes
        buf.extend_from_slice(&(entry.position.partition_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.position.partition_key);

        // ck_len + ck_bytes
        buf.extend_from_slice(&(entry.position.clustering_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.position.clustering_key);
    }

    buf
}

fn deserialize_entries(data: &[u8]) -> IndexResult<Vec<BTreeEntry>> {
    if data.len() < 8 {
        return Err(IndexError::Corrupt("file too short for header".to_string()));
    }

    let entry_count = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(entry_count);
    let mut offset = 8;

    for _ in 0..entry_count {
        let key = read_length_prefixed(data, &mut offset)?;
        let pk = read_length_prefixed(data, &mut offset)?;
        let ck = read_length_prefixed(data, &mut offset)?;

        entries.push(BTreeEntry {
            key,
            position: RowPosition {
                partition_key: pk,
                clustering_key: ck,
            },
        });
    }

    Ok(entries)
}

fn read_length_prefixed(data: &[u8], offset: &mut usize) -> IndexResult<Vec<u8>> {
    if *offset + 4 > data.len() {
        return Err(IndexError::Corrupt(format!(
            "unexpected EOF at offset {offset} reading length"
        )));
    }
    let len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
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
    use ferrosa_common::CellValue;
    use tempfile::tempdir;

    /// Helper: build an index from rows and return a reader.
    fn build_and_read(
        rows: Vec<(&[u8], &[u8], Vec<CellValue>)>,
        column_positions: &[usize],
    ) -> Box<dyn IndexReader> {
        let dir = tempdir().unwrap();
        let config = IndexConfig {
            index_type: IndexType::BTree,
            column_positions: column_positions.to_vec(),
            output_dir: dir.path().to_path_buf(),
            name: "test_btree".to_string(),
        };
        let factory = BTreeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        for (pk, ck, cells) in &rows {
            builder.add_row(pk, ck, cells, column_positions).unwrap();
        }

        let files = builder.finish().unwrap();
        factory.open_reader(&files).unwrap()
    }

    #[test]
    fn point_lookup_finds_correct_row() {
        let reader = build_and_read(
            vec![
                (b"pk1", b"ck1", vec![CellValue::live(b"alpha".to_vec(), 1)]),
                (b"pk2", b"ck2", vec![CellValue::live(b"beta".to_vec(), 2)]),
                (b"pk3", b"ck3", vec![CellValue::live(b"gamma".to_vec(), 3)]),
            ],
            &[0],
        );

        let results = reader.lookup(&IndexKey(b"beta".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[0].clustering_key, b"ck2");
    }

    #[test]
    fn point_lookup_not_found_returns_empty() {
        let reader = build_and_read(
            vec![(b"pk1", b"ck1", vec![CellValue::live(b"alpha".to_vec(), 1)])],
            &[0],
        );

        let results = reader.lookup(&IndexKey(b"missing".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn range_scan_returns_correct_subset() {
        let reader = build_and_read(
            vec![
                (b"pk1", b"ck1", vec![CellValue::live(b"aaa".to_vec(), 1)]),
                (b"pk2", b"ck2", vec![CellValue::live(b"bbb".to_vec(), 2)]),
                (b"pk3", b"ck3", vec![CellValue::live(b"ccc".to_vec(), 3)]),
                (b"pk4", b"ck4", vec![CellValue::live(b"ddd".to_vec(), 4)]),
            ],
            &[0],
        );

        // Inclusive range [bbb, ccc]
        let results = reader
            .range(
                Bound::Included(&IndexKey(b"bbb".to_vec())),
                Bound::Included(&IndexKey(b"ccc".to_vec())),
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[1].partition_key, b"pk3");
    }

    #[test]
    fn range_scan_exclusive_bounds() {
        let reader = build_and_read(
            vec![
                (b"pk1", b"ck1", vec![CellValue::live(b"aaa".to_vec(), 1)]),
                (b"pk2", b"ck2", vec![CellValue::live(b"bbb".to_vec(), 2)]),
                (b"pk3", b"ck3", vec![CellValue::live(b"ccc".to_vec(), 3)]),
                (b"pk4", b"ck4", vec![CellValue::live(b"ddd".to_vec(), 4)]),
            ],
            &[0],
        );

        // Exclusive range (aaa, ddd)
        let results = reader
            .range(
                Bound::Excluded(&IndexKey(b"aaa".to_vec())),
                Bound::Excluded(&IndexKey(b"ddd".to_vec())),
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[1].partition_key, b"pk3");
    }

    #[test]
    fn range_scan_unbounded() {
        let reader = build_and_read(
            vec![
                (b"pk1", b"ck1", vec![CellValue::live(b"aaa".to_vec(), 1)]),
                (b"pk2", b"ck2", vec![CellValue::live(b"bbb".to_vec(), 2)]),
            ],
            &[0],
        );

        let results = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn empty_index_returns_empty_results() {
        let reader = build_and_read(vec![], &[0]);

        let lookup = reader.lookup(&IndexKey(b"anything".to_vec())).unwrap();
        assert!(lookup.is_empty());

        let range = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert!(range.is_empty());
    }

    #[test]
    fn tombstone_rows_are_skipped() {
        let reader = build_and_read(
            vec![
                (b"pk1", b"ck1", vec![CellValue::live(b"alpha".to_vec(), 1)]),
                (b"pk2", b"ck2", vec![CellValue::tombstone(2, 1700000000)]),
                (b"pk3", b"ck3", vec![CellValue::live(b"gamma".to_vec(), 3)]),
            ],
            &[0],
        );

        // Tombstone row should not appear
        let all = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].partition_key, b"pk1");
        assert_eq!(all[1].partition_key, b"pk3");
    }

    #[test]
    fn multiple_rows_with_same_key() {
        let reader = build_and_read(
            vec![
                (b"pk1", b"ck1", vec![CellValue::live(b"same".to_vec(), 1)]),
                (b"pk2", b"ck2", vec![CellValue::live(b"same".to_vec(), 2)]),
                (b"pk3", b"ck3", vec![CellValue::live(b"other".to_vec(), 3)]),
            ],
            &[0],
        );

        let results = reader.lookup(&IndexKey(b"same".to_vec())).unwrap();
        assert_eq!(results.len(), 2);
        // Both pk1 and pk2 should be returned (order is stable since keys are equal)
        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"pk1".as_slice()));
        assert!(pks.contains(&b"pk2".as_slice()));
    }

    #[test]
    fn nearest_returns_unsupported() {
        let reader = build_and_read(
            vec![(b"pk1", b"ck1", vec![CellValue::live(b"alpha".to_vec(), 1)])],
            &[0],
        );

        let result = reader.nearest(&IndexKey(b"alpha".to_vec()));
        assert!(result.is_err());
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn capabilities_include_point_and_range() {
        let reader = build_and_read(vec![], &[0]);
        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
    }

    #[test]
    fn serialization_roundtrip() {
        let entries = vec![
            BTreeEntry {
                key: b"key1".to_vec(),
                position: RowPosition {
                    partition_key: b"pk1".to_vec(),
                    clustering_key: b"ck1".to_vec(),
                },
            },
            BTreeEntry {
                key: b"key2".to_vec(),
                position: RowPosition {
                    partition_key: b"pk2".to_vec(),
                    clustering_key: vec![],
                },
            },
        ];

        let data = serialize_entries(&entries);
        let roundtripped = deserialize_entries(&data).unwrap();
        assert_eq!(entries, roundtripped);
    }
}
