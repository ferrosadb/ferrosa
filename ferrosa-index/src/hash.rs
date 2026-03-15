//! Hash secondary index.
//!
//! Stores entries in a `HashMap<key_bytes, Vec<RowPosition>>` for O(1) point
//! lookups. Does not support range scans or nearest lookups.
//!
//! ## File format
//!
//! Same binary layout as the B-tree index (length-prefixed entries), but
//! entries are not required to be sorted. On read, they are loaded into a
//! `HashMap`.

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
};
use ferrosa_common::CellValue;
use std::collections::HashMap;
use std::ops::Bound;
use std::path::PathBuf;

// ── Factory ──────────────────────────────────────────────────────────────────

/// Factory for creating hash index builders and readers.
pub struct HashIndexFactory;

impl IndexFactory for HashIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let file_path = config.output_dir.join(format!("{}.hash", config.name));
        Ok(Box::new(HashBuilder {
            entries: Vec::new(),
            file_path,
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data.path)?;
        let entries = deserialize_to_map(&data)?;
        Ok(Box::new(HashReader { entries }))
    }

    fn index_type(&self) -> IndexType {
        IndexType::Hash
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Raw entry before building the hash map.
#[derive(Debug, Clone)]
struct RawEntry {
    key: Vec<u8>,
    position: RowPosition,
}

/// Accumulates rows and writes a hash index file.
pub struct HashBuilder {
    entries: Vec<RawEntry>,
    file_path: PathBuf,
}

impl IndexBuilder for HashBuilder {
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

        self.entries.push(RawEntry {
            key: value,
            position: RowPosition {
                partition_key: partition_key.to_vec(),
                clustering_key: clustering_key.to_vec(),
            },
        });

        Ok(())
    }

    fn finish(self: Box<Self>) -> IndexResult<IndexFiles> {
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

/// Reads a hash index for O(1) point lookups.
pub struct HashReader {
    entries: HashMap<Vec<u8>, Vec<RowPosition>>,
}

impl IndexReader for HashReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        Ok(self.entries.get(&key.0).cloned().unwrap_or_default())
    }

    fn range(
        &self,
        _start: Bound<&IndexKey>,
        _end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "range scan not supported by hash index".to_string(),
        ))
    }

    fn nearest(&self, _key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "nearest lookup not supported by hash index".to_string(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP
    }
}

// ── Serialization ────────────────────────────────────────────────────────────

fn serialize_entries(entries: &[RawEntry]) -> Vec<u8> {
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

fn deserialize_to_map(data: &[u8]) -> IndexResult<HashMap<Vec<u8>, Vec<RowPosition>>> {
    if data.len() < 8 {
        return Err(IndexError::Corrupt("file too short for header".to_string()));
    }

    let entry_count = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
    let mut map: HashMap<Vec<u8>, Vec<RowPosition>> = HashMap::new();
    let mut offset = 8;

    for _ in 0..entry_count {
        let key = read_length_prefixed(data, &mut offset)?;
        let pk = read_length_prefixed(data, &mut offset)?;
        let ck = read_length_prefixed(data, &mut offset)?;

        map.entry(key).or_default().push(RowPosition {
            partition_key: pk,
            clustering_key: ck,
        });
    }

    Ok(map)
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

    /// Helper: build a hash index from rows and return a reader.
    fn build_and_read(
        rows: Vec<(&[u8], &[u8], Vec<CellValue>)>,
        column_positions: &[usize],
    ) -> Box<dyn IndexReader> {
        let dir = tempdir().unwrap();
        let config = IndexConfig {
            index_type: IndexType::Hash,
            column_positions: column_positions.to_vec(),
            output_dir: dir.path().to_path_buf(),
            name: "test_hash".to_string(),
        };
        let factory = HashIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        for (pk, ck, cells) in &rows {
            builder.add_row(pk, ck, cells, column_positions).unwrap();
        }

        let files = builder.finish().unwrap();
        factory.open_reader(&files).unwrap()
    }

    #[test]
    fn point_lookup_succeeds() {
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
    fn point_lookup_not_found() {
        let reader = build_and_read(
            vec![(b"pk1", b"ck1", vec![CellValue::live(b"alpha".to_vec(), 1)])],
            &[0],
        );

        let results = reader.lookup(&IndexKey(b"missing".to_vec())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn range_returns_unsupported() {
        let reader = build_and_read(
            vec![(b"pk1", b"ck1", vec![CellValue::live(b"alpha".to_vec(), 1)])],
            &[0],
        );

        let result = reader.range(Bound::Unbounded, Bound::Unbounded);
        assert!(result.is_err());
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
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
    fn empty_index_returns_empty() {
        let reader = build_and_read(vec![], &[0]);

        let results = reader.lookup(&IndexKey(b"anything".to_vec())).unwrap();
        assert!(results.is_empty());
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

        // Tombstone should not be indexed at all
        let alpha = reader.lookup(&IndexKey(b"alpha".to_vec())).unwrap();
        assert_eq!(alpha.len(), 1);
        let gamma = reader.lookup(&IndexKey(b"gamma".to_vec())).unwrap();
        assert_eq!(gamma.len(), 1);

        // pk2 had a tombstone - nothing to look up for it
    }

    #[test]
    fn multiple_rows_with_same_key() {
        let reader = build_and_read(
            vec![
                (b"pk1", b"ck1", vec![CellValue::live(b"same".to_vec(), 1)]),
                (b"pk2", b"ck2", vec![CellValue::live(b"same".to_vec(), 2)]),
            ],
            &[0],
        );

        let results = reader.lookup(&IndexKey(b"same".to_vec())).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn capabilities_point_only() {
        let reader = build_and_read(vec![], &[0]);
        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(!caps.contains(IndexCapabilities::RANGE_SCAN));
    }
}
