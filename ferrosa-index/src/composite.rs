//! Composite multi-column index: concatenates values from multiple columns
//! with length prefixes into a single key for sorted lookup.
//!
//! Supports [`IndexCapabilities::POINT_LOOKUP`] and [`IndexCapabilities::RANGE_SCAN`].
//!
//! Each composite key is formed by concatenating the column values in the order
//! specified by [`IndexConfig::column_positions`], each prefixed with a 4-byte
//! little-endian length. This gives a deterministic total ordering that respects
//! column order.
//!
//! On-disk format is the same as the B-tree index (sorted entries).

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
};
use ferrosa_common::CellValue;
use std::io::{Read as _, Write as _};
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

/// Factory for creating composite index builders and readers.
pub struct CompositeIndexFactory;

impl IndexFactory for CompositeIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        if config.column_positions.len() < 2 {
            return Err(IndexError::Build(
                "composite index requires at least 2 column positions".into(),
            ));
        }
        Ok(Box::new(CompositeBuilder {
            entries: Vec::new(),
            config: config.clone(),
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data_path)?;
        let entries = deserialize_entries(&data)?;
        Ok(Box::new(CompositeReader { entries }))
    }

    fn merge(
        &self,
        readers: Vec<Box<dyn IndexReader>>,
        builder: Box<dyn IndexBuilder>,
    ) -> IndexResult<IndexFiles> {
        let _ = readers;
        builder.finish()
    }
}

/// Accumulates composite-key entries during index build.
pub struct CompositeBuilder {
    entries: Vec<(Vec<u8>, RowPosition)>,
    config: IndexConfig,
}

impl IndexBuilder for CompositeBuilder {
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[(u16, CellValue)],
    ) -> IndexResult<()> {
        // Build composite key from all column_positions.
        let mut composite_key = Vec::new();
        for &col_pos in &self.config.column_positions {
            let cell = cells
                .iter()
                .find(|(pos, _)| *pos as usize == col_pos)
                .map(|(_, cv)| cv);

            let cell = match cell {
                Some(c) => c,
                None => return Ok(()), // Column not present; skip entire row.
            };

            // Skip tombstones.
            if cell.is_tombstone() {
                return Ok(());
            }

            let value = match &cell.value {
                Some(v) => v,
                None => return Ok(()),
            };

            // Length-prefix each component for deterministic ordering.
            composite_key.extend_from_slice(&(value.len() as u32).to_le_bytes());
            composite_key.extend_from_slice(value);
        }

        self.entries.push((
            composite_key,
            RowPosition {
                partition_key: partition_key.to_vec(),
                clustering_key: clustering_key.to_vec(),
            },
        ));

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> IndexResult<IndexFiles> {
        self.entries.sort_by(|a, b| a.0.cmp(&b.0));

        let row_count = self.entries.len() as u64;
        let data = serialize_entries(&self.entries);

        let data_path = self.config.output_dir.join(format!(
            "{}-{}.db",
            self.config.sstable_prefix, self.config.index_name
        ));
        let meta_path = self.config.output_dir.join(format!(
            "{}-{}.meta",
            self.config.sstable_prefix, self.config.index_name
        ));

        std::fs::create_dir_all(&self.config.output_dir)?;
        std::fs::write(&data_path, &data)?;

        let checksum = crc32(&data);
        let file_size = data.len() as u64;
        let build_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let meta = IndexFileMeta {
            index_type: IndexType::Composite {
                columns: self
                    .config
                    .column_positions
                    .iter()
                    .map(|p| format!("col_{p}"))
                    .collect(),
            },
            index_name: self.config.index_name.clone(),
            row_count,
            build_timestamp,
            sstable_id: self.config.sstable_prefix.clone(),
            file_size,
            checksum,
        };

        let meta_json = serde_json::to_vec(&meta)
            .map_err(|e| IndexError::Build(format!("meta serialization: {e}")))?;
        std::fs::write(&meta_path, &meta_json)?;

        Ok(IndexFiles {
            data_path,
            meta_path,
            meta,
        })
    }
}

/// Reads a composite index from a deserialized sorted entry vector.
pub struct CompositeReader {
    entries: Vec<(Vec<u8>, RowPosition)>,
}

impl CompositeReader {
    /// Encode a composite lookup key from individual component byte slices.
    fn encode_composite_key(parts: &[Vec<u8>]) -> Vec<u8> {
        let mut key = Vec::new();
        for part in parts {
            key.extend_from_slice(&(part.len() as u32).to_le_bytes());
            key.extend_from_slice(part);
        }
        key
    }

    /// Extract the raw byte key for comparison from an IndexKey.
    fn extract_key(key: &IndexKey) -> IndexResult<Vec<u8>> {
        match key {
            IndexKey::Composite(parts) => Ok(Self::encode_composite_key(parts)),
            IndexKey::Bytes(b) => Ok(b.clone()),
            _ => Err(IndexError::Query(
                "composite index supports Composite or Bytes keys".into(),
            )),
        }
    }
}

impl IndexReader for CompositeReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let needle = CompositeReader::extract_key(key)?;
        let idx = self
            .entries
            .partition_point(|(k, _)| k.as_slice() < needle.as_slice());

        let mut results = Vec::new();
        for (k, pos) in &self.entries[idx..] {
            if k.as_slice() == needle.as_slice() {
                results.push(pos.clone());
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
            Bound::Included(k) => {
                let needle = CompositeReader::extract_key(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() < needle.as_slice())
            }
            Bound::Excluded(k) => {
                let needle = CompositeReader::extract_key(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() <= needle.as_slice())
            }
            Bound::Unbounded => 0,
        };

        let end_idx = match end {
            Bound::Included(k) => {
                let needle = CompositeReader::extract_key(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() <= needle.as_slice())
            }
            Bound::Excluded(k) => {
                let needle = CompositeReader::extract_key(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() < needle.as_slice())
            }
            Bound::Unbounded => self.entries.len(),
        };

        let results = self.entries[start_idx..end_idx]
            .iter()
            .map(|(_, pos)| pos.clone())
            .collect();
        Ok(results)
    }

    fn nearest(
        &self,
        _query: &[f32],
        _k: usize,
        _ef_search: Option<u16>,
    ) -> IndexResult<Vec<(RowPosition, f32)>> {
        Err(IndexError::Unsupported(
            "nearest not supported by composite index".into(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN
    }
}

// ---------------------------------------------------------------------------
// Helpers (shared binary format with btree)
// ---------------------------------------------------------------------------

fn serialize_entries(entries: &[(Vec<u8>, RowPosition)]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    buf.write_all(&(entries.len() as u64).to_le_bytes())
        .unwrap();
    for (key, pos) in entries {
        buf.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
        buf.write_all(key).unwrap();
        buf.write_all(&(pos.partition_key.len() as u32).to_le_bytes())
            .unwrap();
        buf.write_all(&pos.partition_key).unwrap();
        buf.write_all(&(pos.clustering_key.len() as u32).to_le_bytes())
            .unwrap();
        buf.write_all(&pos.clustering_key).unwrap();
    }
    buf
}

fn deserialize_entries(data: &[u8]) -> IndexResult<Vec<(Vec<u8>, RowPosition)>> {
    let mut cursor = std::io::Cursor::new(data);
    let mut buf8 = [0u8; 8];
    let mut buf4 = [0u8; 4];

    cursor
        .read_exact(&mut buf8)
        .map_err(|e| IndexError::Query(format!("read entry_count: {e}")))?;
    let count = u64::from_le_bytes(buf8) as usize;

    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        cursor
            .read_exact(&mut buf4)
            .map_err(|e| IndexError::Query(format!("read key_len: {e}")))?;
        let key_len = u32::from_le_bytes(buf4) as usize;
        let mut key = vec![0u8; key_len];
        cursor
            .read_exact(&mut key)
            .map_err(|e| IndexError::Query(format!("read key: {e}")))?;

        cursor
            .read_exact(&mut buf4)
            .map_err(|e| IndexError::Query(format!("read pk_len: {e}")))?;
        let pk_len = u32::from_le_bytes(buf4) as usize;
        let mut pk = vec![0u8; pk_len];
        cursor
            .read_exact(&mut pk)
            .map_err(|e| IndexError::Query(format!("read pk: {e}")))?;

        cursor
            .read_exact(&mut buf4)
            .map_err(|e| IndexError::Query(format!("read ck_len: {e}")))?;
        let ck_len = u32::from_le_bytes(buf4) as usize;
        let mut ck = vec![0u8; ck_len];
        cursor
            .read_exact(&mut ck)
            .map_err(|e| IndexError::Query(format!("read ck: {e}")))?;

        entries.push((
            key,
            RowPosition {
                partition_key: pk,
                clustering_key: ck,
            },
        ));
    }

    Ok(entries)
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = CRC32_TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

#[rustfmt::skip]
static CRC32_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let poly: u32 = 0xEDB88320;
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(dir: &std::path::Path) -> IndexConfig {
        IndexConfig {
            index_type: IndexType::Composite {
                columns: vec!["first_name".into(), "last_name".into()],
            },
            column_positions: vec![0, 1],
            output_dir: dir.to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_name_composite".into(),
        }
    }

    fn cell(value: &[u8]) -> CellValue {
        CellValue::live(value.to_vec(), 1000)
    }

    fn tombstone_cell() -> CellValue {
        CellValue::tombstone(1000, 1_700_000_000)
    }

    #[test]
    fn point_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"Smith"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Bob")), (1, cell(b"Jones"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"Alice")), (1, cell(b"Jones"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Lookup (Alice, Smith)
        let results = reader
            .lookup(&IndexKey::Composite(vec![
                b"Alice".to_vec(),
                b"Smith".to_vec(),
            ]))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");

        // Lookup (Alice, Jones)
        let results = reader
            .lookup(&IndexKey::Composite(vec![
                b"Alice".to_vec(),
                b"Jones".to_vec(),
            ]))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk3");

        // Lookup non-existent
        let results = reader
            .lookup(&IndexKey::Composite(vec![
                b"Charlie".to_vec(),
                b"Brown".to_vec(),
            ]))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn range_scan() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"A")), (1, cell(b"1"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"B")), (1, cell(b"2"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"C")), (1, cell(b"3"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Get all via unbounded range
        let results = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(results.len(), 3);

        // Range with composite start/end
        let start = IndexKey::Composite(vec![b"A".to_vec(), b"1".to_vec()]);
        let end = IndexKey::Composite(vec![b"B".to_vec(), b"2".to_vec()]);
        let results = reader
            .range(Bound::Included(&start), Bound::Included(&end))
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        assert_eq!(files.meta.row_count, 0);
        let results = reader
            .lookup(&IndexKey::Composite(vec![
                b"any".to_vec(),
                b"thing".to_vec(),
            ]))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn tombstone_skip() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        // Live row
        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice")), (1, cell(b"Smith"))])
            .unwrap();
        // Row with tombstone in first column
        builder
            .add_row(
                b"pk2",
                b"ck2",
                &[(0, tombstone_cell()), (1, cell(b"Jones"))],
            )
            .unwrap();
        // Row with tombstone in second column
        builder
            .add_row(
                b"pk3",
                b"ck3",
                &[(0, cell(b"Charlie")), (1, tombstone_cell())],
            )
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 1);
    }

    #[test]
    fn duplicate_keys() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"same")), (1, cell(b"key"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"same")), (1, cell(b"key"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader
            .lookup(&IndexKey::Composite(vec![
                b"same".to_vec(),
                b"key".to_vec(),
            ]))
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn missing_column_skips_row() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        // Only provide column 0, not column 1
        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice"))])
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 0);
    }

    #[test]
    fn requires_at_least_two_columns() {
        let dir = tempfile::tempdir().unwrap();
        let config = IndexConfig {
            index_type: IndexType::Composite {
                columns: vec!["only_one".into()],
            },
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_bad".into(),
        };
        let factory = CompositeIndexFactory;
        let result = factory.create_builder(&config);
        assert!(result.is_err());
    }

    #[test]
    fn capabilities_correct() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
        assert!(!caps.contains(IndexCapabilities::PHONETIC));
    }

    #[test]
    fn nearest_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = CompositeIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let result = reader.nearest(&[1.0, 2.0], 5, None);
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }
}
