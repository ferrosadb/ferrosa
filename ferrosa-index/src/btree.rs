//! B-tree index implementation: sorted key-value entries with binary search.
//!
//! Supports [`IndexCapabilities::POINT_LOOKUP`] and [`IndexCapabilities::RANGE_SCAN`].
//! Keys are extracted from the first column position in [`IndexConfig::column_positions`].
//!
//! Binary format (little-endian):
//! ```text
//! entry_count: u64
//! entries[]:
//!   key_len:  u32
//!   key:      [u8; key_len]
//!   pk_len:   u32
//!   pk:       [u8; pk_len]
//!   ck_len:   u32
//!   ck:       [u8; ck_len]
//! ```

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
};
use ferrosa_common::CellValue;
use std::io::{Read as _, Write as _};
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

/// Factory for creating B-tree index builders and readers.
pub struct BTreeIndexFactory;

impl IndexFactory for BTreeIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        Ok(Box::new(BTreeBuilder {
            entries: Vec::new(),
            config: config.clone(),
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data_path)?;
        let entries = deserialize_entries(&data)?;
        Ok(Box::new(BTreeReader { entries }))
    }

    fn merge(
        &self,
        readers: Vec<Box<dyn IndexReader>>,
        builder: Box<dyn IndexBuilder>,
    ) -> IndexResult<IndexFiles> {
        let _ = readers;
        // For now, just finish the builder (merge is a future enhancement).
        builder.finish()
    }
}

/// Accumulates sorted (key, RowPosition) entries during index build.
pub struct BTreeBuilder {
    entries: Vec<(Vec<u8>, RowPosition)>,
    config: IndexConfig,
}

impl IndexBuilder for BTreeBuilder {
    fn add_row(
        &mut self,
        partition_key: &[u8],
        clustering_key: &[u8],
        cells: &[(u16, CellValue)],
    ) -> IndexResult<()> {
        let col_pos = self
            .config
            .column_positions
            .first()
            .copied()
            .ok_or_else(|| IndexError::Build("no column positions configured".into()))?;

        // Find the cell for the indexed column.
        let cell = cells
            .iter()
            .find(|(pos, _)| *pos as usize == col_pos)
            .map(|(_, cv)| cv);

        let cell = match cell {
            Some(c) => c,
            None => return Ok(()), // Column not present in this row; skip.
        };

        // Skip tombstones.
        if cell.is_tombstone() {
            return Ok(());
        }

        let key_bytes = match &cell.value {
            Some(v) => v.clone(),
            None => return Ok(()), // No value (shouldn't happen after tombstone check, but be safe).
        };

        self.entries.push((
            key_bytes,
            RowPosition {
                partition_key: partition_key.to_vec(),
                clustering_key: clustering_key.to_vec(),
            },
        ));

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> IndexResult<IndexFiles> {
        // Sort by key bytes for binary search.
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
            index_type: IndexType::BTree,
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

/// Reads a B-tree index from a deserialized sorted entry vector.
pub struct BTreeReader {
    entries: Vec<(Vec<u8>, RowPosition)>,
}

impl IndexReader for BTreeReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let needle = extract_bytes(key)?;
        // Binary search to the first match, then scan forward for duplicates.
        let idx = self.entries.partition_point(|(k, _)| k.as_slice() < needle);

        let mut results = Vec::new();
        for (k, pos) in &self.entries[idx..] {
            if k.as_slice() == needle {
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
                let needle = extract_bytes(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() < needle)
            }
            Bound::Excluded(k) => {
                let needle = extract_bytes(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() <= needle)
            }
            Bound::Unbounded => 0,
        };

        let end_idx = match end {
            Bound::Included(k) => {
                let needle = extract_bytes(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() <= needle)
            }
            Bound::Excluded(k) => {
                let needle = extract_bytes(k)?;
                self.entries
                    .partition_point(|(key, _)| key.as_slice() < needle)
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
            "nearest not supported by B-tree index".into(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_bytes(key: &IndexKey) -> IndexResult<&[u8]> {
    match key {
        IndexKey::Bytes(b) => Ok(b.as_slice()),
        _ => Err(IndexError::Query(
            "B-tree index only supports Bytes keys".into(),
        )),
    }
}

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

/// Simple CRC-32 (Castagnoli / CRC-32C) using a lookup table.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let idx = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = CRC32_TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

/// Pre-computed CRC-32C table (Castagnoli polynomial 0x1EDC6F41).
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
            index_type: IndexType::BTree,
            column_positions: vec![0],
            output_dir: dir.to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_email".into(),
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
        let factory = BTreeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"alice"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"bob"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"alice"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.lookup(&IndexKey::Bytes(b"alice".to_vec())).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk1");
        assert_eq!(results[1].partition_key, b"pk3");

        let results = reader.lookup(&IndexKey::Bytes(b"bob".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");

        let results = reader
            .lookup(&IndexKey::Bytes(b"charlie".to_vec()))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn range_scan() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = BTreeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"aaa"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"bbb"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"ccc"))])
            .unwrap();
        builder
            .add_row(b"pk4", b"ck4", &[(0, cell(b"ddd"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Inclusive range [bbb, ccc]
        let results = reader
            .range(
                Bound::Included(&IndexKey::Bytes(b"bbb".to_vec())),
                Bound::Included(&IndexKey::Bytes(b"ccc".to_vec())),
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk2");
        assert_eq!(results[1].partition_key, b"pk3");

        // Exclusive start (bbb, ccc]
        let results = reader
            .range(
                Bound::Excluded(&IndexKey::Bytes(b"bbb".to_vec())),
                Bound::Included(&IndexKey::Bytes(b"ccc".to_vec())),
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk3");

        // Unbounded start
        let results = reader
            .range(
                Bound::Unbounded,
                Bound::Excluded(&IndexKey::Bytes(b"ccc".to_vec())),
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk1");
        assert_eq!(results[1].partition_key, b"pk2");

        // Fully unbounded
        let results = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = BTreeIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        assert_eq!(files.meta.row_count, 0);
        let results = reader
            .lookup(&IndexKey::Bytes(b"anything".to_vec()))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn tombstone_skip() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = BTreeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"alive"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, tombstone_cell())])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"also_alive"))])
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 2);

        let reader = factory.open_reader(&files).unwrap();
        let results = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn duplicate_keys() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = BTreeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"same"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"same"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"same"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.lookup(&IndexKey::Bytes(b"same".to_vec())).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn nearest_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = BTreeIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let result = reader.nearest(&[1.0, 2.0], 5, None);
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn capabilities_correct() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = BTreeIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
        assert!(!caps.contains(IndexCapabilities::PHONETIC));
    }
}
