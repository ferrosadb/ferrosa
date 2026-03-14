//! Hash index implementation: O(1) point lookups via HashMap.
//!
//! Supports [`IndexCapabilities::POINT_LOOKUP`] only.
//! [`range()`](IndexReader::range) and [`nearest()`](IndexReader::nearest)
//! return [`IndexError::Unsupported`].

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
};
use ferrosa_common::CellValue;
use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

/// Factory for creating hash index builders and readers.
pub struct HashIndexFactory;

impl IndexFactory for HashIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        Ok(Box::new(HashBuilder {
            entries: Vec::new(),
            config: config.clone(),
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data_path)?;
        let entries = deserialize_entries(&data)?;
        let mut map: HashMap<Vec<u8>, Vec<RowPosition>> = HashMap::new();
        for (key, pos) in entries {
            map.entry(key).or_default().push(pos);
        }
        Ok(Box::new(HashReader { map }))
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

/// Accumulates entries during index build.
pub struct HashBuilder {
    entries: Vec<(Vec<u8>, RowPosition)>,
    config: IndexConfig,
}

impl IndexBuilder for HashBuilder {
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

        let cell = cells
            .iter()
            .find(|(pos, _)| *pos as usize == col_pos)
            .map(|(_, cv)| cv);

        let cell = match cell {
            Some(c) => c,
            None => return Ok(()),
        };

        if cell.is_tombstone() {
            return Ok(());
        }

        let key_bytes = match &cell.value {
            Some(v) => v.clone(),
            None => return Ok(()),
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

    fn finish(self: Box<Self>) -> IndexResult<IndexFiles> {
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
            index_type: IndexType::Hash,
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

/// Reads a hash index from a deserialized HashMap.
pub struct HashReader {
    map: HashMap<Vec<u8>, Vec<RowPosition>>,
}

impl IndexReader for HashReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let needle = extract_bytes(key)?;
        Ok(self.map.get(needle).cloned().unwrap_or_default())
    }

    fn range(
        &self,
        _start: Bound<&IndexKey>,
        _end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "range scan not supported by hash index".into(),
        ))
    }

    fn nearest(
        &self,
        _query: &[f32],
        _k: usize,
        _ef_search: Option<u16>,
    ) -> IndexResult<Vec<(RowPosition, f32)>> {
        Err(IndexError::Unsupported(
            "nearest not supported by hash index".into(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_bytes(key: &IndexKey) -> IndexResult<&[u8]> {
    match key {
        IndexKey::Bytes(b) => Ok(b.as_slice()),
        _ => Err(IndexError::Query(
            "hash index only supports Bytes keys".into(),
        )),
    }
}

/// Same binary format as btree for on-disk storage (order does not matter for hash).
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
            index_type: IndexType::Hash,
            column_positions: vec![0],
            output_dir: dir.to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_email_hash".into(),
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
        let factory = HashIndexFactory;
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

        let results = reader.lookup(&IndexKey::Bytes(b"bob".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");

        let results = reader
            .lookup(&IndexKey::Bytes(b"charlie".to_vec()))
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = HashIndexFactory;
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
        let factory = HashIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"alive"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, tombstone_cell())])
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 1);

        let reader = factory.open_reader(&files).unwrap();
        let results = reader.lookup(&IndexKey::Bytes(b"alive".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn duplicate_keys() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = HashIndexFactory;
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
    fn range_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = HashIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let result = reader.range(Bound::Unbounded, Bound::Unbounded);
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn nearest_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path());
        let factory = HashIndexFactory;
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
        let factory = HashIndexFactory;
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(!caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
    }
}
