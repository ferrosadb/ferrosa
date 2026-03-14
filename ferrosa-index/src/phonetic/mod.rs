//! Phonetic index: encodes text values using phonetic algorithms for
//! fuzzy name matching.
//!
//! Supports [`IndexCapabilities::POINT_LOOKUP`] and [`IndexCapabilities::PHONETIC`].
//! Lookups encode the query text with the same algorithm and find all rows
//! whose indexed column produced the same phonetic code.

pub mod caverphone;
pub mod double_metaphone;
pub mod metaphone;
pub mod soundex;

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, PhoneticAlgorithm, RowPosition,
};
use ferrosa_common::CellValue;
use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

/// Trait for phonetic encoding algorithms.
pub trait PhoneticEncoder: Send + Sync {
    fn encode(&self, input: &str) -> String;
}

/// Factory for creating phonetic index builders and readers.
pub struct PhoneticIndexFactory {
    algorithm: PhoneticAlgorithm,
}

impl PhoneticIndexFactory {
    pub fn new(algorithm: PhoneticAlgorithm) -> Self {
        Self { algorithm }
    }

    fn create_encoder(algorithm: &PhoneticAlgorithm) -> Box<dyn PhoneticEncoder> {
        match algorithm {
            PhoneticAlgorithm::Soundex => Box::new(soundex::Soundex),
            PhoneticAlgorithm::Metaphone => Box::new(metaphone::Metaphone::default()),
            PhoneticAlgorithm::DoubleMetaphone => {
                Box::new(double_metaphone::DoubleMetaphone::default())
            }
            PhoneticAlgorithm::Caverphone => Box::new(caverphone::Caverphone),
        }
    }
}

impl IndexFactory for PhoneticIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        Ok(Box::new(PhoneticBuilder {
            entries: Vec::new(),
            config: config.clone(),
            encoder: Self::create_encoder(&self.algorithm),
            algorithm: self.algorithm.clone(),
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data_path)?;
        let entries = deserialize_entries(&data)?;
        let mut map: HashMap<String, Vec<RowPosition>> = HashMap::new();
        for (code, pos) in entries {
            map.entry(code).or_default().push(pos);
        }
        Ok(Box::new(PhoneticReader {
            map,
            encoder: Self::create_encoder(&self.algorithm),
        }))
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

/// Accumulates (phonetic_code, RowPosition) entries during index build.
struct PhoneticBuilder {
    entries: Vec<(String, RowPosition)>,
    config: IndexConfig,
    encoder: Box<dyn PhoneticEncoder>,
    algorithm: PhoneticAlgorithm,
}

impl IndexBuilder for PhoneticBuilder {
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

        let value_bytes = match &cell.value {
            Some(v) => v,
            None => return Ok(()),
        };

        // Interpret cell bytes as UTF-8 text for phonetic encoding.
        let text = match std::str::from_utf8(value_bytes) {
            Ok(s) => s,
            Err(_) => return Ok(()), // Non-UTF-8 data cannot be phonetically encoded; skip.
        };

        let code = self.encoder.encode(text);
        if code.is_empty() {
            return Ok(());
        }

        self.entries.push((
            code,
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
            index_type: IndexType::Phonetic {
                algorithm: self.algorithm.clone(),
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

/// Reads a phonetic index: on lookup, encodes query text and finds matching rows.
struct PhoneticReader {
    map: HashMap<String, Vec<RowPosition>>,
    encoder: Box<dyn PhoneticEncoder>,
}

impl IndexReader for PhoneticReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let text = extract_text(key)?;
        let code = self.encoder.encode(text);
        if code.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.map.get(&code).cloned().unwrap_or_default())
    }

    fn range(
        &self,
        _start: Bound<&IndexKey>,
        _end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "range scan not supported by phonetic index".into(),
        ))
    }

    fn nearest(
        &self,
        _query: &[f32],
        _k: usize,
        _ef_search: Option<u16>,
    ) -> IndexResult<Vec<(RowPosition, f32)>> {
        Err(IndexError::Unsupported(
            "nearest not supported by phonetic index".into(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::PHONETIC
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_text(key: &IndexKey) -> IndexResult<&str> {
    match key {
        IndexKey::Text(s) => Ok(s.as_str()),
        IndexKey::Bytes(b) => std::str::from_utf8(b)
            .map_err(|e| IndexError::Query(format!("phonetic index requires UTF-8 text: {e}"))),
        _ => Err(IndexError::Query(
            "phonetic index only supports Text or Bytes keys".into(),
        )),
    }
}

/// Serialization format for phonetic entries:
/// ```text
/// entry_count: u64
/// entries[]:
///   code_len: u32
///   code:     [u8; code_len]   (UTF-8 phonetic code)
///   pk_len:   u32
///   pk:       [u8; pk_len]
///   ck_len:   u32
///   ck:       [u8; ck_len]
/// ```
fn serialize_entries(entries: &[(String, RowPosition)]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    buf.write_all(&(entries.len() as u64).to_le_bytes())
        .unwrap();
    for (code, pos) in entries {
        let code_bytes = code.as_bytes();
        buf.write_all(&(code_bytes.len() as u32).to_le_bytes())
            .unwrap();
        buf.write_all(code_bytes).unwrap();
        buf.write_all(&(pos.partition_key.len() as u32).to_le_bytes())
            .unwrap();
        buf.write_all(&pos.partition_key).unwrap();
        buf.write_all(&(pos.clustering_key.len() as u32).to_le_bytes())
            .unwrap();
        buf.write_all(&pos.clustering_key).unwrap();
    }
    buf
}

fn deserialize_entries(data: &[u8]) -> IndexResult<Vec<(String, RowPosition)>> {
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
            .map_err(|e| IndexError::Query(format!("read code_len: {e}")))?;
        let code_len = u32::from_le_bytes(buf4) as usize;
        let mut code_bytes = vec![0u8; code_len];
        cursor
            .read_exact(&mut code_bytes)
            .map_err(|e| IndexError::Query(format!("read code: {e}")))?;
        let code = String::from_utf8(code_bytes)
            .map_err(|e| IndexError::Query(format!("invalid UTF-8 in code: {e}")))?;

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
            code,
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

    fn make_config(dir: &std::path::Path, algorithm: PhoneticAlgorithm) -> IndexConfig {
        IndexConfig {
            index_type: IndexType::Phonetic { algorithm },
            column_positions: vec![0],
            output_dir: dir.to_path_buf(),
            sstable_prefix: "sstable-001".into(),
            index_name: "idx_name_phonetic".into(),
        }
    }

    fn cell(value: &[u8]) -> CellValue {
        CellValue::live(value.to_vec(), 1000)
    }

    fn tombstone_cell() -> CellValue {
        CellValue::tombstone(1000, 1_700_000_000)
    }

    #[test]
    fn phonetic_index_matches_similar_names() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Soundex);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Smith"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Smythe"))])
            .unwrap();
        builder
            .add_row(b"pk3", b"ck3", &[(0, cell(b"Johnson"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Looking up "Smith" should find both Smith and Smythe (same Soundex S530)
        let results = reader.lookup(&IndexKey::Text("Smith".into())).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.partition_key == b"pk1"));
        assert!(results.iter().any(|r| r.partition_key == b"pk2"));

        // Looking up "Smythe" should also find both
        let results = reader.lookup(&IndexKey::Text("Smythe".into())).unwrap();
        assert_eq!(results.len(), 2);

        // Looking up "Johnson" should find only Johnson
        let results = reader.lookup(&IndexKey::Text("Johnson".into())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk3");

        // Looking up a non-matching name
        let results = reader.lookup(&IndexKey::Text("Zzzzz".into())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn phonetic_index_with_metaphone() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Metaphone);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Metaphone);
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Smith"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Smyth"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Smith and Smyth produce the same Metaphone code
        let results = reader.lookup(&IndexKey::Text("Smith".into())).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn phonetic_index_with_double_metaphone() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::DoubleMetaphone);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::DoubleMetaphone);
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Smith"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Smyth"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.lookup(&IndexKey::Text("Smith".into())).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn phonetic_index_with_caverphone() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Caverphone);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Caverphone);
        let mut builder = factory.create_builder(&config).unwrap();

        // Smith and Smyth produce the same Caverphone code
        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Smith"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, cell(b"Smyth"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Smith and Smyth should match with Caverphone
        let results = reader.lookup(&IndexKey::Text("Smith".into())).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Soundex);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        assert_eq!(files.meta.row_count, 0);
        let results = reader.lookup(&IndexKey::Text("anything".into())).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn tombstone_skip() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Soundex);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Alice"))])
            .unwrap();
        builder
            .add_row(b"pk2", b"ck2", &[(0, tombstone_cell())])
            .unwrap();

        let files = builder.finish().unwrap();
        assert_eq!(files.meta.row_count, 1);
    }

    #[test]
    fn bytes_key_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Soundex);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let mut builder = factory.create_builder(&config).unwrap();

        builder
            .add_row(b"pk1", b"ck1", &[(0, cell(b"Smith"))])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Should also work with Bytes key containing UTF-8
        let results = reader.lookup(&IndexKey::Bytes(b"Smith".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn range_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Soundex);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let result = reader.range(Bound::Unbounded, Bound::Unbounded);
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn nearest_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Soundex);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let result = reader.nearest(&[1.0, 2.0], 5, None);
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn capabilities_correct() {
        let dir = tempfile::tempdir().unwrap();
        let config = make_config(dir.path(), PhoneticAlgorithm::Soundex);
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::PHONETIC));
        assert!(!caps.contains(IndexCapabilities::RANGE_SCAN));
        assert!(!caps.contains(IndexCapabilities::NEAREST));
    }
}
