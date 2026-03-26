//! Phonetic encoding algorithms and phonetic index factory.
//!
//! Phonetic indexes allow fuzzy name matching: "Smith" and "Smythe" both
//! encode to the same Soundex code "S530", so a phonetic index lookup for
//! either name will return both rows.

pub mod caverphone;
pub mod double_metaphone;
pub mod metaphone;
pub mod soundex;

use std::collections::HashMap;
use std::ops::Bound;
use std::path::PathBuf;

use ferrosa_common::CellValue;

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
};

// ── PhoneticEncoder trait ────────────────────────────────────────────────────

/// A phonetic encoding algorithm that maps strings to pronunciation-based codes.
pub trait PhoneticEncoder: Send + Sync {
    /// Encode the input string into a phonetic code.
    fn encode(&self, input: &str) -> String;
}

// ── Algorithm selection ──────────────────────────────────────────────────────

/// Which phonetic algorithm to use.
#[derive(Debug, Clone, Copy)]
pub enum PhoneticAlgorithm {
    Soundex,
    Metaphone,
    DoubleMetaphone,
    Caverphone,
}

impl PhoneticAlgorithm {
    pub fn encoder(&self) -> Box<dyn PhoneticEncoder> {
        match self {
            PhoneticAlgorithm::Soundex => Box::new(soundex::SoundexEncoder),
            PhoneticAlgorithm::Metaphone => Box::new(metaphone::MetaphoneEncoder),
            PhoneticAlgorithm::DoubleMetaphone => {
                Box::new(double_metaphone::DoubleMetaphoneEncoder)
            }
            PhoneticAlgorithm::Caverphone => Box::new(caverphone::CaverphoneEncoder),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            PhoneticAlgorithm::Soundex => "soundex",
            PhoneticAlgorithm::Metaphone => "metaphone",
            PhoneticAlgorithm::DoubleMetaphone => "double_metaphone",
            PhoneticAlgorithm::Caverphone => "caverphone",
        }
    }
}

// ── PhoneticIndexFactory ─────────────────────────────────────────────────────

/// Factory that builds and reads phonetic indexes.
///
/// During build, each row's cell value at the configured column is interpreted
/// as UTF-8 text, encoded with the chosen phonetic algorithm, and the mapping
/// `(phonetic_code -> Vec<RowPosition>)` is persisted to a binary file.
pub struct PhoneticIndexFactory {
    algorithm: PhoneticAlgorithm,
}

impl PhoneticIndexFactory {
    pub fn new(algorithm: PhoneticAlgorithm) -> Self {
        Self { algorithm }
    }
}

impl IndexFactory for PhoneticIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let file_path = config.output_dir.join(format!(
            "{}.phonetic_{}.idx",
            config.name,
            self.algorithm.name()
        ));
        Ok(Box::new(PhoneticBuilder {
            encoder: self.algorithm.encoder(),
            entries: HashMap::new(),
            file_path,
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data.path)?;
        let entries = deserialize_phonetic(&data)?;
        Ok(Box::new(PhoneticReader {
            encoder: self.algorithm.encoder(),
            entries,
        }))
    }

    fn index_type(&self) -> IndexType {
        IndexType::Phonetic
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::PHONETIC
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

struct PhoneticBuilder {
    encoder: Box<dyn PhoneticEncoder>,
    entries: HashMap<String, Vec<RowPosition>>,
    file_path: PathBuf,
}

impl IndexBuilder for PhoneticBuilder {
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
        let value = match &cell.value {
            Some(v) => v,
            None => return Ok(()), // skip tombstones
        };

        // Interpret cell value as UTF-8 text
        if let Ok(text) = std::str::from_utf8(value) {
            if !text.is_empty() {
                let code = self.encoder.encode(text);
                if !code.is_empty() {
                    self.entries.entry(code).or_default().push(RowPosition {
                        partition_key: partition_key.to_vec(),
                        clustering_key: clustering_key.to_vec(),
                    });
                }
            }
        }

        Ok(())
    }

    fn finish(self: Box<Self>) -> IndexResult<IndexFiles> {
        let data = serialize_phonetic(&self.entries);
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

struct PhoneticReader {
    encoder: Box<dyn PhoneticEncoder>,
    entries: HashMap<String, Vec<RowPosition>>,
}

impl IndexReader for PhoneticReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        let text = std::str::from_utf8(&key.0)
            .map_err(|e| IndexError::Corrupt(format!("invalid UTF-8 in lookup key: {e}")))?;
        let code = self.encoder.encode(text);
        Ok(self.entries.get(&code).cloned().unwrap_or_default())
    }

    fn range(
        &self,
        _start: Bound<&IndexKey>,
        _end: Bound<&IndexKey>,
    ) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "phonetic indexes do not support range scans".to_string(),
        ))
    }

    fn nearest(&self, _key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
        Err(IndexError::Unsupported(
            "phonetic indexes do not support nearest lookup".to_string(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::PHONETIC
    }
}

// ── Serialization ────────────────────────────────────────────────────────────

/// Binary format:
/// ```text
/// num_codes: u32 LE
/// for each code:
///     code_len: u32 LE
///     code_bytes
///     num_positions: u32 LE
///     for each position:
///         pk_len: u32 LE, pk_bytes
///         ck_len: u32 LE, ck_bytes
/// ```
fn serialize_phonetic(entries: &HashMap<String, Vec<RowPosition>>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    for (code, positions) in entries {
        let code_bytes = code.as_bytes();
        buf.extend_from_slice(&(code_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(code_bytes);

        buf.extend_from_slice(&(positions.len() as u32).to_le_bytes());
        for pos in positions {
            buf.extend_from_slice(&(pos.partition_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&pos.partition_key);
            buf.extend_from_slice(&(pos.clustering_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&pos.clustering_key);
        }
    }

    buf
}

fn deserialize_phonetic(data: &[u8]) -> IndexResult<HashMap<String, Vec<RowPosition>>> {
    if data.len() < 4 {
        return Err(IndexError::Corrupt(
            "phonetic index file too short".to_string(),
        ));
    }

    let num_codes = read_u32(data, 0)? as usize;
    let mut offset = 4;
    let mut entries = HashMap::with_capacity(num_codes);

    for _ in 0..num_codes {
        let code_bytes = read_len_prefixed(data, &mut offset)?;
        let code = String::from_utf8(code_bytes)
            .map_err(|e| IndexError::Corrupt(format!("invalid UTF-8 in phonetic code: {e}")))?;

        let num_positions = read_u32(data, offset)? as usize;
        offset += 4;

        let mut positions = Vec::with_capacity(num_positions);
        for _ in 0..num_positions {
            let pk = read_len_prefixed(data, &mut offset)?;
            let ck = read_len_prefixed(data, &mut offset)?;
            positions.push(RowPosition {
                partition_key: pk,
                clustering_key: ck,
            });
        }

        entries.insert(code, positions);
    }

    Ok(entries)
}

fn read_u32(data: &[u8], offset: usize) -> IndexResult<u32> {
    if offset + 4 > data.len() {
        return Err(IndexError::Corrupt(format!(
            "unexpected EOF at offset {offset} reading u32"
        )));
    }
    Ok(u32::from_le_bytes(
        data[offset..offset + 4].try_into().unwrap(),
    ))
}

fn read_len_prefixed(data: &[u8], offset: &mut usize) -> IndexResult<Vec<u8>> {
    let len = read_u32(data, *offset)? as usize;
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::CellValue;
    use tempfile::tempdir;

    #[test]
    fn phonetic_index_matches_similar_names() {
        let dir = tempdir().unwrap();
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let config = IndexConfig {
            index_type: IndexType::Phonetic,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_phonetic".to_string(),
        };

        let mut builder = factory.create_builder(&config).unwrap();

        // Add rows with similar-sounding names
        builder
            .add_row(b"pk1", b"", &[CellValue::live(b"Smith".to_vec(), 1)], &[0])
            .unwrap();
        builder
            .add_row(b"pk2", b"", &[CellValue::live(b"Smythe".to_vec(), 2)], &[0])
            .unwrap();
        builder
            .add_row(b"pk3", b"", &[CellValue::live(b"Jones".to_vec(), 3)], &[0])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        assert!(reader
            .capabilities()
            .contains(IndexCapabilities::POINT_LOOKUP));
        assert!(reader.capabilities().contains(IndexCapabilities::PHONETIC));

        // "Smyth" should match both Smith and Smythe (all encode to S530)
        let mut results = reader.lookup(&IndexKey(b"Smyth".to_vec())).unwrap();
        results.sort_by(|a, b| a.partition_key.cmp(&b.partition_key));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].partition_key, b"pk1");
        assert_eq!(results[1].partition_key, b"pk2");

        // "Jones" should only match itself
        let results = reader.lookup(&IndexKey(b"Jones".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk3");

        // Range should return unsupported
        assert!(reader.range(Bound::Unbounded, Bound::Unbounded).is_err());
    }

    #[test]
    fn phonetic_index_skips_tombstones() {
        let dir = tempdir().unwrap();
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let config = IndexConfig {
            index_type: IndexType::Phonetic,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_tombstone".to_string(),
        };

        let mut builder = factory.create_builder(&config).unwrap();
        builder
            .add_row(b"pk1", b"", &[CellValue::live(b"Smith".to_vec(), 1)], &[0])
            .unwrap();
        builder
            .add_row(b"pk2", b"", &[CellValue::tombstone(2, 1700000000)], &[0])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.lookup(&IndexKey(b"Smith".to_vec())).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");
    }

    #[test]
    fn phonetic_index_with_metaphone() {
        let dir = tempdir().unwrap();
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Metaphone);
        let config = IndexConfig {
            index_type: IndexType::Phonetic,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_metaphone".to_string(),
        };

        let mut builder = factory.create_builder(&config).unwrap();
        builder
            .add_row(b"pk1", b"", &[CellValue::live(b"Smith".to_vec(), 1)], &[0])
            .unwrap();
        builder
            .add_row(b"pk2", b"", &[CellValue::live(b"Smythe".to_vec(), 2)], &[0])
            .unwrap();

        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        // Metaphone: Smith and Smythe should encode the same
        let results = reader.lookup(&IndexKey(b"Smith".to_vec())).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn phonetic_empty_index() {
        let dir = tempdir().unwrap();
        let factory = PhoneticIndexFactory::new(PhoneticAlgorithm::Soundex);
        let config = IndexConfig {
            index_type: IndexType::Phonetic,
            column_positions: vec![0],
            output_dir: dir.path().to_path_buf(),
            name: "test_empty".to_string(),
        };

        let builder = factory.create_builder(&config).unwrap();
        let files = builder.finish().unwrap();
        let reader = factory.open_reader(&files).unwrap();

        let results = reader.lookup(&IndexKey(b"anything".to_vec())).unwrap();
        assert!(results.is_empty());
    }
}
