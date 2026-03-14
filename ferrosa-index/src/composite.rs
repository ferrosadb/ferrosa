//! Composite multi-column secondary index.
//!
//! Like the B-tree index but extracts multiple columns from each row and
//! concatenates their bytes with length prefixes to form a composite key.
//! Supports both full-key point lookups and prefix range scans.
//!
//! ## Composite key encoding
//!
//! For columns at positions `[c0, c1, c2, ...]`, the composite key is:
//!
//! ```text
//! col_count (u16 LE) |
//! len_0 (u32 LE) | bytes_0 |
//! len_1 (u32 LE) | bytes_1 |
//! ...
//! ```
//!
//! This encoding preserves lexicographic ordering when all component columns
//! have equal-length values, and always supports exact prefix matching.
//!
//! ## File format
//!
//! Same as B-tree: header (entry_count: u64) + sorted entries.

use crate::{
    IndexBuilder, IndexCapabilities, IndexConfig, IndexError, IndexFactory, IndexFileMeta,
    IndexFiles, IndexKey, IndexReader, IndexResult, IndexType, RowPosition,
};
use ferrosa_common::CellValue;
use std::ops::Bound;
use std::path::PathBuf;

/// Entry stored in the composite index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CompositeEntry {
    key: Vec<u8>,
    position: RowPosition,
}

// ── Public helpers ───────────────────────────────────────────────────────────

/// Encode a composite key from individual column values.
///
/// The resulting bytes can be used as an `IndexKey` for lookups.
pub fn encode_composite_key(values: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for v in values {
        buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
        buf.extend_from_slice(v);
    }
    buf
}

/// Encode a prefix key (fewer columns than the full composite). Used for
/// prefix range scans.
pub fn encode_prefix_key(values: &[&[u8]]) -> Vec<u8> {
    // Same encoding, just fewer columns
    encode_composite_key(values)
}

/// Decode a composite key into its component column values.
fn decode_composite_key(data: &[u8]) -> IndexResult<Vec<Vec<u8>>> {
    if data.len() < 2 {
        return Err(IndexError::Corrupt(
            "composite key too short for column count".to_string(),
        ));
    }
    let col_count = u16::from_le_bytes(data[..2].try_into().unwrap()) as usize;
    let mut offset = 2;
    let mut values = Vec::with_capacity(col_count);

    for _ in 0..col_count {
        if offset + 4 > data.len() {
            return Err(IndexError::Corrupt(
                "composite key truncated reading length".to_string(),
            ));
        }
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > data.len() {
            return Err(IndexError::Corrupt(
                "composite key truncated reading value".to_string(),
            ));
        }
        values.push(data[offset..offset + len].to_vec());
        offset += len;
    }

    Ok(values)
}

/// Extract the first `n` columns from a composite key and re-encode them
/// as a prefix key (with col_count = n).
fn extract_prefix_bytes(full_key: &[u8], n: usize) -> IndexResult<Vec<u8>> {
    let cols = decode_composite_key(full_key)?;
    if n > cols.len() {
        return Err(IndexError::Corrupt(format!(
            "requested {n} prefix columns but key has only {}",
            cols.len()
        )));
    }
    let refs: Vec<&[u8]> = cols[..n].iter().map(|v| v.as_slice()).collect();
    Ok(encode_composite_key(&refs))
}

/// Check if `full_key` starts with the columns in `prefix_key`.
fn key_has_prefix(full_key: &[u8], prefix_key: &[u8]) -> IndexResult<bool> {
    let full_cols = decode_composite_key(full_key)?;
    let prefix_cols = decode_composite_key(prefix_key)?;

    if prefix_cols.len() > full_cols.len() {
        return Ok(false);
    }

    for (f, p) in full_cols.iter().zip(prefix_cols.iter()) {
        if f != p {
            return Ok(false);
        }
    }

    Ok(true)
}

// ── Factory ──────────────────────────────────────────────────────────────────

/// Factory for creating composite index builders and readers.
pub struct CompositeIndexFactory;

impl IndexFactory for CompositeIndexFactory {
    fn create_builder(&self, config: &IndexConfig) -> IndexResult<Box<dyn IndexBuilder>> {
        let file_path = config.output_dir.join(format!("{}.composite", config.name));
        Ok(Box::new(CompositeBuilder {
            entries: Vec::new(),
            file_path,
        }))
    }

    fn open_reader(&self, files: &IndexFiles) -> IndexResult<Box<dyn IndexReader>> {
        let data = std::fs::read(&files.data.path)?;
        let entries = deserialize_entries(&data)?;
        Ok(Box::new(CompositeReader { entries }))
    }

    fn index_type(&self) -> IndexType {
        IndexType::Composite
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Accumulates rows and writes a sorted composite index file.
pub struct CompositeBuilder {
    entries: Vec<CompositeEntry>,
    file_path: PathBuf,
}

impl IndexBuilder for CompositeBuilder {
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

        // Extract values from all indexed columns; skip if any column is a tombstone
        let mut col_values: Vec<&[u8]> = Vec::with_capacity(column_positions.len());
        for &col_pos in column_positions {
            if col_pos >= cells.len() {
                return Err(IndexError::MissingColumn(col_pos));
            }
            let cell = &cells[col_pos];
            match &cell.value {
                Some(v) => col_values.push(v.as_slice()),
                None => return Ok(()), // skip row if any indexed column is a tombstone
            }
        }

        let composite_key = encode_composite_key(&col_values);

        self.entries.push(CompositeEntry {
            key: composite_key,
            position: RowPosition {
                partition_key: partition_key.to_vec(),
                clustering_key: clustering_key.to_vec(),
            },
        });

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> IndexResult<IndexFiles> {
        // Sort by composite key for binary search and prefix scans
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

/// Reads a sorted composite index, supporting full-key lookups and prefix
/// range scans.
pub struct CompositeReader {
    entries: Vec<CompositeEntry>,
}

impl IndexReader for CompositeReader {
    fn lookup(&self, key: &IndexKey) -> IndexResult<Vec<RowPosition>> {
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
            "nearest lookup not supported by composite index".to_string(),
        ))
    }

    fn capabilities(&self) -> IndexCapabilities {
        IndexCapabilities::POINT_LOOKUP | IndexCapabilities::RANGE_SCAN
    }
}

impl CompositeReader {
    /// Prefix scan: return all rows whose composite key starts with the given
    /// prefix columns.
    ///
    /// This extracts the first N columns from each entry's composite key and
    /// uses binary search on those extracted columns to efficiently find all
    /// matching entries.
    pub fn prefix_scan(&self, prefix_values: &[&[u8]]) -> IndexResult<Vec<RowPosition>> {
        let prefix_key = encode_prefix_key(prefix_values);

        // We can't use raw byte comparison for prefix search because the
        // composite key encoding includes the full column count in its header.
        // Instead, we extract the first N columns from each entry's key and
        // compare those against the prefix.
        //
        // Use binary search on the extracted prefix columns. We build a
        // comparator that decodes each entry's first N columns and compares
        // them to the prefix.
        let start_idx = self.entries.partition_point(|e| {
            extract_prefix_bytes(&e.key, prefix_values.len())
                .map(|extracted| extracted.as_slice() < prefix_key.as_slice())
                .unwrap_or(false)
        });

        let mut results = Vec::new();
        for entry in &self.entries[start_idx..] {
            match key_has_prefix(&entry.key, &prefix_key) {
                Ok(true) => results.push(entry.position.clone()),
                Ok(false) => {
                    // Check if we've gone past all possible matches.
                    // Entries are sorted by full composite key, and entries
                    // with the same prefix columns are contiguous when those
                    // columns sort the same way. We compare extracted prefix
                    // to know if we've passed the prefix range.
                    let extracted = extract_prefix_bytes(&entry.key, prefix_values.len()).ok();
                    if let Some(ref ex) = extracted {
                        if ex.as_slice() > prefix_key.as_slice() {
                            break;
                        }
                    }
                    // If extracted prefix sorts equal but full key doesn't
                    // match (shouldn't happen), or if extraction fails, skip.
                }
                Err(e) => return Err(e),
            }
        }

        Ok(results)
    }
}

// ── Serialization ────────────────────────────────────────────────────────────

fn serialize_entries(entries: &[CompositeEntry]) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());

    for entry in entries {
        buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.key);

        buf.extend_from_slice(&(entry.position.partition_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.position.partition_key);

        buf.extend_from_slice(&(entry.position.clustering_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.position.clustering_key);
    }

    buf
}

fn deserialize_entries(data: &[u8]) -> IndexResult<Vec<CompositeEntry>> {
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

        entries.push(CompositeEntry {
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

    /// Helper: build a composite index from rows and return the reader
    /// (as a CompositeReader so we can call prefix_scan).
    fn build_composite(
        rows: Vec<(&[u8], &[u8], Vec<CellValue>)>,
        column_positions: &[usize],
    ) -> CompositeReader {
        let dir = tempdir().unwrap();
        let config = IndexConfig {
            index_type: IndexType::Composite,
            column_positions: column_positions.to_vec(),
            output_dir: dir.path().to_path_buf(),
            name: "test_composite".to_string(),
        };
        let factory = CompositeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        for (pk, ck, cells) in &rows {
            builder.add_row(pk, ck, cells, column_positions).unwrap();
        }

        let files = builder.finish().unwrap();
        let data = std::fs::read(&files.data.path).unwrap();
        let entries = deserialize_entries(&data).unwrap();
        CompositeReader { entries }
    }

    /// Helper: build via factory and return a trait-object reader.
    fn build_and_read(
        rows: Vec<(&[u8], &[u8], Vec<CellValue>)>,
        column_positions: &[usize],
    ) -> Box<dyn IndexReader> {
        let dir = tempdir().unwrap();
        let config = IndexConfig {
            index_type: IndexType::Composite,
            column_positions: column_positions.to_vec(),
            output_dir: dir.path().to_path_buf(),
            name: "test_composite".to_string(),
        };
        let factory = CompositeIndexFactory;
        let mut builder = factory.create_builder(&config).unwrap();

        for (pk, ck, cells) in &rows {
            builder.add_row(pk, ck, cells, column_positions).unwrap();
        }

        let files = builder.finish().unwrap();
        factory.open_reader(&files).unwrap()
    }

    #[test]
    fn full_composite_key_lookup() {
        let reader = build_and_read(
            vec![
                (
                    b"pk1",
                    b"ck1",
                    vec![
                        CellValue::live(b"US".to_vec(), 1),
                        CellValue::live(b"NY".to_vec(), 1),
                    ],
                ),
                (
                    b"pk2",
                    b"ck2",
                    vec![
                        CellValue::live(b"US".to_vec(), 2),
                        CellValue::live(b"CA".to_vec(), 2),
                    ],
                ),
                (
                    b"pk3",
                    b"ck3",
                    vec![
                        CellValue::live(b"UK".to_vec(), 3),
                        CellValue::live(b"LN".to_vec(), 3),
                    ],
                ),
            ],
            &[0, 1],
        );

        // Look up (US, NY)
        let key = IndexKey(encode_composite_key(&[b"US", b"NY"]));
        let results = reader.lookup(&key).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");
    }

    #[test]
    fn full_composite_key_not_found() {
        let reader = build_and_read(
            vec![(
                b"pk1",
                b"ck1",
                vec![
                    CellValue::live(b"US".to_vec(), 1),
                    CellValue::live(b"NY".to_vec(), 1),
                ],
            )],
            &[0, 1],
        );

        let key = IndexKey(encode_composite_key(&[b"US", b"TX"]));
        let results = reader.lookup(&key).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn prefix_range_scan() {
        let reader = build_composite(
            vec![
                (
                    b"pk1",
                    b"ck1",
                    vec![
                        CellValue::live(b"US".to_vec(), 1),
                        CellValue::live(b"CA".to_vec(), 1),
                    ],
                ),
                (
                    b"pk2",
                    b"ck2",
                    vec![
                        CellValue::live(b"US".to_vec(), 2),
                        CellValue::live(b"NY".to_vec(), 2),
                    ],
                ),
                (
                    b"pk3",
                    b"ck3",
                    vec![
                        CellValue::live(b"US".to_vec(), 3),
                        CellValue::live(b"TX".to_vec(), 3),
                    ],
                ),
                (
                    b"pk4",
                    b"ck4",
                    vec![
                        CellValue::live(b"UK".to_vec(), 4),
                        CellValue::live(b"LN".to_vec(), 4),
                    ],
                ),
            ],
            &[0, 1],
        );

        // Prefix scan for country = "US"
        let results = reader.prefix_scan(&[b"US"]).unwrap();
        assert_eq!(results.len(), 3);
        let pks: Vec<&[u8]> = results.iter().map(|r| r.partition_key.as_slice()).collect();
        assert!(pks.contains(&b"pk1".as_slice()));
        assert!(pks.contains(&b"pk2".as_slice()));
        assert!(pks.contains(&b"pk3".as_slice()));
    }

    #[test]
    fn prefix_scan_no_match() {
        let reader = build_composite(
            vec![(
                b"pk1",
                b"ck1",
                vec![
                    CellValue::live(b"US".to_vec(), 1),
                    CellValue::live(b"NY".to_vec(), 1),
                ],
            )],
            &[0, 1],
        );

        let results = reader.prefix_scan(&[b"DE"]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn empty_index() {
        let reader = build_and_read(vec![], &[0, 1]);

        let key = IndexKey(encode_composite_key(&[b"any", b"thing"]));
        let results = reader.lookup(&key).unwrap();
        assert!(results.is_empty());

        let range = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert!(range.is_empty());
    }

    #[test]
    fn tombstone_in_any_column_skips_row() {
        let reader = build_and_read(
            vec![
                (
                    b"pk1",
                    b"ck1",
                    vec![
                        CellValue::live(b"US".to_vec(), 1),
                        CellValue::live(b"NY".to_vec(), 1),
                    ],
                ),
                // Second column is a tombstone - row should be skipped
                (
                    b"pk2",
                    b"ck2",
                    vec![
                        CellValue::live(b"US".to_vec(), 2),
                        CellValue::tombstone(2, 1700000000),
                    ],
                ),
                // First column is a tombstone - row should be skipped
                (
                    b"pk3",
                    b"ck3",
                    vec![
                        CellValue::tombstone(3, 1700000000),
                        CellValue::live(b"LN".to_vec(), 3),
                    ],
                ),
            ],
            &[0, 1],
        );

        let all = reader.range(Bound::Unbounded, Bound::Unbounded).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].partition_key, b"pk1");
    }

    #[test]
    fn multiple_rows_with_same_composite_key() {
        let reader = build_and_read(
            vec![
                (
                    b"pk1",
                    b"ck1",
                    vec![
                        CellValue::live(b"US".to_vec(), 1),
                        CellValue::live(b"NY".to_vec(), 1),
                    ],
                ),
                (
                    b"pk2",
                    b"ck2",
                    vec![
                        CellValue::live(b"US".to_vec(), 2),
                        CellValue::live(b"NY".to_vec(), 2),
                    ],
                ),
            ],
            &[0, 1],
        );

        let key = IndexKey(encode_composite_key(&[b"US", b"NY"]));
        let results = reader.lookup(&key).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn capabilities_include_point_and_range() {
        let reader = build_and_read(vec![], &[0, 1]);
        let caps = reader.capabilities();
        assert!(caps.contains(IndexCapabilities::POINT_LOOKUP));
        assert!(caps.contains(IndexCapabilities::RANGE_SCAN));
    }

    #[test]
    fn nearest_returns_unsupported() {
        let reader = build_and_read(vec![], &[0, 1]);
        let key = IndexKey(encode_composite_key(&[b"any"]));
        let result = reader.nearest(&key);
        assert!(matches!(result, Err(IndexError::Unsupported(_))));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let values: Vec<&[u8]> = vec![b"hello", b"world", b"test"];
        let encoded = encode_composite_key(&values);
        let decoded = decode_composite_key(&encoded).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0], b"hello");
        assert_eq!(decoded[1], b"world");
        assert_eq!(decoded[2], b"test");
    }

    #[test]
    fn three_column_composite() {
        let reader = build_composite(
            vec![
                (
                    b"pk1",
                    b"ck1",
                    vec![
                        CellValue::live(b"A".to_vec(), 1),
                        CellValue::live(b"B".to_vec(), 1),
                        CellValue::live(b"C".to_vec(), 1),
                    ],
                ),
                (
                    b"pk2",
                    b"ck2",
                    vec![
                        CellValue::live(b"A".to_vec(), 2),
                        CellValue::live(b"B".to_vec(), 2),
                        CellValue::live(b"D".to_vec(), 2),
                    ],
                ),
                (
                    b"pk3",
                    b"ck3",
                    vec![
                        CellValue::live(b"A".to_vec(), 3),
                        CellValue::live(b"X".to_vec(), 3),
                        CellValue::live(b"Y".to_vec(), 3),
                    ],
                ),
            ],
            &[0, 1, 2],
        );

        // Prefix scan with first two columns (A, B)
        let results = reader.prefix_scan(&[b"A", b"B"]).unwrap();
        assert_eq!(results.len(), 2);

        // Prefix scan with first column only (A)
        let results = reader.prefix_scan(&[b"A"]).unwrap();
        assert_eq!(results.len(), 3);

        // Full key lookup (A, B, C)
        let key = IndexKey(encode_composite_key(&[b"A", b"B", b"C"]));
        let results = reader.lookup(&key).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");
    }
}
