//! SSTable file-based streaming for bulk data transfer.
//!
//! Instead of sending individual row mutations, this module transfers
//! entire SSTable component files (Data, Index, Filter, Statistics, etc.)
//! as opaque byte chunks. The receiver writes them directly to disk and
//! opens the SSTable, avoiding the serialize-per-row overhead.
//!
//! # Protocol
//!
//! Uses the same StreamStart/StreamChunk/StreamEnd message types, but the
//! `StreamedMutation` payload is repurposed:
//!
//! - `keyspace` and `table` identify the SSTable's table.
//! - `key` contains the SSTable component name (e.g., "Data.db", "Index.db").
//! - `row` contains a chunk of the component file's bytes.
//! - `timestamp` contains the byte offset within the file.
//!
//! A single streaming session transfers all components of one SSTable.
//! The receiver reassembles the file chunks by offset and opens the SSTable
//! once all components are received.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::StreamedMutation;

/// Metadata for an SSTable file transfer session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableTransferManifest {
    pub keyspace: String,
    pub table: String,
    /// The SSTable generation/identifier.
    pub sstable_id: String,
    /// List of component files with their sizes.
    pub components: Vec<SSTableComponent>,
    /// Total bytes across all components.
    pub total_bytes: u64,
}

/// A single SSTable component file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableComponent {
    /// Component name (e.g., "Data.db", "Index.db", "Filter.db").
    pub name: String,
    /// Total size of this component in bytes.
    pub size: u64,
}

/// Encode SSTable component file chunks into `StreamedMutation` payloads.
///
/// Each chunk becomes one `StreamedMutation` with:
/// - `key` = component name bytes
/// - `row` = file data chunk
/// - `timestamp` = byte offset
pub fn encode_file_chunk(
    keyspace: &str,
    table: &str,
    component_name: &str,
    offset: u64,
    data: Vec<u8>,
) -> StreamedMutation {
    StreamedMutation {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        key: component_name.as_bytes().to_vec(),
        row: data,
        timestamp: offset as i64,
    }
}

/// Decode a `StreamedMutation` back into file chunk metadata.
pub fn decode_file_chunk(mutation: &StreamedMutation) -> Option<FileChunk> {
    let component_name = String::from_utf8(mutation.key.clone()).ok()?;
    Some(FileChunk {
        component_name,
        offset: mutation.timestamp as u64,
        data: mutation.row.clone(),
    })
}

/// A decoded file chunk ready to be written to disk.
#[derive(Debug)]
pub struct FileChunk {
    pub component_name: String,
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Accumulates file chunks and writes complete SSTable components to disk.
pub struct SSTableAssembler {
    pub sstable_dir: PathBuf,
    /// Accumulated chunks per component, keyed by (component_name, offset).
    chunks: BTreeMap<String, BTreeMap<u64, Vec<u8>>>,
}

impl SSTableAssembler {
    pub fn new(sstable_dir: PathBuf) -> Self {
        Self {
            sstable_dir,
            chunks: BTreeMap::new(),
        }
    }

    /// Add a file chunk to the assembler.
    pub fn add_chunk(&mut self, chunk: FileChunk) {
        self.chunks
            .entry(chunk.component_name)
            .or_default()
            .insert(chunk.offset, chunk.data);
    }

    /// Write all accumulated components to disk.
    ///
    /// Returns the paths of all written files.
    pub fn write_all(&self) -> std::io::Result<Vec<PathBuf>> {
        std::fs::create_dir_all(&self.sstable_dir)?;
        let mut paths = Vec::new();

        for (component_name, offset_map) in &self.chunks {
            let file_path = self.sstable_dir.join(component_name);
            let mut file_data = Vec::new();

            // Concatenate chunks in offset order.
            for data in offset_map.values() {
                file_data.extend_from_slice(data);
            }

            std::fs::write(&file_path, &file_data)?;
            paths.push(file_path);
        }

        Ok(paths)
    }

    /// Number of components accumulated so far.
    pub fn component_count(&self) -> usize {
        self.chunks.len()
    }

    /// Total bytes accumulated across all components.
    pub fn total_bytes(&self) -> u64 {
        self.chunks
            .values()
            .flat_map(|m| m.values())
            .map(|d| d.len() as u64)
            .sum()
    }
}

/// Read an SSTable component from disk and split it into chunks.
///
/// Returns a list of `StreamedMutation` payloads, one per chunk.
pub fn read_sstable_component(
    keyspace: &str,
    table: &str,
    component_path: &Path,
    chunk_size: usize,
) -> std::io::Result<Vec<StreamedMutation>> {
    let component_name = component_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    let data = std::fs::read(component_path)?;
    let mut mutations = Vec::new();
    let mut offset = 0u64;

    for chunk in data.chunks(chunk_size) {
        mutations.push(encode_file_chunk(
            keyspace,
            table,
            component_name,
            offset,
            chunk.to_vec(),
        ));
        offset += chunk.len() as u64;
    }

    Ok(mutations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_file_chunk_roundtrip() {
        let mutation = encode_file_chunk("ks", "tbl", "Data.db", 1024, vec![1, 2, 3, 4]);

        let chunk = decode_file_chunk(&mutation).unwrap();
        assert_eq!(chunk.component_name, "Data.db");
        assert_eq!(chunk.offset, 1024);
        assert_eq!(chunk.data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn assembler_writes_files_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut assembler = SSTableAssembler::new(dir.path().join("sstable-001"));

        // Add chunks for two components.
        assembler.add_chunk(FileChunk {
            component_name: "Data.db".to_string(),
            offset: 0,
            data: vec![0xDE, 0xAD],
        });
        assembler.add_chunk(FileChunk {
            component_name: "Data.db".to_string(),
            offset: 2,
            data: vec![0xBE, 0xEF],
        });
        assembler.add_chunk(FileChunk {
            component_name: "Index.db".to_string(),
            offset: 0,
            data: vec![0xCA, 0xFE],
        });

        assert_eq!(assembler.component_count(), 2);
        assert_eq!(assembler.total_bytes(), 6);

        let paths = assembler.write_all().unwrap();
        assert_eq!(paths.len(), 2);

        let data_content = std::fs::read(dir.path().join("sstable-001/Data.db")).unwrap();
        assert_eq!(data_content, vec![0xDE, 0xAD, 0xBE, 0xEF]);

        let index_content = std::fs::read(dir.path().join("sstable-001/Index.db")).unwrap();
        assert_eq!(index_content, vec![0xCA, 0xFE]);
    }

    #[test]
    fn read_sstable_component_chunks_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("Data.db");
        let data = vec![0u8; 1000];
        std::fs::write(&file_path, &data).unwrap();

        let mutations = read_sstable_component("ks", "tbl", &file_path, 300).unwrap();

        // 1000 / 300 = 3 full chunks + 1 partial = 4 chunks.
        assert_eq!(mutations.len(), 4);
        assert_eq!(mutations[0].timestamp, 0);
        assert_eq!(mutations[0].row.len(), 300);
        assert_eq!(mutations[1].timestamp, 300);
        assert_eq!(mutations[3].timestamp, 900);
        assert_eq!(mutations[3].row.len(), 100);

        // Total bytes match.
        let total: usize = mutations.iter().map(|m| m.row.len()).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn manifest_serializes() {
        let manifest = SSTableTransferManifest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            sstable_id: "mc-001".to_string(),
            components: vec![
                SSTableComponent {
                    name: "Data.db".to_string(),
                    size: 4096,
                },
                SSTableComponent {
                    name: "Index.db".to_string(),
                    size: 512,
                },
            ],
            total_bytes: 4608,
        };

        let bytes = bincode::serialize(&manifest).unwrap();
        let decoded: SSTableTransferManifest = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.sstable_id, "mc-001");
        assert_eq!(decoded.components.len(), 2);
        assert_eq!(decoded.total_bytes, 4608);
    }
}
