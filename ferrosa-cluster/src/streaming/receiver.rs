//! Inbound streaming: accumulates chunks and applies mutations to storage.
//!
//! # Protocol
//!
//! The receiver is called sequentially as protocol messages arrive:
//!
//! 1. The caller passes a decoded `StreamStartPayload` to `StreamReceiver::begin`.
//! 2. Each decoded `StreamChunkPayload` is passed to `session.apply_chunk`.
//! 3. The decoded `StreamEndPayload` is passed to `session.finish`, which
//!    validates the CRC32 and applies all accumulated mutations to storage.
//!
//! # Checksum validation
//!
//! The receiver recomputes the CRC32 over accumulated mutations and compares
//! against the value in `StreamEnd`. A mismatch returns
//! `ClusterError::Internal` and mutations are **not** applied.
//!
//! # Apply strategy
//!
//! Mutations are applied using `StorageEngine::write` — the same path used by
//! normal writes. This means the commit log and memtable both receive each
//! mutation, providing the same durability guarantees.

use std::path::PathBuf;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::{PartitionKey, Token};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::error::{ClusterError, Result};

use super::{
    compute_checksum, sstable_transfer::SSTableAssembler, SstableStreamChunkPayload,
    SstableStreamEndPayload, SstableStreamStartPayload, StreamChunkPayload, StreamEndPayload,
    StreamStartPayload, StreamedMutation,
};

// ---------------------------------------------------------------------------
// StreamResult
// ---------------------------------------------------------------------------

/// Summary produced when a streaming session completes successfully.
#[derive(Debug)]
pub struct StreamResult {
    pub session_id: u64,
    /// Total number of mutations applied to storage.
    pub applied: u64,
}

// ---------------------------------------------------------------------------
// StreamSession — mutable per-session state
// ---------------------------------------------------------------------------

/// In-progress streaming session accumulating mutations from chunks.
pub struct StreamSession {
    pub(crate) start: StreamStartPayload,
    pub(crate) mutations: Vec<StreamedMutation>,
}

impl StreamSession {
    /// Accept the mutations from one `StreamChunk`.
    pub fn apply_chunk(&mut self, chunk: StreamChunkPayload) -> Result<()> {
        if chunk.session_id != self.start.session_id {
            return Err(ClusterError::Internal(format!(
                "stream: session_id mismatch in chunk: expected {}, got {}",
                self.start.session_id, chunk.session_id
            )));
        }
        self.mutations.extend(chunk.mutations);
        Ok(())
    }

    /// Validate `StreamEnd` and apply all accumulated mutations to `storage`.
    ///
    /// On checksum failure the mutations are discarded and an error is returned.
    pub fn finish(self, end: StreamEndPayload, storage: &StorageEngine) -> Result<StreamResult> {
        if end.session_id != self.start.session_id {
            return Err(ClusterError::Internal(format!(
                "stream: session_id mismatch in StreamEnd: expected {}, got {}",
                self.start.session_id, end.session_id
            )));
        }

        // Validate checksum before touching storage.
        StreamReceiver::validate_end(&self.mutations, &end)?;

        // Apply mutations to storage.
        let mut applied = 0u64;
        for mutation in &self.mutations {
            let table_id = TableId::new(&mutation.keyspace, &mutation.table);
            // Reconstruct a minimal DecoratedKey from the raw key bytes.
            // We use Token(0) as a placeholder — the storage engine uses the
            // key bytes for lookup, not the token, so this is safe for
            // mutation application during bootstrap streaming.
            let key = DecoratedKey {
                token: Token(0),
                key: PartitionKey::new(mutation.key.clone()),
            };

            // Decode the row bytes. The sender serializes rows as
            // Vec<RowWire> via bincode. Fall back to a single-cell
            // placeholder if decoding fails (backwards compat with
            // pre-RowWire streams).
            use crate::raft::handlers::RowWire;
            use ferrosa_common::CellValue;
            use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

            let rows: Vec<Row> = match bincode::deserialize::<Vec<RowWire>>(&mutation.row) {
                Ok(wire_rows) if !wire_rows.is_empty() => {
                    wire_rows.into_iter().map(Row::from).collect()
                }
                _ => {
                    // Fallback: treat raw bytes as a single cell value.
                    vec![Row {
                        clustering: vec![],
                        cells: vec![(0, CellValue::live(mutation.row.clone(), mutation.timestamp))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(mutation.timestamp),
                    }]
                }
            };

            for row in rows {
                storage
                    .write(&table_id, &key, row, mutation.timestamp)
                    .map_err(|e| {
                        ClusterError::Internal(format!("stream: storage write failed: {e}"))
                    })?;
            }
            applied += 1;
        }

        tracing::info!(
            session_id = self.start.session_id,
            applied,
            "stream: session applied to storage"
        );

        Ok(StreamResult {
            session_id: self.start.session_id,
            applied,
        })
    }
}

// ---------------------------------------------------------------------------
// StreamReceiver — stateless namespace + helpers
// ---------------------------------------------------------------------------

/// Stateless namespace for inbound streaming operations.
pub struct StreamReceiver;

impl StreamReceiver {
    /// Begin a new streaming session from a `StreamStart` payload.
    pub fn begin(start: StreamStartPayload) -> StreamSession {
        tracing::info!(
            session_id = start.session_id,
            source_node = start.source_node,
            token_range_start = start.token_range_start,
            token_range_end = start.token_range_end,
            estimated_bytes = start.estimated_bytes,
            "stream: session started"
        );
        StreamSession {
            start,
            mutations: Vec::new(),
        }
    }

    /// Convenience method: accumulate chunks and finish in one call.
    ///
    /// This is the synchronous entry point used when all chunks are already
    /// available in memory (e.g. in unit tests or after buffering from a
    /// channel).
    pub async fn receive_and_apply(
        storage: &StorageEngine,
        start: StreamStartPayload,
        chunks: Vec<StreamChunkPayload>,
        end: StreamEndPayload,
    ) -> Result<StreamResult> {
        let mut session = Self::begin(start);
        for chunk in chunks {
            session.apply_chunk(chunk)?;
        }
        session.finish(end, storage)
    }

    /// Validate a `StreamEnd` against the accumulated mutations.
    ///
    /// Returns `ClusterError::Internal` if the checksum or count does not match.
    ///
    /// This is `pub(crate)` so that `mod.rs` tests can call it directly.
    pub(crate) fn validate_end(
        mutations: &[StreamedMutation],
        end: &StreamEndPayload,
    ) -> Result<()> {
        // Count check.
        if mutations.len() as u64 != end.total_mutations {
            return Err(ClusterError::Internal(format!(
                "stream: mutation count mismatch: accumulated {}, StreamEnd says {}",
                mutations.len(),
                end.total_mutations
            )));
        }

        // Checksum check.
        let computed = compute_checksum(mutations);
        if computed != end.checksum {
            return Err(ClusterError::Internal(format!(
                "stream: checksum mismatch: computed {:#010x}, StreamEnd says {:#010x}",
                computed, end.checksum
            )));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SSTable file-based streaming receiver
// ---------------------------------------------------------------------------

/// Summary produced when an SSTable streaming session completes.
#[derive(Debug)]
pub struct SstableStreamResult {
    pub session_id: u64,
    /// Paths of all component files written to disk.
    pub written_files: Vec<PathBuf>,
    /// Total bytes received and written.
    pub total_bytes: u64,
}

/// In-progress SSTable streaming session.
pub struct SstableStreamSession {
    pub(crate) start: SstableStreamStartPayload,
    pub(crate) assembler: SSTableAssembler,
    /// Running CRC32 hasher — fed chunk data in receive order.
    pub(crate) hasher: crc32fast::Hasher,
    /// Running count of bytes received.
    pub(crate) bytes_received: u64,
}

impl SstableStreamSession {
    /// Accept one `SstableStreamChunk`.
    pub fn apply_chunk(&mut self, chunk: SstableStreamChunkPayload) -> Result<()> {
        if chunk.session_id != self.start.session_id {
            return Err(ClusterError::Internal(format!(
                "sstable_stream: session_id mismatch: expected {}, got {}",
                self.start.session_id, chunk.session_id
            )));
        }

        self.hasher.update(&chunk.data);
        self.bytes_received += chunk.data.len() as u64;

        self.assembler
            .add_chunk(super::sstable_transfer::FileChunk {
                component_name: chunk.component,
                offset: chunk.offset,
                data: chunk.data,
            });

        Ok(())
    }

    /// Validate `SstableStreamEnd` and write all component files to disk.
    pub fn finish(self, end: SstableStreamEndPayload) -> Result<SstableStreamResult> {
        if end.session_id != self.start.session_id {
            return Err(ClusterError::Internal(format!(
                "sstable_stream: session_id mismatch in end: expected {}, got {}",
                self.start.session_id, end.session_id
            )));
        }

        // Validate total bytes.
        if self.bytes_received != end.total_bytes {
            return Err(ClusterError::Internal(format!(
                "sstable_stream: byte count mismatch: received {}, end says {}",
                self.bytes_received, end.total_bytes
            )));
        }

        // Validate checksum.
        let computed = self.hasher.finalize();
        if computed != end.checksum {
            return Err(ClusterError::Internal(format!(
                "sstable_stream: checksum mismatch: computed {:#010x}, end says {:#010x}",
                computed, end.checksum
            )));
        }

        // Write files to disk.
        let written_files = self
            .assembler
            .write_all()
            .map_err(|e| ClusterError::Internal(format!("sstable_stream: write files: {e}")))?;

        tracing::info!(
            session_id = self.start.session_id,
            files = written_files.len(),
            bytes = self.bytes_received,
            "sstable_stream: session written to disk"
        );

        Ok(SstableStreamResult {
            session_id: self.start.session_id,
            written_files,
            total_bytes: self.bytes_received,
        })
    }
}

/// Stateless namespace for inbound SSTable streaming.
pub struct SstableStreamReceiver;

impl SstableStreamReceiver {
    /// Begin a new SSTable streaming session.
    ///
    /// `dest_dir` is the directory where SSTable component files will be
    /// written. Typically `{data_dir}/sstables/{keyspace}.{table}/{sstable_id}`.
    pub fn begin(start: SstableStreamStartPayload, dest_dir: PathBuf) -> SstableStreamSession {
        tracing::info!(
            session_id = start.session_id,
            source_node = start.source_node,
            keyspace = %start.keyspace,
            table = %start.table,
            sstable_id = %start.sstable_id,
            components = start.components.len(),
            total_bytes = start.total_bytes,
            "sstable_stream: session started"
        );
        SstableStreamSession {
            start,
            assembler: SSTableAssembler::new(dest_dir),
            hasher: crc32fast::Hasher::new(),
            bytes_received: 0,
        }
    }

    /// Convenience: accumulate all chunks and finish in one call.
    pub fn receive_and_write(
        start: SstableStreamStartPayload,
        chunks: Vec<SstableStreamChunkPayload>,
        end: SstableStreamEndPayload,
        dest_dir: PathBuf,
    ) -> Result<SstableStreamResult> {
        let mut session = Self::begin(start, dest_dir);
        for chunk in chunks {
            session.apply_chunk(chunk)?;
        }
        session.finish(end)
    }

    /// Validate an `SstableStreamEnd` payload against expected values.
    #[allow(dead_code)]
    pub(crate) fn validate_end(
        bytes_received: u64,
        computed_checksum: u32,
        end: &SstableStreamEndPayload,
    ) -> Result<()> {
        if bytes_received != end.total_bytes {
            return Err(ClusterError::Internal(format!(
                "sstable_stream: byte count mismatch: received {bytes_received}, end says {}",
                end.total_bytes
            )));
        }
        if computed_checksum != end.checksum {
            return Err(ClusterError::Internal(format!(
                "sstable_stream: checksum mismatch: computed {computed_checksum:#010x}, end says {:#010x}",
                end.checksum
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::streaming::{compute_checksum, StreamChunkPayload, StreamConfig, StreamedMutation};

    fn make_mutation(i: usize) -> StreamedMutation {
        StreamedMutation {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: (i as u64).to_be_bytes().to_vec(),
            row: format!("row_{i}").into_bytes(),
            timestamp: i as i64,
        }
    }

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn register_table(storage: &StorageEngine, keyspace: &str, table: &str) {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        let schema = TableSchema {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(schema).unwrap();
    }

    // -----------------------------------------------------------------------
    // 1. Good checksum + count → session completes successfully
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn receive_and_apply_succeeds_with_correct_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_table(&storage, "ks", "tbl");

        let mutations: Vec<StreamedMutation> = (0..5).map(make_mutation).collect();
        let checksum = compute_checksum(&mutations);

        let start = StreamStartPayload {
            session_id: 1,
            source_node: 10,
            token_range_start: 0,
            token_range_end: 100,
            estimated_bytes: 0,
        };
        let chunks = vec![StreamChunkPayload {
            session_id: 1,
            mutations: mutations.clone(),
        }];
        let end = StreamEndPayload {
            session_id: 1,
            total_mutations: 5,
            checksum,
        };

        let result = StreamReceiver::receive_and_apply(&storage, start, chunks, end)
            .await
            .unwrap();

        assert_eq!(result.applied, 5);
        assert_eq!(result.session_id, 1);
    }

    // -----------------------------------------------------------------------
    // 2. Bad checksum → error, storage not corrupted
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn receive_and_apply_rejects_bad_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_table(&storage, "ks", "tbl");

        let mutations: Vec<StreamedMutation> = (0..3).map(make_mutation).collect();
        let good_checksum = compute_checksum(&mutations);

        let start = StreamStartPayload {
            session_id: 2,
            source_node: 10,
            token_range_start: 0,
            token_range_end: 100,
            estimated_bytes: 0,
        };
        let chunks = vec![StreamChunkPayload {
            session_id: 2,
            mutations: mutations.clone(),
        }];
        let end = StreamEndPayload {
            session_id: 2,
            total_mutations: 3,
            checksum: good_checksum.wrapping_add(1), // intentionally wrong
        };

        let result = StreamReceiver::receive_and_apply(&storage, start, chunks, end).await;

        assert!(
            matches!(result, Err(ClusterError::Internal(_))),
            "expected ClusterError::Internal for bad checksum"
        );
    }

    // -----------------------------------------------------------------------
    // 3. Count mismatch → error
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn receive_and_apply_rejects_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_table(&storage, "ks", "tbl");

        let mutations: Vec<StreamedMutation> = (0..4).map(make_mutation).collect();
        let checksum = compute_checksum(&mutations);

        let start = StreamStartPayload {
            session_id: 3,
            source_node: 10,
            token_range_start: 0,
            token_range_end: 100,
            estimated_bytes: 0,
        };
        let chunks = vec![StreamChunkPayload {
            session_id: 3,
            mutations,
        }];
        let end = StreamEndPayload {
            session_id: 3,
            total_mutations: 99, // wrong count
            checksum,
        };

        let result = StreamReceiver::receive_and_apply(&storage, start, chunks, end).await;

        assert!(
            matches!(result, Err(ClusterError::Internal(_))),
            "expected ClusterError::Internal for count mismatch"
        );
    }

    // -----------------------------------------------------------------------
    // 4. Multiple chunks are accumulated before validation
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn receive_accumulates_multiple_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_table(&storage, "ks", "tbl");

        let all_mutations: Vec<StreamedMutation> = (0..9).map(make_mutation).collect();
        let checksum = compute_checksum(&all_mutations);

        let start = StreamStartPayload {
            session_id: 4,
            source_node: 10,
            token_range_start: 0,
            token_range_end: 100,
            estimated_bytes: 0,
        };

        // Split into 3 chunks of 3.
        let chunks: Vec<StreamChunkPayload> = all_mutations
            .chunks(3)
            .map(|c| StreamChunkPayload {
                session_id: 4,
                mutations: c.to_vec(),
            })
            .collect();

        let end = StreamEndPayload {
            session_id: 4,
            total_mutations: 9,
            checksum,
        };

        let result = StreamReceiver::receive_and_apply(&storage, start, chunks, end)
            .await
            .unwrap();

        assert_eq!(result.applied, 9);
    }

    // -----------------------------------------------------------------------
    // 5. session_id mismatch in chunk → error
    // -----------------------------------------------------------------------
    #[test]
    fn apply_chunk_rejects_wrong_session_id() {
        let start = StreamStartPayload {
            session_id: 100,
            source_node: 1,
            token_range_start: 0,
            token_range_end: 10,
            estimated_bytes: 0,
        };
        let mut session = StreamReceiver::begin(start);

        let chunk = StreamChunkPayload {
            session_id: 999, // wrong id
            mutations: vec![make_mutation(0)],
        };

        let result = session.apply_chunk(chunk);
        assert!(
            matches!(result, Err(ClusterError::Internal(_))),
            "mismatched session_id must be rejected"
        );
    }

    // -----------------------------------------------------------------------
    // 6. Chunk batching size limit — verify end-to-end with StreamConfig
    // -----------------------------------------------------------------------
    #[test]
    fn chunk_batching_size_limit_end_to_end() {
        use crate::streaming::batch_mutations;

        // 5 MB total: 50 mutations * 100 KB each
        let row = vec![0u8; 100 * 1024];
        let mutations: Vec<StreamedMutation> = (0..50)
            .map(|i| StreamedMutation {
                keyspace: "ks".to_string(),
                table: "tbl".to_string(),
                key: (i as u64).to_be_bytes().to_vec(),
                row: row.clone(),
                timestamp: i,
            })
            .collect();

        let config = StreamConfig {
            chunk_size_bytes: 1024 * 1024, // 1 MB
        };

        let chunks = batch_mutations(mutations, &config);
        assert!(
            chunks.len() >= 5,
            "expected ≥5 chunks; got {}",
            chunks.len()
        );
    }

    // -----------------------------------------------------------------------
    // 7. Full Row round-trip via RowWire encoding
    // -----------------------------------------------------------------------

    /// Verifies that a mutation carrying bincode-encoded Vec<RowWire> is
    /// correctly decoded back into full Rows with clustering keys and
    /// multiple cells — not the single-cell placeholder.
    #[tokio::test]
    async fn receive_decodes_full_row_wire() {
        use crate::raft::handlers::RowWire;
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_table(&storage, "ks", "tbl");

        // Build a Row with clustering keys and multiple cells.
        let row = Row {
            clustering: vec![0, 1, 2, 3],
            cells: vec![
                (0, CellValue::live(b"cell_0".to_vec(), 100)),
                (1, CellValue::live(b"cell_1".to_vec(), 100)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(100),
        };
        let wire: Vec<RowWire> = vec![RowWire::from(row.clone())];
        let row_bytes = bincode::serialize(&wire).unwrap();

        let mutation = StreamedMutation {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: 42u64.to_be_bytes().to_vec(),
            row: row_bytes,
            timestamp: 100,
        };

        let checksum = compute_checksum(std::slice::from_ref(&mutation));
        let start = StreamStartPayload {
            session_id: 10,
            source_node: 1,
            token_range_start: 0,
            token_range_end: 100,
            estimated_bytes: 0,
        };
        let chunks = vec![StreamChunkPayload {
            session_id: 10,
            mutations: vec![mutation],
        }];
        let end = StreamEndPayload {
            session_id: 10,
            total_mutations: 1,
            checksum,
        };

        let result = StreamReceiver::receive_and_apply(&storage, start, chunks, end)
            .await
            .unwrap();

        assert_eq!(result.applied, 1);

        // Read back from storage and verify the full row was applied.
        let table_id = ferrosa_storage::TableId::new("ks", "tbl");
        let key = ferrosa_common::key::DecoratedKey {
            token: ferrosa_common::Token(0),
            key: ferrosa_common::PartitionKey::new(42u64.to_be_bytes().to_vec()),
        };
        let partition = storage.read(&table_id, &key).unwrap();
        assert!(partition.is_some(), "should find stored partition");
        let partition = partition.unwrap();
        assert!(!partition.rows.is_empty(), "should have rows");
        let stored_row = &partition.rows[0];
        assert_eq!(
            stored_row.clustering,
            vec![0, 1, 2, 3],
            "clustering keys must be preserved"
        );
        assert_eq!(stored_row.cells.len(), 2, "both cells must be preserved");
    }

    // =======================================================================
    // SSTable file-based streaming tests
    // =======================================================================

    // -----------------------------------------------------------------------
    // 8. SSTable stream roundtrip: write files, stream, reassemble
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_roundtrip_writes_files() {
        use super::{
            SstableStreamChunkPayload, SstableStreamEndPayload, SstableStreamStartPayload,
        };
        use crate::streaming::sstable_transfer::SSTableComponent;

        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let dest_path = dst_dir.path().join("received-sstable");

        // Create fake SSTable component files on the "sender" side.
        let data_content = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];
        let index_content = vec![0x01, 0x02, 0x03];
        std::fs::write(src_dir.path().join("Data.db"), &data_content).unwrap();
        std::fs::write(src_dir.path().join("Index.db"), &index_content).unwrap();

        // Build the start payload (simulating what the sender would create).
        let start = SstableStreamStartPayload {
            session_id: 100,
            source_node: 1,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            sstable_id: "mc-001".to_string(),
            components: vec![
                SSTableComponent {
                    name: "Data.db".to_string(),
                    size: data_content.len() as u64,
                },
                SSTableComponent {
                    name: "Index.db".to_string(),
                    size: index_content.len() as u64,
                },
            ],
            total_bytes: (data_content.len() + index_content.len()) as u64,
        };

        // Build chunks (split Data.db into two chunks of 3 bytes each).
        let mut hasher = crc32fast::Hasher::new();
        let mut chunks = Vec::new();

        // Data.db chunk 1
        hasher.update(&data_content[..3]);
        chunks.push(SstableStreamChunkPayload {
            session_id: 100,
            component: "Data.db".to_string(),
            offset: 0,
            data: data_content[..3].to_vec(),
        });
        // Data.db chunk 2
        hasher.update(&data_content[3..]);
        chunks.push(SstableStreamChunkPayload {
            session_id: 100,
            component: "Data.db".to_string(),
            offset: 3,
            data: data_content[3..].to_vec(),
        });
        // Index.db single chunk
        hasher.update(&index_content);
        chunks.push(SstableStreamChunkPayload {
            session_id: 100,
            component: "Index.db".to_string(),
            offset: 0,
            data: index_content.clone(),
        });

        let checksum = hasher.finalize();
        let end = SstableStreamEndPayload {
            session_id: 100,
            total_bytes: (data_content.len() + index_content.len()) as u64,
            checksum,
        };

        // Receive and write.
        let result =
            SstableStreamReceiver::receive_and_write(start, chunks, end, dest_path.clone())
                .unwrap();

        assert_eq!(result.session_id, 100);
        assert_eq!(result.written_files.len(), 2);
        assert_eq!(result.total_bytes, 9);

        // Verify file contents on disk.
        let written_data = std::fs::read(dest_path.join("Data.db")).unwrap();
        assert_eq!(written_data, data_content);

        let written_index = std::fs::read(dest_path.join("Index.db")).unwrap();
        assert_eq!(written_index, index_content);
    }

    // -----------------------------------------------------------------------
    // 9. SSTable stream rejects bad checksum
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_rejects_bad_checksum() {
        use super::{
            SstableStreamChunkPayload, SstableStreamEndPayload, SstableStreamStartPayload,
        };
        use crate::streaming::sstable_transfer::SSTableComponent;

        let dst_dir = tempfile::tempdir().unwrap();
        let dest_path = dst_dir.path().join("bad-checksum");

        let start = SstableStreamStartPayload {
            session_id: 200,
            source_node: 1,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            sstable_id: "mc-002".to_string(),
            components: vec![SSTableComponent {
                name: "Data.db".to_string(),
                size: 4,
            }],
            total_bytes: 4,
        };

        let chunks = vec![SstableStreamChunkPayload {
            session_id: 200,
            component: "Data.db".to_string(),
            offset: 0,
            data: vec![1, 2, 3, 4],
        }];

        let end = SstableStreamEndPayload {
            session_id: 200,
            total_bytes: 4,
            checksum: 0xBAD_BEEF, // intentionally wrong
        };

        let result = SstableStreamReceiver::receive_and_write(start, chunks, end, dest_path);
        assert!(
            matches!(result, Err(ClusterError::Internal(ref msg)) if msg.contains("checksum")),
            "bad checksum must produce ClusterError::Internal with checksum message"
        );
    }

    // -----------------------------------------------------------------------
    // 10. SSTable stream rejects byte count mismatch
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_rejects_byte_count_mismatch() {
        use super::{
            SstableStreamChunkPayload, SstableStreamEndPayload, SstableStreamStartPayload,
        };
        use crate::streaming::sstable_transfer::SSTableComponent;

        let dst_dir = tempfile::tempdir().unwrap();
        let dest_path = dst_dir.path().join("count-mismatch");

        let start = SstableStreamStartPayload {
            session_id: 300,
            source_node: 1,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            sstable_id: "mc-003".to_string(),
            components: vec![SSTableComponent {
                name: "Data.db".to_string(),
                size: 4,
            }],
            total_bytes: 4,
        };

        let data = vec![1, 2, 3, 4];
        let mut h = crc32fast::Hasher::new();
        h.update(&data);
        let checksum = h.finalize();

        let chunks = vec![SstableStreamChunkPayload {
            session_id: 300,
            component: "Data.db".to_string(),
            offset: 0,
            data,
        }];

        let end = SstableStreamEndPayload {
            session_id: 300,
            total_bytes: 999, // wrong
            checksum,
        };

        let result = SstableStreamReceiver::receive_and_write(start, chunks, end, dest_path);
        assert!(
            matches!(result, Err(ClusterError::Internal(ref msg)) if msg.contains("byte count")),
            "byte count mismatch must produce ClusterError::Internal"
        );
    }

    // -----------------------------------------------------------------------
    // 11. SSTable stream rejects session_id mismatch in chunk
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_rejects_session_id_mismatch_in_chunk() {
        use super::{SstableStreamChunkPayload, SstableStreamStartPayload};
        use crate::streaming::sstable_transfer::SSTableComponent;

        let dst_dir = tempfile::tempdir().unwrap();
        let dest_path = dst_dir.path().join("session-mismatch");

        let start = SstableStreamStartPayload {
            session_id: 400,
            source_node: 1,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            sstable_id: "mc-004".to_string(),
            components: vec![SSTableComponent {
                name: "Data.db".to_string(),
                size: 4,
            }],
            total_bytes: 4,
        };

        let mut session = SstableStreamReceiver::begin(start, dest_path);

        let chunk = SstableStreamChunkPayload {
            session_id: 999, // wrong
            component: "Data.db".to_string(),
            offset: 0,
            data: vec![1, 2, 3, 4],
        };

        let result = session.apply_chunk(chunk);
        assert!(
            matches!(result, Err(ClusterError::Internal(_))),
            "session_id mismatch in chunk must be rejected"
        );
    }
}
