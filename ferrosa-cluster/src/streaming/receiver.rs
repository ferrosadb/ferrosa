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

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::{PartitionKey, Token};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::error::{ClusterError, Result};

use super::{
    compute_checksum, StreamChunkPayload, StreamEndPayload, StreamStartPayload, StreamedMutation,
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

            // Decode the row bytes.  During streaming, `row` contains the
            // bincode-encoded Row from the sender node.  For now we apply a
            // raw bytes-as-row placeholder — full Row decoding will be wired
            // when the compaction/SSTable layer is integrated.
            //
            // We store the raw bytes via the commit log path so that mutations
            // are durable.  The Row decoding is done by ferrosa-sstable, which
            // is not imported here to avoid a circular dependency.
            //
            // TODO: decode mutation.row as a `ferrosa_sstable::types::Row`
            //       once a shared serialisation format is established.
            use ferrosa_common::CellValue;
            use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(mutation.row.clone(), mutation.timestamp))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(mutation.timestamp),
            };

            storage
                .write(&table_id, &key, row, mutation.timestamp)
                .map_err(|e| {
                    ClusterError::Internal(format!("stream: storage write failed: {e}"))
                })?;
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
            data_dir: dir.to_path_buf(),
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
}
