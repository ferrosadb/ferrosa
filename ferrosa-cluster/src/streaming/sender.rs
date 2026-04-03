//! Outbound streaming: sends a token range's mutations to a remote peer.
//!
//! # Protocol sequence
//!
//! 1. Send `StreamStart` with session metadata.
//! 2. Batch mutations into ≤`chunk_size_bytes` chunks, send one `StreamChunk`
//!    per batch while accumulating a running CRC32 checksum.
//! 3. Send `StreamEnd` with the total mutation count and final checksum.
//!
//! Network I/O (the actual `PeerManager::send` calls) is exercised in docker
//! smoke tests. Unit tests cover the serialisation and batching logic only.

use std::path::Path;

use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;

use crate::error::{ClusterError, Result};

use super::{
    batch_mutations, compute_checksum, sstable_transfer, SstableStreamChunkPayload,
    SstableStreamEndPayload, SstableStreamStartPayload, StreamChunkPayload, StreamConfig,
    StreamEndPayload, StreamStartPayload, StreamedMutation,
};

/// Parameters for an SSTable file-based streaming request.
///
/// Groups the many arguments to `StreamSender::send_sstable_files` to
/// satisfy the 7-argument clippy limit.
#[derive(Debug, Clone, Copy)]
pub struct SstableSendRequest<'a> {
    /// Directory containing the SSTable component files.
    pub sstable_dir: &'a Path,
    /// Keyspace owning the SSTable.
    pub keyspace: &'a str,
    /// Table owning the SSTable.
    pub table: &'a str,
    /// SSTable generation/identifier.
    pub sstable_id: &'a str,
    /// Unique session identifier.
    pub session_id: u64,
    /// Raft node-id of the sender.
    pub source_node: u64,
    /// Maximum bytes per chunk.
    pub chunk_size: usize,
}

/// Stateless namespace for outbound streaming operations.
pub struct StreamSender;

impl StreamSender {
    /// Send `mutations` to `peer_id` as a streaming session.
    ///
    /// Steps:
    /// 1. Send `StreamStart` announcing the session.
    /// 2. Partition mutations into chunks and send `StreamChunk` for each.
    /// 3. Compute a CRC32 across all mutations in order.
    /// 4. Send `StreamEnd` with the total count and checksum.
    ///
    /// Returns `Ok(())` once the peer has received `StreamEnd`, or a
    /// `ClusterError` on any network or serialisation failure.
    pub async fn send_stream(
        mutations: Vec<StreamedMutation>,
        peer_manager: &PeerManager,
        peer_id: Uuid,
        session_id: u64,
        token_range: (i64, i64),
        source_node: u64,
        config: &StreamConfig,
    ) -> Result<()> {
        let estimated_bytes: u64 = mutations
            .iter()
            .map(|m| bincode::serialized_size(m).unwrap_or(0))
            .sum();

        // 1. Send StreamStart.
        let start = StreamStartPayload {
            session_id,
            source_node,
            token_range_start: token_range.0,
            token_range_end: token_range.1,
            estimated_bytes,
        };
        let start_bytes = bincode::serialize(&start).map_err(|e| {
            ClusterError::Internal(format!("stream: failed to serialise StreamStart: {e}"))
        })?;

        peer_manager
            .send(
                peer_id,
                Message::StreamStart(Bytes::from(start_bytes)),
                Lane::Bulk,
            )
            .await
            .map_err(ClusterError::Net)?;

        // 2. Compute checksum and batch mutations.
        let checksum = compute_checksum(&mutations);
        let total_mutations = mutations.len() as u64;

        let chunks = batch_mutations(mutations, config);

        // 3. Send one StreamChunk per batch.
        for chunk_mutations in chunks {
            let chunk = StreamChunkPayload {
                session_id,
                mutations: chunk_mutations,
            };
            let chunk_bytes = bincode::serialize(&chunk).map_err(|e| {
                ClusterError::Internal(format!("stream: failed to serialise StreamChunk: {e}"))
            })?;

            peer_manager
                .send(
                    peer_id,
                    Message::StreamChunk(Bytes::from(chunk_bytes)),
                    Lane::Bulk,
                )
                .await
                .map_err(ClusterError::Net)?;
        }

        // 4. Send StreamEnd.
        let end = StreamEndPayload {
            session_id,
            total_mutations,
            checksum,
        };
        let end_bytes = bincode::serialize(&end).map_err(|e| {
            ClusterError::Internal(format!("stream: failed to serialise StreamEnd: {e}"))
        })?;

        peer_manager
            .send(
                peer_id,
                Message::StreamEnd(Bytes::from(end_bytes)),
                Lane::Bulk,
            )
            .await
            .map_err(ClusterError::Net)?;

        tracing::info!(
            %peer_id,
            session_id,
            total_mutations,
            checksum,
            "stream: session complete"
        );

        Ok(())
    }

    /// Encode a `StreamStart` payload to bytes without sending it.
    ///
    /// Exposed for testing purposes.
    #[allow(dead_code)]
    pub(crate) fn encode_start(payload: &StreamStartPayload) -> Result<Vec<u8>> {
        bincode::serialize(payload)
            .map_err(|e| ClusterError::Internal(format!("stream: encode_start: {e}")))
    }

    /// Encode a `StreamChunk` payload to bytes without sending it.
    #[allow(dead_code)]
    pub(crate) fn encode_chunk(payload: &StreamChunkPayload) -> Result<Vec<u8>> {
        bincode::serialize(payload)
            .map_err(|e| ClusterError::Internal(format!("stream: encode_chunk: {e}")))
    }

    /// Encode a `StreamEnd` payload to bytes without sending it.
    #[allow(dead_code)]
    pub(crate) fn encode_end(payload: &StreamEndPayload) -> Result<Vec<u8>> {
        bincode::serialize(payload)
            .map_err(|e| ClusterError::Internal(format!("stream: encode_end: {e}")))
    }

    // -----------------------------------------------------------------------
    // SSTable file-based streaming
    // -----------------------------------------------------------------------

    /// Send SSTable component files to a remote peer as raw byte chunks.
    ///
    /// This is the bulk transfer path used when the partition count for a
    /// table exceeds `BOOTSTRAP_SSTABLE_THRESHOLD`. Instead of serializing
    /// each row individually, entire SSTable component files are chunked and
    /// sent over the Bulk lane.
    ///
    /// # Protocol
    ///
    /// 1. Send `SstableStreamStart` with the manifest (component list + sizes).
    /// 2. For each component file, read it in `chunk_size`-byte slices and
    ///    send one `SstableStreamChunk` per slice.
    /// 3. Send `SstableStreamEnd` with total bytes and a CRC32 checksum.
    pub async fn send_sstable_files(
        request: &SstableSendRequest<'_>,
        peer_manager: &PeerManager,
        peer_id: Uuid,
    ) -> Result<u64> {
        let SstableSendRequest {
            sstable_dir,
            keyspace,
            table,
            sstable_id,
            session_id,
            source_node,
            chunk_size,
        } = *request;
        // Discover component files in the SSTable directory.
        let entries = std::fs::read_dir(sstable_dir).map_err(|e| {
            ClusterError::Internal(format!(
                "sstable_stream: failed to read dir {}: {e}",
                sstable_dir.display()
            ))
        })?;

        let mut components = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| {
                ClusterError::Internal(format!("sstable_stream: read_dir entry: {e}"))
            })?;
            let metadata = entry.metadata().map_err(|e| {
                ClusterError::Internal(format!("sstable_stream: file metadata: {e}"))
            })?;
            if metadata.is_file() {
                let name = entry.file_name().to_string_lossy().to_string();
                components.push(sstable_transfer::SSTableComponent {
                    name,
                    size: metadata.len(),
                });
            }
        }

        if components.is_empty() {
            return Err(ClusterError::Internal(format!(
                "sstable_stream: no component files in {}",
                sstable_dir.display()
            )));
        }

        // Sort for deterministic ordering.
        components.sort_by(|a, b| a.name.cmp(&b.name));

        let total_bytes: u64 = components.iter().map(|c| c.size).sum();

        // 1. Send SstableStreamStart.
        let start = SstableStreamStartPayload {
            session_id,
            source_node,
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            sstable_id: sstable_id.to_string(),
            components: components.clone(),
            total_bytes,
        };
        let start_bytes = bincode::serialize(&start)
            .map_err(|e| ClusterError::Internal(format!("sstable_stream: serialise start: {e}")))?;
        peer_manager
            .send(
                peer_id,
                Message::SstableStreamStart(Bytes::from(start_bytes)),
                Lane::Bulk,
            )
            .await
            .map_err(ClusterError::Net)?;

        // 2. Send component file chunks.
        let mut hasher = crc32fast::Hasher::new();
        let mut bytes_sent = 0u64;

        for component in &components {
            let file_path = sstable_dir.join(&component.name);
            let data = std::fs::read(&file_path).map_err(|e| {
                ClusterError::Internal(format!("sstable_stream: read {}: {e}", file_path.display()))
            })?;

            let mut offset = 0u64;
            for slice in data.chunks(chunk_size) {
                hasher.update(slice);

                let chunk = SstableStreamChunkPayload {
                    session_id,
                    component: component.name.clone(),
                    offset,
                    data: slice.to_vec(),
                };
                let chunk_bytes = bincode::serialize(&chunk).map_err(|e| {
                    ClusterError::Internal(format!("sstable_stream: serialise chunk: {e}"))
                })?;
                peer_manager
                    .send(
                        peer_id,
                        Message::SstableStreamChunk(Bytes::from(chunk_bytes)),
                        Lane::Bulk,
                    )
                    .await
                    .map_err(ClusterError::Net)?;

                offset += slice.len() as u64;
                bytes_sent += slice.len() as u64;
            }
        }

        // 3. Send SstableStreamEnd.
        let checksum = hasher.finalize();
        let end = SstableStreamEndPayload {
            session_id,
            total_bytes: bytes_sent,
            checksum,
        };
        let end_bytes = bincode::serialize(&end)
            .map_err(|e| ClusterError::Internal(format!("sstable_stream: serialise end: {e}")))?;
        peer_manager
            .send(
                peer_id,
                Message::SstableStreamEnd(Bytes::from(end_bytes)),
                Lane::Bulk,
            )
            .await
            .map_err(ClusterError::Net)?;

        tracing::info!(
            %peer_id,
            session_id,
            bytes_sent,
            checksum,
            components = components.len(),
            "sstable_stream: session complete"
        );

        Ok(bytes_sent)
    }

    /// Encode an `SstableStreamStart` payload to bytes without sending.
    #[allow(dead_code)]
    pub(crate) fn encode_sstable_start(payload: &SstableStreamStartPayload) -> Result<Vec<u8>> {
        bincode::serialize(payload)
            .map_err(|e| ClusterError::Internal(format!("sstable_stream: encode_start: {e}")))
    }

    /// Encode an `SstableStreamChunk` payload to bytes without sending.
    #[allow(dead_code)]
    pub(crate) fn encode_sstable_chunk(payload: &SstableStreamChunkPayload) -> Result<Vec<u8>> {
        bincode::serialize(payload)
            .map_err(|e| ClusterError::Internal(format!("sstable_stream: encode_chunk: {e}")))
    }

    /// Encode an `SstableStreamEnd` payload to bytes without sending.
    #[allow(dead_code)]
    pub(crate) fn encode_sstable_end(payload: &SstableStreamEndPayload) -> Result<Vec<u8>> {
        bincode::serialize(payload)
            .map_err(|e| ClusterError::Internal(format!("sstable_stream: encode_end: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::{batch_mutations, StreamConfig, StreamedMutation};

    fn make_mutation(i: usize, row_size: usize) -> StreamedMutation {
        StreamedMutation {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: (i as u64).to_be_bytes().to_vec(),
            row: vec![0u8; row_size],
            timestamp: i as i64,
        }
    }

    // -----------------------------------------------------------------------
    // Encoding round-trips for all three payload types
    // -----------------------------------------------------------------------

    #[test]
    fn stream_start_encodes_and_decodes() {
        let payload = StreamStartPayload {
            session_id: 42,
            source_node: 7,
            token_range_start: -100,
            token_range_end: 200,
            estimated_bytes: 1024,
        };
        let encoded = StreamSender::encode_start(&payload).unwrap();
        let decoded: StreamStartPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.session_id, 42);
        assert_eq!(decoded.source_node, 7);
        assert_eq!(decoded.token_range_start, -100);
        assert_eq!(decoded.token_range_end, 200);
        assert_eq!(decoded.estimated_bytes, 1024);
    }

    #[test]
    fn stream_chunk_encodes_and_decodes() {
        let mutations: Vec<StreamedMutation> = (0..3).map(|i| make_mutation(i, 10)).collect();
        let payload = StreamChunkPayload {
            session_id: 99,
            mutations: mutations.clone(),
        };
        let encoded = StreamSender::encode_chunk(&payload).unwrap();
        let decoded: StreamChunkPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.session_id, 99);
        assert_eq!(decoded.mutations.len(), 3);
        for (orig, dec) in mutations.iter().zip(decoded.mutations.iter()) {
            assert_eq!(orig, dec);
        }
    }

    #[test]
    fn stream_end_encodes_and_decodes() {
        let payload = StreamEndPayload {
            session_id: 7,
            total_mutations: 50,
            checksum: 0xDEAD_BEEF,
        };
        let encoded = StreamSender::encode_end(&payload).unwrap();
        let decoded: StreamEndPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.session_id, 7);
        assert_eq!(decoded.total_mutations, 50);
        assert_eq!(decoded.checksum, 0xDEAD_BEEF);
    }

    // -----------------------------------------------------------------------
    // Chunk batching is delegated to the shared helper; verify via sender path
    // -----------------------------------------------------------------------

    #[test]
    fn sender_chunk_batching_respects_size_limit() {
        // 20 mutations each with a 100 KB row ≈ 2 MB total
        let mutations: Vec<StreamedMutation> =
            (0..20).map(|i| make_mutation(i, 100 * 1024)).collect();
        let config = StreamConfig {
            chunk_size_bytes: 512 * 1024, // 512 KB per chunk
        };
        let chunks = batch_mutations(mutations, &config);
        // Expect at least 4 chunks for 2 MB / 512 KB.
        assert!(
            chunks.len() >= 4,
            "expected ≥4 chunks; got {}",
            chunks.len()
        );
    }

    // -----------------------------------------------------------------------
    // Empty mutations list produces no chunks
    // -----------------------------------------------------------------------

    #[test]
    fn empty_mutations_produce_no_chunks() {
        let config = StreamConfig::default();
        let chunks = batch_mutations(vec![], &config);
        assert!(chunks.is_empty());
    }

    // =======================================================================
    // SSTable file-based streaming tests
    // =======================================================================

    // -----------------------------------------------------------------------
    // SSTable start payload encodes and decodes
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_start_encodes_and_decodes() {
        use crate::streaming::sstable_transfer::SSTableComponent;

        let payload = SstableStreamStartPayload {
            session_id: 42,
            source_node: 7,
            keyspace: "my_ks".to_string(),
            table: "my_tbl".to_string(),
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
        let encoded = StreamSender::encode_sstable_start(&payload).unwrap();
        let decoded: SstableStreamStartPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.session_id, 42);
        assert_eq!(decoded.source_node, 7);
        assert_eq!(decoded.keyspace, "my_ks");
        assert_eq!(decoded.table, "my_tbl");
        assert_eq!(decoded.sstable_id, "mc-001");
        assert_eq!(decoded.components.len(), 2);
        assert_eq!(decoded.total_bytes, 4608);
    }

    // -----------------------------------------------------------------------
    // SSTable chunk payload encodes and decodes
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_chunk_encodes_and_decodes() {
        let payload = SstableStreamChunkPayload {
            session_id: 99,
            component: "Data.db".to_string(),
            offset: 1024,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };
        let encoded = StreamSender::encode_sstable_chunk(&payload).unwrap();
        let decoded: SstableStreamChunkPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.session_id, 99);
        assert_eq!(decoded.component, "Data.db");
        assert_eq!(decoded.offset, 1024);
        assert_eq!(decoded.data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    // -----------------------------------------------------------------------
    // SSTable end payload encodes and decodes
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_end_encodes_and_decodes() {
        let payload = SstableStreamEndPayload {
            session_id: 7,
            total_bytes: 8192,
            checksum: 0xDEAD_BEEF,
        };
        let encoded = StreamSender::encode_sstable_end(&payload).unwrap();
        let decoded: SstableStreamEndPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.session_id, 7);
        assert_eq!(decoded.total_bytes, 8192);
        assert_eq!(decoded.checksum, 0xDEAD_BEEF);
    }

    // -----------------------------------------------------------------------
    // Full sender → receiver roundtrip using temp files
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_sender_reads_and_chunks_files() {
        // Create a source directory with SSTable component files.
        let src_dir = tempfile::tempdir().unwrap();
        let data_content = vec![0u8; 1000];
        let index_content = vec![1u8; 200];
        std::fs::write(src_dir.path().join("Data.db"), &data_content).unwrap();
        std::fs::write(src_dir.path().join("Index.db"), &index_content).unwrap();

        // Read components using the sstable_transfer helper.
        let data_mutations = sstable_transfer::read_sstable_component(
            "ks",
            "tbl",
            &src_dir.path().join("Data.db"),
            300,
        )
        .unwrap();

        // 1000 / 300 = 3 full + 1 partial = 4 chunks
        assert_eq!(data_mutations.len(), 4);

        let index_mutations = sstable_transfer::read_sstable_component(
            "ks",
            "tbl",
            &src_dir.path().join("Index.db"),
            300,
        )
        .unwrap();

        // 200 / 300 = 1 chunk
        assert_eq!(index_mutations.len(), 1);

        // Verify total bytes.
        let total_data: usize = data_mutations.iter().map(|m| m.row.len()).sum();
        assert_eq!(total_data, 1000);
        let total_index: usize = index_mutations.iter().map(|m| m.row.len()).sum();
        assert_eq!(total_index, 200);
    }
}
