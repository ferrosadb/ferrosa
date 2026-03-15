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

use bytes::Bytes;
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;

use crate::error::{ClusterError, Result};

use super::{
    batch_mutations, compute_checksum, StreamChunkPayload, StreamConfig, StreamEndPayload,
    StreamStartPayload, StreamedMutation,
};

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
}
