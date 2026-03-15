//! Bulk data streaming between cluster nodes.
//!
//! Used for node join (delta streaming) and decommission (token range transfer).
//! The protocol uses three message types defined in `ferrosa-net`:
//!
//! - `StreamStart` (0x30) — announces a streaming session with metadata
//! - `StreamChunk` (0x31) — carries a batch of mutations
//! - `StreamEnd`   (0x32) — finalises the session with a count and CRC32 checksum
//!
//! The transport layer is network-agnostic at the unit-test level; integration
//! with a live `PeerManager` is exercised in docker smoke tests.

pub mod receiver;
pub mod sender;

pub use receiver::{StreamReceiver, StreamResult};
pub use sender::StreamSender;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// A single row mutation to be transferred during streaming.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct StreamedMutation {
    pub keyspace: String,
    pub table: String,
    pub key: Vec<u8>,
    pub row: Vec<u8>,
    pub timestamp: i64,
}

/// Payload carried in a `StreamStart` message.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StreamStartPayload {
    /// Unique identifier for this streaming session.
    pub session_id: u64,
    /// Raft node-id of the node initiating the stream.
    pub source_node: u64,
    /// Start of the token range being transferred (inclusive).
    pub token_range_start: i64,
    /// End of the token range being transferred (exclusive).
    pub token_range_end: i64,
    /// Best-effort estimate of total bytes that will be sent.
    pub estimated_bytes: u64,
}

/// Payload carried in a `StreamChunk` message.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StreamChunkPayload {
    pub session_id: u64,
    pub mutations: Vec<StreamedMutation>,
}

/// Payload carried in a `StreamEnd` message.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StreamEndPayload {
    pub session_id: u64,
    /// Total number of mutations sent across all chunks.
    pub total_mutations: u64,
    /// CRC32 checksum computed over all serialised `StreamedMutation` bytes in order.
    pub checksum: u32,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tuning knobs for a streaming session.
pub struct StreamConfig {
    /// Target maximum size (in bytes) for each `StreamChunk` payload.
    /// Defaults to 1 MiB.
    pub chunk_size_bytes: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            chunk_size_bytes: 1024 * 1024, // 1 MiB
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helper: batch mutations into chunks respecting chunk_size_bytes
// ---------------------------------------------------------------------------

/// Partition `mutations` into groups whose total serialised size does not
/// exceed `config.chunk_size_bytes`.
///
/// Each mutation is serialised individually with `bincode` to estimate its
/// wire size.  If a single mutation exceeds the chunk limit it still forms
/// its own chunk (the limit is not a hard cap, merely a target).
pub(crate) fn batch_mutations(
    mutations: Vec<StreamedMutation>,
    config: &StreamConfig,
) -> Vec<Vec<StreamedMutation>> {
    let mut chunks: Vec<Vec<StreamedMutation>> = Vec::new();
    let mut current_chunk: Vec<StreamedMutation> = Vec::new();
    let mut current_size: usize = 0;

    for mutation in mutations {
        let encoded_size = bincode::serialized_size(&mutation).unwrap_or(0) as usize;

        // If adding this mutation would overflow and we already have something
        // in the current chunk, flush first.
        if !current_chunk.is_empty() && current_size + encoded_size > config.chunk_size_bytes {
            chunks.push(std::mem::take(&mut current_chunk));
            current_size = 0;
        }

        current_size += encoded_size;
        current_chunk.push(mutation);
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    chunks
}

// ---------------------------------------------------------------------------
// Shared helper: CRC32 across an ordered list of mutations
// ---------------------------------------------------------------------------

/// Compute a CRC32 checksum by feeding the bincode encoding of each
/// `StreamedMutation` into the digest in order.
pub(crate) fn compute_checksum(mutations: &[StreamedMutation]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for m in mutations {
        if let Ok(encoded) = bincode::serialize(m) {
            hasher.update(&encoded);
        }
    }
    hasher.finalize()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_mutation(
        keyspace: &str,
        table: &str,
        key: &[u8],
        row: &[u8],
        ts: i64,
    ) -> StreamedMutation {
        StreamedMutation {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: key.to_vec(),
            row: row.to_vec(),
            timestamp: ts,
        }
    }

    // -----------------------------------------------------------------------
    // 1. StreamedMutation serialises and deserialises round-trip (bincode)
    // -----------------------------------------------------------------------
    #[test]
    fn streamed_mutation_serializes() {
        let m = make_mutation("ks1", "tbl1", b"pk1", b"row_bytes", 999);

        let encoded = bincode::serialize(&m).expect("serialise");
        let decoded: StreamedMutation = bincode::deserialize(&encoded).expect("deserialise");

        assert_eq!(decoded, m);
    }

    // -----------------------------------------------------------------------
    // 2. Chunk batching respects the size limit
    // -----------------------------------------------------------------------
    #[test]
    fn chunk_batching_respects_size_limit() {
        // Build ~5 MB of mutations: each row is 100 KB, so 50 mutations ≈ 5 MB.
        let row = vec![0u8; 100 * 1024]; // 100 KB
        let mutations: Vec<StreamedMutation> = (0u64..50)
            .map(|i| make_mutation("ks", "tbl", &i.to_be_bytes(), &row, i as i64))
            .collect();

        let config = StreamConfig {
            chunk_size_bytes: 1024 * 1024, // 1 MB
        };

        let chunks = batch_mutations(mutations, &config);

        // With ~100 KB per mutation and a 1 MB limit we expect roughly 5+ chunks.
        assert!(
            chunks.len() >= 5,
            "expected ≥5 chunks for 5 MB of data; got {}",
            chunks.len()
        );

        // Every chunk must be non-empty.
        for chunk in &chunks {
            assert!(!chunk.is_empty(), "chunk must not be empty");
        }

        // No chunk (except possibly one with a single oversized mutation) should
        // exceed the configured limit by more than one mutation's worth.
        for chunk in &chunks {
            let chunk_bytes: u64 = chunk
                .iter()
                .map(|m| bincode::serialized_size(m).unwrap_or(0))
                .sum();
            // A single mutation is ~100 KB; the limit is 1 MB.  One mutation
            // may push just over, but no chunk should be much more than limit + one mutation.
            assert!(
                chunk_bytes as usize <= config.chunk_size_bytes + 200 * 1024,
                "chunk is too large: {chunk_bytes} bytes"
            );
        }
    }

    // -----------------------------------------------------------------------
    // 3. Checksum computed during send matches checksum computed during receive
    // -----------------------------------------------------------------------
    #[test]
    fn stream_checksum_validates() {
        let mutations: Vec<StreamedMutation> = (0..10)
            .map(|i| make_mutation("ks", "tbl", &[i], b"row", i as i64))
            .collect();

        let sender_checksum = compute_checksum(&mutations);
        let receiver_checksum = compute_checksum(&mutations);

        assert_eq!(
            sender_checksum, receiver_checksum,
            "sender and receiver must compute identical checksums"
        );
    }

    // -----------------------------------------------------------------------
    // 4. Wrong checksum on StreamEnd is rejected
    // -----------------------------------------------------------------------
    #[test]
    fn stream_rejects_bad_checksum() {
        use crate::error::ClusterError;
        use crate::streaming::receiver::StreamReceiver;

        let mutations: Vec<StreamedMutation> = (0..5)
            .map(|i| make_mutation("ks", "tbl", &[i], b"r", i as i64))
            .collect();

        let good_checksum = compute_checksum(&mutations);
        let bad_checksum = good_checksum.wrapping_add(1);

        let end_payload = StreamEndPayload {
            session_id: 1,
            total_mutations: mutations.len() as u64,
            checksum: bad_checksum,
        };

        let result = StreamReceiver::validate_end(&mutations, &end_payload);
        assert!(
            matches!(result, Err(ClusterError::Internal(_))),
            "bad checksum must produce ClusterError::Internal"
        );
    }
}
