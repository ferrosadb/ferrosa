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

pub mod handler;
pub mod receiver;
pub mod sender;
pub mod sstable_transfer;

pub use handler::{SstableStreamHandler, StreamHandler};
pub use receiver::{SstableStreamReceiver, SstableStreamResult, StreamReceiver, StreamResult};
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

// ---------------------------------------------------------------------------
// Streamed partition payload (`StreamedMutation::row`)
// ---------------------------------------------------------------------------

/// Byte in the cell-tag slot that marks a [`StreamedMutation::row`] as a
/// versioned whole-partition envelope. Never a valid cell tag: `0`/`1` are the
/// simple layout's Option tag and `2` is
/// [`COMPLEX_CELL_TAG`](crate::raft::handlers::COMPLEX_CELL_TAG).
pub const STREAMED_PARTITION_TAG: u8 = 0xF5;

/// The only envelope version this build reads or writes.
pub const STREAMED_PARTITION_VERSION: u8 = 1;

/// Clustering bytes of the decoy row that opens the envelope.
const STREAMED_PARTITION_MARKER: &[u8] = b"ferrosa:streamed-partition";

/// Version 1 envelope body: everything a partition holds.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct StreamedPartitionV1 {
    deletion: crate::raft::handlers::DeletionTimeWire,
    static_row: Option<crate::raft::handlers::RowWire>,
    rows: Vec<crate::raft::handlers::RowWire>,
}

/// The fixed bytes that open every envelope, as the legacy decoder reads
/// them: `Vec<RowWire>` length 1, a row whose clustering is
/// [`STREAMED_PARTITION_MARKER`], one cell at column 0, and then
/// [`STREAMED_PARTITION_TAG`] where that cell's leading tag belongs.
///
/// A build that reads only `Vec<RowWire>` therefore fails the payload on that
/// tag with a typed decode error (`InvalidTagEncoding(245)` before the
/// complex-cell format, "unknown leading tag 245" after it) instead of
/// storing the rows and silently dropping the static row and partition
/// deletion. No legacy payload can start with these bytes, because 0xF5 is
/// not a valid cell tag, so detection is unambiguous.
fn streamed_partition_prefix() -> Vec<u8> {
    let mut prefix = Vec::with_capacity(64);
    // Writing into a Vec cannot fail; the expects document that.
    bincode::serialize_into(&mut prefix, &1u64).expect("Vec write");
    bincode::serialize_into(&mut prefix, STREAMED_PARTITION_MARKER).expect("Vec write");
    bincode::serialize_into(&mut prefix, &1u64).expect("Vec write");
    bincode::serialize_into(&mut prefix, &0u16).expect("Vec write");
    prefix.push(STREAMED_PARTITION_TAG);
    prefix
}

/// A streamed partition's clustered rows, converted from the decoded wire
/// vector one at a time as they are pulled (never collected a second time).
pub type StreamedRows = std::iter::Map<
    std::vec::IntoIter<crate::raft::handlers::RowWire>,
    fn(crate::raft::handlers::RowWire) -> ferrosa_sstable::types::Row,
>;

/// The state of one streamed partition, decoded from either payload format.
pub struct StreamedPartition {
    pub deletion: ferrosa_sstable::types::DeletionTime,
    pub static_row: Option<ferrosa_sstable::types::Row>,
    pub rows: StreamedRows,
}

fn streamed_rows(rows: Vec<crate::raft::handlers::RowWire>) -> StreamedRows {
    let convert: fn(crate::raft::handlers::RowWire) -> ferrosa_sstable::types::Row =
        ferrosa_sstable::types::Row::from;
    rows.into_iter().map(convert)
}

/// Encode a partition's state for [`StreamedMutation::row`].
///
/// A partition with no static row and a LIVE deletion is encoded in the
/// legacy `Vec<RowWire>` layout, byte for byte, so it still streams to a node
/// built before the envelope. Anything else uses the version-1 envelope,
/// which such a node refuses with a decode error: it opens with a decoy legacy
/// row whose first cell tag is [`STREAMED_PARTITION_TAG`].
pub fn encode_streamed_partition(
    partition: &ferrosa_sstable::types::Partition,
) -> Result<Vec<u8>, bincode::Error> {
    use crate::raft::handlers::RowWire;
    let rows: Vec<RowWire> = partition.rows.iter().cloned().map(RowWire::from).collect();
    if partition.static_row.is_none() && partition.deletion.is_live() {
        return bincode::serialize(&rows);
    }
    let body = StreamedPartitionV1 {
        deletion: partition.deletion.into(),
        static_row: partition.static_row.clone().map(RowWire::from),
        rows,
    };
    let mut out = streamed_partition_prefix();
    out.push(STREAMED_PARTITION_VERSION);
    bincode::serialize_into(&mut out, &body)?;
    Ok(out)
}

/// Decode a [`StreamedMutation::row`] in either format. Unknown envelope
/// versions and bytes that decode as neither format are errors.
pub fn decode_streamed_partition(bytes: &[u8]) -> Result<StreamedPartition, String> {
    use crate::raft::handlers::RowWire;
    use bincode::Options;
    let prefix = streamed_partition_prefix();
    let Some(rest) = bytes.strip_prefix(prefix.as_slice()) else {
        let rows: Vec<RowWire> = bincode::deserialize(bytes)
            .map_err(|e| format!("do not decode as Vec<RowWire>: {e}"))?;
        return Ok(StreamedPartition {
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            static_row: None,
            rows: streamed_rows(rows),
        });
    };
    let (&version, body) = rest
        .split_first()
        .ok_or_else(|| "streamed partition envelope has no version byte".to_string())?;
    if version != STREAMED_PARTITION_VERSION {
        return Err(format!(
            "streamed partition envelope version {version} is not supported \
             (this build reads version {STREAMED_PARTITION_VERSION}); sent by a newer node?"
        ));
    }
    let body: StreamedPartitionV1 = bincode::options()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(body)
        .map_err(|e| format!("streamed partition envelope v1 does not decode: {e}"))?;
    Ok(StreamedPartition {
        deletion: body.deletion.into(),
        static_row: body.static_row.map(Into::into),
        rows: streamed_rows(body.rows),
    })
}

impl StreamedMutation {
    /// Encode one whole partition for row streaming (bootstrap, decommission,
    /// rebalance): its clustered rows, static row and partition deletion. The
    /// single encoder every sender uses, so what crosses the wire is decided
    /// here and nowhere else.
    pub fn from_partition(
        keyspace: &str,
        table: &str,
        partition: &ferrosa_sstable::types::Partition,
    ) -> Result<Self, bincode::Error> {
        let row = encode_streamed_partition(partition)?;
        let newest = ferrosa_storage::partition_apply::partition_write_timestamp(partition);
        Ok(Self {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: partition.key.key.as_bytes().to_vec(),
            row,
            timestamp: if newest == i64::MIN { 0 } else { newest },
        })
    }
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
// SSTable file-based streaming wire types
// ---------------------------------------------------------------------------

/// Payload carried in an `SstableStreamStart` message.
///
/// Announces a session that will transfer SSTable component files
/// (Data.db, Index.db, Filter.db, etc.) as raw byte chunks.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SstableStreamStartPayload {
    /// Unique identifier for this streaming session.
    pub session_id: u64,
    /// Raft node-id of the node initiating the stream.
    pub source_node: u64,
    /// Keyspace owning the SSTable.
    pub keyspace: String,
    /// Table owning the SSTable.
    pub table: String,
    /// SSTable generation/identifier.
    pub sstable_id: String,
    /// Component files with their sizes — receiver uses this to know when
    /// all data has arrived.
    pub components: Vec<sstable_transfer::SSTableComponent>,
    /// Total bytes across all components.
    pub total_bytes: u64,
}

/// Payload carried in an `SstableStreamChunk` message.
///
/// Each chunk carries a contiguous slice of one component file.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SstableStreamChunkPayload {
    pub session_id: u64,
    /// Component name (e.g. "Data.db").
    pub component: String,
    /// Byte offset within the component file.
    pub offset: u64,
    /// Raw file data.
    pub data: Vec<u8>,
}

/// Payload carried in an `SstableStreamEnd` message.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SstableStreamEndPayload {
    pub session_id: u64,
    /// Total bytes sent across all chunks.
    pub total_bytes: u64,
    /// CRC32 checksum computed over all chunk data bytes in send order.
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
    // Streamed partition envelope: mixed-version compatibility
    // -----------------------------------------------------------------------

    /// Frozen pre-complex-cell `RowWire` (derived serde, Option tag cells):
    /// the receiver every release before 2026-10-03 shipped.
    mod frozen_v0 {
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        pub struct CellValueWire {
            pub value: Option<Vec<u8>>,
            pub timestamp: i64,
            pub ttl: i32,
            pub local_deletion_time: i32,
        }
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        pub struct DeletionTimeWire {
            pub marked_for_delete_at: i64,
            pub local_deletion_time: u32,
        }
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        pub struct LivenessInfoWire {
            pub timestamp: i64,
            pub ttl: i32,
            pub local_deletion_time: i32,
        }
        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        pub struct RowWire {
            pub clustering: Vec<u8>,
            pub cells: Vec<(u16, CellValueWire)>,
            pub deletion: DeletionTimeWire,
            pub primary_key_liveness: LivenessInfoWire,
        }
    }

    fn partition_with(
        deletion: ferrosa_sstable::types::DeletionTime,
        static_row: Option<ferrosa_sstable::types::Row>,
    ) -> ferrosa_sstable::types::Partition {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            deletion,
            static_row,
            rows: vec![Row {
                clustering: vec![0, 0, 0, 1],
                cells: vec![(1, CellValue::live(b"v".to_vec(), 700))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(700),
            }],
        }
    }
    use ferrosa_sstable::types::Partition;

    fn static_row() -> ferrosa_sstable::types::Row {
        use ferrosa_common::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"s".to_vec(), 600))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::NONE,
        }
    }

    #[test]
    fn envelope_round_trips_static_row_and_partition_deletion() {
        let deleted = ferrosa_sstable::types::DeletionTime::new(500, 1_700_000_000);
        let p = partition_with(deleted, Some(static_row()));
        let decoded = decode_streamed_partition(&encode_streamed_partition(&p).unwrap()).unwrap();
        assert_eq!(decoded.deletion, p.deletion);
        assert_eq!(decoded.static_row, p.static_row);
        assert_eq!(decoded.rows.collect::<Vec<_>>(), p.rows);
    }

    /// An OLD receiver (frozen pre-complex-cell structs) must fail an envelope
    /// with a typed decode error, never decode it as rows and drop the static
    /// row and partition deletion.
    #[test]
    fn pre_complex_cell_receiver_rejects_envelope_with_typed_error() {
        let deleted = ferrosa_sstable::types::DeletionTime::new(500, 1_700_000_000);
        for p in [
            partition_with(deleted, None),
            partition_with(
                ferrosa_sstable::types::DeletionTime::LIVE,
                Some(static_row()),
            ),
        ] {
            let bytes = encode_streamed_partition(&p).unwrap();
            let err = bincode::deserialize::<Vec<frozen_v0::RowWire>>(&bytes).unwrap_err();
            assert!(
                matches!(
                    *err,
                    bincode::ErrorKind::InvalidTagEncoding(t) if t == STREAMED_PARTITION_TAG as usize
                ),
                "expected InvalidTagEncoding(0xF5), got {err:?}"
            );
        }
    }

    /// The previous receiver (complex-cell `RowWire`, `Vec<RowWire>` only)
    /// must also refuse the envelope rather than store a decoy row.
    #[test]
    fn complex_cell_receiver_rejects_envelope_with_typed_error() {
        use crate::raft::handlers::RowWire;
        let deleted = ferrosa_sstable::types::DeletionTime::new(500, 1_700_000_000);
        let bytes = encode_streamed_partition(&partition_with(deleted, None)).unwrap();
        let err = bincode::deserialize::<Vec<RowWire>>(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("unknown leading tag 245"),
            "expected the unknown-tag error, got {err}"
        );
    }

    /// A partition with nothing beyond rows keeps the legacy bytes, so it
    /// still streams to an old receiver, and a NEW receiver reads legacy
    /// payloads produced by the frozen encoder.
    #[test]
    fn plain_partition_keeps_legacy_bytes_both_directions() {
        use crate::raft::handlers::RowWire;
        let p = partition_with(ferrosa_sstable::types::DeletionTime::LIVE, None);
        let bytes = encode_streamed_partition(&p).unwrap();
        let legacy: Vec<RowWire> = p.rows.iter().cloned().map(RowWire::from).collect();
        assert_eq!(bytes, bincode::serialize(&legacy).unwrap());
        let old: Vec<frozen_v0::RowWire> = bincode::deserialize(&bytes).unwrap();
        assert_eq!(old.len(), 1);

        let frozen = vec![frozen_v0::RowWire {
            clustering: vec![0, 0, 0, 1],
            cells: vec![(
                1,
                frozen_v0::CellValueWire {
                    value: Some(b"v".to_vec()),
                    timestamp: 700,
                    ttl: 0,
                    local_deletion_time: i32::MAX,
                },
            )],
            deletion: frozen_v0::DeletionTimeWire {
                marked_for_delete_at: i64::MIN,
                local_deletion_time: u32::MAX,
            },
            primary_key_liveness: frozen_v0::LivenessInfoWire {
                timestamp: 700,
                ttl: 0,
                local_deletion_time: i32::MAX,
            },
        }];
        let decoded = decode_streamed_partition(&bincode::serialize(&frozen).unwrap()).unwrap();
        assert!(decoded.deletion.is_live());
        assert_eq!(decoded.static_row, None);
        assert_eq!(decoded.rows.collect::<Vec<_>>(), p.rows);
    }

    /// A future envelope version is refused, not guessed at.
    #[test]
    fn unknown_envelope_version_is_refused() {
        let deleted = ferrosa_sstable::types::DeletionTime::new(500, 1_700_000_000);
        let mut bytes = encode_streamed_partition(&partition_with(deleted, None)).unwrap();
        let version_at = streamed_partition_prefix().len();
        bytes[version_at] = STREAMED_PARTITION_VERSION + 1;
        let Err(err) = decode_streamed_partition(&bytes) else {
            panic!("an unknown envelope version must be refused");
        };
        assert!(err.contains("version 2 is not supported"), "{err}");
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

    // -----------------------------------------------------------------------
    // 5. SSTable streaming payloads round-trip through bincode
    // -----------------------------------------------------------------------
    #[test]
    fn sstable_stream_payloads_serialize() {
        let start = SstableStreamStartPayload {
            session_id: 1,
            source_node: 42,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            sstable_id: "mc-001".to_string(),
            components: vec![sstable_transfer::SSTableComponent {
                name: "Data.db".to_string(),
                size: 4096,
            }],
            total_bytes: 4096,
        };
        let encoded = bincode::serialize(&start).unwrap();
        let decoded: SstableStreamStartPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.session_id, 1);
        assert_eq!(decoded.sstable_id, "mc-001");

        let chunk = SstableStreamChunkPayload {
            session_id: 1,
            component: "Data.db".to_string(),
            offset: 0,
            data: vec![0xFF; 100],
        };
        let encoded = bincode::serialize(&chunk).unwrap();
        let decoded: SstableStreamChunkPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.data.len(), 100);

        let end = SstableStreamEndPayload {
            session_id: 1,
            total_bytes: 4096,
            checksum: 0xCAFE,
        };
        let encoded = bincode::serialize(&end).unwrap();
        let decoded: SstableStreamEndPayload = bincode::deserialize(&encoded).unwrap();
        assert_eq!(decoded.checksum, 0xCAFE);
    }
}
