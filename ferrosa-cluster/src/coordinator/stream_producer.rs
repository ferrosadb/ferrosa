//! Producer for streaming range-read responses (ADR-020).
//!
//! Takes a request payload and a slice of partitions, emits a
//! sequence of `Message::RangeReadStreamChunk` frames followed by a
//! single `Message::RangeReadStreamDone` terminator, all keyed by
//! the request's `request_id` with monotonic `seq`.
//!
//! Transport-free: emits frames through a [`ChunkSink`] trait that
//! the production code implements over `PeerManager::fire` and tests
//! implement over an in-memory `Vec<Message>`.
//!
//! Phase 1 takes a materialized `&[Partition]` (sourced from the
//! existing `StorageEngine::read_range_limited_rows`) and chunks it
//! with `slice::chunks`. Phase 2 will replace the slice with a lazy
//! `impl Iterator<Item = Result<Partition>>` once the storage layer
//! grows a streaming iterator surface — the [`ChunkSink`] contract
//! and frame shape do not change.

use async_trait::async_trait;
use bytes::Bytes;

use ferrosa_sstable::types::Partition;

use crate::raft::handlers::{
    serialize_partition_to_wire_borrowed, RangeReadStreamDonePayload, RangeReadStreamRequestPayload,
};
// `RangeReadStreamChunkPayload` is referenced only from the
// in-module tests (decode_chunk uses it as the deserialise
// target) — re-import it under `cfg(test)` so non-test builds
// don't see an unused-import warning under
// `-D unused-imports`.
#[cfg(test)]
use crate::raft::handlers::RangeReadStreamChunkPayload;
use ferrosa_net::message::Message;

/// Where a stream producer pushes outbound frames. The production
/// implementation forwards to `PeerManager::fire(..., Lane::Bulk)`;
/// tests collect into a `Vec`.
#[async_trait]
pub trait ChunkSink: Send + Sync {
    /// Push one frame. Implementations should be fire-and-forget —
    /// errors are logged at the implementation site, not returned,
    /// so a stalled peer cannot block the producer.
    async fn send(&self, msg: Message);
}

/// Emit a streaming range-read response: N chunks of at most
/// `chunk_size` partitions each, followed by a single Done frame.
///
/// `chunk_size` must be at least 1.
///
/// `truncated` is set on the Done frame to signal that the partition
/// list was bounded externally (Phase 1: by the existing
/// `RANGE_READ_MATERIALIZATION_CAP` in `ferrosa-storage`). Phase 2's
/// lazy iterator removes that cap and the flag becomes unused.
///
/// # Panics
/// Panics if `chunk_size == 0` (would loop forever otherwise).
pub async fn stream_range_response<S: ChunkSink>(
    req: &RangeReadStreamRequestPayload,
    partitions: &[Partition],
    chunk_size: usize,
    truncated: bool,
    sink: &S,
) {
    assert!(chunk_size > 0, "chunk_size must be >= 1; got 0");

    let mut seq: u32 = 0;
    for chunk in partitions.chunks(chunk_size) {
        // Emit the chunk's bytes directly, without first
        // materialising `Vec<PartitionWire>` (which would clone
        // every cell value in the chunk).
        //
        // Wire format of `RangeReadStreamChunkPayload` is
        // `request_id: u32 || seq: u32 || partitions: Vec<PartitionWire>`,
        // and bincode encodes `Vec<T>` as `u64-LE length || items`.
        // Emitting the three fields in declaration order, then
        // each partition via `serialize_partition_to_wire_borrowed`,
        // produces byte-identical output to
        // `bincode::serialize(&RangeReadStreamChunkPayload{..})`.
        // Equivalence is pinned at the helper-level by
        // `serialize_partition_to_wire_borrowed_matches_legacy`
        // and at this call site by
        // `stream_range_response_borrowed_emit_matches_legacy_collect`.
        let mut body: Vec<u8> = Vec::new();
        bincode::serialize_into(&mut body, &req.request_id)
            .expect("RangeReadStreamChunkPayload header serialization is infallible");
        bincode::serialize_into(&mut body, &seq)
            .expect("RangeReadStreamChunkPayload header serialization is infallible");
        let chunk_partitions_len = chunk.len() as u64;
        bincode::serialize_into(&mut body, &chunk_partitions_len)
            .expect("RangeReadStreamChunkPayload header serialization is infallible");
        for partition in chunk {
            serialize_partition_to_wire_borrowed(&mut body, partition)
                .expect("PartitionWire serialization is infallible");
        }
        sink.send(Message::RangeReadStreamChunk(Bytes::from(body)))
            .await;
        seq = seq.saturating_add(1);
    }

    let done = RangeReadStreamDonePayload {
        request_id: req.request_id,
        total_chunks: seq,
        truncated,
    };
    let bytes =
        bincode::serialize(&done).expect("RangeReadStreamDonePayload serialization is infallible");
    sink.send(Message::RangeReadStreamDone(Bytes::from(bytes)))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, Partition};

    /// Test sink: collect every emitted Message into a Vec for
    /// inspection.
    struct VecSink {
        sent: Mutex<Vec<Message>>,
    }

    impl VecSink {
        fn new() -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
            }
        }
        fn into_inner(self) -> Vec<Message> {
            self.sent.into_inner().unwrap()
        }
    }

    #[async_trait]
    impl ChunkSink for VecSink {
        async fn send(&self, msg: Message) {
            self.sent.lock().unwrap().push(msg);
        }
    }

    fn make_partition(tag: u8) -> Partition {
        let key = DecoratedKey::new(PartitionKey::new(vec![tag]));
        Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![],
        }
    }

    fn decode_chunk(msg: &Message) -> RangeReadStreamChunkPayload {
        if let Message::RangeReadStreamChunk(b) = msg {
            bincode::deserialize(b).expect("chunk decode")
        } else {
            panic!("expected RangeReadStreamChunk, got {msg:?}");
        }
    }

    fn decode_done(msg: &Message) -> RangeReadStreamDonePayload {
        if let Message::RangeReadStreamDone(b) = msg {
            bincode::deserialize(b).expect("done decode")
        } else {
            panic!("expected RangeReadStreamDone, got {msg:?}");
        }
    }

    fn req(id: u32) -> RangeReadStreamRequestPayload {
        RangeReadStreamRequestPayload {
            request_id: id,
            keyspace: "ks".into(),
            table: "tbl".into(),
            projected_regular_ordinals: None,
        }
    }

    /// 7 partitions chunked at 3 → 3 chunk frames (3+3+1) + 1 Done.
    /// seq is monotonic 0..3. Done reports total_chunks=3.
    #[tokio::test]
    async fn chunks_partitions_into_seq_then_emits_done() {
        let partitions: Vec<Partition> = (1u8..=7).map(make_partition).collect();
        let sink = VecSink::new();

        stream_range_response(&req(99), &partitions, 3, false, &sink).await;

        let frames = sink.into_inner();
        assert_eq!(frames.len(), 4, "3 chunks + 1 done");

        let c0 = decode_chunk(&frames[0]);
        let c1 = decode_chunk(&frames[1]);
        let c2 = decode_chunk(&frames[2]);
        assert_eq!(c0.seq, 0);
        assert_eq!(c1.seq, 1);
        assert_eq!(c2.seq, 2);
        assert_eq!(c0.partitions.len(), 3);
        assert_eq!(c1.partitions.len(), 3);
        assert_eq!(c2.partitions.len(), 1);

        for c in [&c0, &c1, &c2] {
            assert_eq!(c.request_id, 99);
        }

        let done = decode_done(&frames[3]);
        assert_eq!(done.request_id, 99);
        assert_eq!(done.total_chunks, 3);
        assert!(!done.truncated);
    }

    /// Empty partition list still emits one Done frame with
    /// total_chunks=0 — the coordinator must observe a terminator
    /// for every replica it requested, even empty ones.
    #[tokio::test]
    async fn empty_partitions_emits_only_done_with_zero_chunks() {
        let sink = VecSink::new();
        stream_range_response(&req(1), &[], 4, false, &sink).await;

        let frames = sink.into_inner();
        assert_eq!(frames.len(), 1, "no chunks, just Done");
        let done = decode_done(&frames[0]);
        assert_eq!(done.total_chunks, 0);
    }

    /// `truncated=true` from the caller propagates through to the
    /// Done frame so the coordinator can mark partial results.
    #[tokio::test]
    async fn truncated_flag_propagates_to_done() {
        let sink = VecSink::new();
        stream_range_response(&req(2), &[make_partition(1)], 1, true, &sink).await;

        let frames = sink.into_inner();
        let done = decode_done(&frames[1]);
        assert!(done.truncated);
    }

    /// One frame per partition when chunk_size=1 — exercises the
    /// degenerate chunking case to make sure seq still advances.
    #[tokio::test]
    async fn chunk_size_one_yields_one_frame_per_partition() {
        let partitions: Vec<Partition> = (1u8..=5).map(make_partition).collect();
        let sink = VecSink::new();

        stream_range_response(&req(3), &partitions, 1, false, &sink).await;

        let frames = sink.into_inner();
        assert_eq!(frames.len(), 6, "5 chunks + 1 done");
        for (i, frame) in frames[..5].iter().enumerate() {
            let c = decode_chunk(frame);
            assert_eq!(c.seq, i as u32);
            assert_eq!(c.partitions.len(), 1);
        }
        let done = decode_done(&frames[5]);
        assert_eq!(done.total_chunks, 5);
    }

    #[tokio::test]
    #[should_panic(expected = "chunk_size must be >= 1")]
    async fn zero_chunk_size_panics() {
        let sink = VecSink::new();
        stream_range_response(&req(0), &[], 0, false, &sink).await;
    }
}
