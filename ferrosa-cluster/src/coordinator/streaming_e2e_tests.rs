//! End-to-end test of the ADR-020 streaming range-read lane.
//!
//! Wires the five pieces under test (idle watchdog, wire variants,
//! StreamRouter, payload structs, consumer/producer, frame router,
//! request handler) through an in-memory "fake wire" so the test
//! exercises the full request → chunks → done → assembled result
//! flow without touching `PeerManager`, sockets, or TLS.
//!
//! Architecture under test (single replica, simplest end-to-end):
//!
//! ```text
//!  test driver
//!     │
//!     │  build StaticReader with N partitions
//!     │  build sink = FakeWireSink { router: Arc<StreamRouter> }
//!     │  router.register(REQ_ID) → mpsc::Receiver
//!     │
//!     │  spawn: handle_stream_request(req, &reader, &sink, chunk_size)
//!     │       └── stream_range_response emits frames into sink
//!     │            └── sink calls frame_router.handle(peer, frame)
//!     │                 └── frame_router routes through StreamRouter
//!     │                      └── chunks land on the registered Receiver
//!     │
//!     │  consume_range_stream(rx, IDLE, expected_done=1, REQ_ID)
//!     │       └── assembles StreamConsumeOutcome { partitions, ... }
//!     │
//!     └── assert partitions == N (same as input)
//! ```
//!
//! This test is the contract that future Phase 2 work (lazy storage
//! iterator, bulk-lane multi-message integration into the lane
//! actor) must preserve.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::PartitionKey;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_net::stream_router::StreamRouter;
use ferrosa_sstable::types::{DeletionTime, Partition};
use ferrosa_storage::TableId;

use crate::raft::handlers::RangeReadStreamRequestPayload;

use super::stream_consumer::{consume_range_stream, StreamConsumeError};
use super::stream_frame_router::StreamFrameRouter;
use super::stream_producer::ChunkSink;
use super::stream_request_handler::{handle_stream_request, PartitionStream, StreamRangeReader};

const IDLE: Duration = Duration::from_secs(2);

fn make_partition(tag: u8) -> Partition {
    let key = DecoratedKey::new(PartitionKey::new(vec![tag]));
    Partition {
        key,
        deletion: DeletionTime::LIVE,
        static_row: None,
        rows: vec![],
    }
}

fn peer() -> PeerId {
    (Uuid::nil(), "127.0.0.1:7000".parse().unwrap())
}

/// In-memory sink that simulates the on-wire path: every emitted
/// frame is fed to the coordinator's `StreamFrameRouter`. In
/// production the path is `producer → PeerManager::fire →
/// network → lane inbound dispatch → frame_router`; here we
/// short-circuit through a shared `Arc<StreamRouter>`.
struct FakeWireSink {
    frame_router: StreamFrameRouter,
    from: PeerId,
}

#[async_trait]
impl ChunkSink for FakeWireSink {
    async fn send(&self, msg: Message) {
        // Match the production RpcHandler dispatch shape.
        let _ = self.frame_router.handle(self.from, msg).await;
    }
}

struct StaticReader {
    partitions: Vec<Partition>,
}
impl StreamRangeReader for StaticReader {
    fn range_iter<'a>(
        &'a self,
        _table_id: &TableId,
    ) -> ferrosa_common::Result<PartitionStream<'a>> {
        let items: Vec<ferrosa_common::Result<Partition>> =
            self.partitions.iter().cloned().map(Ok).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
}

/// Single replica, 10 partitions, chunk_size=3 → 4 chunks (3+3+3+1)
/// + 1 Done. Consumer reassembles all 10 partitions in arrival
/// order.
#[tokio::test]
async fn end_to_end_single_replica_streams_all_partitions() {
    let router = Arc::new(StreamRouter::new());
    const REQ_ID: u32 = 0xCAFE_F00D;
    let rx = router.register(REQ_ID, 8);

    let sink = FakeWireSink {
        frame_router: StreamFrameRouter::new(router.clone()),
        from: peer(),
    };
    let reader = StaticReader {
        partitions: (1u8..=10).map(make_partition).collect(),
    };
    let req = RangeReadStreamRequestPayload {
        request_id: REQ_ID,
        keyspace: "ks".into(),
        table: "tbl".into(),
    };

    // Producer runs concurrently with the consumer. In production
    // the producer runs on the handler node and the consumer on the
    // coordinator; here both are tasks on the same runtime sharing
    // the in-memory router.
    let producer = tokio::spawn(async move {
        handle_stream_request(req, Arc::new(reader), &sink, 3).await;
    });

    let outcome = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap();
    producer.await.unwrap();

    assert_eq!(outcome.total_chunks, 4, "10 / 3 = 4 chunks (3+3+3+1)");
    assert_eq!(outcome.partitions.len(), 10);
    assert!(!outcome.any_truncated);

    // Cleanup
    router.unregister(REQ_ID);
    assert!(router.is_empty());
}

/// Two replicas, each streaming 5 partitions: consumer waits for
/// both Done frames before resolving and returns combined
/// partitions.
#[tokio::test]
async fn end_to_end_two_replicas_aggregates_both_streams() {
    let router = Arc::new(StreamRouter::new());
    const REQ_ID: u32 = 0x2222_2222;
    let rx = router.register(REQ_ID, 16);

    let frame_router = Arc::new(StreamFrameRouter::new(router.clone()));

    let req = RangeReadStreamRequestPayload {
        request_id: REQ_ID,
        keyspace: "ks".into(),
        table: "tbl".into(),
    };

    let from_a: PeerId = (Uuid::from_u128(1), "127.0.0.1:7001".parse().unwrap());
    let from_b: PeerId = (Uuid::from_u128(2), "127.0.0.1:7002".parse().unwrap());

    let sink_a = FakeWireSinkShared {
        frame_router: frame_router.clone(),
        from: from_a,
    };
    let sink_b = FakeWireSinkShared {
        frame_router: frame_router.clone(),
        from: from_b,
    };

    let reader_a = StaticReader {
        partitions: (1u8..=5).map(make_partition).collect(),
    };
    let reader_b = StaticReader {
        partitions: (6u8..=10).map(make_partition).collect(),
    };

    let req_a = req.clone();
    let req_b = req.clone();
    let p_a = tokio::spawn(async move {
        handle_stream_request(req_a, Arc::new(reader_a), &sink_a, 2).await;
    });
    let p_b = tokio::spawn(async move {
        handle_stream_request(req_b, Arc::new(reader_b), &sink_b, 2).await;
    });

    let outcome = consume_range_stream(rx, IDLE, 2, REQ_ID).await.unwrap();
    p_a.await.unwrap();
    p_b.await.unwrap();

    assert_eq!(outcome.partitions.len(), 10);
    // 5/2 = 3 chunks (2+2+1) per replica → 6 chunks total
    assert_eq!(outcome.total_chunks, 6);
}

/// Variant of FakeWireSink that holds the frame_router by Arc so it
/// can be shared between multiple producer tasks (replicas).
struct FakeWireSinkShared {
    frame_router: Arc<StreamFrameRouter>,
    from: PeerId,
}
#[async_trait]
impl ChunkSink for FakeWireSinkShared {
    async fn send(&self, msg: Message) {
        let _ = self.frame_router.handle(self.from, msg).await;
    }
}

/// Producer never runs → no frames hit the router → consumer's
/// IdleTimeoutWatchdog fires within the deadline.
#[tokio::test(start_paused = true)]
async fn end_to_end_no_producer_trips_idle_watchdog() {
    let router = Arc::new(StreamRouter::new());
    const REQ_ID: u32 = 1;
    let rx = router.register(REQ_ID, 4);

    let err = consume_range_stream(rx, IDLE, 1, REQ_ID).await.unwrap_err();
    assert!(matches!(err, StreamConsumeError::IdleTimeout { .. }));
}
