//! Server-side inbound dispatch for `RangeReadStreamRequest`.
//!
//! Registered with [`HandlerRegistry`] on every node so an inbound
//! request from a coordinator triggers a streaming response. The
//! [`RpcHandler::handle`] entry point decodes the request, spawns a
//! tokio task to do the actual storage read + frame emission via
//! [`super::stream_producer::stream_range_response`], and returns
//! `None` immediately — the streaming response goes out through the
//! [`ChunkSink`] (production: `PeerManager::fire` on `Lane::Bulk`).
//!
//! Decode failures and storage errors do not synthesize an error
//! reply frame today: the coordinator times out via its
//! [`super::stream_consumer::IdleTimeoutWatchdog`] and surfaces the
//! partial result. Storage errors emit a `RangeReadStreamDone` with
//! `truncated = true` so the coordinator sees a clean terminator
//! and can mark the replica as partial without waiting for the
//! watchdog.
//!
//! [`HandlerRegistry`]: ferrosa_net::rpc::handler::HandlerRegistry

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_sstable::types::Partition;
use ferrosa_storage::TableId;

use super::stream_producer::{stream_range_response, ChunkSink};
use crate::raft::handlers::{RangeReadStreamDonePayload, RangeReadStreamRequestPayload};

/// Storage surface the request handler needs. Mirrors
/// `RangeReadStorage` in `coordinator::read` but is its own trait so
/// tests can swap in an in-memory backing without dragging the full
/// `StorageEngine`.
pub trait StreamRangeReader: Send + Sync {
    /// Read partitions for `table_id`. The Phase 1 implementation
    /// returns a materialized Vec; Phase 2 will return a lazy
    /// iterator (see ADR-020) and the handler will chunk it without
    /// materializing.
    ///
    /// Returns `Err` for table-not-found / IO / decode failures — the
    /// handler turns these into a truncated Done so the coordinator
    /// resolves quickly.
    fn read_range(&self, table_id: &TableId) -> ferrosa_common::Result<Vec<Partition>>;
}

impl StreamRangeReader for ferrosa_storage::StorageEngine {
    fn read_range(&self, table_id: &TableId) -> ferrosa_common::Result<Vec<Partition>> {
        // Phase 1: existing materializing read path, capped at the
        // 10K RANGE_READ_MATERIALIZATION_CAP inside ferrosa-storage.
        // ADR-020 Phase 2 replaces this with a streaming iterator.
        ferrosa_storage::engine::StorageEngine::read_range(
            self,
            table_id,
            None,
            None,
            crate::write_path::DEFAULT_RANGE_READ_LIMIT,
        )
    }
}

/// Pure request handler — separated from the `RpcHandler` impl so
/// tests can drive it with an in-memory sink + reader.
///
/// On storage error: emits a single `RangeReadStreamDone` with
/// `truncated = true` and `total_chunks = 0` so the coordinator
/// gets a clean terminator and marks the replica's contribution as
/// partial.
pub async fn handle_stream_request<R, S>(
    req: RangeReadStreamRequestPayload,
    reader: &R,
    sink: &S,
    chunk_size: usize,
) where
    R: StreamRangeReader,
    S: ChunkSink,
{
    let table_id = TableId::new(&req.keyspace, &req.table);
    let partitions = match reader.read_range(&table_id) {
        Ok(ps) => ps,
        Err(e) => {
            tracing::warn!(
                request_id = req.request_id,
                keyspace = req.keyspace,
                table = req.table,
                "stream request: storage read failed: {e}"
            );
            // Emit truncated Done so the coordinator finishes
            // instead of waiting for the idle-timeout watchdog.
            let done = RangeReadStreamDonePayload {
                request_id: req.request_id,
                total_chunks: 0,
                truncated: true,
            };
            let bytes = bincode::serialize(&done)
                .expect("RangeReadStreamDonePayload serialization is infallible");
            sink.send(Message::RangeReadStreamDone(Bytes::from(bytes))).await;
            return;
        }
    };

    // Phase 1 does not currently exceed any further bound beyond the
    // storage cap; truncated stays false at the handler level.
    stream_range_response(&req, &partitions, chunk_size, false, sink).await;
}

/// `RpcHandler` shell. Decodes the request, spawns the streaming
/// task on the tokio runtime, returns `None`. The spawned task
/// pushes chunks via a `ChunkSink` provided by the caller.
///
/// Production wiring lives outside this module — it builds a sink
/// that forwards to the right peer (via `PeerManager`) and supplies
/// the storage engine.
pub struct RangeReadStreamRequestHandler<R, F>
where
    R: StreamRangeReader + 'static,
    F: SinkFactory + 'static,
{
    reader: Arc<R>,
    sink_factory: Arc<F>,
    chunk_size: usize,
}

/// Builds a per-request `ChunkSink` targeting the originating peer.
/// Production: `PeerManagerSinkFactory` that fires on `Lane::Bulk`.
/// Tests: factory that returns a captured `VecSink`.
pub trait SinkFactory: Send + Sync {
    type Sink: ChunkSink + 'static;
    fn for_peer(&self, from: PeerId, request_id: u32) -> Self::Sink;
}

/// Production sink: fires every chunk back to the originating peer
/// via `PeerManager::fire` on `Lane::Bulk`. Wire-side errors are
/// logged and dropped — the coordinator's `IdleTimeoutWatchdog`
/// surfaces stalled streams independently.
pub struct PeerFireSink {
    peers: Arc<PeerManager>,
    target: uuid::Uuid,
    request_id: u32,
}

impl PeerFireSink {
    pub fn new(peers: Arc<PeerManager>, target: uuid::Uuid, request_id: u32) -> Self {
        Self {
            peers,
            target,
            request_id,
        }
    }
}

#[async_trait]
impl ChunkSink for PeerFireSink {
    async fn send(&self, msg: Message) {
        if let Err(e) = self.peers.fire(self.target, msg, Lane::Bulk).await {
            tracing::warn!(
                request_id = self.request_id,
                target = %self.target,
                "stream chunk fire failed: {e}"
            );
        }
    }
}

/// Production `SinkFactory` that builds a `PeerFireSink` per
/// (peer, request_id). Registered on every node so the request
/// handler can ship chunks back to the originating coordinator.
pub struct PeerManagerSinkFactory {
    peers: Arc<PeerManager>,
}

impl PeerManagerSinkFactory {
    pub fn new(peers: Arc<PeerManager>) -> Self {
        Self { peers }
    }
}

impl SinkFactory for PeerManagerSinkFactory {
    type Sink = PeerFireSink;

    fn for_peer(&self, from: PeerId, request_id: u32) -> PeerFireSink {
        let (host_id, _addr) = from;
        PeerFireSink::new(self.peers.clone(), host_id, request_id)
    }
}

impl<R, F> RangeReadStreamRequestHandler<R, F>
where
    R: StreamRangeReader + 'static,
    F: SinkFactory + 'static,
{
    pub fn new(reader: Arc<R>, sink_factory: Arc<F>, chunk_size: usize) -> Self {
        Self {
            reader,
            sink_factory,
            chunk_size,
        }
    }
}

#[async_trait]
impl<R, F> RpcHandler for RangeReadStreamRequestHandler<R, F>
where
    R: StreamRangeReader + 'static,
    F: SinkFactory + 'static,
{
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::RangeReadStreamRequest(b) => b,
            _ => return None,
        };

        let req: RangeReadStreamRequestPayload = match bincode::deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("RangeReadStreamRequestHandler: decode failed: {e}");
                return None;
            }
        };

        let sink = self.sink_factory.for_peer(from, req.request_id);
        let reader = self.reader.clone();
        let chunk_size = self.chunk_size;

        tokio::spawn(async move {
            handle_stream_request(req, reader.as_ref(), &sink, chunk_size).await;
        });

        // Streaming responses ride on fire-and-forget chunks; no
        // synchronous reply.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::PartitionKey;
    use ferrosa_sstable::types::{DeletionTime, Partition};
    use uuid::Uuid;

    use crate::raft::handlers::{RangeReadStreamChunkPayload, RangeReadStreamDonePayload};

    fn make_partition(tag: u8) -> Partition {
        let key = DecoratedKey::new(PartitionKey::new(vec![tag]));
        Partition {
            key,
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![],
        }
    }

    fn req(id: u32) -> RangeReadStreamRequestPayload {
        RangeReadStreamRequestPayload {
            request_id: id,
            keyspace: "ks".into(),
            table: "tbl".into(),
        }
    }

    /// Test reader returning a fixed Vec<Partition>.
    struct StaticReader {
        partitions: Vec<Partition>,
    }
    impl StreamRangeReader for StaticReader {
        fn read_range(&self, _table_id: &TableId) -> ferrosa_common::Result<Vec<Partition>> {
            Ok(self.partitions.clone())
        }
    }

    /// Test reader that always fails — exercises the
    /// storage-error → truncated-Done path.
    struct FailingReader;
    impl StreamRangeReader for FailingReader {
        fn read_range(&self, _table_id: &TableId) -> ferrosa_common::Result<Vec<Partition>> {
            Err(ferrosa_common::Error::InvalidData("simulated".into()))
        }
    }

    /// In-memory sink that captures every emitted frame.
    struct VecSink {
        sent: Mutex<Vec<Message>>,
    }
    impl VecSink {
        fn new() -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
            }
        }
        fn take(&self) -> Vec<Message> {
            std::mem::take(&mut self.sent.lock().unwrap())
        }
    }
    #[async_trait]
    impl ChunkSink for VecSink {
        async fn send(&self, msg: Message) {
            self.sent.lock().unwrap().push(msg);
        }
    }

    /// Happy path: reader returns 5 partitions, chunk_size=2 →
    /// 3 chunk frames (2+2+1) + 1 Done with total_chunks=3.
    #[tokio::test]
    async fn happy_path_emits_chunks_then_done() {
        let reader = StaticReader {
            partitions: (1u8..=5).map(make_partition).collect(),
        };
        let sink = VecSink::new();

        handle_stream_request(req(11), &reader, &sink, 2).await;

        let frames = sink.take();
        assert_eq!(frames.len(), 4);
        assert!(matches!(frames[0], Message::RangeReadStreamChunk(_)));
        assert!(matches!(frames[1], Message::RangeReadStreamChunk(_)));
        assert!(matches!(frames[2], Message::RangeReadStreamChunk(_)));
        let Message::RangeReadStreamDone(b) = &frames[3] else {
            panic!("last frame must be Done");
        };
        let done: RangeReadStreamDonePayload = bincode::deserialize(b).unwrap();
        assert_eq!(done.total_chunks, 3);
        assert!(!done.truncated);
    }

    /// Empty table: reader returns []. Coordinator still expects a
    /// terminator per replica → exactly one Done with total=0.
    #[tokio::test]
    async fn empty_table_emits_only_done() {
        let reader = StaticReader { partitions: vec![] };
        let sink = VecSink::new();

        handle_stream_request(req(12), &reader, &sink, 4).await;

        let frames = sink.take();
        assert_eq!(frames.len(), 1);
        let Message::RangeReadStreamDone(b) = &frames[0] else {
            panic!("must be Done");
        };
        let done: RangeReadStreamDonePayload = bincode::deserialize(b).unwrap();
        assert_eq!(done.total_chunks, 0);
        assert!(!done.truncated);
    }

    /// Storage error: emit truncated Done so coordinator finishes
    /// without waiting for the idle-timeout watchdog. total_chunks
    /// must be 0 — no partial chunks were sent.
    #[tokio::test]
    async fn storage_error_emits_truncated_done_only() {
        let reader = FailingReader;
        let sink = VecSink::new();

        handle_stream_request(req(13), &reader, &sink, 4).await;

        let frames = sink.take();
        assert_eq!(frames.len(), 1);
        let Message::RangeReadStreamDone(b) = &frames[0] else {
            panic!("must be Done");
        };
        let done: RangeReadStreamDonePayload = bincode::deserialize(b).unwrap();
        assert_eq!(done.total_chunks, 0);
        assert!(done.truncated, "truncated=true signals partial replica");
    }

    /// request_id propagates onto every emitted frame.
    #[tokio::test]
    async fn request_id_appears_on_every_frame() {
        let reader = StaticReader {
            partitions: vec![make_partition(1), make_partition(2), make_partition(3)],
        };
        let sink = VecSink::new();

        handle_stream_request(req(0xDEAD_BEEF), &reader, &sink, 1).await;

        let frames = sink.take();
        for frame in &frames {
            let id = match frame {
                Message::RangeReadStreamChunk(b) => {
                    bincode::deserialize::<RangeReadStreamChunkPayload>(b)
                        .unwrap()
                        .request_id
                }
                Message::RangeReadStreamDone(b) => {
                    bincode::deserialize::<RangeReadStreamDonePayload>(b)
                        .unwrap()
                        .request_id
                }
                other => panic!("unexpected: {other:?}"),
            };
            assert_eq!(id, 0xDEAD_BEEF);
        }
    }

    /// RpcHandler shell: non-request messages are pass-through (None
    /// reply, nothing emitted via the sink).
    #[tokio::test]
    async fn rpc_handler_ignores_non_request_messages() {
        struct StaticFactory;
        impl SinkFactory for StaticFactory {
            type Sink = VecSink;
            fn for_peer(&self, _from: PeerId, _request_id: u32) -> VecSink {
                VecSink::new()
            }
        }
        let handler = RangeReadStreamRequestHandler::new(
            Arc::new(StaticReader { partitions: vec![] }),
            Arc::new(StaticFactory),
            4,
        );

        let from = (Uuid::nil(), "127.0.0.1:7000".parse().unwrap());
        let reply = handler
            .handle(
                from,
                Message::Ping {
                    nonce: 0,
                    sent_at: 0,
                },
            )
            .await;
        assert!(reply.is_none());
    }

    /// RpcHandler shell: malformed RangeReadStreamRequest payload
    /// returns None (no spawned task, coordinator times out).
    #[tokio::test]
    async fn rpc_handler_drops_undecodable_request() {
        struct StaticFactory;
        impl SinkFactory for StaticFactory {
            type Sink = VecSink;
            fn for_peer(&self, _from: PeerId, _request_id: u32) -> VecSink {
                VecSink::new()
            }
        }
        let handler = RangeReadStreamRequestHandler::new(
            Arc::new(StaticReader { partitions: vec![] }),
            Arc::new(StaticFactory),
            4,
        );

        let from = (Uuid::nil(), "127.0.0.1:7000".parse().unwrap());
        // Garbage bytes that won't deserialize.
        let reply = handler
            .handle(
                from,
                Message::RangeReadStreamRequest(Bytes::from_static(&[0xFF; 2])),
            )
            .await;
        assert!(reply.is_none());
    }
}
