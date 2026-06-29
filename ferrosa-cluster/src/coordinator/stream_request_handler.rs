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
//! Decode failures and storage errors emit a `RangeReadStreamDone`
//! with `truncated = true` and stop the stream. The coordinator treats
//! that terminator as an error for the overall read, so corrupt SSTable
//! data cannot be silently converted into a successful partial result.
//!
//! [`HandlerRegistry`]: ferrosa_net::rpc::handler::HandlerRegistry

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_net::task_pool::TaskPool;
use ferrosa_sstable::types::Partition;
use ferrosa_storage::TableId;

use super::stream_producer::ChunkSink;
use crate::raft::handlers::{
    RangeReadStreamCancelPayload, RangeReadStreamDonePayload, RangeReadStreamHeartbeatPayload,
    RangeReadStreamRequestPayload,
};

/// How often to emit a `RangeReadStreamHeartbeat` while a slow
/// storage read blocks. Picked smaller than the coordinator's idle
/// timeout (10s) with margin for tokio scheduling jitter and wire
/// transit; a producer that misses three intervals indicates a real
/// stall.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

/// Default ceiling on the number of clustered rows packed into one stream
/// chunk frame. Matches `ferrosa_storage::range_merger`'s default fragment
/// cap so a single full fragment flushes as one chunk. Env-tunable through
/// the same `FERROSA_RANGE_READ_ROWS_PER_FRAGMENT` knob that bounds the
/// producer, keeping the wire frame and the producer fragment aligned.
const DEFAULT_STREAM_CHUNK_ROW_CAP: usize = 4_096;

pub(crate) fn stream_chunk_row_cap() -> usize {
    match std::env::var("FERROSA_RANGE_READ_ROWS_PER_FRAGMENT") {
        Ok(v) => match v.trim().parse::<usize>() {
            Ok(n) if n >= 1 => n,
            _ => DEFAULT_STREAM_CHUNK_ROW_CAP,
        },
        Err(_) => DEFAULT_STREAM_CHUNK_ROW_CAP,
    }
}

/// Type alias for the per-partition async stream returned by
/// [`StreamRangeReader::range_iter`]. Each item is either a
/// successfully-decoded partition or a (recoverable) per-partition
/// error; producer-fatal errors come back as `Err` from the
/// `range_iter` call itself.
pub type PartitionStream<'a> =
    Pin<Box<dyn Stream<Item = ferrosa_common::Result<Partition>> + Send + 'a>>;

/// Storage surface the request handler pulls from. ADR-020 contract:
/// the reader yields partitions one at a time through an async
/// `Stream`, so the handler's resident memory is bounded by
/// `chunk_size` regardless of total table size. There is no
/// Vec-returning method; materializing the whole partition list at
/// any layer above the per-source iterator is the very antipattern
/// this trait exists to prevent.
pub trait StreamRangeReader: Send + Sync {
    /// Open a lazy stream of every partition in `table_id`,
    /// in token order. Returns `Err` for synchronous open-time
    /// failures (table not found, etc.); per-partition decode
    /// failures surface inside the stream and fail the overall scan
    /// through a truncated Done frame.
    fn range_iter<'a>(
        &'a self,
        table_id: &TableId,
        projected_regular_ordinals: Option<&'a [u16]>,
        start: Option<&'a ferrosa_common::key::DecoratedKey>,
    ) -> ferrosa_common::Result<PartitionStream<'a>>;
}

impl StreamRangeReader for Arc<ferrosa_storage::StorageEngine> {
    fn range_iter<'a>(
        &'a self,
        table_id: &TableId,
        projected_regular_ordinals: Option<&'a [u16]>,
        start: Option<&'a ferrosa_common::key::DecoratedKey>,
    ) -> ferrosa_common::Result<PartitionStream<'a>> {
        // ADR-020 Phase 2 backing: the FRAGMENTED range iterators back the
        // replica stream. They k-way merge across memtable + flushing
        // memtable + per-SSTable iterators AND, crucially, stream a single
        // wide partition's rows in bounded fragments (<= K rows each), so
        // resident memory is `O(num_sources + K)` rows regardless of
        // partition width — not `O(widest_partition_rows)`. A single
        // multi-million-row inverted-index partition (one hot term) is what
        // OOM-killed replicas on a full-table `SELECT *`; this is the fix.
        // The fragments carry the partition header only on the first
        // fragment, so the coordinator's per-partition row bridge reproduces
        // the merged partition byte-for-byte.
        if let Some(wanted) = projected_regular_ordinals {
            Ok(
                ferrosa_storage::StorageEngine::range_iter_projected_fragmented(
                    self.as_ref(),
                    table_id,
                    wanted.to_vec(),
                    start,
                    None,
                ),
            )
        } else {
            Ok(ferrosa_storage::StorageEngine::range_iter_fragmented(
                self.as_ref(),
                table_id,
                start,
                None,
            ))
        }
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
    reader: Arc<R>,
    sink: &S,
    chunk_size: usize,
) where
    R: StreamRangeReader + 'static,
    S: ChunkSink,
{
    handle_stream_request_with_cancel(req, reader, sink, chunk_size, CancellationToken::new())
        .await;
}

pub async fn handle_stream_request_with_cancel<R, S>(
    req: RangeReadStreamRequestPayload,
    reader: Arc<R>,
    sink: &S,
    chunk_size: usize,
    cancel: CancellationToken,
) where
    R: StreamRangeReader + 'static,
    S: ChunkSink,
{
    assert!(chunk_size >= 1, "chunk_size must be >= 1");

    let table_id = TableId::new(&req.keyspace, &req.table);

    // Inclusive lower-bound key for a resumed page (`None` on the first page).
    // Rebuild the DecoratedKey from the raw partition-key bytes so the remote
    // replica starts its fragmented iterator at the same token+key the
    // coordinator's resume cursor encodes.
    let start_key = req.start_key.as_ref().map(|bytes| {
        ferrosa_common::key::DecoratedKey::new(ferrosa_common::key::PartitionKey::from(
            bytes.as_slice(),
        ))
    });

    // Open the lazy partition stream. Errors here are open-time
    // (table not found, etc.) — emit a truncated Done and bail.
    let mut stream = match reader.range_iter(
        &table_id,
        req.projected_regular_ordinals.as_deref(),
        start_key.as_ref(),
    ) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                request_id = req.request_id,
                keyspace = req.keyspace,
                table = req.table,
                "stream request: open failed: {e}"
            );
            send_truncated_done(&req, sink).await;
            return;
        }
    };

    // Heartbeat ticker. Each tick that fires when the partition
    // stream is slow to yield emits a RangeReadStreamHeartbeat so
    // the coordinator's IdleTimeoutWatchdog sees activity. The
    // first tick is discarded — no point heartbeating before we've
    // even started waiting.
    let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;

    // Bound each emitted chunk by TOTAL ROWS, not just partition/fragment
    // count. The fragmented range iterator already splits a wide partition
    // into `<= K`-row items, but `chunk_size` of them would still be
    // `chunk_size * K` rows in one wire frame. Flushing on a row threshold
    // keeps each chunk frame inside the Bulk lane envelope regardless of how
    // the fragments fall. The threshold is the per-fragment cap K (mirrored
    // from storage's default) so a single full fragment flushes promptly.
    let row_chunk_cap: usize = stream_chunk_row_cap();
    let mut heartbeat_seq: u32 = 0;
    let mut chunk_seq: u32 = 0;
    let mut batch: Vec<Partition> = Vec::with_capacity(chunk_size);
    let mut batch_rows: usize = 0;
    let mut total_chunks_emitted: u32 = 0;
    let mut any_decode_error = false;

    'pull: loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(
                    request_id = req.request_id,
                    total_chunks_emitted,
                    "stream request: cancelled"
                );
                return;
            }
            next = stream.next() => match next {
                Some(Ok(partition)) => {
                    batch_rows = batch_rows.saturating_add(partition.rows.len());
                    batch.push(partition);
                    if batch.len() >= chunk_size || batch_rows >= row_chunk_cap {
                        emit_chunk(&req, &mut batch, chunk_seq, sink).await;
                        batch_rows = 0;
                        chunk_seq = chunk_seq.saturating_add(1);
                        total_chunks_emitted = total_chunks_emitted.saturating_add(1);
                    }
                }
                Some(Err(e)) => {
                    tracing::warn!(
                        request_id = req.request_id,
                        "stream request: per-partition decode error: {e}"
                    );
                    batch.clear();
                    total_chunks_emitted = 0;
                    any_decode_error = true;
                    break 'pull;
                }
                None => break 'pull, // stream exhausted
            },
            _ = ticker.tick() => {
                let hb = RangeReadStreamHeartbeatPayload {
                    request_id: req.request_id,
                    seq: heartbeat_seq,
                };
                heartbeat_seq = heartbeat_seq.saturating_add(1);
                let bytes = bincode::serialize(&hb)
                    .expect("RangeReadStreamHeartbeatPayload serialization is infallible");
                sink.send(Message::RangeReadStreamHeartbeat(Bytes::from(bytes))).await;
            }
        }
    }

    // Flush any final partial batch.
    if !any_decode_error && !batch.is_empty() {
        emit_chunk(&req, &mut batch, chunk_seq, sink).await;
        total_chunks_emitted = total_chunks_emitted.saturating_add(1);
    }

    // Terminator.
    let done = RangeReadStreamDonePayload {
        request_id: req.request_id,
        total_chunks: total_chunks_emitted,
        truncated: any_decode_error,
    };
    let bytes =
        bincode::serialize(&done).expect("RangeReadStreamDonePayload serialization is infallible");
    sink.send(Message::RangeReadStreamDone(Bytes::from(bytes)))
        .await;
}

/// Build a `RangeReadStreamChunk` from `batch`, send it via `sink`,
/// and clear `batch` so its backing allocation is reused for the
/// next chunk. The partitions are moved out of the batch — no
/// clone — so memory peaks at `chunk_size` partitions held by the
/// builder, not 2×.
async fn emit_chunk<S: ChunkSink>(
    req: &RangeReadStreamRequestPayload,
    batch: &mut Vec<Partition>,
    seq: u32,
    sink: &S,
) {
    // Drain into a wire-shaped Vec without cloning. After this
    // function returns, both the original Partition Vec and the
    // wire Vec are dropped.
    use crate::raft::handlers::partition_to_wire;
    let wire: Vec<_> = batch.drain(..).map(partition_to_wire).collect();
    let payload = crate::raft::handlers::RangeReadStreamChunkPayload {
        request_id: req.request_id,
        seq,
        partitions: wire,
    };
    let bytes = bincode::serialize(&payload)
        .expect("RangeReadStreamChunkPayload serialization is infallible");
    sink.send(Message::RangeReadStreamChunk(Bytes::from(bytes)))
        .await;
}

async fn send_truncated_done<S: ChunkSink>(req: &RangeReadStreamRequestPayload, sink: &S) {
    let done = RangeReadStreamDonePayload {
        request_id: req.request_id,
        total_chunks: 0,
        truncated: true,
    };
    let bytes =
        bincode::serialize(&done).expect("RangeReadStreamDonePayload serialization is infallible");
    sink.send(Message::RangeReadStreamDone(Bytes::from(bytes)))
        .await;
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
    cancellations: Arc<DashMap<u32, CancellationToken>>,
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
            cancellations: Arc::new(DashMap::new()),
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
            Message::RangeReadStreamCancel(b) => {
                let cancel: RangeReadStreamCancelPayload = match bincode::deserialize(&b) {
                    Ok(cancel) => cancel,
                    Err(e) => {
                        tracing::warn!("RangeReadStreamRequestHandler: cancel decode failed: {e}");
                        return None;
                    }
                };
                if let Some((_request_id, token)) = self.cancellations.remove(&cancel.request_id) {
                    token.cancel();
                }
                return None;
            }
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
        let token = CancellationToken::new();
        self.cancellations.insert(req.request_id, token.clone());
        let cancellations = self.cancellations.clone();

        TaskPool::current("range-stream-request").spawn(async move {
            let request_id = req.request_id;
            handle_stream_request_with_cancel(req, reader, &sink, chunk_size, token).await;
            cancellations.remove(&request_id);
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
            projected_regular_ordinals: None,
            start_key: None,
        }
    }

    /// Test reader that yields a fixed list of partitions lazily,
    /// one at a time through the new `range_iter` async stream.
    struct StaticReader {
        partitions: Vec<Partition>,
    }
    impl StreamRangeReader for StaticReader {
        fn range_iter<'a>(
            &'a self,
            _table_id: &TableId,
            _projected_regular_ordinals: Option<&'a [u16]>,
            _start: Option<&'a ferrosa_common::key::DecoratedKey>,
        ) -> ferrosa_common::Result<PartitionStream<'a>> {
            let items: Vec<ferrosa_common::Result<Partition>> =
                self.partitions.iter().cloned().map(Ok).collect();
            Ok(Box::pin(futures::stream::iter(items)))
        }
    }

    /// Test reader that records the `start` key passed to `range_iter`, so the
    /// handler's `start_key` → `DecoratedKey` plumbing can be asserted.
    struct StartCapturingReader {
        captured: Arc<Mutex<Option<Option<Vec<u8>>>>>,
    }
    impl StreamRangeReader for StartCapturingReader {
        fn range_iter<'a>(
            &'a self,
            _table_id: &TableId,
            _projected_regular_ordinals: Option<&'a [u16]>,
            start: Option<&'a ferrosa_common::key::DecoratedKey>,
        ) -> ferrosa_common::Result<PartitionStream<'a>> {
            *self.captured.lock().unwrap() = Some(start.map(|k| k.key.as_bytes().to_vec()));
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    /// The wire `start_key` must reach the storage reader as the inclusive
    /// `DecoratedKey` lower bound, so a resumed multi-replica page does not
    /// re-stream the already-emitted prefix at each replica.
    #[tokio::test]
    async fn handler_forwards_start_key_to_reader() {
        let captured = Arc::new(Mutex::new(None));
        let reader = Arc::new(StartCapturingReader {
            captured: captured.clone(),
        });
        let sink = VecSink::new();
        let mut request = req(99);
        request.start_key = Some(b"resume-key".to_vec());

        handle_stream_request(request, reader, &sink, 4).await;

        let seen = captured.lock().unwrap().clone();
        assert_eq!(
            seen,
            Some(Some(b"resume-key".to_vec())),
            "handler must forward the wire start_key to the reader as the range lower bound"
        );
    }

    /// Test reader that always fails at open time — exercises the
    /// open-error → truncated-Done path.
    struct FailingReader;
    impl StreamRangeReader for FailingReader {
        fn range_iter<'a>(
            &'a self,
            _table_id: &TableId,
            _projected_regular_ordinals: Option<&'a [u16]>,
            _start: Option<&'a ferrosa_common::key::DecoratedKey>,
        ) -> ferrosa_common::Result<PartitionStream<'a>> {
            Err(ferrosa_common::Error::InvalidData("simulated".into()))
        }
    }

    struct ErrorAfterReader;
    impl StreamRangeReader for ErrorAfterReader {
        fn range_iter<'a>(
            &'a self,
            _table_id: &TableId,
            _projected_regular_ordinals: Option<&'a [u16]>,
            _start: Option<&'a ferrosa_common::key::DecoratedKey>,
        ) -> ferrosa_common::Result<PartitionStream<'a>> {
            let items = vec![
                Ok(make_partition(1)),
                Ok(make_partition(2)),
                Err(ferrosa_common::Error::InvalidData(
                    "corrupt partition".into(),
                )),
                Ok(make_partition(3)),
            ];
            Ok(Box::pin(futures::stream::iter(items)))
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

        handle_stream_request(req(11), Arc::new(reader), &sink, 2).await;

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

        handle_stream_request(req(12), Arc::new(reader), &sink, 4).await;

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

        handle_stream_request(req(13), Arc::new(reader), &sink, 4).await;

        let frames = sink.take();
        assert_eq!(frames.len(), 1);
        let Message::RangeReadStreamDone(b) = &frames[0] else {
            panic!("must be Done");
        };
        let done: RangeReadStreamDonePayload = bincode::deserialize(b).unwrap();
        assert_eq!(done.total_chunks, 0);
        assert!(done.truncated, "truncated=true signals partial replica");
    }

    #[tokio::test]
    async fn partition_decode_error_emits_truncated_done_without_partial_chunks() {
        let reader = ErrorAfterReader;
        let sink = VecSink::new();

        handle_stream_request(req(14), Arc::new(reader), &sink, 4).await;

        let frames = sink.take();
        assert_eq!(
            frames.len(),
            1,
            "decode errors must not emit partial data from a corrupt stream"
        );
        let Message::RangeReadStreamDone(b) = &frames[0] else {
            panic!("only frame must be Done");
        };
        let done: RangeReadStreamDonePayload = bincode::deserialize(b).unwrap();
        assert_eq!(done.total_chunks, 0);
        assert!(
            done.truncated,
            "decode error must fail closed at coordinator"
        );
    }

    /// request_id propagates onto every emitted frame.
    #[tokio::test]
    async fn request_id_appears_on_every_frame() {
        let reader = StaticReader {
            partitions: vec![make_partition(1), make_partition(2), make_partition(3)],
        };
        let sink = VecSink::new();

        handle_stream_request(req(0xDEAD_BEEF), Arc::new(reader), &sink, 1).await;

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

    // -----------------------------------------------------------------------
    // ADR-020 lazy-iterator contract (memory boundedness)
    // -----------------------------------------------------------------------

    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Reader that increments `partitions_yielded` every time
    /// `next()` on its stream produces a partition. A correctly
    /// lazy handler pulls chunk_size partitions, emits a chunk,
    /// drops it, and only THEN pulls the next chunk_size — so the
    /// counter at the moment the first chunk frame lands at the
    /// sink is ≤ chunk_size. A handler that materializes the whole
    /// list before emitting would have already pulled EVERY
    /// partition past the counter before the first emit.
    struct LazyContractReader {
        partitions: Mutex<Option<Vec<Partition>>>,
        partitions_yielded: Arc<AtomicUsize>,
    }
    impl LazyContractReader {
        fn new(partitions: Vec<Partition>, counter: Arc<AtomicUsize>) -> Self {
            Self {
                partitions: Mutex::new(Some(partitions)),
                partitions_yielded: counter,
            }
        }
    }
    impl StreamRangeReader for LazyContractReader {
        fn range_iter<'a>(
            &'a self,
            _table_id: &TableId,
            _projected_regular_ordinals: Option<&'a [u16]>,
            _start: Option<&'a ferrosa_common::key::DecoratedKey>,
        ) -> ferrosa_common::Result<PartitionStream<'a>> {
            let ps = self.partitions.lock().unwrap().take().unwrap_or_default();
            let counter = self.partitions_yielded.clone();
            // Yield Ok(partition) one at a time, bumping the
            // counter on each successful pull. The handler's
            // emission rate vs the counter rate is what proves
            // bounded memory.
            let stream = futures::stream::iter(ps).map(move |p| {
                counter.fetch_add(1, AtomicOrdering::Relaxed);
                Ok::<_, ferrosa_common::Error>(p)
            });
            Ok(Box::pin(stream))
        }
    }

    /// Sink that snapshots the yield counter the FIRST time a
    /// Chunk frame is emitted. Lazy handler: counter == chunk_size.
    /// Materializing handler: counter == total partition count.
    struct YieldWatermarkSink {
        watermark_at_first_chunk: Mutex<Option<usize>>,
        yield_counter: Arc<AtomicUsize>,
        frames: Mutex<Vec<Message>>,
    }
    impl YieldWatermarkSink {
        fn new(counter: Arc<AtomicUsize>) -> Self {
            Self {
                watermark_at_first_chunk: Mutex::new(None),
                yield_counter: counter,
                frames: Mutex::new(Vec::new()),
            }
        }
    }
    #[async_trait]
    impl ChunkSink for YieldWatermarkSink {
        async fn send(&self, msg: Message) {
            if matches!(msg, Message::RangeReadStreamChunk(_)) {
                let mut wm = self.watermark_at_first_chunk.lock().unwrap();
                if wm.is_none() {
                    *wm = Some(self.yield_counter.load(AtomicOrdering::Relaxed));
                }
            }
            self.frames.lock().unwrap().push(msg);
        }
    }

    /// ADR-020 memory boundedness — the handler MUST pull from a
    /// lazy iterator, never materialize the whole partition list
    /// before emitting. With chunk_size = 16 and 1000 partitions,
    /// the yield-counter at the moment the first chunk is emitted
    /// must be ≤ chunk_size (plus iterator-internal slack). The
    /// current trait's Vec-returning shape makes this impossible —
    /// this test will RED until `read_range` is replaced with a
    /// per-partition lazy iterator and the handler is rewired to
    /// pull from it.
    #[tokio::test]
    async fn handler_holds_at_most_chunk_size_partitions_before_first_emit() {
        const TOTAL: usize = 1000;
        const CHUNK: usize = 16;

        let partitions: Vec<Partition> = (0u8..=255)
            .cycle()
            .take(TOTAL)
            .map(make_partition)
            .collect();
        let counter = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(LazyContractReader::new(partitions, counter.clone()));
        let sink = YieldWatermarkSink::new(counter.clone());

        handle_stream_request(req(1), reader, &sink, CHUNK).await;

        let watermark = sink
            .watermark_at_first_chunk
            .lock()
            .unwrap()
            .expect("at least one chunk frame must have been emitted");
        assert!(
            watermark <= CHUNK,
            "handler materialized {watermark} partitions before emitting the first chunk; \
             ADR-020 requires lazy iteration (chunk_size={CHUNK})"
        );
    }

    struct CancelAfterFirstChunkSink {
        cancel: tokio_util::sync::CancellationToken,
        frames: Mutex<Vec<Message>>,
    }

    #[async_trait]
    impl ChunkSink for CancelAfterFirstChunkSink {
        async fn send(&self, msg: Message) {
            if matches!(msg, Message::RangeReadStreamChunk(_)) {
                self.cancel.cancel();
            }
            self.frames.lock().unwrap().push(msg);
        }
    }

    #[tokio::test]
    async fn stream_request_cancel_stops_reader_within_one_batch() {
        const CHUNK: usize = 4;
        let yielded = Arc::new(AtomicUsize::new(0));
        let partitions: Vec<Partition> =
            (0u8..=255).cycle().take(100).map(make_partition).collect();
        let reader = Arc::new(LazyContractReader::new(partitions, yielded.clone()));
        let cancel = tokio_util::sync::CancellationToken::new();
        let sink = CancelAfterFirstChunkSink {
            cancel: cancel.clone(),
            frames: Mutex::new(Vec::new()),
        };

        handle_stream_request_with_cancel(req(2), reader, &sink, CHUNK, cancel).await;

        let frames = sink.frames.lock().unwrap();
        let chunks = frames
            .iter()
            .filter(|msg| matches!(msg, Message::RangeReadStreamChunk(_)))
            .count();
        assert_eq!(chunks, 1, "producer must stop after observing cancel");
        assert!(
            !frames
                .iter()
                .any(|msg| matches!(msg, Message::RangeReadStreamDone(_))),
            "cancelled streams must stop without sending a successful Done terminator"
        );
        assert!(
            yielded.load(AtomicOrdering::Relaxed) <= CHUNK * 2,
            "producer pulled too far after cancel; yielded={}",
            yielded.load(AtomicOrdering::Relaxed)
        );
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
