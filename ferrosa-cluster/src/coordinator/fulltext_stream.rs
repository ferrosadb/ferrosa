//! Producer for streaming fulltext-search responses (t_4ae47a9f) — the
//! `fts_match` twin of the ADR-020 streaming range read.
//!
//! An inbound `FulltextSearchStreamRequest` spawns the node-local FTI walk
//! (`StorageEngine::fulltext_search_each`) on a blocking thread. The walk
//! pushes each matching doc key into a **bounded** channel; the async side
//! drains that channel into `FulltextSearchStreamChunk` frames of at most
//! `chunk_keys` keys, emits `FulltextSearchStreamHeartbeat` while the walk is
//! slow, and terminates with a single `FulltextSearchStreamDone`.
//!
//! Memory contract: the producer never materializes the match set. Resident
//! is O(channel capacity + one chunk) regardless of how many docs match —
//! the shape that OOM-killed replicas at the 2 GiB cap (t_8fc24ce2) held the
//! ENTIRE match set in a score map plus a single up-to-256 MiB response
//! buffer. Consumer-paced: if the coordinator cancels (or the walk's channel
//! closes because this task dropped the receiver), the walk observes
//! `ControlFlow::Break` on its next key and stops immediately.
//!
//! Fail loud: a walk error (malformed query, storage failure) discards any
//! undelivered partial batch and terminates with `Done { truncated: true }`
//! so the coordinator fails the whole search — a silent partial match set is
//! never served (`skills/rules/safety.md`).
//!
//! Transport-free like `stream_producer`: frames leave through the shared
//! [`ChunkSink`] trait (production: `PeerManager::fire` on `Lane::Bulk`;
//! tests: an in-memory Vec).
//!
//! Last revised: 2026-07-16
//! Last changed: Initial streaming fulltext producer (layer 4a of
//! t_4ae47a9f).

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_net::task_pool::TaskPool;
use ferrosa_storage::TableId;

use super::stream_producer::ChunkSink;
use super::stream_request_handler::SinkFactory;
use crate::raft::handlers::{
    FulltextSearchStreamCancelPayload, FulltextSearchStreamChunkPayload,
    FulltextSearchStreamDonePayload, FulltextSearchStreamHeartbeatPayload,
    FulltextSearchStreamRequestPayload,
};

/// Keys per chunk frame. 4096 keys × ~40 B/key ≈ 160 KiB per frame — three
/// orders of magnitude under the Bulk-lane 256 MiB envelope, so even the
/// lane's message-count-only backpressure (256 queued frames) pins at most
/// ~40 MiB for a pathological peer, not gigabytes.
pub const FULLTEXT_STREAM_CHUNK_KEYS: usize = 4_096;

/// Matches the range-read producer's heartbeat cadence: below the
/// coordinator's idle timeout with margin.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

/// Node-local key source the producer walks. Production is
/// `Arc<StorageEngine>` via [`ferrosa_storage::StorageEngine::fulltext_search_each`];
/// tests drive an in-memory source. The implementation MUST honor
/// `ControlFlow::Break` from the callback by stopping the walk.
pub trait FulltextKeySource: Send + Sync {
    fn search_each(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &str,
        on_hit: &mut dyn FnMut(Vec<u8>) -> ControlFlow<()>,
    ) -> ferrosa_common::Result<()>;
}

impl FulltextKeySource for Arc<ferrosa_storage::StorageEngine> {
    fn search_each(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &str,
        on_hit: &mut dyn FnMut(Vec<u8>) -> ControlFlow<()>,
    ) -> ferrosa_common::Result<()> {
        ferrosa_storage::StorageEngine::fulltext_search_each(
            self, table_id, index_name, query, on_hit,
        )
    }
}

/// Serve one streaming fulltext request: walk the local FTI on a blocking
/// thread, emit bounded key chunks + heartbeats, terminate with Done.
pub async fn handle_fulltext_stream_request_with_cancel<S, K>(
    req: FulltextSearchStreamRequestPayload,
    source: Arc<S>,
    sink: &K,
    chunk_keys: usize,
    cancel: CancellationToken,
) where
    S: FulltextKeySource + 'static,
    K: ChunkSink,
{
    assert!(chunk_keys >= 1, "chunk_keys must be >= 1");

    let table_id = TableId::new(&req.keyspace, &req.table);

    // Bounded hand-off between the blocking walk and the async framer. The
    // capacity is the producer's ONLY buffering: when the async side stalls
    // (slow peer, cancel racing in), the walk blocks on `blocking_send` —
    // consumer-paced backpressure with an O(capacity) ceiling.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(chunk_keys.saturating_mul(2));

    let walk_source = source.clone();
    let walk_index = req.index_name.clone();
    let walk_query = req.query.clone();
    let walk_table = table_id.clone();
    let walk = tokio::task::spawn_blocking(move || {
        walk_source.search_each(&walk_table, &walk_index, &walk_query, &mut |key| {
            match tx.blocking_send(key) {
                Ok(()) => ControlFlow::Continue(()),
                // Receiver dropped: the framer stopped (cancel / task end).
                Err(_) => ControlFlow::Break(()),
            }
        })
    });

    let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // discard the immediate first tick

    let mut heartbeat_seq: u32 = 0;
    let mut chunk_seq: u32 = 0;
    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(chunk_keys);
    let mut cancelled = false;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                cancelled = true;
                break;
            }
            next = rx.recv() => match next {
                Some(key) => {
                    batch.push(key);
                    if batch.len() >= chunk_keys {
                        emit_chunk(&req, &mut batch, chunk_seq, sink).await;
                        chunk_seq = chunk_seq.saturating_add(1);
                    }
                }
                None => break, // walk finished (or errored) and dropped tx
            },
            _ = ticker.tick() => {
                let hb = FulltextSearchStreamHeartbeatPayload {
                    request_id: req.request_id,
                    seq: chunk_seq,
                };
                heartbeat_seq = heartbeat_seq.saturating_add(1);
                let _ = heartbeat_seq; // debugging aid only
                let bytes = bincode::serialize(&hb)
                    .expect("FulltextSearchStreamHeartbeatPayload serialization is infallible");
                sink.send(Message::FulltextSearchStreamHeartbeat(Bytes::from(bytes))).await;
            }
        }
    }

    if cancelled {
        // Dropping rx makes the walk's next blocking_send fail → Break.
        // No Done frame: the coordinator unregistered the route before
        // firing the cancel (same contract as the range-read stream).
        drop(rx);
        let _ = walk.await;
        return;
    }

    // The walk ended on its own — surface its result.
    let walk_result = match walk.await {
        Ok(r) => r,
        Err(join_err) => Err(ferrosa_common::Error::InvalidData(format!(
            "fulltext stream walk join: {join_err}"
        ))),
    };

    let truncated = match walk_result {
        Ok(()) => false,
        Err(e) => {
            tracing::warn!(
                request_id = req.request_id,
                keyspace = req.keyspace,
                table = req.table,
                "fulltext stream walk failed: {e}"
            );
            // Fail loud: never deliver a partial batch of a failed walk.
            batch.clear();
            true
        }
    };

    if !truncated && !batch.is_empty() {
        emit_chunk(&req, &mut batch, chunk_seq, sink).await;
        chunk_seq = chunk_seq.saturating_add(1);
    }

    let done = FulltextSearchStreamDonePayload {
        request_id: req.request_id,
        total_chunks: if truncated { 0 } else { chunk_seq },
        truncated,
    };
    let bytes = bincode::serialize(&done)
        .expect("FulltextSearchStreamDonePayload serialization is infallible");
    sink.send(Message::FulltextSearchStreamDone(Bytes::from(bytes)))
        .await;
}

async fn emit_chunk<K: ChunkSink>(
    req: &FulltextSearchStreamRequestPayload,
    batch: &mut Vec<Vec<u8>>,
    seq: u32,
    sink: &K,
) {
    let payload = FulltextSearchStreamChunkPayload {
        request_id: req.request_id,
        seq,
        keys: std::mem::take(batch),
    };
    let bytes = bincode::serialize(&payload)
        .expect("FulltextSearchStreamChunkPayload serialization is infallible");
    sink.send(Message::FulltextSearchStreamChunk(Bytes::from(bytes)))
        .await;
}

/// `RpcHandler` shell: decodes `FulltextSearchStreamRequest`, spawns the
/// streaming task, returns `None` (responses ride on fire-and-forget
/// frames). Also owns the per-request `CancellationToken` map and serves
/// `FulltextSearchStreamCancel`, mirroring `RangeReadStreamRequestHandler`.
pub struct FulltextSearchStreamRequestHandler<S, F>
where
    S: FulltextKeySource + 'static,
    F: SinkFactory + 'static,
{
    source: Arc<S>,
    sink_factory: Arc<F>,
    chunk_keys: usize,
    cancellations: Arc<DashMap<u32, CancellationToken>>,
}

impl<S, F> FulltextSearchStreamRequestHandler<S, F>
where
    S: FulltextKeySource + 'static,
    F: SinkFactory + 'static,
{
    pub fn new(source: Arc<S>, sink_factory: Arc<F>, chunk_keys: usize) -> Self {
        Self {
            source,
            sink_factory,
            chunk_keys,
            cancellations: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl<S, F> RpcHandler for FulltextSearchStreamRequestHandler<S, F>
where
    S: FulltextKeySource + 'static,
    F: SinkFactory + 'static,
{
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let bytes = match msg {
            Message::FulltextSearchStreamRequest(b) => b,
            Message::FulltextSearchStreamCancel(b) => {
                let cancel: FulltextSearchStreamCancelPayload = match bincode::deserialize(&b) {
                    Ok(cancel) => cancel,
                    Err(e) => {
                        tracing::warn!(
                            "FulltextSearchStreamRequestHandler: cancel decode failed: {e}"
                        );
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

        let req: FulltextSearchStreamRequestPayload = match bincode::deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("FulltextSearchStreamRequestHandler: decode failed: {e}");
                return None;
            }
        };

        let sink = self.sink_factory.for_peer(from, req.request_id);
        let source = self.source.clone();
        let chunk_keys = self.chunk_keys;
        let token = CancellationToken::new();
        self.cancellations.insert(req.request_id, token.clone());
        let cancellations = self.cancellations.clone();

        TaskPool::current("fulltext-stream-request").spawn(async move {
            let request_id = req.request_id;
            handle_fulltext_stream_request_with_cancel(req, source, &sink, chunk_keys, token).await;
            cancellations.remove(&request_id);
        });

        None
    }
}

// ---------------------------------------------------------------------------
// Consumer + coordinator fan-out (layer 4b)
// ---------------------------------------------------------------------------

/// Route buffer for one replica's fulltext stream. Fulltext chunks are small
/// (≤ [`FULLTEXT_STREAM_CHUNK_KEYS`] keys ≈ 160 KiB), and the per-replica
/// consumer forwards each chunk promptly into the bounded merge channel, so a
/// deeper buffer than the range-read stream's is safe and absorbs producer
/// bursts without tripping the fail-loud route-full close.
pub const FULLTEXT_STREAM_ROUTE_BUFFER: usize = 256;

/// Producer inactivity bound — same rationale as the range-read stream's.
const FULLTEXT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Terminal outcome of consuming one replica's fulltext key stream.
#[derive(Debug, PartialEq, Eq)]
pub enum FulltextConsumeOutcome {
    /// The replica terminated cleanly with `Done { truncated: false }`.
    Done,
    /// The downstream merge stopped accepting keys (query satisfied or
    /// abandoned). The caller must cancel the remote producer.
    EarlyStop,
}

/// All the ways consuming one replica's fulltext stream can fail. Any of
/// these fails the WHOLE search — a missing replica's keys would silently
/// drop matching rows from the result (fail loud, never fake).
#[derive(Debug)]
pub enum FulltextConsumeError {
    IdleTimeout { request_id: u32 },
    Decode { request_id: u32, message: String },
    UnexpectedFrame { request_id: u32 },
    ChannelClosedBeforeDone { request_id: u32 },
    TruncatedReplica { request_id: u32 },
}

impl std::fmt::Display for FulltextConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdleTimeout { request_id } => {
                write!(f, "fulltext stream {request_id}: producer idle timeout")
            }
            Self::Decode {
                request_id,
                message,
            } => write!(f, "fulltext stream {request_id}: frame decode: {message}"),
            Self::UnexpectedFrame { request_id } => {
                write!(f, "fulltext stream {request_id}: unexpected frame type")
            }
            Self::ChannelClosedBeforeDone { request_id } => write!(
                f,
                "fulltext stream {request_id}: route closed before Done (gap/overflow/producer death)"
            ),
            Self::TruncatedReplica { request_id } => write!(
                f,
                "fulltext stream {request_id}: replica reported truncated walk"
            ),
        }
    }
}

/// Consume one replica's fulltext stream: decode chunk frames, forward each
/// chunk's keys into `forward` (bounded — the back-pressure point), treat
/// heartbeats as activity, finish on Done.
pub async fn consume_fulltext_key_stream(
    receiver: tokio::sync::mpsc::Receiver<Message>,
    request_id: u32,
    forward: &tokio::sync::mpsc::Sender<Vec<Vec<u8>>>,
) -> Result<FulltextConsumeOutcome, FulltextConsumeError> {
    let mut watchdog =
        ferrosa_net::idle_timeout::IdleTimeoutWatchdog::new(receiver, FULLTEXT_STREAM_IDLE_TIMEOUT);
    loop {
        match watchdog.next().await {
            Ok(Some(Message::FulltextSearchStreamChunk(bytes))) => {
                let chunk: FulltextSearchStreamChunkPayload = bincode::deserialize(&bytes)
                    .map_err(|e| FulltextConsumeError::Decode {
                        request_id,
                        message: format!("chunk: {e}"),
                    })?;
                if forward.send(chunk.keys).await.is_err() {
                    return Ok(FulltextConsumeOutcome::EarlyStop);
                }
            }
            Ok(Some(Message::FulltextSearchStreamHeartbeat(_))) => continue,
            Ok(Some(Message::FulltextSearchStreamDone(bytes))) => {
                let done: FulltextSearchStreamDonePayload =
                    bincode::deserialize(&bytes).map_err(|e| FulltextConsumeError::Decode {
                        request_id,
                        message: format!("done: {e}"),
                    })?;
                if done.truncated {
                    return Err(FulltextConsumeError::TruncatedReplica { request_id });
                }
                return Ok(FulltextConsumeOutcome::Done);
            }
            Ok(Some(_)) => return Err(FulltextConsumeError::UnexpectedFrame { request_id }),
            Ok(None) => return Err(FulltextConsumeError::ChannelClosedBeforeDone { request_id }),
            Err(_elapsed) => return Err(FulltextConsumeError::IdleTimeout { request_id }),
        }
    }
}

/// Fire a best-effort `FulltextSearchStreamCancel` so an abandoned remote
/// walk stops instead of streaming keys to nobody (the fulltext analogue of
/// the t_3fc6be3c range-read leak).
async fn fire_fulltext_stream_cancel(
    peers: &ferrosa_net::peer::PeerManager,
    host_id: uuid::Uuid,
    request_id: u32,
) {
    let payload = FulltextSearchStreamCancelPayload { request_id };
    let body = bincode::serialize(&payload)
        .expect("FulltextSearchStreamCancelPayload serialization is infallible");
    match peers
        .fire(
            host_id,
            Message::FulltextSearchStreamCancel(Bytes::from(body)),
            ferrosa_net::codec::Lane::Bulk,
        )
        .await
    {
        Ok(()) => tracing::info!(
            request_id,
            peer = %host_id,
            "streaming fulltext: fired cancel to stop the remote walk"
        ),
        Err(e) => tracing::warn!(
            request_id,
            peer = %host_id,
            "streaming fulltext: cancel fire failed — remote walk runs to Done or disconnect: {e}"
        ),
    }
}

/// Internal merge event from one replica's feeder task.
enum ReplicaEvent {
    Keys(Vec<Vec<u8>>),
    ReplicaDone,
    ReplicaFailed(String),
}

impl crate::ClusterCoordinator {
    /// Streaming cluster-wide fulltext search (t_4ae47a9f): fans a
    /// `FulltextSearchStreamRequest` out to every node (walking the local FTI
    /// in-process), N-way merges the returning key chunks over bounded
    /// channels, dedups against a single `seen` set, and yields deduped key
    /// batches through the returned bounded receiver.
    ///
    /// Memory: O(route buffers + merge channel + `seen`). The `seen` dedup
    /// set is the one intentionally O(distinct matches) allocation in the
    /// whole path — keys only, no scores, one copy, needed for corretness of
    /// the cross-replica union. Every other hop is bounded.
    ///
    /// Fail loud: any replica failure (fire error, truncated walk, idle
    /// timeout, route close) yields `Err` through the stream — never a
    /// silent partial union (unlike the legacy degrading path). Dropping the
    /// receiver early cancels every in-flight remote walk.
    pub async fn coordinate_fulltext_search_stream(
        &self,
        table_id: &TableId,
        index_name: &str,
        query: &str,
    ) -> crate::error::Result<tokio::sync::mpsc::Receiver<crate::error::Result<Vec<Vec<u8>>>>> {
        let ring = self.ring.load();
        let node_ids = ring.node_ids();
        let mut remotes: Vec<(uuid::Uuid, String)> = Vec::new();
        for &id in node_ids.iter() {
            if id == self.local_node_id {
                continue;
            }
            match ring.get_node(id) {
                Some(n) => remotes.push((n.host_id, n.addr.clone())),
                None => {
                    return Err(crate::error::ClusterError::Internal(format!(
                        "streaming fulltext: ring node {id} has no address entry"
                    )))
                }
            }
        }
        drop(ring);

        let replica_count = remotes.len() + 1; // + local walk
        let (merge_tx, mut merge_rx) = tokio::sync::mpsc::channel::<ReplicaEvent>(16);
        let (out_tx, out_rx) = tokio::sync::mpsc::channel::<crate::error::Result<Vec<Vec<u8>>>>(8);

        // --- local walk feeder -------------------------------------------------
        {
            let storage = self.storage.clone();
            let table_id = table_id.clone();
            let index_name = index_name.to_string();
            let query = query.to_string();
            let merge_tx = merge_tx.clone();
            TaskPool::current("fulltext-stream-local").spawn(async move {
                let feeder_tx = merge_tx.clone();
                let walk = tokio::task::spawn_blocking(move || {
                    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(FULLTEXT_STREAM_CHUNK_KEYS);
                    let result =
                        storage.fulltext_search_each(&table_id, &index_name, &query, &mut |key| {
                            batch.push(key);
                            if batch.len() >= FULLTEXT_STREAM_CHUNK_KEYS {
                                let full = std::mem::take(&mut batch);
                                if feeder_tx.blocking_send(ReplicaEvent::Keys(full)).is_err() {
                                    return ControlFlow::Break(());
                                }
                            }
                            ControlFlow::Continue(())
                        });
                    match result {
                        Ok(()) => {
                            if !batch.is_empty() {
                                let _ = feeder_tx.blocking_send(ReplicaEvent::Keys(batch));
                            }
                            let _ = feeder_tx.blocking_send(ReplicaEvent::ReplicaDone);
                        }
                        Err(e) => {
                            let _ = feeder_tx
                                .blocking_send(ReplicaEvent::ReplicaFailed(format!("local: {e}")));
                        }
                    }
                });
                if let Err(join_err) = walk.await {
                    let _ = merge_tx
                        .send(ReplicaEvent::ReplicaFailed(format!(
                            "local walk join: {join_err}"
                        )))
                        .await;
                }
            });
        }

        // --- remote feeders ----------------------------------------------------
        for (host_id, _addr) in remotes {
            let request_id = self.next_stream_request_id();
            let receiver = self
                .stream_router
                .register(request_id, FULLTEXT_STREAM_ROUTE_BUFFER);
            let req = FulltextSearchStreamRequestPayload {
                request_id,
                keyspace: table_id.keyspace.clone(),
                table: table_id.table.clone(),
                index_name: index_name.to_string(),
                query: query.to_string(),
            };
            let body = match bincode::serialize(&req) {
                Ok(b) => Bytes::from(b),
                Err(e) => {
                    self.stream_router.unregister(request_id);
                    return Err(crate::error::ClusterError::Internal(format!(
                        "streaming fulltext: request encode: {e}"
                    )));
                }
            };
            if let Err(e) = self
                .peer_manager
                .fire(
                    host_id,
                    Message::FulltextSearchStreamRequest(body),
                    ferrosa_net::codec::Lane::Bulk,
                )
                .await
            {
                self.stream_router.unregister(request_id);
                return Err(crate::error::ClusterError::Internal(format!(
                    "streaming fulltext: failed to fire request to {host_id}: {e} \
                     (failing the search — a missing replica would silently drop matches)"
                )));
            }

            let merge_tx = merge_tx.clone();
            let stream_router = self.stream_router.clone();
            let peers = self.peer_manager.clone();
            TaskPool::current("fulltext-stream-consume").spawn(async move {
                let (keys_tx, mut keys_rx) = tokio::sync::mpsc::channel::<Vec<Vec<u8>>>(4);
                let consume = consume_fulltext_key_stream(receiver, request_id, &keys_tx);
                tokio::pin!(consume);
                let outcome = loop {
                    tokio::select! {
                        out = &mut consume => break out,
                        batch = keys_rx.recv() => {
                            if let Some(batch) = batch {
                                if merge_tx.send(ReplicaEvent::Keys(batch)).await.is_err() {
                                    // Merge gone: stop consuming; the dropped
                                    // keys_rx makes consume return EarlyStop.
                                    keys_rx.close();
                                }
                            }
                        }
                    }
                };
                // Drain any batches the consumer buffered before finishing.
                while let Ok(batch) = keys_rx.try_recv() {
                    let _ = merge_tx.send(ReplicaEvent::Keys(batch)).await;
                }
                stream_router.unregister(request_id);
                match outcome {
                    Ok(FulltextConsumeOutcome::Done) => {
                        let _ = merge_tx.send(ReplicaEvent::ReplicaDone).await;
                    }
                    Ok(FulltextConsumeOutcome::EarlyStop) => {
                        fire_fulltext_stream_cancel(&peers, host_id, request_id).await;
                    }
                    Err(e) => {
                        fire_fulltext_stream_cancel(&peers, host_id, request_id).await;
                        let _ = merge_tx
                            .send(ReplicaEvent::ReplicaFailed(e.to_string()))
                            .await;
                    }
                }
            });
        }
        drop(merge_tx);

        // --- merge + dedup -----------------------------------------------------
        TaskPool::current("fulltext-stream-merge").spawn(async move {
            let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            let mut pending = replica_count;
            while let Some(event) = merge_rx.recv().await {
                match event {
                    ReplicaEvent::Keys(batch) => {
                        let unique: Vec<Vec<u8>> = batch
                            .into_iter()
                            .filter(|k| seen.insert(k.clone()))
                            .collect();
                        if !unique.is_empty() && out_tx.send(Ok(unique)).await.is_err() {
                            // Consumer gone (query satisfied/abandoned):
                            // exiting drops merge_rx; feeders observe closed
                            // sends and cancel their remotes.
                            return;
                        }
                    }
                    ReplicaEvent::ReplicaDone => {
                        pending -= 1;
                        if pending == 0 {
                            return; // clean end: out_tx drops, stream closes
                        }
                    }
                    ReplicaEvent::ReplicaFailed(reason) => {
                        let _ = out_tx
                            .send(Err(crate::error::ClusterError::Internal(format!(
                                "streaming fulltext failed (no partial union served): {reason}"
                            ))))
                            .await;
                        return;
                    }
                }
            }
            // merge_rx closed with replicas still pending: feeders died
            // without a terminal event — fail loud.
            if pending > 0 {
                let _ = out_tx
                    .send(Err(crate::error::ClusterError::Internal(
                        "streaming fulltext: feeder(s) ended without terminal signal".into(),
                    )))
                    .await;
            }
        });

        Ok(out_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Test sink: collect every emitted Message.
    struct VecSink {
        sent: Mutex<Vec<Message>>,
    }
    impl VecSink {
        fn new() -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
            }
        }
        fn frames(&self) -> Vec<Message> {
            self.sent.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl ChunkSink for VecSink {
        async fn send(&self, msg: Message) {
            self.sent.lock().unwrap().push(msg);
        }
    }

    /// In-memory key source yielding `n` deterministic keys, honoring Break.
    struct FakeSource {
        n: usize,
        fail: bool,
    }
    impl FulltextKeySource for FakeSource {
        fn search_each(
            &self,
            _table_id: &TableId,
            _index_name: &str,
            _query: &str,
            on_hit: &mut dyn FnMut(Vec<u8>) -> ControlFlow<()>,
        ) -> ferrosa_common::Result<()> {
            if self.fail {
                return Err(ferrosa_common::Error::InvalidFormat(
                    "fts_match query error: synthetic".into(),
                ));
            }
            for i in 0..self.n {
                if on_hit(format!("key-{i:08}").into_bytes()).is_break() {
                    return Ok(());
                }
            }
            Ok(())
        }
    }

    fn req(id: u32) -> FulltextSearchStreamRequestPayload {
        FulltextSearchStreamRequestPayload {
            request_id: id,
            keyspace: "ks".into(),
            table: "tbl".into(),
            index_name: "idx".into(),
            query: "memory".into(),
        }
    }

    fn decode_chunk(msg: &Message) -> FulltextSearchStreamChunkPayload {
        match msg {
            Message::FulltextSearchStreamChunk(b) => bincode::deserialize(b).expect("chunk"),
            other => panic!("expected FulltextSearchStreamChunk, got {other:?}"),
        }
    }
    fn decode_done(msg: &Message) -> FulltextSearchStreamDonePayload {
        match msg {
            Message::FulltextSearchStreamDone(b) => bincode::deserialize(b).expect("done"),
            other => panic!("expected FulltextSearchStreamDone, got {other:?}"),
        }
    }

    /// 10 keys at chunk size 4 → chunks of 4+4+2 with monotonic seq, then a
    /// clean Done reporting total_chunks=3. Every key arrives exactly once,
    /// in walk order.
    #[tokio::test]
    async fn chunks_keys_then_emits_done() {
        let sink = VecSink::new();
        handle_fulltext_stream_request_with_cancel(
            req(7),
            Arc::new(FakeSource { n: 10, fail: false }),
            &sink,
            4,
            CancellationToken::new(),
        )
        .await;

        let frames = sink.frames();
        assert_eq!(frames.len(), 4, "3 chunks + 1 done: {frames:?}");
        let sizes: Vec<usize> = frames[..3]
            .iter()
            .map(|f| decode_chunk(f).keys.len())
            .collect();
        assert_eq!(sizes, vec![4, 4, 2]);
        for (i, f) in frames[..3].iter().enumerate() {
            let c = decode_chunk(f);
            assert_eq!(c.request_id, 7);
            assert_eq!(c.seq, i as u32);
        }
        let all: Vec<Vec<u8>> = frames[..3]
            .iter()
            .flat_map(|f| decode_chunk(f).keys)
            .collect();
        let expected: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("key-{i:08}").into_bytes())
            .collect();
        assert_eq!(all, expected, "every key exactly once, in walk order");

        let done = decode_done(&frames[3]);
        assert_eq!(done.request_id, 7);
        assert_eq!(done.total_chunks, 3);
        assert!(!done.truncated);
    }

    /// Zero matches still terminates with a clean Done (total_chunks=0) —
    /// the coordinator must observe a terminator for every replica.
    #[tokio::test]
    async fn empty_walk_emits_only_done() {
        let sink = VecSink::new();
        handle_fulltext_stream_request_with_cancel(
            req(1),
            Arc::new(FakeSource { n: 0, fail: false }),
            &sink,
            8,
            CancellationToken::new(),
        )
        .await;
        let frames = sink.frames();
        assert_eq!(frames.len(), 1);
        let done = decode_done(&frames[0]);
        assert_eq!(done.total_chunks, 0);
        assert!(!done.truncated);
    }

    /// A failed walk (malformed query / storage error) MUST NOT deliver any
    /// partial batch: it terminates with Done{truncated:true} so the
    /// coordinator fails the search instead of serving a silent subset.
    #[tokio::test]
    async fn walk_error_terminates_truncated_without_partial_keys() {
        let sink = VecSink::new();
        handle_fulltext_stream_request_with_cancel(
            req(2),
            Arc::new(FakeSource { n: 0, fail: true }),
            &sink,
            8,
            CancellationToken::new(),
        )
        .await;
        let frames = sink.frames();
        assert_eq!(frames.len(), 1, "no chunks, only the truncated Done");
        let done = decode_done(&frames[0]);
        assert!(done.truncated);
        assert_eq!(done.total_chunks, 0);
    }

    /// Cancellation stops the stream without a Done frame (the coordinator
    /// unregisters the route before cancelling) and unblocks the walk.
    #[tokio::test]
    async fn cancel_stops_stream_without_done() {
        let sink = VecSink::new();
        let token = CancellationToken::new();
        token.cancel(); // cancelled before the first frame
        handle_fulltext_stream_request_with_cancel(
            req(3),
            Arc::new(FakeSource {
                n: 1_000_000,
                fail: false,
            }),
            &sink,
            64,
            token,
        )
        .await;
        let frames = sink.frames();
        assert!(
            frames
                .iter()
                .all(|f| !matches!(f, Message::FulltextSearchStreamDone(_))),
            "no Done after cancel: {frames:?}"
        );
    }

    // --- consumer-side contract ---------------------------------------------

    fn chunk_frame(request_id: u32, seq: u32, keys: Vec<Vec<u8>>) -> Message {
        let payload = FulltextSearchStreamChunkPayload {
            request_id,
            seq,
            keys,
        };
        Message::FulltextSearchStreamChunk(Bytes::from(bincode::serialize(&payload).unwrap()))
    }
    fn done_frame(request_id: u32, total_chunks: u32, truncated: bool) -> Message {
        let payload = FulltextSearchStreamDonePayload {
            request_id,
            total_chunks,
            truncated,
        };
        Message::FulltextSearchStreamDone(Bytes::from(bincode::serialize(&payload).unwrap()))
    }

    /// Chunks arrive → batches forward in order → clean Done outcome.
    #[tokio::test]
    async fn consume_forwards_batches_then_done() {
        let (route_tx, route_rx) = tokio::sync::mpsc::channel(8);
        let (fwd_tx, mut fwd_rx) = tokio::sync::mpsc::channel(8);
        route_tx
            .send(chunk_frame(9, 0, vec![b"a".to_vec(), b"b".to_vec()]))
            .await
            .unwrap();
        route_tx
            .send(chunk_frame(9, 1, vec![b"c".to_vec()]))
            .await
            .unwrap();
        route_tx.send(done_frame(9, 2, false)).await.unwrap();
        drop(route_tx);

        let outcome = consume_fulltext_key_stream(route_rx, 9, &fwd_tx)
            .await
            .unwrap();
        assert_eq!(outcome, FulltextConsumeOutcome::Done);
        drop(fwd_tx);
        assert_eq!(
            fwd_rx.recv().await.unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(fwd_rx.recv().await.unwrap(), vec![b"c".to_vec()]);
        assert!(fwd_rx.recv().await.is_none());
    }

    /// `Done { truncated: true }` fails the replica — a truncated walk must
    /// fail the search, never contribute a silent subset.
    #[tokio::test]
    async fn consume_truncated_done_is_an_error() {
        let (route_tx, route_rx) = tokio::sync::mpsc::channel(4);
        let (fwd_tx, _fwd_rx) = tokio::sync::mpsc::channel(4);
        route_tx.send(done_frame(3, 0, true)).await.unwrap();
        let err = consume_fulltext_key_stream(route_rx, 3, &fwd_tx)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            FulltextConsumeError::TruncatedReplica { request_id: 3 }
        ));
    }

    /// Route closing before Done (gap-close, producer death, buffer
    /// overflow) is an error, not a short result.
    #[tokio::test]
    async fn consume_route_close_before_done_is_an_error() {
        let (route_tx, route_rx) = tokio::sync::mpsc::channel(4);
        let (fwd_tx, _fwd_rx) = tokio::sync::mpsc::channel(4);
        route_tx
            .send(chunk_frame(5, 0, vec![b"k".to_vec()]))
            .await
            .unwrap();
        drop(route_tx); // closed without Done
        let err = consume_fulltext_key_stream(route_rx, 5, &fwd_tx)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            FulltextConsumeError::ChannelClosedBeforeDone { request_id: 5 }
        ));
    }

    /// A dropped forward receiver = downstream satisfied/abandoned →
    /// EarlyStop (the caller then cancels the remote walk).
    #[tokio::test]
    async fn consume_dropped_forward_is_early_stop() {
        let (route_tx, route_rx) = tokio::sync::mpsc::channel(4);
        let (fwd_tx, fwd_rx) = tokio::sync::mpsc::channel(1);
        drop(fwd_rx);
        route_tx
            .send(chunk_frame(6, 0, vec![b"k".to_vec()]))
            .await
            .unwrap();
        let outcome = consume_fulltext_key_stream(route_rx, 6, &fwd_tx)
            .await
            .unwrap();
        assert_eq!(outcome, FulltextConsumeOutcome::EarlyStop);
    }

    /// The producer's resident set stays bounded: a walk of 200k keys with a
    /// tiny chunk size never holds more than the channel capacity + one
    /// batch. (Behavioral proxy: the stream completes and every key arrives
    /// exactly once — the allocator-level bound is pinned end-to-end by the
    /// storage-layer test and the cluster memory-bound test.)
    #[tokio::test]
    async fn large_walk_streams_completely() {
        let sink = VecSink::new();
        let n = 200_000;
        handle_fulltext_stream_request_with_cancel(
            req(4),
            Arc::new(FakeSource { n, fail: false }),
            &sink,
            FULLTEXT_STREAM_CHUNK_KEYS,
            CancellationToken::new(),
        )
        .await;
        let frames = sink.frames();
        let done = decode_done(frames.last().unwrap());
        assert!(!done.truncated);
        let total_keys: usize = frames[..frames.len() - 1]
            .iter()
            .map(|f| decode_chunk(f).keys.len())
            .sum();
        assert_eq!(total_keys, n);
        assert_eq!(done.total_chunks as usize, frames.len() - 1);
    }
}
