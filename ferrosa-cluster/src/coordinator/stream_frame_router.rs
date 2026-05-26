//! Coordinator-side inbound dispatch for streaming range-read frames.
//!
//! Registered with [`HandlerRegistry`] on the coordinator node so
//! every `Message::RangeReadStreamChunk`, `RangeReadStreamHeartbeat`,
//! and `RangeReadStreamDone` frame received on the Bulk lane is
//! decoded just far enough to extract the leading `request_id` and
//! routed to the per-request [`tokio::sync::mpsc::Receiver`] that
//! `consume_range_stream` is reading from.
//!
//! No payload semantics here — that lives in [`stream_consumer`].
//! This handler exists purely to bridge the existing `RpcHandler`
//! inbound dispatch into the [`StreamRouter`].
//!
//! [`HandlerRegistry`]: ferrosa_net::rpc::handler::HandlerRegistry
//! [`StreamRouter`]: ferrosa_net::stream_router::StreamRouter
//! [`stream_consumer`]: super::stream_consumer

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use ferrosa_net::codec::MsgType;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_net::stream_router::{RouteError, StreamRouter};

use crate::raft::handlers::{RangeReadStreamChunkPayload, RangeReadStreamDonePayload};

type StreamSeqKey = (u32, uuid::Uuid, SocketAddr);

struct PendingDone {
    total_chunks: u32,
    message: Message,
}

#[derive(Default)]
struct StreamSeqState {
    next_chunk_seq: u32,
    pending_done: Option<PendingDone>,
}

/// Inbound dispatch handler for the three streaming range-read
/// response frame types. Decodes the leading `request_id` from each
/// frame's bincode payload and pushes the whole `Message` through
/// the shared [`StreamRouter`].
pub struct StreamFrameRouter {
    router: Arc<StreamRouter>,
    seq_state: Mutex<HashMap<StreamSeqKey, StreamSeqState>>,
}

impl StreamFrameRouter {
    pub fn new(router: Arc<StreamRouter>) -> Self {
        Self {
            router,
            seq_state: Mutex::new(HashMap::new()),
        }
    }

    fn clear_request_state(&self, request_id: u32) {
        self.seq_state
            .lock()
            .expect("stream sequence mutex poisoned")
            .retain(|(id, _, _), _| *id != request_id);
    }

    fn accept_chunk_seq(
        &self,
        from: PeerId,
        request_id: u32,
        bytes: &[u8],
    ) -> Option<Option<Message>> {
        let Ok(payload) = bincode::deserialize::<RangeReadStreamChunkPayload>(bytes) else {
            return Some(None);
        };
        let key = (request_id, from.0, from.1);
        let mut guard = self
            .seq_state
            .lock()
            .expect("stream sequence mutex poisoned");
        let state = guard.entry(key).or_default();
        if payload.seq != state.next_chunk_seq {
            tracing::warn!(
                request_id,
                peer = %from.0,
                expected_seq = state.next_chunk_seq,
                observed_seq = payload.seq,
                "stream chunk sequence gap/reorder; closing stream route"
            );
            drop(guard);
            self.router.unregister(request_id);
            self.clear_request_state(request_id);
            return None;
        }
        state.next_chunk_seq = state.next_chunk_seq.saturating_add(1);

        let pending_done = state
            .pending_done
            .as_ref()
            .is_some_and(|done| done.total_chunks == state.next_chunk_seq)
            .then(|| state.pending_done.take().expect("checked Some").message);
        if pending_done.is_some() {
            guard.remove(&key);
        }
        Some(pending_done)
    }

    fn accept_done_seq(
        &self,
        from: PeerId,
        request_id: u32,
        bytes: &[u8],
        msg: Message,
    ) -> Option<Option<Message>> {
        let Ok(payload) = bincode::deserialize::<RangeReadStreamDonePayload>(bytes) else {
            return Some(Some(msg));
        };
        let key = (request_id, from.0, from.1);
        let mut guard = self
            .seq_state
            .lock()
            .expect("stream sequence mutex poisoned");
        let state = guard.entry(key).or_default();
        let observed = state.next_chunk_seq;

        if payload.total_chunks < observed {
            tracing::warn!(
                request_id,
                peer = %from.0,
                observed_chunks = observed,
                reported_chunks = payload.total_chunks,
                "stream Done chunk count mismatch; closing stream route"
            );
            drop(guard);
            self.router.unregister(request_id);
            self.clear_request_state(request_id);
            return None;
        }

        if payload.total_chunks == observed {
            guard.remove(&key);
            return Some(Some(msg));
        }

        if state.pending_done.is_some() {
            tracing::warn!(
                request_id,
                peer = %from.0,
                observed_chunks = observed,
                reported_chunks = payload.total_chunks,
                "duplicate early stream Done; closing stream route"
            );
            drop(guard);
            self.router.unregister(request_id);
            self.clear_request_state(request_id);
            return None;
        }

        state.pending_done = Some(PendingDone {
            total_chunks: payload.total_chunks,
            message: msg,
        });
        Some(None)
    }

    fn route_frame(&self, request_id: u32, msg_type: MsgType, msg: Message) {
        match self.router.route(request_id, msg) {
            Ok(()) => {}
            Err(RouteError::NoRoute(id)) => {
                self.clear_request_state(id);
                // Stale frame for a request the consumer already
                // finished or never registered. Common after
                // cancellation; debug level only.
                tracing::debug!(
                    request_id = id,
                    ?msg_type,
                    "stream frame for unknown request_id (stale or already done)"
                );
            }
            Err(RouteError::ChannelClosed(id)) => {
                self.clear_request_state(id);
                tracing::debug!(
                    request_id = id,
                    ?msg_type,
                    "stream consumer dropped before this frame arrived"
                );
            }
            Err(RouteError::ChannelFull(id)) => {
                self.clear_request_state(id);
                tracing::warn!(
                    request_id = id,
                    ?msg_type,
                    "stream consumer buffer full; closing route so consumer fails instead of returning partial data"
                );
            }
        }
    }
}

/// Decode the leading `u32` `request_id` from a bincode-serialized
/// streaming-payload byte slice. Returns `None` if the payload is
/// too short to contain a request_id.
///
/// Every streaming payload (chunk, heartbeat, done, cancel,
/// request) starts with `request_id: u32` in bincode's default LE
/// fixint encoding — see
/// `streaming_chunk_payload_starts_with_request_id_for_router_dispatch`
/// in raft::handlers tests.
fn peek_request_id(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 4 {
        return None;
    }
    Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[async_trait]
impl RpcHandler for StreamFrameRouter {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let (request_id, msg_type) = match &msg {
            Message::RangeReadStreamChunk(b) => (peek_request_id(b), MsgType::RangeReadStreamChunk),
            Message::RangeReadStreamHeartbeat(b) => {
                (peek_request_id(b), MsgType::RangeReadStreamHeartbeat)
            }
            Message::RangeReadStreamDone(b) => (peek_request_id(b), MsgType::RangeReadStreamDone),
            // Not ours — return None so the handler dispatch chain
            // can give a different handler a chance. (HandlerRegistry
            // currently dispatches by MsgType, so this is defensive.)
            _ => return None,
        };

        let Some(request_id) = request_id else {
            tracing::warn!(
                ?msg_type,
                "stream frame too short to carry request_id; dropping"
            );
            return None;
        };

        let pending_after_chunk = match msg {
            Message::RangeReadStreamChunk(bytes) => {
                let pending_done = self.accept_chunk_seq(from, request_id, bytes.as_ref())?;
                self.route_frame(request_id, msg_type, Message::RangeReadStreamChunk(bytes));
                pending_done
            }
            Message::RangeReadStreamDone(bytes) => {
                let msg = Message::RangeReadStreamDone(bytes.clone());
                let route_done = self.accept_done_seq(from, request_id, bytes.as_ref(), msg)?;
                if let Some(done) = route_done {
                    self.route_frame(request_id, msg_type, done);
                }
                return None;
            }
            other => {
                self.route_frame(request_id, msg_type, other);
                None
            }
        };

        if let Some(done) = pending_after_chunk {
            self.route_frame(request_id, MsgType::RangeReadStreamDone, done);
        }

        // Streaming response frames are fire-and-forget — never a
        // synchronous reply.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use std::time::Duration;
    use uuid::Uuid;

    use crate::raft::handlers::{
        RangeReadStreamChunkPayload, RangeReadStreamDonePayload, RangeReadStreamHeartbeatPayload,
    };
    use ferrosa_net::rpc::handler::PeerId;

    const REQ_ID: u32 = 7;

    fn peer() -> PeerId {
        (Uuid::nil(), "127.0.0.1:7000".parse().unwrap())
    }

    fn encoded_chunk(id: u32) -> Message {
        encoded_chunk_seq(id, 0)
    }

    fn encoded_chunk_seq(id: u32, seq: u32) -> Message {
        let payload = RangeReadStreamChunkPayload {
            request_id: id,
            seq,
            partitions: vec![],
        };
        Message::RangeReadStreamChunk(Bytes::from(bincode::serialize(&payload).unwrap()))
    }

    fn encoded_heartbeat(id: u32) -> Message {
        let payload = RangeReadStreamHeartbeatPayload {
            request_id: id,
            seq: 0,
        };
        Message::RangeReadStreamHeartbeat(Bytes::from(bincode::serialize(&payload).unwrap()))
    }

    fn encoded_done(id: u32, total_chunks: u32) -> Message {
        let payload = RangeReadStreamDonePayload {
            request_id: id,
            total_chunks,
            truncated: false,
        };
        Message::RangeReadStreamDone(Bytes::from(bincode::serialize(&payload).unwrap()))
    }

    /// A chunk frame for a registered request_id reaches the
    /// per-request receiver.
    #[tokio::test]
    async fn chunk_frame_routed_to_registered_receiver() {
        let router = Arc::new(StreamRouter::new());
        let mut rx = router.register(REQ_ID, 4);
        let handler = StreamFrameRouter::new(router.clone());

        let reply = handler.handle(peer(), encoded_chunk(REQ_ID)).await;
        assert!(reply.is_none(), "streaming dispatch never replies sync");

        let frame = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("frame must arrive within deadline")
            .expect("channel must be open");
        assert!(matches!(frame, Message::RangeReadStreamChunk(_)));
    }

    /// All three frame types are routed when their request_id is
    /// registered.
    #[tokio::test]
    async fn chunk_heartbeat_and_done_all_route() {
        let router = Arc::new(StreamRouter::new());
        let mut rx = router.register(REQ_ID, 8);
        let handler = StreamFrameRouter::new(router.clone());

        handler.handle(peer(), encoded_chunk(REQ_ID)).await;
        handler.handle(peer(), encoded_heartbeat(REQ_ID)).await;
        handler.handle(peer(), encoded_done(REQ_ID, 1)).await;

        let f1 = rx.recv().await.unwrap();
        let f2 = rx.recv().await.unwrap();
        let f3 = rx.recv().await.unwrap();
        assert!(matches!(f1, Message::RangeReadStreamChunk(_)));
        assert!(matches!(f2, Message::RangeReadStreamHeartbeat(_)));
        assert!(matches!(f3, Message::RangeReadStreamDone(_)));
    }

    /// Net dispatch spawns one task per inbound frame, so a Done frame
    /// can beat its preceding chunk to this router. The consumer drains
    /// those stragglers after Done; the router must not close the route
    /// just because Done.total_chunks is ahead of chunks observed so far.
    #[tokio::test]
    async fn done_before_chunk_keeps_route_open_for_consumer_straggler_drain() {
        let router = Arc::new(StreamRouter::new());
        let mut rx = router.register(REQ_ID, 8);
        let handler = StreamFrameRouter::new(router.clone());

        handler.handle(peer(), encoded_done(REQ_ID, 1)).await;

        assert!(
            !router.is_empty(),
            "Done racing ahead of chunks is valid; consumer drains stragglers"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "early Done is held until its declared chunks arrive"
        );

        handler.handle(peer(), encoded_chunk_seq(REQ_ID, 0)).await;
        assert!(matches!(
            rx.recv().await,
            Some(Message::RangeReadStreamChunk(_))
        ));
        assert!(matches!(
            rx.recv().await,
            Some(Message::RangeReadStreamDone(_))
        ));
    }

    /// Frame for an unregistered request_id is dropped silently
    /// (consumer already finished or cancelled). No reply, no panic.
    #[tokio::test]
    async fn unregistered_request_id_is_dropped() {
        let router = Arc::new(StreamRouter::new());
        let handler = StreamFrameRouter::new(router.clone());

        let reply = handler.handle(peer(), encoded_chunk(99)).await;
        assert!(reply.is_none());
        // No panic, nothing in the routing table either.
        assert!(router.is_empty());
    }

    #[tokio::test]
    async fn full_stream_buffer_closes_route_so_consumer_fails_loudly() {
        let router = Arc::new(StreamRouter::new());
        let mut rx = router.register(REQ_ID, 1);
        let handler = StreamFrameRouter::new(router.clone());

        handler.handle(peer(), encoded_chunk_seq(REQ_ID, 0)).await;
        handler.handle(peer(), encoded_chunk_seq(REQ_ID, 1)).await;

        assert!(
            router.is_empty(),
            "full stream buffer must close the route instead of dropping a chunk and allowing partial success"
        );
        assert!(matches!(
            rx.recv().await,
            Some(Message::RangeReadStreamChunk(_))
        ));
        assert_eq!(
            rx.recv().await,
            None,
            "consumer must observe channel close after route is closed"
        );
    }

    #[tokio::test]
    async fn missing_chunk_sequence_closes_route_so_consumer_fails_loudly() {
        let router = Arc::new(StreamRouter::new());
        let mut rx = router.register(REQ_ID, 8);
        let handler = StreamFrameRouter::new(router.clone());

        handler.handle(peer(), encoded_chunk_seq(REQ_ID, 1)).await;

        assert!(
            router.is_empty(),
            "seq=1 as the first chunk proves a missing chunk and must close the route"
        );
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn reordered_chunk_sequence_closes_route_so_consumer_fails_loudly() {
        let router = Arc::new(StreamRouter::new());
        let mut rx = router.register(REQ_ID, 8);
        let handler = StreamFrameRouter::new(router.clone());

        handler.handle(peer(), encoded_chunk_seq(REQ_ID, 0)).await;
        handler.handle(peer(), encoded_chunk_seq(REQ_ID, 0)).await;

        assert!(
            router.is_empty(),
            "duplicate seq=0 after seq=0 was already accepted proves reorder/duplication and must close the route"
        );
        assert!(matches!(
            rx.recv().await,
            Some(Message::RangeReadStreamChunk(_))
        ));
        assert_eq!(rx.recv().await, None);
    }

    /// Non-streaming frames are not ours — handler returns None
    /// without touching the router.
    #[tokio::test]
    async fn non_streaming_frame_is_pass_through() {
        let router = Arc::new(StreamRouter::new());
        let handler = StreamFrameRouter::new(router.clone());

        let reply = handler
            .handle(
                peer(),
                Message::Ping {
                    nonce: 1,
                    sent_at: 0,
                },
            )
            .await;
        assert!(reply.is_none());
    }

    /// A frame whose payload is too short to contain a `request_id`
    /// is dropped at the peek step rather than routed to id=0 (which
    /// could collide with a real stream).
    #[tokio::test]
    async fn truncated_payload_does_not_route_to_id_zero() {
        let router = Arc::new(StreamRouter::new());
        let _rx0 = router.register(0, 4);
        let handler = StreamFrameRouter::new(router.clone());

        // Only 2 bytes — peek_request_id returns None.
        let reply = handler
            .handle(
                peer(),
                Message::RangeReadStreamChunk(Bytes::from_static(&[0, 0])),
            )
            .await;
        assert!(reply.is_none());

        // The id=0 receiver should not have received the truncated
        // frame.
        let mut rx0 = _rx0;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx0.recv())
                .await
                .is_err(),
            "no frame should have been routed"
        );
    }

    /// peek_request_id pulls the LE-encoded u32 from the first 4
    /// bytes — pins the on-wire contract.
    #[test]
    fn peek_request_id_returns_le_u32() {
        assert_eq!(
            peek_request_id(&[0x78, 0x56, 0x34, 0x12]),
            Some(0x1234_5678)
        );
        assert_eq!(peek_request_id(&[1, 2, 3]), None, "too short → None");
        assert_eq!(peek_request_id(&[0, 0, 0, 0, 99]), Some(0));
    }
}
