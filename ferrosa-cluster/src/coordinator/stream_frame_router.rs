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

use std::sync::Arc;

use async_trait::async_trait;

use ferrosa_net::codec::MsgType;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_net::stream_router::{RouteError, StreamRouter};

/// Inbound dispatch handler for the three streaming range-read
/// response frame types. Decodes the leading `request_id` from each
/// frame's bincode payload and pushes the whole `Message` through
/// the shared [`StreamRouter`].
pub struct StreamFrameRouter {
    router: Arc<StreamRouter>,
}

impl StreamFrameRouter {
    pub fn new(router: Arc<StreamRouter>) -> Self {
        Self { router }
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
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
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

        match self.router.route(request_id, msg) {
            Ok(()) => {}
            Err(RouteError::NoRoute(id)) => {
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
                tracing::debug!(
                    request_id = id,
                    ?msg_type,
                    "stream consumer dropped before this frame arrived"
                );
            }
            Err(RouteError::ChannelFull(id)) => {
                tracing::warn!(
                    request_id = id,
                    ?msg_type,
                    "stream consumer buffer full; dropping frame — coordinator falling behind"
                );
            }
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
        let payload = RangeReadStreamChunkPayload {
            request_id: id,
            seq: 0,
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

    fn encoded_done(id: u32) -> Message {
        let payload = RangeReadStreamDonePayload {
            request_id: id,
            total_chunks: 0,
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
        handler.handle(peer(), encoded_done(REQ_ID)).await;

        let f1 = rx.recv().await.unwrap();
        let f2 = rx.recv().await.unwrap();
        let f3 = rx.recv().await.unwrap();
        assert!(matches!(f1, Message::RangeReadStreamChunk(_)));
        assert!(matches!(f2, Message::RangeReadStreamHeartbeat(_)));
        assert!(matches!(f3, Message::RangeReadStreamDone(_)));
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
