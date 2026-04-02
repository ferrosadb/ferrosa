// ferrosa-net/src/rpc/handler.rs
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::codec::MsgType;
use crate::message::Message;

/// Peer identifier: (host_id, socket_addr).
pub type PeerId = (uuid::Uuid, std::net::SocketAddr);

/// Trait for handling incoming RPC messages.
#[async_trait::async_trait]
pub trait RpcHandler: Send + Sync {
    /// Handle a message from a peer. Returns None for fire-and-forget.
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message>;
}

/// Registry mapping MsgType → handler.
///
/// Thread-safe for dynamic registration — handlers can be added after
/// the RPC server starts (e.g., when transitioning to pair mode).
pub struct HandlerRegistry {
    handlers: RwLock<HashMap<MsgType, Arc<dyn RpcHandler>>>,
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            handlers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a handler. Can be called at any time, including after
    /// the RPC server is running.
    pub fn register(&self, msg_type: MsgType, handler: Arc<dyn RpcHandler>) {
        self.handlers.write().insert(msg_type, handler);
    }

    /// Check if a handler is registered for the given message type.
    pub fn has_handler(&self, msg_type: MsgType) -> bool {
        self.handlers.read().contains_key(&msg_type)
    }

    pub async fn dispatch(&self, from: PeerId, msg_type: MsgType, msg: Message) -> Option<Message> {
        let handler = self.handlers.read().get(&msg_type).cloned();
        match handler {
            Some(handler) => handler.handle(from, msg).await,
            None => {
                tracing::warn!(?msg_type, "no handler registered");
                None
            }
        }
    }
}

/// Handler for inbound Ping messages. Replies with Pong, stamping the local
/// wall-clock time as `ping_recv_at` and `sent_at`.
pub struct PingHandler;

#[async_trait::async_trait]
impl RpcHandler for PingHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let Message::Ping { nonce, .. } = msg else {
            return None;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Some(Message::Pong {
            nonce,
            ping_recv_at: now,
            sent_at: now,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoPingHandler;

    #[async_trait::async_trait]
    impl RpcHandler for EchoPingHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            match msg {
                Message::Ping { nonce, .. } => Some(Message::Pong {
                    nonce,
                    ping_recv_at: 0,
                    sent_at: 0,
                }),
                _ => None,
            }
        }
    }

    #[tokio::test]
    async fn registry_dispatches_to_registered_handler() {
        let registry = HandlerRegistry::new();
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));

        let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let msg = Message::Ping {
            nonce: 42,
            sent_at: 0,
        };
        let response = registry.dispatch(peer_id, MsgType::Ping, msg).await;
        assert!(matches!(response, Some(Message::Pong { nonce: 42, .. })));
    }

    #[tokio::test]
    async fn registry_returns_none_for_unregistered_type() {
        let registry = HandlerRegistry::new();
        let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let msg = Message::Ping {
            nonce: 1,
            sent_at: 0,
        };
        let response = registry.dispatch(peer_id, MsgType::Ping, msg).await;
        assert!(response.is_none());
    }

    /// Regression test for BUG-RAFT-HANDLER-RACE.
    ///
    /// Simulates the cluster formation race: RaftVote messages arrive at a peer
    /// before its Raft handlers are registered (because handler registration
    /// happens asynchronously in spawn_tracked, after the mode transition).
    ///
    /// The test proves the bug exists: dispatch returns None for RaftVote when
    /// handlers haven't been registered yet, causing the sender to time out.
    /// Once fixed, this test should be updated to assert the message is handled.
    #[tokio::test]
    async fn raft_vote_dropped_when_handler_not_yet_registered() {
        // Simulate: node just transitioned to Cluster mode, but spawn_tracked
        // hasn't registered RaftVoteHandler yet.
        let registry = Arc::new(HandlerRegistry::new());
        let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());

        // RaftVote arrives immediately after mode transition — before handler registration
        let vote_msg = Message::RaftVote(bytes::Bytes::from_static(b"vote-request-payload"));
        let response = registry
            .dispatch(peer_id, MsgType::RaftVote, vote_msg)
            .await;

        // BUG: response is None because no handler is registered yet.
        // The sender will time out waiting for a VoteResponse.
        assert!(
            response.is_none(),
            "BUG-RAFT-HANDLER-RACE: RaftVote dispatched to missing handler returns None, \
             causing election timeout. Once fixed, this assertion should flip to Some."
        );

        // Simulate: spawn_tracked eventually completes and registers the handler
        let registry_clone = registry.clone();
        let handler_registered = tokio::spawn(async move {
            // Artificial delay simulating FerrosRaft::new()
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            registry_clone.register(MsgType::RaftVote, Arc::new(EchoPingHandler));
        });
        handler_registered.await.unwrap();

        // After registration, votes work — but any votes sent during the window were lost
        assert!(registry.has_handler(MsgType::RaftVote));
    }
}
