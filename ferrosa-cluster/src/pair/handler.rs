use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use crate::pair::coordinator::{decode_batch, decode_mutation, PairCoordinator};
use crate::pair::PairRole;

/// Handles incoming PairWriteForward messages.
///
/// Primary: applies locally + replicates to secondary, then ACKs.
/// Secondary: applies locally, then ACKs (no further replication).
pub struct PairWriteForwardHandler {
    role: Arc<ArcSwap<PairRole>>,
    coordinator: Arc<PairCoordinator>,
}

impl PairWriteForwardHandler {
    pub fn new(role: Arc<ArcSwap<PairRole>>, coordinator: Arc<PairCoordinator>) -> Self {
        Self { role, coordinator }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairWriteForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairWriteForward(b) => b,
            _ => return None,
        };

        let mutation = match decode_mutation(&body) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("failed to decode PairWriteForward: {e}");
                return None;
            }
        };

        let result = match **self.role.load() {
            PairRole::Primary => {
                // Forwarded write from the secondary: apply here and replicate
                // it back concurrently, as `coordinate_write_with` does; the
                // ACK still waits for both.
                let (local, replica) = tokio::join!(
                    self.coordinator.apply_locally_async(mutation.clone(), None),
                    self.coordinator.replicate_to_peer(&mutation)
                );
                if let Err(e) = local {
                    tracing::error!("failed to apply forwarded write: {e}");
                    return None;
                }
                replica
            }
            PairRole::Secondary => {
                // Replicated write from primary: apply locally only
                self.coordinator.apply_locally_async(mutation, None).await
            }
        };

        match result {
            Ok(()) => Some(Message::PairWriteAck(Bytes::new())),
            Err(e) => {
                tracing::error!("PairWriteForward handler failed: {e}");
                None
            }
        }
    }
}

/// Handles incoming `PairBatchForward` messages: one atomic batch per message.
///
/// Primary: a batch the secondary forwarded. Applies it atomically with one
/// sync and replicates the same encoded batch back, concurrently, then ACKs.
/// Secondary: a batch the primary replicated. Applies it atomically with one
/// sync, then ACKs. Either way the node never holds part of the batch.
pub struct PairBatchForwardHandler {
    role: Arc<ArcSwap<PairRole>>,
    coordinator: Arc<PairCoordinator>,
}

impl PairBatchForwardHandler {
    pub fn new(role: Arc<ArcSwap<PairRole>>, coordinator: Arc<PairCoordinator>) -> Self {
        Self { role, coordinator }
    }
}

#[async_trait::async_trait]
impl RpcHandler for PairBatchForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::PairBatchForward(b) => b,
            _ => return None,
        };
        let (_batch_id, mutations) = match decode_batch(&body) {
            Ok(decoded) => decoded,
            Err(e) => {
                tracing::error!("failed to decode PairBatchForward: {e}");
                return None;
            }
        };

        let result = match **self.role.load() {
            PairRole::Primary => {
                let (local, replica) = tokio::join!(
                    self.coordinator.apply_batch_locally_async(mutations, None),
                    self.coordinator.replicate_batch_to_peer(body)
                );
                local.and(replica)
            }
            PairRole::Secondary => self
                .coordinator
                .apply_batch_locally_async(mutations, None)
                .await
                .map(drop),
        };

        match result {
            Ok(()) => Some(Message::PairBatchAck(Bytes::new())),
            Err(e) => {
                tracing::error!("PairBatchForward handler failed: {e}");
                None
            }
        }
    }
}
