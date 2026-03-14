use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use crate::pair::coordinator::{decode_mutation, PairCoordinator};
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
                // Forwarded write from secondary: apply + replicate back
                if let Err(e) = self.coordinator.apply_locally(&mutation) {
                    tracing::error!("failed to apply forwarded write: {e}");
                    return None;
                }
                self.coordinator.replicate_to_peer(&mutation).await
            }
            PairRole::Secondary => {
                // Replicated write from primary: apply locally only
                self.coordinator.apply_locally(&mutation)
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
