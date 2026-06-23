//! Transport seam for the Accord coordinator driver (Phase 2).
//!
//! The driver's only network dependency is "send a [`Message`] to a peer
//! `host_id` on a [`Lane`] and await the response message". Abstracting that
//! behind [`AccordTransport`] lets tests inject a mock that returns controllable
//! per-node responses, so the multi-node Commit/Apply per-shard quorum logic can
//! be exercised deterministically without a real network. [`PeerManager`] is the
//! production implementation — a thin forward to its inherent `send`.

use async_trait::async_trait;
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;

/// Request/response transport used by the Accord coordinator driver.
#[async_trait]
pub trait AccordTransport: Send + Sync {
    /// Send `msg` to `host_id` on `lane`, awaiting the peer's response message.
    async fn send(
        &self,
        host_id: uuid::Uuid,
        msg: Message,
        lane: Lane,
    ) -> ferrosa_net::error::Result<Message>;
}

#[async_trait]
impl AccordTransport for PeerManager {
    async fn send(
        &self,
        host_id: uuid::Uuid,
        msg: Message,
        lane: Lane,
    ) -> ferrosa_net::error::Result<Message> {
        // Forward to the inherent method (this trait impl only adds the dyn seam).
        PeerManager::send(self, host_id, msg, lane).await
    }
}
