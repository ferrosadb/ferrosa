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

    /// Whether `host_id` advertised [`ferrosa_net::handshake::CAP_ACCORD_CAPNP`],
    /// i.e. decodes the Cap'n Proto `AccordApplyV2Capnp` body.
    ///
    /// Defaults to `false`: a transport that cannot positively confirm the peer's
    /// capability must fall back to the bincode `AccordApplyV2` frame. Sending the
    /// Cap'n Proto type to a peer that does not know the type byte drops the whole
    /// internode connection — a rolled-back (version-skewed) peer must still be sent
    /// a frame it can decode.
    async fn supports_accord_capnp(&self, _host_id: uuid::Uuid) -> bool {
        false
    }

    /// Whether `host_id` advertised
    /// [`ferrosa_net::handshake::CAP_ACCORD_APPLY_REGION`], i.e. decodes the
    /// region-REFERENCE `AccordApplyV2Region` body.
    ///
    /// Defaults to `false`: a transport that cannot positively confirm the peer's
    /// capability must fall back to the inline `AccordApplyV2Capnp` (or bincode
    /// `AccordApplyV2`) frame. Sending the region type to a peer that does not know
    /// the type byte drops the whole internode connection.
    async fn supports_accord_apply_region(&self, _host_id: uuid::Uuid) -> bool {
        false
    }
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

    async fn supports_accord_capnp(&self, host_id: uuid::Uuid) -> bool {
        self.peer_capabilities(host_id)
            .await
            .is_some_and(|caps| caps & ferrosa_net::handshake::CAP_ACCORD_CAPNP != 0)
    }

    async fn supports_accord_apply_region(&self, host_id: uuid::Uuid) -> bool {
        self.peer_capabilities(host_id)
            .await
            .is_some_and(|caps| caps & ferrosa_net::handshake::CAP_ACCORD_APPLY_REGION != 0)
    }
}
