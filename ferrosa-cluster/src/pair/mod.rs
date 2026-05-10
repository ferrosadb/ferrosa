pub mod catchup;
pub mod coordinator;
pub mod ddl;
pub mod handler;
pub mod node;
pub mod switchover;

pub use coordinator::PairCoordinator;
pub use handler::PairWriteForwardHandler;
pub use node::PairNode;

use std::net::SocketAddr;
use uuid::Uuid;

/// Role within a pair. Determined by connection direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairRole {
    Primary,
    Secondary,
}

impl PairRole {
    /// Determine role from connection direction.
    ///
    /// - `is_inbound = true`: this node received the connection (seed) → Primary.
    ///   The seed has data and is the authority.
    /// - `is_inbound = false`: this node initiated the connection (joiner) → Secondary.
    ///   The joiner will receive data from the primary.
    ///
    /// This is deterministic from the TCP connection direction — no UUID comparison,
    /// no consensus, no race conditions.
    pub fn from_connection_direction(is_inbound: bool) -> Self {
        if is_inbound {
            Self::Primary
        } else {
            Self::Secondary
        }
    }

    /// Legacy: Determine this node's role by comparing host_ids.
    /// Deprecated in favor of [`Self::from_connection_direction`].
    #[deprecated(note = "use from_connection_direction — UUID election has race conditions")]
    pub fn elect(local_id: Uuid, peer_id: Uuid) -> Self {
        if local_id > peer_id {
            Self::Primary
        } else {
            Self::Secondary
        }
    }

    /// Return the opposite role.
    pub fn opposite(&self) -> Self {
        match self {
            Self::Primary => Self::Secondary,
            Self::Secondary => Self::Primary,
        }
    }
}

impl std::fmt::Display for PairRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Secondary => write!(f, "secondary"),
        }
    }
}

/// Tracks the state of the pair relationship.
pub struct PairState {
    /// This node's current role.
    pub role: PairRole,
    /// Peer's host_id.
    pub peer_host_id: Uuid,
    /// Peer's internode address.
    pub peer_addr: SocketAddr,
    /// Whether the peer is currently connected.
    pub connected: bool,
    /// Last commit log position successfully replicated to peer.
    /// `(segment_id, offset)`.
    pub last_replicated_position: Option<(u64, u64)>,
}

impl PairState {
    pub fn new(role: PairRole, peer_host_id: Uuid, peer_addr: SocketAddr) -> Self {
        Self {
            role,
            peer_host_id,
            peer_addr,
            connected: false,
            last_replicated_position: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbound_connection_becomes_primary() {
        assert_eq!(PairRole::from_connection_direction(true), PairRole::Primary);
    }

    #[test]
    fn outbound_connection_becomes_secondary() {
        assert_eq!(
            PairRole::from_connection_direction(false),
            PairRole::Secondary
        );
    }

    #[test]
    #[allow(deprecated)]
    fn legacy_elect_primary_higher_id_wins() {
        let high = Uuid::from_bytes([0xFF; 16]);
        let low = Uuid::from_bytes([0x00; 16]);

        assert_eq!(PairRole::elect(high, low), PairRole::Primary);
        assert_eq!(PairRole::elect(low, high), PairRole::Secondary);
    }

    #[test]
    fn role_opposite() {
        assert_eq!(PairRole::Primary.opposite(), PairRole::Secondary);
        assert_eq!(PairRole::Secondary.opposite(), PairRole::Primary);
    }

    /// Hazard P1-5 (W1.18): role is assigned by TCP connection direction,
    /// independent of UUID order. The receiver of the connection (inbound)
    /// is Primary; the initiator (outbound) is Secondary.
    ///
    /// This pins the contract `PairRole::elect` violated: under elect(), a
    /// node could swap roles depending on which side won a UUID race.
    #[test]
    fn pair_role_assigned_by_connection_direction() {
        // Direction-based assignment never consults a UUID — so by
        // construction it's independent of UUID order. To pin that, this
        // test asserts both directions produce the documented role and
        // makes no UUID arguments available to from_connection_direction
        // (the function signature itself enforces UUID independence).

        // Node receiving the connection (inbound) → Primary.
        // (If we had used PairRole::elect, this would depend on UUID.)
        assert_eq!(
            PairRole::from_connection_direction(true),
            PairRole::Primary,
            "the inbound (receiving) side is Primary regardless of UUID"
        );

        // Node initiating the connection (outbound) → Secondary.
        assert_eq!(
            PairRole::from_connection_direction(false),
            PairRole::Secondary,
            "the outbound (initiating) side is Secondary regardless of UUID"
        );

        // Round-trip: a pair that swaps directions also swaps roles.
        assert_ne!(
            PairRole::from_connection_direction(true),
            PairRole::from_connection_direction(false),
            "the two ends of a connection have opposite roles"
        );
    }

    #[test]
    fn pair_state_default_not_connected() {
        let state = PairState::new(
            PairRole::Primary,
            Uuid::new_v4(),
            "127.0.0.1:7000".parse().unwrap(),
        );
        assert!(!state.connected);
        assert!(state.last_replicated_position.is_none());
    }
}
