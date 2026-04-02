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
