use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::RwLock;

use ferrosa_schema::system::peers::{ClusterState, PeerInfo};

use crate::config::ClusterConfig;
use crate::pair::PairState;
use crate::ring::TokenRing;

/// Standalone cluster state — reports no peers.
pub struct SingleNodeClusterState;

impl ClusterState for SingleNodeClusterState {
    fn peers(&self) -> Vec<PeerInfo> {
        vec![]
    }
}

/// ClusterState implementation for pair mode.
///
/// Returns the single peer as the only entry in `peers()`.
pub struct PairClusterState {
    config: Arc<ClusterConfig>,
    state: Arc<RwLock<PairState>>,
}

impl PairClusterState {
    pub fn new(config: Arc<ClusterConfig>, state: Arc<RwLock<PairState>>) -> Self {
        Self { config, state }
    }
}

impl ClusterState for PairClusterState {
    fn peers(&self) -> Vec<PeerInfo> {
        // Use try_read to avoid blocking. If locked, return empty.
        let state = match self.state.try_read() {
            Ok(s) => s,
            Err(_) => return vec![],
        };

        let peer_addr = state.peer_addr;
        let ip = peer_addr.ip();
        let port = peer_addr.port();

        vec![PeerInfo {
            peer: ip,
            peer_port: port,
            data_center: self.config.data_center.clone(),
            rack: self.config.rack.clone(),
            host_id: state.peer_host_id,
            preferred_ip: None,
            preferred_port: None,
            native_address: ip,
            native_port: 9042,
            schema_version: uuid::Uuid::nil(),
            tokens: vec![],
            release_version: ferrosa_schema::system::RELEASE_VERSION.to_string(),
        }]
    }
}

/// ClusterState implementation for Raft cluster mode.
///
/// Reads node metadata from the token ring to produce the peer list.
pub struct RaftClusterState {
    ring: Arc<ArcSwap<TokenRing>>,
    local_node_id: u64,
}

impl RaftClusterState {
    pub fn new(ring: Arc<ArcSwap<TokenRing>>, local_node_id: u64) -> Self {
        Self {
            ring,
            local_node_id,
        }
    }
}

impl ClusterState for RaftClusterState {
    fn peers(&self) -> Vec<PeerInfo> {
        let ring = self.ring.load();
        ring.node_ids()
            .iter()
            .filter(|&&id| id != self.local_node_id)
            .filter_map(|&id| {
                let info = ring.get_node(id)?;
                // Parse addr string "host:port" into IP + port.
                let (ip, port) = parse_addr(&info.addr)?;
                Some(PeerInfo {
                    peer: ip,
                    peer_port: port,
                    data_center: info.data_center.clone(),
                    rack: info.rack.clone(),
                    host_id: info.host_id,
                    preferred_ip: None,
                    preferred_port: None,
                    native_address: ip,
                    native_port: 9042,
                    schema_version: uuid::Uuid::nil(),
                    tokens: vec![],
                    release_version: ferrosa_schema::system::RELEASE_VERSION.to_string(),
                })
            })
            .collect()
    }
}

/// Parse "ip:port" or "host:port" into (IpAddr, u16).
/// Returns None if parsing fails.
fn parse_addr(addr: &str) -> Option<(IpAddr, u16)> {
    let socket_addr: std::net::SocketAddr = addr.parse().ok()?;
    Some((socket_addr.ip(), socket_addr.port()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    use crate::pair::PairRole;
    use uuid::Uuid;

    #[test]
    fn pair_cluster_state_returns_peer() {
        let config = Arc::new(ClusterConfig::default());
        let peer_id = Uuid::new_v4();
        let peer_addr = "10.0.1.5:7000".parse().unwrap();
        let state = Arc::new(RwLock::new(PairState::new(
            PairRole::Primary,
            peer_id,
            peer_addr,
        )));
        let cluster_state = PairClusterState::new(config, state);

        let peers = cluster_state.peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].host_id, peer_id);
        assert_eq!(peers[0].peer, "10.0.1.5".parse::<IpAddr>().unwrap());
        assert_eq!(peers[0].peer_port, 7000);
    }

    /// BUG-020: system.peers release_version must match system.local
    /// (Cassandra-compatible "5.1.0-ferrosa"), not the Cargo crate version.
    #[test]
    fn peer_release_version_matches_system_local() {
        let config = Arc::new(ClusterConfig::default());
        let peer_id = Uuid::new_v4();
        let peer_addr = "10.0.1.5:7000".parse().unwrap();
        let state = Arc::new(RwLock::new(PairState::new(
            PairRole::Primary,
            peer_id,
            peer_addr,
        )));
        let cluster_state = PairClusterState::new(config, state);

        let peers = cluster_state.peers();
        assert_eq!(peers.len(), 1);
        // system.local reports "5.1.0-ferrosa"; system.peers must match.
        assert_eq!(
            peers[0].release_version, "5.1.0-ferrosa",
            "peer release_version should be Cassandra-compatible, not CARGO_PKG_VERSION"
        );
    }
}
