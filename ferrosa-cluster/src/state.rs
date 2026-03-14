use std::sync::Arc;

use tokio::sync::RwLock;

use ferrosa_schema::system::peers::{ClusterState, PeerInfo};

use crate::config::ClusterConfig;
use crate::pair::PairState;

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
            release_version: env!("CARGO_PKG_VERSION").to_string(),
        }]
    }
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
}
