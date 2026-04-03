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
/// Uses PeerManager to look up CQL broadcast addresses exchanged during
/// the internode handshake (for system.peers.native_address).
pub struct RaftClusterState {
    ring: Arc<ArcSwap<TokenRing>>,
    local_node_id: u64,
    peer_manager: Option<Arc<ferrosa_net::peer::PeerManager>>,
}

impl RaftClusterState {
    pub fn new(ring: Arc<ArcSwap<TokenRing>>, local_node_id: u64) -> Self {
        Self {
            ring,
            local_node_id,
            peer_manager: None,
        }
    }

    /// Create with a PeerManager reference for CQL broadcast lookups.
    pub fn with_peer_manager(
        ring: Arc<ArcSwap<TokenRing>>,
        local_node_id: u64,
        peer_manager: Arc<ferrosa_net::peer::PeerManager>,
    ) -> Self {
        Self {
            ring,
            local_node_id,
            peer_manager: Some(peer_manager),
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
                // Use cql_broadcast for native_address (host-reachable address
                // for port-mapped container clusters). Check three sources:
                // 1. NodeInfo.cql_broadcast (set during ring construction for local node)
                // 2. PeerManager broadcast (exchanged during internode handshake)
                // 3. Fall back to internode IP with port 9042
                let peer_broadcast = info.cql_broadcast.clone().or_else(|| {
                    let pm = self.peer_manager.as_ref()?;
                    // PeerManager uses async RwLock; use try_read to avoid blocking.
                    pm.get_peer_cql_broadcast_sync(info.host_id)
                });
                let (native_addr, native_port) = if let Some(ref broadcast) = peer_broadcast {
                    parse_addr(broadcast).unwrap_or((ip, 9042))
                } else {
                    (ip, 9042)
                };
                Some(PeerInfo {
                    peer: ip,
                    peer_port: port,
                    data_center: info.data_center.clone(),
                    rack: info.rack.clone(),
                    host_id: info.host_id,
                    preferred_ip: None,
                    preferred_port: None,
                    native_address: native_addr,
                    native_port,
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

    #[test]
    fn raft_cluster_state_uses_cql_broadcast_for_native_address() {
        use crate::raft::{NodeInfo, NodeState};
        use crate::ring::TokenRing;

        let local_id = 1_u64;
        let peer_id = 2_u64;

        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "172.17.0.2:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "172.17.0.3:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("192.168.1.100:19043".to_string()),
            },
        );

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));
        let state = RaftClusterState::new(ring_arc, local_id);
        let peers = state.peers();

        assert_eq!(peers.len(), 1);
        // native_address must be the CQL broadcast, not the container IP.
        assert_eq!(
            peers[0].native_address,
            "192.168.1.100".parse::<IpAddr>().unwrap()
        );
        assert_eq!(peers[0].native_port, 19043);
        // peer (internode) address stays as the container IP.
        assert_eq!(peers[0].peer, "172.17.0.3".parse::<IpAddr>().unwrap());
        assert_eq!(peers[0].peer_port, 7000);
    }

    #[test]
    fn raft_cluster_state_falls_back_to_internode_ip() {
        use crate::raft::{NodeInfo, NodeState};
        use crate::ring::TokenRing;

        let local_id = 1_u64;
        let peer_id = 2_u64;

        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "10.0.1.1:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "10.0.1.2:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None, // no broadcast set
            },
        );

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));
        let state = RaftClusterState::new(ring_arc, local_id);
        let peers = state.peers();

        assert_eq!(peers.len(), 1);
        // Without cql_broadcast, native_address falls back to internode IP.
        assert_eq!(
            peers[0].native_address,
            "10.0.1.2".parse::<IpAddr>().unwrap()
        );
        assert_eq!(peers[0].native_port, 9042);
    }

    /// Regression test for bug-system-peers-missing-tokens.md.
    ///
    /// In production, NodeInfo.cql_broadcast is None for PEER nodes (only
    /// the local node sets it during ring construction). The peer's CQL
    /// broadcast is learned via handshake and stored in PeerManager.
    /// system.peers must use PeerManager to look up the broadcast.
    #[tokio::test]
    async fn peer_broadcast_from_peer_manager_flows_into_system_peers() {
        use crate::raft::{NodeInfo, NodeState};
        use crate::ring::TokenRing;
        use ferrosa_net::config::NetConfig;
        use ferrosa_net::peer::PeerManager;

        let local_id = 1_u64;
        let peer_id = 2_u64;
        let peer_host_id = Uuid::new_v4();

        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "172.17.0.2:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: peer_host_id,
                addr: "172.17.0.3:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                // In production, this is None — peer doesn't set its own broadcast
                // in the ring. Only the local node sets it.
                cql_broadcast: None,
            },
        );

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));

        // Create a PeerManager that knows the peer's CQL broadcast (from handshake).
        struct NoopListener;
        impl ferrosa_net::peer::PeerEventListener for NoopListener {
            fn on_peer_connected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_disconnected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_suspected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_recovered(&self, _: uuid::Uuid) {}
            fn on_peer_failed(&self, _: uuid::Uuid) {}
        }
        let net_config = Arc::new(NetConfig::default());
        let pm = Arc::new(PeerManager::new(
            net_config,
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        // Simulate: handshake stored peer's CQL broadcast in PeerManager.
        pm.set_peer_cql_broadcast(peer_host_id, "127.0.0.1:19043".to_string())
            .await;

        // Build RaftClusterState WITH the PeerManager.
        let state = RaftClusterState::with_peer_manager(ring_arc, local_id, pm);
        let peers = state.peers();

        assert_eq!(peers.len(), 1);
        // native_address MUST be the broadcast from PeerManager,
        // not the container-internal IP 172.17.0.3.
        assert_eq!(
            peers[0].native_address,
            "127.0.0.1".parse::<IpAddr>().unwrap(),
            "system.peers must use CQL broadcast from PeerManager, not container IP"
        );
        assert_eq!(
            peers[0].native_port, 19043,
            "system.peers must use CQL broadcast port from PeerManager"
        );
        // Internode address stays as container IP.
        assert_eq!(peers[0].peer, "172.17.0.3".parse::<IpAddr>().unwrap());
    }

    /// Without PeerManager, same setup falls back to container IP.
    /// This proves the PeerManager lookup is what makes the difference.
    #[test]
    fn peer_broadcast_without_peer_manager_falls_back_to_container_ip() {
        use crate::raft::{NodeInfo, NodeState};
        use crate::ring::TokenRing;

        let local_id = 1_u64;
        let peer_id = 2_u64;

        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "172.17.0.2:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            },
        );
        ring.add_node(
            peer_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "172.17.0.3:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None, // no broadcast in NodeInfo
            },
        );

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));
        // No PeerManager — uses RaftClusterState::new (no peer_manager field).
        let state = RaftClusterState::new(ring_arc, local_id);
        let peers = state.peers();

        assert_eq!(peers.len(), 1);
        // Without PeerManager, falls back to container IP.
        assert_eq!(
            peers[0].native_address,
            "172.17.0.3".parse::<IpAddr>().unwrap(),
            "without PeerManager, native_address must be the container IP"
        );
        assert_eq!(peers[0].native_port, 9042);
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
