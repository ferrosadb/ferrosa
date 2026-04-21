use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
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
/// Maintains a cached result via `ArcSwap` so that `RwLock` contention
/// returns the last known peer list instead of an empty vec.
pub struct PairClusterState {
    config: Arc<ClusterConfig>,
    state: Arc<RwLock<PairState>>,
    broadcast_resolver: Option<Arc<dyn BroadcastResolver>>,
    /// Cached peers result — returned on `RwLock` contention instead of
    /// an empty vec. Updated each time `peers()` successfully reads state.
    cached_peers: ArcSwap<Vec<PeerInfo>>,
}

impl PairClusterState {
    pub fn new(config: Arc<ClusterConfig>, state: Arc<RwLock<PairState>>) -> Self {
        Self {
            config,
            state,
            broadcast_resolver: None,
            cached_peers: ArcSwap::from_pointee(Vec::new()),
        }
    }

    pub fn with_peer_manager(
        config: Arc<ClusterConfig>,
        state: Arc<RwLock<PairState>>,
        peer_manager: Arc<ferrosa_net::peer::PeerManager>,
    ) -> Self {
        Self {
            config,
            state,
            broadcast_resolver: Some(peer_manager as Arc<dyn BroadcastResolver>),
            cached_peers: ArcSwap::from_pointee(Vec::new()),
        }
    }
}

impl ClusterState for PairClusterState {
    fn peers(&self) -> Vec<PeerInfo> {
        // Use try_read to avoid blocking. On contention, return cached result.
        let state = match self.state.try_read() {
            Ok(s) => s,
            Err(_) => return (**self.cached_peers.load()).clone(),
        };

        let peer_addr = state.peer_addr;
        let ip = peer_addr.ip();
        let port = peer_addr.port();
        let peer_broadcast = self
            .broadcast_resolver
            .as_ref()
            .and_then(|resolver| resolver.resolve_broadcast(state.peer_host_id));
        let (native_addr, native_port) = if let Some(ref broadcast) = peer_broadcast {
            parse_addr(broadcast).unwrap_or((ip, 9042))
        } else {
            (ip, 9042)
        };
        let peer_node_id = crate::raft::uuid_to_node_id(state.peer_host_id);
        let tokens: Vec<String> = crate::controller::deterministic_tokens_for_node(
            peer_node_id,
            self.config.num_tokens as usize,
        )
        .into_iter()
        .map(|t| t.to_string())
        .collect();

        let result = vec![PeerInfo {
            peer: ip,
            peer_port: port,
            data_center: self.config.data_center.clone(),
            rack: self.config.rack.clone(),
            host_id: state.peer_host_id,
            preferred_ip: None,
            preferred_port: None,
            native_address: native_addr,
            native_port,
            internal_native_address: ip,
            internal_native_port: 9042,
            schema_version: uuid::Uuid::nil(),
            tokens,
            release_version: ferrosa_schema::system::RELEASE_VERSION.to_string(),
        }];

        // Update the cache for future contention cases.
        self.cached_peers.store(Arc::new(result.clone()));

        result
    }
}

/// Trait for resolving CQL broadcast addresses for peers.
///
/// Decouples state.rs from ferrosa-net::PeerManager. Implementations
/// provide broadcast addresses learned during internode handshakes.
pub trait BroadcastResolver: Send + Sync {
    /// Look up the CQL broadcast address for a peer by host_id.
    /// Non-blocking — returns None if the address is unavailable.
    fn resolve_broadcast(&self, host_id: uuid::Uuid) -> Option<String>;
}

/// PeerManager implements BroadcastResolver via its broadcast map.
impl BroadcastResolver for ferrosa_net::peer::PeerManager {
    fn resolve_broadcast(&self, host_id: uuid::Uuid) -> Option<String> {
        self.get_peer_cql_broadcast_sync(host_id)
    }
}

/// ClusterState implementation for Raft cluster mode.
///
/// Reads node metadata from the token ring to produce the peer list.
/// Uses a BroadcastResolver to look up CQL broadcast addresses exchanged
/// during the internode handshake (for system.peers.native_address).
pub struct RaftClusterState {
    ring: Arc<ArcSwap<TokenRing>>,
    local_node_id: u64,
    broadcast_resolver: Option<Arc<dyn BroadcastResolver>>,
}

impl RaftClusterState {
    pub fn new(ring: Arc<ArcSwap<TokenRing>>, local_node_id: u64) -> Self {
        Self {
            ring,
            local_node_id,
            broadcast_resolver: None,
        }
    }

    /// Create with a broadcast resolver for CQL broadcast lookups.
    pub fn with_peer_manager(
        ring: Arc<ArcSwap<TokenRing>>,
        local_node_id: u64,
        peer_manager: Arc<ferrosa_net::peer::PeerManager>,
    ) -> Self {
        Self {
            ring,
            local_node_id,
            broadcast_resolver: Some(peer_manager as Arc<dyn BroadcastResolver>),
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
                    let resolver = self.broadcast_resolver.as_ref()?;
                    resolver.resolve_broadcast(info.host_id)
                });
                let (native_addr, native_port) = if let Some(ref broadcast) = peer_broadcast {
                    parse_addr(broadcast).unwrap_or((ip, 9042))
                } else {
                    (ip, 9042)
                };
                // Populate `tokens` from the ring. cdrs-tokio's
                // `is_peer_row_valid` filters out any peer row whose
                // `tokens` column is empty — and on an empty pool the
                // session-build hangs (see
                // specs/in-process/bug-cql-auth-enabled-cluster-times-
                // out-for-cdrs-clients.md). Tokens are formatted as
                // decimal strings, matching Cassandra's system.peers
                // wire shape (`set<text>`).
                let tokens: Vec<String> = ring
                    .tokens_for_node(id)
                    .into_iter()
                    .map(|t| t.to_string())
                    .collect();
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
                    internal_native_address: ip,
                    internal_native_port: 9042,
                    schema_version: uuid::Uuid::nil(),
                    tokens,
                    release_version: ferrosa_schema::system::RELEASE_VERSION.to_string(),
                })
            })
            .collect()
    }
}

/// Parse "ip:port" or "host:port" into (IpAddr, u16).
/// Returns None if parsing fails.
fn parse_addr(addr: &str) -> Option<(IpAddr, u16)> {
    if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        return Some((socket_addr.ip(), socket_addr.port()));
    }

    let mut addrs = addr.to_socket_addrs().ok()?;
    addrs
        .next()
        .map(|socket_addr| (socket_addr.ip(), socket_addr.port()))
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
    fn pair_cluster_state_populates_deterministic_tokens_for_peer() {
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
        assert!(
            !peers[0].tokens.is_empty(),
            "pair-mode peer rows must carry tokens so cdrs-tokio accepts them"
        );
        for tok in &peers[0].tokens {
            tok.parse::<i64>()
                .expect("pair-mode token strings must round-trip through i64");
        }
    }

    /// Pin the bug fix from
    /// `specs/in-process/bug-cql-auth-enabled-cluster-times-out-for-cdrs-clients.md`:
    /// `system.peers.tokens` MUST be non-empty for any peer that has tokens
    /// assigned in the ring. cdrs-tokio's `is_peer_row_valid` filters out
    /// peer rows whose `tokens` column is empty, leaving the session-build
    /// pool effectively single-node and causing intermittent hangs.
    #[test]
    fn raft_cluster_state_populates_tokens_from_ring_for_peers() {
        use crate::raft::{NodeInfo, NodeState};
        use crate::ring::TokenRing;

        let local_id = 1_u64;
        let peer_id = 2_u64;

        let mut ring = TokenRing::new();
        ring.add_node(
            local_id,
            NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "10.0.0.1:7000".to_string(),
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
                addr: "10.0.0.2:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19043".to_string()),
            },
        );
        // Assign a few tokens to the peer.
        ring.assign_tokens(peer_id, &[-1000, 0, 1000, i64::MAX]);

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));
        let state = RaftClusterState::new(ring_arc, local_id);
        let peers = state.peers();

        assert_eq!(peers.len(), 1);
        let peer = &peers[0];
        assert!(
            !peer.tokens.is_empty(),
            "peer.tokens must be non-empty so cdrs-tokio's is_peer_row_valid \
             accepts the row — empty tokens cause session-build hangs"
        );
        assert_eq!(peer.tokens.len(), 4, "expected all 4 ring tokens");
        // Token strings parse as i64 (Cassandra wire format for set<text>).
        for tok_str in &peer.tokens {
            tok_str
                .parse::<i64>()
                .expect("each token string must round-trip through i64");
        }
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
    fn raft_cluster_state_resolves_hostname_broadcast_for_native_address() {
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
                cql_broadcast: Some("localhost:19043".to_string()),
            },
        );

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));
        let state = RaftClusterState::new(ring_arc, local_id);
        let peers = state.peers();

        assert_eq!(peers.len(), 1);
        assert!(
            peers[0].native_address.is_loopback(),
            "hostname broadcast should resolve to a loopback IP, got {}",
            peers[0].native_address
        );
        assert_eq!(peers[0].native_port, 19043);
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

    #[tokio::test]
    async fn pair_cluster_state_uses_peer_manager_broadcast_for_native_address() {
        use ferrosa_net::config::NetConfig;
        use ferrosa_net::peer::PeerManager;

        struct NoopListener;
        impl ferrosa_net::peer::PeerEventListener for NoopListener {
            fn on_peer_connected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_disconnected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_suspected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_recovered(&self, _: uuid::Uuid) {}
            fn on_peer_failed(&self, _: uuid::Uuid) {}
        }

        let config = Arc::new(ClusterConfig::default());
        let peer_id = Uuid::new_v4();
        let peer_addr = "172.17.0.3:7000".parse().unwrap();
        let state = Arc::new(RwLock::new(PairState::new(
            PairRole::Primary,
            peer_id,
            peer_addr,
        )));
        let pm = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        pm.set_peer_cql_broadcast(peer_id, "127.0.0.1:19043".to_string())
            .await;

        let cluster_state = PairClusterState::with_peer_manager(config, state, pm);
        let peers = cluster_state.peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0].native_address,
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        assert_eq!(peers[0].native_port, 19043);
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
