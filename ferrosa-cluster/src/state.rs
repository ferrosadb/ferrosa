use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::RwLock;

use ferrosa_schema::system::peers::{ClusterState, PeerInfo};

use crate::addr_cache::NonBlockingAddrCache;
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
    /// Resolves the peer's CQL broadcast without blocking the CQL runtime on DNS.
    addr_cache: NonBlockingAddrCache,
}

impl PairClusterState {
    pub fn new(config: Arc<ClusterConfig>, state: Arc<RwLock<PairState>>) -> Self {
        Self {
            config,
            state,
            broadcast_resolver: None,
            cached_peers: ArcSwap::from_pointee(Vec::new()),
            addr_cache: NonBlockingAddrCache::default(),
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
            addr_cache: NonBlockingAddrCache::default(),
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
        let (native_addr, native_port) = peer_broadcast
            .and_then(|broadcast| self.addr_cache.get(&broadcast))
            .map_or((ip, 9042), |a| (a.ip(), a.port()));
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

    /// The internode socket address this node last dialled `host_id` at --
    /// kept after the peer goes down. Non-blocking; `None` if unknown or the
    /// lock is contended.
    fn known_internode_addr(&self, _host_id: uuid::Uuid) -> Option<SocketAddr> {
        None
    }
}

/// PeerManager implements BroadcastResolver via its broadcast map.
impl BroadcastResolver for ferrosa_net::peer::PeerManager {
    fn resolve_broadcast(&self, host_id: uuid::Uuid) -> Option<String> {
        self.get_peer_cql_broadcast_sync(host_id)
    }

    fn known_internode_addr(&self, host_id: uuid::Uuid) -> Option<SocketAddr> {
        self.peer_socket_addr_sync(host_id)
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
    /// Resolves ring hostnames (`node3:7000`) without blocking the CQL
    /// runtime on DNS; see [`crate::addr_cache`].
    addr_cache: NonBlockingAddrCache,
}

impl RaftClusterState {
    pub fn new(ring: Arc<ArcSwap<TokenRing>>, local_node_id: u64) -> Self {
        Self {
            ring,
            local_node_id,
            broadcast_resolver: None,
            addr_cache: NonBlockingAddrCache::default(),
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
            addr_cache: NonBlockingAddrCache::default(),
        }
    }

    /// Replace the hostname resolver (tests inject a controlled lookup).
    pub fn with_addr_cache(mut self, addr_cache: NonBlockingAddrCache) -> Self {
        self.addr_cache = addr_cache;
        self
    }

    /// The peer's internode `(ip, port)`: its ring address when that resolves
    /// (from cache, never blocking), else the address this node last dialled
    /// it at. A down peer whose hostname no longer resolves keeps its row.
    fn internode_endpoint(&self, info: &crate::raft::NodeInfo) -> Option<(IpAddr, u16)> {
        if let Some(addr) = self.addr_cache.get(&info.addr) {
            return Some((addr.ip(), addr.port()));
        }
        let known = self
            .broadcast_resolver
            .as_ref()?
            .known_internode_addr(info.host_id)?;
        let ring_port = info
            .addr
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok());
        Some((known.ip(), ring_port.unwrap_or(known.port())))
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
                let Some((ip, port)) = self.internode_endpoint(info) else {
                    tracing::warn!(
                        node_id = id,
                        host_id = %info.host_id,
                        addr = %info.addr,
                        "ring member omitted from system.peers: its address has not resolved and no dialled address is known"
                    );
                    return None;
                };
                // Use cql_broadcast for native_address (host-reachable address
                // for port-mapped container clusters). Check three sources:
                // 1. NodeInfo.cql_broadcast (set during ring construction for local node)
                // 2. PeerManager broadcast (exchanged during internode handshake)
                // 3. Fall back to internode IP with port 9042
                let peer_broadcast = info.cql_broadcast.clone().or_else(|| {
                    let resolver = self.broadcast_resolver.as_ref()?;
                    resolver.resolve_broadcast(info.host_id)
                });
                let (native_addr, native_port) = peer_broadcast
                    .and_then(|broadcast| self.addr_cache.get(&broadcast))
                    .map_or((ip, 9042), |a| (a.ip(), a.port()));
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

    fn noop_peer_manager() -> Arc<ferrosa_net::peer::PeerManager> {
        struct NoopListener;
        impl ferrosa_net::peer::PeerEventListener for NoopListener {
            fn on_peer_connected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_disconnected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_suspected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_recovered(&self, _: uuid::Uuid) {}
            fn on_peer_failed(&self, _: uuid::Uuid) {}
        }
        Arc::new(ferrosa_net::peer::PeerManager::new(
            Arc::new(ferrosa_net::config::NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ))
    }

    /// A two-node ring: the local node at an IP, the peer at `peer_addr`
    /// (a hostname, as `FERROSA_INTERNODE_BROADCAST` advertises it).
    fn ring_with_hostname_peer(peer_addr: &str) -> (Arc<ArcSwap<TokenRing>>, u64, Uuid) {
        use crate::raft::{NodeInfo, NodeState};
        let (local_id, peer_id, peer_host_id) = (1_u64, 2_u64, Uuid::new_v4());
        let node = |host_id: Uuid, addr: &str| NodeInfo {
            host_id,
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };
        let mut ring = TokenRing::new();
        ring.add_node(local_id, node(Uuid::new_v4(), "172.18.0.3:7000"));
        ring.add_node(peer_id, node(peer_host_id, peer_addr));
        ring.assign_tokens(peer_id, &[0]);
        (
            Arc::new(ArcSwap::from_pointee(ring)),
            local_id,
            peer_host_id,
        )
    }

    /// Nightly docker smoke, Phase 8: once node3's container is stopped its
    /// name stops resolving, and `system.peers` dropped node3 -- the ring still
    /// holds it and this node still knows the address it dialled it at. A down
    /// peer is still a ring member; Cassandra lists it with its last address.
    #[tokio::test]
    async fn raft_peers_lists_down_peer_whose_hostname_no_longer_resolves() {
        let (ring, local_id, peer_host_id) =
            ring_with_hostname_peer("ferrosa-stopped-peer.invalid:7000");
        let pm = noop_peer_manager();
        pm.add_peer_entry((peer_host_id, "172.18.0.5:7000".parse().unwrap()))
            .await;

        let peers = RaftClusterState::with_peer_manager(ring, local_id, pm).peers();

        assert_eq!(
            peers.len(),
            1,
            "a down ring member must stay in system.peers"
        );
        assert_eq!(peers[0].host_id, peer_host_id);
        assert_eq!(peers[0].peer, "172.18.0.5".parse::<IpAddr>().unwrap());
        assert_eq!(peers[0].peer_port, 7000);
    }

    /// The CQL runtime stall itself: on the CI runner `getaddrinfo("node3")`
    /// for the stopped container takes 5 s (node logs: FERROSA_SEED node3:7000
    /// "not usable yet" exactly 5.00 s after startup), and `system.peers` ran it
    /// on the CQL runtime for every connecting driver. `peers()` must answer
    /// within a bounded wait however long the resolver hangs.
    #[tokio::test]
    async fn raft_peers_does_not_wait_out_a_hanging_resolver() {
        use crate::addr_cache::{DEFAULT_FIRST_RESOLVE_WAIT, DEFAULT_REFRESH_AFTER};
        use std::time::{Duration, Instant};

        let (ring, local_id, peer_host_id) = ring_with_hostname_peer("node3:7000");
        let pm = noop_peer_manager();
        pm.add_peer_entry((peer_host_id, "172.18.0.5:7000".parse().unwrap()))
            .await;
        let hanging_dns = crate::addr_cache::NonBlockingAddrCache::with_lookup(
            Arc::new(|_: &str| {
                std::thread::sleep(Duration::from_secs(5));
                Err(std::io::Error::other(
                    "Temporary failure in name resolution",
                ))
            }),
            DEFAULT_REFRESH_AFTER,
            DEFAULT_FIRST_RESOLVE_WAIT,
        );
        let state =
            RaftClusterState::with_peer_manager(ring, local_id, pm).with_addr_cache(hanging_dns);

        let started = Instant::now();
        let first = state.peers();
        let second = state.peers();
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "system.peers blocked {elapsed:?} on a hanging resolver"
        );
        for peers in [first, second] {
            assert_eq!(peers.len(), 1);
            assert_eq!(peers[0].peer, "172.18.0.5".parse::<IpAddr>().unwrap());
        }
    }
}
