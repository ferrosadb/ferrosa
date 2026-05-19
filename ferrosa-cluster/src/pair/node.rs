use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::RwLock;
use uuid::Uuid;

use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::pool::PriorityPool;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_storage::engine::StorageEngine;

use crate::config::ClusterConfig;
use crate::error::Result;
use crate::pair::coordinator::PairCoordinator;
use crate::pair::handler::PairWriteForwardHandler;
use crate::pair::{PairRole, PairState};

/// Integration struct for pair mode.
///
/// Owns the coordinator, RPC handlers, peer manager, and RPC server.
/// Implements `PeerEventListener` for lifecycle events.
pub struct PairNode {
    #[allow(dead_code)] // Used in Phase 2 (DDL forwarding, mode transitions)
    pub(crate) config: Arc<ClusterConfig>,
    pub(crate) net_config: Arc<NetConfig>,
    pub(crate) local_host_id: Uuid,
    pub(crate) role: Arc<ArcSwap<PairRole>>,
    pub(crate) state: Arc<RwLock<PairState>>,
    pub(crate) coordinator: Arc<PairCoordinator>,
    pub(crate) peer_manager: Arc<PeerManager>,
    pub(crate) storage: Arc<StorageEngine>,
}

/// Listener that logs peer events. PairNode handles events through
/// a separate mechanism since PeerManager owns the listener.
pub struct PairEventListener {
    role: Arc<ArcSwap<PairRole>>,
    state: Arc<RwLock<PairState>>,
}

impl PairEventListener {
    pub fn new(role: Arc<ArcSwap<PairRole>>, state: Arc<RwLock<PairState>>) -> Self {
        Self { role, state }
    }
}

impl PeerEventListener for PairEventListener {
    fn on_peer_connected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::info!(
            role = %**self.role.load(),
            peer = %host_id,
            "peer connected"
        );
        let state = self.state.clone();
        tokio::spawn(async move {
            state.write().await.connected = true;
        });
    }

    fn on_peer_disconnected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(
            role = %**self.role.load(),
            peer = %host_id,
            "peer disconnected"
        );
        let state = self.state.clone();
        tokio::spawn(async move {
            state.write().await.connected = false;
        });
    }

    fn on_peer_suspected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(
            role = %**self.role.load(),
            peer = %host_id,
            "peer suspected dead"
        );
        let state = self.state.clone();
        tokio::spawn(async move {
            state.write().await.connected = false;
        });
    }

    fn on_peer_recovered(&self, peer_id: uuid::Uuid) {
        tracing::info!(
            role = %**self.role.load(),
            peer = %peer_id,
            "peer recovered"
        );
        // Hint delivery will be wired in Slice 3 (Task 14)
    }

    fn on_peer_failed(&self, peer_id: uuid::Uuid) {
        tracing::warn!(
            role = %**self.role.load(),
            peer = %peer_id,
            "peer failed — excluding from replica set"
        );
    }
}

impl PairNode {
    /// Create a new PairNode. Does not start networking — call `start()`.
    pub fn new(
        config: Arc<ClusterConfig>,
        net_config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_host_id: Uuid,
        peer_addr: SocketAddr,
        storage: Arc<StorageEngine>,
    ) -> Self {
        #[allow(deprecated)]
        let role = PairRole::elect(local_host_id, peer_host_id);
        let role_arc = Arc::new(ArcSwap::from_pointee(role));
        let state = Arc::new(RwLock::new(PairState::new(role, peer_host_id, peer_addr)));

        let listener = Arc::new(PairEventListener::new(role_arc.clone(), state.clone()));
        let peer_manager = Arc::new(PeerManager::new(
            net_config.clone(),
            local_host_id,
            listener,
        ));

        let coordinator = Arc::new(PairCoordinator::new(
            role_arc.clone(),
            peer_host_id,
            storage.clone(),
            peer_manager.clone(),
        ));

        Self {
            config,
            net_config,
            local_host_id,
            role: role_arc,
            state,
            coordinator,
            peer_manager,
            storage,
        }
    }

    /// Build the handler registry with pair mode RPC handlers.
    pub fn build_registry(&self) -> HandlerRegistry {
        let registry = HandlerRegistry::new();
        let handler = Arc::new(PairWriteForwardHandler::new(
            self.role.clone(),
            self.coordinator.clone(),
        ));
        registry.register(MsgType::PairWriteForward, handler);

        let catchup_handler = Arc::new(crate::pair::catchup::PairCatchUpHandler::new(
            self.storage.clone(),
        ));
        registry.register(MsgType::PairCatchUp, catchup_handler);

        let role_swap_handler = Arc::new(crate::pair::switchover::RoleSwapHandler::new(
            self.local_host_id,
            self.role.clone(),
        ));
        registry.register(MsgType::RoleSwap, role_swap_handler);

        registry
    }

    /// Start the RPC server and connect to peer.
    /// Returns the bound address of this node's RPC server.
    pub async fn start(&self) -> Result<SocketAddr> {
        let registry = Arc::new(self.build_registry());
        let server = Arc::new(RpcServer::new(
            (*self.net_config).clone(),
            self.local_host_id,
            registry,
        ));
        let addr = server
            .start_and_get_addr()
            .await
            .map_err(crate::error::ClusterError::Net)?;

        // Try to connect to peer — log and continue if peer isn't up yet.
        let peer_addr = self.state.read().await.peer_addr;
        let peer_host_id = self.state.read().await.peer_host_id;
        match PriorityPool::connect(
            self.net_config.clone(),
            self.local_host_id,
            &peer_addr.to_string(),
            None,
            None,
        )
        .await
        {
            Ok(pool) => {
                self.peer_manager
                    .add_peer((peer_host_id, peer_addr), pool)
                    .await;
                self.state.write().await.connected = true;
                tracing::info!(
                    role = %self.role(),
                    peer = %peer_host_id,
                    addr = %addr,
                    "pair node started, peer connected"
                );
            }
            Err(e) => {
                tracing::warn!(
                    role = %self.role(),
                    peer = %peer_host_id,
                    error = %e,
                    addr = %addr,
                    "pair node started, peer connection deferred"
                );
            }
        }

        Ok(addr)
    }

    /// Connect (or reconnect) to the peer. Call after peer is known to be up.
    pub async fn connect_to_peer(&self, peer_addr: SocketAddr) -> Result<()> {
        let pool = PriorityPool::connect(
            self.net_config.clone(),
            self.local_host_id,
            &peer_addr.to_string(),
            None,
            None,
        )
        .await
        .map_err(crate::error::ClusterError::Net)?;

        let peer_host_id = self.state.read().await.peer_host_id;
        self.peer_manager
            .add_peer((peer_host_id, peer_addr), pool)
            .await;
        self.state.write().await.connected = true;
        Ok(())
    }

    /// Get current role.
    pub fn role(&self) -> PairRole {
        **self.role.load()
    }

    /// Get the coordinator for write coordination.
    pub fn coordinator(&self) -> &Arc<PairCoordinator> {
        &self.coordinator
    }

    /// Initiate a switchover: swap primary and secondary roles.
    /// Must be called on the current primary.
    pub async fn switchover(&self) -> Result<()> {
        let peer_host_id = self.state.read().await.peer_host_id;
        crate::pair::switchover::initiate_switchover(
            &self.peer_manager,
            self.local_host_id,
            peer_host_id,
            &self.role,
        )
        .await
    }

    /// Check if peer is connected.
    pub async fn is_peer_connected(&self) -> bool {
        self.state.read().await.connected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_storage_config(dir: &std::path::Path) -> ferrosa_storage::StorageEngineConfig {
        use ferrosa_storage::CommitLogConfig;
        use ferrosa_storage::CompactionConfig;
        ferrosa_storage::StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        }
    }

    #[test]
    fn pair_node_elects_role_on_creation() {
        let high = Uuid::from_bytes([0xFF; 16]);
        let low = Uuid::from_bytes([0x00; 16]);

        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_config = test_storage_config(storage_dir.path());
        let storage = Arc::new(StorageEngine::new(storage_config, None).unwrap());

        let node = PairNode::new(
            config,
            net_config,
            high,
            low,
            "127.0.0.1:7000".parse().unwrap(),
            storage,
        );
        assert_eq!(node.role(), PairRole::Primary);
    }
}
