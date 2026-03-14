//! Mode controller — manages runtime transitions between deployment modes.
//!
//! The controller implements [`PeerEventListener`] and swaps the active
//! [`WritePath`] and [`ClusterStateHolder`] atomically when the deployment mode
//! changes (standalone → pair → cluster).

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use uuid::Uuid;

use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_schema::system::peers::{ClusterState, PeerInfo};
use ferrosa_storage::engine::StorageEngine;

use crate::config::ClusterConfig;
use crate::mode::DeploymentMode;
use crate::pair::coordinator::PairCoordinator;
use crate::pair::{PairRole, PairState};
use crate::state::PairClusterState;
use crate::write_path::WritePath;

/// Swappable cluster state — enum dispatch to avoid trait object Sized issues.
pub enum ClusterStateHolder {
    Standalone,
    Pair(PairClusterState),
}

impl ClusterState for ClusterStateHolder {
    fn peers(&self) -> Vec<PeerInfo> {
        match self {
            Self::Standalone => vec![],
            Self::Pair(s) => s.peers(),
        }
    }
}

/// Manages deployment mode transitions at runtime.
///
/// Created at startup with standalone mode. When peers connect/disconnect,
/// transitions the mode and atomically swaps the write path and cluster state.
pub struct ModeController {
    mode: ArcSwap<DeploymentMode>,
    write_path: Arc<ArcSwap<WritePath>>,
    cluster_state: Arc<ArcSwap<ClusterStateHolder>>,
    storage: Arc<StorageEngine>,
    config: Arc<ClusterConfig>,
    #[allow(dead_code)]
    net_config: Arc<NetConfig>,
    local_host_id: Uuid,
    peer_manager: ArcSwap<Option<Arc<PeerManager>>>,
}

/// Handles returned from ModeController::new() for wiring into SharedState.
pub struct ModeControllerHandles {
    pub write_path: Arc<ArcSwap<WritePath>>,
    pub cluster_state: Arc<ArcSwap<ClusterStateHolder>>,
}

impl ModeController {
    /// Create a new ModeController in standalone mode.
    pub fn new(
        config: Arc<ClusterConfig>,
        net_config: Arc<NetConfig>,
        local_host_id: Uuid,
        storage: Arc<StorageEngine>,
    ) -> (Arc<Self>, ModeControllerHandles) {
        let write_path = Arc::new(ArcSwap::from_pointee(WritePath::direct(storage.clone())));
        let cluster_state = Arc::new(ArcSwap::from_pointee(ClusterStateHolder::Standalone));

        let controller = Arc::new(Self {
            mode: ArcSwap::from_pointee(DeploymentMode::Standalone),
            write_path: write_path.clone(),
            cluster_state: cluster_state.clone(),
            storage,
            config,
            net_config,
            local_host_id,
            peer_manager: ArcSwap::from_pointee(None),
        });

        let handles = ModeControllerHandles {
            write_path,
            cluster_state,
        };

        (controller, handles)
    }

    /// Set the peer manager reference. Must be called after PeerManager is created.
    pub fn set_peer_manager(&self, pm: Arc<PeerManager>) {
        self.peer_manager.store(Arc::new(Some(pm)));
    }

    /// Get current deployment mode.
    pub fn mode(&self) -> DeploymentMode {
        **self.mode.load()
    }

    fn transition_to_pair(&self, peer_host_id: Uuid, peer_addr: SocketAddr) {
        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to pair: peer_manager not set");
                return;
            }
        };

        let role = PairRole::elect(self.local_host_id, peer_host_id);
        let role_arc = Arc::new(ArcSwap::from_pointee(role));

        let coordinator = Arc::new(PairCoordinator::new(
            role_arc,
            peer_host_id,
            self.storage.clone(),
            peer_manager,
        ));

        self.write_path
            .store(Arc::new(WritePath::pair(coordinator)));

        let pair_state = Arc::new(tokio::sync::RwLock::new(PairState::new(
            role,
            peer_host_id,
            peer_addr,
        )));
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Pair(PairClusterState::new(
                self.config.clone(),
                pair_state,
            ))));

        self.mode.store(Arc::new(DeploymentMode::Pair));
        tracing::info!(
            %role,
            peer = %peer_host_id,
            "mode transition: standalone → pair"
        );
    }

    fn transition_to_standalone(&self) {
        self.write_path
            .store(Arc::new(WritePath::direct(self.storage.clone())));
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Standalone));
        self.mode.store(Arc::new(DeploymentMode::Standalone));
        tracing::warn!("mode transition: pair → standalone (peer lost)");
    }
}

impl PeerEventListener for ModeController {
    fn on_peer_connected(&self, peer: PeerId) {
        let (host_id, addr) = peer;
        tracing::info!(peer = %host_id, %addr, "peer connected");

        let current_mode = **self.mode.load();
        if current_mode == DeploymentMode::Standalone {
            self.transition_to_pair(host_id, addr);
        }
    }

    fn on_peer_disconnected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(peer = %host_id, "peer disconnected");

        let current_mode = **self.mode.load();
        if current_mode == DeploymentMode::Pair {
            self.transition_to_standalone();
        }
    }

    fn on_peer_suspected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(peer = %host_id, "peer suspected dead (not transitioning)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            data_dir: dir.to_path_buf(),
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    #[test]
    fn starts_in_standalone_mode() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let host_id = Uuid::new_v4();

        let (controller, _handles) = ModeController::new(config, net_config, host_id, storage);

        assert_eq!(controller.mode(), DeploymentMode::Standalone);
    }

    #[test]
    fn peer_connect_transitions_to_pair() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let (controller, _handles) =
            ModeController::new(config, net_config.clone(), local_id, storage);

        // Create a PeerManager and set it
        let pm = Arc::new(PeerManager::new(net_config, local_id, controller.clone()));
        controller.set_peer_manager(pm);

        // Simulate peer connection
        let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        controller.on_peer_connected((peer_id, peer_addr));

        assert_eq!(controller.mode(), DeploymentMode::Pair);
    }

    #[test]
    fn peer_disconnect_transitions_to_standalone() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let (controller, _handles) =
            ModeController::new(config, net_config.clone(), local_id, storage);

        let pm = Arc::new(PeerManager::new(net_config, local_id, controller.clone()));
        controller.set_peer_manager(pm);

        // Connect then disconnect
        let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        controller.on_peer_connected((peer_id, peer_addr));
        assert_eq!(controller.mode(), DeploymentMode::Pair);

        controller.on_peer_disconnected((peer_id, peer_addr));
        assert_eq!(controller.mode(), DeploymentMode::Standalone);
    }
}
