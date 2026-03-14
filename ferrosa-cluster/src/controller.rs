//! Mode controller — manages runtime transitions between deployment modes.
//!
//! The controller implements [`PeerEventListener`] and swaps the active
//! [`WritePath`] and [`ClusterStateHolder`] atomically when the deployment mode
//! changes (standalone → pair → cluster).
//!
//! Failover lifecycle:
//!   1. Pair mode active, both nodes connected
//!   2. Peer disconnects → writes become unavailable (degraded)
//!   3. Operator calls `force_promote()` → standalone with direct writes
//!   4. Peer reconnects → auto re-pair, promoted node stays primary
//!   5. Operator can `switchover()` to swap roles

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use uuid::Uuid;

use bytes::Bytes;
use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::pool::PriorityPool;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::{HandlerRegistry, InboundPeerCallback};
use ferrosa_schema::system::peers::{ClusterState, PeerInfo};
use ferrosa_schema::Schema;
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::CommitLogPosition;

use crate::config::ClusterConfig;
use crate::ddl_path::DdlPath;
use crate::error::{ClusterError, Result};
use crate::mode::DeploymentMode;
use crate::pair::coordinator::{encode_mutation, PairCoordinator};
use crate::pair::ddl::{DdlCoordinator, PairDdlForwardHandler, PairSchemaSyncHandler};
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

/// Pair mode context stored across transitions.
struct PairContext {
    role: Arc<ArcSwap<PairRole>>,
    peer_host_id: Uuid,
    #[allow(dead_code)]
    peer_addr: SocketAddr,
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
    schema: Arc<Schema>,
    ddl_path: Arc<ArcSwap<DdlPath>>,
    config: Arc<ClusterConfig>,
    net_config: Arc<NetConfig>,
    local_host_id: Uuid,
    peer_manager: ArcSwap<Option<Arc<PeerManager>>>,
    registry: Arc<HandlerRegistry>,
    /// Stored during pair mode for switchover/promote operations.
    pair_context: Mutex<Option<PairContext>>,
    /// Set by force_promote — overrides UUID election on next pair transition.
    force_promoted: AtomicBool,
}

/// Handles returned from ModeController::new() for wiring into SharedState.
pub struct ModeControllerHandles {
    pub write_path: Arc<ArcSwap<WritePath>>,
    pub cluster_state: Arc<ArcSwap<ClusterStateHolder>>,
    pub ddl_path: Arc<ArcSwap<DdlPath>>,
}

impl ModeController {
    /// Create a new ModeController in standalone mode.
    pub fn new(
        config: Arc<ClusterConfig>,
        net_config: Arc<NetConfig>,
        local_host_id: Uuid,
        storage: Arc<StorageEngine>,
        schema: Arc<Schema>,
        registry: Arc<HandlerRegistry>,
    ) -> (Arc<Self>, ModeControllerHandles) {
        let write_path = Arc::new(ArcSwap::from_pointee(WritePath::direct(storage.clone())));
        let cluster_state = Arc::new(ArcSwap::from_pointee(ClusterStateHolder::Standalone));
        let ddl_path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
            schema: schema.clone(),
            engine: storage.clone(),
        }));

        let controller = Arc::new(Self {
            mode: ArcSwap::from_pointee(DeploymentMode::Standalone),
            write_path: write_path.clone(),
            cluster_state: cluster_state.clone(),
            storage,
            schema,
            ddl_path: ddl_path.clone(),
            config,
            net_config,
            local_host_id,
            peer_manager: ArcSwap::from_pointee(None),
            registry,
            pair_context: Mutex::new(None),
            force_promoted: AtomicBool::new(false),
        });

        let handles = ModeControllerHandles {
            write_path,
            cluster_state,
            ddl_path,
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

    /// Get current pair role, if in pair mode.
    pub fn role(&self) -> Option<PairRole> {
        let ctx = self.pair_context.lock().unwrap();
        ctx.as_ref().map(|c| **c.role.load())
    }

    /// Get local host_id.
    pub fn host_id(&self) -> Uuid {
        self.local_host_id
    }

    /// Force-promote this node to standalone primary.
    ///
    /// Use when the peer is unreachable and the operator wants to resume writes.
    /// Subsequent peer reconnection will auto re-pair with this node as primary.
    pub fn force_promote(&self) -> Result<()> {
        self.write_path
            .store(Arc::new(WritePath::direct(self.storage.clone())));
        self.ddl_path.store(Arc::new(DdlPath::Direct {
            schema: self.schema.clone(),
            engine: self.storage.clone(),
        }));
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Standalone));
        self.mode.store(Arc::new(DeploymentMode::Standalone));
        self.force_promoted.store(true, Ordering::Release);
        *self.pair_context.lock().unwrap() = None;
        tracing::info!("force promoted to standalone primary");
        Ok(())
    }

    /// Initiate switchover: swap primary/secondary roles.
    ///
    /// Must be called on the current primary. Both nodes must be connected.
    pub async fn switchover(&self) -> Result<()> {
        let (role_arc, peer_host_id) = {
            let ctx = self.pair_context.lock().unwrap();
            let ctx = ctx
                .as_ref()
                .ok_or(ClusterError::Internal("not in pair mode".into()))?;
            (ctx.role.clone(), ctx.peer_host_id)
        };

        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                return Err(ClusterError::Internal("peer_manager not set".into()));
            }
        };

        crate::pair::switchover::initiate_switchover(
            &peer_manager,
            self.local_host_id,
            peer_host_id,
            &role_arc,
        )
        .await
    }

    fn transition_to_pair(&self, peer_host_id: Uuid, peer_addr: SocketAddr, need_reverse: bool) {
        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to pair: peer_manager not set");
                return;
            }
        };

        // If this node was force-promoted, override UUID election — stay primary.
        let was_promoted = self.force_promoted.swap(false, Ordering::AcqRel);
        let role = if was_promoted {
            PairRole::Primary
        } else {
            PairRole::elect(self.local_host_id, peer_host_id)
        };
        let role_arc = Arc::new(ArcSwap::from_pointee(role));

        let coordinator = Arc::new(PairCoordinator::new(
            role_arc.clone(),
            peer_host_id,
            self.storage.clone(),
            peer_manager.clone(),
        ));

        // Register pair mode RPC handlers dynamically
        let write_fwd_handler = Arc::new(crate::pair::handler::PairWriteForwardHandler::new(
            role_arc.clone(),
            coordinator.clone(),
        ));
        self.registry
            .register(MsgType::PairWriteForward, write_fwd_handler);

        let role_swap_handler = Arc::new(crate::pair::switchover::RoleSwapHandler::new(
            self.local_host_id,
            role_arc.clone(),
        ));
        self.registry.register(MsgType::RoleSwap, role_swap_handler);

        // DDL coordination
        let ddl_coordinator = Arc::new(DdlCoordinator::new(
            role_arc.clone(),
            peer_host_id,
            self.schema.clone(),
            self.storage.clone(),
            peer_manager.clone(),
        ));

        let ddl_fwd_handler = Arc::new(PairDdlForwardHandler::new(
            role_arc.clone(),
            ddl_coordinator.clone(),
        ));
        self.registry
            .register(MsgType::PairDdlForward, ddl_fwd_handler);

        let schema_sync_handler = Arc::new(PairSchemaSyncHandler::new(
            self.schema.clone(),
            self.storage.clone(),
        ));
        self.registry
            .register(MsgType::PairSchemaSync, schema_sync_handler);

        self.ddl_path
            .store(Arc::new(DdlPath::Pair(ddl_coordinator)));

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

        // Store pair context for switchover/promote
        *self.pair_context.lock().unwrap() = Some(PairContext {
            role: role_arc,
            peer_host_id,
            peer_addr,
        });

        self.mode.store(Arc::new(DeploymentMode::Pair));
        tracing::info!(
            %role,
            peer = %peer_host_id,
            promoted = was_promoted,
            "mode transition: standalone → pair"
        );

        // When triggered by an inbound peer connection, our peer_manager doesn't
        // have the peer registered — create a reverse outbound pool for RPC sends.
        if need_reverse {
            let pm = peer_manager.clone();
            let net_cfg = self.net_config.clone();
            let local_id = self.local_host_id;
            let internode_port = self.net_config.bind_addr.port();
            let reverse_addr = SocketAddr::new(peer_addr.ip(), internode_port);
            tokio::spawn(async move {
                // Small delay to let peer's RPC server be ready.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                match PriorityPool::connect(net_cfg, local_id, reverse_addr).await {
                    Ok(pool) => {
                        pm.add_peer((peer_host_id, reverse_addr), pool).await;
                        tracing::info!(%peer_host_id, %reverse_addr, "reverse connection established");
                    }
                    Err(e) => {
                        tracing::warn!(%e, "reverse connection to peer failed");
                    }
                }
            });
        }

        // After force-promoted re-pairing, correct the peer's role and replay data.
        if was_promoted {
            let local_id = self.local_host_id;
            let pm = peer_manager;
            let storage = self.storage.clone();
            let schema = self.schema.clone();
            tokio::spawn(async move {
                // Wait for reverse connection + peer pair transition + handler registration.
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;

                // Tell peer to become secondary.
                match pm
                    .send(
                        peer_host_id,
                        Message::RoleSwap {
                            new_primary: local_id,
                            new_secondary: peer_host_id,
                        },
                        Lane::Raft,
                    )
                    .await
                {
                    Ok(_) => tracing::info!("sent role correction to rejoined peer"),
                    Err(e) => {
                        tracing::warn!(%e, "failed to send role correction to peer");
                        return;
                    }
                }

                // Send schema snapshot before mutation replay.
                let snap = schema.snapshot();
                match serde_json::to_vec(&*snap) {
                    Ok(json) => {
                        match pm
                            .send(
                                peer_host_id,
                                Message::PairSchemaSync(Bytes::from(json)),
                                Lane::Bulk,
                            )
                            .await
                        {
                            Ok(_) => tracing::info!("schema snapshot sent to rejoined peer"),
                            Err(e) => tracing::warn!(%e, "failed to send schema snapshot"),
                        }
                    }
                    Err(e) => tracing::warn!(%e, "failed to serialize schema snapshot"),
                }

                // Replay recent data to bring peer up to date.
                let position = CommitLogPosition {
                    segment_id: 0,
                    offset: 0,
                };
                match storage.replay_from(position) {
                    Ok(mutations) if !mutations.is_empty() => {
                        tracing::info!(count = mutations.len(), "replaying data to rejoined peer");
                        for mutation in &mutations {
                            let body = encode_mutation(mutation);
                            if let Err(e) = pm
                                .send(peer_host_id, Message::PairWriteForward(body), Lane::Data)
                                .await
                            {
                                tracing::warn!(%e, "catch-up replay send failed");
                                break;
                            }
                        }
                        tracing::info!("catch-up replay complete");
                    }
                    Ok(_) => tracing::info!("no data to replay for catch-up"),
                    Err(e) => tracing::warn!(%e, "catch-up replay_from failed"),
                }
            });
        }
    }

    /// Transition to degraded state: writes unavailable, reads still work.
    fn transition_to_degraded(&self) {
        self.write_path.store(Arc::new(WritePath::unavailable()));
        self.ddl_path.store(Arc::new(DdlPath::Unavailable));
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Standalone));
        self.mode.store(Arc::new(DeploymentMode::Standalone));
        *self.pair_context.lock().unwrap() = None;
        tracing::warn!("mode transition: pair → degraded (peer lost, writes unavailable)");
    }
}

impl PeerEventListener for ModeController {
    fn on_peer_connected(&self, peer: PeerId) {
        let (host_id, addr) = peer;
        tracing::info!(peer = %host_id, %addr, "peer connected");

        let current_mode = **self.mode.load();
        if current_mode == DeploymentMode::Standalone {
            // Outbound connection — we already have a pool, no reverse needed.
            self.transition_to_pair(host_id, addr, false);
        }
    }

    fn on_peer_disconnected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(peer = %host_id, "peer disconnected");

        let current_mode = **self.mode.load();
        if current_mode == DeploymentMode::Pair {
            self.transition_to_degraded();
        }
    }

    fn on_peer_suspected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(peer = %host_id, "peer suspected dead (not transitioning)");
    }
}

impl InboundPeerCallback for ModeController {
    fn on_inbound_peer(&self, peer_id: PeerId) {
        let (host_id, addr) = peer_id;
        tracing::info!(peer = %host_id, %addr, "inbound peer connected");

        let current_mode = **self.mode.load();
        if current_mode == DeploymentMode::Standalone {
            // Inbound connection — we need a reverse outbound pool for sends.
            self.transition_to_pair(host_id, addr, true);
        }
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

    fn test_schema() -> Arc<Schema> {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
            PasswordPolicy, RateLimitConfig, SchemaConfig,
        };
        let config = SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: SchemaDeploymentMode::Development,
        };
        Arc::new(Schema::new(config).unwrap())
    }

    #[test]
    fn starts_in_standalone_mode() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let schema = test_schema();
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let host_id = Uuid::new_v4();

        let registry = Arc::new(HandlerRegistry::new());
        let (controller, _handles) =
            ModeController::new(config, net_config, host_id, storage, schema, registry);

        assert_eq!(controller.mode(), DeploymentMode::Standalone);
    }

    #[test]
    fn peer_connect_transitions_to_pair() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let schema = test_schema();
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let registry = Arc::new(HandlerRegistry::new());
        let (controller, _handles) = ModeController::new(
            config,
            net_config.clone(),
            local_id,
            storage,
            schema,
            registry,
        );

        // Create a PeerManager and set it
        let pm = Arc::new(PeerManager::new(net_config, local_id, controller.clone()));
        controller.set_peer_manager(pm);

        // Simulate peer connection
        let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        controller.on_peer_connected((peer_id, peer_addr));

        assert_eq!(controller.mode(), DeploymentMode::Pair);
    }

    #[test]
    fn peer_disconnect_transitions_to_degraded() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let schema = test_schema();
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();

        let registry = Arc::new(HandlerRegistry::new());
        let (controller, _handles) = ModeController::new(
            config,
            net_config.clone(),
            local_id,
            storage,
            schema,
            registry,
        );

        let pm = Arc::new(PeerManager::new(net_config, local_id, controller.clone()));
        controller.set_peer_manager(pm);

        // Connect then disconnect
        let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        controller.on_peer_connected((peer_id, peer_addr));
        assert_eq!(controller.mode(), DeploymentMode::Pair);

        controller.on_peer_disconnected((peer_id, peer_addr));
        // Degraded transitions to standalone mode with unavailable writes
        assert_eq!(controller.mode(), DeploymentMode::Standalone);
    }

    #[test]
    fn force_promote_sets_direct_write_path() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let schema = test_schema();
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();

        let registry = Arc::new(HandlerRegistry::new());
        let (controller, _handles) =
            ModeController::new(config, net_config, local_id, storage, schema, registry);

        controller.force_promote().unwrap();
        assert_eq!(controller.mode(), DeploymentMode::Standalone);
        assert!(controller.force_promoted.load(Ordering::Acquire));
    }
}
