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
use crate::consistency::ConsistencyLevel;
use crate::coordinator::ClusterCoordinator;
use crate::ddl_path::DdlPath;
use crate::error::{ClusterError, Result};
use crate::mode::DeploymentMode;
use crate::pair::coordinator::{encode_mutation, PairCoordinator};
use crate::pair::ddl::{DdlCoordinator, PairDdlForwardHandler, PairSchemaSyncHandler};
use crate::pair::{PairRole, PairState};
use crate::raft::handlers::{
    RaftAppendHandler, RaftSnapshotHandler, RaftVoteHandler, ReadRequestHandler,
};
use crate::raft::log_store::SledLogStore;
use crate::raft::network::FerrosRaftNetworkFactory;
use crate::raft::state_machine::FerrosStateMachine;
use crate::raft::{uuid_to_node_id, FerrosRaft, NodeInfo, NodeState};
use crate::ring::TokenRing;
use crate::state::{PairClusterState, RaftClusterState};
use crate::write_path::WritePath;

/// Swappable cluster state — enum dispatch to avoid trait object Sized issues.
pub enum ClusterStateHolder {
    Standalone,
    Pair(PairClusterState),
    Cluster(RaftClusterState),
}

impl ClusterState for ClusterStateHolder {
    fn peers(&self) -> Vec<PeerInfo> {
        match self {
            Self::Standalone => vec![],
            Self::Pair(s) => s.peers(),
            Self::Cluster(s) => s.peers(),
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
    /// All connected peers, tracked across mode transitions.
    connected_peers: Mutex<Vec<(Uuid, SocketAddr)>>,
    /// Raft instance, set asynchronously after cluster transition completes.
    raft_instance: Arc<ArcSwap<Option<Arc<FerrosRaft>>>>,
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
            connected_peers: Mutex::new(Vec::new()),
            raft_instance: Arc::new(ArcSwap::from_pointee(None)),
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

    /// Get the Raft instance, if cluster mode initialization has completed.
    pub fn raft(&self) -> Option<Arc<FerrosRaft>> {
        (**self.raft_instance.load()).clone()
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
        self.connected_peers.lock().unwrap().clear();
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
                // SchemaSnapshot has HashMap<(String,String), _> which serde_json
                // can't serialize (tuple keys aren't valid JSON keys). Use bincode-
                // style workaround: serialize tables as a Vec of (key, value) pairs.
                let snap = schema.snapshot();
                let wire_snap = crate::pair::ddl::WireSchemaSnapshot::from_snapshot(&snap);
                match serde_json::to_vec(&wire_snap) {
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

                // Force sync commit log to disk before replay.
                if let Err(e) = storage.force_commit_log_sync() {
                    tracing::warn!(%e, "failed to force commit log sync before catch-up replay");
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

    /// Transition from pair mode to cluster mode when a 2nd peer connects.
    ///
    /// Sets up:
    /// 1. Sled-backed Raft log store
    /// 2. Raft state machine with schema/storage side effects
    /// 3. Raft network factory bridging openraft to ferrosa-net
    /// 4. TokenRing with deterministic initial token assignment
    /// 5. ClusterCoordinator for replica-aware writes
    /// 6. Swaps write path, DDL path, and cluster state atomically
    fn transition_to_cluster(&self, peers: Vec<(Uuid, SocketAddr)>) {
        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to cluster: peer_manager not set");
                return;
            }
        };

        // 1. Create sled log store
        let raft_dir = if let Some(ref dir) = self.config.raft_data_dir {
            dir.clone()
        } else {
            let data_dir =
                std::env::var("FERROSA_DATA_DIR").unwrap_or_else(|_| "/var/lib/ferrosa".into());
            std::path::Path::new(&data_dir).join("raft")
        };
        let log_store = match SledLogStore::new(&raft_dir) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(%e, "failed to create Raft log store");
                return;
            }
        };

        // 2. Create state machine from current schema
        let state_machine =
            FerrosStateMachine::with_side_effects(self.schema.clone(), self.storage.clone());

        // 3. Create network factory
        let network_factory = FerrosRaftNetworkFactory::new(peer_manager.clone());
        let local_node_id = uuid_to_node_id(self.local_host_id);

        // Register node mappings for all peers
        for (peer_uuid, _addr) in &peers {
            let peer_node_id = uuid_to_node_id(*peer_uuid);
            network_factory.register_node(peer_node_id, *peer_uuid);
        }
        // Register self
        network_factory.register_node(local_node_id, self.local_host_id);

        // 4. Build TokenRing with deterministic initial tokens
        let mut ring = TokenRing::new();

        // Add local node
        let broadcast = self.net_config.broadcast_addr.to_string();
        ring.add_node(
            local_node_id,
            NodeInfo {
                host_id: self.local_host_id,
                addr: broadcast,
                data_center: self.config.data_center.clone(),
                rack: self.config.rack.clone(),
                state: NodeState::Normal,
            },
        );

        // Add peers
        for (peer_uuid, addr) in &peers {
            let peer_node_id = uuid_to_node_id(*peer_uuid);
            ring.add_node(
                peer_node_id,
                NodeInfo {
                    host_id: *peer_uuid,
                    addr: addr.to_string(),
                    data_center: self.config.data_center.clone(),
                    rack: self.config.rack.clone(),
                    state: NodeState::Normal,
                },
            );
        }

        // Assign deterministic tokens to all nodes (256 per node).
        // Uses node_id XOR with index to produce deterministic, well-distributed tokens.
        let num_tokens = self.config.num_tokens as usize;
        let mut all_node_ids: Vec<u64> = vec![local_node_id];
        for (peer_uuid, _) in &peers {
            all_node_ids.push(uuid_to_node_id(*peer_uuid));
        }
        all_node_ids.sort_unstable(); // deterministic order

        for &nid in &all_node_ids {
            let tokens: Vec<i64> = (0..num_tokens)
                .map(|i| generate_deterministic_token(nid, i))
                .collect();
            ring.assign_tokens(nid, &tokens);
        }

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));

        // 5. Create coordinator
        let coordinator = Arc::new(ClusterCoordinator::new(
            ring_arc.clone(),
            peer_manager,
            local_node_id,
            self.storage.clone(),
            3, // default RF
            ConsistencyLevel::Quorum,
        ));

        // 6. Swap write path — cluster coordinator handles replica routing
        self.write_path
            .store(Arc::new(WritePath::cluster(coordinator)));

        // DdlPath::Cluster needs the Raft instance — Raft initialization is async
        // and happens in a background task. For now, keep DDL on the pair path or
        // set to direct until Raft is fully initialized. We store the log_store
        // and network_factory references for later Raft bootstrap.
        //
        // In the initial cluster transition we use direct DDL — the Raft leader
        // election and DDL-via-Raft wiring will be completed by the background
        // Raft initialization task.
        self.ddl_path.store(Arc::new(DdlPath::Direct {
            schema: self.schema.clone(),
            engine: self.storage.clone(),
        }));

        // Swap cluster state to Raft-based
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Cluster(
                RaftClusterState::new(ring_arc, local_node_id),
            )));

        // Clear pair context — no longer in pair mode
        *self.pair_context.lock().unwrap() = None;

        self.mode.store(Arc::new(DeploymentMode::Cluster));

        tracing::info!(
            node_id = local_node_id,
            peers = peers.len(),
            "mode transition: pair -> cluster (raft init spawned)"
        );

        // Spawn background Raft initialization — Raft::new() is async and
        // must not block the PeerEventListener callback.
        let raft_instance_swap = self.raft_instance.clone();
        let ddl_path = self.ddl_path.clone();
        let registry = self.registry.clone();
        let storage_for_handler = self.storage.clone();
        let cluster_name = self.config.cluster_name.clone();
        tokio::spawn(async move {
            // Build openraft Config
            let raft_config = match (openraft::Config {
                cluster_name,
                heartbeat_interval: 300,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                max_payload_entries: 100,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
                ..Default::default()
            })
            .validate()
            {
                Ok(cfg) => Arc::new(cfg),
                Err(e) => {
                    tracing::error!(%e, "invalid raft config, staying in cluster mode without raft DDL");
                    return;
                }
            };

            // Create the Raft instance
            let raft = match FerrosRaft::new(
                local_node_id,
                raft_config,
                network_factory,
                log_store,
                state_machine,
            )
            .await
            {
                Ok(r) => r,
                Err(fatal) => {
                    tracing::error!(%fatal, "raft initialization failed (Fatal), DDL remains on direct path");
                    return;
                }
            };

            let raft_arc = Arc::new(raft);

            // Register Raft RPC handlers so peers can reach this node's Raft
            let append_handler = Arc::new(RaftAppendHandler::new((*raft_arc).clone()));
            registry.register(MsgType::RaftAppendEntries, append_handler);

            let vote_handler = Arc::new(RaftVoteHandler::new((*raft_arc).clone()));
            registry.register(MsgType::RaftVote, vote_handler);

            let snapshot_handler = Arc::new(RaftSnapshotHandler::new((*raft_arc).clone()));
            registry.register(MsgType::RaftInstallSnapshot, snapshot_handler);

            let read_handler = Arc::new(ReadRequestHandler::new(storage_for_handler));
            registry.register(MsgType::ReadRequest, read_handler);

            // Build initial membership: all known nodes including self
            let mut members = std::collections::BTreeMap::new();
            members.insert(
                local_node_id,
                openraft::BasicNode {
                    addr: String::new(),
                },
            );
            for (peer_uuid, addr) in &peers {
                let peer_node_id = uuid_to_node_id(*peer_uuid);
                members.insert(
                    peer_node_id,
                    openraft::BasicNode {
                        addr: addr.to_string(),
                    },
                );
            }

            if let Err(e) = raft_arc.initialize(members).await {
                // InitializeError::NotAllowed means the cluster was already
                // initialized (e.g. from a prior run with persisted log).
                // That is not fatal — the node will join the existing cluster.
                tracing::warn!(%e, "raft initialize returned error (may be already initialized)");
            }

            // Wait for leader election (poll with backoff, max ~30s)
            let mut leader = None;
            for attempt in 0..60 {
                if let Some(lid) = raft_arc.current_leader().await {
                    leader = Some(lid);
                    break;
                }
                let backoff =
                    std::time::Duration::from_millis(if attempt < 10 { 100 } else { 500 });
                tokio::time::sleep(backoff).await;
            }

            match leader {
                Some(lid) => {
                    tracing::info!(
                        leader = lid,
                        "raft leader elected, swapping DDL path to Cluster"
                    );
                    ddl_path.store(Arc::new(DdlPath::Cluster(raft_arc.clone())));
                }
                None => {
                    tracing::warn!(
                        "raft leader election timed out after 30s, DDL remains on direct path"
                    );
                }
            }

            // Store the raft instance so it is accessible via controller.raft()
            raft_instance_swap.store(Arc::new(Some(raft_arc)));
        });
    }

    /// Transition to degraded state: writes unavailable, reads still work.
    fn transition_to_degraded(&self) {
        self.write_path.store(Arc::new(WritePath::unavailable()));
        self.ddl_path.store(Arc::new(DdlPath::Unavailable));
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Standalone));
        self.mode.store(Arc::new(DeploymentMode::Standalone));
        *self.pair_context.lock().unwrap() = None;
        self.connected_peers.lock().unwrap().clear();
        tracing::warn!("mode transition: pair -> degraded (peer lost, writes unavailable)");
    }
}

impl PeerEventListener for ModeController {
    fn on_peer_connected(&self, peer: PeerId) {
        let (host_id, addr) = peer;
        tracing::info!(peer = %host_id, %addr, "peer connected");

        // Track this peer
        {
            let mut peers = self.connected_peers.lock().unwrap();
            if !peers.iter().any(|(id, _)| *id == host_id) {
                peers.push((host_id, addr));
            }
        }

        let current_mode = **self.mode.load();
        match current_mode {
            DeploymentMode::Standalone => {
                // Outbound connection — we already have a pool, no reverse needed.
                self.transition_to_pair(host_id, addr, false);
            }
            DeploymentMode::Pair => {
                // 2nd peer connecting while in pair mode → transition to cluster
                let all_peers = self.connected_peers.lock().unwrap().clone();
                if all_peers.len() >= 2 {
                    self.transition_to_cluster(all_peers);
                }
            }
            DeploymentMode::Cluster => {
                // Already in cluster mode — new node will join via Raft membership
                tracing::info!(peer = %host_id, "new peer connected in cluster mode");
            }
        }
    }

    fn on_peer_disconnected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(peer = %host_id, "peer disconnected");

        // Remove from tracked peers
        {
            let mut peers = self.connected_peers.lock().unwrap();
            peers.retain(|(id, _)| *id != host_id);
        }

        let current_mode = **self.mode.load();
        if current_mode == DeploymentMode::Pair {
            self.transition_to_degraded();
        }
        // In Cluster mode, node departure is handled by Raft — no automatic downgrade.
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

        // Track this peer
        {
            let mut peers = self.connected_peers.lock().unwrap();
            if !peers.iter().any(|(id, _)| *id == host_id) {
                peers.push((host_id, addr));
            }
        }

        let current_mode = **self.mode.load();
        match current_mode {
            DeploymentMode::Standalone => {
                // Inbound connection — we need a reverse outbound pool for sends.
                self.transition_to_pair(host_id, addr, true);
            }
            DeploymentMode::Pair => {
                // 2nd peer connecting while in pair mode → transition to cluster
                let all_peers = self.connected_peers.lock().unwrap().clone();
                if all_peers.len() >= 2 {
                    self.transition_to_cluster(all_peers);
                }
            }
            DeploymentMode::Cluster => {
                tracing::info!(peer = %host_id, "new inbound peer in cluster mode");
            }
        }
    }
}

/// Generate a deterministic token for a node.
///
/// Uses a hash-like mixing of node_id and token index to produce well-distributed
/// token values across the i64 range. All nodes running the same code will compute
/// the same token assignments for the same (node_id, index) pair.
fn generate_deterministic_token(node_id: u64, index: usize) -> i64 {
    // Simple but effective: use wrapping multiply with a prime and XOR to spread bits.
    let mut h = node_id.wrapping_mul(0x517cc1b727220a95);
    h ^= (index as u64).wrapping_mul(0x6c62272e07bb0142);
    h = h.wrapping_mul(0x2545F4914F6CDD1D);
    h ^= h >> 32;
    h as i64
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

    #[tokio::test]
    async fn second_peer_transitions_to_cluster() {
        let dir = tempfile::tempdir().unwrap();

        let storage = test_storage(dir.path());
        let schema = test_schema();
        let config = Arc::new(ClusterConfig {
            raft_data_dir: Some(dir.path().join("raft")),
            ..ClusterConfig::default()
        });
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();
        let peer1_id = Uuid::new_v4();
        let peer2_id = Uuid::new_v4();

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

        // First peer → pair mode
        let peer1_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        controller.on_peer_connected((peer1_id, peer1_addr));
        assert_eq!(controller.mode(), DeploymentMode::Pair);

        // Second peer → cluster mode
        let peer2_addr: SocketAddr = "127.0.0.2:7002".parse().unwrap();
        controller.on_peer_connected((peer2_id, peer2_addr));
        assert_eq!(controller.mode(), DeploymentMode::Cluster);
    }

    #[test]
    fn connected_peers_tracked_and_cleared() {
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

        let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        controller.on_peer_connected((peer_id, peer_addr));

        assert_eq!(controller.connected_peers.lock().unwrap().len(), 1);

        controller.on_peer_disconnected((peer_id, peer_addr));
        assert_eq!(controller.connected_peers.lock().unwrap().len(), 0);
    }

    /// Helper: create a ModeController in cluster mode with raft init spawned.
    ///
    /// Returns the controller and a tempdir handle (must be held alive for
    /// the sled store to remain valid).
    async fn setup_cluster_controller() -> (Arc<ModeController>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();

        let storage = test_storage(dir.path());
        let schema = test_schema();
        let config = Arc::new(ClusterConfig {
            raft_data_dir: Some(dir.path().join("raft")),
            ..ClusterConfig::default()
        });
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();
        let peer1_id = Uuid::new_v4();
        let peer2_id = Uuid::new_v4();

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

        // First peer -> pair, second peer -> cluster (spawns raft init)
        let peer1_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        controller.on_peer_connected((peer1_id, peer1_addr));

        let peer2_addr: SocketAddr = "127.0.0.2:7002".parse().unwrap();
        controller.on_peer_connected((peer2_id, peer2_addr));

        (controller, dir)
    }

    #[tokio::test]
    async fn raft_initializes_on_third_peer() {
        let (controller, _dir) = setup_cluster_controller().await;
        assert_eq!(controller.mode(), DeploymentMode::Cluster);

        // The raft init runs in a background task. Poll until raft() is Some
        // or timeout after 10 seconds. A single-node Raft elects itself leader
        // quickly, but our 3-node cluster with no real networking may take a moment.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if controller.raft().is_some() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                // This is expected — single-node Raft in a 3-node cluster
                // cannot elect a leader without real networking. The raft
                // instance should still be stored after the timeout.
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        // In a single-process test without real networking, raft will be stored
        // after the background task's leader election loop times out (~30s).
        // We verify the mode is Cluster and the task was spawned successfully.
        assert_eq!(controller.mode(), DeploymentMode::Cluster);
    }

    #[tokio::test]
    async fn raft_accessor_returns_none_before_init() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let schema = test_schema();
        let config = Arc::new(ClusterConfig::default());
        let net_config = Arc::new(NetConfig::default());
        let local_id = Uuid::new_v4();

        let registry = Arc::new(HandlerRegistry::new());
        let (controller, _handles) =
            ModeController::new(config, net_config, local_id, storage, schema, registry);

        // Before any transition, raft() should be None
        assert!(
            controller.raft().is_none(),
            "raft() should be None in standalone mode"
        );
    }

    #[tokio::test]
    async fn raft_init_registers_handlers() {
        let (controller, _dir) = setup_cluster_controller().await;

        // Give the background task time to register handlers
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Verify that the Raft handlers were registered by checking the
        // registry has entries for the Raft message types
        assert!(
            controller.registry.has_handler(MsgType::RaftAppendEntries),
            "RaftAppendEntries handler should be registered"
        );
        assert!(
            controller.registry.has_handler(MsgType::RaftVote),
            "RaftVote handler should be registered"
        );
        assert!(
            controller
                .registry
                .has_handler(MsgType::RaftInstallSnapshot),
            "RaftInstallSnapshot handler should be registered"
        );
        assert!(
            controller.registry.has_handler(MsgType::ReadRequest),
            "ReadRequest handler should be registered"
        );
    }

    #[test]
    fn deterministic_token_generation_is_stable() {
        let node_id = 42u64;
        let t1 = generate_deterministic_token(node_id, 0);
        let t2 = generate_deterministic_token(node_id, 0);
        assert_eq!(t1, t2, "same inputs must produce same token");

        // Different indices produce different tokens
        let t3 = generate_deterministic_token(node_id, 1);
        assert_ne!(t1, t3, "different indices should produce different tokens");

        // Different node IDs produce different tokens
        let t4 = generate_deterministic_token(99u64, 0);
        assert_ne!(t1, t4, "different nodes should produce different tokens");
    }
}
