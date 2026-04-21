//! Mode controller — manages runtime transitions between deployment modes.
//!
//! The controller implements [`ferrosa_net::peer::PeerEventListener`] and swaps the active
//! [`WritePath`] and [`ClusterStateHolder`] atomically when the deployment mode
//! changes (standalone → pair → cluster).
//!
//! Failover lifecycle:
//!   1. Pair mode active, both nodes connected
//!   2. Peer disconnects → writes become unavailable (degraded)
//!   3. Operator calls `force_promote()` → standalone with direct writes
//!   4. Peer reconnects → auto re-pair, promoted node stays primary
//!   5. Operator can `switchover()` to swap roles

mod cluster;
mod membership;
mod operator;
mod pair;
mod peer_events;
mod token;

pub(crate) use token::deterministic_tokens_for_node;
#[cfg(test)]
pub(crate) use token::generate_deterministic_token;

use std::collections::BTreeSet;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

/// Maximum tracked connected peers before eviction (prevents unbounded growth).
pub(super) const MAX_CONNECTED_PEERS: usize = 1000;
/// Maximum pending join requests before eviction.
#[allow(dead_code)] // Used in tests; retained for future eviction logic.
pub(super) const MAX_PENDING_JOINS: usize = 100;
/// Maximum seen invite initiators before eviction.
#[allow(dead_code)]
pub(super) const MAX_SEEN_INVITE_INITIATORS: usize = 100;

use parking_lot::Mutex;

use arc_swap::ArcSwap;
use uuid::Uuid;

use ferrosa_net::config::NetConfig;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_schema::system::peers::{ClusterState, PeerInfo};
use ferrosa_schema::Schema;
use ferrosa_storage::engine::StorageEngine;

use crate::config::ClusterConfig;
use crate::ddl_path::DdlPath;
use crate::hints::{HintConfig, HintStore};
use crate::mode::DeploymentMode;
use crate::pair::PairRole;
use crate::raft::FerrosRaft;
use crate::ring::TokenRing;
use crate::state::PairClusterState;
use crate::write_path::WritePath;

// Re-exports used by tests via `use super::*`.
#[cfg(test)]
pub(crate) use crate::error::ClusterError;
#[cfg(test)]
pub(crate) use crate::raft::uuid_to_node_id;
#[cfg(test)]
pub(crate) use ferrosa_net::codec::MsgType;
#[cfg(test)]
pub(crate) use ferrosa_net::peer::{PeerEventListener, PeerManager};
#[cfg(test)]
pub(crate) use ferrosa_net::rpc::InboundPeerCallback;

/// Swappable cluster state — enum dispatch to avoid trait object Sized issues.
pub enum ClusterStateHolder {
    Standalone,
    Pair(PairClusterState),
    Cluster(crate::state::RaftClusterState),
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
pub(super) struct PairContext {
    pub(super) role: Arc<ArcSwap<PairRole>>,
    pub(super) peer_host_id: Uuid,
    #[allow(dead_code)]
    pub(super) peer_addr: SocketAddr,
}

/// Atomic counters for lock contention measurements.
///
/// Tracks how long the transition guard is held and how many times it
/// was acquired. Useful for diagnosing cluster mode transition latency.
pub struct ContentionMetrics {
    /// Number of times the transition guard was acquired.
    pub transition_guard_acquires: std::sync::atomic::AtomicU64,
    /// Cumulative nanoseconds spent holding the transition guard.
    pub transition_guard_hold_ns: std::sync::atomic::AtomicU64,
}

impl ContentionMetrics {
    pub fn new() -> Self {
        Self {
            transition_guard_acquires: std::sync::atomic::AtomicU64::new(0),
            transition_guard_hold_ns: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Record a transition guard acquisition with the given hold duration.
    pub fn record_guard_hold(&self, duration: std::time::Duration) {
        self.transition_guard_acquires
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.transition_guard_hold_ns.fetch_add(
            duration.as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

impl Default for ContentionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages deployment mode transitions at runtime.
///
/// Created at startup with standalone mode. When peers connect/disconnect,
/// transitions the mode and atomically swaps the write path and cluster state.
pub struct ModeController {
    pub(super) mode: Arc<ArcSwap<DeploymentMode>>,
    pub(super) write_path: Arc<ArcSwap<WritePath>>,
    pub(super) cluster_state: Arc<ArcSwap<ClusterStateHolder>>,
    pub(super) storage: Arc<StorageEngine>,
    pub(super) schema: Arc<Schema>,
    pub(super) ddl_path: Arc<ArcSwap<DdlPath>>,
    pub(super) config: Arc<ClusterConfig>,
    pub(super) net_config: Arc<NetConfig>,
    pub(super) local_host_id: Uuid,
    pub(super) peer_manager: ArcSwap<Option<Arc<ferrosa_net::peer::PeerManager>>>,
    pub(super) registry: Arc<HandlerRegistry>,
    /// Stored during pair mode for switchover/promote operations.
    pub(super) pair_context: Mutex<Option<PairContext>>,
    /// Set by force_promote — overrides UUID election on next pair transition.
    pub(super) force_promoted: AtomicBool,
    /// Lamport counter incremented on each force_promote. On reconnect after
    /// a partition, the node with the higher promote_epoch wins primary role.
    /// Prevents split-brain when both nodes force_promote independently.
    pub(super) promote_epoch: std::sync::atomic::AtomicU64,
    /// All connected peers, tracked across mode transitions.
    pub(super) connected_peers: Mutex<Vec<(Uuid, SocketAddr)>>,
    /// Raft instance, set asynchronously after cluster transition completes.
    pub(super) raft_instance: Arc<ArcSwap<Option<Arc<FerrosRaft>>>>,
    /// Persistent hint store — holds mutations destined for temporarily
    /// unreachable replicas.  Shared with `ClusterCoordinator`.
    pub(super) hint_store: Arc<HintStore>,
    /// Hint delivery configuration — batch size, interval, etc.
    pub(super) hint_config: HintConfig,
    /// Set of host IDs approved to join the cluster.
    ///
    /// Mirrors `RaftState.approved_nodes` for synchronous access in join checks.
    /// Updated when `ApproveNode` commands are committed.
    pub(super) approved_nodes: Mutex<BTreeSet<Uuid>>,
    /// Live token ring, set when transitioning to cluster mode.
    /// `None` in standalone and pair modes.
    pub(super) ring: Arc<ArcSwap<Option<Arc<TokenRing>>>>,
    /// Peers whose join has been triggered via `handle_join_request`.
    ///
    /// Tracked so that the same peer is not re-admitted on reconnect and
    /// for testability (unit tests can inspect pending joins without
    /// requiring a full Raft cluster).
    pub(super) pending_joins: Arc<Mutex<Vec<Uuid>>>,
    /// Serializes mode transitions. Held across the check-and-transition
    /// window to prevent concurrent `on_peer_connected` calls from both
    /// triggering `transition_to_pair` when two peers arrive simultaneously.
    pub(super) transition_guard: Mutex<()>,
    /// Formation epoch — incremented each time we enter Forming state.
    /// Used to reject stale ClusterInvite messages from previous formation attempts.
    pub(super) formation_epoch: std::sync::atomic::AtomicU64,
    /// Initiators already seen in this formation epoch. Deduplicates invites.
    pub(super) seen_invite_initiators: Mutex<BTreeSet<Uuid>>,
    /// Tracks all spawned background tasks. Replaces fire-and-forget spawns
    /// so panics are detected and tasks can be cancelled on shutdown.
    pub(super) background_tasks: Mutex<tokio::task::JoinSet<()>>,
    /// Cancellation token — cancelled during shutdown to signal all background
    /// tasks to stop. Passed to spawned tasks that should respect graceful shutdown.
    pub(super) cancel: tokio_util::sync::CancellationToken,
    /// Committed cluster size — set when transitioning to Cluster mode.
    /// Used for quorum calculations instead of the dynamic connected count
    /// to prevent false quorum restoration after network partitions.
    pub(super) committed_cluster_size: AtomicUsize,
    /// Receiver for DDL operations queued during Forming state.
    pub(super) ddl_queue_rx: Arc<
        parking_lot::Mutex<
            Option<tokio::sync::mpsc::UnboundedReceiver<crate::pair::ddl::DdlOperation>>,
        >,
    >,
    /// Contention metrics for the transition guard.
    pub contention_metrics: Arc<ContentionMetrics>,
    /// Dedicated Raft runtime for openraft tasks.
    pub(super) raft_runtime: std::sync::OnceLock<Arc<tokio::runtime::Runtime>>,
    /// Dedicated Data runtime for internode IO.
    pub(super) data_runtime: std::sync::OnceLock<Arc<tokio::runtime::Runtime>>,
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

        // Build a HintConfig from ClusterConfig, then initialise the store.
        // If the hint store fails to initialise (e.g. bad directory permissions)
        // we log the error and continue — the worst outcome is missing hints, not
        // a crash at startup.
        let hint_config = HintConfig {
            dir: config.hinted_handoff_dir.clone(),
            max_per_peer_mb: config.hinted_handoff_max_mb,
            ..HintConfig::default()
        };
        let hint_store = match HintStore::new(hint_config.clone()) {
            Ok(hs) => Arc::new(hs),
            Err(e) => {
                tracing::error!("hint store initialisation failed: {e} — hinted handoff disabled");
                Arc::new(
                    HintStore::new(HintConfig {
                        dir: std::env::temp_dir().join("ferrosa_hints_fallback"),
                        ..HintConfig::default()
                    })
                    .unwrap_or_else(|_| {
                        // If even the fallback fails, create a store in a temp dir.
                        HintStore::new(HintConfig::default()).expect("fallback hint store failed")
                    }),
                )
            }
        };

        let controller = Arc::new(Self {
            mode: Arc::new(ArcSwap::from_pointee(DeploymentMode::Standalone)),
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
            promote_epoch: std::sync::atomic::AtomicU64::new(0),
            connected_peers: Mutex::new(Vec::new()),
            raft_instance: Arc::new(ArcSwap::from_pointee(None)),
            hint_store,
            hint_config,
            approved_nodes: Mutex::new(BTreeSet::new()),
            ring: Arc::new(ArcSwap::from_pointee(None)),
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            transition_guard: Mutex::new(()),
            formation_epoch: std::sync::atomic::AtomicU64::new(0),
            seen_invite_initiators: Mutex::new(BTreeSet::new()),
            background_tasks: Mutex::new(tokio::task::JoinSet::new()),
            cancel: tokio_util::sync::CancellationToken::new(),
            committed_cluster_size: AtomicUsize::new(0),
            ddl_queue_rx: Arc::new(parking_lot::Mutex::new(None)),
            contention_metrics: Arc::new(ContentionMetrics::new()),
            raft_runtime: std::sync::OnceLock::new(),
            data_runtime: std::sync::OnceLock::new(),
        });

        let handles = ModeControllerHandles {
            write_path,
            cluster_state,
            ddl_path,
        };

        (controller, handles)
    }

    /// Create a standalone-mode `ModeController` for unit tests.
    ///
    /// No networking, no handlers — only mode/role queries (e.g. `is_cql_ready()`)
    /// work.  All other fields are initialised to harmless defaults.
    pub fn standalone_for_test(schema: Arc<Schema>, engine: Arc<StorageEngine>) -> Arc<Self> {
        let write_path = Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone())));
        let cluster_state = Arc::new(ArcSwap::from_pointee(ClusterStateHolder::Standalone));
        let ddl_path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        }));
        let hint_config = HintConfig::default();
        let hint_store = Arc::new(HintStore::new(hint_config.clone()).expect("test hint store"));

        Arc::new(Self {
            mode: Arc::new(ArcSwap::from_pointee(DeploymentMode::Standalone)),
            write_path,
            cluster_state,
            storage: engine,
            schema,
            ddl_path,
            config: Arc::new(ClusterConfig::default()),
            net_config: Arc::new(NetConfig::default()),
            local_host_id: Uuid::new_v4(),
            peer_manager: ArcSwap::from_pointee(None),
            registry: Arc::new(HandlerRegistry::new()),
            pair_context: Mutex::new(None),
            force_promoted: AtomicBool::new(false),
            promote_epoch: std::sync::atomic::AtomicU64::new(0),
            connected_peers: Mutex::new(Vec::new()),
            raft_instance: Arc::new(ArcSwap::from_pointee(None)),
            hint_store,
            hint_config,
            approved_nodes: Mutex::new(BTreeSet::new()),
            ring: Arc::new(ArcSwap::from_pointee(None)),
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            transition_guard: Mutex::new(()),
            formation_epoch: std::sync::atomic::AtomicU64::new(0),
            seen_invite_initiators: Mutex::new(BTreeSet::new()),
            background_tasks: Mutex::new(tokio::task::JoinSet::new()),
            cancel: tokio_util::sync::CancellationToken::new(),
            committed_cluster_size: AtomicUsize::new(0),
            ddl_queue_rx: Arc::new(parking_lot::Mutex::new(None)),
            contention_metrics: Arc::new(ContentionMetrics::new()),
            raft_runtime: std::sync::OnceLock::new(),
            data_runtime: std::sync::OnceLock::new(),
        })
    }

    /// Create a pair-secondary `ModeController` for unit tests.
    ///
    /// Like `standalone_for_test`, but the mode is `Pair` and the role is
    /// `Secondary`, so `is_cql_ready()` returns `false`.  Useful for testing
    /// CQL connection rejection on secondary nodes.
    pub fn pair_secondary_for_test(schema: Arc<Schema>, engine: Arc<StorageEngine>) -> Arc<Self> {
        let write_path = Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone())));
        let cluster_state = Arc::new(ArcSwap::from_pointee(ClusterStateHolder::Standalone));
        let ddl_path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        }));
        let hint_config = HintConfig::default();
        let hint_store = Arc::new(HintStore::new(hint_config.clone()).expect("test hint store"));

        let role = Arc::new(ArcSwap::from_pointee(PairRole::Secondary));
        let pair_ctx = PairContext {
            role,
            peer_host_id: Uuid::new_v4(),
            peer_addr: "127.0.0.1:7000".parse().unwrap(),
        };

        Arc::new(Self {
            mode: Arc::new(ArcSwap::from_pointee(DeploymentMode::Pair)),
            write_path,
            cluster_state,
            storage: engine,
            schema,
            ddl_path,
            config: Arc::new(ClusterConfig::default()),
            net_config: Arc::new(NetConfig::default()),
            local_host_id: Uuid::new_v4(),
            peer_manager: ArcSwap::from_pointee(None),
            registry: Arc::new(HandlerRegistry::new()),
            pair_context: Mutex::new(Some(pair_ctx)),
            force_promoted: AtomicBool::new(false),
            promote_epoch: std::sync::atomic::AtomicU64::new(0),
            connected_peers: Mutex::new(Vec::new()),
            raft_instance: Arc::new(ArcSwap::from_pointee(None)),
            hint_store,
            hint_config,
            approved_nodes: Mutex::new(BTreeSet::new()),
            ring: Arc::new(ArcSwap::from_pointee(None)),
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            transition_guard: Mutex::new(()),
            formation_epoch: std::sync::atomic::AtomicU64::new(0),
            seen_invite_initiators: Mutex::new(BTreeSet::new()),
            background_tasks: Mutex::new(tokio::task::JoinSet::new()),
            cancel: tokio_util::sync::CancellationToken::new(),
            committed_cluster_size: AtomicUsize::new(0),
            ddl_queue_rx: Arc::new(parking_lot::Mutex::new(None)),
            contention_metrics: Arc::new(ContentionMetrics::new()),
            raft_runtime: std::sync::OnceLock::new(),
            data_runtime: std::sync::OnceLock::new(),
        })
    }

    /// Set the peer manager reference. Must be called after PeerManager is created.
    ///
    /// Also registers the `ClusterInviteHandler` so this node can process
    /// incoming `ClusterInvite` messages and connect to discovered peers.
    pub fn set_peer_manager(self: &Arc<Self>, pm: Arc<ferrosa_net::peer::PeerManager>) {
        // Register ClusterInvite handler so this node can process
        // incoming invites and connect to discovered peers.
        // The handler gets a Weak<ModeController> so it can trigger
        // cluster transition when receiving an invite in Pair mode.
        use ferrosa_net::codec::MsgType;
        let invite_handler = Arc::new(cluster::ClusterInviteHandler::new(
            self.local_host_id,
            pm.clone(),
            self.net_config.clone(),
            Arc::downgrade(self),
        ));
        self.registry
            .register(MsgType::ClusterInvite, invite_handler);

        self.peer_manager.store(Arc::new(Some(pm)));
    }

    /// Set a dedicated runtime for Raft consensus tasks.
    pub fn set_raft_runtime(&self, rt: Arc<tokio::runtime::Runtime>) {
        let _ = self.raft_runtime.set(rt);
    }

    /// Set a dedicated runtime for internode data IO.
    pub fn set_data_runtime(&self, rt: Arc<tokio::runtime::Runtime>) {
        let _ = self.data_runtime.set(rt);
    }

    /// Get current deployment mode.
    pub fn mode(&self) -> DeploymentMode {
        **self.mode.load()
    }

    /// Get current pair role, if in pair mode.
    pub fn role(&self) -> Option<PairRole> {
        let ctx = self.pair_context.lock();
        ctx.as_ref().map(|c| **c.role.load())
    }

    /// Whether this node should accept CQL client connections.
    ///
    /// In pair mode only the primary accepts clients; the secondary exists
    /// solely for replication.  Standalone and cluster nodes always accept.
    pub fn is_cql_ready(&self) -> bool {
        match self.mode() {
            DeploymentMode::Standalone => true,
            DeploymentMode::Pair => self.role() == Some(PairRole::Primary),
            DeploymentMode::Forming => false, // Not ready until Raft leader elected
            DeploymentMode::Cluster => true,
            DeploymentMode::DegradedPair => true, // Stale reads available
            DeploymentMode::DegradedCluster => true, // Stale reads at CL=ONE
        }
    }

    /// Get local host_id.
    pub fn host_id(&self) -> Uuid {
        self.local_host_id
    }

    /// Graceful shutdown: cancel all background tasks and wait for them to finish.
    ///
    /// Call before dropping the node to ensure in-flight Raft proposals,
    /// schema syncs, and cluster joins complete or abort cleanly.
    pub async fn shutdown(&self) {
        tracing::info!("ModeController shutting down — cancelling background tasks");
        self.cancel.cancel();

        // Take the JoinSet out of the Mutex to avoid holding the lock across await.
        let mut tasks = {
            let mut guard = self.background_tasks.lock();
            std::mem::take(&mut *guard)
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while let Some(result) = tokio::time::timeout_at(deadline, tasks.join_next())
            .await
            .ok()
            .flatten()
        {
            match result {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => tracing::warn!("background task panicked during shutdown: {e}"),
            }
        }
        let remaining = tasks.len();
        if remaining > 0 {
            tracing::warn!(remaining, "shutdown timed out — aborting remaining tasks");
            tasks.abort_all();
        }
        tracing::info!("ModeController shutdown complete");
    }

    /// Return a child cancellation token for background tasks that should
    /// stop on shutdown.
    pub fn cancel_token(&self) -> tokio_util::sync::CancellationToken {
        self.cancel.clone()
    }

    /// Spawn a tracked background task. Unlike bare `tokio::spawn`, panics
    /// in these tasks are detectable via the JoinSet and tasks can be
    /// cancelled on shutdown.
    pub(super) fn spawn_tracked<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.background_tasks.lock().spawn(future);
    }

    /// Get the Raft instance, if cluster mode initialization has completed.
    pub fn raft(&self) -> Option<Arc<FerrosRaft>> {
        (**self.raft_instance.load()).clone()
    }

    /// Return a snapshot of the current token ring.
    ///
    /// Returns the live [`TokenRing`] if the node is in cluster mode with an
    /// initialized ring, `None` otherwise (standalone or pair mode).
    pub fn token_ring(&self) -> Option<Arc<TokenRing>> {
        (**self.ring.load()).clone()
    }

    /// Directly install a token ring.
    ///
    /// Used by the cluster mode transition and in tests to seed ring state
    /// without going through a full peer-connection lifecycle.
    pub fn set_token_ring(&self, ring: Arc<TokenRing>) {
        self.ring.store(Arc::new(Some(ring)));
    }

    /// Return a reference to the shared hint store.
    ///
    /// Used by callers that build a [`crate::coordinator::ClusterCoordinator`] and want to
    /// attach the same `HintStore` instance via
    /// [`crate::coordinator::ClusterCoordinator::with_hint_store`].
    pub fn hint_store(&self) -> Arc<HintStore> {
        self.hint_store.clone()
    }
}

#[cfg(test)]
mod tests;
