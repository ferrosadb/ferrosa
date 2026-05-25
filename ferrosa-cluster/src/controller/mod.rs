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

pub mod bootstrap;
pub mod cluster;
pub mod cluster_rejoin;
mod invite;
mod membership;
mod operator;
mod pair;
mod peer_events;
mod peer_plan;
mod token;

pub use cluster::bootstrap_silent_failure_counts;
pub use cluster_rejoin::{
    cluster_rejoin_attempts_total, cluster_rejoin_failures_total, CLUSTER_REJOIN_ATTEMPTS_TOTAL,
    CLUSTER_REJOIN_FAILURES_TOTAL,
};

pub use token::deterministic_tokens_for_node;
#[cfg(test)]
pub(crate) use token::generate_deterministic_token;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

/// Maximum tracked connected peers before eviction (prevents unbounded growth).
pub(super) const MAX_CONNECTED_PEERS: usize = 1000;
/// Maximum pending join requests before eviction.
#[allow(dead_code)] // Used in tests; retained for future eviction logic.
pub(super) const MAX_PENDING_JOINS: usize = 100;
/// Maximum seen invite initiators before eviction.
#[allow(dead_code)]
pub(super) const MAX_SEEN_INVITE_INITIATORS: usize = 100;
/// Minimum interval between cluster-mode reconnect invites for the same peer.
pub(super) const CLUSTER_RECONNECT_INVITE_COOLDOWN: Duration = Duration::from_secs(30);

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
use crate::raft::{FerrosRaft, RaftGroupId};
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
    /// Per-DC Raft groups, set asynchronously after cluster transition
    /// completes. Multi-DC deployments populate one entry per DC; single-DC
    /// (the default) populates a single entry under
    /// [`RaftGroupId::default_dc`]. Replaces the prior single
    /// `raft_instance` field — see ADR-015.
    pub(super) raft_groups: Arc<ArcSwap<HashMap<RaftGroupId, Arc<FerrosRaft>>>>,
    /// Known DC for each connected peer. Populated by
    /// [`Self::record_peer_dc`] when a peer announces its DC during the
    /// connection handshake. Used by `transition_to_cluster` to filter
    /// Raft voters down to the local DC (W6.3).
    ///
    /// Peers absent from this map default to the local DC — this
    /// preserves single-DC behavior unchanged.
    pub(super) peer_dcs: Mutex<HashMap<Uuid, String>>,
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
    /// Last time this node sent a cluster-mode reconnect invite to a peer.
    /// Prevents duplicate inbound/outbound reconnect callbacks from swapping
    /// lanes and destabilising Raft while still allowing a recreated peer to
    /// receive a fresh invite after startup.
    pub(super) recent_reconnect_invites: Mutex<BTreeMap<Uuid, std::time::Instant>>,
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
            raft_groups: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            peer_dcs: Mutex::new(HashMap::new()),
            hint_store,
            hint_config,
            approved_nodes: Mutex::new(BTreeSet::new()),
            ring: Arc::new(ArcSwap::from_pointee(None)),
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            transition_guard: Mutex::new(()),
            formation_epoch: std::sync::atomic::AtomicU64::new(0),
            seen_invite_initiators: Mutex::new(BTreeSet::new()),
            recent_reconnect_invites: Mutex::new(BTreeMap::new()),
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
            raft_groups: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            peer_dcs: Mutex::new(HashMap::new()),
            hint_store,
            hint_config,
            approved_nodes: Mutex::new(BTreeSet::new()),
            ring: Arc::new(ArcSwap::from_pointee(None)),
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            transition_guard: Mutex::new(()),
            formation_epoch: std::sync::atomic::AtomicU64::new(0),
            seen_invite_initiators: Mutex::new(BTreeSet::new()),
            recent_reconnect_invites: Mutex::new(BTreeMap::new()),
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
            raft_groups: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            peer_dcs: Mutex::new(HashMap::new()),
            hint_store,
            hint_config,
            approved_nodes: Mutex::new(BTreeSet::new()),
            ring: Arc::new(ArcSwap::from_pointee(None)),
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            transition_guard: Mutex::new(()),
            formation_epoch: std::sync::atomic::AtomicU64::new(0),
            seen_invite_initiators: Mutex::new(BTreeSet::new()),
            recent_reconnect_invites: Mutex::new(BTreeMap::new()),
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
    /// Read access to the registered `PeerManager`, if any.
    ///
    /// Returns `None` until `set_peer_manager` has been called — typically
    /// during early bootstrap before networking comes up. Used by the
    /// `/admin/membership-snapshot` endpoint (Sprint 2 W2.3).
    pub fn peer_manager_arc(&self) -> Option<Arc<ferrosa_net::peer::PeerManager>> {
        self.peer_manager.load().as_ref().clone()
    }

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
        self.registry.register(
            MsgType::ClusterMembershipForward,
            Arc::new(crate::raft_forward::ClusterMembershipForwardUnavailableHandler),
        );

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
    ///
    /// Backward-compat shim for single-DC callers (see ADR-015): if
    /// exactly one Raft group exists, return it; otherwise return the
    /// group bound to the local node's `data_center` (or
    /// [`RaftGroupId::default_dc`] when no DC was configured). Returns
    /// `None` when no group has been installed yet (standalone, pair,
    /// or pre-init cluster mode).
    ///
    /// Multi-DC callers should prefer [`Self::raft_for_dc`] or
    /// [`Self::raft_for_group`] for clarity.
    pub fn raft(&self) -> Option<Arc<FerrosRaft>> {
        let groups = self.raft_groups.load();
        if groups.len() == 1 {
            return groups.values().next().cloned();
        }
        // Multi-DC (or empty): prefer the local DC's group.
        let local_id = RaftGroupId::for_dc(&self.config.data_center);
        if let Some(g) = groups.get(&local_id) {
            return Some(g.clone());
        }
        // Last-ditch fallback: the conventional "default" group.
        groups.get(&RaftGroupId::default_dc()).cloned()
    }

    /// Look up the Raft group bound to a specific DC name.
    ///
    /// Returns `None` if no group has been installed for that DC, or
    /// if cluster mode initialization has not yet completed.
    pub fn raft_for_dc(&self, dc_name: &str) -> Option<Arc<FerrosRaft>> {
        self.raft_for_group(RaftGroupId::for_dc(dc_name))
    }

    /// Look up the Raft group bound to a specific [`RaftGroupId`].
    pub fn raft_for_group(&self, id: RaftGroupId) -> Option<Arc<FerrosRaft>> {
        self.raft_groups.load().get(&id).cloned()
    }

    /// Snapshot of the current Raft group map.
    ///
    /// Returns a fresh `HashMap` (not held under any lock) so callers
    /// can iterate without touching the live `ArcSwap`.
    pub fn raft_groups(&self) -> HashMap<RaftGroupId, Arc<FerrosRaft>> {
        (**self.raft_groups.load()).clone()
    }

    /// Install a Raft group under the given [`RaftGroupId`].
    ///
    /// Idempotent: re-installing the same id replaces the prior entry.
    /// Used by cluster bootstrap (and tests) to publish the per-DC
    /// `Arc<FerrosRaft>` once initialization completes. Operator
    /// surface for W6.7 (`bootstrap-dc`) wraps this.
    pub fn set_raft_for_group(&self, id: RaftGroupId, raft: Arc<FerrosRaft>) {
        let current = self.raft_groups.load_full();
        let mut next: HashMap<RaftGroupId, Arc<FerrosRaft>> = (*current).clone();
        next.insert(id, raft);
        self.raft_groups.store(Arc::new(next));
    }

    /// Convenience wrapper for [`Self::set_raft_for_group`] that derives
    /// the `RaftGroupId` from a DC name.
    pub fn set_raft_for_dc(&self, dc_name: &str, raft: Arc<FerrosRaft>) {
        self.set_raft_for_group(RaftGroupId::for_dc(dc_name), raft);
    }

    /// Record a connected peer's DC. Called by the peer handshake (or
    /// tests, before triggering `transition_to_cluster`) so that the
    /// per-DC Raft formation path (W6.3) can filter local-DC voters
    /// from cross-DC peers.
    ///
    /// Idempotent: re-recording overwrites the prior entry. Pass the
    /// peer's `data_center` as configured at the peer (matches the
    /// `FERROSA_DATA_CENTER` env var on that node).
    pub fn record_peer_dc(&self, host_id: Uuid, dc_name: impl Into<String>) {
        self.peer_dcs.lock().insert(host_id, dc_name.into());
    }

    /// Look up a peer's DC, if known. Returns `None` for peers that
    /// have not announced a DC; callers should treat that as "same DC
    /// as local" for backward compat.
    pub fn peer_dc(&self, host_id: Uuid) -> Option<String> {
        self.peer_dcs.lock().get(&host_id).cloned()
    }

    /// Snapshot of the peer-DC map. Used by `transition_to_cluster` and
    /// by tests.
    pub(crate) fn peer_dcs_snapshot(&self) -> HashMap<Uuid, String> {
        self.peer_dcs.lock().clone()
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
