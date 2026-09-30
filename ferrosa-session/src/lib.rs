//! Protocol-agnostic shared session core for Ferrosa query front-ends.
//!
//! Per blueprint decision **D10**, this crate holds the neutral engine state that
//! every front-end needs — storage, schema, write/DDL routing, cluster mode, the
//! Accord clock and peer manager — so that a new front-end (e.g. `ferrosa-postgres`)
//! can share it **without** depending on the ~54k-LOC `ferrosa-cql` crate.
//!
//! [`SessionCore`] is consumed by `ferrosa-cql`'s `SharedState` (and, later, the
//! Postgres front-end) via `Deref`, so protocol-specific state (prepared-statement
//! caches, CQL event channels, etc.) is composed on top rather than mixed in here.
//!
//! Dependency direction (acyclic): `ferrosa-cql` / `ferrosa-postgres` →
//! `ferrosa-session` → `ferrosa-cluster` / `ferrosa-storage` / `ferrosa-schema` /
//! `ferrosa-net` / `ferrosa-udf` / `ferrosa-common`.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_cluster::{ClusterStateHolder, DdlPath, ModeController, WritePath};
use ferrosa_common::accord::HybridLogicalClock;
use ferrosa_net::peer::PeerManager;
use ferrosa_schema::{NodeConfig, Schema};
use ferrosa_storage::StorageEngine;
use ferrosa_udf::UdfExecutor;

/// Neutral engine state shared across protocol front-ends.
///
/// Fields are the protocol-agnostic subset of the former `ferrosa-cql`
/// `SharedState`. CQL-specific state (prepared-statement cache, EVENT channel,
/// CQL metrics, topology policy, observability trackers) stays in `ferrosa-cql`
/// and is composed alongside an `Arc<SessionCore>`.
pub struct SessionCore {
    /// Local storage engine (read/write path against memtables, SSTables, S3).
    pub engine: Arc<StorageEngine>,
    /// Keyspace/table/role/cluster metadata and authorization.
    pub schema: Arc<Schema>,
    /// This node's identity, replication policy, and cluster settings.
    pub node_config: Arc<NodeConfig>,
    /// Current cluster topology (Standalone / Pair / Cluster).
    pub cluster_state: Arc<ArcSwap<ClusterStateHolder>>,
    /// Write routing (direct or Accord-coordinated), swappable as mode changes.
    pub write_path: Arc<ArcSwap<WritePath>>,
    /// DDL replication routing (direct, pair-coordinated, or Raft).
    pub ddl_path: Arc<ArcSwap<DdlPath>>,
    /// WASM user-defined-function executor.
    pub udf_executor: Arc<UdfExecutor>,
    /// Pair-mode HA readiness controller.
    pub mode_controller: Arc<ModeController>,
    /// When `true`, permission failures are logged and allowed through (soak
    /// observation mode) rather than denied.
    pub auth_warn: bool,
    /// Peer connection manager for Accord coordinator fan-out. `None` in
    /// standalone mode / unit tests.
    pub peer_manager: Option<Arc<PeerManager>>,
    /// Hybrid logical clock for monotone transaction timestamps. `None` when
    /// `peer_manager` is `None`.
    pub accord_clock: Option<Arc<HybridLogicalClock>>,
    /// Shared slot holding this node's live `AccordState`, filled by the cluster
    /// controller during formation. The transaction committer reads it so the
    /// coordinator can vote its own PreAccept locally (a node is never in its
    /// own peer map); without it a sole-replica `BEGIN…COMMIT` fails "Accord
    /// quorum unavailable". Empty in standalone/tests — use
    /// [`ferrosa_cluster::accord::empty_accord_state_slot`].
    pub accord_state: ferrosa_cluster::accord::AccordStateSlot,
}

impl SessionCore {
    /// Whether this node can route strict-serializable transactions through
    /// Accord **right now**.
    ///
    /// Three conditions, all live:
    ///
    /// 1. a peer manager and a clock exist (always true in a real process —
    ///    `ferrosa/src/main.rs` wires both unconditionally, so they cannot by
    ///    themselves distinguish a single node from a cluster);
    /// 2. the live write path is [`WritePath::Cluster`].
    ///
    /// The second is the one that matters, and it must be re-read on every call.
    /// A committer's per-key resolver calls `WritePath::accord_replicas_for_key`,
    /// which returns `None` in every mode other than `Cluster`; the driver turns
    /// that `None` into `Accord network error: no replicas resolved for a key in
    /// keyspace '…' (cluster mode required)`. So offering a committer to a node
    /// that is not (yet) a Raft cluster makes every table statement through the
    /// front-end fail.
    ///
    /// `WritePath` rather than the mode label because it is the exact predicate
    /// the resolver consults, and because the two can briefly disagree:
    /// `transition_to_cluster` installs the cluster write path *before* it flips
    /// the mode, and the mode is flipped back to `Cluster` (RestoreClusterMode)
    /// before the election callback restores the write path. Gating on the write
    /// path is therefore both earlier and never optimistic. It is also, by
    /// construction, a per-statement answer: the ArcSwap it reads is the same
    /// handle the controller swaps at formation, so a node that is still
    /// `Standalone` when a front-end listener is built — which is the normal
    /// startup order — acquires Accord the moment it actually joins.
    pub fn accord_enabled(&self) -> bool {
        self.peer_manager.is_some()
            && self.accord_clock.is_some()
            && matches!(&**self.write_path.load(), WritePath::Cluster(_))
    }

    /// Build the Accord transaction committer for cluster-wide `BEGIN`/`COMMIT`
    /// (ADR-021 / D11), or `None` when this node is not a Raft cluster. Built on
    /// demand from the current write path + schema — cheap (Arc clones + a
    /// closure) — so no committer is stored in or threaded through
    /// `SharedState`, and every front-end (CQL and Postgres) gets it the same
    /// way.
    ///
    /// The per-key replica resolver wraps `WritePath::accord_replicas_for_key`
    /// keyed by each write's keyspace replication; replica placement stays in the
    /// cluster layer, never the front-ends.
    pub fn accord_transaction_committer(
        &self,
    ) -> Option<Arc<dyn ferrosa_storage::accord::TransactionCommitter>> {
        if !self.accord_enabled() {
            return None;
        }
        let peers = self.peer_manager.clone()?;
        let clock = self.accord_clock.clone()?;
        let node_id = u64::from_be_bytes(
            self.node_config.host_id.as_bytes()[..8]
                .try_into()
                .expect("uuid is 16 bytes"),
        );
        let write_path = self.write_path.clone();
        let schema = self.schema.clone();
        let resolve: ferrosa_cluster::accord::ReplicaResolver =
            Arc::new(move |ks: &str, key: &[u8]| {
                let snap = schema.snapshot();
                let replication = &snap.keyspaces.get(ks)?.replication;
                write_path
                    .load()
                    .accord_replicas_for_key(key, replication)
                    .ok()
                    .flatten()
            });
        let applier = Arc::new(ferrosa_cluster::accord::EngineStorageApplier::new(
            self.engine.clone(),
        ));
        // Attach the node's live Accord state (published by the controller at
        // formation) so a replica-coordinator votes its own PreAccept locally.
        // An empty slot (standalone/pre-formation) leaves remote-only votes.
        Some(Arc::new(
            ferrosa_cluster::accord::AccordTransactionCommitter::new(
                node_id, clock, peers, applier, resolve,
            )
            .with_local_accord_state_slot(&self.accord_state),
        ))
    }

    /// Register the PostgreSQL MVCC observer on this node's local Accord apply
    /// path, *before* the node is a cluster.
    ///
    /// A front-end listener is built before the node has connected to its seeds,
    /// so at that moment this method must not depend on the committer gate: the
    /// observer has to survive cluster formation. The `accord_state` slot is the
    /// right target precisely because it is stable — it retains a registered
    /// observer and attaches it to the apply engine when the controller
    /// publishes the node's `AccordState` at formation
    /// ([`ferrosa_cluster::accord::AccordStateSlot::register_postgres_mvcc_observer`]).
    /// Registering through the committer instead would drop the observer
    /// whenever the gate correctly answered `None` at startup, silently
    /// disabling PostgreSQL MVCC visibility through Accord Apply on every real
    /// cluster.
    pub fn register_postgres_mvcc_observer(
        &self,
        observer: Arc<dyn ferrosa_storage::accord::PostgresMvccApplyObserver>,
    ) -> Result<(), String> {
        self.accord_state.register_postgres_mvcc_observer(observer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_cluster::DeploymentMode;
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::PeerEventListener;
    use ferrosa_net::rpc::handler::PeerId;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode as SchemaMode, NodeConfig, PasswordHasher, PasswordPolicy,
        RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig};

    /// A `PeerManager` needs a listener that accepts peer events; the gate under
    /// test never fires an event, so a no-op is sufficient AND more faithful
    /// than `None` — production always has one.
    struct NoopListener;
    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _: PeerId) {}
        fn on_peer_disconnected(&self, _: PeerId) {}
        fn on_peer_suspected(&self, _: PeerId) {}
        fn on_peer_recovered(&self, _: uuid::Uuid) {}
        fn on_peer_failed(&self, _: uuid::Uuid) {}
    }

    fn schema() -> Arc<Schema> {
        Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                mode: SchemaMode::Development,
            })
            .expect("schema bootstraps"),
        )
    }

    fn engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.join("commitlog"),
                checkpoint_dir: dir.join("commitlog"),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: false,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
        };
        Arc::new(StorageEngine::new(config, None).expect("engine"))
    }

    /// A genuine `WritePath::Cluster` — a coordinator over a one-node ring — so
    /// the gate's cluster branch is exercised with the same enum the production
    /// controller installs at formation (`transition_to_cluster` stores exactly
    /// `WritePath::cluster(coordinator)` before it flips the mode label).
    fn cluster_write_path(
        engine: Arc<StorageEngine>,
        peers: Arc<PeerManager>,
    ) -> ferrosa_cluster::WritePath {
        use ferrosa_cluster::raft::{NodeInfo, NodeState};
        let mut ring = ferrosa_cluster::ring::TokenRing::new();
        let host_id = uuid::Uuid::new_v4();
        ring.add_node(
            1,
            NodeInfo {
                host_id,
                addr: "127.0.0.1:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            },
        );
        ring.assign_tokens(1, &[0]);
        let coordinator = ferrosa_cluster::ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            peers,
            1,
            engine,
            1,
            ferrosa_cluster::ConsistencyLevel::One,
        );
        ferrosa_cluster::WritePath::cluster(Arc::new(coordinator))
    }

    /// A `SessionCore` with the peer manager AND Accord clock wired — exactly
    /// what `ferrosa/src/main.rs` passes UNCONDITIONALLY in every deployment
    /// mode (lines 2182/2190). `peer_manager`/`accord_clock` are therefore
    /// always `Some` in a real process and cannot themselves distinguish a
    /// single node from a cluster. `write_path` is the live handle the gate
    /// must read; `mode` is recorded on the controller for realism.
    fn core(
        mode: DeploymentMode,
        cluster: bool,
    ) -> (
        Arc<SessionCore>,
        Arc<StorageEngine>,
        Arc<PeerManager>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine(dir.path());
        let schema = schema();
        let mode_controller =
            ferrosa_cluster::ModeController::standalone_for_test(schema.clone(), engine.clone());
        mode_controller.set_mode_for_test(mode);
        let node_config = Arc::new(NodeConfig {
            cluster_name: "test".into(),
            data_center: "dc1".into(),
            rack: "rack1".into(),
            rpc_port: 9042,
            host_id: uuid::Uuid::new_v4(),
            listen_address: "127.0.0.1".parse().unwrap(),
            listen_port: 7000,
            broadcast_address: "127.0.0.1".parse().unwrap(),
            broadcast_port: 7000,
            rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_port: 9042,
            tokens: vec![],
        });
        let peers = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            node_config.host_id,
            Arc::new(NoopListener),
        ));
        let node_id = u64::from_be_bytes(node_config.host_id.as_bytes()[..8].try_into().unwrap());
        let accord_clock = Arc::new(HybridLogicalClock::new(node_id, 0));
        let udf_executor =
            Arc::new(ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).unwrap());
        let write_path = if cluster {
            cluster_write_path(engine.clone(), peers.clone())
        } else {
            WritePath::direct(engine.clone())
        };
        let core = Arc::new(SessionCore {
            engine: engine.clone(),
            schema: schema.clone(),
            node_config,
            cluster_state: Arc::new(ArcSwap::from_pointee(ClusterStateHolder::Standalone)),
            write_path: Arc::new(ArcSwap::from_pointee(write_path)),
            ddl_path: Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
                schema,
                engine: engine.clone(),
            })),
            udf_executor,
            mode_controller,
            auth_warn: false,
            peer_manager: Some(peers.clone()),
            accord_clock: Some(accord_clock),
            accord_state: ferrosa_cluster::accord::empty_accord_state_slot(),
        });
        (core, engine, peers, dir)
    }

    /// D-48 regression: a SINGLE-NODE deployment must not produce an Accord
    /// transaction committer.
    ///
    /// The committer's per-key resolver calls `WritePath::accord_replicas_for_key`,
    /// which returns `None` outside `WritePath::Cluster` — and the driver turns
    /// that into `Accord network error: no replicas resolved for a key in
    /// keyspace 'public' (cluster mode required)`. So handing a committer to the
    /// Postgres front-end on a standalone node makes EVERY table SELECT/INSERT
    /// fail: `execute_simple` treats any data statement as an implicit
    /// transaction when a committer is present.
    ///
    /// The gate must be the ACTUAL live routing mode, not the mere presence of a
    /// peer manager and clock (both are always wired in `main.rs`).
    #[test]
    fn single_node_yields_no_accord_committer() {
        for mode in [
            DeploymentMode::Standalone,
            DeploymentMode::Pair,
            DeploymentMode::Forming,
            DeploymentMode::DegradedPair,
            DeploymentMode::DegradedCluster,
        ] {
            let (core, _engine, _peers, _dir) = core(mode, false);
            assert!(
                core.accord_transaction_committer().is_none(),
                "{mode}: a node whose write path is not a Raft cluster must NOT \
                 offer an Accord committer — its resolver cannot place a key, so \
                 every Postgres table statement fails 'no replicas resolved'"
            );
        }
    }

    /// The cluster case must KEEP the committer. This is the regression risk of
    /// gating on mode: if the gate is too strict, clustered nodes silently lose
    /// Accord ordering for Postgres transactions.
    #[test]
    fn cluster_node_still_yields_an_accord_committer() {
        let (core, _engine, _peers, _dir) = core(DeploymentMode::Cluster, true);
        assert!(
            core.accord_transaction_committer().is_some(),
            "cluster mode must keep routing Postgres transactions through Accord"
        );
    }

    /// The gate is read from the LIVE write path on every call, never cached.
    ///
    /// This is the property the startup ordering demands: the Postgres listener
    /// is built at step 11b while seed connection (and therefore formation)
    /// starts at step 12, so a node that will become a cluster is still
    /// `Standalone` when its front-end is constructed. A one-shot startup gate
    /// would hand that node a permanent `None` and kill its Accord ordering.
    /// Flipping the shared handle here models exactly that: the same `SessionCore`
    /// acquires and then loses Accord as the controller swaps `write_path`.
    #[test]
    fn committer_gate_follows_the_live_write_path() {
        let (core, engine, peers, _dir) = core(DeploymentMode::Standalone, false);

        assert!(
            core.accord_transaction_committer().is_none(),
            "a solo node has no committer"
        );

        // Formation: the controller installs the cluster write path into the
        // SAME ArcSwap the session already holds.
        core.write_path
            .store(Arc::new(cluster_write_path(engine.clone(), peers.clone())));
        assert!(
            core.accord_transaction_committer().is_some(),
            "formation must make Accord available without rebuilding the session"
        );

        // Quorum lost: the controller swaps the write path to unavailable.
        core.write_path
            .store(Arc::new(ferrosa_cluster::WritePath::unavailable()));
        assert!(
            core.accord_transaction_committer().is_none(),
            "a degraded node must not offer a committer it cannot place keys with"
        );
    }

    /// `accord_enabled` and the committer gate answer the same question, so they
    /// cannot drift apart. `accord_enabled` used to be dead code whose body
    /// (`peer_manager.is_some() && accord_clock.is_some()`) was always true in a
    /// real process — the exact shape of the D-48 bug.
    #[test]
    fn accord_enabled_agrees_with_the_committer_gate() {
        for (mode, cluster) in [
            (DeploymentMode::Standalone, false),
            (DeploymentMode::Pair, false),
            (DeploymentMode::Cluster, true),
            (DeploymentMode::DegradedCluster, false),
        ] {
            let (core, _engine, _peers, _dir) = core(mode, cluster);
            assert_eq!(
                core.accord_enabled(),
                core.accord_transaction_committer().is_some(),
                "{mode}: accord_enabled must not disagree with the committer gate"
            );
        }
    }
}
