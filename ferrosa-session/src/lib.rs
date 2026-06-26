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
}

impl SessionCore {
    /// Whether this node can route strict-serializable transactions through
    /// Accord — i.e. it is in cluster mode with both a peer manager and a clock.
    ///
    /// This is the structural precondition the Postgres front-end checks before
    /// honoring D11 (explicit `BEGIN…COMMIT` blocks routed to Accord).
    pub fn accord_enabled(&self) -> bool {
        self.peer_manager.is_some() && self.accord_clock.is_some()
    }

    /// Build the Accord transaction committer for cluster-wide `BEGIN`/`COMMIT`
    /// (ADR-021 / D11), or `None` in standalone mode. Built on demand from the
    /// current write path + schema — cheap (Arc clones + a closure) — so no
    /// committer is stored in or threaded through `SharedState`, and every
    /// front-end (CQL and Postgres) gets it the same way.
    ///
    /// The per-key replica resolver wraps `WritePath::accord_replicas_for_key`
    /// keyed by each write's keyspace replication; replica placement stays in the
    /// cluster layer, never the front-ends.
    pub fn accord_transaction_committer(
        &self,
    ) -> Option<Arc<dyn ferrosa_storage::accord::TransactionCommitter>> {
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
        Some(Arc::new(
            ferrosa_cluster::accord::AccordTransactionCommitter::new(
                node_id, clock, peers, applier, resolve,
            ),
        ))
    }
}
