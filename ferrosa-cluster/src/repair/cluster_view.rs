//! Real, verified-healthy-replica [`ClusterView`] for the storage-side
//! self-heal controller.
//!
//! The storage crate's self-heal controller gates quarantine on a healthy
//! replica (FMEA #1: never quarantine the only copy of data). It cannot reach
//! the cluster layer itself, so it asks an injected [`ClusterView`] for the
//! replica posture of a `(table, range)`. The single-node stub in
//! `ferrosa_storage` always answers `SingleNode`, so on a real cluster
//! quarantine never fires.
//!
//! [`ClusterRepairView`] is the real implementation. It answers
//! [`ReplicaPosture::HealthyReplicaAvailable`] for a table **only when a
//! reachable peer is VERIFIED to hold a non-corrupt copy** — it probes a peer
//! using the existing repair Merkle/digest RPC (a successful, non-empty digest
//! response proves the peer holds readable, non-corrupt data for the range).
//! Every other outcome — RF ≤ 1, no reachable peer, probe failure, or an empty
//! digest — yields a not-healthy posture, so the controller escalates instead
//! of quarantining.
//!
//! ## Probe abstraction
//!
//! The real Merkle RPC ([`RemoteRepairStore::build_merkle`](super::rpc::RemoteRepairStore))
//! is async and needs a live `PeerManager` + connection pool, which is too
//! heavy to drive from a synchronous `ClusterView::replica_posture` call and
//! from a unit test. So the network step is abstracted behind the small,
//! synchronous [`RepairProbe`] port:
//!
//! - [`RpcRepairProbe`] is the production impl — a thin, documented wrapper
//!   that blocks on `RemoteRepairStore::build_merkle` via a tokio runtime
//!   handle and reports whether the returned tree was non-empty.
//! - Unit tests use a mock [`RepairProbe`] to drive every posture branch
//!   without standing up the wire protocol.
//!
//! Topology (which nodes are candidate replicas, and their `host_id` UUIDs) is
//! likewise abstracted behind [`ClusterTopology`], implemented for the live
//! ring by [`RingTopology`] and mocked in tests.

use std::sync::Arc;

use uuid::Uuid;

use ferrosa_net::peer::PeerManager;
use ferrosa_storage::self_heal::{ClusterView, ReplicaPosture, TableKey};
use ferrosa_storage::TableId;

use super::rpc::RemoteRepairStore;
use super::RepairStore;

/// Result of probing one peer for a verified, non-corrupt copy of a range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// The peer answered the digest RPC and reported **non-empty** content for
    /// the range — a verified healthy copy that can refill after quarantine.
    HealthyNonEmpty,
    /// The peer answered but the digest was **empty** (the peer holds no data
    /// for this range) — not a usable refill source.
    Empty,
    /// The peer was unreachable or the probe RPC failed. Not a usable source.
    Unreachable,
}

/// Synchronous port that verifies whether a single peer holds a non-corrupt,
/// non-empty copy of a `(table, range)` by exchanging a repair digest.
///
/// Kept synchronous so [`ClusterView::replica_posture`] (a sync trait method)
/// can call it directly; the production impl blocks on the async Merkle RPC
/// internally. Abstracted as a trait so the posture logic is unit-testable
/// against a mock without the wire protocol.
pub trait RepairProbe: Send + Sync {
    /// Probe `peer` for a verified copy of `table` over `[range.0, range.1)`.
    fn probe_digest(&self, peer: Uuid, table: &TableId, range: (i64, i64)) -> ProbeOutcome;
}

/// Topology facts the view needs from the ring, abstracted so the posture
/// logic is testable without a live `TokenRing`.
pub trait ClusterTopology: Send + Sync {
    /// This node's `u64` ring id (deterministic initiator selection).
    fn this_node_id(&self) -> u64;

    /// Owners (`u64` ring ids) of `table`'s ranges, or `None` if unknown.
    /// Used both for `ClusterView::owners` and to scope the probe set.
    fn owners(&self, table: &TableKey) -> Option<Vec<u64>>;

    /// Other reachable replica peers for `table`, as `(ring_id, host_id)`
    /// pairs, in a deterministic (ascending ring-id) order, excluding this
    /// node. Empty when this is effectively single-node / RF ≤ 1 for the
    /// table or no peer is currently reachable.
    fn replica_peers(&self, table: &TableKey) -> Vec<(u64, Uuid)>;
}

/// The real [`ClusterView`]: verified-healthy-replica posture via the repair
/// digest RPC.
pub struct ClusterRepairView {
    topology: Arc<dyn ClusterTopology>,
    probe: Arc<dyn RepairProbe>,
}

impl ClusterRepairView {
    /// Construct from a topology source and a probe.
    ///
    /// On a real node the binary supplies a [`RingTopology`] (over the shared
    /// `TokenRing`) and an [`RpcRepairProbe`] (over the `PeerManager`). Tests
    /// supply mocks for both.
    pub fn new(topology: Arc<dyn ClusterTopology>, probe: Arc<dyn RepairProbe>) -> Self {
        Self { topology, probe }
    }

    /// The token range probed for a table's posture. The `ClusterView` trait
    /// is per-table (no range), and a corrupt generation can span the whole
    /// ring, so we probe the full ring span: any peer that returns a non-empty
    /// digest holds readable, non-corrupt data this node can refill from.
    const PROBE_RANGE: (i64, i64) = (i64::MIN, i64::MAX);
}

impl ClusterView for ClusterRepairView {
    fn this_host(&self) -> u64 {
        self.topology.this_node_id()
    }

    fn owners(&self, table: &TableKey) -> Option<Vec<u64>> {
        self.topology.owners(table)
    }

    fn replica_posture(&self, table: &TableKey) -> ReplicaPosture {
        let peers = self.topology.replica_peers(table);
        if peers.is_empty() {
            // RF ≤ 1 or no reachable peer → the local (possibly corrupt) copy
            // is the only one. Never quarantine (FMEA #1).
            tracing::warn!(
                keyspace = %table.keyspace,
                table = %table.table,
                "self-heal: no reachable replica peer for table — posture=SingleNode \
                 (quarantine will be refused; FMEA #1)"
            );
            return ReplicaPosture::SingleNode;
        }

        let table_id = TableId::new(&table.keyspace, &table.table);
        for (ring_id, host_id) in &peers {
            match self
                .probe
                .probe_digest(*host_id, &table_id, Self::PROBE_RANGE)
            {
                ProbeOutcome::HealthyNonEmpty => {
                    tracing::info!(
                        keyspace = %table.keyspace,
                        table = %table.table,
                        peer_ring_id = ring_id,
                        %host_id,
                        "self-heal: verified healthy replica via repair digest — \
                         posture=HealthyReplicaAvailable"
                    );
                    return ReplicaPosture::HealthyReplicaAvailable;
                }
                ProbeOutcome::Empty => {
                    tracing::debug!(
                        keyspace = %table.keyspace,
                        table = %table.table,
                        peer_ring_id = ring_id,
                        %host_id,
                        "self-heal: replica peer returned empty digest — not a refill source"
                    );
                }
                ProbeOutcome::Unreachable => {
                    tracing::debug!(
                        keyspace = %table.keyspace,
                        table = %table.table,
                        peer_ring_id = ring_id,
                        %host_id,
                        "self-heal: replica peer unreachable / probe failed"
                    );
                }
            }
        }

        // Peers exist but none verified a non-empty copy → no healthy replica.
        tracing::warn!(
            keyspace = %table.keyspace,
            table = %table.table,
            peers = peers.len(),
            "self-heal: no peer verified a non-empty copy of table — \
             posture=NoHealthyReplica (quarantine will be refused; FMEA #1)"
        );
        ReplicaPosture::NoHealthyReplica
    }
}

/// Production [`RepairProbe`] backed by the existing repair Merkle RPC.
///
/// Wraps `PeerManager` and blocks on [`RemoteRepairStore::build_merkle`] via a
/// tokio runtime handle. The wiring is intentionally thin: it constructs the
/// same `RemoteRepairStore` the executor uses, builds a Merkle tree for the
/// range, and treats a non-zero root hash (non-empty content) as a verified
/// healthy copy. A send/RPC error or an empty (zero-root) tree is a non-source.
///
/// `build_merkle` is async and the `ClusterView` method is sync, so this blocks
/// on the future using the supplied [`tokio::runtime::Handle`]. The binary
/// supplies the node's runtime handle; the probe must therefore be called from
/// a blocking context (the self-heal controller already runs its tick on a
/// dedicated task and the probe is fire-and-verify, not on a hot path).
pub struct RpcRepairProbe {
    peer_manager: Arc<PeerManager>,
    runtime: tokio::runtime::Handle,
}

impl RpcRepairProbe {
    /// Construct from the node's `PeerManager` and runtime handle.
    pub fn new(peer_manager: Arc<PeerManager>, runtime: tokio::runtime::Handle) -> Self {
        Self {
            peer_manager,
            runtime,
        }
    }
}

impl RepairProbe for RpcRepairProbe {
    fn probe_digest(&self, peer: Uuid, table: &TableId, range: (i64, i64)) -> ProbeOutcome {
        let remote = RemoteRepairStore {
            host_id: peer,
            peer_manager: self.peer_manager.clone(),
        };
        let table = table.clone();
        let (start, end) = range;
        // Block on the async Merkle RPC. `block_in_place` releases the current
        // worker thread so we don't deadlock the runtime while waiting.
        let result = tokio::task::block_in_place(|| {
            self.runtime
                .block_on(async move { remote.build_merkle(&table, start, end).await })
        });
        match result {
            Ok(tree) if tree.root_hash() != 0 => ProbeOutcome::HealthyNonEmpty,
            Ok(_) => ProbeOutcome::Empty,
            Err(e) => {
                tracing::debug!(%peer, %e, "self-heal probe: build_merkle RPC failed");
                ProbeOutcome::Unreachable
            }
        }
    }
}

/// Production [`ClusterTopology`] over the live `TokenRing`.
///
/// The binary supplies the shared ring (typically `Arc<RwLock<TokenRing>>`)
/// behind a snapshot closure plus this node's `u64` ring id, so the view always
/// reflects current membership. `replica_peers` returns the table's eligible
/// replica peers (excluding self), mapping each `u64` ring id to its `host_id`
/// UUID for the probe.
pub struct RingTopology<F>
where
    F: Fn() -> crate::ring::TokenRing + Send + Sync,
{
    this_node_id: u64,
    /// Replication factor used to scope the replica set. The binary passes the
    /// keyspace RF (per FMEA #11 — not a hardcoded constant).
    rf: usize,
    snapshot: F,
}

impl<F> RingTopology<F>
where
    F: Fn() -> crate::ring::TokenRing + Send + Sync,
{
    /// Construct from this node's ring id, the replication factor, and a
    /// closure that snapshots the current ring.
    pub fn new(this_node_id: u64, rf: usize, snapshot: F) -> Self {
        Self {
            this_node_id,
            rf,
            snapshot,
        }
    }
}

impl<F> ClusterTopology for RingTopology<F>
where
    F: Fn() -> crate::ring::TokenRing + Send + Sync,
{
    fn this_node_id(&self) -> u64 {
        self.this_node_id
    }

    fn owners(&self, _table: &TableKey) -> Option<Vec<u64>> {
        // Ownership is range-based; for the posture gate (and the controller's
        // initiator selection) the relevant owner set is the table's replica
        // nodes. We report the eligible replica node ids for a representative
        // (full-ring) probe — the controller only uses this to pick the
        // lowest-id initiator, which is stable across the ring.
        let ring = (self.snapshot)();
        let mut owners: Vec<u64> = ring
            .node_ids()
            .into_iter()
            .filter(|&n| {
                ring.get_node(n).is_some_and(|info| match info.state {
                    crate::raft::NodeState::Normal => true,
                    crate::raft::NodeState::Learner { owns_tokens } => owns_tokens,
                    _ => false,
                })
            })
            .collect();
        if owners.is_empty() {
            return None;
        }
        owners.sort_unstable();
        Some(owners)
    }

    fn replica_peers(&self, _table: &TableKey) -> Vec<(u64, Uuid)> {
        let ring = (self.snapshot)();
        // Eligible replica nodes other than self, in ascending ring-id order.
        // We treat the table's replica set as the eligible nodes up to RF;
        // probing any one that verifies a non-empty copy satisfies FMEA #1.
        let mut peers: Vec<(u64, Uuid)> = ring
            .node_ids()
            .into_iter()
            .filter(|&n| n != self.this_node_id)
            .filter_map(|n| {
                ring.get_node(n).and_then(|info| {
                    let eligible = match info.state {
                        crate::raft::NodeState::Normal => true,
                        crate::raft::NodeState::Learner { owns_tokens } => owns_tokens,
                        _ => false,
                    };
                    eligible.then_some((n, info.host_id))
                })
            })
            .collect();
        peers.sort_by_key(|(n, _)| *n);
        // Scope to at most RF-1 peers (the other replicas besides self).
        let cap = self.rf.saturating_sub(1);
        peers.truncate(cap);
        peers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock topology with a fixed peer list and owner set.
    struct MockTopology {
        this: u64,
        owners: Option<Vec<u64>>,
        peers: Vec<(u64, Uuid)>,
    }
    impl ClusterTopology for MockTopology {
        fn this_node_id(&self) -> u64 {
            self.this
        }
        fn owners(&self, _t: &TableKey) -> Option<Vec<u64>> {
            self.owners.clone()
        }
        fn replica_peers(&self, _t: &TableKey) -> Vec<(u64, Uuid)> {
            self.peers.clone()
        }
    }

    /// Mock probe returning a per-peer canned outcome, recording calls.
    struct MockProbe {
        outcomes: HashMap<Uuid, ProbeOutcome>,
        probed: Mutex<Vec<Uuid>>,
    }
    impl MockProbe {
        fn new(outcomes: Vec<(Uuid, ProbeOutcome)>) -> Self {
            Self {
                outcomes: outcomes.into_iter().collect(),
                probed: Mutex::new(Vec::new()),
            }
        }
    }
    impl RepairProbe for MockProbe {
        fn probe_digest(&self, peer: Uuid, _t: &TableId, _r: (i64, i64)) -> ProbeOutcome {
            self.probed.lock().unwrap().push(peer);
            self.outcomes
                .get(&peer)
                .copied()
                .unwrap_or(ProbeOutcome::Unreachable)
        }
    }

    fn view(topology: MockTopology, probe: MockProbe) -> ClusterRepairView {
        ClusterRepairView::new(Arc::new(topology), Arc::new(probe))
    }

    #[test]
    fn verified_non_empty_peer_yields_healthy_posture() {
        let peer = Uuid::new_v4();
        let v = view(
            MockTopology {
                this: 1,
                owners: Some(vec![1, 2]),
                peers: vec![(2, peer)],
            },
            MockProbe::new(vec![(peer, ProbeOutcome::HealthyNonEmpty)]),
        );
        assert_eq!(
            v.replica_posture(&TableKey::new("ks", "t")),
            ReplicaPosture::HealthyReplicaAvailable
        );
    }

    #[test]
    fn no_peer_yields_single_node_posture() {
        let v = view(
            MockTopology {
                this: 1,
                owners: None,
                peers: vec![],
            },
            MockProbe::new(vec![]),
        );
        assert_eq!(
            v.replica_posture(&TableKey::new("ks", "t")),
            ReplicaPosture::SingleNode
        );
    }

    #[test]
    fn unreachable_peer_yields_no_healthy_replica() {
        let peer = Uuid::new_v4();
        let v = view(
            MockTopology {
                this: 1,
                owners: Some(vec![1, 2]),
                peers: vec![(2, peer)],
            },
            MockProbe::new(vec![(peer, ProbeOutcome::Unreachable)]),
        );
        assert_eq!(
            v.replica_posture(&TableKey::new("ks", "t")),
            ReplicaPosture::NoHealthyReplica
        );
    }

    #[test]
    fn empty_digest_peer_yields_no_healthy_replica() {
        let peer = Uuid::new_v4();
        let v = view(
            MockTopology {
                this: 1,
                owners: Some(vec![1, 2]),
                peers: vec![(2, peer)],
            },
            MockProbe::new(vec![(peer, ProbeOutcome::Empty)]),
        );
        assert_eq!(
            v.replica_posture(&TableKey::new("ks", "t")),
            ReplicaPosture::NoHealthyReplica
        );
    }

    #[test]
    fn first_healthy_peer_short_circuits_in_deterministic_order() {
        // peer A (ring id 2) is empty, peer B (ring id 3) is healthy. Probe
        // order is the topology's order; both must be tried and the healthy
        // one wins.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let probe = Arc::new(MockProbe::new(vec![
            (a, ProbeOutcome::Empty),
            (b, ProbeOutcome::HealthyNonEmpty),
        ]));
        let v = ClusterRepairView::new(
            Arc::new(MockTopology {
                this: 1,
                owners: Some(vec![1, 2, 3]),
                peers: vec![(2, a), (3, b)],
            }),
            probe.clone(),
        );
        assert_eq!(
            v.replica_posture(&TableKey::new("ks", "t")),
            ReplicaPosture::HealthyReplicaAvailable
        );
        // Both peers were probed (A empty, then B healthy).
        assert_eq!(probe.probed.lock().unwrap().as_slice(), &[a, b]);
    }

    #[test]
    fn cluster_view_owners_and_this_host_delegate_to_topology() {
        let v = view(
            MockTopology {
                this: 7,
                owners: Some(vec![3, 7, 9]),
                peers: vec![],
            },
            MockProbe::new(vec![]),
        );
        assert_eq!(v.this_host(), 7);
        assert_eq!(v.owners(&TableKey::new("ks", "t")), Some(vec![3, 7, 9]));
    }

    // ---- RingTopology over a real TokenRing ----

    use crate::raft::{NodeInfo, NodeState};
    use crate::ring::TokenRing;

    fn node(addr: &str, state: NodeState) -> (Uuid, NodeInfo) {
        let host_id = Uuid::new_v4();
        (
            host_id,
            NodeInfo {
                host_id,
                addr: addr.to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state,
                cql_broadcast: None,
            },
        )
    }

    #[test]
    fn ring_topology_returns_other_eligible_peers_capped_by_rf() {
        let (_h1, n1) = node("10.0.0.1:7000", NodeState::Normal);
        let (h2, n2) = node("10.0.0.2:7000", NodeState::Normal);
        let (h3, n3) = node("10.0.0.3:7000", NodeState::Normal);
        let (_h4, n4) = node("10.0.0.4:7000", NodeState::Learner { owns_tokens: false });
        let mut ring = TokenRing::new();
        ring.add_node(1, n1);
        ring.add_node(2, n2);
        ring.add_node(3, n3);
        ring.add_node(4, n4);

        let topo = RingTopology::new(1, 3, move || ring.clone());
        let peers = topo.replica_peers(&TableKey::new("ks", "t"));
        // self (1) excluded, learner-without-tokens (4) excluded, RF=3 → 2 peers.
        assert_eq!(peers, vec![(2, h2), (3, h3)]);
        // owners excludes the witness learner but includes self.
        let mut owners = topo.owners(&TableKey::new("ks", "t")).unwrap();
        owners.sort_unstable();
        assert_eq!(owners, vec![1, 2, 3]);
    }

    #[test]
    fn ring_topology_single_node_has_no_peers() {
        let (_h1, n1) = node("10.0.0.1:7000", NodeState::Normal);
        let mut ring = TokenRing::new();
        ring.add_node(1, n1);
        let topo = RingTopology::new(1, 1, move || ring.clone());
        assert!(topo.replica_peers(&TableKey::new("ks", "t")).is_empty());
    }
}
