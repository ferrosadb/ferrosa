//! Anti-entropy repair scheduler — drives `RepairSession` runs across every
//! `(table, owned-range, peer)` triple and reports per-session results.
//!
//! ## Design
//!
//! The coordinator is parameterised by a [`SessionExecutor`] trait so the
//! scheduling logic can be unit-tested in isolation from the RPC + streaming
//! machinery that actually runs a session against a remote peer.
//!
//! For a given (ring, local_node, rf, table):
//!
//! 1. Enumerate every token range the local node is a replica of, using the
//!    ring's vnode topology.
//! 2. For each range, look up the peer replicas via [`super::repair_participants`].
//!    Skip self.
//! 3. For each `(range, peer)` pair, invoke the executor. Cap concurrency
//!    via a tokio semaphore.
//! 4. Collect per-session results and return.
//!
//! The actual RPC + streaming impl of `SessionExecutor` lives in a follow-up
//! PR alongside the wire-protocol additions.

use async_trait::async_trait;
use std::sync::Arc;

use ferrosa_net::task_pool::TaskPool;
use ferrosa_storage::TableId;

use crate::ring::TokenRing;

/// Statistics returned by a single repair session between two replicas.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionStats {
    /// Partitions streamed from the remote peer to the local node.
    pub partitions_streamed_in: u64,
    /// Partitions streamed from the local node to the remote peer.
    pub partitions_streamed_out: u64,
    /// Partition keys with identical timestamps and divergent content —
    /// surfaced to the operator, NOT auto-resolved.
    pub timestamp_ties: u64,
}

/// A `(table, range, peer)` triple scheduled for repair plus the
/// [`SessionStats`] (or error) the executor returned.
#[derive(Debug)]
pub struct SessionResult {
    pub table: TableId,
    pub range_start: i64,
    pub range_end: i64,
    pub peer: u64,
    pub result: Result<SessionStats, String>,
}

/// What one table's *initiated* repair produced: the per-`(merged range, peer)`
/// sessions, plus how many of this node's own ranges had a live peer at all.
///
/// The second field exists so an EMPTY session list is not reported as "nothing
/// was verified" for every replica. An empty list means one of two benign
/// things, and only the caller that sees this count can tell them apart — see
/// `NoSessionsReason` in [`super::scheduler`].
#[derive(Debug)]
pub struct InitiatedRepair {
    /// One entry per `(range, peer)` session attempted.
    pub results: Vec<SessionResult>,
    /// Ranges this node owns that have at least one live peer. `0` means there
    /// is no replica to compare against, so no session was possible.
    pub ranges_with_live_peers: usize,
}

/// Executes a single repair session between the local node and one peer
/// over one token range of one table.
///
/// The production implementation runs the Merkle exchange + partition diff
/// + streaming over the internode RPC channel. Tests inject mocks that
/// record their inputs without actually doing any I/O.
#[async_trait]
pub trait SessionExecutor: Send + Sync {
    async fn run_session(
        &self,
        table: &TableId,
        range_start: i64,
        range_end: i64,
        peer: u64,
    ) -> Result<SessionStats, String>;
}

/// Coordinator that fans repair sessions out across every (range, peer)
/// pair for a given table.
pub struct RepairCoordinator {
    /// Max concurrent sessions in flight. Bounded so a node with many
    /// vnodes × many peers doesn't open hundreds of simultaneous streams.
    pub max_concurrent_sessions: usize,
}

impl Default for RepairCoordinator {
    fn default() -> Self {
        Self {
            // 4 matches Cassandra's per-node default. With the
            // `read_token_range` primitive wiring (only materialises
            // partitions in the requested token sub-range, not a full
            // key-ordered prefix), the per-session working set is bounded
            // by leaf cardinality — typically << 1 partition per leaf at
            // TREE_DEPTH=15 — so 4 concurrent sessions fit comfortably in
            // the 2 GB fmem container.
            max_concurrent_sessions: 4,
        }
    }
}

/// One token range paired with the peers (excluding self) to reconcile over it —
/// the unit [`merge_contiguous_same_peers`] operates on, and the shape both the
/// table-wide and the initiated repair paths build before scheduling.
pub(crate) type RangeWithPeers = (i64, i64, Vec<u64>);

/// Collapse adjacent token ranges that share a replica set into one span.
///
/// Every `(range, peer)` pair inside such a group does identical work, so
/// issuing N×peers wire calls where 1×peers would suffice only amplifies load on
/// the peer — and on the local CQL listener, which has crashed in the past from
/// a 1 536-session storm on RF=3 / 3-node.
///
/// Sorts by `start` first, so the wrap segment `[i64::MIN, first_token)` — which
/// [`owned_token_ranges`] emits last — lands before the body and can merge with
/// it where the replica set agrees. Merges only where the span is **contiguous**
/// (`last.end == start`) AND the **peer set is identical**: merging across a
/// differing replica set would run a session for a range the peer does not hold.
///
/// The peers are canonicalised (sorted) here, because "the same replica set" is a
/// property of the SET: `repair_table` pre-sorted its peers, but
/// [`select_initiated_ranges`] does not, so comparing `Vec`s directly would make
/// the merge depend on the order `repair_participants` happened to enumerate them
/// in — and silently collapse nothing when two adjacent ranges enumerating the
/// same replicas in different orders.
pub(crate) fn merge_contiguous_same_peers(mut ranges: Vec<RangeWithPeers>) -> Vec<RangeWithPeers> {
    ranges.sort_by_key(|(start, _, _)| *start);
    let mut merged: Vec<RangeWithPeers> = Vec::new();
    for (range_start, range_end, mut peers) in ranges {
        peers.sort_unstable();
        if let Some(last) = merged.last_mut() {
            if last.1 == range_start && last.2 == peers {
                // Same replica set AND contiguous in token order.
                last.1 = range_end;
                continue;
            }
        }
        merged.push((range_start, range_end, peers));
    }
    merged
}

impl RepairCoordinator {
    /// Repair `table` across every owned token range against every peer
    /// participant. Returns one [`SessionResult`] per `(range, peer)`
    /// invocation, in arbitrary order.
    pub async fn repair_table(
        &self,
        executor: Arc<dyn SessionExecutor>,
        ring: &TokenRing,
        local_node_id: u64,
        rf: usize,
        table: &TableId,
    ) -> Vec<SessionResult> {
        // Build the work list synchronously. We collapse adjacent
        // owned vnode ranges that share the same replica set into a
        // single session — every (range, peer) pair within such a
        // group does identical work, so issuing N×peers wire calls
        // when 1×peers would suffice is just amplifying load on the
        // peer (and the local CQL listener, which has crashed in the
        // past from a 1 536-session storm on RF=3 / 3-node).
        let owned_ranges = owned_token_ranges(ring, local_node_id, rf);
        // (start, end, peers_excluding_self_sorted).
        let owned_with_peers: Vec<RangeWithPeers> = owned_ranges
            .into_iter()
            .map(|(s, e)| {
                let mut peers: Vec<u64> = super::repair_participants(ring, s, rf)
                    .into_iter()
                    .filter(|p| *p != local_node_id)
                    .collect();
                peers.sort_unstable();
                (s, e, peers)
            })
            .collect();
        let merged = merge_contiguous_same_peers(owned_with_peers);
        // One session per (merged-range, peer). Per-session memory
        // is now bounded INSIDE the session by the Merkle-then-
        // stream shape: build a streaming Merkle tree of the local
        // replica (bounded by `MERKLE_BUILD_BATCH`), exchange leaf
        // hashes with the peer, and only fetch partitions for the
        // small subset of leaves that actually differ. So the
        // coordinator no longer needs to pre-split a merged range
        // into K artificial chunks — the diff itself does the
        // chunking, scaled to actual divergence rather than worst-
        // case partition distribution.
        let mut tasks: Vec<(i64, i64, u64)> = Vec::new();
        for (range_start, range_end, peers) in merged {
            for &peer in &peers {
                tasks.push((range_start, range_end, peer));
            }
        }

        self.run_session_tasks(executor, table, tasks).await
    }

    /// Repair **only the token ranges this node is the deterministic initiator
    /// for** (lowest live host_id among the range's replicas — see
    /// [`select_initiated_ranges`](super::scheduler::select_initiated_ranges)),
    /// against that range's live peers.
    ///
    /// This is the entry point the automatic-repair scheduler uses: unlike
    /// [`Self::repair_table`] (which repairs *every* owned range and is meant to
    /// be triggered on a single node), this is safe to run on **every** node
    /// simultaneously — each range is initiated exactly once (FMEA #1, no herd).
    /// Like [`Self::repair_table`] it collapses contiguous ranges that share a
    /// replica set into one session per replica set before scheduling, so the
    /// automatic path cannot storm a peer (CL-64).
    pub async fn repair_initiated(
        &self,
        executor: Arc<dyn SessionExecutor>,
        ring: &TokenRing,
        local_node_id: u64,
        rf: usize,
        table: &TableId,
    ) -> InitiatedRepair {
        // How many of this node's own ranges had a peer to compare against at
        // all. Returned beside the sessions so a caller reporting an empty cycle
        // can say WHICH benign situation it is in rather than calling every
        // replica unverified (`NoSessionsReason`).
        let ranges_with_live_peers =
            super::scheduler::owned_ranges_with_live_peers(ring, local_node_id, rf);
        let initiated = super::scheduler::select_initiated_ranges(ring, local_node_id, rf);
        let mut tasks: Vec<(i64, i64, u64)> = Vec::new();
        for (start, end, peers) in merge_contiguous_same_peers(
            initiated
                .into_iter()
                .map(|r| (r.start, r.end, r.peers))
                .collect(),
        ) {
            for peer in peers {
                tasks.push((start, end, peer));
            }
        }
        InitiatedRepair {
            results: self.run_session_tasks(executor, table, tasks).await,
            ranges_with_live_peers,
        }
    }

    /// Fan out one repair session per `(range_start, range_end, peer)` task,
    /// bounded by `max_concurrent_sessions`. Shared by [`Self::repair_table`]
    /// and [`Self::repair_initiated`].
    async fn run_session_tasks(
        &self,
        executor: Arc<dyn SessionExecutor>,
        table: &TableId,
        tasks: Vec<(i64, i64, u64)>,
    ) -> Vec<SessionResult> {
        // Bound parallelism with a semaphore.
        let sem = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent_sessions));
        let mut handles = Vec::with_capacity(tasks.len());
        for (range_start, range_end, peer) in tasks {
            let table = table.clone();
            let executor = executor.clone();
            let sem = sem.clone();
            handles.push(TaskPool::current("repair-session").spawn(async move {
                // OwnedSemaphorePermit: held for the body of run_session,
                // released on drop. Bound on the runtime queue, not on
                // throughput of any single session.
                let _permit = sem.acquire_owned().await.expect("semaphore not closed");
                let result = executor
                    .run_session(&table, range_start, range_end, peer)
                    .await;
                SessionResult {
                    table,
                    range_start,
                    range_end,
                    peer,
                    result,
                }
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for h in handles {
            // A panicking session is reported as an error rather than
            // aborting the whole repair run.
            match h.await {
                Ok(r) => results.push(r),
                Err(e) => {
                    // We can't reconstruct the (range, peer) from a panic,
                    // so use sentinel values. Tests treat this as
                    // unexpected; production logs it.
                    results.push(SessionResult {
                        table: table.clone(),
                        range_start: 0,
                        range_end: 0,
                        peer: 0,
                        result: Err(format!("session task panicked: {e}")),
                    });
                }
            }
        }
        results
    }
}

/// Token ranges this node is a replica of, as `[start, end)` pairs over the
/// vnode partitioning. Excludes ranges where the local node isn't in the
/// replica set for `rf`. For the wrap-around segment (last token → first
/// token via i64::MIN/MAX), we emit TWO ranges so callers don't have to
/// special-case it.
pub(crate) fn owned_token_ranges(
    ring: &TokenRing,
    local_node_id: u64,
    rf: usize,
) -> Vec<(i64, i64)> {
    let mut all_tokens: Vec<i64> = ring
        .node_ids()
        .iter()
        .flat_map(|&n| ring.tokens_for_node(n))
        .collect();
    all_tokens.sort_unstable();
    all_tokens.dedup();
    if all_tokens.is_empty() {
        return vec![];
    }

    let mut ranges = Vec::new();
    for (i, &start) in all_tokens.iter().enumerate() {
        let end_token = all_tokens.get(i + 1).copied().unwrap_or(i64::MAX); // wrap segment continues to i64::MAX...
                                                                            // A range belongs to the local node if local_node_id is in the
                                                                            // replica set of the range's start token.
        if ring.replicas(start, rf).contains(&local_node_id) {
            ranges.push((start, end_token));
        }
    }
    // ... and from i64::MIN to the smallest token, if that wrap segment's
    // replicas include local.
    let first = all_tokens[0];
    if first != i64::MIN && ring.replicas(i64::MIN, rf).contains(&local_node_id) {
        ranges.push((i64::MIN, first));
    }
    ranges
}

/// Ring membership health, derived from node states alone.
///
/// A member that is not [`crate::raft::NodeState::Normal`] is either on its way in
/// (`Joining`), on its way out (`Leaving` / `Decommissioned`), or a
/// token-owning learner. `TokenRing::replicas()` excludes such members, so
/// their tokens silently land on other nodes. That is a *degraded* ring, but
/// nothing in the system said so — the failure was found by hand.
///
/// This type makes it sayable. See `joining_node_health.rs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RingHealth {
    /// `(node_id, state)` for every member that is not `Normal`, ordered by
    /// node_id. A non-empty list means the cluster is NOT healthy.
    pub non_normal_members: Vec<(u64, crate::raft::NodeState)>,
    /// Total members known to the ring (Normal and non-Normal alike).
    pub total_members: usize,
}

impl RingHealth {
    /// True when any member is not `Normal` — i.e. the ring is degraded and
    /// some tokens may be served by a node that does not own them.
    pub fn has_non_normal_members(&self) -> bool {
        !self.non_normal_members.is_empty()
    }

    /// True only for a fully-`Normal` ring. This is the signal that MUST NOT
    /// be silent: a cluster reporting unhealthy here is misrouting replicas.
    pub fn is_healthy(&self) -> bool {
        self.non_normal_members.is_empty()
    }
}

/// Snapshot ring health from the token ring's node states.
///
/// INVARIANT: `health.has_non_normal_members()` is true **iff** some member
/// is not `Normal`. A silent degraded ring is the defect this exists to
/// surface (`ring_health(all_normal).is_healthy() == true` is equally
/// required — a signal that always fires is noise).
pub fn ring_health(ring: &TokenRing) -> RingHealth {
    let mut non_normal_members: Vec<(u64, crate::raft::NodeState)> = ring
        .node_ids()
        .into_iter()
        .filter_map(|id| {
            ring.get_node(id)
                .map(|info| (id, info.state))
                .filter(|(_, st)| !matches!(st, crate::raft::NodeState::Normal))
        })
        .collect();
    non_normal_members.sort_unstable_by_key(|(id, _)| *id);
    RingHealth {
        non_normal_members,
        total_members: ring.node_ids().len(),
    }
}

/// Node ids that own ring tokens but are excluded from `replicas()` purely
/// because of their state.
///
/// These nodes are the concrete misroute: their token ranges are handed to
/// other nodes, so a coordinator writing "for" them actually writes
/// elsewhere, and repair from their perspective sees no ranges at all.
///
/// INVARIANT: for every id in the result, `ring.tokens_for_node(id)` is
/// non-empty AND `replicas()` skips it for at least one of its own tokens.
pub fn ring_data_scatter_risk(ring: &TokenRing) -> Vec<u64> {
    let mut at_risk: Vec<u64> = ring_health(ring)
        .non_normal_members
        .into_iter()
        .filter_map(|(id, _)| {
            let mut owned = ring.tokens_for_node(id);
            if owned.is_empty() {
                return None;
            }
            owned.sort_unstable();
            // The node is at risk when a token it owns resolves to a replica
            // set that does not include it.
            let misrouted = owned
                .iter()
                .any(|&token| !ring.replicas(token, 1).contains(&id));
            misrouted.then_some(id)
        })
        .collect();
    at_risk.sort_unstable();
    at_risk
}

/// Plan the Promote phase: one `SetNodeState { Normal }` per member still in
/// `Joining` WHOSE BOOTSTRAP IS RECORDED COMPLETE, in node_id order.
///
/// `Joining` is the only state the Promote phase moves forward. `Leaving` and
/// `Decommissioned` are deliberate operator intent and MUST NOT be promoted
/// back, and learners are a distinct state machine (ADR-014), not joiners.
///
/// P0-4: a `Joining` member is promoted only when `bootstrap_complete` holds
/// its committed `RecordBootstrapComplete`. Without it the member might be a
/// join whose stream a restart cut off; promoting it would make it a replica
/// without its data. Such members are reported by
/// [`joiners_awaiting_bootstrap`] and stay `Joining` until their bootstrap
/// reruns. Members already `Normal` are grandfathered and need no record.
///
/// INVARIANT: `plan` contains exactly one op per recorded `Joining` member,
/// and is empty when there is none (no needless Raft churn).
pub fn promote_joining_members(
    members: &std::collections::BTreeMap<u64, crate::raft::NodeState>,
    bootstrap_complete: &std::collections::BTreeSet<u64>,
) -> Vec<crate::raft::RaftOp> {
    members
        .iter()
        .filter(|(id, st)| {
            matches!(st, crate::raft::NodeState::Joining) && bootstrap_complete.contains(id)
        })
        .map(|(&node_id, _)| crate::raft::RaftOp::SetNodeState {
            node_id,
            state: crate::raft::NodeState::Normal,
        })
        .collect()
}

/// `Joining` members with no bootstrap-complete record: their data has not
/// been verified, so the promote pass must NOT move them to `Normal`. They
/// stay `Joining` until their bootstrap reruns and records completion.
pub fn joiners_awaiting_bootstrap(
    members: &std::collections::BTreeMap<u64, crate::raft::NodeState>,
    bootstrap_complete: &std::collections::BTreeSet<u64>,
) -> Vec<u64> {
    members
        .iter()
        .filter(|(id, st)| {
            matches!(st, crate::raft::NodeState::Joining) && !bootstrap_complete.contains(id)
        })
        .map(|(&id, _)| id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeInfo, NodeState};
    use std::sync::Mutex;
    use uuid::Uuid;

    /// `node_with`, but with a FIXED `host_id` so the initiator election
    /// (`min(host_id)` among a range's live replicas) is deterministic and a test
    /// can pin an exact expected work list.
    fn node_with_host_id(addr: &str, state: NodeState, host_id: u128) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::from_u128(host_id),
            ..node_with(addr, state)
        }
    }

    fn node_with(addr: &str, state: NodeState) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state,
            cql_broadcast: None,
        }
    }

    /// Mock executor that records every invocation and returns the
    /// configured stats. Lets tests assert on the (range, peer) work
    /// the coordinator scheduled without doing any real I/O.
    struct MockExecutor {
        calls: Mutex<Vec<(TableId, i64, i64, u64)>>,
        next_stats: SessionStats,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                next_stats: SessionStats::default(),
            }
        }
    }

    #[async_trait]
    impl SessionExecutor for MockExecutor {
        async fn run_session(
            &self,
            table: &TableId,
            range_start: i64,
            range_end: i64,
            peer: u64,
        ) -> Result<SessionStats, String> {
            self.calls
                .lock()
                .unwrap()
                .push((table.clone(), range_start, range_end, peer));
            Ok(self.next_stats.clone())
        }
    }

    fn three_node_ring() -> TokenRing {
        let mut ring = TokenRing::new();
        ring.add_node(1, node_with("10.0.0.1:7000", NodeState::Normal));
        ring.add_node(2, node_with("10.0.0.2:7000", NodeState::Normal));
        ring.add_node(3, node_with("10.0.0.3:7000", NodeState::Normal));
        ring.assign_tokens(1, &[100, 400, 700]);
        ring.assign_tokens(2, &[200, 500, 800]);
        ring.assign_tokens(3, &[300, 600, 900]);
        ring
    }

    /// FMEA #1 at the coordinator level: when every node runs `repair_initiated`
    /// (the automatic path), each token range is repaired by exactly ONE
    /// initiator — not RF times. So exactly one of the three nodes schedules
    /// sessions; the other two schedule none, and each of them reports a
    /// non-zero `ranges_with_live_peers`, which is what lets the scheduler say
    /// "another replica is the initiator" instead of "nothing was verified".
    #[tokio::test]
    async fn repair_initiated_runs_each_range_once_across_all_nodes() {
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        let coord = RepairCoordinator {
            max_concurrent_sessions: 4,
        };
        let mut nonempty = 0usize;
        let mut total = 0usize;
        for node in [1u64, 2, 3] {
            let exec = Arc::new(MockExecutor::new());
            let initiated = coord
                .repair_initiated(exec.clone(), &ring, node, 3, &table)
                .await;
            assert!(
                initiated.ranges_with_live_peers > 0,
                "node {node} owns ranges with live peers, so `not an initiator` is the honest \
                 report for the nodes that schedule nothing"
            );
            if !initiated.results.is_empty() {
                nonempty += 1;
            }
            total += initiated.results.len();
        }
        assert_eq!(
            nonempty, 1,
            "exactly one node initiates each range (no herd)"
        );
        assert!(total > 0, "the sole initiator scheduled its sessions");
    }

    /// Union of a set of `[start, end)` spans, as the minimal sorted list of
    /// disjoint spans covering exactly the same token space. Used to state the
    /// merge's coverage invariant as VALUES (exact spans), not as a count.
    fn union_of_spans(mut spans: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
        spans.sort_unstable();
        let mut out: Vec<(i64, i64)> = Vec::new();
        for (s, e) in spans {
            match out.last_mut() {
                Some(last) if s <= last.1 => last.1 = last.1.max(e),
                _ => out.push((s, e)),
            }
        }
        out
    }

    /// t_af51a55c: the AUTOMATIC path must collapse work exactly as
    /// [`RepairCoordinator::repair_table`] does. Measured live: **769 ranges ×
    /// 2 peers = 1 538 sessions per tick**, because `repair_initiated` emitted
    /// one session per `(range, peer)` instead of one per replica set — the very
    /// storm the coordinator's own comment records as having crashed the local
    /// CQL listener (1 536 sessions on RF=3 / 3-node).
    ///
    /// RED as written: the count assertion fails on the CURRENT code, which
    /// already schedules sessions — it fails on the BEHAVIOUR (too many, each
    /// covering one vnode range), not on a missing symbol.
    #[tokio::test]
    async fn repair_initiated_merges_into_one_session_per_replica_set() {
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        let coord = RepairCoordinator {
            max_concurrent_sessions: 4,
        };

        // Exactly one node is the initiator for every range in this ring, so
        // find it rather than assuming the fixture's host_id order.
        let mut scheduling = Vec::new();
        let mut selected_spans: Vec<(i64, i64)> = Vec::new();
        for node in [1u64, 2, 3] {
            let exec = Arc::new(MockExecutor::new());
            let initiated = coord
                .repair_initiated(exec.clone(), &ring, node, 3, &table)
                .await;
            let calls: Vec<(i64, i64, u64)> = exec
                .calls
                .lock()
                .unwrap()
                .iter()
                .map(|(_, s, e, p)| (*s, *e, *p))
                .collect();
            if !calls.is_empty() {
                selected_spans.extend(
                    crate::repair::scheduler::select_initiated_ranges(&ring, node, 3)
                        .into_iter()
                        .map(|r| (r.start, r.end)),
                );
                assert_eq!(
                    initiated.results.len(),
                    calls.len(),
                    "one SessionResult per scheduled session"
                );
                scheduling.push((node, calls));
            } else {
                assert!(
                    initiated.results.is_empty(),
                    "node {node} initiates nothing, so it must schedule nothing \
                     (the merge must not invent work)"
                );
            }
        }

        assert_eq!(scheduling.len(), 1, "exactly one initiator (no herd)");
        let (node, calls) = &scheduling[0];

        // The whole point: one session per non-self replica, not per range.
        assert_eq!(
            calls.len(),
            2,
            "node {node}: RF=3/3-node must merge into one session per non-self peer, \
             but scheduled {} sessions ({} selected ranges × 2 peers)",
            calls.len(),
            selected_spans.len()
        );
        // VALUES, not just the count: each merged session covers the full ring.
        for (s, e, p) in calls.iter() {
            assert_eq!(
                (*s, *e),
                (i64::MIN, i64::MAX),
                "the wrap segment must merge across the ring seam (peer {p})"
            );
        }
        let peers: std::collections::BTreeSet<u64> = calls.iter().map(|(_, _, p)| *p).collect();
        assert_eq!(
            peers,
            [1u64, 2, 3].iter().copied().filter(|n| n != node).collect(),
            "sessions run against the two replicas other than self, and never self"
        );

        // Coverage invariant: merging must not drop or invent token space.
        assert_eq!(
            union_of_spans(calls.iter().map(|(s, e, _)| (*s, *e)).collect()),
            union_of_spans(selected_spans),
            "the merged spans must cover exactly the token space that was selected"
        );
    }

    /// The OTHER half of the rule: the merge keys on the replica set, not on
    /// contiguity alone. Fusing `[400,500)` (peer 4) with `[500,600)` (peer 2)
    /// would run a session for token space that peer does not hold.
    ///
    /// Four nodes, RF=2, with **fixed** host_ids (node N → host_id N) so the
    /// election is deterministic, giving one node a contiguous pair of ranges
    /// with DIFFERENT replica sets. The assertion compares the cluster-wide work
    /// list against an oracle written from the RULE (group a node's selected
    /// ranges by peer set; merge maximal contiguous runs within each group), and
    /// the fixture asserts up front both that such a mismatched seam exists and
    /// that a legitimate merge does fire — so this guard cannot pass vacuously in
    /// either direction.
    #[tokio::test]
    async fn repair_initiated_never_merges_ranges_with_different_replica_sets() {
        let mut ring = TokenRing::new();
        for (id, addr) in [
            (1u64, "10.0.0.1:7000"),
            (2, "10.0.0.2:7000"),
            (3, "10.0.0.3:7000"),
            (4, "10.0.0.4:7000"),
        ] {
            ring.add_node(id, node_with_host_id(addr, NodeState::Normal, id as u128));
        }
        ring.assign_tokens(1, &[100, 500]);
        ring.assign_tokens(2, &[200, 600]);
        ring.assign_tokens(3, &[300, 700]);
        ring.assign_tokens(4, &[400, 800]);
        let table = TableId::new("ks", "tbl");
        let coord = RepairCoordinator {
            max_concurrent_sessions: 4,
        };

        let mut work: std::collections::BTreeSet<(i64, i64, u64)> =
            std::collections::BTreeSet::new();
        // Spec oracle: group by peer set, merge maximal contiguous runs within
        // each group. Written from the rule, not from the implementation.
        let mut expected: std::collections::BTreeSet<(i64, i64, u64)> =
            std::collections::BTreeSet::new();
        let mut one_per_range = 0usize;
        let mut selected: Vec<(i64, i64)> = Vec::new();
        // Precondition: at least one node initiates two ranges that are
        // contiguous in token order but have DIFFERENT replica sets — the only
        // shape in which a contiguity-only merge misfires.
        let mut mismatched_seams = 0usize;
        for node in [1u64, 2, 3, 4] {
            let exec = Arc::new(MockExecutor::new());
            coord
                .repair_initiated(exec.clone(), &ring, node, 2, &table)
                .await;
            work.extend(
                exec.calls
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(_, s, e, p)| (*s, *e, *p)),
            );

            let mut ranges = crate::repair::scheduler::select_initiated_ranges(&ring, node, 2);
            ranges.sort_by_key(|r| r.start);
            for w in ranges.windows(2) {
                if w[0].end == w[1].start && w[0].peers != w[1].peers {
                    mismatched_seams += 1;
                }
            }
            one_per_range += ranges.iter().map(|r| r.peers.len()).sum::<usize>();
            for r in &ranges {
                selected.push((r.start, r.end));
            }
            let mut i = 0;
            while i < ranges.len() {
                let mut peers = ranges[i].peers.clone();
                peers.sort_unstable();
                let mut end = ranges[i].end;
                let mut j = i + 1;
                while j < ranges.len() {
                    let mut next = ranges[j].peers.clone();
                    next.sort_unstable();
                    if ranges[j].start == end && next == peers {
                        end = ranges[j].end;
                        j += 1;
                    } else {
                        break;
                    }
                }
                for p in peers {
                    expected.insert((ranges[i].start, end, p));
                }
                i = j;
            }
        }
        assert!(
            mismatched_seams > 0,
            "fixture precondition: no node has a contiguous pair with differing \
             replica sets, so this guard cannot fire — fix the fixture, not the test"
        );
        assert!(
            expected.len() < one_per_range,
            "fixture precondition: no legitimate merge occurs in this ring \
             ({expected:?} vs {one_per_range} sessions), so 'merges nothing' is vacuous"
        );
        assert_eq!(
            work, expected,
            "the work list must be exactly one session per maximal contiguous run \
             of a node's selected ranges that share a replica set"
        );
        assert_eq!(
            union_of_spans(work.iter().map(|(s, e, _)| (*s, *e)).collect()),
            union_of_spans(selected),
            "and it must still cover exactly the selected token space"
        );
    }

    /// The merge predicate itself, on hand-made values: contiguity AND an
    /// identical peer set are both required, and unsorted input (the wrap
    /// segment `[i64::MIN, …)` arrives last from `owned_token_ranges`) must still
    /// merge across the ring seam.
    ///
    /// Characterisation suite for the rule extracted from `repair_table`'s body —
    /// it is not the red that produced the code (that is
    /// `repair_initiated_merges_into_one_session_per_replica_set`); its
    /// non-vacuity is shown by the contiguity-only negative control.
    #[test]
    fn merge_contiguous_same_peers_requires_both_contiguity_and_a_matching_replica_set() {
        let merge = super::merge_contiguous_same_peers;

        // Contiguous + identical peers → one span.
        assert_eq!(
            merge(vec![
                (10, 20, vec![2, 3]),
                (20, 30, vec![2, 3]),
                (30, 40, vec![2, 3]),
            ]),
            vec![(10, 40, vec![2, 3])]
        );
        // Contiguous but DIFFERENT peers → must not merge.
        assert_eq!(
            merge(vec![(10, 20, vec![2]), (20, 30, vec![3])]),
            vec![(10, 20, vec![2]), (20, 30, vec![3])]
        );
        // Contiguous + the SAME replica set enumerated in a different ORDER →
        // still ONE span: the peer set is a set, not a sequence. (This is what
        // `select_initiated_ranges` supplies — it does not pre-sort its peers.)
        assert_eq!(
            merge(vec![(10, 20, vec![3, 2]), (20, 30, vec![2, 3])]),
            vec![(10, 30, vec![2, 3])]
        );
        // Identical peers but a GAP → must not merge.
        assert_eq!(
            merge(vec![(10, 20, vec![2]), (25, 30, vec![2])]),
            vec![(10, 20, vec![2]), (25, 30, vec![2])]
        );
        // Unsorted, with the wrap segment last → merges across the ring seam.
        assert_eq!(
            merge(vec![
                (100, 200, vec![2, 3]),
                (i64::MIN, 100, vec![2, 3, 4]),
                (200, i64::MAX, vec![2, 3]),
            ]),
            vec![(i64::MIN, 100, vec![2, 3, 4]), (100, i64::MAX, vec![2, 3])]
        );
        // Empty in, empty out.
        assert_eq!(merge(vec![]), vec![]);
    }

    #[tokio::test]
    async fn repair_table_merges_ranges_sharing_the_same_replica_set() {
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        let exec = Arc::new(MockExecutor::new());
        let coord = RepairCoordinator {
            max_concurrent_sessions: 4,
        };
        // RF=3/3-node: every owned range has the same replica set, so
        // the coordinator merges them into one super-range per
        // replica-set group → one session per non-self peer (the
        // session is internally bounded-memory via Merkle-then-
        // stream, so no further chunking at the scheduler is needed).
        let results = coord.repair_table(exec.clone(), &ring, 1, 3, &table).await;
        let calls = exec.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            2,
            "RF=3/3-node merges all owned ranges into one session per non-self peer"
        );
        assert_eq!(results.len(), calls.len());
        assert!(results.iter().all(|r| r.result.is_ok()));
        let peers: std::collections::BTreeSet<u64> = calls.iter().map(|(_, _, _, p)| *p).collect();
        assert_eq!(peers, [2u64, 3].iter().copied().collect());
        // Each session covers the full ring `[i64::MIN, i64::MAX)`.
        for (_, s, e, _) in calls.iter() {
            assert_eq!(*s, i64::MIN);
            assert_eq!(*e, i64::MAX);
        }
    }

    #[tokio::test]
    async fn repair_table_skips_self() {
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        let exec = Arc::new(MockExecutor::new());
        let coord = RepairCoordinator::default();
        coord.repair_table(exec.clone(), &ring, 2, 3, &table).await;
        let calls = exec.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|(_, _, _, p)| *p == 2),
            "local node 2 must never be its own peer"
        );
    }

    #[tokio::test]
    async fn repair_table_empty_ring_returns_no_results() {
        let ring = TokenRing::new();
        let table = TableId::new("ks", "tbl");
        let exec = Arc::new(MockExecutor::new());
        let coord = RepairCoordinator::default();
        let results = coord.repair_table(exec.clone(), &ring, 1, 3, &table).await;
        assert!(results.is_empty());
        assert!(exec.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn repair_table_concurrency_cap_caps_in_flight() {
        // Custom executor that holds the OwnedSemaphorePermit while sleeping,
        // letting us observe that more than max_concurrent_sessions don't
        // run simultaneously.
        struct ConcurrencyTracker {
            in_flight: Arc<std::sync::atomic::AtomicUsize>,
            peak: Arc<std::sync::atomic::AtomicUsize>,
        }
        #[async_trait]
        impl SessionExecutor for ConcurrencyTracker {
            async fn run_session(
                &self,
                _table: &TableId,
                _: i64,
                _: i64,
                _: u64,
            ) -> Result<SessionStats, String> {
                let n = self
                    .in_flight
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                self.peak.fetch_max(n, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                self.in_flight
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                Ok(SessionStats::default())
            }
        }
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let exec: Arc<dyn SessionExecutor> = Arc::new(ConcurrencyTracker {
            in_flight: in_flight.clone(),
            peak: peak.clone(),
        });
        let coord = RepairCoordinator {
            max_concurrent_sessions: 2,
        };
        let ring = three_node_ring();
        let table = TableId::new("ks", "tbl");
        coord.repair_table(exec, &ring, 1, 3, &table).await;
        let observed_peak = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            observed_peak <= 2,
            "peak concurrency was {observed_peak}, must be <= 2"
        );
        assert!(
            observed_peak >= 2,
            "with 18 sessions and cap=2 we should hit the cap at some point; saw peak={observed_peak}"
        );
    }
}
