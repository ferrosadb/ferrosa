//! Reproduces P0-17 — Raft election storm when a follower's log diverges.
//!
//! ## What these tests demonstrate
//!
//! A follower whose log has fallen behind the cluster repeatedly fires
//! elections, bumps its term, and is rejected because the leader's
//! `last_log_id` is greater.  The term grows without bound while the
//! cluster is stable.  Observed in production: ~18 000 failed elections
//! over 32 hours, node3 at term T18 348 while leader stayed at T8.
//!
//! ## Test design
//!
//! Three real openraft instances share an in-process channel network (no
//! TCP, no ferrosa-net).  Node3's `AppendEntries` can be blocked via an
//! `AtomicBool` to simulate the partition that causes log divergence.
//! The `run_election_guard` task monitors node3's metrics and suppresses
//! elections when the storm signature is detected.
//!
//! Both tests:
//!   - `election_storm_does_not_occur_on_log_divergence` — block node3,
//!     write more entries, unblock, assert node3 converges with bounded term.
//!   - `election_storm_recovery_metric_increments` — same, but also
//!     asserts the `ELECTION_STORM_TERM_JUMPS_TOTAL` counter fires.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, CommittedLeaderId, Config, Entry, LogId, OptionalSend, Raft, RaftLogReader,
    RaftNetwork, RaftSnapshotBuilder, ServerState, Snapshot, SnapshotMeta, SnapshotPolicy,
    StoredMembership, Vote,
};
use tokio::sync::Mutex;

use ferrosa_cluster::raft::election_guard::{
    election_storm_term_jumps_total, run_election_guard, ELECTION_STORM_TERM_JUMPS_TOTAL,
};

// ---------------------------------------------------------------------------
// Type config — minimal in-process Raft for storm tests
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    /// Minimal type config — does NOT depend on ferrosa storage or net.
    pub StormTestConfig:
        D            = u64,
        R            = (),
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<StormTestConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

type TestRaft = Raft<StormTestConfig>;

// ---------------------------------------------------------------------------
// In-process network
// ---------------------------------------------------------------------------

/// Shared router: NodeId -> Arc<TestRaft>.
#[derive(Clone)]
struct InProcessRouter {
    nodes: Arc<parking_lot::RwLock<BTreeMap<u64, Arc<TestRaft>>>>,
    /// When true, AppendEntries/InstallSnapshot *to* node3 return Unreachable.
    /// This prevents the leader from delivering log entries or snapshots to node3.
    block_node3_append: Arc<AtomicBool>,
    /// When true, vote RPCs *sent by* node3 to others return Unreachable.
    /// Combined with block_node3_append, node3 becomes a stuck candidate:
    /// its log is stale, it can't win elections, and it keeps bumping its term.
    block_node3_outgoing_votes: Arc<AtomicBool>,
}

impl InProcessRouter {
    fn new() -> Self {
        Self {
            nodes: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            block_node3_append: Arc::new(AtomicBool::new(false)),
            block_node3_outgoing_votes: Arc::new(AtomicBool::new(false)),
        }
    }

    fn register(&self, id: u64, raft: Arc<TestRaft>) {
        self.nodes.write().insert(id, raft);
    }

    fn get(&self, id: u64) -> Option<Arc<TestRaft>> {
        self.nodes.read().get(&id).cloned()
    }
}

/// Per-target network handle.
///
/// `source` is the NodeId of the Raft node that owns this network handle.
/// `target` is the NodeId of the remote peer this handle connects to.
struct InProcessNetwork {
    router: InProcessRouter,
    source: u64,
    target: u64,
}

impl RaftNetwork<StormTestConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<StormTestConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.target == 3 && self.router.block_node3_append.load(Ordering::Relaxed) {
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "node3 append blocked",
                ),
            )));
        }
        let raft = self.router.get(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "target not found",
            )))
        })?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        // Drop outgoing vote RPCs FROM node3 so it cannot win elections.
        // node3 will keep bumping its term on each timeout but the other
        // nodes never see its RequestVote — they stay at their stable term.
        if self.source == 3
            && self
                .router
                .block_node3_outgoing_votes
                .load(Ordering::Relaxed)
        {
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "node3 outgoing vote blocked",
                ),
            )));
        }
        let raft = self.router.get(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "target not found",
            )))
        })?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<StormTestConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let raft = self.router.get(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "target not found",
            )))
        })?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }
}

struct InProcessFactory {
    router: InProcessRouter,
    /// NodeId of the local Raft node that owns this factory.
    source: u64,
}

impl RaftNetworkFactory<StormTestConfig> for InProcessFactory {
    type Network = InProcessNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        InProcessNetwork {
            router: self.router.clone(),
            source: self.source,
            target,
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory log store
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct MemLogStore {
    inner: Arc<Mutex<MemLogInner>>,
}

#[derive(Default)]
struct MemLogInner {
    vote: Option<Vote<u64>>,
    entries: BTreeMap<u64, Entry<StormTestConfig>>,
    committed: Option<LogId<u64>>,
}

impl MemLogInner {
    fn last_log_id(&self) -> Option<LogId<u64>> {
        self.entries.values().next_back().map(|e| e.log_id)
    }
}

impl RaftLogReader<StormTestConfig> for MemLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<StormTestConfig>>, openraft::StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let inner = self.inner.lock().await;
        Ok(inner.entries.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<StormTestConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<StormTestConfig>, openraft::StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(openraft::storage::LogState {
            last_purged_log_id: None,
            last_log_id: inner.last_log_id(),
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), openraft::StorageError<u64>> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, openraft::StorageError<u64>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), openraft::StorageError<u64>> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, openraft::StorageError<u64>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<StormTestConfig>,
    ) -> Result<(), openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<StormTestConfig>> + OptionalSend,
    {
        let mut inner = self.inner.lock().await;
        for e in entries {
            inner.entries.insert(e.log_id.index, e);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), openraft::StorageError<u64>> {
        self.inner
            .lock()
            .await
            .entries
            .retain(|&k, _| k < log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), openraft::StorageError<u64>> {
        self.inner
            .lock()
            .await
            .entries
            .retain(|&k, _| k > log_id.index);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory state machine + snapshot builder
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MemSm {
    last_applied: Option<LogId<u64>>,
}

impl RaftSnapshotBuilder<StormTestConfig> for MemSm {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<StormTestConfig>, openraft::StorageError<u64>> {
        let log_id = self
            .last_applied
            .unwrap_or_else(|| LogId::new(CommittedLeaderId::new(0, 0), 0));
        let meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
            snapshot_id: format!("snap-{}", log_id.index),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

impl RaftStateMachine<StormTestConfig> for MemSm {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), openraft::StorageError<u64>>
    {
        Ok((self.last_applied, StoredMembership::default()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<StormTestConfig>> + OptionalSend,
    {
        let mut out = Vec::new();
        for e in entries {
            self.last_applied = Some(e.log_id);
            out.push(());
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MemSm {
            last_applied: self.last_applied,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, openraft::StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<u64>> {
        self.last_applied = meta.last_log_id;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<StormTestConfig>>, openraft::StorageError<u64>> {
        // Return None — openraft will trigger a fresh build via get_snapshot_builder.
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Cluster bootstrap helper
// ---------------------------------------------------------------------------

fn test_raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            // Snapshot after 10 entries so node3 gets an InstallSnapshot
            // rather than waiting for full log replay.
            snapshot_policy: SnapshotPolicy::LogsSinceLast(10),
            ..Config::default()
        }
        .validate()
        .expect("test raft config"),
    )
}

/// Production-cadence config — matches the post-bulk-write-fix deployment.
///
/// election_timeout_min = 3000 ms mirrors `bug-bulk-write-raft-starvation.md`
/// P2 fix.  heartbeat_interval is scaled proportionally.
fn prod_cadence_raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 500,
            election_timeout_min: 3_000,
            election_timeout_max: 6_000,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(10),
            ..Config::default()
        }
        .validate()
        .expect("prod cadence raft config"),
    )
}

/// Spin up 3 in-process openraft nodes sharing `router` using `cfg`.
///
/// Returns `(node1, node2, node3)`.  node1 is the seed and calls
/// `initialize()`; the cluster elects a leader before returning.
///
/// The `leader_wait_secs` parameter controls how long to wait for an initial
/// leader — use a longer value when `election_timeout_min` is large.
async fn build_3_node_cluster_with_config(
    router: InProcessRouter,
    cfg: Arc<Config>,
    leader_wait_secs: u64,
) -> (Arc<TestRaft>, Arc<TestRaft>, Arc<TestRaft>) {
    macro_rules! make_node {
        ($id:expr) => {{
            let id: u64 = $id;
            let ls = MemLogStore::default();
            let sm = MemSm::default();
            let factory = InProcessFactory {
                router: router.clone(),
                source: id,
            };
            let raft = Arc::new(
                Raft::new(id, cfg.clone(), factory, ls, sm)
                    .await
                    .unwrap_or_else(|e| panic!("Raft::new({id}) failed: {e}")),
            );
            router.register(id, raft.clone());
            raft
        }};
    }

    let n1 = make_node!(1);
    let n2 = make_node!(2);
    let n3 = make_node!(3);

    let mut members = BTreeMap::new();
    members.insert(
        1u64,
        BasicNode {
            addr: String::new(),
        },
    );
    members.insert(
        2u64,
        BasicNode {
            addr: String::new(),
        },
    );
    members.insert(
        3u64,
        BasicNode {
            addr: String::new(),
        },
    );
    n1.initialize(members).await.expect("cluster init failed");

    // Wait for leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(leader_wait_secs);
    loop {
        if n1.current_leader().await.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader within {leader_wait_secs}s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    (n1, n2, n3)
}

/// Spin up 3 in-process openraft nodes using the fast test config.
///
/// Thin wrapper around [`build_3_node_cluster_with_config`].
async fn build_3_node_cluster(
    router: InProcessRouter,
) -> (Arc<TestRaft>, Arc<TestRaft>, Arc<TestRaft>) {
    build_3_node_cluster_with_config(router, test_raft_config(), 15).await
}

/// Wait for a leader to emerge among node1/node2 only (not node3).
///
/// If node3 is currently the leader, suppress its elections (causing it to
/// stop sending heartbeats) and wait for node1 or node2 to win a new
/// election.  Once a non-node3 leader is established, re-enable node3's
/// elections so it becomes a normal follower.
///
/// This is necessary for the storm-reproduction setup: we need node3 to be
/// a follower so we can create a log gap between it and the cluster leader.
async fn ensure_non_node3_leader(
    n1: &Arc<TestRaft>,
    n2: &Arc<TestRaft>,
    n3: &Arc<TestRaft>,
) -> Arc<TestRaft> {
    // If node3 is the leader, force a leadership transfer by disabling its
    // tick (which stops heartbeats) and waiting for node1/node2 to elect.
    if n3.metrics().borrow().state == ServerState::Leader {
        n3.runtime_config().tick(false);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if n1.metrics().borrow().state == ServerState::Leader {
            n3.runtime_config().tick(true);
            return n1.clone();
        }
        if n2.metrics().borrow().state == ServerState::Leader {
            n3.runtime_config().tick(true);
            return n2.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no node1/node2 leader within 15 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Write `count` entries through `leader`.
async fn write_entries(leader: &Arc<TestRaft>, count: u64) {
    for i in 0..count {
        leader
            .client_write(i)
            .await
            .unwrap_or_else(|e| panic!("write {i} failed: {e}"));
    }
}

// ---------------------------------------------------------------------------
// Test 1 — primary correctness assertion (P0-17)
// ---------------------------------------------------------------------------

/// With the election guard, node3 converges to the cluster log and term
/// after its partition is lifted.  Without the guard, `node3_term` would
/// keep climbing while `leader_term` stays stable — that delta is P0-17.
///
/// ## Setup
///
/// 1. Build cluster, ensure node1 or node2 is the leader (not node3).
///    Write initial entries so all three have a baseline log.
/// 2. Block AppendEntries to node3 and suppress node3 elections.
///    Write 100 more entries so the cluster log advances past node3.
/// 3. Re-enable node3 elections but KEEP AppendEntries blocked AND
///    block node3's outgoing vote RPCs.  node3 now fires elections with
///    a stale log, bumps term, and retries — but node1/node2 are isolated
///    from node3's inflated term.  This is the P0-17 storm in isolation.
/// 4. Start the election guard.  It detects term_delta >= 2 per poll
///    window and suppresses node3's elections.
/// 5. Unblock everything.  The leader delivers an InstallSnapshot; node3
///    converges.
///
/// Pre-fix: fails because `node3_final_term > leader_final_term + 5`
///          (without the guard node3 would be thousands of terms ahead).
/// Post-fix: passes — guard suppresses elections, node3 converges.
// Bound multi-node harness oversubscription across all test binaries so the
// short raft election timers stay serviceable (deterministic election). See the
// shared module for the full rationale.
#[path = "common/harness_slot.rs"]
mod harness_slot;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_storm_does_not_occur_on_log_divergence() {
    let _slot = harness_slot::acquire_harness_slot().await;
    ELECTION_STORM_TERM_JUMPS_TOTAL.store(0, Ordering::Relaxed);

    let router = InProcessRouter::new();
    let (n1, n2, n3) = build_3_node_cluster(router.clone()).await;

    // --- Phase 1: write initial entries, ensure a non-node3 leader ---
    //
    // We need node3 to be a follower so we can create a log gap.  If node3
    // happens to win the initial election, force a failover to node1/node2.
    let leader = ensure_non_node3_leader(&n1, &n2, &n3).await;
    write_entries(&leader, 20).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let a3 = n3
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        if a3 >= 10 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "initial convergence timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let initial_term = leader.metrics().borrow().current_term;

    // --- Phase 2: build log gap on node3 ---
    //
    // Suppress node3 elections (so it stays a passive follower) and block
    // AppendEntries to node3 so it cannot receive the new entries.
    // Write 50 more entries on node1+node2 to widen the gap.
    n3.runtime_config().elect(false);
    router.block_node3_append.store(true, Ordering::Relaxed);
    write_entries(&leader, 50).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let leader_log_target = leader.metrics().borrow().last_log_index.unwrap_or(0);
    let node3_log_before = n3.metrics().borrow().last_log_index.unwrap_or(0);
    assert!(
        node3_log_before < leader_log_target,
        "setup: node3 log ({node3_log_before}) should lag leader ({leader_log_target})"
    );

    // --- Phase 3: trigger the election storm on node3 ---
    //
    // Also block outgoing votes FROM node3 so node1/node2 never see node3's
    // inflated term — this isolates the storm to node3 exactly as in production
    // (the production leader stayed at T8 while node3 hit T18 348).
    //
    // AppendEntries stays blocked: node3 can't receive the snapshot.
    // node3 fires elections, is rejected (stale log), bumps term, retries.
    router
        .block_node3_outgoing_votes
        .store(true, Ordering::Relaxed);
    n3.runtime_config().elect(true);

    // Let the storm build for ~2.5 s before the guard is started, so the
    // guard's first poll after baseline observes a clear term_delta >= 2.
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let node3_term_after_storm = n3.metrics().borrow().current_term;

    // Verify the storm is real — node3 term must have climbed.
    assert!(
        node3_term_after_storm > initial_term + 1,
        "storm did not materialize (node3_term={node3_term_after_storm} initial={initial_term}); \
         check test setup"
    );

    // --- Phase 4: start the guard ---
    //
    // The guard immediately sees prev_term=0 vs current=node3_term_after_storm
    // on its first poll (after 1 s).  term_delta >> TERM_JUMP_THRESHOLD;
    // it suppresses elections and increments the counter.
    let guard_cancel = tokio_util::sync::CancellationToken::new();
    {
        let guard_raft = n3.clone();
        let guard_cancel2 = guard_cancel.clone();
        tokio::spawn(async move {
            run_election_guard(guard_raft, guard_cancel2, 200).await;
        });
    }

    // --- Phase 5: unblock, wait for node3 to converge ---
    router
        .block_node3_outgoing_votes
        .store(false, Ordering::Relaxed);
    router.block_node3_append.store(false, Ordering::Relaxed);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let node3_log = n3.metrics().borrow().last_log_index.unwrap_or(0);
        if node3_log >= leader_log_target {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node3 did not converge within 30 s: node3_log={node3_log} target={leader_log_target}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    guard_cancel.cancel();

    let leader_final_term = leader.metrics().borrow().current_term;
    let node3_final_term = n3.metrics().borrow().current_term;
    let node3_final_log = n3.metrics().borrow().last_log_index.unwrap_or(0);

    // 1. node3's log has caught up.
    assert!(
        node3_final_log >= leader_log_target,
        "node3 log ({node3_final_log}) did not reach leader target ({leader_log_target})"
    );

    // 2. P0-17 assertion: after the guard, node3's term is within 2 of the
    //    cluster term (the guard suppressed the runaway storm).
    //    Without the guard, node3_final_term would be >> leader_final_term.
    assert!(
        node3_final_term <= leader_final_term + 2,
        "P0-17: node3 term ({node3_final_term}) still far above leader term \
         ({leader_final_term}) — guard did not suppress the election storm"
    );

    n1.shutdown().await.ok();
    n2.shutdown().await.ok();
    n3.shutdown().await.ok();
}

// ---------------------------------------------------------------------------
// Test 2 — metric counter assertion
// ---------------------------------------------------------------------------

/// The `ELECTION_STORM_TERM_JUMPS_TOTAL` counter must increment when the
/// guard detects a storm.  Uses the same log-divergence setup as test 1
/// but explicitly asserts the metric fires.
///
/// ## Guard-start timing
///
/// The guard is started AFTER the storm has been running for 2.5 s, so its
/// first poll sees `prev_term=0` and `current_term=storm_term` — a huge
/// delta that unambiguously exceeds `TERM_JUMP_THRESHOLD`.  This guarantees
/// the counter fires on the guard's first poll cycle.
///
/// Pre-fix: counter is 0 (guard does not exist).
/// Post-fix: counter is >= 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_storm_recovery_metric_increments() {
    let _slot = harness_slot::acquire_harness_slot().await;
    ELECTION_STORM_TERM_JUMPS_TOTAL.store(0, Ordering::Relaxed);

    let router = InProcessRouter::new();
    let (n1, n2, n3) = build_3_node_cluster(router.clone()).await;

    // Ensure a non-node3 leader for consistent gap-building.
    let leader = ensure_non_node3_leader(&n1, &n2, &n3).await;
    write_entries(&leader, 20).await;

    // Wait for initial convergence.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let applied = n3
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        if applied >= 10 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "initial convergence timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Build log gap: suppress node3 elections while advancing node1+node2.
    n3.runtime_config().elect(false);
    router.block_node3_append.store(true, Ordering::Relaxed);
    write_entries(&leader, 50).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let leader_log = leader.metrics().borrow().last_log_index.unwrap_or(0);
    let baseline_term = n3.metrics().borrow().current_term;

    // Trigger the storm: re-enable elections with stale log.
    // Block outgoing votes so node1/node2 are isolated from node3's term.
    router
        .block_node3_outgoing_votes
        .store(true, Ordering::Relaxed);
    n3.runtime_config().elect(true);

    // Let the storm run 2.5 s — node3's term climbs far above baseline.
    // With election_timeout_min=200ms, expect 5–12 term bumps.
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let storm_term = n3.metrics().borrow().current_term;
    assert!(
        storm_term > baseline_term + 1,
        "storm did not build (storm_term={storm_term} baseline={baseline_term}); check test setup"
    );

    // Start the guard NOW.  Its initial `prev_term` is 0; after the first
    // poll (1 s) it sees current_term=storm_term, giving term_delta=storm_term.
    // That is >> TERM_JUMP_THRESHOLD=1, so the counter fires immediately.
    let guard_cancel = tokio_util::sync::CancellationToken::new();
    {
        let guard_raft = n3.clone();
        let guard_cancel2 = guard_cancel.clone();
        tokio::spawn(async move {
            run_election_guard(guard_raft, guard_cancel2, 200).await;
        });
    }

    // Wait for the guard's first poll to fire (1 s) + a small margin.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    // Assert the counter fired before unblocking replication.
    let jumps_before_unblock = election_storm_term_jumps_total();
    assert!(
        jumps_before_unblock >= 1,
        "P0-17: ELECTION_STORM_TERM_JUMPS_TOTAL should be >= 1 after guard's first poll \
         (got {jumps_before_unblock}); \
         baseline_term={baseline_term} storm_term={storm_term} — \
         check run_election_guard detection logic"
    );

    // Unblock — leader delivers InstallSnapshot, node3 converges.
    router
        .block_node3_outgoing_votes
        .store(false, Ordering::Relaxed);
    router.block_node3_append.store(false, Ordering::Relaxed);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let node3_log = n3.metrics().borrow().last_log_index.unwrap_or(0);
        if node3_log >= leader_log {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node3 did not converge: node3_log={node3_log} leader_log={leader_log}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    guard_cancel.cancel();

    let node3_final_term = n3.metrics().borrow().current_term;
    let leader_final_term = leader.metrics().borrow().current_term;
    assert!(
        node3_final_term <= leader_final_term + 2,
        "after guard: node3 term ({node3_final_term}) still far above leader \
         ({leader_final_term}) — guard suppression did not hold"
    );

    n1.shutdown().await.ok();
    n2.shutdown().await.ok();
    n3.shutdown().await.ok();
}

// ---------------------------------------------------------------------------
// Test 3 — P0-19: production-cadence storm (election_timeout_min = 3000 ms)
// ---------------------------------------------------------------------------

/// The rolling-window detector must fire on a storm driven at production
/// cadence (`election_timeout_min = 3000 ms`), where per-poll term delta is
/// 0 or 1 — well below the burst detector's threshold of > 1.
///
/// ## Why the existing tests don't cover this
///
/// Tests 1 and 2 use `election_timeout_min = 200 ms` and start the guard
/// AFTER 2.5 s of accumulated storm, so the guard's seed `prev_term=0`
/// produces a huge first-poll delta that trips the burst path immediately.
/// In production, the guard is started at node startup (before any storm),
/// so each 1-second poll window sees only the incremental term delta — 0 or
/// 1 per window, never ≥ 2.
///
/// ## Test strategy
///
/// 1. Build the cluster with `prod_cadence_raft_config` (election_timeout_min
///    = 3000 ms, heartbeat = 500 ms).
/// 2. Create a log gap and trigger a storm on node3.
/// 3. Start the guard IMMEDIATELY (before the storm runs), seeding it from
///    the current term so it observes real incremental per-poll deltas.
/// 4. Assert the guard fires within 60 s.
///
/// ## Pre-fix behavior (proves the bug)
///
/// Against the original `term_delta > TERM_JUMP_THRESHOLD` (burst-only)
/// implementation, this test FAILS because per-poll delta at 3000 ms cadence
/// is at most 1, never > 1, so `ELECTION_STORM_TERM_JUMPS_TOTAL` stays 0.
///
/// ## Post-fix behavior
///
/// The rolling-window detector accumulates ≥ 2 term jumps over 30 s and
/// fires suppression.  `ELECTION_STORM_TERM_JUMPS_TOTAL` reaches ≥ 1 within
/// 60 s of storm onset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_storm_guard_fires_at_production_cadence() {
    let _slot = harness_slot::acquire_harness_slot().await;
    ELECTION_STORM_TERM_JUMPS_TOTAL.store(0, Ordering::Relaxed);

    let router = InProcessRouter::new();

    // Use production-cadence config: election_timeout_min = 3000 ms.
    // leader_wait_secs = 30 to allow the first election to complete.
    let (n1, n2, n3) =
        build_3_node_cluster_with_config(router.clone(), prod_cadence_raft_config(), 30).await;

    // Ensure a non-node3 leader.
    let leader = ensure_non_node3_leader(&n1, &n2, &n3).await;
    write_entries(&leader, 20).await;

    // Wait for initial convergence on node3 (generous timeout for slow cadence).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let applied = n3
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        if applied >= 10 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "initial convergence timeout (prod cadence)"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let baseline_term = n3.metrics().borrow().current_term;

    // Build log gap: suppress node3 elections while advancing node1+node2.
    n3.runtime_config().elect(false);
    router.block_node3_append.store(true, Ordering::Relaxed);
    write_entries(&leader, 50).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_log = leader.metrics().borrow().last_log_index.unwrap_or(0);
    let node3_log_before = n3.metrics().borrow().last_log_index.unwrap_or(0);
    assert!(
        node3_log_before < leader_log,
        "setup: node3 log ({node3_log_before}) should lag leader ({leader_log})"
    );

    // Start the guard NOW — BEFORE the storm begins.
    //
    // This is the key difference from tests 1 and 2.  The guard seeds
    // prev_term = baseline_term (from current metrics), so each subsequent
    // 1-second poll window sees only the real incremental per-poll delta
    // (0 or 1).  The burst detector's `term_delta > 1` path will never fire.
    // Only the rolling-window detector can catch this storm.
    let guard_cancel = tokio_util::sync::CancellationToken::new();
    {
        let guard_raft = n3.clone();
        let guard_cancel2 = guard_cancel.clone();
        tokio::spawn(async move {
            run_election_guard(guard_raft, guard_cancel2, 3_000).await;
        });
    }

    // Trigger the storm: re-enable node3 elections with stale log.
    // Block outgoing votes so node1/node2 are isolated from node3's term.
    router
        .block_node3_outgoing_votes
        .store(true, Ordering::Relaxed);
    n3.runtime_config().elect(true);

    // Wait for the rolling-window detector to fire.
    //
    // At election_timeout_min=3000ms, one election per ~3–6 s.
    // Two elections (ROLLING_WINDOW_MIN_JUMPS=2) take ≤ 12 s.
    // Add ROLLING_WINDOW_MS (30 s) for the window to accumulate.
    // Total expected: ≤ 42 s.  Budget: 60 s.
    //
    // P0-19: without the rolling-window fix this assertion FAILS because
    // ELECTION_STORM_TERM_JUMPS_TOTAL stays 0 for the full 60 s.
    let storm_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let jumps = election_storm_term_jumps_total();
        if jumps >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < storm_deadline,
            "P0-19: rolling-window detector did not fire within 60 s at \
             election_timeout_min=3000ms — ELECTION_STORM_TERM_JUMPS_TOTAL still 0; \
             baseline_term={baseline_term} node3_term={}",
            n3.metrics().borrow().current_term,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Unblock replication so node3 can converge.
    router
        .block_node3_outgoing_votes
        .store(false, Ordering::Relaxed);
    router.block_node3_append.store(false, Ordering::Relaxed);

    // Wait for node3 to catch up (generous: snapshot transfer at slow cadence).
    let converge_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let node3_log = n3.metrics().borrow().last_log_index.unwrap_or(0);
        if node3_log >= leader_log {
            break;
        }
        assert!(
            tokio::time::Instant::now() < converge_deadline,
            "node3 did not converge within 90 s: node3_log={node3_log} target={leader_log}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    guard_cancel.cancel();

    let node3_final_term = n3.metrics().borrow().current_term;
    let leader_final_term = leader.metrics().borrow().current_term;

    // After suppression + convergence, node3's term must be close to the cluster.
    assert!(
        node3_final_term <= leader_final_term + 3,
        "P0-19: node3 term ({node3_final_term}) still far above leader \
         ({leader_final_term}) — rolling-window suppression did not hold"
    );

    let final_jumps = election_storm_term_jumps_total();
    assert!(
        final_jumps >= 1,
        "P0-19: expected ELECTION_STORM_TERM_JUMPS_TOTAL >= 1, got {final_jumps}"
    );

    n1.shutdown().await.ok();
    n2.shutdown().await.ok();
    n3.shutdown().await.ok();
}

// ---------------------------------------------------------------------------
// W3.12 — ferrosa_partitioned_node_does_not_advance_term (ADR-012)
// ---------------------------------------------------------------------------

/// Build a Config that mirrors the sprint-03 ferrosa defaults: PreVote enabled,
/// CheckQuorum ratio 0.75. Mirrors the wiring in
/// `controller/cluster.rs:840`. See ADR-012.
fn prevote_checkquorum_raft_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(10),
            // ADR-012 ferrosa defaults (turned ON for this test).
            enable_pre_vote: true,
            check_quorum_ratio: 0.75,
            ..Config::default()
        }
        .validate()
        .expect("prevote+checkquorum config must validate"),
    )
}

/// **W3.12 — Sprint 3 acceptance test (ADR-012).**
///
/// 3-node ferrosa cluster with PreVote+CheckQuorum enabled (the new fork
/// knobs from `correctness/prevote-checkquorum`). Partition node3 from
/// {node1, node2}, then heal. Assert node3's persisted term advanced by **at
/// most 0** during the partition window — the protocol-level fix for
/// `bug-raft-stale-candidate-runaway-term-no-prevote.md`.
///
/// # Gap closed
///
/// This test previously sat behind `#[cfg(feature = "sprint-03-engine-prevote")]`
/// because the engine-side PreVote path was not wired in the openraft fork —
/// the election tick called `Engine::elect()` unconditionally, incrementing the
/// term on every timer fire even while partitioned. The fork commit pinned in
/// the workspace `Cargo.toml` (`af87fa60…`) closes that gap: `handle_tick_election`
/// now runs a synchronous `run_pre_vote_round()` gated behind
/// `Config::enable_pre_vote` and only proceeds to `elect()` (term bump) when the
/// pre-vote is granted by a quorum. A partitioned node therefore cannot win a
/// pre-vote round and never advances its persisted term — `term_advance == 0`.
///
/// The gate is removed; this runs in the default suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ferrosa_partitioned_node_does_not_advance_term() {
    let _slot = harness_slot::acquire_harness_slot().await;
    ELECTION_STORM_TERM_JUMPS_TOTAL.store(0, Ordering::Relaxed);

    let router = InProcessRouter::new();
    let cfg = prevote_checkquorum_raft_config();
    let (n1, n2, n3) = build_3_node_cluster_with_config(router.clone(), cfg, 15).await;

    // Phase 1: cluster is healthy. Pick a non-node3 leader so node3 is a
    // follower we can partition.
    let leader = ensure_non_node3_leader(&n1, &n2, &n3).await;
    write_entries(&leader, 5).await;

    // Wait for node3 to fully replicate.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let a3 = n3
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        if a3 >= 5 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "initial convergence timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let initial_node3_term = n3.metrics().borrow().current_term;
    let initial_leader_term = leader.metrics().borrow().current_term;

    // Phase 2: partition node3 from {n1, n2}.
    //
    // Symmetric isolation: block both AppendEntries TO node3 (so it stops
    // hearing from leader and its lease times out) and outgoing votes FROM
    // node3 (so other nodes never see its inflated term — modeling the
    // production failure mode where a partitioned candidate disrupts on heal).
    router.block_node3_append.store(true, Ordering::Relaxed);
    router
        .block_node3_outgoing_votes
        .store(true, Ordering::Relaxed);

    // Hold the partition for ~5 election-timeouts so node3 would have
    // attempted multiple election rounds without PreVote.
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let partitioned_node3_term = n3.metrics().borrow().current_term;
    let term_advance = partitioned_node3_term.saturating_sub(initial_node3_term);

    // Phase 3: heal partition.
    router
        .block_node3_outgoing_votes
        .store(false, Ordering::Relaxed);
    router.block_node3_append.store(false, Ordering::Relaxed);

    // Wait for the cluster to re-converge.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let leader_term_now = leader.metrics().borrow().current_term;
        let n3_term_now = n3.metrics().borrow().current_term;
        if n3_term_now <= leader_term_now + 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "post-heal convergence timeout: leader_term={leader_term_now} n3_term={n3_term_now}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let final_leader_term = leader.metrics().borrow().current_term;

    // ADR-012 W3.12 assertion: with PreVote enabled, the partitioned node
    // does not advance its persisted term during the partition window.
    //
    // The exact bound:
    //   - With PreVote (target state, after engine wire-up):
    //         term_advance == 0
    //   - Without PreVote (current state of the local fork, types-only):
    //         term_advance >> 0 (typically 5-12 over 2.5s with our timeouts)
    //
    // We assert the strict-zero invariant. If the engine handler is not
    // wired yet, the test fails with a clear message pointing back to the
    // ADR and the deferred work item.
    assert_eq!(
        term_advance, 0,
        "ADR-012 W3.12: PreVote did not suppress term advance during partition. \
         node3 term advanced from {initial_node3_term} to {partitioned_node3_term} \
         (delta {term_advance}). Cluster final leader_term={final_leader_term}, \
         initial_leader_term={initial_leader_term}. \
         If `enable_pre_vote: true` is in Config but the engine still increments \
         the term on election timeout, the engine-side `handle_pre_vote_req` \
         and `ServerState::PreCandidate` are not yet wired in the openraft fork \
         (see specs/in-process/sprint-03-openraft-patches.md, items W3.3 and \
         W3.7-engine)."
    );

    n1.shutdown().await.ok();
    n2.shutdown().await.ok();
    n3.shutdown().await.ok();
}
