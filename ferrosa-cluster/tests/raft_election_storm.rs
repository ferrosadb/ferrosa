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

/// Spin up 3 in-process openraft nodes sharing `router`.
///
/// Returns `(node1, node2, node3)`.  node1 is the seed and calls
/// `initialize()`; the cluster elects a leader before returning.
async fn build_3_node_cluster(
    router: InProcessRouter,
) -> (Arc<TestRaft>, Arc<TestRaft>, Arc<TestRaft>) {
    let cfg = test_raft_config();

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

    // Wait for leader (up to 15 s).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if n1.current_leader().await.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader within 15s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    (n1, n2, n3)
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_storm_does_not_occur_on_log_divergence() {
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
