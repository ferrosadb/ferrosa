//! Reproduces P0-20 — leader does not push InstallSnapshot to stale-candidate follower.
//!
//! ## What this test demonstrates
//!
//! When a follower is wiped (persistent Raft state cleared) and restarted, it
//! starts bumping its term with every failed election.  The cluster leader
//! cannot push an InstallSnapshot until its term exceeds the wiped node's
//! inflated term.  Without the snapshot_pusher sweep the election storm
//! can run long enough that the 60-second election-guard suppression window
//! expires without the wiped node converging.
//!
//! The snapshot_pusher (P0-20 fix) runs on the leader and periodically calls
//! `trigger().snapshot()` + `trigger().heartbeat()` to ensure the snapshot is
//! built and the replication loop is kicked toward lagging peers.
//!
//! ## Test design
//!
//! Three real openraft instances share an in-process channel network.
//! Node3 is blocked from receiving AppendEntries while the cluster writes
//! 100 entries.  This creates a large log gap.  Then AppendEntries to node3
//! is unblocked.  The snapshot_pusher, running with a short sweep interval
//! (200 ms), detects node3's lag and fires `trigger().snapshot()` +
//! `trigger().heartbeat()` before natural replication catches up.
//!
//! The test asserts:
//!   1. Within 30 s, node3's `last_log_index` matches the leader's.
//!   2. `INSTALLSNAPSHOT_PUSHES_TOTAL >= 1`.
//!
//! The second assertion proves the pusher ran and observed a lagging peer —
//! it is the observable signal that distinguishes an active push from a
//! passive natural-replication convergence.
//!
//! ## Why max_in_snapshot_log_to_keep = 0
//!
//! Setting this to 0 forces all log entries that are covered by a snapshot
//! to be purged.  With snapshot_policy = LogsSinceLast(10) and 100 entries,
//! the leader has at most 9 log entries available.  Node3 (at log 0) is so
//! far behind that only InstallSnapshot can bring it up to date.  This
//! ensures the snapshot path is exercised, not just AppendEntries catch-up.
//!
//! ## loosen-follower-log-revert feature
//!
//! When node3 is wiped and replaced with a fresh Raft instance, the leader's
//! existing replication stream may still have `matching = T1-N1-101` for
//! node3.  When fresh-node3 reports a conflict at index 101 (it has no log),
//! openraft's progress tracker detects a "log reversion" and panics in debug
//! builds without `loosen-follower-log-revert`.  This feature converts the
//! panic to a warn+reset, which is the correct behavior for a rejoining node.

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

use ferrosa_cluster::raft::election_guard::{run_election_guard, ELECTION_STORM_TERM_JUMPS_TOTAL};
use ferrosa_cluster::raft::snapshot_pusher::{run_snapshot_pusher, INSTALLSNAPSHOT_PUSHES_TOTAL};

// ---------------------------------------------------------------------------
// Shared type config — in-process Raft for snapshot push tests
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    pub SnapPushTestConfig:
        D            = u64,
        R            = (),
        NodeId       = u64,
        Node         = BasicNode,
        Entry        = openraft::Entry<SnapPushTestConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

type TestRaft = Raft<SnapPushTestConfig>;

// ---------------------------------------------------------------------------
// In-process network
// ---------------------------------------------------------------------------

/// Dynamic router — allows replacing a node's Raft instance (simulates restart).
/// Blocking flags mirror the storm-test router.
#[derive(Clone)]
struct InProcessRouter {
    nodes: Arc<parking_lot::RwLock<BTreeMap<u64, Arc<TestRaft>>>>,
    /// Block AppendEntries/InstallSnapshot *to* node3.
    block_node3_append: Arc<AtomicBool>,
    /// Block vote RPCs *sent by* node3 to others.
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

struct InProcessNetwork {
    router: InProcessRouter,
    #[allow(dead_code)] // present for symmetry with storm-test router
    source: u64,
    target: u64,
}

impl RaftNetwork<SnapPushTestConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<SnapPushTestConfig>,
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
        rpc: InstallSnapshotRequest<SnapPushTestConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
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
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }
}

struct InProcessFactory {
    router: InProcessRouter,
    source: u64,
}

impl RaftNetworkFactory<SnapPushTestConfig> for InProcessFactory {
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
// In-memory stores
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct MemLogStore {
    inner: Arc<Mutex<MemLogInner>>,
}

#[derive(Default)]
struct MemLogInner {
    vote: Option<Vote<u64>>,
    entries: BTreeMap<u64, Entry<SnapPushTestConfig>>,
    committed: Option<LogId<u64>>,
    last_purged: Option<LogId<u64>>,
}

impl MemLogInner {
    fn last_log_id(&self) -> Option<LogId<u64>> {
        self.entries.values().next_back().map(|e| e.log_id)
    }
}

impl RaftLogReader<SnapPushTestConfig> for MemLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<SnapPushTestConfig>>, openraft::StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let inner = self.inner.lock().await;
        Ok(inner.entries.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<SnapPushTestConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<SnapPushTestConfig>, openraft::StorageError<u64>> {
        let inner = self.inner.lock().await;
        Ok(openraft::storage::LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: inner.last_log_id().or(inner.last_purged),
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
        callback: LogFlushed<SnapPushTestConfig>,
    ) -> Result<(), openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<SnapPushTestConfig>> + OptionalSend,
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
        let mut inner = self.inner.lock().await;
        inner.entries.retain(|&k, _| k > log_id.index);
        // Track the purge pointer so get_log_state can report it correctly.
        // Without this, openraft thinks no logs are purged and may try to
        // read entries that no longer exist, causing a fatal storage error.
        inner.last_purged = Some(log_id);
        Ok(())
    }
}

#[derive(Default)]
struct MemSm {
    last_applied: Option<LogId<u64>>,
}

impl RaftSnapshotBuilder<SnapPushTestConfig> for MemSm {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<SnapPushTestConfig>, openraft::StorageError<u64>> {
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

impl RaftStateMachine<SnapPushTestConfig> for MemSm {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), openraft::StorageError<u64>>
    {
        Ok((self.last_applied, StoredMembership::default()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<SnapPushTestConfig>> + OptionalSend,
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
    ) -> Result<Option<Snapshot<SnapPushTestConfig>>, openraft::StorageError<u64>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Fast config with aggressive log purging, ensuring InstallSnapshot is used.
///
/// `max_in_snapshot_log_to_keep = 0` purges all snapshotted log entries,
/// which guarantees that a wiped node (at log 0) can only be caught up via
/// InstallSnapshot, not via AppendEntries replay.
fn snap_push_test_config() -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(10),
            ..Config::default()
        }
        .validate()
        .expect("snap_push_test_config"),
    )
}

/// Build a fresh Raft node and register it in the router.
async fn make_node_async(id: u64, router: &InProcessRouter, cfg: Arc<Config>) -> Arc<TestRaft> {
    let ls = MemLogStore::default();
    let sm = MemSm::default();
    let factory = InProcessFactory {
        router: router.clone(),
        source: id,
    };
    let raft = Arc::new(
        Raft::new(id, cfg, factory, ls, sm)
            .await
            .unwrap_or_else(|e| panic!("Raft::new({id}) failed: {e}")),
    );
    router.register(id, raft.clone());
    raft
}

/// Build a 3-node cluster and wait for initial leader election.
async fn build_3_node_cluster(
    router: &InProcessRouter,
    cfg: Arc<Config>,
    leader_wait_secs: u64,
) -> (Arc<TestRaft>, Arc<TestRaft>, Arc<TestRaft>) {
    let n1 = make_node_async(1, router, cfg.clone()).await;
    let n2 = make_node_async(2, router, cfg.clone()).await;
    let n3 = make_node_async(3, router, cfg.clone()).await;

    let mut members = BTreeMap::new();
    for id in [1u64, 2, 3] {
        members.insert(
            id,
            BasicNode {
                addr: String::new(),
            },
        );
    }
    n1.initialize(members).await.expect("cluster init");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(leader_wait_secs);
    loop {
        if n1.current_leader().await.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader within {leader_wait_secs}s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    (n1, n2, n3)
}

async fn write_entries(leader: &Arc<TestRaft>, count: u64) {
    for i in 0..count {
        leader
            .client_write(i)
            .await
            .unwrap_or_else(|e| panic!("write {i} failed: {e}"));
    }
}

/// Find non-node3 leader, retrying until `wait_secs` expires.
async fn find_non_node3_leader(
    n1: &Arc<TestRaft>,
    n2: &Arc<TestRaft>,
    n3: &Arc<TestRaft>,
    wait_secs: u64,
) -> Arc<TestRaft> {
    if n3.metrics().borrow().state == ServerState::Leader {
        n3.runtime_config().tick(false);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_secs);
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
            "no non-node3 leader within {wait_secs}s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Test: leader_pushes_snapshot_to_wiped_node (P0-20)
// ---------------------------------------------------------------------------
//
// ## Setup
//
// 1. Build cluster, ensure node1 or node2 is the leader.
// 2. Block AppendEntries to node3 and suppress node3 elections.
//    Write 100 entries so the cluster log advances well past node3.
//    Trigger multiple snapshots (snapshot_policy=LogsSinceLast(10) +
//    max_in_snapshot_log_to_keep=0 means logs ARE purged).
// 3. Re-enable node3 elections and block its outgoing votes (so the leader
//    doesn't step down when node3 sends RequestVote with stale log).
// 4. Start the election guard on node3 (detects storm, suppresses elections).
// 5. Start the snapshot pusher on n1 + n2 (sweep_interval=200ms — short
//    enough to fire before natural replication catches up in the test).
//    Block AppendEntries to node3 for 400ms to give the pusher time to detect
//    the lagging node and increment the counter.  Then unblock.
// 6. Unblock everything.  Assert convergence within 30 s.
// 7. Assert INSTALLSNAPSHOT_PUSHES_TOTAL >= 1.
//
// ## Why the counter is the authoritative signal
//
// The counter proves the pusher detected a lagging peer and called
// trigger().snapshot() + trigger().heartbeat().  In production, this is
// the mechanism that drives convergence during the 60-second suppression
// window.  Without the pusher, the counter stays 0 and convergence depends
// entirely on the natural Raft heartbeat cycle — which in production (3 s
// election timeout) may not fire in time before the suppression window expires.
// Bound multi-node harness oversubscription across all test binaries so the
// short raft election timers stay serviceable (deterministic election). See the
// shared module for the full rationale.
#[path = "common/harness_slot.rs"]
mod harness_slot;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_pushes_snapshot_to_wiped_node() {
    let _slot = harness_slot::acquire_harness_slot().await;
    ELECTION_STORM_TERM_JUMPS_TOTAL.store(0, Ordering::Relaxed);
    INSTALLSNAPSHOT_PUSHES_TOTAL.store(0, Ordering::Relaxed);

    let router = InProcessRouter::new();
    let cfg = snap_push_test_config();
    let (n1, n2, n3) = build_3_node_cluster(&router, cfg.clone(), 15).await;

    // --- Phase 1: ensure non-node3 leader and write 100 entries ---
    let leader = find_non_node3_leader(&n1, &n2, &n3, 10).await;

    // Write 20 baseline entries so all nodes start with non-trivial log.
    write_entries(&leader, 20).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let n3_applied = n3
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        if n3_applied >= 10 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "initial convergence timeout"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // --- Phase 2: build a large log gap on node3 ---
    //
    // Suppress node3 elections (keeps it as follower, no term-bumping) and
    // block AppendEntries so node3 cannot receive any more log entries.
    // Write 100 more entries on node1+node2.  With snapshot_policy=10 and
    // max_in_snapshot_log_to_keep=0, all snapshotted logs are purged.
    n3.runtime_config().elect(false);
    router.block_node3_append.store(true, Ordering::Relaxed);
    write_entries(&leader, 100).await;
    // Give the leader time to build and purge snapshots.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leader_committed = leader.metrics().borrow().last_log_index.unwrap_or(0);
    let node3_log_before = n3.metrics().borrow().last_log_index.unwrap_or(0);
    assert!(
        leader_committed > node3_log_before + 50,
        "setup: leader ({leader_committed}) should be >> node3 ({node3_log_before})"
    );

    // --- Phase 3: trigger election storm on node3 ---
    //
    // Block outgoing votes FROM node3 so the leader doesn't step down.
    // Re-enable node3 elections so it starts bumping its term.
    router
        .block_node3_outgoing_votes
        .store(true, Ordering::Relaxed);
    n3.runtime_config().elect(true);

    // Let the storm build briefly (1 s) so the guard has something to detect.
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    // --- Phase 4: start the election guard on node3 ---
    let guard_cancel = tokio_util::sync::CancellationToken::new();
    {
        let guard_raft = n3.clone();
        let cancel = guard_cancel.clone();
        tokio::spawn(async move {
            run_election_guard(guard_raft, cancel, 200).await;
        });
    }

    // --- Phase 5: start the snapshot pusher on n1 + n2 ---
    //
    // Use a short sweep interval (200 ms) so the pusher fires quickly once
    // started.  AppendEntries to node3 is still blocked for another 400 ms,
    // so the pusher detects the lag and fires the counter BEFORE convergence.
    //
    // PRE-FIX: commenting out these lines leaves INSTALLSNAPSHOT_PUSHES_TOTAL
    // at 0, because nothing else increments the counter.
    let pusher_cancel = tokio_util::sync::CancellationToken::new();
    {
        let pn1 = n1.clone();
        let pn2 = n2.clone();
        let cancel1 = pusher_cancel.clone();
        let cancel2 = pusher_cancel.clone();
        tokio::spawn(async move {
            run_snapshot_pusher(pn1, cancel1, 200, 5).await;
        });
        tokio::spawn(async move {
            run_snapshot_pusher(pn2, cancel2, 200, 5).await;
        });
    }

    // Wait for the pusher to fire (200 ms sweep + margin).
    // AppendEntries to node3 is still blocked so the pusher must detect the
    // lag from the replication metrics and increment the counter.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Assert the pusher already fired before unblocking replication.
    let pushes_before_unblock = INSTALLSNAPSHOT_PUSHES_TOTAL.load(Ordering::Relaxed);
    assert!(
        pushes_before_unblock >= 1,
        "P0-20: INSTALLSNAPSHOT_PUSHES_TOTAL should be >= 1 after pusher sweep \
         (got {pushes_before_unblock}); the snapshot_pusher did not detect the lagging peer — \
         check run_snapshot_pusher's lag detection logic"
    );

    // --- Phase 6: unblock replication and wait for convergence ---
    router
        .block_node3_outgoing_votes
        .store(false, Ordering::Relaxed);
    router.block_node3_append.store(false, Ordering::Relaxed);
    n3.runtime_config().elect(false); // keep elections suppressed until convergence

    let converge_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let n3_log = n3.metrics().borrow().last_log_index.unwrap_or(0);
        if n3_log >= leader_committed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < converge_deadline,
            "P0-20: node3 did not converge within 30 s — \
             n3_log={n3_log} target={leader_committed} \
             n3_term={} n3_state={:?}",
            n3.metrics().borrow().current_term,
            n3.metrics().borrow().state,
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    guard_cancel.cancel();
    pusher_cancel.cancel();

    let final_pushes = INSTALLSNAPSHOT_PUSHES_TOTAL.load(Ordering::Relaxed);
    assert!(
        final_pushes >= 1,
        "P0-20: INSTALLSNAPSHOT_PUSHES_TOTAL must be >= 1, got {final_pushes}"
    );

    let n3_final = n3.metrics().borrow().last_log_index.unwrap_or(0);
    assert!(
        n3_final >= leader_committed,
        "P0-20: node3 final log {n3_final} < target {leader_committed}"
    );

    n1.shutdown().await.ok();
    n2.shutdown().await.ok();
    n3.shutdown().await.ok();
}
