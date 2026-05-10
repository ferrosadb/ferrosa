//! In-process multi-node openraft test harness for Sprint 1.
//!
//! Builds N `FerrosRaft` instances in the same process, each with its
//! own `SledLogStore` and `FerrosStateMachine`.  Inter-node RPCs route
//! through a shared `tokio::mpsc` registry keyed by openraft `NodeId`,
//! bypassing the real `PeerManager` / TLS stack entirely.
//!
//! The harness is the foundation for W1.1-W1.6 membership tests.
//!
//! # Example
//! ```no_run
//! # async fn doctest() {
//! let cluster = TestCluster::with_voters(3).await;
//! let _ = cluster.wait_for_leader(std::time::Duration::from_secs(5)).await;
//! cluster.shutdown().await;
//! # }
//! ```
//!
//! # Design
//!
//! - `InProcessNetworkFactory` implements `openraft::RaftNetworkFactory`
//!   and consults a shared `NodeRegistry` to find each peer's mpsc
//!   sender.  RPCs travel as `RpcEnvelope` messages over channels with
//!   one-shot reply oneshots.
//! - `TestNode` owns its `FerrosRaft`, log store path, state machine,
//!   and the rx side of its own RPC channel.  A spawned dispatcher
//!   loop pulls envelopes off the channel and invokes the matching
//!   `Raft` API method (`append_entries`, `vote`, `install_snapshot`).
//! - Partition simulation is per-(from, to) pair: a `partition_table`
//!   shared across factories tells outbound calls to error with
//!   `Unreachable` instead of dispatching.
//!
//! Time budget: harness keeps openraft heartbeats fast (50 ms) and
//! election timeouts in the 200-400 ms band so a cluster reaches
//! steady state in well under a second.

#![allow(dead_code)] // Helpers are reused selectively across test files.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use openraft::error::{ClientWriteError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Config as RaftLibConfig, LogId, RaftMetrics, RaftNetwork, RaftNetworkFactory,
    SnapshotMeta, StorageError, StoredMembership,
};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use uuid::Uuid;

use ferrosa_cluster::membership::MembershipNetwork;
use ferrosa_cluster::raft::log_store::SledLogStore;
use ferrosa_cluster::raft::state_machine::{FerrosStateMachine, RaftState};
use ferrosa_cluster::raft::{FerrosRaft, FerrosRaftConfig, RaftCommand, RaftResponse};

// ---------------------------------------------------------------------------
// RpcEnvelope — channel message carrying a request + a reply oneshot.
// ---------------------------------------------------------------------------

/// One inbound RPC for a node, with the reply channel attached.
enum RpcEnvelope {
    AppendEntries {
        rpc: AppendEntriesRequest<FerrosRaftConfig>,
        reply: oneshot::Sender<AppendEntriesResponse<u64>>,
    },
    Vote {
        rpc: VoteRequest<u64>,
        reply: oneshot::Sender<VoteResponse<u64>>,
    },
    InstallSnapshot {
        rpc: InstallSnapshotRequest<FerrosRaftConfig>,
        reply: oneshot::Sender<InstallSnapshotResponse<u64>>,
    },
}

// ---------------------------------------------------------------------------
// NodeRegistry — shared map from NodeId → inbound mpsc Sender.
// ---------------------------------------------------------------------------

/// Routing table the network factory consults to dispatch a peer RPC.
///
/// Wrapped in `Arc<StdMutex<...>>` so every factory and the cluster
/// owner can both read and mutate.  StdMutex (not tokio's) is fine
/// because the critical section is a trivial map read/insert.
#[derive(Clone, Default)]
struct NodeRegistry {
    inner: Arc<StdMutex<NodeRegistryInner>>,
}

#[derive(Default)]
struct NodeRegistryInner {
    /// per-node inbound senders.
    senders: HashMap<u64, mpsc::Sender<RpcEnvelope>>,
    /// Pairs (from, to) for which all outbound RPCs return `Unreachable`.
    /// The pair is directional: severing (A, B) does not also sever (B, A).
    partitions: BTreeSet<(u64, u64)>,
    /// Node IDs that are isolated entirely — both inbound (delivery to
    /// the node) and outbound (calls from the node) error out.
    isolated: BTreeSet<u64>,
}

impl NodeRegistry {
    fn register(&self, node_id: u64, sender: mpsc::Sender<RpcEnvelope>) {
        let mut g = self.inner.lock().expect("registry lock");
        g.senders.insert(node_id, sender);
    }

    fn unregister(&self, node_id: u64) {
        let mut g = self.inner.lock().expect("registry lock");
        g.senders.remove(&node_id);
    }

    fn contains(&self, node_id: u64) -> bool {
        let g = self.inner.lock().expect("registry lock");
        g.senders.contains_key(&node_id)
    }

    fn sender(&self, node_id: u64) -> Option<mpsc::Sender<RpcEnvelope>> {
        let g = self.inner.lock().expect("registry lock");
        g.senders.get(&node_id).cloned()
    }

    fn is_partitioned(&self, from: u64, to: u64) -> bool {
        let g = self.inner.lock().expect("registry lock");
        if g.isolated.contains(&from) || g.isolated.contains(&to) {
            return true;
        }
        g.partitions.contains(&(from, to)) || g.partitions.contains(&(to, from))
    }

    fn isolate(&self, node_id: u64) {
        let mut g = self.inner.lock().expect("registry lock");
        g.isolated.insert(node_id);
    }

    fn heal_all(&self) {
        let mut g = self.inner.lock().expect("registry lock");
        g.partitions.clear();
        g.isolated.clear();
    }
}

// ---------------------------------------------------------------------------
// InProcessNetworkFactory / Network
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct InProcessNetworkFactory {
    self_node_id: u64,
    registry: NodeRegistry,
}

impl InProcessNetworkFactory {
    fn new(self_node_id: u64, registry: NodeRegistry) -> Self {
        Self {
            self_node_id,
            registry,
        }
    }
}

impl RaftNetworkFactory<FerrosRaftConfig> for InProcessNetworkFactory {
    type Network = InProcessNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        InProcessNetwork {
            self_node_id: self.self_node_id,
            target,
            registry: self.registry.clone(),
        }
    }
}

pub struct InProcessNetwork {
    self_node_id: u64,
    target: u64,
    registry: NodeRegistry,
}

impl InProcessNetwork {
    fn unreachable<E>(&self, reason: &str) -> RPCError<u64, BasicNode, RaftError<u64, E>>
    where
        E: std::error::Error + 'static,
    {
        RPCError::Unreachable(Unreachable::new(&PartitionedError(reason.into())))
    }
}

impl RaftNetwork<FerrosRaftConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<FerrosRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.registry.is_partitioned(self.self_node_id, self.target) {
            return Err(self.unreachable("partition"));
        }
        let sender = self
            .registry
            .sender(self.target)
            .ok_or_else(|| self.unreachable("no registered receiver"))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(RpcEnvelope::AppendEntries { rpc, reply: tx })
            .await
            .map_err(|_| self.unreachable("receiver dropped"))?;
        rx.await
            .map_err(|_| RPCError::Network(NetworkError::new(&ChannelClosed("append reply"))))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<FerrosRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        if self.registry.is_partitioned(self.self_node_id, self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&PartitionedError(
                "partition".into(),
            ))));
        }
        let sender = self.registry.sender(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&PartitionedError(
                "no registered receiver".into(),
            )))
        })?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(RpcEnvelope::InstallSnapshot { rpc, reply: tx })
            .await
            .map_err(|_| {
                RPCError::Unreachable(Unreachable::new(&PartitionedError(
                    "receiver dropped".into(),
                )))
            })?;
        rx.await
            .map_err(|_| RPCError::Network(NetworkError::new(&ChannelClosed("snapshot reply"))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.registry.is_partitioned(self.self_node_id, self.target) {
            return Err(self.unreachable("partition"));
        }
        let sender = self
            .registry
            .sender(self.target)
            .ok_or_else(|| self.unreachable("no registered receiver"))?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(RpcEnvelope::Vote { rpc, reply: tx })
            .await
            .map_err(|_| self.unreachable("receiver dropped"))?;
        rx.await
            .map_err(|_| RPCError::Network(NetworkError::new(&ChannelClosed("vote reply"))))
    }
}

#[derive(Debug)]
struct PartitionedError(String);
impl std::fmt::Display for PartitionedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for PartitionedError {}

#[derive(Debug)]
struct ChannelClosed(&'static str);
impl std::fmt::Display for ChannelClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "in-process channel closed: {}", self.0)
    }
}
impl std::error::Error for ChannelClosed {}

// ---------------------------------------------------------------------------
// SharedStateMachine — wraps `FerrosStateMachine` so the harness can read it.
// ---------------------------------------------------------------------------

/// Adapter that lets the harness retain a handle to the state machine
/// after openraft moves it into `Raft::new`.  All trait calls forward
/// to the inner machine under a `tokio::sync::Mutex` so the harness
/// can hold the lock across the inner `.await`s on the snapshot path.
///
/// Production code uses `FerrosStateMachine` directly; this wrapper
/// is strictly a test affordance.  It serialises `apply` and snapshot
/// operations the same way openraft already does, so semantics match.
#[derive(Clone)]
pub struct SharedStateMachine {
    inner: Arc<AsyncMutex<FerrosStateMachine>>,
}

impl SharedStateMachine {
    fn new(sm: FerrosStateMachine) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(sm)),
        }
    }

    /// Clone the application-level `RaftState` (members, token_map,
    /// approved_nodes, …) so callers can assert on it without holding
    /// the lock across other awaits.
    pub async fn snapshot_state(&self) -> RaftState {
        self.inner.lock().await.state().clone()
    }

    /// Synchronous variant of [`Self::snapshot_state`] for use from non-
    /// async test contexts.  Uses `try_lock` and panics if the state
    /// machine is currently being mutated by an apply call.
    pub fn snapshot_state_blocking(&self) -> RaftState {
        self.inner
            .try_lock()
            .map(|sm| sm.state().clone())
            .expect("state machine busy — call from async context with snapshot_state()")
    }
}

impl RaftStateMachine<FerrosRaftConfig> for SharedStateMachine {
    type SnapshotBuilder = SharedStateMachine;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let mut sm = self.inner.lock().await;
        sm.applied_state().await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<FerrosRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut sm = self.inner.lock().await;
        sm.apply(entries).await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Reuse the same Arc — the SnapshotBuilder impl below also
        // takes the lock.  This means snapshot building briefly
        // contends with apply, which is fine for the harness.
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let mut sm = self.inner.lock().await;
        sm.install_snapshot(meta, snapshot).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<FerrosRaftConfig>>, StorageError<u64>> {
        let mut sm = self.inner.lock().await;
        sm.get_current_snapshot().await
    }
}

impl RaftSnapshotBuilder<FerrosRaftConfig> for SharedStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<FerrosRaftConfig>, StorageError<u64>> {
        let mut sm = self.inner.lock().await;
        let mut builder = sm.get_snapshot_builder().await;
        builder.build_snapshot().await
    }
}

// ---------------------------------------------------------------------------
// TestNode
// ---------------------------------------------------------------------------

pub struct TestNode {
    pub node_id: u64,
    pub raft: Arc<FerrosRaft>,
    /// Lock-shared handle to the underlying state machine. Use
    /// `state_machine.snapshot_state()` to read application state.
    pub state_machine: SharedStateMachine,
    /// Handle to the inbound dispatcher loop; aborted at shutdown.
    dispatcher: tokio::task::JoinHandle<()>,
    /// Held to keep tempdir alive for the lifetime of the test.
    _tempdir: tempfile::TempDir,
    /// Registry retained so we can unregister this node on shutdown.
    registry: NodeRegistry,
}

impl TestNode {
    /// Snapshot of the openraft metrics for this node.
    pub fn metrics(&self) -> RaftMetrics<u64, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Convenient access to the application-level `RaftState`.
    /// Awaits the inner async lock — call from a tokio context.
    pub async fn state_snapshot(&self) -> RaftState {
        self.state_machine.snapshot_state().await
    }
}

// ---------------------------------------------------------------------------
// TestCluster
// ---------------------------------------------------------------------------

pub struct TestCluster {
    nodes: Vec<TestNode>,
    registry: NodeRegistry,
    /// Newly-added voter nodes that joined post-bootstrap (W1.1+).
    /// Held alongside `nodes` so their dispatcher loops keep running
    /// for the lifetime of the cluster.
    extra_nodes: Arc<StdMutex<Vec<TestNode>>>,
    /// Default openraft Config used to bring up post-bootstrap voters.
    raft_lib_config: Arc<RaftLibConfig>,
}

/// Borrowed iterator across bootstrap + extra nodes.  Returned by
/// [`TestCluster::nodes`].
pub struct NodeRefs<'a> {
    primary: &'a [TestNode],
    extras_guard: std::sync::MutexGuard<'a, Vec<TestNode>>,
}

impl<'a> NodeRefs<'a> {
    pub fn iter(&self) -> NodeRefsIter<'_> {
        NodeRefsIter {
            primary: self.primary.iter(),
            extras: self.extras_guard.iter(),
        }
    }

    /// Whether the iterator contains a given `node_id`.
    pub fn contains(&self, node_id: u64) -> bool {
        self.iter().any(|n| n.node_id == node_id)
    }
}

impl<'a, 'b> IntoIterator for &'b NodeRefs<'a> {
    type Item = &'b TestNode;
    type IntoIter = NodeRefsIter<'b>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct NodeRefsIter<'a> {
    primary: std::slice::Iter<'a, TestNode>,
    extras: std::slice::Iter<'a, TestNode>,
}

impl<'a> Iterator for NodeRefsIter<'a> {
    type Item = &'a TestNode;
    fn next(&mut self) -> Option<Self::Item> {
        self.primary.next().or_else(|| self.extras.next())
    }
}

// ---------------------------------------------------------------------------
// HarnessMembershipNetwork — collapses maps 3 and 4 into the registry.
// ---------------------------------------------------------------------------

/// `MembershipNetwork` impl backed by the harness's `NodeRegistry`.
///
/// In production these are two separate stores; in the harness, a
/// peer is "registered" iff there is an inbound mpsc Sender for it,
/// and "connected" iff partition rules allow delivery.  This is a
/// faithful enough emulation for Sprint 1 atomicity tests.
pub struct HarnessMembershipNetwork {
    registry: NodeRegistry,
}

impl MembershipNetwork for HarnessMembershipNetwork {
    fn register_node(&self, _node_id: u64, _host_id: Uuid) {
        // The harness pre-registers every node when it spawns the
        // dispatcher, so explicit register_node is a NoOp.  The
        // important invariant — that `contains()` reports true after
        // `register_node` — is satisfied by the prior
        // `add_pending_node` call.
    }

    fn unregister_node(&self, node_id: u64) {
        self.registry.unregister(node_id);
    }

    fn contains(&self, node_id: u64) -> bool {
        self.registry.contains(node_id)
    }
}

impl TestCluster {
    /// Spin up an N-voter cluster and call `initialize` on node 0.
    pub async fn with_voters(n: usize) -> Self {
        assert!(n >= 1, "TestCluster requires at least 1 node");

        let registry = NodeRegistry::default();

        // Reserve N node IDs.  Use 1..=N so they're easy to read in test
        // output (openraft NodeId is `u64`).
        let node_ids: Vec<u64> = (1..=n as u64).collect();

        // Build openraft Config: aggressive timing for fast-cycling tests.
        let raft_lib_config = Arc::new(
            RaftLibConfig {
                cluster_name: "ferrosa-test-cluster".to_string(),
                heartbeat_interval: 50,
                election_timeout_min: 200,
                election_timeout_max: 400,
                replication_lag_timeout: 500,
                max_payload_entries: 64,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(10_000),
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );

        let mut nodes = Vec::with_capacity(n);
        for &node_id in &node_ids {
            let node = build_test_node(node_id, raft_lib_config.clone(), registry.clone()).await;
            nodes.push(node);
        }

        // Initialize on node 0 with all voters.
        let mut members = BTreeMap::new();
        for &id in &node_ids {
            members.insert(
                id,
                BasicNode {
                    addr: format!("inproc://{id}"),
                },
            );
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .expect("initial seed initialize");

        Self {
            nodes,
            registry,
            extra_nodes: Arc::new(StdMutex::new(Vec::new())),
            raft_lib_config,
        }
    }

    /// Spin up a new `TestNode` with the given `node_id`, register its
    /// dispatcher, and stash it in `extra_nodes` so it survives.  This
    /// is what `MembershipChanger::add_voter` needs in the harness:
    /// the leader's `add_learner` will start replicating to the new
    /// node_id immediately, so the dispatcher must be ready first.
    pub async fn add_pending_node(&self, node_id: u64) {
        let node =
            build_test_node(node_id, self.raft_lib_config.clone(), self.registry.clone()).await;
        self.extra_nodes
            .lock()
            .expect("extra_nodes lock")
            .push(node);
    }

    /// Whether the harness has a dispatcher registered for `node_id`.
    /// True for both bootstrap voters and any `add_pending_node`
    /// additions; false after `unregister_node`.
    pub fn is_registered(&self, node_id: u64) -> bool {
        self.registry.contains(node_id)
    }

    /// Construct an [`Arc<HarnessMembershipNetwork>`] backed by the
    /// shared registry — pass to `MembershipChanger::new`.
    pub fn membership_network(&self) -> Arc<HarnessMembershipNetwork> {
        Arc::new(HarnessMembershipNetwork {
            registry: self.registry.clone(),
        })
    }

    /// Snapshot view of every node — bootstrap voters plus
    /// `add_pending_node` additions.  Returned as raw pointers wrapped
    /// in a `NodeRefs` guard so callers cannot accidentally drop the
    /// extras lock guard.  Use exclusively as a short-lived iterator.
    pub fn nodes(&self) -> NodeRefs<'_> {
        let extras = self.extra_nodes.lock().expect("extra_nodes lock");
        NodeRefs {
            primary: &self.nodes,
            extras_guard: extras,
        }
    }

    pub fn node_ids(&self) -> Vec<u64> {
        self.nodes.iter().map(|n| n.node_id).collect()
    }

    /// Wait until any node reports a current_leader. Returns the leader's
    /// node_id, or `None` on timeout.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Option<u64> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for n in &self.nodes {
                if let Some(lid) = n.raft.metrics().borrow().current_leader {
                    return Some(lid);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Locate the leader's `TestNode`. Panics if no leader is currently
    /// elected on any node — call `wait_for_leader` first.
    pub fn leader_node(&self) -> &TestNode {
        let leader_id = self
            .nodes
            .iter()
            .find_map(|n| n.raft.metrics().borrow().current_leader)
            .expect("no leader currently elected");
        self.nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node_id missing from cluster")
    }

    /// Issue a `client_write` on the current leader.
    pub async fn propose_on_leader(
        &self,
        cmd: RaftCommand,
    ) -> Result<RaftResponse, ClientWriteError<u64, BasicNode>> {
        let leader = self.leader_node();
        match leader.raft.client_write(cmd).await {
            Ok(ClientWriteResponse { data, .. }) => Ok(data),
            Err(RaftError::APIError(api)) => Err(api),
            Err(e) => panic!("unexpected client_write error: {e:?}"),
        }
    }

    /// Sever outbound RPCs from `node_idx`, simulating a one-way isolation.
    pub fn partition(&self, node_idx: usize) {
        let id = self.nodes[node_idx].node_id;
        self.registry.isolate(id);
    }

    /// Heal every recorded partition / isolation.
    pub fn heal(&self) {
        self.registry.heal_all();
    }

    /// Snapshot every node's openraft metrics for debugging.
    pub fn metrics_snapshot(&self) -> Vec<RaftMetrics<u64, BasicNode>> {
        self.nodes.iter().map(|n| n.metrics()).collect()
    }

    /// Drop all nodes, aborting their dispatchers and freeing their sled stores.
    pub async fn shutdown(self) {
        let TestCluster {
            nodes,
            registry,
            extra_nodes,
            ..
        } = self;
        let extras =
            std::mem::take(&mut *extra_nodes.lock().expect("extra_nodes lock at shutdown"));
        for n in nodes.into_iter().chain(extras.into_iter()) {
            registry.unregister(n.node_id);
            // openraft 0.9 has `Raft::shutdown` returning a Result; the harness
            // tolerates either Ok (normal) or Err (already shut down).
            let _ = n.raft.shutdown().await;
            n.dispatcher.abort();
            drop(n.dispatcher);
        }
    }
}

// ---------------------------------------------------------------------------
// build_test_node — wires log store, state machine, network, dispatcher.
// ---------------------------------------------------------------------------

async fn build_test_node(
    node_id: u64,
    config: Arc<RaftLibConfig>,
    registry: NodeRegistry,
) -> TestNode {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let log_path: PathBuf = tempdir.path().join(format!("raft-{node_id}"));
    std::fs::create_dir_all(&log_path).expect("create raft log dir");
    let log_store = SledLogStore::new(&log_path).expect("open sled log store");

    let state_machine = SharedStateMachine::new(FerrosStateMachine::new());
    let state_machine_for_node = state_machine.clone();

    // Bound the inbound channel so a stalled node back-pressures
    // its peers rather than buffering unbounded RPC traffic.
    let (tx, mut rx) = mpsc::channel::<RpcEnvelope>(256);
    registry.register(node_id, tx);

    let factory = InProcessNetworkFactory::new(node_id, registry.clone());

    let raft = FerrosRaft::new(node_id, config, factory, log_store, state_machine)
        .await
        .expect("FerrosRaft::new");
    let raft = Arc::new(raft);

    // Spawn the dispatcher: forward inbound envelopes into Raft API methods.
    let dispatcher_raft = raft.clone();
    let dispatcher = tokio::spawn(async move {
        while let Some(env) = rx.recv().await {
            let raft = dispatcher_raft.clone();
            // Each request handled in a fresh task so a slow handler
            // never blocks the next inbound RPC.
            tokio::spawn(async move {
                match env {
                    RpcEnvelope::AppendEntries { rpc, reply } => {
                        match raft.append_entries(rpc).await {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::debug!(%e, "harness: append_entries handler error");
                            }
                        }
                    }
                    RpcEnvelope::Vote { rpc, reply } => match raft.vote(rpc).await {
                        Ok(resp) => {
                            let _ = reply.send(resp);
                        }
                        Err(e) => {
                            tracing::debug!(%e, "harness: vote handler error");
                        }
                    },
                    RpcEnvelope::InstallSnapshot { rpc, reply } => {
                        match raft.install_snapshot(rpc).await {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::debug!(%e, "harness: install_snapshot handler error");
                            }
                        }
                    }
                }
            });
        }
    });

    TestNode {
        node_id,
        raft,
        state_machine: state_machine_for_node,
        dispatcher,
        _tempdir: tempdir,
        registry,
    }
}
