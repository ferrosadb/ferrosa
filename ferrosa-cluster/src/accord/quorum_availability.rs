//! Quorum availability of PostgreSQL transactions (FMEA CL-28).
//!
//! The governing criterion: while a quorum of replicas is reachable, a
//! transaction must commit no matter how one replica misbehaves — no per-peer
//! timeout may decide its fate. When a quorum is NOT reachable, an error is
//! correct, and it must say so promptly rather than surface as a dependency-wait
//! timeout.
//!
//! These tests drive real [`AccordHandler`] state machines through the real
//! [`AccordTransactionCommitter`] over an in-process transport, so a "paused"
//! replica behaves like `docker pause`: the connection stays up and a request
//! to it never gets an answer until the RPC deadline.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ferrosa_common::accord::HybridLogicalClock;
use ferrosa_net::error::NetError;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_storage::accord::sync_writer::MockSyncWriter;
use ferrosa_storage::accord::{CommitOutcome, TransactionCommitter, TransactionWrite};
use uuid::Uuid;

use crate::accord::apply::{ApplyError, ApplyMutation, StorageApplier};

use crate::accord::handlers::{AccordHandler, AccordState};
use crate::accord::state_machine::AccordStateMachine;
use crate::accord::transaction_commit::{AccordTransactionCommitter, ReplicaResolver};
use crate::accord::transport::AccordTransport;

/// Build a staged Accord write-set from resident writes — the shape
/// [`TransactionCommitter::commit_postgres`] now takes. These tests drive one key at a
/// time, so the set stays wholly resident and never touches the filesystem.
fn staged(writes: Vec<TransactionWrite>) -> ferrosa_storage::accord::StagedAccordWriteSet {
    let mut builder =
        ferrosa_storage::accord::StagedAccordWriteSetBuilder::with_threshold_bytes(u64::MAX);
    for write in writes {
        builder.accept(write).expect("stage write");
    }
    builder.finish().expect("finish staging")
}

/// One buffered write, the shape the PostgreSQL front-end stages for a row.
fn write(keyspace: &str, key: &str, mutation: &str) -> TransactionWrite {
    TransactionWrite {
        keyspace: keyspace.to_string(),
        key: key.as_bytes().to_vec(),
        mutation: mutation.as_bytes().to_vec(),
    }
}

/// Records the payload of every mutation the apply engine persists, so a test can
/// assert the VALUES the engine produced rather than only how many transactions it
/// reported as committed.
#[derive(Default)]
struct RecordingApplier {
    applied: parking_lot::Mutex<Vec<Vec<u8>>>,
}

impl RecordingApplier {
    fn applied(&self) -> Vec<Vec<u8>> {
        self.applied.lock().clone()
    }
}

#[async_trait]
impl StorageApplier for RecordingApplier {
    fn register_postgres_mvcc_observer(
        &self,
        _observer: Arc<dyn ferrosa_storage::accord::PostgresMvccApplyObserver>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn apply(
        &self,
        _txn_id: ferrosa_common::accord::TxnId,
        mutation: ApplyMutation,
    ) -> Result<(), ApplyError> {
        if !mutation.data.is_empty() {
            self.applied.lock().push(mutation.data);
        }
        Ok(())
    }
}

/// The on-call total a write-skew workload leaves behind: `doctor-a` and `doctor-b`
/// both start on call (1) and a writer moves one of them off call (0). A correct
/// engine leaves exactly one on call; a violation leaves none.
fn on_call_total(appliers: &[Arc<RecordingApplier>]) -> i64 {
    let mut on_call = std::collections::BTreeMap::from([("doctor-a", 1_i64), ("doctor-b", 1_i64)]);
    for applier in appliers {
        for payload in applier.applied() {
            match payload.as_slice() {
                b"doctor-a=0" => {
                    on_call.insert("doctor-a", 0);
                }
                b"doctor-b=0" => {
                    on_call.insert("doctor-b", 0);
                }
                _ => {}
            }
        }
    }
    on_call.values().sum()
}

/// How a replica answers requests.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Reachability {
    Live,
    /// `docker pause`: the request is accepted and never answered; the caller
    /// gives up at its RPC deadline.
    Paused,
    /// Connection refused or reset: the caller fails at once.
    Down,
    /// Accepted, applied, but the ACK is slower than the caller's RPC deadline:
    /// the FIRST Apply to this replica consumes the full deadline and the caller
    /// gives up (its late `ApplyOK` is the live `orphan RPC response`), and a
    /// RE-SENT Apply for the same transaction is answered at once. This is the
    /// live incident exactly: the replica is REACHABLE and its rows are durable,
    /// only its ack outlived the caller's per-peer timeout.
    SlowApplyAck,
    /// The Apply ACK never comes back at all: every attempt is accepted and the
    /// caller gives up at its RPC deadline. This is the OTHER half of the live
    /// `orphan RPC response` — the ack is LOST, not merely late — so the apply
    /// quorum is never reached and the committed-but-not-quorum path runs.
    ApplyAckLost,
}

/// What an `ApplyAckLost` replica costs each caller (short: the fixture models a
/// LOST ack, not a slow one).
const LOST_APPLY_ACK_DEADLINE: Duration = Duration::from_millis(100);

/// The Data-lane RPC deadline a paused peer costs its caller.
const PAUSED_RPC_DEADLINE: Duration = Duration::from_secs(10);

struct ClusterTransport {
    handlers: HashMap<Uuid, Arc<AccordHandler>>,
    reachability: parking_lot::Mutex<HashMap<Uuid, Reachability>>,
    /// Hosts whose first Apply has already been answered late, so a re-sent
    /// Apply is served at once (`Reachability::SlowApplyAck`).
    slow_apply_seen: parking_lot::Mutex<HashSet<Uuid>>,
    /// When set, the first PreAccept each coordinator sends waits here until
    /// every coordinator has registered the transaction locally, so their
    /// PreAccepts cross on the wire (node 1 sees Y then X, node 2 sees X then
    /// Y). Raced for real, this window is a few hundred microseconds wide.
    preaccept_crossing: parking_lot::Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

/// Whether `msg` is an Apply request — the only phase whose ack the slow-peer
/// fixture withholds (`Reachability::SlowApplyAck`).
fn is_apply_request(msg: &Message) -> bool {
    matches!(
        msg,
        Message::AccordApply(_)
            | Message::AccordApplyV2(_)
            | Message::AccordApplyV2Capnp(_)
            | Message::AccordApplyV2Region(_)
    )
}

/// A coordinator's view of the cluster transport.
struct NodeTransport {
    cluster: Arc<ClusterTransport>,
    crossed: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl AccordTransport for NodeTransport {
    async fn send(
        &self,
        host: Uuid,
        msg: Message,
        lane: ferrosa_net::codec::Lane,
    ) -> ferrosa_net::error::Result<Message> {
        let is_preaccept = matches!(
            msg,
            Message::AccordPreAccept(_) | Message::AccordPreAcceptV2(_)
        );
        if is_preaccept && !self.crossed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let gate = self.cluster.preaccept_crossing.lock().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
        }
        self.cluster.send(host, msg, lane).await
    }
}

impl ClusterTransport {
    fn set(&self, host: Uuid, reachability: Reachability) {
        self.reachability.lock().insert(host, reachability);
    }
}

#[async_trait]
impl AccordTransport for ClusterTransport {
    async fn send(
        &self,
        host: Uuid,
        msg: Message,
        _lane: ferrosa_net::codec::Lane,
    ) -> ferrosa_net::error::Result<Message> {
        let reachability = self
            .reachability
            .lock()
            .get(&host)
            .copied()
            .unwrap_or(Reachability::Down);
        match reachability {
            Reachability::Down => return Err(NetError::Timeout("peer unreachable".into())),
            Reachability::Paused => {
                tokio::time::sleep(PAUSED_RPC_DEADLINE).await;
                return Err(NetError::Timeout("Data lane timeout".into()));
            }
            Reachability::SlowApplyAck if is_apply_request(&msg) => {
                // The FIRST Apply is applied but its ack outlives the caller's
                // RPC deadline, so the caller gives up (this is the live
                // `orphan RPC response`). A RE-SENT Apply for the same
                // transaction is a durable no-op on the replica, so it is
                // answered at once — the peer was reachable the whole time.
                let first = self.slow_apply_seen.lock().insert(host);
                if first {
                    tokio::time::sleep(PAUSED_RPC_DEADLINE).await;
                    return Err(NetError::Timeout(
                        "Apply ack outlived the RPC deadline".into(),
                    ));
                }
            }
            Reachability::ApplyAckLost if is_apply_request(&msg) => {
                // The ack is lost: the replica applied (in the live incident its
                // `ApplyOK` arrived as an orphan long after the caller stopped
                // waiting), so the coordinator can never learn of it from THIS
                // transaction's fan-out.
                tokio::time::sleep(LOST_APPLY_ACK_DEADLINE).await;
                return Err(NetError::Timeout("Apply ack lost".into()));
            }
            Reachability::Live | Reachability::SlowApplyAck | Reachability::ApplyAckLost => {}
        }
        let handler = self
            .handlers
            .get(&host)
            .ok_or_else(|| NetError::Timeout("unknown peer".into()))?;
        let peer: PeerId = (host, "127.0.0.1:0".parse().expect("addr"));
        handler
            .handle(peer, msg)
            .await
            .ok_or_else(|| NetError::Timeout("no response".into()))
    }
}

fn host(n: u128) -> Uuid {
    Uuid::from_u128((n << 64) | n)
}

fn node_id_of(host: Uuid) -> u64 {
    u64::from_be_bytes(host.as_bytes()[..8].try_into().expect("uuid 16 bytes"))
}

/// Three replicas (every key lives on all of them) and one committer per node.
struct Cluster {
    hosts: [Uuid; 3],
    transport: Arc<ClusterTransport>,
    node_transports: Vec<Arc<NodeTransport>>,
    committers: Vec<Arc<AccordTransactionCommitter>>,
    /// One recording applier per node — the mutations each node's replica
    /// actually persisted, so a test can assert the resulting VALUES.
    appliers: Vec<Arc<RecordingApplier>>,
}

/// How a key's replica set is placed on the three nodes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// Every key on every replica: ONE rf=3 shard, whose slow quorum is 2 of 3.
    AllReplicas,
    /// Each key on exactly ONE replica: one rf=1 shard per owner, so a
    /// transaction that spans the owners needs EVERY owner's ack — the bench's
    /// RF=1 shape, where the apply quorum is the whole (three-node) replica
    /// set. The two PostgreSQL protocol keys live on the coordinator (node 1),
    /// so a barrier never depends on a remote shard.
    PerKeyOwner,
}

/// The replica host ids `key` resolves to under `placement`.
fn placed_replicas(placement: Placement, hosts: &[Uuid; 3], key: &[u8]) -> Vec<Uuid> {
    match placement {
        Placement::AllReplicas => hosts.to_vec(),
        Placement::PerKeyOwner => {
            let is_protocol = key.starts_with(b"\0ferrosa:postgres:");
            match key.last().filter(|last| last.is_ascii_digit()) {
                Some(last) if !is_protocol => {
                    let owner = ((last - b'1') as usize).min(hosts.len() - 1);
                    vec![hosts[owner]]
                }
                _ => vec![hosts[0]],
            }
        }
    }
}

impl Cluster {
    fn three_nodes() -> Self {
        Self::three_nodes_placed(Placement::AllReplicas)
    }

    fn three_nodes_placed(placement: Placement) -> Self {
        let hosts = [host(0x1111), host(0x2222), host(0x3333)];
        let mut handlers = HashMap::new();
        let mut states: Vec<AccordState> = Vec::new();
        let mut appliers: Vec<Arc<RecordingApplier>> = Vec::new();
        for h in hosts {
            let node_id = node_id_of(h);
            let applier = Arc::new(RecordingApplier::default());
            appliers.push(Arc::clone(&applier));
            let state: AccordState = Arc::new(parking_lot::Mutex::new(
                AccordStateMachine::with_applier(node_id, Arc::new(MockSyncWriter::new()), applier),
            ));
            handlers.insert(h, Arc::new(AccordHandler::new(state.clone(), node_id)));
            states.push(state);
        }
        let transport = Arc::new(ClusterTransport {
            handlers,
            reachability: parking_lot::Mutex::new(
                hosts.iter().map(|h| (*h, Reachability::Live)).collect(),
            ),
            slow_apply_seen: parking_lot::Mutex::new(HashSet::new()),
            preaccept_crossing: parking_lot::Mutex::new(None),
        });
        let replicas = hosts.to_vec();
        let node_transports: Vec<Arc<NodeTransport>> = hosts
            .iter()
            .map(|_| {
                Arc::new(NodeTransport {
                    cluster: transport.clone(),
                    crossed: std::sync::atomic::AtomicBool::new(true),
                })
            })
            .collect();
        let committers = hosts
            .iter()
            .zip(states)
            .zip(node_transports.iter())
            .zip(appliers.iter())
            .map(|(((h, state), node_transport), recording)| {
                let node_id = node_id_of(*h);
                let replicas = replicas.clone();
                let hosts_for_resolve = hosts;
                let resolve: ReplicaResolver = match placement {
                    Placement::AllReplicas => {
                        Arc::new(move |_ks: &str, _key: &[u8]| Some(replicas.clone()))
                    }
                    Placement::PerKeyOwner => Arc::new(move |_ks: &str, key: &[u8]| {
                        Some(placed_replicas(placement, &hosts_for_resolve, key))
                    }),
                };
                // origin/main built each committer with a `NoopStorageApplier`;
                // the write-skew tests assert the VALUES a node persisted, so
                // each committer keeps its own node's recording applier.
                let applier: Arc<dyn StorageApplier> = recording.clone();
                Arc::new(
                    AccordTransactionCommitter::new(
                        node_id,
                        Arc::new(HybridLogicalClock::new(node_id, 0)),
                        node_transport.clone(),
                        applier,
                        resolve,
                    )
                    .with_local_accord_state(state),
                )
            })
            .collect();
        Self {
            hosts,
            transport,
            node_transports,
            committers,
            appliers,
        }
    }

    /// Make the next PreAccept each node sends wait for the other, so the two
    /// transactions cross on the wire.
    fn arm_preaccept_crossing(&self) {
        // Exactly CROSSING_NODES nodes may cross: the barrier below is sized to
        // that, so a third participant would wait on a party that never comes.
        // Named rather than a bare `take(2)` so the pairing with the barrier is
        // explicit (and so the P0 OOM audit does not read it as a result cap).
        const CROSSING_NODES: usize = 2;
        *self.transport.preaccept_crossing.lock() =
            Some(Arc::new(tokio::sync::Barrier::new(CROSSING_NODES)));
        for node in &self.node_transports[..CROSSING_NODES] {
            node.crossed
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// One PostgreSQL transaction as the gateway runs it: a snapshot barrier at
/// BEGIN, then a commit carrying the data write. A stale snapshot is the
/// ordinary serialization failure a client retries, so it is not a failure
/// here; any other error is.
async fn run_postgres_transaction(
    committer: &AccordTransactionCommitter,
    actor: usize,
    attempt_budget: usize,
) -> Result<(), String> {
    for attempt in 0..attempt_budget {
        let snapshot = committer
            .begin_postgres_snapshot("ks")
            .await
            .map_err(|e| format!("begin: {}", e.reason))?;
        // A SELECT inside the transaction re-validates its snapshot.
        match committer.validate_postgres_snapshot("ks", snapshot).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => return Err(format!("validate: {}", e.reason)),
        }
        let write = TransactionWrite {
            keyspace: "ks".to_string(),
            key: format!("account-{actor}").into_bytes(),
            mutation: format!("write-{actor}-{attempt}").into_bytes(),
        };
        match committer
            // This synthetic workload runs no table-level `SELECT` (its transfer
            // reads are row-level), so its read set is EMPTY. `["t"]` was an inert
            // placeholder while the commit path ignored the argument; now that a
            // table-level read participates in the Accord ordering, a shared
            // placeholder would make every actor conflict on one table and starve
            // the queue — which is not what this quorum-availability test measures.
            .commit_postgres("ks", staged(vec![write]), Vec::new(), snapshot)
            .await
        {
            Ok(CommitOutcome::Committed) => return Ok(()),
            Ok(CommitOutcome::Aborted { .. }) => continue,
            Err(e) => return Err(format!("commit: {}", e.reason)),
        }
    }
    Err(format!(
        "actor {actor} never committed in {attempt_budget} attempts"
    ))
}

/// A read-only transaction: BEGIN, then the COMMIT-time snapshot validation.
async fn run_postgres_read(committer: &AccordTransactionCommitter) -> Result<(), String> {
    let snapshot = committer
        .begin_postgres_snapshot("ks")
        .await
        .map_err(|e| format!("read begin: {}", e.reason))?;
    committer
        .validate_postgres_snapshot("ks", snapshot)
        .await
        .map(|_still_valid| ())
        .map_err(|e| format!("read validate: {}", e.reason))
}

/// The PostgreSQL Jepsen fault schedule (run 36812586709): clients on nodes 1
/// and 2, node 3 paused. Nodes 1 and 2 are a quorum of three, so every
/// transaction must commit. Before CL-28 a transaction queued behind its
/// predecessors on the single barrier key and failed "snapshot barrier
/// dependencies were not applied locally" once the queue outlasted a fixed 5 s
/// dependency wait — a timeout, not a missing quorum, decided its fate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_replica_does_not_fail_postgres_transactions_while_a_quorum_is_live() {
    // The shape of `postgres_strict_serializable`: five actors on the two live
    // nodes, each running transfers interleaved with read-only transactions.
    const ACTORS: usize = 5;
    const TRANSACTIONS_PER_ACTOR: usize = 2;

    let cluster = Cluster::three_nodes();
    cluster
        .transport
        .set(cluster.hosts[2], Reachability::Paused);

    let started = Instant::now();
    let mut actors = Vec::new();
    for actor in 0..ACTORS {
        let committer = Arc::clone(&cluster.committers[actor % 2]);
        actors.push(tokio::spawn(async move {
            for _ in 0..TRANSACTIONS_PER_ACTOR {
                run_postgres_transaction(&committer, actor, 20).await?;
                run_postgres_read(&committer).await?;
            }
            Ok::<(), String>(())
        }));
    }

    let mut failures = Vec::new();
    for actor in actors {
        if let Err(error) = actor.await.expect("actor task panicked") {
            failures.push(error);
        }
    }
    assert!(
        failures.is_empty(),
        "a live quorum (nodes 1 and 2) must commit every transaction while node 3 is \
         paused; {} of {ACTORS} actors failed after {:?}: {failures:#?}",
        failures.len(),
        started.elapsed()
    );
}

/// A LONG PostgreSQL queue over a paused replica — the same criterion as
/// [`a_paused_replica_does_not_fail_postgres_transactions_while_a_quorum_is_live`],
/// stressed past the 5 s barrier budget.
///
/// Every PG transaction serializes on the one cluster-global barrier key, and a
/// paused replica widens each transaction's PreAccept-to-Commit window by the
/// fast-path wait, so the queue drains at roughly one transaction per second.
/// A barrier near the tail therefore waits far longer than the 5 s barrier
/// budget — but the engine is applying continuously underneath it, one
/// predecessor at a time. The barrier must decide on that PROGRESS, not on a
/// wall clock: each completed apply resets its stall backstop, so the queue
/// drains and every transaction commits. Before that fix the tail abstained at
/// exactly 5 s with "PostgreSQL snapshot barrier dependencies were not applied
/// locally" while its predecessors were still applying — a live quorum failing
/// a transaction the engine was still resolving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_postgres_queue_over_a_paused_replica_still_commits_on_a_live_quorum() {
    // Enough actors that the tail of the single-key queue outlasts the 5 s
    // barrier budget; the queue's own drain (not a timeout) must decide.
    const ACTORS: usize = 9;
    const TRANSACTIONS_PER_ACTOR: usize = 2;

    let cluster = Cluster::three_nodes();
    cluster
        .transport
        .set(cluster.hosts[2], Reachability::Paused);

    let started = Instant::now();
    let mut actors = Vec::new();
    for actor in 0..ACTORS {
        let committer = Arc::clone(&cluster.committers[actor % 2]);
        actors.push(tokio::spawn(async move {
            for _ in 0..TRANSACTIONS_PER_ACTOR {
                run_postgres_transaction(&committer, actor, 20).await?;
                run_postgres_read(&committer).await?;
            }
            Ok::<(), String>(())
        }));
    }

    let mut failures = Vec::new();
    for actor in actors {
        if let Err(error) = actor.await.expect("actor task panicked") {
            failures.push(error);
        }
    }
    assert!(
        failures.is_empty(),
        "a live quorum must commit every transaction of a long queue while node 3 is \
         paused; {} of {ACTORS} actors failed after {:?}: {failures:#?}",
        failures.len(),
        started.elapsed()
    );
}

/// The other half of the criterion: with only one of three replicas reachable
/// there is no quorum, so an error is correct — and it must be prompt and name
/// the missing quorum, never a dependency-wait timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_quorum_fails_promptly_and_names_the_quorum() {
    let cluster = Cluster::three_nodes();
    cluster.transport.set(cluster.hosts[1], Reachability::Down);
    cluster.transport.set(cluster.hosts[2], Reachability::Down);

    let started = Instant::now();
    let error = cluster.committers[0]
        .begin_postgres_snapshot("ks")
        .await
        .expect_err("one reachable replica of three is not a quorum");
    let elapsed = started.elapsed();

    assert!(
        error.reason.to_lowercase().contains("quorum"),
        "the error must name the missing quorum, got: {}",
        error.reason
    );
    assert!(
        !error.reason.contains("not applied locally"),
        "a missing quorum must not surface as a dependency-wait failure: {}",
        error.reason
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "an unreachable quorum must fail promptly, took {elapsed:?}"
    );
}

/// A snapshot barrier and a data commit started on different nodes whose
/// PreAccepts cross.
///
/// Node 1 registers the barrier (Y) then the commit (X); node 2 registers X then
/// Y. X's `t0` is the later, so X depends on Y, yet Y's execution timestamp is
/// bumped past X's. Each now waits for the other: X's Apply parks on Y's apply,
/// and Y's snapshot read waits for X to apply. The apply engine's wait graph
/// cannot see that cycle because one edge lives in the snapshot dependency wait.
/// Every replica is live, so only the cycle can fail either, and only a timeout
/// breaks it. A quorum is up, so both transactions must commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_barrier_and_a_commit_whose_preaccepts_cross_do_not_deadlock() {
    let cluster = Cluster::three_nodes();
    let snapshot = cluster.committers[1]
        .begin_postgres_snapshot("ks")
        .await
        .expect("an idle cluster grants a snapshot");
    cluster.arm_preaccept_crossing();

    let started = Instant::now();
    let barrier_node = Arc::clone(&cluster.committers[0]);
    let commit_node = Arc::clone(&cluster.committers[1]);
    let barrier = tokio::spawn(async move { barrier_node.begin_postgres_snapshot("ks").await });
    // The barrier mints its t0 first, so the commit's t0 is the later one.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let write = TransactionWrite {
        keyspace: "ks".to_string(),
        key: b"account".to_vec(),
        mutation: b"write".to_vec(),
    };
    let commit = tokio::spawn(async move {
        commit_node
            .commit_postgres("ks", staged(vec![write]), vec!["t".to_string()], snapshot)
            .await
    });
    let barrier = barrier.await.expect("barrier task panicked");
    let commit = commit.await.expect("commit task panicked");

    assert!(
        barrier.is_ok() && matches!(commit, Ok(CommitOutcome::Committed)),
        "both must commit with every replica live; after {:?}: barrier -> {:?}, commit -> {:?}",
        started.elapsed(),
        barrier.as_ref().err().map(|e| e.reason.clone()),
        commit.as_ref().map_err(|e| e.reason.clone()),
    );
}

/// The PostgreSQL write-skew invariant, at the engine seam (FMEA CL-72).
///
/// Two SERIALIZABLE transactions each read the whole `doctors` table — their
/// `WHERE id = $1` SELECT degrades to a TABLE-level read because a bind parameter
/// is not an inline literal — and each moves ONE doctor off call, on DISJOINT
/// write keys. Serializable requires ONE of them to be refused: if both commit,
/// both doctors are off call and the on-call total collapses from 1 to 0. That
/// is the exact invariant `postgres_strict_serializable` asserts
/// (`final_state["doctor-a"] + final_state["doctor-b"] == 1`) and violates ~50%
/// of live runs.
///
/// T1 commits first, so T2's table read — taken at its BEGIN snapshot, before T1
/// committed — is provably stale by the time T2 orders. Today the read set never
/// reaches Accord (the commit path drops the `tables` argument), so T2's write of
/// a DISJOINT row conflicts with nothing and it commits: the engine produces 0
/// doctors on call. The fix must make the read table participate in the ordering
/// so T2 is refused as a retryable serialization failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_disjoint_writer_serializes_against_a_table_the_earlier_writer_changed() {
    let cluster = Cluster::three_nodes();

    // Both BEGIN before either COMMITs — the barrier the Jepsen actors hold.
    let first_snapshot = cluster.committers[0]
        .begin_postgres_snapshot("public")
        .await
        .expect("first BEGIN");
    let second_snapshot = cluster.committers[1]
        .begin_postgres_snapshot("public")
        .await
        .expect("second BEGIN");

    // Both SELECT the table: doctor-a = 1, doctor-b = 1 — so both decide to move
    // their own doctor off call.
    let read_tables = vec!["public.doctors".to_string()];

    let first = cluster.committers[0]
        .commit_postgres(
            "public",
            staged(vec![write("public", "doctor-a", "doctor-a=0")]),
            read_tables.clone(),
            first_snapshot,
        )
        .await
        .expect("first commit");
    assert_eq!(
        first,
        CommitOutcome::Committed,
        "the first writer must commit"
    );

    // T2 holds a snapshot from before T1 committed, read the table T1 changed,
    // and writes a DISJOINT row. It must be refused, not committed.
    let second = cluster.committers[1]
        .commit_postgres(
            "public",
            staged(vec![write("public", "doctor-b", "doctor-b=0")]),
            read_tables.clone(),
            second_snapshot,
        )
        .await
        .expect("second commit");

    let total = on_call_total(&cluster.appliers);
    assert_eq!(
        total, 1,
        "serializable write-skew must leave ONE doctor on call; the engine applied writes \
         leaving {total} (0 means BOTH writers committed and the on-call cell was lost); \
         second writer outcome: {second:?}"
    );
    assert!(
        matches!(second, CommitOutcome::Aborted { .. }),
        "the second writer read a table the first writer changed after its snapshot, so it must \
         be refused as a retryable serialization failure; got {second:?}"
    );
}

/// The SAME workload, but genuinely simultaneous: both writers commit at once, so
/// neither PreAccept is guaranteed to observe the other ahead of time. A correct
/// engine must never let BOTH go off call — the read set must participate in the
/// DISTRIBUTED ordering, not merely a local check that a different node's commit
/// has not yet reached.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_simultaneous_write_skew_writers_never_both_go_off_call() {
    let cluster = Cluster::three_nodes();
    let first_snapshot = cluster.committers[0]
        .begin_postgres_snapshot("public")
        .await
        .expect("first BEGIN");
    let second_snapshot = cluster.committers[1]
        .begin_postgres_snapshot("public")
        .await
        .expect("second BEGIN");
    // Force the two commits' PreAccepts to cross on the wire, so each node sees
    // the other's transaction while its own is in flight — the live race window.
    cluster.arm_preaccept_crossing();

    let read_tables = vec!["public.doctors".to_string()];
    let first = tokio::spawn({
        let committer = Arc::clone(&cluster.committers[0]);
        let tables = read_tables.clone();
        async move {
            committer
                .commit_postgres(
                    "public",
                    staged(vec![write("public", "doctor-a", "doctor-a=0")]),
                    tables,
                    first_snapshot,
                )
                .await
        }
    });
    let second = tokio::spawn({
        let committer = Arc::clone(&cluster.committers[1]);
        let tables = read_tables.clone();
        async move {
            committer
                .commit_postgres(
                    "public",
                    staged(vec![write("public", "doctor-b", "doctor-b=0")]),
                    tables,
                    second_snapshot,
                )
                .await
        }
    });
    let first = first
        .await
        .expect("first task panicked")
        .expect("first commit");
    let second = second
        .await
        .expect("second task panicked")
        .expect("second commit");

    let total = on_call_total(&cluster.appliers);
    // Serializable permits either writer to lose; it never permits BOTH to go off
    // call. With both PreAccepts in flight each node refuses the other, so both may
    // abort here — the live workload retries and the survivor converges — but the
    // on-call cell is never lost.
    assert!(
        total >= 1,
        "two simultaneous write-skew writers must never BOTH go off call; the engine applied \
         writes leaving {total} on call (0 means both committed); first: {first:?}, second: {second:?}"
    );
}

/// An OVERSIZED (multi-key) commit spanning more than one shard in the bench's
/// RF=1 shape, where a shard has ONE owner and the apply quorum is therefore
/// EVERY owner.
///
/// One owner's Apply is REACHABLE but slower than the coordinator's per-peer RPC
/// deadline: it applied durably, its `ApplyOK` outlived the deadline (the live
/// `orphan RPC response`), and the coordinator's own shard plus the one acked
/// peer reach only 2 of 3 shards. A quorum of replicas is up, so ONE peer's
/// per-peer timeout must not decide the transaction's fate: the commit must
/// succeed and every node must then serve a following SELECT. Before the apply
/// fan-out re-sent to a shard whose ack was merely late AND the coordinator
/// converged its OWN shard on the committed-but-not-quorum path, the commit
/// failed `Apply quorum not reached within 10s, but 1 replica durably applied
/// ... (NOT an abandon; do NOT retry)`, the coordinator's own shard was never
/// written, and every following statement on every node failed `PostgreSQL
/// snapshot barrier dependencies were not applied locally` (FMEA CL-71).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_apply_ack_does_not_decide_an_oversized_commits_fate() {
    let cluster = Cluster::three_nodes_placed(Placement::PerKeyOwner);
    // Node 3 owns the shard whose apply ack is slow; nodes 1 and 2 are a quorum.
    cluster
        .transport
        .set(cluster.hosts[2], Reachability::SlowApplyAck);

    let committer = Arc::clone(&cluster.committers[0]);
    let snapshot = committer
        .begin_postgres_snapshot("ks")
        .await
        .expect("an idle cluster grants a snapshot");
    // One key per owner: the write-set spans three rf=1 shards, so the
    // coordinator self-acks only its own and needs BOTH peers to reach quorum.
    let writes: Vec<TransactionWrite> = (1..=3)
        .map(|owner| TransactionWrite {
            keyspace: "ks".to_string(),
            key: format!("k-owner-{owner}").into_bytes(),
            mutation: format!("write-{owner}").into_bytes(),
        })
        .collect();

    let started = Instant::now();
    let outcome = committer
        .commit_postgres("ks", staged(writes), vec!["t".to_string()], snapshot)
        .await;
    assert!(
        matches!(outcome, Ok(CommitOutcome::Committed)),
        "a quorum of replicas (nodes 1 and 2) is up and node 3 is only SLOW, so the commit \
         must succeed — one peer's per-peer RPC deadline must never decide the fate of a \
         transaction that is committed and durably applied; after {:?}: {outcome:?}",
        started.elapsed()
    );

    for (node, committer) in cluster.committers.iter().enumerate() {
        let read = run_postgres_read(committer).await;
        assert!(
            read.is_ok(),
            "a SELECT must succeed on node {} after the commit: {read:?}",
            node + 1
        );
    }
}

/// The other half of the criterion. When the apply quorum is genuinely
/// UNREACHABLE — here the third owner's Apply ACK is LOST, not merely late — the
/// commit must fail loud and non-retryably, because the transaction IS committed
/// on the replicas that acked. But the coordinator must ALSO converge its OWN
/// shard in the same breath: leaving it unwritten strands this committed
/// transaction as a permanent dependency on the coordinator's own keys, so every
/// later statement there abstains ("PostgreSQL snapshot barrier dependencies were
/// not applied locally") until a restart. The error is the correct outcome; a
/// permanent wedge is not. This is the deferred own-shard apply of CL-60 run on
/// the committed-but-not-quorum path — the abandon ordering it protects is
/// untouched, because this path never abandons.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_apply_ack_still_converges_the_coordinators_own_shard() {
    let cluster = Cluster::three_nodes_placed(Placement::PerKeyOwner);
    cluster
        .transport
        .set(cluster.hosts[2], Reachability::ApplyAckLost);

    let committer = Arc::clone(&cluster.committers[0]);
    let snapshot = committer
        .begin_postgres_snapshot("ks")
        .await
        .expect("an idle cluster grants a snapshot");
    let writes: Vec<TransactionWrite> = (1..=3)
        .map(|owner| TransactionWrite {
            keyspace: "ks".to_string(),
            key: format!("k-owner-{owner}").into_bytes(),
            mutation: format!("write-{owner}").into_bytes(),
        })
        .collect();

    let outcome = committer
        .commit_postgres("ks", staged(writes), vec!["t".to_string()], snapshot)
        .await;
    let error = outcome.expect_err("an unreachable apply quorum must fail loud, never ack");
    assert!(
        error.reason.contains("committed"),
        "the failure must NAME the commit so the client knows not to retry: {}",
        error.reason
    );
    assert!(
        !error.reason.contains("safe to retry"),
        "a committed transaction must never be advertised as safe to retry: {}",
        error.reason
    );

    // The coordinator's own shard MUST have converged: its local replica owns the
    // barrier/marker keys AND k-owner-1, so a barrier that never resolves there
    // wedges every later statement on the node.
    let coordinator_read = run_postgres_read(&cluster.committers[0]).await;
    assert!(
        coordinator_read.is_ok(),
        "the coordinator's own shard must converge on the committed-but-not-quorum path, \
         otherwise every later statement on the node abstains; got {coordinator_read:?}"
    );
    // The replica that acked applied its own shard, so its statements work too.
    let acked_read = run_postgres_read(&cluster.committers[1]).await;
    assert!(
        acked_read.is_ok(),
        "the acked replica's shard must be readable; got {acked_read:?}"
    );
}

/// The absent-row half of a scan's read set: a transaction that INSERTs a row into
/// a table a SERIALIZABLE transaction READ must still be refused, even though the
/// INSERTer never scanned the table and so registers no table-level read key.
///
/// A scan's read set is not only the rows it SAW. Strict serializability must also
/// catch a PHANTOM — a row INSERTED into the scanned range after the snapshot — and
/// no set of visited rows can observe it. The table-level read key cannot either:
/// only a SCANNER registers it, so a writer that never scanned is invisible to it,
/// and the stale scan commits on the strength of a row it never saw (the FMEA CL-72 /
/// PG-17 residual). The writer must instead leave EVIDENCE the scanner consults.
///
/// Both transactions write DISJOINT rows of `public.doctors`, and the reader's scan
/// saw NEITHER. The reader BEGINs first, so its snapshot predates the insert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_into_a_table_a_scanner_read_is_refused() {
    let cluster = Cluster::three_nodes();

    // The reader BEGINs first: its snapshot predates the insert.
    let reader_snapshot = cluster.committers[0]
        .begin_postgres_snapshot("public")
        .await
        .expect("reader BEGIN");

    // A DISJOINT writer inserts one row of the SAME table. It never scans, so it
    // carries no table-level read key.
    let inserter_snapshot = cluster.committers[1]
        .begin_postgres_snapshot("public")
        .await
        .expect("inserter BEGIN");
    let inserted = cluster.committers[1]
        .commit_postgres(
            "public",
            staged(vec![write("public", "doctor-c", "doctor-c=1")]),
            Vec::new(),
            inserter_snapshot,
        )
        .await
        .expect("inserter commit");
    assert_eq!(
        inserted,
        CommitOutcome::Committed,
        "the inserter reads nothing, so nothing it did can conflict"
    );

    // The reader COMMITs after the insert, having read the table at its earlier
    // snapshot. The row it never saw is a phantom its snapshot is stale against.
    let scanned = cluster.committers[0]
        .commit_postgres(
            "public",
            staged(vec![write("public", "doctor-d", "doctor-d=1")]),
            vec!["public.doctors".to_string()],
            reader_snapshot,
        )
        .await
        .expect("reader commit");
    assert!(
        matches!(scanned, CommitOutcome::Aborted { .. }),
        "a transaction that read a table a later INSERT wrote into must be refused as a \
         retryable serialization failure, not committed on the strength of a row it never \
         saw; got {scanned:?}"
    );
}
