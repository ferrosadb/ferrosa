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

use std::collections::HashMap;
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

use crate::accord::apply::NoopStorageApplier;
use crate::accord::handlers::{AccordHandler, AccordState};
use crate::accord::state_machine::AccordStateMachine;
use crate::accord::transaction_commit::{AccordTransactionCommitter, ReplicaResolver};
use crate::accord::transport::AccordTransport;

/// How a replica answers requests.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Reachability {
    Live,
    /// `docker pause`: the request is accepted and never answered; the caller
    /// gives up at its RPC deadline.
    Paused,
    /// Connection refused or reset: the caller fails at once.
    Down,
}

/// The Data-lane RPC deadline a paused peer costs its caller.
const PAUSED_RPC_DEADLINE: Duration = Duration::from_secs(10);

struct ClusterTransport {
    handlers: HashMap<Uuid, Arc<AccordHandler>>,
    reachability: parking_lot::Mutex<HashMap<Uuid, Reachability>>,
    /// When set, the first PreAccept each coordinator sends waits here until
    /// every coordinator has registered the transaction locally, so their
    /// PreAccepts cross on the wire (node 1 sees Y then X, node 2 sees X then
    /// Y). Raced for real, this window is a few hundred microseconds wide.
    preaccept_crossing: parking_lot::Mutex<Option<Arc<tokio::sync::Barrier>>>,
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
            Reachability::Live => {}
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
}

impl Cluster {
    fn three_nodes() -> Self {
        let hosts = [host(0x1111), host(0x2222), host(0x3333)];
        let mut handlers = HashMap::new();
        let mut states: Vec<AccordState> = Vec::new();
        for h in hosts {
            let node_id = node_id_of(h);
            let state: AccordState =
                Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
                    node_id,
                    Arc::new(MockSyncWriter::new()),
                    Arc::new(NoopStorageApplier::new()),
                )));
            handlers.insert(h, Arc::new(AccordHandler::new(state.clone(), node_id)));
            states.push(state);
        }
        let transport = Arc::new(ClusterTransport {
            handlers,
            reachability: parking_lot::Mutex::new(
                hosts.iter().map(|h| (*h, Reachability::Live)).collect(),
            ),
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
            .map(|((h, state), node_transport)| {
                let node_id = node_id_of(*h);
                let replicas = replicas.clone();
                let resolve: ReplicaResolver =
                    Arc::new(move |_ks: &str, _key: &[u8]| Some(replicas.clone()));
                Arc::new(
                    AccordTransactionCommitter::new(
                        node_id,
                        Arc::new(HybridLogicalClock::new(node_id, 0)),
                        node_transport.clone(),
                        Arc::new(NoopStorageApplier::new()),
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
        }
    }

    /// Make the next PreAccept each node sends wait for the other, so the two
    /// transactions cross on the wire.
    fn arm_preaccept_crossing(&self) {
        *self.transport.preaccept_crossing.lock() = Some(Arc::new(tokio::sync::Barrier::new(2)));
        self.node_transports.iter().take(2).for_each(|node| {
            node.crossed
                .store(false, std::sync::atomic::Ordering::SeqCst)
        });
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
            .commit_postgres("ks", vec![write], vec!["t".to_string()], snapshot)
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
            .commit_postgres("ks", vec![write], vec!["t".to_string()], snapshot)
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
