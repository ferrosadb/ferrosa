//! A CQL DDL whose Raft proposal cannot commit must fail **bounded**, loudly and
//! retryably — never hang until the client's own deadline fires.
//!
//! `ddl_path::execute_via_raft` awaits `raft.client_write` with no deadline.
//! Every other wait on that path is bounded (`DDL_AGREEMENT_TIMEOUT` = 5 s), but
//! openraft's `client_write` has no deadline of its own, and it does not notify
//! a pending proposal when the leader stops leading: `client_resp_channels` is
//! drained only when the entry is applied (openraft `core/raft_core.rs:802`) or
//! when a conflicting log truncates it (`:1960`). So once a standing leader
//! cannot reach a quorum — which a fault schedule guarantees while a node is
//! paused and stepping down — the proposal never returns and the only thing
//! that eventually fires is the CQL client's 30 s deadline. That is the
//! "drop strict-serializability workload table through CQL / Request execution
//! exceeded a client timeout of 30000ms" teardown failure behind the merge-queue
//! ejections of PR #551.
//!
//! # Instrument
//!
//! The in-process 3-voter harness (`tests/common/raft_harness.rs`) builds **real**
//! `FerrosRaft` instances inside a `#[tokio::test]` — so the note in
//! `ddl_path.rs` claiming a `FerrosRaft` cannot be built without a running tokio
//! runtime is wrong, and no dockerised fault schedule is needed to reproduce
//! this. `TestCluster::isolate_by_node_id` severs every RPC to and from the
//! leader inside `NodeRegistry::is_partitioned`, leaving the node still
//! believing it is the leader (the harness runs openraft's upstream default
//! `check_quorum_ratio = 0.0`, and even with CheckQuorum on, a stepped-down
//! leader still never notifies the pending proposal) while no proposal can
//! reach a quorum commit. Deterministic: no timing race, no load dependence.

mod common;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use common::raft_harness::TestCluster;
use ferrosa_cluster::ddl_path::execute_via_raft;
use ferrosa_cluster::pair::ddl::DdlOperation;
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

const KS: &str = "bounded_ddl_ks";

/// Outer window for the test itself. Must be comfortably longer than the bound
/// the DDL path is allowed to take (`DDL_AGREEMENT_TIMEOUT` = 5 s) so the test
/// measures the *product's* bound and not its own; a hang on the unfixed code
/// trips this and is reported as a hang, never as a fast failure.
const TEST_WINDOW: Duration = Duration::from_secs(25);

/// The product's own bound, plus generous scheduling margin for a loaded host.
const EXPECTED_BOUND: Duration = Duration::from_secs(15);

fn keyspace() -> KeyspaceMetadata {
    let mut options = HashMap::new();
    options.insert("replication_factor".to_string(), "3".to_string());
    KeyspaceMetadata {
        name: KS.to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options,
        },
    }
}

/// A DDL the client asked for and that was never confirmed must be reported as
/// a failure the caller can retry: never `Ok` (it was not committed), and never
/// a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ddl_proposal_on_a_leader_that_cannot_commit_fails_bounded_and_names_the_operation() {
    let cluster = TestCluster::with_voters(3).await;
    let leader_id = cluster
        .wait_for_all_voters_leader(Duration::from_secs(10))
        .await
        .expect("all voters should agree on a leader");
    let raft = cluster
        .raft_for_node_id(leader_id)
        .expect("leader raft handle present");

    // Sever every RPC to and from the leader. It keeps believing it is the
    // leader, but no proposal of its can ever reach a quorum.
    cluster.isolate_by_node_id(leader_id);
    let state = raft.metrics().borrow().state;
    assert_eq!(
        state,
        openraft::ServerState::Leader,
        "precondition: the isolated node must still consider itself the leader, \
         otherwise openraft would reject the proposal up front with ForwardToLeader \
         and this test would not exercise the unbounded await (state={state:?})"
    );

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        TEST_WINDOW,
        execute_via_raft(&raft, DdlOperation::CreateKeyspace(keyspace())),
    )
    .await;
    let elapsed = started.elapsed();

    let result = outcome.unwrap_or_else(|_elapsed| {
        panic!(
            "execute_via_raft did not return within {TEST_WINDOW:?} (waited {elapsed:?}) while the \
             leader could not commit: the proposal is awaited with no deadline, so nothing stops \
             it running until the CQL client's own 30 s deadline fires (#551)"
        )
    });

    let err = result.expect_err(
        "a DDL that was never committed must be reported as failed, never as done: \
         reporting Ok here means a DDL the client believes landed may not exist",
    );

    assert!(
        elapsed <= EXPECTED_BOUND,
        "the DDL proposal must give up on its own bound ({EXPECTED_BOUND:?}) rather than \
         waiting on the client's deadline; elapsed={elapsed:?}"
    );

    let message = err.to_string();
    assert!(
        message.contains(KS),
        "the failure must name the DDL that could not complete (expected it to mention \
         {KS:?}), got: {message}"
    );

    // The bound that expired is the existing agreement bound, and the class is a
    // retryable one: the DDL is unconfirmed, so the client must retry (possibly
    // on another node) rather than treat the statement as done.
    match &err {
        ferrosa_cluster::ClusterError::DdlProposalNotAgreed {
            operation,
            required,
            alive,
            timeout_ms,
            elapsed_ms,
        } => {
            assert!(
                operation.contains(KS),
                "the error must name the statement, got {operation:?}"
            );
            assert_eq!(
                *required, 2,
                "a 3-voter group needs 2 voters to agree, got {required}"
            );
            assert_eq!(
                *alive, 1,
                "only the local voter holds the unconfirmed proposal, got {alive}"
            );
            assert_eq!(
                *timeout_ms, 5000,
                "the bound must be the existing 5 s agreement bound, not a new one"
            );
            assert!(
                *elapsed_ms >= 5000,
                "the proposal must have been awaited for the whole bound, got {elapsed_ms}ms"
            );
        }
        other => panic!(
            "expected ClusterError::DdlProposalNotAgreed (retryable, unconfirmed DDL), got \
             {other:?}"
        ),
    }
}
