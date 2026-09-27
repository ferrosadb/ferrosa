//! Contract tests for `TestCluster::pin_leadership`.
//!
//! A test that runs membership changes on "the leader" used to race a spontaneous
//! election: on a starved CPU a heartbeat is missed, a follower campaigns, and the
//! `MembershipChanger` the test built moments earlier now points at a node that is no
//! longer leader (`NotLeader { leader_node_id: Some(..) }`). The harness cannot make
//! the scheduler fair, so it removes the source of nondeterminism instead:
//! `pin_leadership` stops tick-driven elections on every node once all voters agree on
//! one leader, so leadership only changes when a test changes it.

mod common;

use std::time::Duration;

use common::raft_harness::TestCluster;

const FORMATION: Duration = Duration::from_secs(10);

/// The highest term any node reports.
fn max_term(cluster: &TestCluster) -> u64 {
    cluster
        .metrics_snapshot()
        .iter()
        .map(|m| m.current_term)
        .max()
        .expect("cluster has nodes")
}

/// Silence the leader so no follower hears a heartbeat.
fn silence_leader(cluster: &TestCluster) {
    cluster.leader_node().raft.runtime_config().heartbeat(false);
}

fn restore_leader(cluster: &TestCluster) {
    cluster.leader_node().raft.runtime_config().heartbeat(true);
}

/// Control: WITHOUT pinning, the same experiment does produce an election. This is
/// what makes the pinned test below meaningful (it can only pass if pinning is what
/// prevented the election). It waits for the condition with a generous deadline
/// instead of sleeping a fixed time, so a slow host cannot make it fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_pinning_a_silenced_leader_is_replaced() {
    let cluster = TestCluster::with_voters(3).await;
    let leader = cluster
        .wait_for_all_voters_leader(FORMATION)
        .await
        .expect("all voters agree on a leader");
    let term_before = max_term(&cluster);

    silence_leader(&cluster);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while max_term(&cluster) == term_before {
        assert!(
            tokio::time::Instant::now() < deadline,
            "control failed: no election in 30s after silencing leader {leader}; \
             the pinned test below would be vacuous"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.shutdown().await;
}

/// With leadership pinned, silencing the leader for many election timeouts changes
/// neither the leader nor the term on any node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_leadership_survives_a_silenced_leader() {
    let cluster = TestCluster::with_voters(3).await;
    let leader = cluster.pin_leadership(FORMATION).await;
    let term_before = max_term(&cluster);

    silence_leader(&cluster);
    // Ten election-timeout maxima (400 ms): an unpinned follower campaigns within one.
    tokio::time::sleep(Duration::from_secs(4)).await;
    restore_leader(&cluster);

    assert_eq!(
        max_term(&cluster),
        term_before,
        "a pinned cluster must not start an election"
    );
    for m in cluster.metrics_snapshot() {
        assert_eq!(
            m.current_leader,
            Some(leader),
            "node {} changed leader",
            m.id
        );
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pinned_leader_node_is_the_leader_itself() {
    let cluster = TestCluster::with_voters(3).await;
    let leader = cluster.pin_leadership(FORMATION).await;
    let node = cluster.leader_node();
    assert_eq!(node.node_id, leader);
    assert_eq!(
        node.metrics().current_leader,
        Some(node.node_id),
        "leader_node() must be a node that itself holds leadership"
    );
    cluster.shutdown().await;
}
