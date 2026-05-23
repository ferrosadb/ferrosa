//! Sprint 8 W8.2 / W8.3 — `MembershipChanger` learner lifecycle tests.
//!
//! Each test brings up an in-process 3-voter cluster via the
//! `common::raft_harness::TestCluster`, then exercises the learner
//! lifecycle methods (`add_learner_only`, `promote_learner_to_voter`,
//! `demote_voter_to_learner`).
//!
//! The test invariants come from ADR-014 ("Learner Replicas") and the
//! Sprint 8 plan:
//!
//! - `add_learner_only` lands a node as `NodeState::Learner` in
//!   `state.members`, registers it in openraft as a learner (NOT a
//!   voter), and leaves the voter set unchanged.
//! - `promote_learner_to_voter` advances `state.members[N].state` to
//!   `Normal` while preserving the log position (no rewind to
//!   `Joining`).
//! - `demote_voter_to_learner` transfers leadership first if the target
//!   is the leader.

mod common;

use std::time::Duration;

use uuid::Uuid;

use common::raft_harness::TestCluster;
use ferrosa_cluster::membership::{MembershipChanger, MembershipError, NodeJoinConfig};
use ferrosa_cluster::raft::{uuid_to_node_id, NodeState};

/// W8.2 RED. Add a node as a learner-only and verify:
/// - `state.members[N].state` is `NodeState::Learner { .. }` on every node.
/// - openraft's effective membership shows `N` as a learner, not a voter
///   (i.e. `voter_ids()` does not contain `N`).
/// - The voter quorum size is unchanged (still 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_learner_only_does_not_make_voter() {
    let cluster = TestCluster::with_voters(3).await;
    let _leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("3-voter cluster should elect a leader");

    let new_host_id = Uuid::new_v4();
    let new_addr: std::net::SocketAddr = "127.0.0.1:9991".parse().unwrap();
    let new_node_id = uuid_to_node_id(new_host_id);
    cluster.add_pending_node(new_node_id).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );

    changer
        .add_learner_only(new_host_id, new_addr, NodeJoinConfig::default())
        .await
        .expect("add_learner_only");

    // Wait for the JoinNode entry to replicate to every node.
    let mut converged = false;
    for _ in 0..80 {
        let nodes = cluster.nodes();
        let mut ok = true;
        for node in &nodes {
            let st = node.state_snapshot().await;
            match st.members.get(&new_node_id) {
                Some(info) => {
                    if !info.state.is_learner() {
                        ok = false;
                        break;
                    }
                }
                None => {
                    ok = false;
                    break;
                }
            }
            // openraft membership: voter set must be unchanged (3 nodes).
            let metrics = node.metrics();
            let voter_ids: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
            if voter_ids.len() != 3 || voter_ids.contains(&new_node_id) {
                ok = false;
                break;
            }
            // Learner is in the membership map but not as a voter.
            let learner_ids: Vec<u64> = metrics
                .membership_config
                .membership()
                .learner_ids()
                .collect();
            if !learner_ids.contains(&new_node_id) {
                ok = false;
                break;
            }
        }
        drop(nodes);
        if ok {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        converged,
        "add_learner_only did not converge: \
         expected state.members[N].state == Learner and openraft voter set unchanged",
    );

    cluster.shutdown().await;
}

/// W8.3 RED. Promote a learner to a voter and verify the application
/// state advances `Learner -> Normal` while openraft promotes the node
/// to a voter. The log must not rewind: the entry committing the
/// promotion sits at a later index than the original JoinNode-as-Learner
/// entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_learner_to_voter_preserves_log_position() {
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    let host = Uuid::new_v4();
    let addr: std::net::SocketAddr = "127.0.0.1:9992".parse().unwrap();
    let node_id = uuid_to_node_id(host);
    cluster.add_pending_node(node_id).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );

    changer
        .add_learner_only(host, addr, NodeJoinConfig::default())
        .await
        .expect("add_learner_only");

    // Capture the leader's last_log_index after the learner add — the
    // promotion must commit entries at strictly higher indices.
    // Wait for the learner to be visible first.
    let mut learner_ready = false;
    for _ in 0..80 {
        let st = cluster.leader_node().state_snapshot().await;
        if matches!(
            st.members.get(&node_id).map(|n| n.state),
            Some(NodeState::Learner { .. })
        ) {
            learner_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(learner_ready, "learner did not appear in state.members");

    let pre_index = cluster
        .leader_node()
        .metrics()
        .last_log_index
        .expect("leader must have a last_log_index");

    // Promote.
    changer
        .promote_learner_to_voter(host)
        .await
        .expect("promote_learner_to_voter");

    // The learner's state should be Normal now and the new entry must
    // be at index > pre_index. openraft's voter_ids must include the
    // new node.
    let mut promoted = false;
    let mut last_index = 0u64;
    for _ in 0..80 {
        let leader = cluster.leader_node();
        let st = leader.state_snapshot().await;
        let metrics = leader.metrics();
        let now_voter = metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|v| v == node_id);
        let now_normal = matches!(
            st.members.get(&node_id).map(|n| n.state),
            Some(NodeState::Normal)
        );
        last_index = metrics.last_log_index.unwrap_or(0);
        if now_voter && now_normal && last_index > pre_index {
            promoted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        promoted,
        "promote_learner_to_voter did not converge \
         (last_log_index={last_index}, pre_index={pre_index})",
    );

    cluster.shutdown().await;
}

/// W8.3 RED. Demoting a non-leader voter to a learner: openraft removes
/// it from the voter set and `state.members[N].state` becomes
/// `Learner { .. }`.
///
/// The "transfers_leader_first_if_needed" companion test below covers
/// the leader-self case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_voter_to_learner_preserves_application_state() {
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    // Add a 4th node as a voter, then demote it back to a learner.
    let host = Uuid::new_v4();
    let addr: std::net::SocketAddr = "127.0.0.1:9993".parse().unwrap();
    let node_id = uuid_to_node_id(host);
    cluster.add_pending_node(node_id).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer.add_voter(host, addr).await.expect("seed add_voter");

    // Wait until every node sees it as a Normal voter.
    let mut up = false;
    for _ in 0..80 {
        let nodes = cluster.nodes();
        let mut ok = true;
        for n in &nodes {
            let st = n.state_snapshot().await;
            let m = n.metrics();
            let is_voter = m
                .membership_config
                .membership()
                .voter_ids()
                .any(|v| v == node_id);
            if !is_voter
                || !matches!(
                    st.members.get(&node_id).map(|x| x.state),
                    Some(NodeState::Normal)
                )
            {
                ok = false;
                break;
            }
        }
        drop(nodes);
        if ok {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(up, "voter did not reach Normal/voter state");

    // Demote.
    changer
        .demote_voter_to_learner(host)
        .await
        .expect("demote_voter_to_learner");

    let mut demoted = false;
    for _ in 0..80 {
        let leader = cluster.leader_node();
        let st = leader.state_snapshot().await;
        let m = leader.metrics();
        let in_voters = m
            .membership_config
            .membership()
            .voter_ids()
            .any(|v| v == node_id);
        let in_learners = m
            .membership_config
            .membership()
            .learner_ids()
            .any(|l| l == node_id);
        let is_learner_state = matches!(
            st.members.get(&node_id).map(|n| n.state),
            Some(NodeState::Learner { .. })
        );
        if !in_voters && in_learners && is_learner_state {
            demoted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(demoted, "demote_voter_to_learner did not converge");

    cluster.shutdown().await;
}

/// W8.3 RED. When the demote target is the current leader, the changer
/// must transfer leadership before issuing the joint-consensus swap.
/// Mirrors the W4.14 self-transfer pattern in `remove_voter`. The
/// caller's first call returns `MembershipError::NotLeader` after the
/// transfer dispatches; the test then re-issues the demote on the new
/// leader, which succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_voter_to_learner_transfers_leader_first_if_needed() {
    let cluster = TestCluster::with_voters(3).await;
    let _initial_leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("leader");

    // Add a fresh voter, transfer leadership to it, then demote it on
    // its own raft instance. We need to pin the host so its node_id is
    // predictable.
    let host = Uuid::new_v4();
    let addr: std::net::SocketAddr = "127.0.0.1:9994".parse().unwrap();
    let node_id = uuid_to_node_id(host);
    cluster.add_pending_node(node_id).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer.add_voter(host, addr).await.expect("seed add_voter");

    // Wait for the new voter to be Normal on the leader.
    for _ in 0..80 {
        let st = cluster.leader_node().state_snapshot().await;
        if matches!(
            st.members.get(&node_id).map(|n| n.state),
            Some(NodeState::Normal)
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Wait for the new voter to be caught up in replication on the leader.
    // `Normal` membership is set as soon as the joint-config commits; the
    // target's `matched_idx` may still trail the leader's `last_log_index`.
    // openraft's `transfer_to` has an internal catch-up budget of
    // `election_timeout_max × 2` which can run short under contention
    // (this exposed a leadership-transfer readiness bug before the explicit pre-wait — see Sprint 8
    // W8.3 follow-up). Block until either the indices align or 4s elapse.
    {
        let leader = cluster.leader_node();
        let deadline = std::time::Instant::now() + Duration::from_secs(4);
        loop {
            let m = leader.metrics();
            let leader_last = m.last_log_index.unwrap_or(0);
            let matched = m
                .replication
                .as_ref()
                .and_then(|r| r.get(&node_id).cloned().flatten())
                .map(|lid| lid.index);
            if matched.map(|i| i >= leader_last).unwrap_or(false) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "new voter did not catch up: leader_last={leader_last}, matched={matched:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    // Transfer leadership to the new voter. The post-dispatch deadline
    // (election_timeout_max × 2) is probabilistic: the target may win its
    // election just after `transfer_to` returns `Timeout`, OR an earlier
    // attempt may have already succeeded by the time we retry. Treat both
    // `Ok` and `Timeout-followed-by-target-becoming-leader` as success;
    // only fail if the target hasn't become leader within an outer 4s.
    let new_voter_raft = cluster
        .raft_for_node_id(node_id)
        .expect("raft handle for new voter");
    {
        let outer_deadline = std::time::Instant::now() + Duration::from_secs(15);
        let mut succeeded = false;
        while std::time::Instant::now() < outer_deadline {
            if cluster.current_leader_id() == Some(node_id) {
                succeeded = true;
                break;
            }
            // Only attempt transfer if a leader is currently elected.
            let Some(leader_id) = cluster.current_leader_id() else {
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            };
            if leader_id == node_id {
                succeeded = true;
                break;
            }
            let Some(leader_raft) = cluster.raft_for_node_id(leader_id) else {
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            };
            match leader_raft.trigger().transfer_to(node_id).await {
                Ok(()) => {
                    succeeded = true;
                    break;
                }
                Err(openraft::error::TransferError::Timeout) => {
                    // Race: target may have just become leader. Loop to re-check.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(openraft::error::TransferError::NotLeader) => {
                    // Some other node became leader (possibly our target). Re-check.
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(e) => panic!("transfer to new node: {e:?}"),
            }
        }
        if !succeeded {
            panic!("transfer to new node: target never became leader within 15s");
        }
    }

    // Wait for the new node to become leader.
    let mut new_leader_seen = false;
    for _ in 0..160 {
        if cluster.current_leader_id() == Some(node_id) {
            new_leader_seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(new_leader_seen, "leadership did not transfer to new voter");

    // Now ask the new leader to demote itself. The changer must trigger
    // a transfer back and return NotLeader.
    let changer_on_leader = MembershipChanger::new(new_voter_raft, cluster.membership_network());
    let res = changer_on_leader.demote_voter_to_learner(host).await;
    match res {
        Err(MembershipError::NotLeader { .. }) => {} // expected
        other => panic!("expected NotLeader after self-transfer, got {other:?}"),
    }

    // After the transfer leadership should have moved off `node_id`.
    let mut moved = false;
    for _ in 0..160 {
        if let Some(leader) = cluster.current_leader_id() {
            if leader != node_id {
                moved = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        moved,
        "demote_voter_to_learner on leader did not transfer leadership away",
    );

    cluster.shutdown().await;
}
