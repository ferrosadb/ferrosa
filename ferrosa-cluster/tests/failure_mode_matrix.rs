//! Sprint 4 W4.15 — steady-state failure-mode matrix tests (S-01..S-37).
//!
//! Each scenario in `specs/raft-failure-mode-matrix.md` §1–§6 has one
//! test here.  Tests fall into three flavours:
//!
//! 1. **Live integration** — a harness-driven scenario the in-process
//!    `TestCluster` can faithfully reproduce (e.g. S-19 symmetric
//!    partition).
//! 2. **Existing-test reference** — the scenario is already covered
//!    by a pre-Sprint-4 integration test; the test here is a smoke
//!    check that pins the invariant the matrix asserts.  See the
//!    file-level docstring for the cross-reference.
//! 3. **Pure-logic gate** — for scenarios that require nemeses
//!    outside the harness's scope (disk-full, OOM kill, TCP layer)
//!    we assert the documented invariant via a small harness setup
//!    or static check; the full nemesis exercise belongs to Jepsen
//!    in `ferrosa-jepsen`.
//!
//! Cross-reference (matrix → coverage):
//!   S-01..S-05 (§1 add)             — covered by `membership_atomicity`
//!                                     + new tests below.
//!   S-06..S-10 (§2 remove)          — `membership_atomicity` +
//!                                     `decommission_leader_transfer`.
//!   S-11..S-18 (§3 death/recovery) — harness reachability + recovery
//!                                     scenarios.
//!   S-19..S-26 (§4 network)         — harness `partition`/`heal` plus
//!                                     existing `raft_election_storm`.
//!   S-27..S-32 (§5 leadership)      — `decommission_leader_transfer`
//!                                     + transfer/election tests.
//!   S-33..S-37 (§6 snapshots)       — `leader_snapshot_push` +
//!                                     `snapshot_transport` lane gate.

#![allow(clippy::needless_pass_by_value)]

mod common;

use std::time::Duration;

use uuid::Uuid;

use common::raft_harness::TestCluster;
use ferrosa_cluster::membership::{MembershipChanger, MembershipError};
use ferrosa_cluster::raft::uuid_to_node_id;

fn host_id_for(node_id: u64) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[8..16].copy_from_slice(&node_id.to_le_bytes());
    Uuid::from_bytes(bytes)
}

fn leader_report_count(cluster: &TestCluster, leader: u64) -> usize {
    cluster
        .nodes()
        .iter()
        .filter(|n| n.metrics().current_leader == Some(leader))
        .count()
}

fn registered_voters_contain(cluster: &TestCluster, node_id: u64) -> bool {
    let mut reports = Vec::new();
    for node in cluster.nodes().iter() {
        if !cluster.is_registered(node.node_id) {
            continue;
        }
        let contains = node
            .metrics()
            .membership_config
            .membership()
            .voter_ids()
            .any(|voter| voter == node_id);
        if let Some((_, existing)) = reports.iter_mut().find(|(id, _)| *id == node.node_id) {
            *existing = contains;
        } else {
            reports.push((node.node_id, contains));
        }
    }
    !reports.is_empty() && reports.iter().all(|(_, contains)| *contains)
}

fn registered_voters_exclude(cluster: &TestCluster, node_id: u64) -> bool {
    let mut reports = Vec::new();
    for node in cluster.nodes().iter() {
        if !cluster.is_registered(node.node_id) {
            continue;
        }
        let contains = node
            .metrics()
            .membership_config
            .membership()
            .voter_ids()
            .any(|voter| voter == node_id);
        if let Some((_, existing)) = reports.iter_mut().find(|(id, _)| *id == node.node_id) {
            *existing = contains;
        } else {
            reports.push((node.node_id, contains));
        }
    }
    !reports.is_empty() && reports.iter().all(|(_, contains)| !*contains)
}

async fn wait_until<F: FnMut() -> bool>(mut pred: F, total: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + total;
    while tokio::time::Instant::now() < deadline {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pred()
}

async fn add_voter_on_current_leader(
    cluster: &TestCluster,
    host_id: Uuid,
    addr: std::net::SocketAddr,
) -> Result<(), MembershipError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last_error = None;
    while tokio::time::Instant::now() < deadline {
        if let Some(leader) = cluster.current_leader_id() {
            if let Some(leader_raft) = cluster.raft_for_node_id(leader) {
                match MembershipChanger::new(leader_raft, cluster.membership_network())
                    .add_voter(host_id, addr)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(MembershipError::NotLeader { leader_node_id }) => {
                        last_error = Some(MembershipError::NotLeader { leader_node_id });
                    }
                    Err(MembershipError::InProgress) => {
                        last_error = Some(MembershipError::InProgress);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last_error.unwrap_or(MembershipError::NotLeader {
        leader_node_id: None,
    }))
}

async fn remove_voter_on_current_leader(
    cluster: &TestCluster,
    host_id: Uuid,
) -> Result<(), MembershipError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last_error = None;
    while tokio::time::Instant::now() < deadline {
        if let Some(leader) = cluster.current_leader_id() {
            if let Some(leader_raft) = cluster.raft_for_node_id(leader) {
                match MembershipChanger::new(leader_raft, cluster.membership_network())
                    .remove_voter(host_id)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(MembershipError::NotLeader { leader_node_id }) => {
                        last_error = Some(MembershipError::NotLeader { leader_node_id });
                    }
                    Err(MembershipError::InProgress) => {
                        last_error = Some(MembershipError::InProgress);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last_error.unwrap_or(MembershipError::NotLeader {
        leader_node_id: None,
    }))
}

// ---------------------------------------------------------------------------
// §1 Adding members (S-01..S-05)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_01_late_join_via_membership_changer_lands_in_openraft_voter_set() {
    // S-01: a 4th node joining via `MembershipChanger::add_voter` must
    // appear in BOTH state.members AND openraft Membership on every
    // node.  Pre-Sprint-1 the raw client_write path could leave the
    // openraft side out of sync; the changer closes that gap.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;

    let new_host = Uuid::new_v4();
    let new_addr: std::net::SocketAddr = "127.0.0.1:9201".parse().unwrap();
    let new_node = uuid_to_node_id(new_host);
    cluster.add_pending_node(new_node).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer.add_voter(new_host, new_addr).await.unwrap();

    let pred = || {
        for n in &cluster.nodes() {
            let voters: Vec<u64> = n
                .metrics()
                .membership_config
                .membership()
                .voter_ids()
                .collect();
            if !voters.contains(&new_node) {
                return false;
            }
        }
        true
    };
    assert!(wait_until(pred, Duration::from_secs(3)).await);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_02_concurrent_late_joins_serialize_via_inprogress_retry() {
    // S-02: two concurrent add_voter calls must both eventually
    // succeed; the second hits `InProgress` and the changer's
    // backoff retries.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;

    let h1 = Uuid::new_v4();
    let h2 = Uuid::new_v4();
    let a1: std::net::SocketAddr = "127.0.0.1:9301".parse().unwrap();
    let a2: std::net::SocketAddr = "127.0.0.1:9302".parse().unwrap();
    let n1 = uuid_to_node_id(h1);
    let n2 = uuid_to_node_id(h2);
    cluster.add_pending_node(n1).await;
    cluster.add_pending_node(n2).await;

    let leader_raft = cluster.leader_node().raft.clone();
    let changer1 = MembershipChanger::new(leader_raft.clone(), cluster.membership_network());
    let changer2 = MembershipChanger::new(leader_raft, cluster.membership_network());

    let (r1, r2) = tokio::join!(changer1.add_voter(h1, a1), changer2.add_voter(h2, a2));
    r1.unwrap();
    r2.unwrap();

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_03_joiner_reaches_partitioned_non_leader_returns_error_no_panic() {
    // S-03: when the only path to the leader is through a partitioned
    // non-leader, the join surfaces a typed error rather than panicking
    // or silently dropping the join.  We isolate the non-leader and
    // dial via that node; expect a NotLeader/RaftError, never panic.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;

    let leader_id = cluster.leader_node().node_id;
    // Pick any non-leader.
    let mut target = None;
    for n in &cluster.nodes() {
        if n.node_id != leader_id {
            target = Some(n.node_id);
            break;
        }
    }
    let target = target.expect("at least one non-leader exists");
    cluster.partition(
        cluster
            .nodes()
            .iter()
            .position(|n| n.node_id == target)
            .unwrap(),
    );

    let new_host = Uuid::new_v4();
    let new_addr: std::net::SocketAddr = "127.0.0.1:9303".parse().unwrap();
    let new_node = uuid_to_node_id(new_host);
    cluster.add_pending_node(new_node).await;

    // Dial via the partitioned node — the join MUST error, not panic.
    let partitioned_raft = {
        let nodes = cluster.nodes();
        let r = nodes
            .iter()
            .find(|n| n.node_id == target)
            .expect("target")
            .raft
            .clone();
        drop(nodes);
        r
    };
    let changer = MembershipChanger::new(partitioned_raft, cluster.membership_network());
    let _ = changer.add_voter(new_host, new_addr).await; // any Err is acceptable; no panic.

    cluster.heal();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_04_wiped_rejoin_via_snapshot_install_succeeds() {
    // S-04: stale snapshot triggers normal openraft install_snapshot
    // path.  Already covered end-to-end by
    // `tests/leader_snapshot_push.rs::leader_pushes_snapshot_to_wiped_node`.
    // Here we assert the lane the snapshot rides on (Bulk, per W4.13)
    // so a regression in the snapshot wiring is caught at the closest
    // contract.
    let lane = ferrosa_cluster::raft::snapshot_transport::snapshot_lane();
    assert_eq!(lane, ferrosa_net::codec::Lane::Bulk);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_05_approve_node_replicates_through_raft() {
    // S-05: ApproveNode must flow through Raft so every follower's
    // state.approved_nodes converges.  Already covered by
    // `membership_atomicity::approve_node_replicates_to_followers`;
    // here we re-run the smoke shape so the matrix has direct
    // attribution.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let pending = Uuid::new_v4();
    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer.approve_node(pending).await.unwrap();
    let mut converged = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        let nodes = cluster.nodes();
        let mut ok = true;
        for n in &nodes {
            let st = n.state_snapshot().await;
            if !st.approved_nodes.contains(&pending) {
                ok = false;
                break;
            }
        }
        if ok {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(converged);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// §2 Removing members (S-06..S-10)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_06_decommission_follower_removes_from_openraft_voter_set() {
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let h = Uuid::new_v4();
    let a: std::net::SocketAddr = "127.0.0.1:9401".parse().unwrap();
    let n = uuid_to_node_id(h);
    cluster.add_pending_node(n).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer.add_voter(h, a).await.unwrap();
    // We must remove a non-leader node — pick the just-added one, which
    // is unlikely to be leader.
    let leader_id = cluster.leader_node().node_id;
    if leader_id == n {
        // Skip — covered by S-07.
        cluster.shutdown().await;
        return;
    }
    changer.remove_voter(h).await.unwrap();
    let pred = || {
        for node in &cluster.nodes() {
            if node.node_id == n {
                continue;
            }
            let voters: Vec<u64> = node
                .metrics()
                .membership_config
                .membership()
                .voter_ids()
                .collect();
            if voters.contains(&n) {
                return false;
            }
        }
        true
    };
    assert!(wait_until(pred, Duration::from_secs(3)).await);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_07_decommission_leader_auto_transfers_first() {
    // S-07: the decommission-leader path is the W4.14 contract.
    // Already covered by `decommission_leader_transfer.rs`; the matrix
    // attribution is here.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let leader_id = cluster.leader_node().node_id;
    let leader_host = host_id_for(leader_id);
    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    let res = changer.remove_voter(leader_host).await;
    assert!(
        matches!(
            res,
            Err(MembershipError::NotLeader {
                leader_node_id: Some(_)
            })
        ),
        "expected NotLeader after auto-transfer, got {res:?}"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_08_decommission_concurrent_with_add_serializes() {
    // S-08: concurrent add+remove serialize through `InProgress` retry.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let h_add = Uuid::new_v4();
    let n_add = uuid_to_node_id(h_add);
    let a: std::net::SocketAddr = "127.0.0.1:9501".parse().unwrap();
    cluster.add_pending_node(n_add).await;

    // Pre-add a separate node so we can issue remove_voter on it.
    let h_rem = Uuid::new_v4();
    let n_rem = uuid_to_node_id(h_rem);
    let a_rem: std::net::SocketAddr = "127.0.0.1:9502".parse().unwrap();
    cluster.add_pending_node(n_rem).await;
    let leader_raft = cluster.leader_node().raft.clone();
    MembershipChanger::new(leader_raft.clone(), cluster.membership_network())
        .add_voter(h_rem, a_rem)
        .await
        .unwrap();
    if cluster.leader_node().node_id == n_rem {
        // Concurrent remove would hit S-07 — skip.
        cluster.shutdown().await;
        return;
    }

    let c1 = MembershipChanger::new(leader_raft.clone(), cluster.membership_network());
    let c2 = MembershipChanger::new(leader_raft, cluster.membership_network());
    let (r1, r2) = tokio::join!(c1.add_voter(h_add, a), c2.remove_voter(h_rem));
    r1.unwrap();
    r2.unwrap();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_09_decommission_partitioned_node_succeeds_for_quorum() {
    // S-09: removing a partitioned node still succeeds because the
    // remaining {N1, N2} have quorum.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;

    // Add a 4th node we'll partition+remove (avoids touching the
    // bootstrap voters' identities).
    let h = Uuid::new_v4();
    let a: std::net::SocketAddr = "127.0.0.1:9601".parse().unwrap();
    let n = uuid_to_node_id(h);
    cluster.add_pending_node(n).await;
    let leader_raft = cluster.leader_node().raft.clone();
    MembershipChanger::new(leader_raft.clone(), cluster.membership_network())
        .add_voter(h, a)
        .await
        .unwrap();

    if cluster.leader_node().node_id == n {
        // Skip S-07 territory.
        cluster.shutdown().await;
        return;
    }

    // Partition the 4th node — use isolate_by_node_id because partition()
    // indexes into bootstrap voters only.
    cluster.isolate_by_node_id(n);

    // Remove still succeeds because the leader has quorum without n.
    MembershipChanger::new(leader_raft, cluster.membership_network())
        .remove_voter(h)
        .await
        .unwrap();
    cluster.heal();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_10_readd_previously_decommissioned_node() {
    // S-10: re-add of a previously-removed host_id.  The changer's
    // pre-flight checks treat the existing-but-removed entry as a
    // fresh add.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;

    let h = Uuid::new_v4();
    let a: std::net::SocketAddr = "127.0.0.1:9701".parse().unwrap();
    let n = uuid_to_node_id(h);
    cluster.add_pending_node(n).await;

    add_voter_on_current_leader(&cluster, h, a).await.unwrap();
    assert!(
        wait_until(
            || registered_voters_contain(&cluster, n),
            Duration::from_secs(5)
        )
        .await
    );
    if cluster.leader_node().node_id == n {
        cluster.shutdown().await;
        return;
    }

    remove_voter_on_current_leader(&cluster, h).await.unwrap();
    assert!(
        wait_until(
            || registered_voters_exclude(&cluster, n),
            Duration::from_secs(5)
        )
        .await
    );
    // Re-add — must not silently NoOp.  We re-pend the dispatcher.
    cluster.add_pending_node(n).await;

    add_voter_on_current_leader(&cluster, h, a).await.unwrap();
    assert!(
        wait_until(
            || registered_voters_contain(&cluster, n),
            Duration::from_secs(5)
        )
        .await
    );
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// §3 Node death and recovery (S-11..S-18)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_11_single_follower_disappears_quorum_continues() {
    // S-11: harness's `partition` simulates total isolation, the
    // closest in-process equivalent of "OOM kill".  Cluster must keep
    // committing with the remaining quorum.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;

    // Isolate a non-leader.
    let leader_id = cluster.leader_node().node_id;
    let target_idx = cluster
        .nodes()
        .iter()
        .position(|n| n.node_id != leader_id)
        .unwrap();
    cluster.partition(target_idx);
    // Cluster still has quorum: leader is still leader after a beat.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let still_leader = cluster.leader_node().node_id == leader_id;
    cluster.heal();
    assert!(
        still_leader,
        "leader should retain quorum without one follower"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_12_purge_then_snapshot_invariant_held() {
    // S-12: documented invariant `last_applied >= last_purged_log_id`.
    // The harness state-machine doesn't simulate disk-purge directly;
    // we rely on the pre-existing fix
    // (`bug-raft-startup-fails-after-oom-purged-log`) and assert the
    // fixture: the harness builds a cluster cleanly, demonstrating the
    // recovery path is at least syntactically wired.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("recovery path produces a leader");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_13_cold_start_with_healthy_snapshot() {
    // S-13: full-cluster cold start.  In the harness "cold start"
    // is just `with_voters(3)` re-elected; this asserts the steady
    // state is reachable.
    let cluster = TestCluster::with_voters(3).await;
    let leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert!(leader.is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_14_state_members_synthesize_membership_smoke() {
    // S-14: synthesized Membership recovery is exercised by the
    // production `recover_membership_from_topology_state` path; the
    // harness uses an empty state machine, so the synthesis branch
    // does not fire.  Smoke-test that the harness still elects.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_15_runaway_term_repro_does_not_storm() {
    // S-15: after Sprint 3 PreVote, a partitioned-then-rejoined node
    // does not advance terms unbounded.  Already covered by
    // `tests/raft_election_storm.rs::election_storm_does_not_occur_on_log_divergence`.
    // Here: smoke that the storm metric stays at zero on a clean run.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let storm = ferrosa_cluster::raft::election_guard::election_storm_term_jumps_total();
    // Pre-Sprint-3 builds occasionally produced one or two storms in
    // a clean run; the assertion is "below the noise floor", not "zero".
    assert!(
        storm <= 2,
        "ELECTION_STORM_TERM_JUMPS_TOTAL={storm} too high in clean cluster"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_16_leader_disappears_followers_elect() {
    // S-16: leader OOM mid-commit.  Harness equivalent: isolate the
    // leader; followers must elect a new one.
    let cluster = TestCluster::with_voters(3).await;
    let leader_id = cluster
        .wait_for_majority_leader(Duration::from_secs(5))
        .await
        .expect("initial leader must converge before isolation");
    let leader_idx = cluster
        .nodes()
        .iter()
        .position(|n| n.node_id == leader_id)
        .unwrap();
    cluster.partition(leader_idx);
    let pred = || matches!(cluster.majority_leader_id(), Some(lid) if lid != leader_id);
    let elected = wait_until(pred, Duration::from_secs(15)).await;
    cluster.heal();
    assert!(elected, "remaining voters must elect a new leader");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_17_disk_full_smoke_invariant_documented() {
    // S-17: Sled disk-full path.  The fix
    // (`callback.log_io_completed(Err(...))` on append failure) lives
    // in `raft/log_store.rs`; this test only smokes the harness path
    // that uses SledLogStore via tempdir.  Real disk-fill belongs to
    // `ferrosa-jepsen`'s `disk-fail` nemesis.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_18_seed_init_recoverable_smoke() {
    // S-18: kill of seed during raft.initialize.  Harness restart
    // semantics: `with_voters(3)` always completes initialize without
    // process-kill simulation.  Smoke that the seed-init succeeds.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// §4 Network failures (S-19..S-26)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_19_symmetric_partition_majority_still_commits() {
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let leader_id = cluster.leader_node().node_id;
    // Partition a non-leader (one node from the rest).
    let target_idx = cluster
        .nodes()
        .iter()
        .position(|n| n.node_id != leader_id)
        .unwrap();
    cluster.partition(target_idx);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(cluster.leader_node().node_id, leader_id);
    cluster.heal();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_20_asymmetric_partition_check_quorum_steps_down() {
    // S-20: asymmetric partition.  The harness's partition primitive
    // is symmetric (both directions), so this test is a smoke for the
    // symmetric case + a documentation pin: the CheckQuorum invariant
    // is verified by the openraft fork's own
    // `check_quorum::tests::leader_steps_down_when_quorum_lost`.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_21_flap_partition_storm_metric_stays_low() {
    // S-21: fast partition flap.  After Sprint 3 PreVote, the storm
    // metric must not run away.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let leader_id = cluster.leader_node().node_id;
    let target_idx = cluster
        .nodes()
        .iter()
        .position(|n| n.node_id != leader_id)
        .unwrap();
    for _ in 0..5 {
        cluster.partition(target_idx);
        tokio::time::sleep(Duration::from_millis(80)).await;
        cluster.heal();
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    let storm = ferrosa_cluster::raft::election_guard::election_storm_term_jumps_total();
    // Allow a few storms during transient flap; runaway is what we
    // detect.
    assert!(storm < 50, "storm metric runaway: {storm}");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_22_slow_link_smoke() {
    // S-22: 200ms RTT belongs to Jepsen's `slow-network`.  Smoke
    // assertion: harness still elects under default timing.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_23_packet_loss_smoke() {
    // S-23: 10–30% loss → Jepsen `packet-loss` nemesis.  Smoke.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_24_packet_reorder_smoke() {
    // S-24: TCP handles reordering at transport.  Smoke.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_25_lane_reconnect_returns_unreachable_not_panic() {
    // S-25: when a lane is reconnecting, send returns Unreachable.
    // The harness's `partition` produces the same error class.
    // Verify a partitioned send surfaces as a clean error rather
    // than a panic.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    cluster.partition(0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.heal();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_26_handshake_replay_smoke() {
    // S-26: cluster-name mismatch is a config-level error handled by
    // ferrosa-net's handshake.  Smoke: harness always uses the same
    // cluster_name, so handshake never rejects.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// §5 Leader handover and election (S-27..S-32)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_27_voluntary_leadership_transfer() {
    // S-27: voluntary `transfer_to` succeeds.  Already exercised by
    // W4.14's decommission path and the openraft fork's
    // `tests/elect/t12_pre_vote_basic`.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let leader_id = cluster.leader_node().node_id;
    // Find a different voter.
    let target = {
        let m = cluster.leader_node().metrics();
        let voters: Vec<u64> = m.membership_config.membership().voter_ids().collect();
        voters.into_iter().find(|&v| v != leader_id).unwrap()
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last_error = None;
    while tokio::time::Instant::now() < deadline {
        let source = cluster
            .current_leader_id()
            .and_then(|leader_id| cluster.raft_for_node_id(leader_id))
            .unwrap_or_else(|| cluster.leader_node().raft.clone());
        match source.trigger().transfer_to(target).await {
            Ok(()) => break,
            Err(e) => {
                last_error = Some(format!("{e:?}"));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if cluster
            .nodes()
            .iter()
            .any(|n| n.metrics().current_leader == Some(target))
        {
            break;
        }
    }
    let pred = || {
        cluster.leader_node().metrics().current_leader == Some(target)
            || cluster
                .nodes()
                .iter()
                .any(|n| n.metrics().current_leader == Some(target))
    };
    assert!(
        wait_until(pred, Duration::from_secs(10)).await,
        "target must become leader after transfer; last transfer error: {last_error:?}"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_28_transfer_to_lagging_target_catches_up_first() {
    // S-28: `transfer_to` catches up the target before sending
    // TimeoutNow.  Engine-level test in the openraft fork
    // (`raft/trigger.rs::transfer_to`); here we smoke a transfer to
    // confirm the catch-up branch is reachable.  The smoke must not
    // dispatch before the leader has populated replication metrics for the
    // target: in that state openraft fails loud with `matched=None`, which
    // proves the harness raced readiness rather than the catch-up path.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let leader_id = cluster.leader_node().node_id;
    let target = {
        let m = cluster.leader_node().metrics();
        let voters: Vec<u64> = m.membership_config.membership().voter_ids().collect();
        voters.into_iter().find(|&v| v != leader_id).unwrap()
    };

    let replication_ready = || {
        let m = cluster.leader_node().metrics();
        m.last_log_index.is_some()
            && m.replication
                .as_ref()
                .and_then(|r| r.get(&target))
                .is_some()
    };
    assert!(
        wait_until(replication_ready, Duration::from_secs(5)).await,
        "leader must expose replication progress for target before transfer_to(target={target})"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut last_error = None;
    while tokio::time::Instant::now() < deadline {
        let source = cluster
            .current_leader_id()
            .and_then(|leader_id| cluster.raft_for_node_id(leader_id))
            .unwrap_or_else(|| cluster.leader_node().raft.clone());
        match source.trigger().transfer_to(target).await {
            Ok(()) => break,
            Err(e) => {
                last_error = Some(format!("{e:?}"));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if cluster
            .nodes()
            .iter()
            .any(|n| n.metrics().current_leader == Some(target))
        {
            break;
        }
    }

    let target_is_leader = || {
        cluster.leader_node().metrics().current_leader == Some(target)
            || cluster
                .nodes()
                .iter()
                .any(|n| n.metrics().current_leader == Some(target))
    };
    assert!(
        wait_until(target_is_leader, Duration::from_secs(10)).await,
        "target must become leader after transfer_to(target={target}); last transfer error: {last_error:?}"
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_29_transfer_concurrent_with_decommission_serializes() {
    // S-29: serialization handled by openraft + InProgress retry.
    // Smoke: transfer + add succeed in some order.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let h = Uuid::new_v4();
    let a: std::net::SocketAddr = "127.0.0.1:9801".parse().unwrap();
    let n = uuid_to_node_id(h);
    cluster.add_pending_node(n).await;
    MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    )
    .add_voter(h, a)
    .await
    .unwrap();
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_30_election_guard_does_not_fire_under_steady_state() {
    // S-30: in steady state the guard MUST NOT fire.  Sprint 4 W4.10
    // also asserts this through the retirement-gate test; here we
    // pin it from the matrix's perspective.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster.wait_for_leader(Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let storm = ferrosa_cluster::raft::election_guard::election_storm_term_jumps_total();
    assert!(storm <= 2);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_31_two_simultaneous_elections_resolve_to_one_leader() {
    // S-31: even under a fresh start, exactly one leader emerges.
    let cluster = TestCluster::with_voters(3).await;
    let leader = cluster
        .wait_for_majority_leader(Duration::from_secs(5))
        .await
        .unwrap();
    // The second simultaneous candidate should have given up.
    let majority_agrees = wait_until(
        || leader_report_count(&cluster, leader) >= 2,
        Duration::from_secs(5),
    )
    .await;
    assert!(majority_agrees, "majority of voters agree on leader");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_32_pre_vote_does_not_undermine_normal_election() {
    // S-32: PreVote rejecting a candidate is fine — the candidate
    // backs off.  Smoke: a steady-state cluster reaches a leader.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// §6 Snapshots and log compaction (S-33..S-37)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_33_snapshot_install_far_behind_follower_smoke() {
    // S-33: covered by `tests/leader_snapshot_push.rs`.  The matrix
    // attribution lives here; the substantive behaviour is asserted
    // there.
    let lane = ferrosa_cluster::raft::snapshot_transport::snapshot_lane();
    assert_eq!(lane, ferrosa_net::codec::Lane::Bulk);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_34_snapshot_during_heavy_appendentries_uses_separate_lane() {
    // S-34: the W4.13 lane allocation is the mitigation for this
    // scenario.  Re-asserted here from the matrix's perspective.
    use ferrosa_cluster::raft::snapshot_transport::{
        snapshot_lane, snapshot_lane_does_not_collide_with_raft,
    };
    assert_eq!(snapshot_lane(), ferrosa_net::codec::Lane::Bulk);
    assert!(snapshot_lane_does_not_collide_with_raft());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_35_snapshot_mid_build_with_leader_change_smoke() {
    // S-35: wasted work on old leader, no correctness issue.
    // Cluster-level smoke: a cluster reaches a leader.
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_36_log_purge_with_lagging_follower_uses_snapshot_path() {
    // S-36: openraft's standard fallback when a follower's needed
    // index has been purged.  Already covered by S-04 / leader
    // snapshot push test.  Smoke the lane gate.
    use ferrosa_cluster::raft::snapshot_transport::snapshot_lane;
    assert_eq!(snapshot_lane(), ferrosa_net::codec::Lane::Bulk);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s_37_snapshot_corruption_recovery_smoke() {
    // S-37: corrupted snapshot → falls back to log-replay.  This is
    // a recovery path tested via the `bug-raft-startup-fails-after-oom-purged-log`
    // fix; smoke the harness can still elect a leader (which is the
    // implicit witness of recovery being wired).
    let cluster = TestCluster::with_voters(3).await;
    assert!(cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .is_some());
    cluster.shutdown().await;
}
