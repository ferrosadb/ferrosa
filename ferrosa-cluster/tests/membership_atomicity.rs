//! Sprint 1 W1.1-W1.6: membership atomicity tests.
//!
//! Each test brings up an in-process 3-voter cluster via the
//! `common::raft_harness::TestCluster`, then exercises the
//! `MembershipChanger` API to assert that all four membership maps
//! agree.  Two of the four maps (`state.members`, openraft
//! `Membership`) are observable directly; the other two
//! (`network_factory.node_map`, `PeerManager.peers`) are emulated by
//! the harness's `NodeRegistry` and inspected via the test
//! `MembershipNetwork` impl.

mod common;

use std::time::Duration;

use uuid::Uuid;

use common::raft_harness::TestCluster;
use ferrosa_cluster::membership::{MembershipChanger, MembershipError};
use ferrosa_cluster::raft::uuid_to_node_id;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_voter_updates_all_four_maps() {
    // 3-voter cluster + a 4th node we'll add via add_voter.
    let cluster = TestCluster::with_voters(3).await;
    let _leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("3-voter cluster should elect a leader");

    // The 4th node — pre-create its dispatcher loop in the harness so
    // that when add_voter calls add_learner + change_membership the
    // leader can replicate to it.
    let new_host_id = Uuid::new_v4();
    let new_addr: std::net::SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let new_node_id = uuid_to_node_id(new_host_id);
    cluster.add_pending_node(new_node_id).await;

    // Build a MembershipChanger pointing at the leader's Raft +
    // harness network.
    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );

    changer
        .add_voter(new_host_id, new_addr)
        .await
        .expect("add_voter should succeed");

    // Assert all four maps now contain the new node, on every node.
    // openraft replicates eventually — give followers up to 2 s to apply.
    let mut all_caught_up = false;
    for _ in 0..40 {
        let nodes = cluster.nodes();
        let mut ok = true;
        for node in &nodes {
            let st = node.state_snapshot().await;
            if !st.members.contains_key(&new_node_id) {
                ok = false;
                break;
            }
            let metrics = node.metrics();
            let voter_ids: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
            if !voter_ids.contains(&new_node_id) {
                ok = false;
                break;
            }
        }
        drop(nodes);
        if ok {
            all_caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        all_caught_up,
        "followers did not converge on add_voter outcome within 2 s; \
         host_id={new_host_id} node_id={new_node_id}",
    );

    // Map 3 & 4 (collapsed in the harness): NodeRegistry has the new node.
    assert!(
        cluster.is_registered(new_node_id),
        "harness NodeRegistry missing the new node"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approve_node_replicates_to_followers() {
    // W1.6 — approve_node must propose RaftOp::ApproveNode so every
    // follower's state.approved_nodes reflects the approval.  Today's
    // controller-only cache is a regression footgun (auto_join=false
    // clusters split-brain on approvals).
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    let pending_host = Uuid::new_v4();

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer
        .approve_node(pending_host)
        .await
        .expect("approve_node");

    let mut converged = false;
    for _ in 0..40 {
        let nodes = cluster.nodes();
        let mut ok = true;
        for node in &nodes {
            let st = node.state_snapshot().await;
            if !st.approved_nodes.contains(&pending_host) {
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
        "approve_node did not replicate to every follower's approved_nodes",
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_metadata_propagates_addr() {
    // 3-voter cluster.  We update node 2's addr and expect every node's
    // state.members to reflect the new value.
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    // Pick a voter to refresh.  Use node 2 — we need its host_id.
    // The harness assigns NodeIds 1..=N but does not retain the original
    // host_id.  Recover it by inverting: in the harness, NodeId N is
    // synthesised, not derived from a host_id.  So inject a JoinNode
    // first that establishes a known (host, addr) pair that subsequent
    // updates can target.
    let target_host = Uuid::new_v4();
    let target_addr_v1: std::net::SocketAddr = "127.0.0.1:7100".parse().unwrap();
    let target_addr_v2: std::net::SocketAddr = "127.0.0.1:7200".parse().unwrap();
    let target_node_id = uuid_to_node_id(target_host);
    cluster.add_pending_node(target_node_id).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer
        .add_voter(target_host, target_addr_v1)
        .await
        .expect("seed add_voter");

    // Sanity: addr is v1 on every node.
    let mut converged = false;
    for _ in 0..40 {
        let nodes = cluster.nodes();
        let mut ok = true;
        for node in &nodes {
            let st = node.state_snapshot().await;
            match st.members.get(&target_node_id) {
                Some(info) if info.addr == target_addr_v1.to_string() => {}
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        drop(nodes);
        if ok {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(converged, "v1 addr did not propagate before update");

    // Now update.
    changer
        .update_metadata(target_host, Some(target_addr_v2), None)
        .await
        .expect("update_metadata");

    // Every node should converge on v2 within 2 s.
    let mut updated = false;
    for _ in 0..40 {
        let nodes = cluster.nodes();
        let mut ok = true;
        for node in &nodes {
            let st = node.state_snapshot().await;
            match st.members.get(&target_node_id) {
                Some(info) if info.addr == target_addr_v2.to_string() => {}
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        drop(nodes);
        if ok {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(updated, "addr update did not propagate to followers");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_voter_idempotent() {
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    let new_host_id = Uuid::new_v4();
    let new_addr: std::net::SocketAddr = "127.0.0.1:9998".parse().unwrap();
    let new_node_id = uuid_to_node_id(new_host_id);
    cluster.add_pending_node(new_node_id).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );

    // First add — succeeds.
    changer.add_voter(new_host_id, new_addr).await.unwrap();
    // Second add — should be a NoOp Ok, not an error.
    changer
        .add_voter(new_host_id, new_addr)
        .await
        .expect("second add_voter must be idempotent");

    // Voter set is exactly 4 — no duplicates.
    let metrics = cluster.leader_node().metrics();
    let voter_ids: std::collections::BTreeSet<u64> =
        metrics.membership_config.membership().voter_ids().collect();
    assert_eq!(
        voter_ids.len(),
        4,
        "voter set should be 4, got {voter_ids:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_voter_concurrent_serializes() {
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    // Two distinct candidates; we'll add them simultaneously.
    let h1 = Uuid::new_v4();
    let h2 = Uuid::new_v4();
    let n1 = uuid_to_node_id(h1);
    let n2 = uuid_to_node_id(h2);
    cluster.add_pending_node(n1).await;
    cluster.add_pending_node(n2).await;

    let changer1 = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    let changer2 = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );

    let addr1: std::net::SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let addr2: std::net::SocketAddr = "127.0.0.1:9002".parse().unwrap();

    let h1_clone = h1;
    let h2_clone = h2;
    let t1 = tokio::spawn(async move { changer1.add_voter(h1_clone, addr1).await });
    let t2 = tokio::spawn(async move { changer2.add_voter(h2_clone, addr2).await });

    let r1 = t1.await.unwrap();
    let r2 = t2.await.unwrap();
    assert!(r1.is_ok(), "concurrent add #1: {r1:?}");
    assert!(r2.is_ok(), "concurrent add #2: {r2:?}");

    let metrics = cluster.leader_node().metrics();
    let voter_ids: std::collections::BTreeSet<u64> =
        metrics.membership_config.membership().voter_ids().collect();
    assert!(voter_ids.contains(&n1), "voter set missing {n1}");
    assert!(voter_ids.contains(&n2), "voter set missing {n2}");
    assert_eq!(voter_ids.len(), 5, "expected 5 voters, got {voter_ids:?}");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_voter_clears_all_four_maps() {
    let cluster = TestCluster::with_voters(3).await;
    cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();

    // Add a 4th, then remove it.  We avoid testing leader-self removal
    // (that's the W1.4 caveat needing transfer_leader).
    let new_host_id = Uuid::new_v4();
    let new_addr: std::net::SocketAddr = "127.0.0.1:9100".parse().unwrap();
    let new_node_id = uuid_to_node_id(new_host_id);
    cluster.add_pending_node(new_node_id).await;

    let changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    changer.add_voter(new_host_id, new_addr).await.unwrap();

    // Sanity: it's there.
    assert!(cluster
        .leader_node()
        .state_snapshot()
        .await
        .members
        .contains_key(&new_node_id));

    // Now remove.  If it would target the leader, we expect TransferFirst.
    let leader_id = cluster.leader_node().node_id;
    let result = changer.remove_voter(new_host_id).await;
    if leader_id == new_node_id {
        assert!(
            matches!(result, Err(MembershipError::TransferFirst)),
            "leader-self decommission should return TransferFirst, got {result:?}",
        );
    } else {
        result.expect("remove_voter should succeed for non-leader target");
    }

    // Verify every map is clean on every node (eventually consistent).
    if leader_id != new_node_id {
        let mut converged = false;
        for _ in 0..40 {
            let nodes = cluster.nodes();
            let mut ok = true;
            for node in &nodes {
                if node.node_id == new_node_id {
                    continue;
                }
                let st = node.state_snapshot().await;
                if st.members.contains_key(&new_node_id) {
                    ok = false;
                    break;
                }
                let metrics = node.metrics();
                let voter_ids: Vec<u64> =
                    metrics.membership_config.membership().voter_ids().collect();
                if voter_ids.contains(&new_node_id) {
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
            "followers did not converge on remove_voter outcome within 2 s",
        );

        assert!(
            !cluster.is_registered(new_node_id),
            "harness NodeRegistry should no longer have removed node"
        );
    }

    cluster.shutdown().await;
}
