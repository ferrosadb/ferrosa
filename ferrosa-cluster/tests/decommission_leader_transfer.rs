//! Sprint 4 W4.14: decommissioning the current leader transfers
//! leadership BEFORE LeaveNode applies, so writes do not fail during
//! the transition.
//!
//! This complements `membership_atomicity::remove_voter_clears_all_four_maps`
//! which deliberately avoided exercising leader-self removal (that
//! test pre-dated the leadership-transfer wiring).

mod common;

use std::time::Duration;

use uuid::Uuid;

use common::raft_harness::TestCluster;
use ferrosa_cluster::membership::{MembershipChanger, MembershipError};
use ferrosa_cluster::raft::uuid_to_node_id;

/// Build a `host_id` that hashes to `node_id` under `uuid_to_node_id`.
/// `uuid_to_node_id` reads `bytes[8..16]` little-endian; we construct
/// the inverse so the test does not need to read the harness's
/// internal NodeId↔Uuid map.
fn host_id_for(node_id: u64) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[8..16].copy_from_slice(&node_id.to_le_bytes());
    Uuid::from_bytes(bytes)
}

/// W4.14 RED → GREEN: decommissioning the leader runs the
/// leadership-transfer path inside `MembershipChanger::remove_voter`.
///
/// The contract:
/// - Pre-W4.14: returns `Err(MembershipError::TransferFirst)` and the
///   operator runs `ferrosa-ctl raft transfer-leader` manually.
/// - Post-W4.14: the changer dispatches `transfer_to`, awaits the new
///   leader, and surfaces `Err(MembershipError::NotLeader { Some(new) })`
///   so the caller can forward LeaveNode through
///   `Message::ClusterMembershipForward` (the wire-level forwarder
///   added in Sprint 1 W1.5/W1.13).
///
/// This test asserts the entire end-to-end:
///   1. Initiating the decommission moves leadership to a different
///      voter (post-condition observable via metrics on every node).
///   2. The follow-up forwarded LeaveNode (driven here by building a
///      second changer rooted at the new leader) cleanly removes the
///      old leader from the openraft voter set on every survivor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decommission_leader_transfers_first() {
    let cluster = TestCluster::with_voters(3).await;
    cluster.require_leader(Duration::from_secs(5)).await;

    let leader_node_id = cluster.leader_node().node_id;
    let leader_host_id = host_id_for(leader_node_id);
    assert_eq!(uuid_to_node_id(leader_host_id), leader_node_id);

    // Step 1: call remove_voter on the leader — exercises the new
    // auto-transfer path.  Pre-W4.14 returns TransferFirst; post-W4.14
    // returns NotLeader with the new leader's id.
    let leader_changer = MembershipChanger::new(
        cluster.leader_node().raft.clone(),
        cluster.membership_network(),
    );
    let result = leader_changer.remove_voter(leader_host_id).await;
    let new_leader_id = match result {
        Err(MembershipError::NotLeader {
            leader_node_id: Some(lid),
        }) => {
            assert_ne!(lid, leader_node_id, "leadership did transfer");
            lid
        }
        Err(MembershipError::TransferFirst) => {
            panic!("W4.14 regression: remove_voter on leader returned TransferFirst");
        }
        Ok(()) => {
            // Acceptable: the implementation completed the LeaveNode
            // forward internally.  Either is a valid W4.14 outcome.
            // Confirm via metrics that some other node is leader.
            let mut new_lid = None;
            for node in &cluster.nodes() {
                if node.node_id == leader_node_id {
                    continue;
                }
                if let Some(lid) = node.raft.metrics().borrow().current_leader {
                    new_lid = Some(lid);
                    break;
                }
            }
            new_lid.expect("a new leader exists after Ok(())")
        }
        Err(other) => panic!("unexpected remove_voter outcome: {other:?}"),
    };

    // Step 2: forward the LeaveNode through the new leader.  In
    // production this is what `ClusterMembershipForward` does at the
    // wire layer; in-process we do it directly by constructing a
    // changer rooted at the new leader's Raft.
    if !matches!(
        leader_changer.remove_voter(leader_host_id).await,
        Ok(()) // skip if the first call already finished
    ) {
        let new_leader_node = {
            let nodes = cluster.nodes();
            let mut found = None;
            for node in &nodes {
                if node.node_id == new_leader_id {
                    found = Some(node.raft.clone());
                    break;
                }
            }
            drop(nodes);
            found.expect("new leader present in harness")
        };
        let follow_up = MembershipChanger::new(new_leader_node, cluster.membership_network());
        // The new leader observes the leader-self transfer outcome:
        // its metrics.current_leader is itself, so calling
        // remove_voter on the *old* leader's host_id takes the normal
        // (non-leader-target) path inside the changer.
        follow_up
            .remove_voter(leader_host_id)
            .await
            .expect("forwarded LeaveNode succeeds against the new leader");
    }

    // Step 3: every survivor's openraft voter set has dropped the old
    // leader.
    let mut converged = false;
    for _ in 0..80 {
        let nodes = cluster.nodes();
        let mut ok = true;
        for node in &nodes {
            if node.node_id == leader_node_id {
                continue;
            }
            let metrics = node.metrics();
            let voter_ids: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
            if voter_ids.contains(&leader_node_id) {
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
        "remaining voters did not converge on leader removal within 4 s"
    );

    // Step 4: NodeRegistry on the harness no longer has the removed
    // node (map 3 cleaned).
    assert!(
        !cluster.is_registered(leader_node_id),
        "harness NodeRegistry should drop the removed leader's node_id"
    );

    cluster.shutdown().await;
}
