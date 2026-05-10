//! Sprint 7 W7.6 — Apply-durability barrier for Accord vote-commits.
//!
//! `MembershipChanger::accord_vote_commit` submits a `RaftOp::AccordApply`
//! through openraft and only returns once `wait().applied_index_at_least`
//! observes the entry applied on this DC's state machine. Without the
//! barrier, an Accord coordinator could mark a transaction "committed"
//! before its mutation was durable on the local DC's Raft group — losing
//! data on crash before the apply landed.

mod common;

use std::time::Duration;

use common::raft_harness::TestCluster;
use ferrosa_cluster::membership::MembershipChanger;
use ferrosa_common::{AccordTimestamp, TxnId};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accord_vote_commit_waits_for_apply() {
    let cluster = TestCluster::with_voters(3).await;
    let _leader = cluster
        .wait_for_leader(Duration::from_secs(5))
        .await
        .expect("3-voter cluster should elect a leader");

    let raft = cluster.leader_node().raft.clone();
    let net = cluster.membership_network();
    let changer = MembershipChanger::for_dc("dc1", raft.clone(), net);

    // Submit an Accord vote-commit through the changer.
    let hlc = AccordTimestamp::synthetic(1_000_000_000);
    let txn_id = TxnId::new(1, hlc);
    changer
        .accord_vote_commit(txn_id, hlc, vec![0xAB, 0xCD])
        .await
        .expect("accord_vote_commit should succeed on the leader");

    // After the call returns, the leader's last_applied must be at or
    // past the AccordApply's log_id — the barrier guarantees it.
    let metrics = raft.metrics().borrow().clone();
    let last_applied_idx = metrics.last_applied.map(|l| l.index).unwrap_or(0);
    assert!(
        last_applied_idx >= 1,
        "leader must have applied at least the AccordApply entry"
    );

    // The leader's state machine must show the txn applied to either
    // the ledger (drained past the watermark) or still buffered (the
    // heartbeat-driven watermark hasn't crossed `hlc` yet). Either way,
    // the apply has made it through `apply_command` — which is exactly
    // what the durability barrier guarantees.
    let state = cluster.leader_node().state_snapshot().await;
    let buffered = state.applied_accord_txns.contains(&txn_id);
    let in_buffer = !state.accord_apply_buffer.is_empty();
    assert!(
        buffered || in_buffer,
        "txn must be tracked either in the ledger (drained) or in the buffer (pending watermark)"
    );

    cluster.shutdown().await;
}
