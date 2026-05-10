//! Sprint 7 W7.7 — cross-DC write at QUORUM goes through Accord.
//!
//! Two independent in-process Raft clusters simulate a 3+3 dual-DC
//! topology. We:
//!
//! 1. Build a `CrossDcAccordAdapter` per DC (one per `MembershipChanger`).
//! 2. Issue a vote-commit through each — the Sprint 7 path that
//!    replaces Sprint 6's `NotImplemented` stub.
//! 3. Assert the route helper returns `CrossDcAccord` (not
//!    `NotImplementedCrossDc`) and the cross-DC vote-commit metric
//!    counter ticks up by exactly 2 (one per DC).
//!
//! The metric increment is the trace required by the spec
//! ("verify via metrics or trace"). Sprint 8 layers full
//! pre-accept / recovery semantics on top of this scaffolding.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::accord::{cross_dc_vote_commit_count, CrossDcAccordAdapter};
use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::cl_routing::{route_for_cl, CLRoute};
use ferrosa_cluster::membership::MembershipChanger;
use ferrosa_common::{AccordTimestamp, TxnId};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_dc_write_uses_accord() {
    // 1. Routing chokepoint: QUORUM in multi-DC topology must hit
    //    the CrossDcAccord route (not NotImplemented).
    assert!(matches!(
        route_for_cl(ConsistencyLevel::Quorum, 2),
        CLRoute::CrossDcAccord
    ));

    // 2. Bring up two independent 3-voter clusters as our 3+3 dual-DC
    //    topology stand-in.
    let dc1 = TestCluster::with_voters(3).await;
    let dc2 = TestCluster::with_voters(3).await;
    dc1.wait_for_leader(Duration::from_secs(5))
        .await
        .expect("dc1 leader");
    dc2.wait_for_leader(Duration::from_secs(5))
        .await
        .expect("dc2 leader");

    let dc1_changer = Arc::new(MembershipChanger::for_dc(
        "dc1",
        dc1.leader_node().raft.clone(),
        dc1.membership_network(),
    ));
    let dc2_changer = Arc::new(MembershipChanger::for_dc(
        "dc2",
        dc2.leader_node().raft.clone(),
        dc2.membership_network(),
    ));
    let dc1_adapter = CrossDcAccordAdapter::new(dc1_changer);
    let dc2_adapter = CrossDcAccordAdapter::new(dc2_changer);
    assert_eq!(dc1_adapter.dc_name(), "dc1");
    assert_eq!(dc2_adapter.dc_name(), "dc2");

    // 3. Snapshot the metric, then dispatch one vote-commit per DC
    //    (the cross-DC write path: each DC commits its share via
    //    accord_vote_commit, gated by the apply-durability barrier
    //    from W7.6).
    let before = cross_dc_vote_commit_count();
    let hlc = AccordTimestamp::synthetic(1_000_000_000);
    let txn_id = TxnId::new(1, hlc);

    dc1_adapter
        .vote_commit_local(txn_id, hlc, vec![0xAA])
        .await
        .expect("dc1 vote-commit");
    dc2_adapter
        .vote_commit_local(txn_id, hlc, vec![0xAA])
        .await
        .expect("dc2 vote-commit");

    // 4. Trace assertion: counter incremented by exactly 2.
    let after = cross_dc_vote_commit_count();
    assert_eq!(
        after - before,
        2,
        "each DC must record one cross-DC vote-commit; before={before} after={after}"
    );

    dc1.shutdown().await;
    dc2.shutdown().await;
}
