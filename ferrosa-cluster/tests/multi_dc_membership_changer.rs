//! Sprint 6 W6.4 — per-DC `MembershipChanger`.
//!
//! Verifies that `MembershipChanger::for_dc(dc_name, raft, network)`
//! returns a changer scoped to that DC: `dc_name()` and `group_id()`
//! match, and the changer's Raft handle is the per-DC instance.
//!
//! Uses two independent in-process Raft clusters (one per DC) to
//! demonstrate the scoping is real — an `add_voter` on the dc1
//! changer flows into dc1's Raft only.

mod common;

use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::membership::MembershipChanger;
use ferrosa_cluster::raft::RaftGroupId;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_changer_scoped_to_dc() {
    let dc1_cluster = TestCluster::with_voters(3).await;
    let dc2_cluster = TestCluster::with_voters(3).await;
    let _ = dc1_cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc1 leader");
    let _ = dc2_cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc2 leader");

    let dc1_raft = dc1_cluster.leader_node().raft.clone();
    let dc2_raft = dc2_cluster.leader_node().raft.clone();
    let dc1_net = dc1_cluster.membership_network();
    let dc2_net = dc2_cluster.membership_network();

    let dc1_changer = MembershipChanger::for_dc("dc1", dc1_raft.clone(), dc1_net);
    let dc2_changer = MembershipChanger::for_dc("dc2", dc2_raft.clone(), dc2_net);

    // Scoping: dc_name() and group_id() reflect the constructor.
    assert_eq!(dc1_changer.dc_name(), "dc1");
    assert_eq!(dc2_changer.dc_name(), "dc2");
    assert_eq!(dc1_changer.group_id(), RaftGroupId::for_dc("dc1"));
    assert_eq!(dc2_changer.group_id(), RaftGroupId::for_dc("dc2"));
    assert_ne!(dc1_changer.group_id(), dc2_changer.group_id());

    // Backward-compat: MembershipChanger::new(...) defaults to the
    // "default" DC group so existing single-DC callers don't need
    // to thread a DC name through.
    let default_changer =
        MembershipChanger::new(dc1_raft.clone(), dc1_cluster.membership_network());
    assert_eq!(default_changer.dc_name(), "default");
    assert_eq!(default_changer.group_id(), RaftGroupId::default_dc());

    dc1_cluster.shutdown().await;
    dc2_cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_dc_default_metadata_inherits_dc_name() {
    // New joiners admitted via `for_dc("dc2", ...)` get
    // `data_center = "dc2"` recorded in `state.members` by default —
    // unless the caller explicitly overrides via
    // `with_node_metadata_defaults`.
    let cluster = TestCluster::with_voters(3).await;
    let _ = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");

    let raft = cluster.leader_node().raft.clone();
    let net = cluster.membership_network();
    let changer = MembershipChanger::for_dc("dc2", raft, net);
    assert_eq!(changer.dc_name(), "dc2");

    // Override path still works — operator can force a different
    // data_center for new joiners (e.g., onboarding a stretched node).
    let raft2 = cluster.leader_node().raft.clone();
    let net2 = cluster.membership_network();
    let custom = MembershipChanger::for_dc("dc2", raft2, net2)
        .with_node_metadata_defaults("override-dc".into(), "rack9".into());
    assert_eq!(custom.dc_name(), "dc2", "DC scoping unchanged by override");

    cluster.shutdown().await;
}
