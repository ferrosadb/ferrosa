//! Read-your-writes for forwarded DDL.
//!
//! When a non-leader node forwards a DDL to the leader, it must wait for its
//! OWN state machine to apply the committed entry before returning success to
//! the client — otherwise a client connected to that follower can run DDL → DML
//! in quick succession and have the DML rejected ("schema may still be
//! propagating") because the follower has the entry replicated but not yet
//! applied.
//!
//! The deterministic mechanism is `ddl_path::wait_for_local_apply`, which polls
//! the node's own `last_applied` index — observable locally, unlike a
//! follower's apply progress as seen from the leader.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::raft_harness::TestCluster;
use ferrosa_cluster::raft::{RaftCommand, RaftOp};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

fn create_ks_command(name: &str) -> RaftCommand {
    let mut opts = HashMap::new();
    opts.insert("replication_factor".to_string(), "1".to_string());
    RaftCommand {
        op: RaftOp::CreateKeyspace(KeyspaceMetadata {
            name: name.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: opts,
            },
        }),
        schema_version: uuid::Uuid::new_v4(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_local_apply_gives_follower_read_your_writes() {
    let cluster = TestCluster::with_voters(3).await;
    let leader_id = cluster
        .wait_for_all_voters_leader(Duration::from_secs(10))
        .await
        .expect("all voters should agree on a leader");

    // Commit a DDL on the leader. client_write returns after the LEADER applies,
    // so the leader's last_applied is now the committed index of this op.
    cluster
        .propose_on_leader(create_ks_command("ryw_ks"))
        .await
        .expect("client_write should succeed on the leader");

    let committed = {
        let nodes = cluster.nodes();
        let leader = nodes
            .iter()
            .find(|n| n.node_id == leader_id)
            .expect("leader node present");
        leader
            .raft
            .metrics()
            .borrow()
            .last_applied
            .expect("leader has applied the DDL")
            .index
    };

    let follower_id = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader_id)
        .expect("a follower exists in a 3-node cluster");

    // Clone the follower's raft handle out of the guard so we don't hold a
    // non-Send guard across the await.
    let follower_raft = {
        let nodes = cluster.nodes();
        let follower = nodes
            .iter()
            .find(|n| n.node_id == follower_id)
            .expect("follower node present");
        follower.raft.clone()
    };

    // The invariant under test: after wait_for_local_apply returns true, the
    // follower's own state machine has applied up to the committed DDL index —
    // so a read on the follower observes the schema change (read-your-writes).
    let caught_up = ferrosa_cluster::ddl_path::wait_for_local_apply(
        &follower_raft,
        committed,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        caught_up,
        "follower failed to apply committed index {committed} within the deadline"
    );

    let applied = follower_raft
        .metrics()
        .borrow()
        .last_applied
        .map(|l| l.index)
        .unwrap_or(0);
    assert!(
        applied >= committed,
        "follower last_applied {applied} < committed {committed}"
    );
}
