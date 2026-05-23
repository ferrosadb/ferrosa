//! Smoke test for the in-process multi-node Raft test harness.
//!
//! W1.0 (harness gate): a 3-voter cluster spun up via channels (no real
//! TCP) must elect a leader and commit a no-op `client_write` within a
//! few seconds.  All subsequent membership tests (W1.1-W1.6) build on
//! this primitive.

mod common;

use std::time::Duration;

use common::raft_harness::TestCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn harness_3_node_cluster_elects_leader_and_commits() {
    let cluster = TestCluster::with_voters(3).await;

    // Wait for leader election.
    let leader = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader should elect within 10 s");

    // The leader must be one of the three nodes.
    assert!(
        cluster.node_ids().contains(&leader),
        "leader {leader} not in node set {:?}",
        cluster.node_ids()
    );

    // Verify every node sees the same leader.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let all_agree = {
            let nodes = cluster.nodes();
            nodes
                .iter()
                .all(|node| node.raft.metrics().borrow().current_leader == Some(leader))
        };
        if all_agree {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let observed: Vec<_> = cluster
                .nodes()
                .iter()
                .map(|node| (node.node_id, node.raft.metrics().borrow().current_leader))
                .collect();
            panic!("nodes did not converge on leader {leader}: {observed:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Verify a client_write goes through and replicates.
    let resp = cluster
        .propose_on_leader(noop_command())
        .await
        .expect("client_write should succeed on the leader");
    assert!(
        matches!(resp, ferrosa_cluster::raft::RaftResponse::Ok),
        "expected RaftResponse::Ok, got {resp:?}",
    );

    cluster.shutdown().await;
}

fn noop_command() -> ferrosa_cluster::raft::RaftCommand {
    use ferrosa_cluster::raft::{RaftCommand, RaftOp};
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use std::collections::HashMap;
    let mut opts = HashMap::new();
    opts.insert("replication_factor".to_string(), "1".to_string());
    RaftCommand {
        op: RaftOp::CreateKeyspace(KeyspaceMetadata {
            name: format!("smoke_ks_{}", uuid::Uuid::new_v4().simple()),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: opts,
            },
        }),
        schema_version: uuid::Uuid::new_v4(),
    }
}
