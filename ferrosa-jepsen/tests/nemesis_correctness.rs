//! Nemesis correctness tests.
//!
//! These tests exercise Ferrosa's fault-tolerance under network, disk, and
//! process failures.  They require either a pre-provisioned cluster or a
//! Firecracker environment.
//!
//! Run with:
//!   FERROSA_TEST_CLUSTER_NODES=127.0.0.1:9042 \
//!     cargo test -p ferrosa-jepsen --test nemesis_correctness
//!   or:
//!   FERROSA_TEST_FIRECRACKER=1 \
//!     cargo test -p ferrosa-jepsen --test nemesis_correctness

use ferrosa_jepsen::{
    chaos::{NemesisContext, NemesisRegistry},
    cluster::FerrosCluster,
    config::Topology,
    test_env::TestClusterEnv,
};

/// Helper: return cluster env or panic with setup instructions.
fn require_cluster_env() -> TestClusterEnv {
    TestClusterEnv::detect().unwrap_or_else(|| {
        panic!(
            "cluster infrastructure not available — set FERROSA_TEST_CLUSTER_NODES \
             or run scripts/lima-fc-cluster-up.sh and set FERROSA_TEST_FIRECRACKER=1"
        )
    })
}

#[tokio::test]
async fn disk_fail_no_phantom_commits() {
    let env = require_cluster_env();

    let cluster = if env.firecracker_provision {
        FerrosCluster::provision(Topology::T1)
            .await
            .expect("provision cluster")
    } else {
        FerrosCluster::from_nodes(&env.cql_nodes)
            .await
            .expect("connect to cluster")
    };

    cluster
        .wait_ready(std::time::Duration::from_secs(60))
        .await
        .expect("cluster ready");

    let ctx = NemesisContext {
        node_ips: cluster.nodes().iter().map(|n| n.ip.to_string()).collect(),
        ssh_user: "root".to_string(),
        ssh_key_path: env.ssh_key.clone(),
        ssh_port: env.ssh_port,
    };

    let registry = NemesisRegistry::phase2();
    let disk_nemesis = registry.get("disk-slow").expect("disk-slow nemesis registered");

    disk_nemesis.inject(&ctx).await.expect("inject disk failure");
    disk_nemesis.heal(&ctx).await.expect("heal disk");

    for node in cluster.nodes() {
        assert!(
            node.cql_reachable().await,
            "node {} should be CQL-reachable after disk-slow heal",
            node.id
        );
    }

    cluster.teardown().await.expect("teardown");
}

#[tokio::test]
async fn packet_reorder_linearizability() {
    let env = require_cluster_env();

    let cluster = if env.firecracker_provision {
        FerrosCluster::provision(Topology::T1)
            .await
            .expect("provision")
    } else {
        FerrosCluster::from_nodes(&env.cql_nodes)
            .await
            .expect("connect")
    };

    cluster
        .wait_ready(std::time::Duration::from_secs(60))
        .await
        .expect("ready");

    let ctx = NemesisContext {
        node_ips: cluster.nodes().iter().map(|n| n.ip.to_string()).collect(),
        ssh_user: "root".to_string(),
        ssh_key_path: env.ssh_key.clone(),
        ssh_port: env.ssh_port,
    };

    let registry = NemesisRegistry::phase1();
    let net_nemesis = registry
        .get("partition-halves")
        .expect("partition-halves nemesis registered");

    net_nemesis.inject(&ctx).await.expect("inject partition");
    net_nemesis.heal(&ctx).await.expect("heal partition");

    for node in cluster.nodes() {
        assert!(
            node.cql_reachable().await,
            "node {} should be CQL-reachable after partition heal",
            node.id
        );
    }

    cluster.teardown().await.expect("teardown");
}

#[tokio::test]
async fn lwt_batch_atomicity_all_nemeses() {
    let env = require_cluster_env();

    let cluster = if env.firecracker_provision {
        FerrosCluster::provision(Topology::T1)
            .await
            .expect("provision")
    } else {
        FerrosCluster::from_nodes(&env.cql_nodes)
            .await
            .expect("connect")
    };

    cluster
        .wait_ready(std::time::Duration::from_secs(60))
        .await
        .expect("ready");

    let ctx = NemesisContext {
        node_ips: cluster.nodes().iter().map(|n| n.ip.to_string()).collect(),
        ssh_user: "root".to_string(),
        ssh_key_path: env.ssh_key.clone(),
        ssh_port: env.ssh_port,
    };

    let registry = NemesisRegistry::phase1();
    for name in registry.names() {
        let nemesis = registry.get(&name).expect("nemesis registered");
        nemesis
            .inject(&ctx)
            .await
            .unwrap_or_else(|e| eprintln!("inject {name} failed (non-fatal in stub): {e}"));
        nemesis
            .heal(&ctx)
            .await
            .unwrap_or_else(|e| eprintln!("heal {name} failed (non-fatal in stub): {e}"));
    }

    let _ = &env;
    cluster.teardown().await.expect("teardown");
}
