//! Sprint 6 W6.6 — `dc-partition` nemesis behaviour (per ADR-015 §S-48).
//!
//! Per ADR-015 the steady-state behaviour under a WAN partition is:
//!
//! 1. Each DC keeps LOCAL_QUORUM availability — the local Raft group
//!    has no peers in the other DC so a partition is invisible to it.
//! 2. Cross-DC writes (`QUORUM`, `EACH_QUORUM`, `ALL`, `SERIAL`)
//!    cleanly fail with `NotImplemented` — Sprint 7's Accord lifts
//!    the limitation.
//!
//! The Sprint 5 sim test runs the full nemesis on a Firecracker
//! cluster; this in-process variant uses two independent `TestCluster`s
//! to simulate the WAN partition (the channels never cross) and the
//! Sprint 6 CL-routing helper to confirm the cross-DC error path.

mod common;

use std::time::Duration;

use common::raft_harness::TestCluster;

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::coordinator::cl_routing::route_for_cl;
use ferrosa_cluster::error::ClusterError;
use ferrosa_cluster::raft::{RaftCommand, RaftOp};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

fn ddl_op(suffix: &str) -> RaftCommand {
    let mut opts = std::collections::HashMap::new();
    opts.insert("replication_factor".to_string(), "3".to_string());
    RaftCommand {
        op: RaftOp::CreateKeyspace(KeyspaceMetadata {
            name: format!("dc_partition_{}_{}", suffix, uuid::Uuid::new_v4().simple()),
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
async fn dc_partition_does_not_cause_global_outage() {
    // Two independent 3-voter Raft groups standing in for dc1 and
    // dc2. Because the harness uses per-cluster channels, there is
    // no inter-cluster traffic — exactly what a WAN partition looks
    // like to each side: invisible.
    let dc1 = TestCluster::with_voters(3).await;
    let dc2 = TestCluster::with_voters(3).await;

    let _ = dc1
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc1 leader during 'partition'");
    let _ = dc2
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("dc2 leader during 'partition'");

    // 1. LOCAL_QUORUM-equivalent writes succeed on each DC's leader.
    let dc1_resp = dc1
        .propose_on_leader(ddl_op("dc1"))
        .await
        .expect("dc1 LOCAL_QUORUM write committed under partition");
    assert!(
        matches!(dc1_resp, ferrosa_cluster::raft::RaftResponse::Ok),
        "dc1 write must commit despite WAN partition; got {dc1_resp:?}"
    );

    let dc2_resp = dc2
        .propose_on_leader(ddl_op("dc2"))
        .await
        .expect("dc2 LOCAL_QUORUM write committed under partition");
    assert!(
        matches!(dc2_resp, ferrosa_cluster::raft::RaftResponse::Ok),
        "dc2 write must commit despite WAN partition; got {dc2_resp:?}"
    );

    // 2. Cross-DC writes route through Accord in Sprint 7
    // (`CrossDcAccord`); cross-DC SERIAL (LWT) is deferred to
    // Sprint 8. The CL routing helper is the single chokepoint
    // that decides this.
    for cl in [
        ConsistencyLevel::Quorum,
        ConsistencyLevel::EachQuorum,
        ConsistencyLevel::All,
    ] {
        let r = route_for_cl(cl, 2);
        assert!(
            r.is_cross_dc_accord(),
            "CL {cl:?} cross-DC must take the CrossDcAccord route; got {r:?}"
        );
    }
    // SERIAL (cross-DC LWT) still NotImplemented in Sprint 7.
    let r = route_for_cl(ConsistencyLevel::Serial, 2).into_result();
    match r {
        Err(ClusterError::NotImplemented { feature }) => {
            assert!(
                feature.contains("SERIAL"),
                "SERIAL feature label expected in NotImplemented: {feature}"
            );
        }
        other => panic!("SERIAL cross-DC must remain NotImplemented in Sprint 7; got {other:?}"),
    }

    // 3. LOCAL CLs keep working in dual-DC mode (per ADR-015 §S-48).
    for cl in [
        ConsistencyLevel::LocalQuorum,
        ConsistencyLevel::LocalOne,
        ConsistencyLevel::LocalSerial,
    ] {
        assert!(
            route_for_cl(cl, 2).is_local_dc_only(),
            "{cl:?} must keep LOCAL availability under DC partition"
        );
    }

    dc1.shutdown().await;
    dc2.shutdown().await;
}
