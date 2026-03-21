//! Integration test: NetworkTopologyStrategy end-to-end.
//!
//! Verifies the full pipeline:
//! 1. Build a multi-DC token ring.
//! 2. Parse NTS replication params from CQL-style options.
//! 3. Select replicas with rack diversity.
//! 4. Verify LOCAL_QUORUM computes correct block_for against local DC RF.

use std::collections::{HashMap, HashSet};

use ferrosa_cluster::consistency::ConsistencyLevel;
use ferrosa_cluster::raft::{NodeInfo, NodeState};
use ferrosa_cluster::ring::strategy::ReplicationStrategy;
use ferrosa_cluster::ring::TokenRing;
use ferrosa_schema::metadata::keyspace::ReplicationParams;
use uuid::Uuid;

fn make_node(addr: &str, dc: &str, rack: &str) -> NodeInfo {
    NodeInfo {
        host_id: Uuid::new_v4(),
        addr: addr.to_string(),
        data_center: dc.to_string(),
        rack: rack.to_string(),
        state: NodeState::Normal,
    }
}

/// Build a realistic 6-node, 2-DC, 3-rack-per-DC ring.
fn build_multi_dc_ring() -> TokenRing {
    let mut ring = TokenRing::new();

    // DC1: us-east-1, 3 nodes across 3 racks
    ring.add_node(1, make_node("10.0.1.1:7000", "us-east-1", "us-east-1a"));
    ring.add_node(2, make_node("10.0.1.2:7000", "us-east-1", "us-east-1b"));
    ring.add_node(3, make_node("10.0.1.3:7000", "us-east-1", "us-east-1c"));

    // DC2: eu-west-1, 3 nodes across 3 racks
    ring.add_node(4, make_node("10.0.2.1:7000", "eu-west-1", "eu-west-1a"));
    ring.add_node(5, make_node("10.0.2.2:7000", "eu-west-1", "eu-west-1b"));
    ring.add_node(6, make_node("10.0.2.3:7000", "eu-west-1", "eu-west-1c"));

    // Interleave tokens across DCs (simulating vnode assignment)
    ring.assign_tokens(1, &[-3_000_000_000]);
    ring.assign_tokens(4, &[-2_000_000_000]);
    ring.assign_tokens(2, &[-1_000_000_000]);
    ring.assign_tokens(5, &[0]);
    ring.assign_tokens(3, &[1_000_000_000]);
    ring.assign_tokens(6, &[2_000_000_000]);

    ring
}

#[test]
fn end_to_end_nts_strategy_parsing() {
    // Parse NTS from CQL-style replication options
    let params = ReplicationParams {
        strategy: "NetworkTopologyStrategy".to_string(),
        options: HashMap::from([
            ("us-east-1".to_string(), "3".to_string()),
            ("eu-west-1".to_string(), "2".to_string()),
        ]),
    };
    let strategy = ReplicationStrategy::try_from(&params).unwrap();
    assert_eq!(strategy.replication_factor(), 5);
}

#[test]
fn end_to_end_nts_replica_selection() {
    let ring = build_multi_dc_ring();

    let dc_rf = HashMap::from([
        ("us-east-1".to_string(), 3usize),
        ("eu-west-1".to_string(), 2usize),
    ]);

    let replicas = ring.nts_replicas(0, &dc_rf);
    assert_eq!(replicas.len(), 5, "should have 5 total replicas");

    // Verify per-DC counts
    let us_east = replicas
        .iter()
        .filter(|&&id| ring.get_node(id).unwrap().data_center == "us-east-1")
        .count();
    let eu_west = replicas
        .iter()
        .filter(|&&id| ring.get_node(id).unwrap().data_center == "eu-west-1")
        .count();
    assert_eq!(us_east, 3);
    assert_eq!(eu_west, 2);

    // Verify all replicas are distinct
    let unique: HashSet<u64> = replicas.iter().copied().collect();
    assert_eq!(unique.len(), 5);

    // Verify rack diversity in us-east-1 (rf=3, 3 racks => all racks used)
    let us_east_racks: HashSet<&str> = replicas
        .iter()
        .filter(|&&id| ring.get_node(id).unwrap().data_center == "us-east-1")
        .map(|&id| ring.get_node(id).unwrap().rack.as_str())
        .collect();
    assert_eq!(
        us_east_racks.len(),
        3,
        "all 3 us-east-1 racks should be represented"
    );
}

#[test]
fn end_to_end_local_quorum_block_for() {
    // LOCAL_QUORUM with dc_rf=3 => block_for_dc(3) = 2
    let cl = ConsistencyLevel::LocalQuorum;
    assert_eq!(cl.block_for_dc(3), 2);
    assert_eq!(cl.block_for_dc(2), 2);
    assert_eq!(cl.block_for_dc(1), 1);
}

#[test]
fn end_to_end_each_quorum_block_for_all_dcs() {
    // EACH_QUORUM: each DC independently needs quorum
    let cl = ConsistencyLevel::EachQuorum;
    let dc_rf = HashMap::from([
        ("us-east-1".to_string(), 3usize),
        ("eu-west-1".to_string(), 2usize),
    ]);

    // Total required = sum of per-DC quorums
    let total_required: usize = dc_rf.values().map(|&rf| cl.block_for_dc(rf)).sum();
    // us-east-1: quorum(3) = 2, eu-west-1: quorum(2) = 2 => total 4
    assert_eq!(total_required, 4);
}

#[test]
fn end_to_end_nts_multiple_query_tokens() {
    // Verify that different query tokens still produce correct per-DC counts.
    let ring = build_multi_dc_ring();
    let dc_rf = HashMap::from([
        ("us-east-1".to_string(), 2usize),
        ("eu-west-1".to_string(), 1usize),
    ]);

    for token in [i64::MIN, -500, 0, 500, i64::MAX] {
        let replicas = ring.nts_replicas(token, &dc_rf);

        let us_east = replicas
            .iter()
            .filter(|&&id| ring.get_node(id).unwrap().data_center == "us-east-1")
            .count();
        let eu_west = replicas
            .iter()
            .filter(|&&id| ring.get_node(id).unwrap().data_center == "eu-west-1")
            .count();

        assert_eq!(
            us_east, 2,
            "token {token}: us-east-1 should have 2 replicas"
        );
        assert_eq!(eu_west, 1, "token {token}: eu-west-1 should have 1 replica");
    }
}

#[test]
fn end_to_end_replicas_for_strategy_dispatch() {
    let ring = build_multi_dc_ring();

    // Simple strategy uses existing replicas()
    let simple = ReplicationStrategy::Simple {
        replication_factor: 3,
    };
    let simple_replicas = ring.replicas_for_strategy(0, &simple);
    assert_eq!(simple_replicas.len(), 3);

    // NTS uses nts_replicas()
    let nts = ReplicationStrategy::NetworkTopology {
        dc_rf: HashMap::from([
            ("us-east-1".to_string(), 2usize),
            ("eu-west-1".to_string(), 1usize),
        ]),
    };
    let nts_replicas = ring.replicas_for_strategy(0, &nts);
    assert_eq!(nts_replicas.len(), 3);
}
