//! Sprint 6 — multi-DC cluster formation routing tests.
//!
//! W6.3: cluster formation reads `self.config.data_center`, groups
//! peers by DC, and instantiates one Raft group per DC. Each DC's log
//! lives at `raft_data_dir/<dc-name>/`.
//!
//! These tests target the *routing decision* — that the local Raft
//! group only includes same-DC voters and that the log-dir layout is
//! per-DC. They do not exercise live cross-network formation; that is
//! covered by W6.9 (T3 topology bring-up).

use std::collections::HashMap;
use std::net::SocketAddr;

use ferrosa_cluster::controller::cluster::{partition_peers_by_dc, raft_log_dir_for_dc};
use uuid::Uuid;

#[test]
fn cluster_formation_groups_peers_by_dc_when_dc_metadata_present() {
    let dc1_a = Uuid::new_v4();
    let dc1_b = Uuid::new_v4();
    let dc1_c = Uuid::new_v4();
    let dc2_a = Uuid::new_v4();
    let dc2_b = Uuid::new_v4();
    let dc2_c = Uuid::new_v4();

    let addr = |port: u16| -> SocketAddr { format!("127.0.0.1:{port}").parse().unwrap() };
    let peers = vec![
        // local node is dc1_a; here we list the *other* peers.
        (dc1_b, addr(7001)),
        (dc1_c, addr(7002)),
        (dc2_a, addr(7003)),
        (dc2_b, addr(7004)),
        (dc2_c, addr(7005)),
    ];

    let mut dcs = HashMap::new();
    dcs.insert(dc1_a, "dc1".to_string());
    dcs.insert(dc1_b, "dc1".to_string());
    dcs.insert(dc1_c, "dc1".to_string());
    dcs.insert(dc2_a, "dc2".to_string());
    dcs.insert(dc2_b, "dc2".to_string());
    dcs.insert(dc2_c, "dc2".to_string());

    let (local_voters, others) = partition_peers_by_dc(&peers, &dcs, "dc1");

    // dc1 voters: the two other dc1 peers (dc1_b, dc1_c).
    assert_eq!(local_voters.len(), 2, "dc1 voters from the peer list");
    assert!(local_voters.iter().any(|(id, _)| *id == dc1_b));
    assert!(local_voters.iter().any(|(id, _)| *id == dc1_c));
    // dc2 peers excluded from the local Raft group.
    assert!(!local_voters.iter().any(|(id, _)| *id == dc2_a));

    // dc2 grouped separately for cross-DC routing (Sprint 7).
    let dc2 = others.get("dc2").expect("dc2 grouped");
    assert_eq!(dc2.len(), 3);
    assert!(dc2.iter().any(|(id, _)| *id == dc2_a));
    assert!(dc2.iter().any(|(id, _)| *id == dc2_b));
    assert!(dc2.iter().any(|(id, _)| *id == dc2_c));
}

#[test]
fn cluster_formation_defaults_unknown_peers_to_local_dc() {
    // Backward-compat: when no per-peer DC has been recorded, every
    // peer falls into the local-DC voter set. This is the single-DC
    // path that production has run against today.
    let p1 = Uuid::new_v4();
    let p2 = Uuid::new_v4();
    let addr = |port: u16| -> SocketAddr { format!("127.0.0.1:{port}").parse().unwrap() };
    let peers = vec![(p1, addr(7001)), (p2, addr(7002))];
    let dcs = HashMap::new();

    let (local_voters, others) = partition_peers_by_dc(&peers, &dcs, "datacenter1");

    assert_eq!(local_voters.len(), 2, "all peers default to local DC");
    assert!(others.is_empty(), "no cross-DC group when DCs unknown");
}

#[test]
fn raft_log_dir_for_dc_lays_out_per_dc_subdir() {
    let base = std::path::Path::new("/var/lib/ferrosa/raft");
    let dc1 = raft_log_dir_for_dc(base, "dc1");
    let dc2 = raft_log_dir_for_dc(base, "dc2");
    assert_eq!(dc1, std::path::Path::new("/var/lib/ferrosa/raft/dc1"));
    assert_eq!(dc2, std::path::Path::new("/var/lib/ferrosa/raft/dc2"));
    assert_ne!(dc1, dc2, "per-DC isolation");
}
