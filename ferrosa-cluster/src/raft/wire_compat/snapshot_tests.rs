//! A released build's persisted state-machine snapshot must restore under
//! every later build.
//!
//! 2026-11-06: 0.35.1 inserted `ClusterConfig::pair_quorum_strict` in the
//! middle of the struct with `#[serde(default)]`. Bincode is positional and
//! ignores `serde(default)`, so every 0.30.7 snapshot misread from that field
//! on ("tag for enum is not valid, found 44") and Raft refused to start on
//! every upgraded node. The fixtures here were written by the 0.30.7 and
//! 0.35.1 sources themselves; regenerating them defeats their purpose.

use super::*;

fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn restore(name: &str) -> FerrosStateMachine {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state-machine.snapshot.bin");
    std::fs::copy(fixture(name), &path).unwrap();
    let mut sm = FerrosStateMachine::with_snapshot_path(path);
    let restored = sm
        .recover_from_persisted_snapshot()
        .unwrap_or_else(|e| panic!("{name} no longer restores: {e}"));
    assert!(restored, "{name} must be loaded, not skipped");
    drop(dir);
    sm
}

fn assert_golden_state(sm: &FerrosStateMachine) {
    let state = sm.state();
    assert_eq!(state.config.cluster_name, "golden");
    assert_eq!(state.config.expected_cluster_size, 3);
    assert_eq!(state.config.raft_heartbeat_ms, 250);
    assert!(state.keyspaces.contains_key("golden_ks"));
}

#[test]
fn a_snapshot_written_by_0_30_7_restores() {
    assert_golden_state(&restore("state_machine_snapshot_v0_30_7.bin"));
}

#[test]
fn a_snapshot_written_by_0_35_1_restores() {
    assert_golden_state(&restore("state_machine_snapshot_v0_35_1.bin"));
}
