//! Raft log entries carrying a `ClusterConfig` (`RaftOp::UpdateConfig`) must
//! decode across releases, and new entries must stay readable by the release
//! a node can roll back to. The existing golden gate
//! (`golden_raft_log_entries_decode`) never covered `UpdateConfig`, which is
//! how the 0.35.1 layout change passed CI. Fixtures were written by the 0.30.7
//! and 0.35.1 sources.

use super::*;
use openraft::{CommittedLeaderId, EntryPayload, LogId};

fn fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn decoded_config(name: &str) -> crate::config::ClusterConfig {
    let entry = SledLogStore::deserialize_entry(&fixture(name))
        .unwrap_or_else(|e| panic!("{name} no longer decodes: {e}"));
    match entry.payload {
        EntryPayload::Normal(cmd) => match cmd.op {
            crate::raft::RaftOp::UpdateConfig(config) => config,
            other => panic!("{name} decoded to the wrong op: {other:?}"),
        },
        other => panic!("{name} decoded to the wrong payload: {other:?}"),
    }
}

#[test]
fn an_update_config_entry_written_by_0_30_7_decodes() {
    let config = decoded_config("update_config_entry_v0_30_7.bin");
    assert_eq!(config.cluster_name, "golden");
    assert_eq!(config.raft_heartbeat_ms, 250);
}

#[test]
fn an_update_config_entry_written_by_0_35_1_decodes() {
    let config = decoded_config("update_config_entry_v0_35_1.bin");
    assert_eq!(config.cluster_name, "golden");
    assert_eq!(config.raft_heartbeat_ms, 250);
}

/// A node rolled back to 0.30.7 must still read what this build wrote, so
/// the writer emits the 0.30.7 bytes exactly.
#[test]
fn an_update_config_entry_is_written_in_the_layout_0_30_7_reads() {
    let config = crate::config::ClusterConfig {
        cluster_name: "golden".to_string(),
        num_tokens: 0,
        expected_cluster_size: 3,
        raft_heartbeat_ms: 250,
        ..crate::config::ClusterConfig::default()
    };
    let entry = openraft::Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 0), 1),
        payload: EntryPayload::Normal(crate::raft::RaftCommand {
            op: crate::raft::RaftOp::UpdateConfig(config),
            schema_version: uuid::Uuid::from_u128(0x0bad_cafe_0000_0000_0000_0000_0000_0001),
        }),
    };
    let written = SledLogStore::serialize_entry(&entry).unwrap();
    assert_eq!(
        written,
        fixture("update_config_entry_v0_30_7.bin"),
        "this build's UpdateConfig entry is not byte-identical to what 0.30.7 writes; \
         a rollback could not read it"
    );
}
