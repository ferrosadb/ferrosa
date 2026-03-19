//! Restore validation functions.

use crate::snapshot::metadata::SnapshotMetadata;

/// Validates that the snapshot's node_id matches the current node.
///
/// Returns Ok(()) if node IDs match, or if `force` is true.
/// Returns Err if node IDs differ and `force` is false.
pub fn validate_node_id(
    metadata: &SnapshotMetadata,
    current_node_id: &str,
    force: bool,
) -> ferrosa_common::Result<()> {
    if metadata.node_id != current_node_id && !force {
        return Err(ferrosa_common::Error::InvalidFormat(format!(
            "snapshot '{}' was created by node '{}' but current node is '{}'. \
             Use force=true to restore from a different node's snapshot",
            metadata.name, metadata.node_id, current_node_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::CommitLogPosition;
    use crate::snapshot::metadata::SnapshotMetadata;

    fn sample_metadata(node_id: &str) -> SnapshotMetadata {
        SnapshotMetadata {
            format_version: 1,
            name: "test-snap".to_string(),
            created_at: "2026-03-19T00:00:00Z".to_string(),
            expires_at: None,
            commit_log_position: CommitLogPosition {
                segment_id: 1,
                offset: 0,
            },
            node_id: node_id.to_string(),
            ephemeral: false,
            manifest_sha256: "abc".to_string(),
        }
    }

    #[test]
    fn same_node_id_passes() {
        let meta = sample_metadata("node-1");
        assert!(validate_node_id(&meta, "node-1", false).is_ok());
    }

    #[test]
    fn different_node_id_without_force_fails() {
        let meta = sample_metadata("node-1");
        let err = validate_node_id(&meta, "node-2", false).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("node-1"), "should mention snapshot node");
        assert!(msg.contains("node-2"), "should mention current node");
        assert!(msg.contains("force"), "should suggest force flag");
    }

    #[test]
    fn different_node_id_with_force_passes() {
        let meta = sample_metadata("node-1");
        assert!(validate_node_id(&meta, "node-2", true).is_ok());
    }
}
