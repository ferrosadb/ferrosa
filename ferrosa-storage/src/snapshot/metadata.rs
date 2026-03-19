//! Snapshot metadata stored alongside each named snapshot in S3.
//!
//! [`SnapshotMetadata`] is written to `{prefix}/snapshots/{name}/metadata.json`
//! and describes the snapshot's identity, timing, and commit-log position.

use serde::{Deserialize, Serialize};

use crate::commitlog::CommitLogPosition;

/// Format version for the metadata file.
pub const METADATA_FORMAT_VERSION: u32 = 1;

/// Metadata document stored at `{prefix}/snapshots/{name}/metadata.json`.
///
/// Contains everything needed to locate the snapshot's manifest, validate
/// its integrity, and determine which commit-log segments it covers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotMetadata {
    /// Format version for forward compatibility.
    pub format_version: u32,
    /// Human-readable name, URL-safe characters only.
    pub name: String,
    /// ISO 8601 UTC timestamp when the snapshot was created.
    pub created_at: String,
    /// Optional ISO 8601 UTC expiry time. `None` means never expire.
    pub expires_at: Option<String>,
    /// Commit-log position at the time the snapshot was taken.
    ///
    /// All mutations before this position are captured in the manifest.
    pub commit_log_position: CommitLogPosition,
    /// Node ID of the node that created the snapshot.
    pub node_id: String,
    /// If true, this snapshot is ephemeral (e.g., created before compaction).
    pub ephemeral: bool,
    /// SHA-256 hex digest of `manifest.json` as stored in S3.
    pub manifest_sha256: String,
}

/// Validates that a snapshot name is safe for use as a path component.
///
/// Accepts ASCII alphanumeric characters, `-`, and `_`. Rejects empty names,
/// names longer than 128 characters, and names containing path-unsafe chars.
pub fn validate_snapshot_name(name: &str) -> ferrosa_common::Result<()> {
    if name.is_empty() {
        return Err(ferrosa_common::Error::InvalidFormat(
            "snapshot name must not be empty".to_string(),
        ));
    }
    if name.len() > 128 {
        return Err(ferrosa_common::Error::InvalidFormat(format!(
            "snapshot name exceeds 128 characters: {}",
            name.len()
        )));
    }
    for ch in name.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_' {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "snapshot name contains invalid character: '{ch}'"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_valid_names() {
        assert!(validate_snapshot_name("backup-2026-01-01").is_ok());
        assert!(validate_snapshot_name("my_snapshot").is_ok());
        assert!(validate_snapshot_name("abc123").is_ok());
        assert!(validate_snapshot_name("a").is_ok());
    }

    #[test]
    fn validate_rejects_empty_name() {
        assert!(validate_snapshot_name("").is_err());
    }

    #[test]
    fn validate_rejects_path_separator() {
        assert!(validate_snapshot_name("bad/name").is_err());
        assert!(validate_snapshot_name("bad\\name").is_err());
    }

    #[test]
    fn validate_rejects_too_long() {
        let name = "a".repeat(129);
        assert!(validate_snapshot_name(&name).is_err());
    }

    #[test]
    fn validate_accepts_max_length() {
        let name = "a".repeat(128);
        assert!(validate_snapshot_name(&name).is_ok());
    }

    #[test]
    fn metadata_round_trips_json() {
        let pos = CommitLogPosition {
            segment_id: 7,
            offset: 1024,
        };
        let meta = SnapshotMetadata {
            format_version: METADATA_FORMAT_VERSION,
            name: "test-snap".to_string(),
            created_at: "2026-03-18T00:00:00Z".to_string(),
            expires_at: None,
            commit_log_position: pos,
            node_id: "node-1".to_string(),
            ephemeral: false,
            manifest_sha256: "abc123".to_string(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let decoded: SnapshotMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, decoded);
    }
}
