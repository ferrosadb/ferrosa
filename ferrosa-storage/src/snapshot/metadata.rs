//! Snapshot metadata: write-once JSON document stored at
//! `{prefix}/snapshots/{name}/metadata.json` in S3.

use serde::{Deserialize, Serialize};

use crate::commitlog::CommitLogPosition;

/// Metadata for a point-in-time snapshot.
///
/// Write-once: created during `create_snapshot()`, never modified.
/// The `manifest_sha256` field provides integrity verification —
/// on restore, the manifest is re-hashed and compared.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotMetadata {
    /// Schema version for forward compatibility.
    pub format_version: u32,
    /// Snapshot name (user-provided, validated: alphanumeric + hyphens + underscores).
    pub name: String,
    /// ISO 8601 timestamp when the snapshot was created.
    pub created_at: String,
    /// Optional ISO 8601 expiry timestamp. None = no expiry.
    pub expires_at: Option<String>,
    /// Commit log position at snapshot creation time.
    /// All mutations at or before this position are included.
    pub commit_log_position: CommitLogPosition,
    /// Node ID that created this snapshot.
    pub node_id: String,
    /// If true, this snapshot is ephemeral (auto-deleted on next startup).
    pub ephemeral: bool,
    /// SHA-256 hex digest of the manifest.json at snapshot time.
    /// Used for integrity verification during restore.
    pub manifest_sha256: String,
}

/// Current metadata format version.
pub const METADATA_FORMAT_VERSION: u32 = 1;

/// Validates a snapshot name: must be non-empty, max 128 chars,
/// only alphanumeric + hyphens + underscores.
pub fn validate_snapshot_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("snapshot name must not be empty".to_string());
    }
    if name.len() > 128 {
        return Err(format!(
            "snapshot name too long: {} chars (max 128)",
            name.len()
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err(format!(
            "snapshot name contains invalid characters: '{}' (only alphanumeric, hyphens, underscores allowed)",
            name
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::CommitLogPosition;

    fn sample_metadata() -> SnapshotMetadata {
        SnapshotMetadata {
            format_version: METADATA_FORMAT_VERSION,
            name: "daily-backup".to_string(),
            created_at: "2026-03-18T12:00:00Z".to_string(),
            expires_at: Some("2026-03-25T12:00:00Z".to_string()),
            commit_log_position: CommitLogPosition {
                segment_id: 42,
                offset: 1024,
            },
            node_id: "node-1".to_string(),
            ephemeral: false,
            manifest_sha256: "cafe0123".repeat(8),
        }
    }

    #[test]
    fn serde_round_trip() {
        let meta = sample_metadata();
        let json = serde_json::to_vec_pretty(&meta).unwrap();
        let deserialized: SnapshotMetadata = serde_json::from_slice(&json).unwrap();
        assert_eq!(meta, deserialized);
    }

    #[test]
    fn serde_contains_expected_fields() {
        let meta = sample_metadata();
        let json = serde_json::to_string_pretty(&meta).unwrap();
        assert!(json.contains("\"format_version\": 1"));
        assert!(json.contains("\"name\": \"daily-backup\""));
        assert!(json.contains("\"segment_id\": 42"));
        assert!(json.contains("\"offset\": 1024"));
        assert!(json.contains("\"ephemeral\": false"));
        assert!(json.contains("\"manifest_sha256\""));
    }

    #[test]
    fn serde_no_expires() {
        let mut meta = sample_metadata();
        meta.expires_at = None;
        let json = serde_json::to_vec(&meta).unwrap();
        let deserialized: SnapshotMetadata = serde_json::from_slice(&json).unwrap();
        assert_eq!(deserialized.expires_at, None);
    }

    #[test]
    fn validate_name_valid() {
        assert!(validate_snapshot_name("daily-backup").is_ok());
        assert!(validate_snapshot_name("snapshot_2026_03_18").is_ok());
        assert!(validate_snapshot_name("a").is_ok());
        assert!(validate_snapshot_name("ABC123").is_ok());
    }

    #[test]
    fn validate_name_empty() {
        let err = validate_snapshot_name("").unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn validate_name_too_long() {
        let long_name = "a".repeat(129);
        let err = validate_snapshot_name(&long_name).unwrap_err();
        assert!(err.contains("too long"));
    }

    #[test]
    fn validate_name_invalid_chars() {
        let err = validate_snapshot_name("snap/shot").unwrap_err();
        assert!(err.contains("invalid characters"));
        assert!(validate_snapshot_name("snap shot").is_err());
        assert!(validate_snapshot_name("snap.shot").is_err());
    }

    #[test]
    fn validate_name_max_length_ok() {
        let name = "a".repeat(128);
        assert!(validate_snapshot_name(&name).is_ok());
    }
}
