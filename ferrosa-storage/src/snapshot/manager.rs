//! [`SnapshotManager`]: stateless lifecycle manager for named snapshots in S3.
//!
//! All snapshot state lives in object storage. The manager writes three objects
//! per snapshot:
//!
//! ```text
//! {prefix}/snapshots/{name}/metadata.json
//! {prefix}/snapshots/{name}/manifest.json
//! {prefix}/snapshots/{name}/schema.json
//! ```
//!
//! SSTables are never copied — the snapshot manifest references the same
//! object paths as the live manifest at the time of snapshotting.

use std::collections::HashSet;
use std::fmt::Write as FmtWrite;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use sha2::{Digest, Sha256};

use crate::commitlog::CommitLogPosition;
use crate::manifest::Manifest;
use crate::snapshot::metadata::{
    validate_snapshot_name, SnapshotMetadata, METADATA_FORMAT_VERSION,
};

// ── SnapshotManager ──────────────────────────────────────────────────────────

/// Stateless lifecycle manager for named PITR snapshots stored in S3.
///
/// All state is in object storage; the struct itself is `Clone`-friendly
/// (wraps an `Arc`). Multiple nodes can share the same prefix safely because
/// snapshot creation is idempotent-checked (GET before PUT).
pub struct SnapshotManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl SnapshotManager {
    /// Creates a new manager backed by `store` with the given key prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /// Creates a new named snapshot in S3.
    ///
    /// Validates the name, checks for duplicates, writes manifest + schema +
    /// metadata objects, and returns the completed [`SnapshotMetadata`].
    ///
    /// Returns `Err` if the name is invalid or a snapshot with that name
    /// already exists.
    // Each parameter maps directly to a distinct field in SnapshotMetadata.
    // Grouping them into a builder would add indirection without clarity.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_snapshot(
        &self,
        name: &str,
        manifest: &Manifest,
        schema_json: &[u8],
        position: CommitLogPosition,
        node_id: &str,
        expires_at: Option<String>,
        ephemeral: bool,
    ) -> ferrosa_common::Result<SnapshotMetadata> {
        // Precondition: name must be valid.
        validate_snapshot_name(name)?;

        // Precondition: snapshot must not already exist.
        let metadata_path = self.metadata_path(name);
        if self.object_exists(&metadata_path).await? {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "snapshot '{name}' already exists"
            )));
        }

        // Serialize manifest and compute SHA-256.
        let manifest_bytes = serde_json::to_vec_pretty(manifest).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to serialize manifest: {e}"))
        })?;
        let manifest_sha256 = hex_sha256(&manifest_bytes);

        // Write manifest.json.
        self.put_object(self.manifest_path(name), Bytes::from(manifest_bytes))
            .await?;

        // Write schema.json.
        self.put_object(self.schema_path(name), Bytes::copy_from_slice(schema_json))
            .await?;

        // Build and write metadata.json.
        let meta = SnapshotMetadata {
            format_version: METADATA_FORMAT_VERSION,
            name: name.to_string(),
            created_at: now_iso8601(),
            expires_at,
            commit_log_position: position,
            node_id: node_id.to_string(),
            ephemeral,
            manifest_sha256,
        };
        let meta_bytes = serde_json::to_vec_pretty(&meta).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to serialize metadata: {e}"))
        })?;
        self.put_object(metadata_path, Bytes::from(meta_bytes))
            .await?;

        Ok(meta)
    }

    /// Lists all snapshots stored under this prefix, sorted by `created_at`.
    pub async fn list_snapshots(&self) -> ferrosa_common::Result<Vec<SnapshotMetadata>> {
        let prefix_path = ObjectPath::from(format!("{}/snapshots/", self.prefix));

        // Use list_with_delimiter to get the "directories" one level deep.
        let list_result = self
            .store
            .list_with_delimiter(Some(&prefix_path))
            .await
            .map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!("failed to list snapshots: {e}"))
            })?;

        let mut snapshots = Vec::with_capacity(list_result.common_prefixes.len());

        for dir_path in list_result.common_prefixes {
            // Each common_prefix looks like "{prefix}/snapshots/{name}/".
            // Extract the snapshot name from the path.
            let name = extract_snapshot_name(&self.prefix, &dir_path);
            if let Some(name) = name {
                match self.load_snapshot_metadata(&name).await {
                    Ok(meta) => snapshots.push(meta),
                    Err(_) => {
                        // Skip snapshots whose metadata is unreadable (partial upload, etc.).
                    }
                }
            }
        }

        // Sort by created_at (lexicographic ISO 8601 is chronological).
        snapshots.sort_by(|a, b| a.created_at.cmp(&b.created_at));

        Ok(snapshots)
    }

    /// Deletes a snapshot's three metadata objects from S3.
    ///
    /// Does NOT delete any SSTables — snapshot manifests are references only.
    /// Returns `Err` if the snapshot does not exist.
    pub async fn delete_snapshot(&self, name: &str) -> ferrosa_common::Result<()> {
        validate_snapshot_name(name)?;

        // Precondition: snapshot must exist.
        let metadata_path = self.metadata_path(name);
        if !self.object_exists(&metadata_path).await? {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "snapshot '{name}' does not exist"
            )));
        }

        // Delete all three objects. Best-effort: ignore NotFound on manifest/schema
        // in case of a partial prior write.
        self.delete_object(metadata_path).await?;
        self.delete_object_ignore_not_found(self.manifest_path(name))
            .await?;
        self.delete_object_ignore_not_found(self.schema_path(name))
            .await?;

        Ok(())
    }

    /// Loads and deserializes the manifest for the named snapshot.
    pub async fn load_snapshot_manifest(&self, name: &str) -> ferrosa_common::Result<Manifest> {
        validate_snapshot_name(name)?;

        let path = self.manifest_path(name);
        let data = self.get_object(&path).await?;
        serde_json::from_slice(&data).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to parse snapshot manifest for '{name}': {e}"
            ))
        })
    }

    /// Collects all SSTable IDs referenced across every live snapshot.
    ///
    /// Used by compaction / GC to determine which SSTables must not be deleted.
    pub async fn all_referenced_sstable_ids(&self) -> ferrosa_common::Result<HashSet<String>> {
        let snapshots = self.list_snapshots().await?;
        let mut ids = HashSet::new();

        for meta in &snapshots {
            match self.load_snapshot_manifest(&meta.name).await {
                Ok(manifest) => {
                    for entries in manifest.sstables.values() {
                        for entry in entries {
                            ids.insert(entry.id.clone());
                        }
                    }
                }
                Err(_) => {
                    // Tolerate unreadable manifests (partial write, etc.).
                }
            }
        }

        Ok(ids)
    }

    /// Returns the minimum commit-log position across all live snapshots, or
    /// `None` if no snapshots exist.
    ///
    /// Used to determine the earliest point the commit log must be retained.
    pub async fn oldest_snapshot_position(
        &self,
    ) -> ferrosa_common::Result<Option<CommitLogPosition>> {
        let snapshots = self.list_snapshots().await?;
        let min = snapshots.iter().map(|m| m.commit_log_position).min(); // CommitLogPosition implements Ord
        Ok(min)
    }

    /// Returns segment IDs that are safe to delete from S3 archive.
    ///
    /// A segment is safe to delete only if its ID is:
    /// 1. Below the retention cutoff (based on age)
    /// 2. Below the oldest snapshot's commit log position segment_id
    ///
    /// This prevents deleting segments that a snapshot's PITR restore
    /// would need for replay.
    pub async fn segments_safe_to_delete(
        &self,
        candidate_segment_ids: &[u64],
        retention_cutoff_segment_id: u64,
    ) -> ferrosa_common::Result<Vec<u64>> {
        let oldest_snapshot_pos = self.oldest_snapshot_position().await?;

        let effective_cutoff = match oldest_snapshot_pos {
            Some(pos) => retention_cutoff_segment_id.min(pos.segment_id),
            None => retention_cutoff_segment_id,
        };

        let safe: Vec<u64> = candidate_segment_ids
            .iter()
            .filter(|&&id| id < effective_cutoff)
            .copied()
            .collect();

        Ok(safe)
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    async fn load_snapshot_metadata(&self, name: &str) -> ferrosa_common::Result<SnapshotMetadata> {
        let path = self.metadata_path(name);
        let data = self.get_object(&path).await?;
        serde_json::from_slice(&data).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to parse snapshot metadata for '{name}': {e}"
            ))
        })
    }

    async fn object_exists(&self, path: &ObjectPath) -> ferrosa_common::Result<bool> {
        match self.store.head(path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                "failed to check object existence: {e}"
            ))),
        }
    }

    async fn put_object(&self, path: ObjectPath, data: Bytes) -> ferrosa_common::Result<()> {
        self.store
            .put(&path, PutPayload::from(data))
            .await
            .map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to write object at {path}: {e}"
                ))
            })?;
        Ok(())
    }

    async fn get_object(&self, path: &ObjectPath) -> ferrosa_common::Result<Bytes> {
        let result = self.store.get(path).await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to read object at {path}: {e}"))
        })?;
        result.bytes().await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to read bytes from {path}: {e}"))
        })
    }

    async fn delete_object(&self, path: ObjectPath) -> ferrosa_common::Result<()> {
        self.store.delete(&path).await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to delete object at {path}: {e}"))
        })
    }

    async fn delete_object_ignore_not_found(&self, path: ObjectPath) -> ferrosa_common::Result<()> {
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                "failed to delete object at {path}: {e}"
            ))),
        }
    }

    fn metadata_path(&self, name: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/snapshots/{}/metadata.json", self.prefix, name))
    }

    fn manifest_path(&self, name: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/snapshots/{}/manifest.json", self.prefix, name))
    }

    fn schema_path(&self, name: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/snapshots/{}/schema.json", self.prefix, name))
    }
}

// ── Free helpers ─────────────────────────────────────────────────────────────

/// Returns the hex-encoded SHA-256 digest of `data`.
fn hex_sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Returns the current UTC time as an ISO 8601 string (e.g. `"2026-03-18T12:00:00Z"`).
fn now_iso8601() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (year, month, day, hour, min, sec) = secs_to_datetime(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Converts seconds since Unix epoch to (year, month, day, hour, min, sec).
fn secs_to_datetime(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let sec = secs % 60;
    let min = (secs / 60) % 60;
    let hour = (secs / 3600) % 24;
    let days = secs / 86400;

    let mut year = 1970u64;
    let mut remaining = days;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }

    let leap = is_leap(year);
    let month_days: [u64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];

    let mut month = 1u64;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }

    let day = remaining + 1;
    (year, month, day, hour, min, sec)
}

fn is_leap(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

/// Extracts the snapshot name from a "directory" path like
/// `"{prefix}/snapshots/{name}/"`.
fn extract_snapshot_name(prefix: &str, dir_path: &ObjectPath) -> Option<String> {
    let path_str = dir_path.as_ref();
    // Expected format: "{prefix}/snapshots/{name}/" or "{prefix}/snapshots/{name}"
    let base = format!("{prefix}/snapshots/");
    let stripped = path_str.strip_prefix(base.as_str())?;
    let name = stripped.trim_end_matches('/');
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::manifest::ManifestEntry;

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn make_manager(store: Arc<dyn ObjectStore>) -> SnapshotManager {
        SnapshotManager::new(store, "test")
    }

    fn sample_position() -> CommitLogPosition {
        CommitLogPosition {
            segment_id: 3,
            offset: 512,
        }
    }

    fn sample_manifest() -> Manifest {
        let mut m = Manifest::new();
        m.add_sstable(
            "ks.users",
            ManifestEntry {
                id: "sst-001".to_string(),
                size: 4096,
                min_token: -100,
                max_token: 100,
                min_timestamp: 1000,
                max_timestamp: 2000,
            },
        );
        m
    }

    fn sample_schema() -> Vec<u8> {
        br#"{"version":"00000000-0000-0000-0000-000000000001","keyspaces":{}}"#.to_vec()
    }

    // ── Test 1 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_snapshot_writes_three_objects() {
        let store = make_store();
        let mgr = make_manager(Arc::clone(&store));
        let manifest = sample_manifest();

        let meta = mgr
            .create_snapshot(
                "snap1",
                &manifest,
                &sample_schema(),
                sample_position(),
                "node-1",
                None,
                false,
            )
            .await
            .unwrap();

        // All three objects must exist.
        assert!(store
            .head(&ObjectPath::from("test/snapshots/snap1/metadata.json"))
            .await
            .is_ok());
        assert!(store
            .head(&ObjectPath::from("test/snapshots/snap1/manifest.json"))
            .await
            .is_ok());
        assert!(store
            .head(&ObjectPath::from("test/snapshots/snap1/schema.json"))
            .await
            .is_ok());

        // SHA-256 in metadata must match manifest bytes stored in S3.
        let manifest_bytes = store
            .get(&ObjectPath::from("test/snapshots/snap1/manifest.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let expected_sha = hex_sha256(&manifest_bytes);
        assert_eq!(meta.manifest_sha256, expected_sha);
    }

    // ── Test 2 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_snapshot_rejects_invalid_name() {
        let store = make_store();
        let mgr = make_manager(store);

        let err = mgr
            .create_snapshot(
                "bad/name",
                &sample_manifest(),
                &sample_schema(),
                sample_position(),
                "node-1",
                None,
                false,
            )
            .await;

        assert!(err.is_err(), "expected error for invalid name");
    }

    // ── Test 3 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn create_snapshot_rejects_duplicate_name() {
        let store = make_store();
        let mgr = make_manager(store);

        mgr.create_snapshot(
            "snap-dup",
            &sample_manifest(),
            &sample_schema(),
            sample_position(),
            "node-1",
            None,
            false,
        )
        .await
        .unwrap();

        let err = mgr
            .create_snapshot(
                "snap-dup",
                &sample_manifest(),
                &sample_schema(),
                sample_position(),
                "node-1",
                None,
                false,
            )
            .await;

        assert!(err.is_err(), "expected error for duplicate snapshot name");
    }

    // ── Test 4 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_snapshots_returns_created_snapshots() {
        let store = make_store();
        let mgr = make_manager(store);

        mgr.create_snapshot(
            "snap-a",
            &sample_manifest(),
            &sample_schema(),
            CommitLogPosition {
                segment_id: 1,
                offset: 0,
            },
            "node-1",
            None,
            false,
        )
        .await
        .unwrap();

        mgr.create_snapshot(
            "snap-b",
            &sample_manifest(),
            &sample_schema(),
            CommitLogPosition {
                segment_id: 2,
                offset: 0,
            },
            "node-1",
            None,
            false,
        )
        .await
        .unwrap();

        let snapshots = mgr.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 2);

        let names: Vec<&str> = snapshots.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"snap-a"), "snap-a should be listed");
        assert!(names.contains(&"snap-b"), "snap-b should be listed");
    }

    // ── Test 5 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn delete_snapshot_removes_metadata_but_not_sstables() {
        let store = make_store();
        let mgr = make_manager(Arc::clone(&store));

        mgr.create_snapshot(
            "snap-del",
            &sample_manifest(),
            &sample_schema(),
            sample_position(),
            "node-1",
            None,
            false,
        )
        .await
        .unwrap();

        mgr.delete_snapshot("snap-del").await.unwrap();

        // All three metadata objects must be gone.
        assert!(store
            .head(&ObjectPath::from("test/snapshots/snap-del/metadata.json"))
            .await
            .is_err());
        assert!(store
            .head(&ObjectPath::from("test/snapshots/snap-del/manifest.json"))
            .await
            .is_err());
        assert!(store
            .head(&ObjectPath::from("test/snapshots/snap-del/schema.json"))
            .await
            .is_err());
    }

    // ── Test 6 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn delete_nonexistent_snapshot_returns_error() {
        let store = make_store();
        let mgr = make_manager(store);

        let err = mgr.delete_snapshot("ghost").await;
        assert!(
            err.is_err(),
            "expected error when deleting non-existent snapshot"
        );
    }

    // ── Test 7 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn load_snapshot_manifest_returns_correct_data() {
        let store = make_store();
        let mgr = make_manager(store);
        let manifest = sample_manifest();

        mgr.create_snapshot(
            "snap-mf",
            &manifest,
            &sample_schema(),
            sample_position(),
            "node-1",
            None,
            false,
        )
        .await
        .unwrap();

        let loaded = mgr.load_snapshot_manifest("snap-mf").await.unwrap();

        // Verify the loaded manifest contains the same SSTable entries.
        let entries = loaded
            .sstables
            .get("ks.users")
            .expect("ks.users should be present");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "sst-001");
    }

    // ── Test 8 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn all_referenced_sstable_ids_across_snapshots() {
        let store = make_store();
        let mgr = make_manager(store);

        // Snapshot A: sst-001 and sst-002.
        let mut manifest_a = Manifest::new();
        manifest_a.add_sstable(
            "ks.t",
            ManifestEntry {
                id: "sst-001".to_string(),
                size: 1,
                min_token: 0,
                max_token: 0,
                min_timestamp: 0,
                max_timestamp: 0,
            },
        );
        manifest_a.add_sstable(
            "ks.t",
            ManifestEntry {
                id: "sst-002".to_string(),
                size: 1,
                min_token: 0,
                max_token: 0,
                min_timestamp: 0,
                max_timestamp: 0,
            },
        );

        // Snapshot B: sst-002 and sst-003 (overlapping on sst-002).
        let mut manifest_b = Manifest::new();
        manifest_b.add_sstable(
            "ks.t",
            ManifestEntry {
                id: "sst-002".to_string(),
                size: 1,
                min_token: 0,
                max_token: 0,
                min_timestamp: 0,
                max_timestamp: 0,
            },
        );
        manifest_b.add_sstable(
            "ks.t",
            ManifestEntry {
                id: "sst-003".to_string(),
                size: 1,
                min_token: 0,
                max_token: 0,
                min_timestamp: 0,
                max_timestamp: 0,
            },
        );

        mgr.create_snapshot(
            "snap-x",
            &manifest_a,
            &sample_schema(),
            CommitLogPosition {
                segment_id: 1,
                offset: 0,
            },
            "node-1",
            None,
            false,
        )
        .await
        .unwrap();
        mgr.create_snapshot(
            "snap-y",
            &manifest_b,
            &sample_schema(),
            CommitLogPosition {
                segment_id: 2,
                offset: 0,
            },
            "node-1",
            None,
            false,
        )
        .await
        .unwrap();

        let ids = mgr.all_referenced_sstable_ids().await.unwrap();

        assert_eq!(ids.len(), 3, "union should contain exactly 3 unique IDs");
        assert!(ids.contains("sst-001"));
        assert!(ids.contains("sst-002"));
        assert!(ids.contains("sst-003"));
    }

    // ── Test 9 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn snapshot_captures_consistent_manifest_state() {
        // Verify that a snapshot captures a self-consistent manifest.
        // Even if the live manifest is updated between our read and the
        // snapshot write, the snapshot's copy is internally consistent.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "node1";
        let mgr = SnapshotManager::new(Arc::clone(&store), prefix.to_string());

        // Create a manifest with 2 SSTables
        let mut manifest = Manifest::new();
        manifest.add_sstable(
            "ks.t",
            ManifestEntry {
                id: "sst1".into(),
                size: 100,
                min_token: 0,
                max_token: 50,
                min_timestamp: 1000,
                max_timestamp: 2000,
            },
        );
        manifest.add_sstable(
            "ks.t",
            ManifestEntry {
                id: "sst2".into(),
                size: 200,
                min_token: 51,
                max_token: 100,
                min_timestamp: 2000,
                max_timestamp: 3000,
            },
        );

        let pos = CommitLogPosition {
            segment_id: 5,
            offset: 512,
        };
        mgr.create_snapshot("pre-compaction", &manifest, b"{}", pos, "n1", None, false)
            .await
            .unwrap();

        // Simulate compaction: replace sst1+sst2 with sst3
        let mut compacted = Manifest::new();
        compacted.add_sstable(
            "ks.t",
            ManifestEntry {
                id: "sst3".into(),
                size: 250,
                min_token: 0,
                max_token: 100,
                min_timestamp: 1000,
                max_timestamp: 3000,
            },
        );

        let pos2 = CommitLogPosition {
            segment_id: 6,
            offset: 0,
        };
        mgr.create_snapshot(
            "post-compaction",
            &compacted,
            b"{}",
            pos2,
            "n1",
            None,
            false,
        )
        .await
        .unwrap();

        // Pre-compaction snapshot references sst1, sst2
        let pre = mgr.load_snapshot_manifest("pre-compaction").await.unwrap();
        let pre_ids: Vec<&str> = pre.sstables["ks.t"].iter().map(|e| e.id.as_str()).collect();
        assert!(pre_ids.contains(&"sst1"));
        assert!(pre_ids.contains(&"sst2"));
        assert!(!pre_ids.contains(&"sst3"));

        // Post-compaction snapshot references sst3
        let post = mgr.load_snapshot_manifest("post-compaction").await.unwrap();
        let post_ids: Vec<&str> = post.sstables["ks.t"]
            .iter()
            .map(|e| e.id.as_str())
            .collect();
        assert!(post_ids.contains(&"sst3"));
        assert!(!post_ids.contains(&"sst1"));

        // Both snapshots are self-consistent
        assert_eq!(pre.sstables["ks.t"].len(), 2);
        assert_eq!(post.sstables["ks.t"].len(), 1);
    }

    // ── Test 10 ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn archive_retention_respects_snapshot_position() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "node1";
        let mgr = SnapshotManager::new(Arc::clone(&store), prefix.to_string());

        // Create snapshot at segment 10
        let manifest = Manifest::new();
        let pos = CommitLogPosition {
            segment_id: 10,
            offset: 0,
        };
        mgr.create_snapshot("snap1", &manifest, b"{}", pos, "n1", None, false)
            .await
            .unwrap();

        // Retention says delete segments below 15
        // But snapshot needs segments >= 10 for PITR replay
        // So effective cutoff = min(15, 10) = 10
        let safe = mgr
            .segments_safe_to_delete(&[5, 8, 10, 12, 14], 15)
            .await
            .unwrap();

        // Only segments strictly below 10 are safe
        assert_eq!(safe, vec![5, 8]);
    }

    // ── Test 11 ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn archive_retention_no_snapshots_uses_retention_cutoff() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "node1";
        let mgr = SnapshotManager::new(Arc::clone(&store), prefix.to_string());

        // No snapshots — use retention cutoff directly
        let safe = mgr
            .segments_safe_to_delete(&[5, 8, 10, 12, 14], 10)
            .await
            .unwrap();

        assert_eq!(safe, vec![5, 8]);
    }
}
