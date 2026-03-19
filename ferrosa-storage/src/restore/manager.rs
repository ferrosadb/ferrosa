//! [`RestoreManager`]: stateless orchestrator for point-in-time restoration.
//!
//! Loads snapshot metadata, verifies manifest integrity, downloads SSTables
//! and archived commit log segments from S3 to a local directory.

use std::fmt::Write as FmtWrite;
use std::path::Path;
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::commitlog::manifest::ArchiveManifest;
use crate::commitlog::CommitLogPosition;
use crate::manifest::Manifest;
use crate::snapshot::metadata::SnapshotMetadata;
use crate::upload::manager::hex_prefix_for;

// ── Public config / result types ─────────────────────────────────────────────

/// Configuration controlling a point-in-time restore operation.
pub struct RestoreConfig {
    /// Snapshot name to restore from.
    pub snapshot_name: String,
    /// Optional point-in-time timestamp (Unix millis). If `None`, restore to snapshot time.
    pub point_in_time: Option<i64>,
    /// Allow cross-node restore (snapshot from a different `node_id`).
    pub force: bool,
    /// Current node ID for validation.
    pub node_id: String,
}

/// Summary of a completed restore operation.
pub struct RestoreResult {
    pub snapshot_name: String,
    pub sstables_downloaded: usize,
    pub segments_downloaded: usize,
    pub mutations_replayed: usize,
    pub restore_position: CommitLogPosition,
}

// ── RestoreManager ────────────────────────────────────────────────────────────

/// Stateless orchestrator for PITR snapshot restoration.
///
/// All persistent state lives in object storage and on the local filesystem.
/// The struct is `Clone`-friendly — it wraps only an `Arc` and a `String`.
pub struct RestoreManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl RestoreManager {
    /// Creates a new restore manager backed by `store` with the given key prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self { store, prefix }
    }

    /// Loads and validates snapshot metadata.
    ///
    /// Reads `metadata.json` and `manifest.json` from
    /// `{prefix}/snapshots/{name}/`, computes the SHA-256 of the manifest
    /// bytes, and compares it against the digest recorded in the metadata.
    ///
    /// Returns `(metadata, manifest)` if validation passes; returns an error
    /// if either object is missing or the digest does not match.
    pub async fn load_and_validate_snapshot(
        &self,
        name: &str,
    ) -> ferrosa_common::Result<(SnapshotMetadata, Manifest)> {
        // Precondition: name must be non-empty.
        if name.is_empty() {
            return Err(ferrosa_common::Error::InvalidFormat(
                "snapshot name must not be empty".to_string(),
            ));
        }

        // 1. Load metadata.json.
        let meta_path =
            ObjectPath::from(format!("{}/snapshots/{}/metadata.json", self.prefix, name));
        let meta_bytes = self.get_bytes(&meta_path).await?;
        let metadata: SnapshotMetadata = serde_json::from_slice(&meta_bytes).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to parse snapshot metadata for '{name}': {e}"
            ))
        })?;

        // 2. Load manifest.json.
        let manifest_path =
            ObjectPath::from(format!("{}/snapshots/{}/manifest.json", self.prefix, name));
        let manifest_bytes = self.get_bytes(&manifest_path).await?;

        // 3. Compute SHA-256 of manifest bytes.
        let actual_sha256 = hex_sha256(&manifest_bytes);

        // 4. Compare to stored digest.
        if actual_sha256 != metadata.manifest_sha256 {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "manifest integrity check failed for snapshot '{name}': \
                 sha256 digest mismatch (expected {}, got {actual_sha256})",
                metadata.manifest_sha256
            )));
        }

        // 5. Deserialize manifest.
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to parse manifest for snapshot '{name}': {e}"
            ))
        })?;

        Ok((metadata, manifest))
    }

    /// Downloads SSTable components from S3 to `target_dir`.
    ///
    /// For each SSTable entry in the manifest, creates
    /// `{target_dir}/{table_id}/{sstable_id}/` and downloads every component
    /// file found at `{prefix}/{hex}/{table_id}/{sstable_id}/` in S3.
    ///
    /// Returns the number of SSTables downloaded (one per `ManifestEntry`).
    pub async fn download_sstables(
        &self,
        manifest: &Manifest,
        target_dir: &Path,
    ) -> ferrosa_common::Result<usize> {
        let mut sstable_count: usize = 0;

        for (table_id, entries) in &manifest.sstables {
            for entry in entries {
                let sstable_id = &entry.id;
                let hex = hex_prefix_for(sstable_id);
                let s3_prefix =
                    ObjectPath::from(format!("{}/{hex}/{table_id}/{sstable_id}/", self.prefix));

                // List all component objects under this SSTable prefix.
                let list_result = self
                    .store
                    .list_with_delimiter(Some(&s3_prefix))
                    .await
                    .map_err(|e| {
                        ferrosa_common::Error::InvalidFormat(format!(
                            "failed to list SSTable components for '{sstable_id}': {e}"
                        ))
                    })?;

                // Create the local directory for this SSTable.
                let local_dir = target_dir.join(table_id).join(sstable_id);
                tokio::fs::create_dir_all(&local_dir)
                    .await
                    .map_err(ferrosa_common::Error::Io)?;

                // Download each component file.
                for object_meta in list_result.objects {
                    let component_name = component_name_from_path(&object_meta.location);
                    let data = self.get_bytes(&object_meta.location).await?;
                    let local_file = local_dir.join(&component_name);
                    write_local_file(&local_file, &data).await?;
                }

                sstable_count += 1;
            }
        }

        Ok(sstable_count)
    }

    /// Downloads archived commit log segments from S3 to `target_dir`.
    ///
    /// Loads the [`ArchiveManifest`], filters to segments with
    /// `id >= min_segment_id`, downloads each to `{target_dir}/{id}.log`,
    /// verifies the SHA-256 digest, and returns the sorted list of downloaded
    /// segment IDs.
    pub async fn download_segments(
        &self,
        min_segment_id: u64,
        target_dir: &Path,
    ) -> ferrosa_common::Result<Vec<u64>> {
        // 1. Load ArchiveManifest.
        let archive = ArchiveManifest::load(self.store.as_ref(), &self.prefix).await?;

        // 2. Filter segments >= min_segment_id.
        let mut to_download: Vec<_> = archive
            .segments
            .iter()
            .filter(|e| e.id >= min_segment_id)
            .collect();
        to_download.sort_by_key(|e| e.id);

        // 3. Download each segment.
        let mut downloaded_ids = Vec::with_capacity(to_download.len());
        for entry in to_download {
            let hex = hex_prefix_for(&entry.id.to_string());
            let s3_path = ObjectPath::from(format!(
                "{}/commitlog-archive/{hex}/{}.log",
                self.prefix, entry.id
            ));

            let data = self.get_bytes(&s3_path).await?;

            // 4. Verify SHA-256.
            let actual_sha256 = hex_sha256(&data);
            if actual_sha256 != entry.sha256 {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "segment {} integrity check failed: sha256 digest mismatch \
                     (expected {}, got {actual_sha256})",
                    entry.id, entry.sha256
                )));
            }

            // 5. Write to local file.
            let local_path = target_dir.join(format!("{}.log", entry.id));
            write_local_file(&local_path, &data).await?;

            downloaded_ids.push(entry.id);
        }

        // Return sorted list of downloaded segment IDs.
        downloaded_ids.sort_unstable();
        Ok(downloaded_ids)
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Fetches the raw bytes of an object from S3.
    async fn get_bytes(&self, path: &ObjectPath) -> ferrosa_common::Result<bytes::Bytes> {
        let result = self.store.get(path).await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to read object at {path}: {e}"))
        })?;
        result.bytes().await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to read bytes from {path}: {e}"))
        })
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────────

/// Returns the hex-encoded SHA-256 digest of `data`.
fn hex_sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Extracts the final path component (filename) from an object path.
///
/// Given `prefix/hex/table/sst-id/Data.db`, returns `Data.db`.
/// Falls back to the full path string if no `/` separator is found.
fn component_name_from_path(path: &ObjectPath) -> String {
    let s = path.as_ref();
    s.rsplit('/').next().unwrap_or(s).to_string()
}

/// Writes `data` to `path`, creating parent directories as needed.
async fn write_local_file(path: &Path, data: &[u8]) -> ferrosa_common::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(ferrosa_common::Error::Io)?;
    }
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(ferrosa_common::Error::Io)?;
    file.write_all(data)
        .await
        .map_err(ferrosa_common::Error::Io)?;
    file.flush().await.map_err(ferrosa_common::Error::Io)?;
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, PutPayload};

    use crate::commitlog::manifest::{ArchiveManifest, ArchiveSegmentEntry};
    use crate::commitlog::CommitLogPosition;
    use crate::manifest::{Manifest, ManifestEntry};
    use crate::restore::RestoreManager;
    use crate::snapshot::SnapshotManager;
    use crate::upload::manager::hex_prefix_for;

    use super::hex_sha256;

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn sample_position() -> CommitLogPosition {
        CommitLogPosition {
            segment_id: 5,
            offset: 128,
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

    async fn create_test_snapshot(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        name: &str,
    ) -> crate::snapshot::metadata::SnapshotMetadata {
        let snap_mgr = SnapshotManager::new(Arc::clone(&store), prefix);
        snap_mgr
            .create_snapshot(
                name,
                &sample_manifest(),
                &sample_schema(),
                sample_position(),
                "node-1",
                None,
                false,
            )
            .await
            .unwrap()
    }

    // ── Test 1 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn load_and_validate_snapshot_succeeds() {
        let store = make_store();
        let snap_meta = create_test_snapshot(Arc::clone(&store), "test", "snap1").await;

        let restore_mgr = RestoreManager::new(Arc::clone(&store), "test".to_string());
        let (meta, manifest) = restore_mgr
            .load_and_validate_snapshot("snap1")
            .await
            .unwrap();

        assert_eq!(meta.name, snap_meta.name);
        assert_eq!(meta.manifest_sha256, snap_meta.manifest_sha256);
        assert_eq!(meta.commit_log_position, snap_meta.commit_log_position);
        assert!(manifest.sstables.contains_key("ks.users"));
        assert_eq!(manifest.sstables["ks.users"][0].id, "sst-001");
    }

    // ── Test 2 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn load_and_validate_detects_corrupt_manifest() {
        let store = make_store();
        create_test_snapshot(Arc::clone(&store), "test", "snap-corrupt").await;

        // Overwrite manifest.json with corrupted bytes.
        let corrupt_path = ObjectPath::from("test/snapshots/snap-corrupt/manifest.json");
        store
            .put(
                &corrupt_path,
                PutPayload::from(bytes::Bytes::from_static(b"this is not valid json")),
            )
            .await
            .unwrap();

        let restore_mgr = RestoreManager::new(Arc::clone(&store), "test".to_string());
        let result = restore_mgr.load_and_validate_snapshot("snap-corrupt").await;

        assert!(result.is_err(), "expected error for corrupt manifest");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("integrity")
                || err_msg.contains("sha256")
                || err_msg.contains("mismatch")
                || err_msg.contains("digest"),
            "expected integrity error, got: {err_msg}"
        );
    }

    // ── Test 3 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn load_and_validate_nonexistent_snapshot_errors() {
        let store = make_store();
        let restore_mgr = RestoreManager::new(store, "test".to_string());

        let result = restore_mgr
            .load_and_validate_snapshot("ghost-snapshot")
            .await;
        assert!(result.is_err(), "expected error for nonexistent snapshot");
    }

    // ── Test 4 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn download_sstables_creates_local_files() {
        let store = make_store();
        let prefix = "test";

        // Create snapshot to get a valid manifest.
        create_test_snapshot(Arc::clone(&store), prefix, "snap-dl").await;

        let restore_mgr = RestoreManager::new(Arc::clone(&store), prefix.to_string());
        let (_, manifest) = restore_mgr
            .load_and_validate_snapshot("snap-dl")
            .await
            .unwrap();

        // Put fake SSTable component data in S3 at the expected paths.
        // Path format: {prefix}/{hex}/{table_id}/{sstable_id}/{component}
        let sstable_id = "sst-001";
        let table_id = "ks.users";
        let hex = hex_prefix_for(sstable_id);
        let components = ["Data.db", "Partitions.db"];
        for component in &components {
            let s3_path = ObjectPath::from(format!(
                "{prefix}/{hex}/{table_id}/{sstable_id}/{component}"
            ));
            store
                .put(
                    &s3_path,
                    PutPayload::from(bytes::Bytes::from(format!("fake {component} data"))),
                )
                .await
                .unwrap();
        }

        let target_dir = tempfile::tempdir().unwrap();
        let count = restore_mgr
            .download_sstables(&manifest, target_dir.path())
            .await
            .unwrap();

        // Should have downloaded 1 SSTable.
        assert_eq!(count, 1, "expected 1 SSTable downloaded");

        // Verify files exist on disk under target_dir/{table_id}/{sstable_id}/.
        for component in &components {
            let local_path = target_dir
                .path()
                .join(table_id)
                .join(sstable_id)
                .join(component);
            assert!(
                local_path.exists(),
                "expected file at {}",
                local_path.display()
            );
        }
    }

    // ── Test 5 ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn download_segments_filters_by_min_id() {
        let store = make_store();
        let prefix = "test";

        // Build an archive manifest with segments 1..=5.
        let mut archive = ArchiveManifest::new();
        for id in 1u64..=5 {
            // Put segment data in S3.
            let hex = hex_prefix_for(&id.to_string());
            let s3_path = ObjectPath::from(format!("{prefix}/commitlog-archive/{hex}/{id}.log"));
            let data = format!("segment-{id}-data");
            let data_bytes = bytes::Bytes::from(data.clone());
            store
                .put(&s3_path, PutPayload::from(data_bytes.clone()))
                .await
                .unwrap();

            // Compute SHA-256 for the manifest entry.
            let actual_sha256 = hex_sha256(data_bytes.as_ref());

            archive.append_segment(ArchiveSegmentEntry {
                id,
                sha256: actual_sha256,
                size: data.len() as u64,
                archived_at: "2026-03-19T00:00:00Z".to_string(),
            });
        }
        ArchiveManifest::save(store.as_ref(), prefix, &archive)
            .await
            .unwrap();

        let restore_mgr = RestoreManager::new(Arc::clone(&store), prefix.to_string());
        let target_dir = tempfile::tempdir().unwrap();

        let downloaded = restore_mgr
            .download_segments(3, target_dir.path())
            .await
            .unwrap();

        // Should have downloaded segments 3, 4, 5 only.
        assert_eq!(downloaded, vec![3u64, 4, 5], "expected segments [3,4,5]");

        // Verify the files exist locally.
        for id in 3u64..=5 {
            let local_path = target_dir.path().join(format!("{id}.log"));
            assert!(local_path.exists(), "expected segment file {id}.log");
        }

        // Verify segments 1 and 2 are NOT present.
        for id in 1u64..=2 {
            let local_path = target_dir.path().join(format!("{id}.log"));
            assert!(
                !local_path.exists(),
                "segment {id}.log should not have been downloaded"
            );
        }
    }
}
