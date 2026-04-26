//! [`RestoreManager`]: downloads snapshot artefacts from S3 to local disk.
//!
//! Responsibilities:
//! - Load and validate a named snapshot (metadata + manifest integrity check)
//! - Download SSTable component files to a local directory
//! - Download archived commit log segments to a local directory
//!
//! The manager is stateless; all persistent state is in S3 and local disk.

use std::path::Path;
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};

use crate::manifest::Manifest;
use crate::snapshot::metadata::SnapshotMetadata;

/// Downloads snapshot artefacts (SSTables + commit log segments) from S3.
pub struct RestoreManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl RestoreManager {
    /// Creates a new manager backed by `store` with the given key prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /// Loads a snapshot's metadata and manifest from S3, then validates the
    /// manifest's SHA-256 against the digest stored in the metadata.
    ///
    /// Returns `(metadata, manifest)` on success.
    pub async fn load_and_validate_snapshot(
        &self,
        snapshot_name: &str,
    ) -> ferrosa_common::Result<(SnapshotMetadata, Manifest)> {
        // Load metadata.json.
        let meta_path = ObjectPath::from(format!(
            "{}/snapshots/{}/metadata.json",
            self.prefix, snapshot_name
        ));
        let meta_bytes = self.get_bytes(&meta_path).await?;
        let metadata: SnapshotMetadata = serde_json::from_slice(&meta_bytes).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to parse snapshot metadata for '{snapshot_name}': {e}"
            ))
        })?;

        // Load manifest.json.
        let manifest_path = ObjectPath::from(format!(
            "{}/snapshots/{}/manifest.json",
            self.prefix, snapshot_name
        ));
        let manifest_bytes = self.get_bytes(&manifest_path).await?;

        // Validate SHA-256.
        let actual_sha256 = hex_sha256(&manifest_bytes);
        if actual_sha256 != metadata.manifest_sha256 {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "snapshot '{snapshot_name}' manifest integrity check failed: \
                 expected sha256={}, got sha256={}",
                metadata.manifest_sha256, actual_sha256
            )));
        }

        // Deserialize manifest.
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to parse snapshot manifest for '{snapshot_name}': {e}"
            ))
        })?;

        Ok((metadata, manifest))
    }

    /// Downloads all SSTable component files referenced in `manifest` to
    /// `dest_dir`.
    ///
    /// Files already present on disk are skipped (idempotent). Returns the
    /// total number of SSTables processed (not individual files).
    pub async fn download_sstables(
        &self,
        manifest: &Manifest,
        dest_dir: &Path,
    ) -> ferrosa_common::Result<usize> {
        let components = [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
        ];

        let mut total = 0usize;

        for (table_id_str, entries) in &manifest.sstables {
            let table_dir = dest_dir.join(table_id_str);
            std::fs::create_dir_all(&table_dir).map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to create table dir {}: {e}",
                    table_dir.display()
                ))
            })?;

            for entry in entries {
                let hex = crate::upload::manager::hex_prefix_for(&entry.id);

                for component in &components {
                    // Use the shared key constructor — guarantees upload and
                    // download always agree on the path format.  The key
                    // format is: {prefix}/{hex}/{table}/{sstable}/{sstable}-{component}
                    let s3_path = crate::upload::manager::sstable_object_key(
                        &self.prefix,
                        &hex,
                        table_id_str,
                        &entry.id,
                        component,
                    );
                    let local_path = table_dir.join(format!("{}-{component}", entry.id));

                    // Skip if already present.
                    if local_path.exists() {
                        continue;
                    }

                    match self.store.get(&s3_path).await {
                        Ok(result) => {
                            let data = result.bytes().await.map_err(|e| {
                                ferrosa_common::Error::InvalidFormat(format!(
                                    "failed to read bytes from {s3_path}: {e}"
                                ))
                            })?;
                            std::fs::write(&local_path, &data).map_err(|e| {
                                ferrosa_common::Error::InvalidFormat(format!(
                                    "failed to write {}: {e}",
                                    local_path.display()
                                ))
                            })?;
                        }
                        // Optional components (e.g., CompressionInfo.db) may be absent.
                        Err(object_store::Error::NotFound { .. }) => continue,
                        Err(e) => {
                            return Err(ferrosa_common::Error::InvalidFormat(format!(
                                "S3 download failed for {s3_path}: {e}"
                            )));
                        }
                    }
                }
                total += 1;
            }
        }

        Ok(total)
    }

    /// Downloads archived commit log segments starting from `from_segment_id`
    /// (inclusive) to `dest_dir`.
    ///
    /// Reads the archive manifest to discover which segments are available,
    /// then downloads each one. Returns the sorted list of downloaded segment IDs.
    pub async fn download_segments(
        &self,
        from_segment_id: u64,
        dest_dir: &Path,
    ) -> ferrosa_common::Result<Vec<u64>> {
        // Load archive manifest.
        let archive_manifest =
            crate::commitlog::manifest::ArchiveManifest::load(self.store.as_ref(), &self.prefix)
                .await?;

        let mut downloaded_ids = Vec::new();

        for entry in &archive_manifest.segments {
            if entry.id < from_segment_id {
                continue;
            }

            let hex = crate::upload::manager::hex_prefix_for(&entry.id.to_string());
            let s3_path = ObjectPath::from(format!(
                "{}/commitlog-archive/{hex}/{}.log",
                self.prefix, entry.id
            ));
            let local_path = dest_dir.join(format!("commitlog-{}.log", entry.id));

            // Skip if already present.
            if local_path.exists() {
                downloaded_ids.push(entry.id);
                continue;
            }

            let data = self.get_bytes(&s3_path).await?;
            std::fs::write(&local_path, &data).map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to write segment {} to {}: {e}",
                    entry.id,
                    local_path.display()
                ))
            })?;
            downloaded_ids.push(entry.id);
        }

        downloaded_ids.sort_unstable();
        Ok(downloaded_ids)
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    async fn get_bytes(&self, path: &ObjectPath) -> ferrosa_common::Result<bytes::Bytes> {
        let result = self.store.get(path).await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to read object at {path}: {e}"))
        })?;
        result.bytes().await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to read bytes from {path}: {e}"))
        })
    }
}

/// Returns the hex-encoded SHA-256 digest of `data`.
fn hex_sha256(data: &[u8]) -> String {
    use std::fmt::Write as FmtWrite;
    let digest = Sha256::digest(data);
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, PutPayload};

    use super::*;
    use crate::commitlog::CommitLogPosition;
    use crate::manifest::{Manifest, ManifestEntry};
    use crate::snapshot::SnapshotManager;

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn create_test_snapshot(
        store: &Arc<dyn ObjectStore>,
        prefix: &str,
        name: &str,
        node_id: &str,
    ) -> SnapshotMetadata {
        let manifest = Manifest::new();
        manifest
            .save_with_retry(store.as_ref(), prefix)
            .await
            .unwrap();
        crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
            .await
            .unwrap();

        let snap_mgr = SnapshotManager::new(Arc::clone(store), prefix.to_string());
        let pos = CommitLogPosition {
            segment_id: 1,
            offset: 0,
        };
        snap_mgr
            .create_snapshot(name, &manifest, b"{}", pos, node_id, None, false)
            .await
            .unwrap()
    }

    // ── Test 1: load_and_validate_snapshot ────────────────────────────────

    #[tokio::test]
    async fn load_and_validate_snapshot_succeeds() {
        let store = make_store();
        let prefix = "test";

        create_test_snapshot(&store, prefix, "snap1", "node-1").await;

        let mgr = RestoreManager::new(Arc::clone(&store), prefix);
        let (meta, _manifest) = mgr.load_and_validate_snapshot("snap1").await.unwrap();
        assert_eq!(meta.name, "snap1");
        assert_eq!(meta.node_id, "node-1");
    }

    #[tokio::test]
    async fn load_and_validate_snapshot_detects_corruption() {
        let store = make_store();
        let prefix = "test";

        create_test_snapshot(&store, prefix, "snap-corrupt", "node-1").await;

        // Overwrite the manifest with garbage bytes (SHA-256 will no longer match).
        let bad_path = ObjectPath::from(format!("{prefix}/snapshots/snap-corrupt/manifest.json"));
        store
            .put(
                &bad_path,
                PutPayload::from(bytes::Bytes::from_static(b"garbage")),
            )
            .await
            .unwrap();

        let mgr = RestoreManager::new(Arc::clone(&store), prefix);
        let result = mgr.load_and_validate_snapshot("snap-corrupt").await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("integrity check"),
            "error should mention integrity check"
        );
    }

    // ── Test 2: download_segments (no segments in archive → empty list) ──

    #[tokio::test]
    async fn download_segments_returns_empty_when_no_archive() {
        let store = make_store();
        let prefix = "test";
        let dir = tempfile::tempdir().unwrap();

        let mgr = RestoreManager::new(Arc::clone(&store), prefix);
        let ids = mgr.download_segments(1, dir.path()).await.unwrap();
        assert!(
            ids.is_empty(),
            "no segments should be downloaded when archive is empty"
        );
    }

    // ── Test 3: download_sstables with empty manifest ────────────────────

    #[tokio::test]
    async fn download_sstables_empty_manifest_returns_zero() {
        let store = make_store();
        let prefix = "test";
        let dir = tempfile::tempdir().unwrap();

        let manifest = Manifest::new();
        let mgr = RestoreManager::new(Arc::clone(&store), prefix);
        let count = mgr.download_sstables(&manifest, dir.path()).await.unwrap();
        assert_eq!(count, 0);
    }

    // ── Test 4: download_sstables downloads available files ──────────────

    #[tokio::test]
    async fn download_sstables_downloads_files() {
        let store = make_store();
        let prefix = "test";
        let dir = tempfile::tempdir().unwrap();

        // Place a fake SSTable file in S3 at the canonical path.
        let table_id = "ks.users";
        let sstable_id = "0001";
        let hex = crate::upload::manager::hex_prefix_for(sstable_id);
        // Use sstable_object_key so this test always stays in sync with the
        // upload path.  The file must be at {prefix}/{hex}/{table}/{id}/{id}-Data.db.
        let s3_path = crate::upload::manager::sstable_object_key(
            prefix, &hex, table_id, sstable_id, "Data.db",
        );
        store
            .put(
                &s3_path,
                PutPayload::from(bytes::Bytes::from_static(b"data")),
            )
            .await
            .unwrap();

        let mut manifest = Manifest::new();
        manifest.add_sstable(
            table_id,
            ManifestEntry {
                id: sstable_id.to_string(),
                size: 4,
                min_token: 0,
                max_token: 0,
                min_timestamp: 0,
                max_timestamp: 0,
            },
        );

        let mgr = RestoreManager::new(Arc::clone(&store), prefix);
        let count = mgr.download_sstables(&manifest, dir.path()).await.unwrap();

        // One SSTable processed.
        assert_eq!(count, 1);

        // The file should exist on local disk.
        let local = dir
            .path()
            .join(table_id)
            .join(format!("{sstable_id}-Data.db"));
        assert!(local.exists(), "Data.db should be present on disk");
    }
}
