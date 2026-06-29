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

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::manifest::Manifest;
use crate::snapshot::metadata::SnapshotMetadata;

/// Downloads snapshot artefacts (SSTables + commit log segments) from S3.
pub struct RestoreManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl RestoreManager {
    const REQUIRED_SSTABLE_COMPONENTS: [&'static str; 3] = ["Data.db", "Partitions.db", "Rows.db"];
    const OPTIONAL_SSTABLE_COMPONENTS: [&'static str; 4] = [
        "Filter.db",
        "Statistics.db",
        "TOC.txt",
        "CompressionInfo.db",
    ];
    const ALL_SSTABLE_COMPONENTS: [&'static str; 7] = [
        "Data.db",
        "Partitions.db",
        "Rows.db",
        "Filter.db",
        "Statistics.db",
        "TOC.txt",
        "CompressionInfo.db",
    ];

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
                let local_complete = Self::REQUIRED_SSTABLE_COMPONENTS.iter().all(|component| {
                    Self::generation_component_path(&table_dir, &entry.id, component).is_some()
                });
                if local_complete {
                    total += 1;
                    continue;
                }

                let local_incomplete = Self::generation_exists(&table_dir, &entry.id);
                let staging_dir = Self::temp_download_directory(&table_dir, &entry.id);
                let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                tokio::fs::create_dir_all(&staging_dir).await.map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "failed to create SSTable restore staging dir {}: {e}",
                        staging_dir.display()
                    ))
                })?;

                for component in &Self::REQUIRED_SSTABLE_COMPONENTS {
                    let s3_path = crate::upload::manager::sstable_object_key(
                        &self.prefix,
                        &hex,
                        table_id_str,
                        &entry.id,
                        component,
                    );
                    let local_path = staging_dir.join(format!("{}-{component}", entry.id));

                    if Self::download_component_to_path(self.store.as_ref(), &s3_path, &local_path)
                        .await?
                        .is_none()
                    {
                        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
                        if local_incomplete {
                            Self::quarantine_incomplete_generation(&table_dir, &entry.id);
                        }
                        return Err(ferrosa_common::Error::InvalidFormat(format!(
                            "snapshot SSTable {} for table {table_id_str} is missing required \
                             component {component} at {s3_path}; refusing to publish partial restore",
                            entry.id
                        )));
                    }
                }

                for component in &Self::OPTIONAL_SSTABLE_COMPONENTS {
                    let s3_path = crate::upload::manager::sstable_object_key(
                        &self.prefix,
                        &hex,
                        table_id_str,
                        &entry.id,
                        component,
                    );
                    let local_path = staging_dir.join(format!("{}-{component}", entry.id));

                    if Self::download_component_to_path(self.store.as_ref(), &s3_path, &local_path)
                        .await?
                        .is_none()
                    {
                        tracing::debug!(
                            table = table_id_str,
                            sstable = entry.id,
                            component,
                            "optional snapshot SSTable component absent in object store"
                        );
                    }
                }

                Self::sync_directory(&staging_dir).map_err(|e| {
                    let _ = std::fs::remove_dir_all(&staging_dir);
                    ferrosa_common::Error::InvalidFormat(format!(
                        "failed to sync SSTable restore staging dir {}: {e}",
                        staging_dir.display()
                    ))
                })?;

                if local_incomplete {
                    Self::quarantine_incomplete_generation(&table_dir, &entry.id);
                }

                let final_dir = table_dir.join(&entry.id);
                if final_dir.exists() {
                    Self::quarantine_incomplete_generation(&table_dir, &entry.id);
                }
                tokio::fs::rename(&staging_dir, &final_dir)
                    .await
                    .map_err(|e| {
                        let _ = std::fs::remove_dir_all(&staging_dir);
                        ferrosa_common::Error::InvalidFormat(format!(
                            "failed to atomically promote SSTable restore {} to {}: {e}",
                            staging_dir.display(),
                            final_dir.display()
                        ))
                    })?;
                Self::sync_directory(&table_dir).map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "failed to sync table dir {} after SSTable restore promotion: {e}",
                        table_dir.display()
                    ))
                })?;
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

    fn generation_component_path(
        table_dir: &Path,
        sstable_id: &str,
        component: &str,
    ) -> Option<std::path::PathBuf> {
        let generation_dir_path = table_dir
            .join(sstable_id)
            .join(format!("{sstable_id}-{component}"));
        if generation_dir_path.exists() {
            return Some(generation_dir_path);
        }

        let flat_path = table_dir.join(format!("{sstable_id}-{component}"));
        flat_path.exists().then_some(flat_path)
    }

    fn generation_exists(table_dir: &Path, sstable_id: &str) -> bool {
        table_dir.join(sstable_id).exists()
            || Self::ALL_SSTABLE_COMPONENTS
                .iter()
                .any(|component| table_dir.join(format!("{sstable_id}-{component}")).exists())
    }

    fn temp_download_directory(table_dir: &Path, sstable_id: &str) -> std::path::PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|time| time.as_nanos())
            .unwrap_or(0);
        table_dir.join(format!(
            ".download-{sstable_id}-{}-{suffix}",
            std::process::id()
        ))
    }

    async fn download_component_to_path(
        store: &dyn ObjectStore,
        s3_path: &ObjectPath,
        local_path: &Path,
    ) -> ferrosa_common::Result<Option<u64>> {
        let result = match store.get(s3_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "S3 download failed for {s3_path}: {e}"
                )));
            }
        };

        let mut stream = result.into_stream();
        let mut file = tokio::fs::File::create(local_path).await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create SSTable restore temp file {}: {e}",
                local_path.display()
            ))
        })?;
        let mut bytes = 0u64;
        while let Some(chunk) = stream.try_next().await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to stream SSTable component {s3_path}: {e}"
            ))
        })? {
            bytes = bytes.saturating_add(chunk.len() as u64);
            file.write_all(&chunk).await.map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to write SSTable restore temp file {}: {e}",
                    local_path.display()
                ))
            })?;
        }
        file.sync_data().await.map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to sync SSTable restore temp file {}: {e}",
                local_path.display()
            ))
        })?;
        Ok(Some(bytes))
    }

    fn sync_directory(dir: &Path) -> std::io::Result<()> {
        let file = std::fs::File::open(dir)?;
        file.sync_all()
    }

    fn quarantine_incomplete_generation(table_dir: &Path, sstable_id: &str) {
        let quarantine_dir = table_dir.join("quarantine");
        if let Err(e) = std::fs::create_dir_all(&quarantine_dir) {
            tracing::warn!(
                %e,
                sstable = sstable_id,
                dir = %table_dir.display(),
                "failed to create quarantine dir for incomplete SSTable restore"
            );
            return;
        }

        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|time| time.as_nanos())
            .unwrap_or(0);
        let generation_dir = table_dir.join(sstable_id);
        if generation_dir.exists() {
            let dst = quarantine_dir.join(format!("{sstable_id}-{suffix}"));
            if let Err(e) = std::fs::rename(&generation_dir, &dst) {
                tracing::warn!(
                    %e,
                    sstable = sstable_id,
                    src = %generation_dir.display(),
                    dst = %dst.display(),
                    "failed to quarantine incomplete generation directory"
                );
            }
        }

        for component in &Self::ALL_SSTABLE_COMPONENTS {
            let src = table_dir.join(format!("{sstable_id}-{component}"));
            if !src.exists() {
                continue;
            }
            let dst = quarantine_dir.join(format!("{sstable_id}-{suffix}-{component}"));
            if let Err(e) = std::fs::rename(&src, &dst) {
                tracing::warn!(
                    %e,
                    sstable = sstable_id,
                    src = %src.display(),
                    dst = %dst.display(),
                    "failed to quarantine incomplete flat component"
                );
            }
        }
    }

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

    async fn put_sstable_component(
        store: &Arc<dyn ObjectStore>,
        prefix: &str,
        table_id: &str,
        sstable_id: &str,
        component: &str,
        bytes: &'static [u8],
    ) {
        let hex = crate::upload::manager::hex_prefix_for(sstable_id);
        let s3_path = crate::upload::manager::sstable_object_key(
            prefix, &hex, table_id, sstable_id, component,
        );
        store
            .put(&s3_path, PutPayload::from(bytes::Bytes::from_static(bytes)))
            .await
            .unwrap();
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
        // Use sstable_object_key via the helper so this test always stays in
        // sync with the upload path.
        put_sstable_component(&store, prefix, table_id, sstable_id, "Data.db", b"data").await;
        put_sstable_component(
            &store,
            prefix,
            table_id,
            sstable_id,
            "Partitions.db",
            b"partitions",
        )
        .await;
        put_sstable_component(&store, prefix, table_id, sstable_id, "Rows.db", b"rows").await;

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
        let local = RestoreManager::generation_component_path(
            &dir.path().join(table_id),
            sstable_id,
            "Data.db",
        )
        .expect("Data.db should be present on disk");
        assert!(local.exists(), "Data.db should be present on disk");
    }

    #[tokio::test]
    async fn download_sstables_rejects_data_only_without_publishing_live_file() {
        let store = make_store();
        let prefix = "test-data-only";
        let dir = tempfile::tempdir().unwrap();
        let table_id = "ks.users";
        let sstable_id = "0007";

        put_sstable_component(&store, prefix, table_id, sstable_id, "Data.db", b"data").await;

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
        let result = mgr.download_sstables(&manifest, dir.path()).await;

        assert!(
            result.is_err(),
            "Data-only snapshot restore must fail closed instead of publishing an orphan"
        );
        let table_dir = dir.path().join(table_id);
        assert!(
            !table_dir.join(format!("{sstable_id}-Data.db")).exists(),
            "restore must not publish a flat Data-only component"
        );
        assert!(
            !table_dir
                .join(sstable_id)
                .join(format!("{sstable_id}-Data.db"))
                .exists(),
            "restore must not publish a generation-dir Data-only component"
        );
    }
}
