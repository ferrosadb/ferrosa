//! Archive manifest: JSON document in S3 tracking all archived commit log segments.
//!
//! The manifest lives at `{prefix}/commitlog-archive/archive-manifest.json`
//! in S3. It uses a read-modify-write pattern: load the current manifest,
//! append new segment entries, write it back.
//!
//! # Schema
//!
//! ```json
//! {
//!   "version": 1,
//!   "segments": [
//!     {"id": 42, "sha256": "hex...", "size": 33554432, "archived_at": "ISO8601"}
//!   ],
//!   "oldest_segment_id": 42,
//!   "newest_segment_id": 42
//! }
//! ```

use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

/// Path suffix within the prefix for the archive manifest.
const MANIFEST_PATH: &str = "commitlog-archive/archive-manifest.json";

/// Archive manifest tracking all archived commit log segments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveManifest {
    /// Schema version (always 1 for now).
    pub version: u32,
    /// Archived segment entries, ordered by segment ID ascending.
    pub segments: Vec<ArchiveSegmentEntry>,
    /// Smallest segment ID in the manifest (None if empty).
    pub oldest_segment_id: Option<u64>,
    /// Largest segment ID in the manifest (None if empty).
    pub newest_segment_id: Option<u64>,
}

/// A single archived segment entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveSegmentEntry {
    /// Segment ID.
    pub id: u64,
    /// SHA-256 hex digest of the segment file.
    pub sha256: String,
    /// Size in bytes.
    pub size: u64,
    /// ISO 8601 timestamp when the segment was archived.
    pub archived_at: String,
}

impl ArchiveManifest {
    /// Creates an empty manifest.
    pub fn new() -> Self {
        Self {
            version: 1,
            segments: Vec::new(),
            oldest_segment_id: None,
            newest_segment_id: None,
        }
    }

    /// Appends a segment entry and updates the bounds.
    pub fn append_segment(&mut self, entry: ArchiveSegmentEntry) {
        let id = entry.id;
        self.segments.push(entry);

        self.oldest_segment_id = Some(
            self.oldest_segment_id
                .map_or(id, |existing| existing.min(id)),
        );
        self.newest_segment_id = Some(
            self.newest_segment_id
                .map_or(id, |existing| existing.max(id)),
        );
    }

    /// Loads the manifest from S3. Returns an empty manifest if none exists.
    pub async fn load(store: &dyn ObjectStore, prefix: &str) -> ferrosa_common::Result<Self> {
        let path = ObjectPath::from(format!("{prefix}/{MANIFEST_PATH}"));
        match store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "failed to read archive manifest: {e}"
                    ))
                })?;
                let manifest: ArchiveManifest = serde_json::from_slice(&bytes).map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "failed to parse archive manifest: {e}"
                    ))
                })?;
                Ok(manifest)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self::new()),
            Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                "failed to load archive manifest: {e}"
            ))),
        }
    }

    /// Saves the manifest to S3, overwriting any existing version.
    pub async fn save(
        store: &dyn ObjectStore,
        prefix: &str,
        manifest: &ArchiveManifest,
    ) -> ferrosa_common::Result<()> {
        let path = ObjectPath::from(format!("{prefix}/{MANIFEST_PATH}"));
        let json = serde_json::to_vec_pretty(manifest).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to serialize archive manifest: {e}"
            ))
        })?;
        store
            .put(&path, bytes::Bytes::from(json).into())
            .await
            .map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "failed to save archive manifest: {e}"
                ))
            })?;
        Ok(())
    }

    /// Read-modify-write: loads the current manifest, appends a segment,
    /// and saves it back. This is the primary way the archiver adds entries.
    pub async fn append_and_save(
        store: &dyn ObjectStore,
        prefix: &str,
        entry: ArchiveSegmentEntry,
    ) -> ferrosa_common::Result<()> {
        let mut manifest = Self::load(store, prefix).await?;
        manifest.append_segment(entry);
        Self::save(store, prefix, &manifest).await
    }
}

impl Default for ArchiveManifest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn make_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn empty_manifest_serializes() {
        let manifest = ArchiveManifest::new();
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        assert!(json.contains("\"version\": 1"));
        assert!(json.contains("\"segments\": []"));
    }

    #[test]
    fn append_segment_updates_bounds() {
        let mut manifest = ArchiveManifest::new();
        manifest.append_segment(ArchiveSegmentEntry {
            id: 5,
            sha256: "abc123".to_string(),
            size: 1024,
            archived_at: "2026-03-18T00:00:00Z".to_string(),
        });
        manifest.append_segment(ArchiveSegmentEntry {
            id: 10,
            sha256: "def456".to_string(),
            size: 2048,
            archived_at: "2026-03-18T00:01:00Z".to_string(),
        });

        assert_eq!(manifest.segments.len(), 2);
        assert_eq!(manifest.oldest_segment_id, Some(5));
        assert_eq!(manifest.newest_segment_id, Some(10));
    }

    #[test]
    fn manifest_round_trip_json() {
        let mut manifest = ArchiveManifest::new();
        manifest.append_segment(ArchiveSegmentEntry {
            id: 42,
            sha256: "abcdef0123456789".to_string(),
            size: 33554432,
            archived_at: "2026-03-18T12:00:00Z".to_string(),
        });

        let json = serde_json::to_vec(&manifest).unwrap();
        let deserialized: ArchiveManifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(deserialized.version, 1);
        assert_eq!(deserialized.segments.len(), 1);
        assert_eq!(deserialized.segments[0].id, 42);
        assert_eq!(deserialized.segments[0].sha256, "abcdef0123456789");
        assert_eq!(deserialized.oldest_segment_id, Some(42));
        assert_eq!(deserialized.newest_segment_id, Some(42));
    }

    #[test]
    fn save_and_load_from_s3() {
        let rt = make_runtime();
        rt.block_on(async {
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let prefix = "node1";

            let mut manifest = ArchiveManifest::new();
            manifest.append_segment(ArchiveSegmentEntry {
                id: 1,
                sha256: "aaa".to_string(),
                size: 100,
                archived_at: "2026-03-18T00:00:00Z".to_string(),
            });

            // Save.
            ArchiveManifest::save(store.as_ref(), prefix, &manifest)
                .await
                .unwrap();

            // Load.
            let loaded = ArchiveManifest::load(store.as_ref(), prefix).await.unwrap();
            assert_eq!(loaded.segments.len(), 1);
            assert_eq!(loaded.segments[0].id, 1);
        });
    }

    #[test]
    fn load_returns_empty_when_no_manifest_exists() {
        let rt = make_runtime();
        rt.block_on(async {
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let loaded = ArchiveManifest::load(store.as_ref(), "empty-prefix")
                .await
                .unwrap();
            assert!(loaded.segments.is_empty());
            assert_eq!(loaded.version, 1);
        });
    }

    #[test]
    fn concurrent_append_uses_read_modify_write() {
        let rt = make_runtime();
        rt.block_on(async {
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let prefix = "node1";

            // Save initial manifest with segment 1.
            let mut manifest = ArchiveManifest::new();
            manifest.append_segment(ArchiveSegmentEntry {
                id: 1,
                sha256: "aaa".to_string(),
                size: 100,
                archived_at: "2026-03-18T00:00:00Z".to_string(),
            });
            ArchiveManifest::save(store.as_ref(), prefix, &manifest)
                .await
                .unwrap();

            // Load, append segment 2, save.
            let mut loaded = ArchiveManifest::load(store.as_ref(), prefix).await.unwrap();
            loaded.append_segment(ArchiveSegmentEntry {
                id: 2,
                sha256: "bbb".to_string(),
                size: 200,
                archived_at: "2026-03-18T00:01:00Z".to_string(),
            });
            ArchiveManifest::save(store.as_ref(), prefix, &loaded)
                .await
                .unwrap();

            // Verify both segments are present.
            let final_manifest = ArchiveManifest::load(store.as_ref(), prefix).await.unwrap();
            assert_eq!(final_manifest.segments.len(), 2);
            assert_eq!(final_manifest.oldest_segment_id, Some(1));
            assert_eq!(final_manifest.newest_segment_id, Some(2));
        });
    }
}
