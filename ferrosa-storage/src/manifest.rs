//! S3 manifest: JSON document listing all live SSTables.
//!
//! Updated atomically via conditional put (etag-based compare-and-swap).
//! On conflict, the caller re-reads and retries.

use std::collections::HashMap;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};

/// Maximum number of CAS retry attempts for manifest saves.
pub const MAX_CAS_RETRIES: u32 = 3;

/// Manifest listing all live SSTables in object storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version for forward compatibility.
    pub format_version: u32,
    /// Map of table_id → SSTable entries.
    pub sstables: HashMap<String, Vec<ManifestEntry>>,
    /// ISO 8601 timestamp of the last compaction.
    pub last_compacted_at: Option<String>,
}

/// Metadata for a single SSTable in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub id: String,
    pub size: u64,
    pub min_token: i64,
    pub max_token: i64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

impl Manifest {
    /// Creates an empty manifest.
    pub fn new() -> Self {
        Self {
            format_version: 1,
            sstables: HashMap::new(),
            last_compacted_at: None,
        }
    }

    /// Loads the manifest from object storage.
    ///
    /// Returns `(manifest, update_version)` for CAS on subsequent save.
    /// If the manifest doesn't exist, returns a new empty manifest.
    pub async fn load(
        store: &dyn ObjectStore,
        prefix: &str,
    ) -> ferrosa_common::Result<(Self, Option<UpdateVersion>)> {
        let path = Self::manifest_path(prefix);
        match store.get(&path).await {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let data = result.bytes().await.map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!("failed to read manifest: {e}"))
                })?;
                let manifest: Manifest = serde_json::from_slice(&data).map_err(|e| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "failed to parse manifest JSON: {e}"
                    ))
                })?;
                Ok((manifest, version))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((Self::new(), None)),
            Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
                "failed to load manifest: {e}"
            ))),
        }
    }

    /// Saves the manifest with conditional put (etag-based CAS).
    ///
    /// If `version` doesn't match the current version in the store,
    /// returns an error (concurrent update detected).
    pub async fn save(
        &self,
        store: &dyn ObjectStore,
        prefix: &str,
        version: Option<UpdateVersion>,
    ) -> ferrosa_common::Result<()> {
        let path = Self::manifest_path(prefix);
        let data = serde_json::to_vec_pretty(self).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to serialize manifest: {e}"))
        })?;

        let opts = match version {
            Some(v) => PutOptions {
                mode: PutMode::Update(v),
                ..Default::default()
            },
            None => PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        };

        store
            .put_opts(&path, PutPayload::from(Bytes::from(data)), opts)
            .await
            .map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!("failed to save manifest: {e}"))
            })?;

        Ok(())
    }

    /// Saves the manifest with automatic CAS retry and exponential backoff.
    ///
    /// On each attempt, re-loads the latest version from the store, then
    /// attempts a conditional put. If a CAS conflict occurs (another writer
    /// updated the manifest concurrently), waits with exponential backoff
    /// and retries up to [`MAX_CAS_RETRIES`] times.
    pub async fn save_with_retry(
        &self,
        store: &dyn ObjectStore,
        prefix: &str,
    ) -> ferrosa_common::Result<()> {
        for attempt in 0..MAX_CAS_RETRIES {
            let (_, version) = Self::load(store, prefix).await?;
            match self.save(store, prefix, version).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < MAX_CAS_RETRIES - 1 => {
                    eprintln!(
                        "manifest CAS conflict on attempt {}, retrying: {e}",
                        attempt + 1
                    );
                    let backoff = std::time::Duration::from_millis(10 * 2u64.pow(attempt));
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!()
    }

    /// Adds an SSTable entry to the manifest.
    pub fn add_sstable(&mut self, table_id: &str, entry: ManifestEntry) {
        self.sstables
            .entry(table_id.to_string())
            .or_default()
            .push(entry);
    }

    /// Removes SSTable entries by ID (after compaction replaces them).
    pub fn remove_sstables(&mut self, table_id: &str, ids: &[String]) {
        if let Some(entries) = self.sstables.get_mut(table_id) {
            entries.retain(|e| !ids.contains(&e.id));
        }
    }

    fn manifest_path(prefix: &str) -> ObjectPath {
        if prefix.is_empty() {
            ObjectPath::from("manifest.json")
        } else {
            ObjectPath::from(format!("{prefix}/manifest.json"))
        }
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

// ── Schema snapshot persistence ────────────────────────────────────────────

/// Save a serialized schema snapshot to S3 alongside the manifest.
///
/// Stored at `{prefix}/schema.json`. This is an unconditional PUT
/// (no CAS) — last writer wins, which is safe because DDL operations
/// are serialized through the schema registry.
pub async fn save_schema_snapshot(
    store: &dyn ObjectStore,
    prefix: &str,
    snapshot_json: &[u8],
) -> ferrosa_common::Result<()> {
    let path = schema_path(prefix);
    eprintln!(
        "saving schema snapshot to S3 at {path} ({} bytes)",
        snapshot_json.len()
    );
    store
        .put(
            &path,
            PutPayload::from(Bytes::copy_from_slice(snapshot_json)),
        )
        .await
        .map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to save schema snapshot: {e}"))
        })?;
    eprintln!(
        "schema snapshot saved to S3 at {path} ({} bytes)",
        snapshot_json.len()
    );
    Ok(())
}

/// Load a schema snapshot from S3. Returns `None` if not found.
pub async fn load_schema_snapshot(
    store: &dyn ObjectStore,
    prefix: &str,
) -> ferrosa_common::Result<Option<Vec<u8>>> {
    let path = schema_path(prefix);
    eprintln!("loading schema snapshot from S3 at {path}");
    match store.get(&path).await {
        Ok(result) => {
            let data = result.bytes().await.map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!("failed to read schema snapshot: {e}"))
            })?;
            eprintln!(
                "schema snapshot loaded from S3 at {path} ({} bytes)",
                data.len()
            );
            Ok(Some(data.to_vec()))
        }
        Err(object_store::Error::NotFound { .. }) => {
            eprintln!("schema snapshot not found in S3 at {path}");
            Ok(None)
        }
        Err(e) => Err(ferrosa_common::Error::InvalidFormat(format!(
            "failed to load schema snapshot from {path}: {e}"
        ))),
    }
}

fn schema_path(prefix: &str) -> ObjectPath {
    if prefix.is_empty() {
        ObjectPath::from("schema.json")
    } else {
        ObjectPath::from(format!("{prefix}/schema.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn make_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn sample_entry(id: &str) -> ManifestEntry {
        ManifestEntry {
            id: id.to_string(),
            size: 1024,
            min_token: -100,
            max_token: 100,
            min_timestamp: 1000,
            max_timestamp: 2000,
        }
    }

    #[test]
    fn load_save_round_trip() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = InMemory::new();

            // First load: empty manifest.
            let (mut manifest, version) = Manifest::load(&store, "test").await.unwrap();
            assert!(manifest.sstables.is_empty());

            // Add entries and save.
            manifest.add_sstable("ks.table", sample_entry("sst1"));
            manifest.add_sstable("ks.table", sample_entry("sst2"));
            manifest.save(&store, "test", version).await.unwrap();

            // Re-load and verify.
            let (loaded, _) = Manifest::load(&store, "test").await.unwrap();
            assert_eq!(loaded.sstables["ks.table"].len(), 2);
            assert_eq!(loaded.sstables["ks.table"][0].id, "sst1");
            assert_eq!(loaded.sstables["ks.table"][1].id, "sst2");
        });
    }

    #[test]
    fn remove_sstables() {
        let mut manifest = Manifest::new();
        manifest.add_sstable("ks.t", sample_entry("a"));
        manifest.add_sstable("ks.t", sample_entry("b"));
        manifest.add_sstable("ks.t", sample_entry("c"));

        manifest.remove_sstables("ks.t", &["a".to_string(), "c".to_string()]);

        let entries = &manifest.sstables["ks.t"];
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "b");
    }

    #[test]
    fn max_cas_retries_is_reasonable() {
        const { assert!(MAX_CAS_RETRIES >= 2) };
        const { assert!(MAX_CAS_RETRIES <= 10) };
    }

    #[tokio::test]
    async fn manifest_save_with_retry_succeeds_on_fresh_store() {
        let store = InMemory::new();
        let mut manifest = Manifest::new();
        manifest.add_sstable("ks.table", sample_entry("sst1"));

        // save_with_retry should work on a fresh store (no conflicts).
        manifest.save_with_retry(&store, "test").await.unwrap();

        // Verify it was actually persisted.
        let (loaded, _) = Manifest::load(&store, "test").await.unwrap();
        assert_eq!(loaded.sstables["ks.table"].len(), 1);
        assert_eq!(loaded.sstables["ks.table"][0].id, "sst1");
    }

    #[test]
    fn empty_prefix() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = InMemory::new();

            let mut manifest = Manifest::new();
            manifest.add_sstable("t", sample_entry("x"));
            manifest.save(&store, "", None).await.unwrap();

            let (loaded, _) = Manifest::load(&store, "").await.unwrap();
            assert_eq!(loaded.sstables["t"].len(), 1);
        });
    }

    #[test]
    fn schema_snapshot_save_load_roundtrip() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = InMemory::new();
            let snapshot_json = br#"{"version":"00000000-0000-0000-0000-000000000001","keyspaces":{},"tables":{},"roles":{},"grants":{},"indexes":{}}"#;

            save_schema_snapshot(&store, "", snapshot_json).await.unwrap();
            let loaded = load_schema_snapshot(&store, "").await.unwrap();
            assert!(loaded.is_some());
            assert_eq!(loaded.unwrap(), snapshot_json.to_vec());
        });
    }

    #[test]
    fn schema_snapshot_not_found_returns_none() {
        let rt = make_runtime();
        rt.block_on(async {
            let store = InMemory::new();
            let loaded = load_schema_snapshot(&store, "").await.unwrap();
            assert!(loaded.is_none());
        });
    }
}
