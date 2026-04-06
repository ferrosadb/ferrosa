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

/// Probe whether the object store supports conditional puts (CAS).
///
/// Writes a small test object with `PutMode::Create`, then tries to
/// overwrite it with `PutMode::Update`.  If the store rejects the
/// conditional write with "not implemented" or similar, returns `false`.
/// The probe object is deleted afterwards.
pub async fn probe_conditional_put_support(store: &dyn ObjectStore) -> bool {
    let probe_path = ObjectPath::from("__ferrosa_cas_probe");
    let payload = PutPayload::from(Bytes::from_static(b"probe"));

    // Step 1: unconditional write to create the object.
    let create_result = store.put(&probe_path, payload.clone()).await;
    let e_tag = match create_result {
        Ok(r) => r.e_tag,
        Err(_) => return false, // can't even write — assume no CAS
    };

    // Step 2: conditional overwrite using the etag.
    let cas_opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag,
            version: None,
        }),
        ..Default::default()
    };
    let cas_ok = store.put_opts(&probe_path, payload, cas_opts).await.is_ok();

    // Cleanup — best effort.
    let _ = store.delete(&probe_path).await;

    cas_ok
}

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

    /// Saves the manifest to the object store using a conditional put (CAS).
    ///
    /// Always uses etag-based compare-and-swap to prevent lost updates from
    /// concurrent writers. Callers must supply the `version` returned by the
    /// most recent [`Manifest::load`] call:
    ///
    /// - `None` → the object must not yet exist (`PutMode::Create`)
    /// - `Some(v)` → the object must still match etag `v` (`PutMode::Update`)
    ///
    /// Prefer [`Manifest::save_with_retry`] for automatic CAS conflict retry.
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
    /// Re-loads the latest manifest version on each attempt and uses
    /// conditional put (etag-based CAS). On conflict, retries with
    /// exponential backoff up to [`MAX_CAS_RETRIES`] times.
    ///
    /// The object store **must** support conditional puts. If it does not,
    /// callers must detect this at startup (via [`probe_conditional_put_support`])
    /// and refuse to start — passing a non-CAS store here is a configuration
    /// error that cannot be silently recovered.
    pub async fn save_with_retry(
        &self,
        store: &dyn ObjectStore,
        prefix: &str,
    ) -> ferrosa_common::Result<()> {
        self.save_with_retry_and_removals(store, prefix, &[]).await
    }

    /// Save the manifest with CAS retry, applying both additions and removals.
    ///
    /// On CAS conflict, re-loads the latest manifest, merges new entries from
    /// `self`, then re-applies `removals` to ensure compaction cleanup is not
    /// lost during merge. Without this, `merge_into` would re-introduce entries
    /// from the latest version that we intended to remove.
    pub async fn save_with_retry_and_removals(
        &self,
        store: &dyn ObjectStore,
        prefix: &str,
        removals: &[(String, Vec<String>)],
    ) -> ferrosa_common::Result<()> {
        for attempt in 0..MAX_CAS_RETRIES {
            // Re-load the LATEST manifest from S3, apply removals to it
            // FIRST (so old entries are gone), then merge in our new entries.
            //
            // Order matters: if compaction output reuses an input's ID
            // (gen collision), removing after merge would delete the new
            // entry. Removing from latest first ensures the old entry is
            // gone before self's replacement is merged in.
            let (mut latest, version) = Self::load(store, prefix).await?;
            for (table_id, ids) in removals {
                latest.remove_sstables(table_id, ids);
            }
            let merged = self.merge_into(&latest);

            match merged.save(store, prefix, version).await {
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

    /// Merge this manifest's entries into `base`, producing a new manifest
    /// that contains the union of both. Entries with the same `(table_id, id)`
    /// are deduplicated (self's version wins).
    ///
    /// Used by `save_with_retry` to preserve entries added by other nodes
    /// between our load and save.
    pub fn merge_into(&self, base: &Manifest) -> Manifest {
        let mut merged = base.clone();
        for (table_id, entries) in &self.sstables {
            let existing = merged.sstables.entry(table_id.clone()).or_default();
            for entry in entries {
                // Replace existing entries with the same ID (compaction output
                // can reuse an input's generation number). If self has a newer
                // entry for the same ID, it should win over base's version.
                if let Some(pos) = existing.iter().position(|e| e.id == entry.id) {
                    existing[pos] = entry.clone();
                } else {
                    existing.push(entry.clone());
                }
            }
        }
        if self.last_compacted_at > merged.last_compacted_at {
            merged.last_compacted_at = self.last_compacted_at.clone();
        }
        merged
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

    /// C2.3: CAS is always enforced — there is no unconditional PUT fallback.
    ///
    /// `save_with_retry` no longer accepts a `cas_supported` flag; it always
    /// uses conditional put.  An object store that does not support CAS must
    /// be detected by `probe_conditional_put_support` at startup and must
    /// cause the engine to refuse to start rather than silently allowing
    /// concurrent updates to overwrite each other.
    ///
    /// This test confirms the new API compiles without a `cas_supported`
    /// parameter and that a successful round-trip through `save_with_retry`
    /// uses CAS (evidenced by a second concurrent save failing with a CAS
    /// conflict rather than silently succeeding).
    #[tokio::test]
    async fn manifest_cas_required_at_startup() {
        let store = InMemory::new();
        let mut manifest = Manifest::new();
        manifest.add_sstable("ks.table", sample_entry("sst1"));

        // First save must succeed on an empty store (CAS Create).
        manifest
            .save_with_retry(&store, "test")
            .await
            .expect("first save must succeed");

        // A second save using a stale version (None → Create) must fail
        // because the manifest already exists — proving CAS is enforced.
        let stale_manifest = Manifest::new();
        let stale_result = stale_manifest.save(&store, "test", None).await;
        assert!(
            stale_result.is_err(),
            "CAS Create on an existing manifest must be rejected by the store"
        );
    }

    /// C2.3: Two concurrent CAS-based manifest updates must both succeed.
    ///
    /// Simulates two concurrent flushes each adding a different SSTable entry
    /// to the manifest via `save_with_retry`. The final manifest must contain
    /// both new entries plus the original, proving that CAS retry logic
    /// correctly merges concurrent writers rather than losing one update.
    #[tokio::test]
    async fn manifest_concurrent_flush_preserves_all_entries() {
        use std::sync::Arc;

        let store = Arc::new(InMemory::new());

        // Seed: one existing entry.
        let mut seed = Manifest::new();
        seed.add_sstable("ks.t", sample_entry("seed"));
        seed.save_with_retry(store.as_ref(), "concurrent")
            .await
            .unwrap();

        // Two concurrent tasks each add a distinct SSTable entry.
        let store_a = Arc::clone(&store);
        let task_a = tokio::spawn(async move {
            let (mut m, _) = Manifest::load(store_a.as_ref(), "concurrent")
                .await
                .unwrap();
            m.add_sstable("ks.t", sample_entry("flush-a"));
            m.save_with_retry(store_a.as_ref(), "concurrent")
                .await
                .unwrap();
        });

        let store_b = Arc::clone(&store);
        let task_b = tokio::spawn(async move {
            let (mut m, _) = Manifest::load(store_b.as_ref(), "concurrent")
                .await
                .unwrap();
            m.add_sstable("ks.t", sample_entry("flush-b"));
            m.save_with_retry(store_b.as_ref(), "concurrent")
                .await
                .unwrap();
        });

        tokio::try_join!(task_a, task_b).expect("concurrent saves must not panic");

        // Both tasks must have persisted their entry.
        let (final_manifest, _) = Manifest::load(store.as_ref(), "concurrent").await.unwrap();
        let entries = &final_manifest.sstables["ks.t"];
        let ids: Vec<&str> = entries.iter().map(|e| e.id.as_str()).collect();
        assert!(
            ids.contains(&"flush-a"),
            "flush-a entry must be present; got: {ids:?}"
        );
        assert!(
            ids.contains(&"flush-b"),
            "flush-b entry must be present; got: {ids:?}"
        );
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

    /// RED TEST: proves that save_with_retry (without removals) loses
    /// compaction deletions. This test MUST FAIL to demonstrate the bug.
    ///
    /// The bug: save_with_retry calls merge_into which starts from the
    /// latest S3 manifest (which still has old entries) and only adds new
    /// entries from self. Removals applied to self are lost.
    #[tokio::test]
    async fn save_with_retry_loses_compaction_removals() {
        let store = InMemory::new();

        // Initial manifest: 3 SSTables
        let mut initial = Manifest::new();
        initial.add_sstable("ks.t", sample_entry("sst1"));
        initial.add_sstable("ks.t", sample_entry("sst2"));
        initial.add_sstable("ks.t", sample_entry("sst3"));
        initial.save_with_retry(&store, "test").await.unwrap();

        // Simulate compaction: remove sst1 + sst2, add sst4
        let (mut manifest, _version) = Manifest::load(&store, "test").await.unwrap();
        manifest.remove_sstables("ks.t", &["sst1".to_string(), "sst2".to_string()]);
        manifest.add_sstable("ks.t", sample_entry("sst4"));

        // Save using the OLD API (no removals parameter) — this is how
        // the engine currently calls it for compaction
        manifest.save_with_retry(&store, "test").await.unwrap();

        // Verify: sst1 and sst2 should NOT be in the manifest
        let (final_manifest, _) = Manifest::load(&store, "test").await.unwrap();
        let ids: Vec<&str> = final_manifest.sstables["ks.t"]
            .iter()
            .map(|e| e.id.as_str())
            .collect();

        // This FAILS — proving the bug: removals are lost through save_with_retry
        assert!(
            !ids.contains(&"sst1"),
            "BUG: sst1 was removed but reappeared through save_with_retry merge; got: {ids:?}"
        );
    }

    /// GREEN TEST: save_with_retry_and_removals preserves compaction deletions.
    #[tokio::test]
    async fn compaction_removal_persists_through_save_with_retry_and_removals() {
        let store = InMemory::new();

        // Initial manifest: 3 SSTables
        let mut initial = Manifest::new();
        initial.add_sstable("ks.t", sample_entry("sst1"));
        initial.add_sstable("ks.t", sample_entry("sst2"));
        initial.add_sstable("ks.t", sample_entry("sst3"));
        initial.save_with_retry(&store, "test").await.unwrap();

        // Simulate compaction: remove sst1 + sst2, add sst4
        let (mut manifest, _version) = Manifest::load(&store, "test").await.unwrap();
        manifest.remove_sstables("ks.t", &["sst1".to_string(), "sst2".to_string()]);
        manifest.add_sstable("ks.t", sample_entry("sst4"));

        // Save with removals — the fix
        let removals = vec![(
            "ks.t".to_string(),
            vec!["sst1".to_string(), "sst2".to_string()],
        )];
        manifest
            .save_with_retry_and_removals(&store, "test", &removals)
            .await
            .unwrap();

        // Verify: sst1 and sst2 must NOT be in the manifest
        let (final_manifest, _) = Manifest::load(&store, "test").await.unwrap();
        let ids: Vec<&str> = final_manifest.sstables["ks.t"]
            .iter()
            .map(|e| e.id.as_str())
            .collect();

        assert!(
            !ids.contains(&"sst1"),
            "sst1 must be removed after compaction; got: {ids:?}"
        );
        assert!(
            !ids.contains(&"sst2"),
            "sst2 must be removed after compaction; got: {ids:?}"
        );
        assert!(
            ids.contains(&"sst3"),
            "sst3 must still be present; got: {ids:?}"
        );
        assert!(
            ids.contains(&"sst4"),
            "sst4 (compaction output) must be present; got: {ids:?}"
        );
    }
}
