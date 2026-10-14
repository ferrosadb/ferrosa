//! Recovery of a generation excluded as corrupt, from the OBJECT STORE.
//!
//! A generation the loader excludes as corrupt (a `Data.db` shorter than the
//! extent its own index claims, `ferrosa-sstable/src/reader.rs`
//! `validate_data_extent`) is **terminal** today: nothing re-admits it. The
//! only recovery sources are a healthy peer's copy (`self_heal::action`, gated
//! on `ReplicaPosture::can_refill`) and the quarantined files — and a node that
//! sees no healthy replica has neither, so `quarantine_with_healthy_replica`
//! refuses and the rows are gone from the readable set for good.
//!
//! The third source is the object store, which very plausibly still holds a
//! COMPLETE copy of the component: the generation was uploaded whole by the
//! flush that produced it. This module fetches that copy, verifies it, and
//! re-admits the generation **only** if the copy passes the loader's own
//! validation.
//!
//! # Invariants (each pinned by a test in this module's `tests`)
//!
//! (a) **Never delete or overwrite the last copy.** Every attempt fetches into
//!     a fresh staging directory and touches no live file until the fetched
//!     bytes have been verified. A refused attempt leaves the corrupt
//!     generation byte-identical — still on disk, still excluded, still
//!     available for a later attempt. The corrupt bytes may be the only
//!     surviving trace of that data; losing them while failing to recover is
//!     worse than doing nothing. On success the corrupt files are *moved* (never
//!     deleted, never overwritten) under a unique `quarantine/<gen>-<stamp>` name
//!     before the verified copy takes their place.
//! (b) **Never re-admit an unverified generation.** Re-admission calls the
//!     loader's own validation — [`StorageEngine::smoke_test_generation`], the
//!     same smoke test startup runs — rather than a second, weaker verifier. A
//!     fetched copy that still fails it is refused and the generation stays
//!     excluded.
//! (c) **A re-admitted generation is servable.** Re-admission installs the
//!     generation into the live view (so it is read, not merely on disk) and
//!     the tests assert the VALUE read back through [`StorageEngine::read`].
//!
//! Recovery is **node-local**: it repairs this node's own corrupt generation
//! from the store this node uploaded to, so it needs no peer and no cluster
//! initiator — exactly like the vector-index rebuild (`decide`, FMEA #4).

use std::path::{Path, PathBuf};

use object_store::ObjectStore;

use crate::engine::StorageEngine;
use crate::TableId;

use super::metrics;

/// Every component of a generation, and whether a complete copy needs it.
///
/// `Data.db` and `Partitions.db` are the two a reader cannot be opened without
/// (`evicted_read::QUERY_COMPONENTS`, `RestoreManager::REQUIRED_SSTABLE_COMPONENTS`);
/// the rest the loader reads as absent.
const COMPONENTS: &[(&str, bool)] = &[
    ("Data.db", true),
    ("Partitions.db", true),
    ("Rows.db", false),
    ("Filter.db", false),
    ("Statistics.db", false),
    ("TOC.txt", false),
    ("CompressionInfo.db", false),
    ("Digest.crc32", false),
    ("CRC.db", false),
];

/// What one recovery attempt did. Every arm other than [`Self::Reinstated`]
/// means the corrupt generation is still on disk and still excluded, exactly as
/// it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// The store's copy was fetched whole, passed the loader's own validation,
    /// and the generation is live and servable again.
    Reinstated,
    /// The store does not hold a complete copy. `missing` names the components
    /// that are absent.
    StoreIncomplete { missing: Vec<String> },
    /// The store's copy came back whole but still fails the loader's
    /// validation. `reason` is that validation's own message.
    StillCorrupt { reason: String },
    /// This node has no object store, so the third recovery source does not
    /// exist here.
    NoStore { reason: String },
    /// The attempt itself failed (an I/O error, a refused promotion, an
    /// unregistered table). `reason` names it.
    Failed { reason: String },
}

impl RecoveryOutcome {
    /// A one-line reason for the log and for the controller's ledger.
    pub fn reason(&self) -> String {
        match self {
            Self::Reinstated => "recovery succeeded".to_string(),
            Self::StoreIncomplete { missing } => {
                format!("the object store has no complete copy: missing {missing:?}")
            }
            Self::StillCorrupt { reason } => {
                format!("the object store's copy still fails validation: {reason}")
            }
            Self::NoStore { reason } => reason.clone(),
            Self::Failed { reason } => reason.clone(),
        }
    }
}

/// Wall-clock nanoseconds, purely to make a quarantine/staging name unique. The
/// self-heal decision path never reads a clock; this is a file name.
fn stamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

impl StorageEngine {
    /// Attempt to recover generation `gen` of `table_id` from this node's object
    /// store, and re-admit the generation into the live view if — and only if —
    /// the store's copy passes the loader's own validation.
    ///
    /// This is the **third** recovery source. The first two are a healthy peer's
    /// copy (`self_heal::action`, gated on [`ReplicaPosture::can_refill`]) and the
    /// quarantined files; a node whose table has no healthy replica on any peer
    /// has neither, and until this existed its corrupt generation stayed excluded
    /// for the lifetime of the process.
    ///
    /// Node-local, so it needs no peer and no cluster initiator: this node
    /// uploaded the generation, and this node can fetch it back.
    ///
    /// Never deletes and never overwrites (invariant (a)): a successful attempt
    /// moves the corrupt files under a unique `quarantine/<gen>-<stamp>/` name
    /// before the verified copy takes their place, and any refusal leaves them
    /// exactly as they were, still on disk, for a later attempt.
    ///
    /// See the module docs for the invariants each test in this module pins.
    pub fn recover_corrupt_generation(&self, table_id: &TableId, gen: u64) -> RecoveryOutcome {
        let Some((store, prefix)) = self.resolve_store_and_prefix() else {
            return self.refuse(
                table_id,
                gen,
                RecoveryOutcome::NoStore {
                    reason: "no object store is configured on this node, and no peer can \
                             refill it: the corrupt generation stays excluded"
                        .to_string(),
                },
            );
        };
        let table_dir = self.table_sstable_dir(table_id);
        let staging = match fetch_and_verify(
            &table_id.to_string(),
            &table_dir,
            gen,
            store.as_ref(),
            &prefix,
        ) {
            Ok(staging) => staging,
            Err(refusal) => return self.refuse(table_id, gen, refusal),
        };
        let generation_dir = match promote(&staging, &table_dir, gen) {
            Ok(dir) => dir,
            Err(e) => {
                return self.refuse(
                    table_id,
                    gen,
                    RecoveryOutcome::Failed {
                        // The staging directory holds the only verifiably-complete
                        // copy and is deliberately left in place; name it.
                        reason: format!(
                            "the verified copy could not be promoted out of {}: {e}",
                            staging.display()
                        ),
                    },
                );
            }
        };
        let outcome = self.admit_recovered_generation(table_id, &table_dir, gen, generation_dir);
        match &outcome {
            RecoveryOutcome::Reinstated => {
                metrics::inc_recovered(1);
                tracing::warn!(
                    keyspace = %table_id.keyspace(),
                    table = %table_id.table(),
                    generation = gen,
                    "self-heal: RECOVERED a corrupt-generation exclusion from the object \
                     store — the verified copy passed the loader's own validation and the \
                     generation is live again; the corrupt files are preserved under \
                     quarantine/"
                );
            }
            _ => return self.refuse(table_id, gen, outcome),
        }
        outcome
    }

    /// Log a refusal loudly, naming the table, the generation and the reason,
    /// and count it — a refusal is never silent.
    fn refuse(&self, table_id: &TableId, gen: u64, outcome: RecoveryOutcome) -> RecoveryOutcome {
        metrics::inc_recovery_refused();
        tracing::error!(
            keyspace = %table_id.keyspace(),
            table = %table_id.table(),
            generation = gen,
            reason = %outcome.reason(),
            "self-heal: could NOT recover corrupt generation from the object store; the \
             corrupt copy is untouched on disk, still excluded, and a later attempt may \
             succeed"
        );
        outcome
    }

    /// Install the promoted generation into the live view so it is read, not
    /// merely on disk (invariant (c)), and mark the generation's index unusable
    /// if its sidecars could not be opened — a consult over it must fail rather
    /// than answer with missing postings.
    fn admit_recovered_generation(
        &self,
        table_id: &TableId,
        table_dir: &Path,
        gen: u64,
        generation_dir: PathBuf,
    ) -> RecoveryOutcome {
        let gen_str = gen.to_string();
        let reader = match Self::open_sstable_from_dir(table_dir, &gen_str) {
            Ok(reader) => std::sync::Arc::new(reader),
            Err(e) => {
                return RecoveryOutcome::Failed {
                    reason: format!(
                        "the recovered generation {} could not be opened for the view: {e}",
                        generation_dir.display()
                    ),
                }
            }
        };
        let mut index_failures = Vec::new();
        let sidecars = Self::load_sidecars_for_generation(table_dir, gen, &mut index_failures);
        let installed = self.with_table_store(table_id, |store| {
            let live = store.install_recovered_generation(
                &gen_str,
                generation_dir.clone(),
                reader,
                sidecars,
            );
            for failed in &index_failures {
                store.mark_index_unavailable(failed);
            }
            live
        });
        match installed {
            // Already live, or installed: either way the generation is servable.
            Some(_) => RecoveryOutcome::Reinstated,
            None => RecoveryOutcome::Failed {
                reason: "the table is not registered on this node, so the recovered \
                         generation cannot be admitted into its view"
                    .to_string(),
            },
        }
    }
}

/// Fetch the object store's copy of generation `gen` of `table_id` into a fresh
/// staging directory beside `table_dir`, and verify it with the loader's own
/// validation. Returns the staging directory when the copy is verifiably
/// complete and valid.
///
/// **Touches no live file**: the corrupt generation is not read, moved or
/// written here, so a refusal costs nothing (invariant (a)). `Err` carries the
/// refusal to report.
fn fetch_and_verify(
    table_id: &str,
    table_dir: &Path,
    gen: u64,
    store: &dyn ObjectStore,
    prefix: &str,
) -> Result<PathBuf, RecoveryOutcome> {
    let staging = table_dir.join(format!(".recover-{gen}-{}-{}", std::process::id(), stamp()));
    let cleanup = |staging: &Path| {
        let _ = std::fs::remove_dir_all(staging);
    };
    if let Err(e) = std::fs::create_dir_all(&staging) {
        return Err(RecoveryOutcome::Failed {
            reason: format!(
                "cannot create the recovery staging directory {}: {e}",
                staging.display()
            ),
        });
    }

    let gen_str = gen.to_string();
    let hex = crate::upload::manager::hex_prefix_for(&gen_str);
    let mut missing = Vec::new();
    for (component, required) in COMPONENTS {
        let key =
            crate::upload::manager::sstable_object_key(prefix, &hex, table_id, &gen_str, component);
        let local = staging.join(format!("{gen_str}-{component}"));
        match StorageEngine::block_on_rehydration(
            StorageEngine::download_sstable_component_to_path(store, &key, &local),
        ) {
            Ok(Some(_)) => {}
            Ok(None) if *required => missing.push((*component).to_string()),
            // Optional and absent: the loader reads it as absent, exactly as it
            // does for a locally-written generation that never had one.
            Ok(None) => {}
            Err(e) => {
                cleanup(&staging);
                return Err(RecoveryOutcome::Failed {
                    reason: format!("fetching {key} failed: {e}"),
                });
            }
        }
    }
    // The generation's index artifacts come down with it, so the recovered
    // generation's index consults are complete rather than silently short.
    let pulled = StorageEngine::block_on_rehydration(StorageEngine::pull_index_artifacts(
        store, prefix, &hex, table_id, &gen_str, &staging,
    ));
    if let Err(e) = pulled {
        cleanup(&staging);
        return Err(RecoveryOutcome::Failed {
            reason: format!("pulling the generation's index artifacts failed: {e}"),
        });
    }

    if !missing.is_empty() {
        cleanup(&staging);
        return Err(RecoveryOutcome::StoreIncomplete { missing });
    }

    if let Err(e) = sync_dir(&staging) {
        cleanup(&staging);
        return Err(RecoveryOutcome::Failed {
            reason: format!("syncing {} failed: {e}", staging.display()),
        });
    }

    // Invariant (b): the SAME validation the loader runs at startup — never a
    // second, weaker verifier.
    if let Err(e) = StorageEngine::smoke_test_generation(&staging, gen) {
        cleanup(&staging);
        return Err(RecoveryOutcome::StillCorrupt {
            reason: e.to_string(),
        });
    }
    Ok(staging)
}

/// Move `gen`'s corrupt local files under a unique `quarantine/<gen>-<stamp>/`
/// name — moved, never deleted, never overwritten (invariant (a)) — and then put
/// the verified staged components where the generation lived, in the layout it
/// lived in. Returns that directory.
fn promote(staging: &Path, table_dir: &Path, gen: u64) -> ferrosa_common::Result<PathBuf> {
    let gen_str = gen.to_string();
    let generation_dir = table_dir.join(&gen_str);
    // The engine flushes flat (`<table_dir>/<gen>-Data.db`); a snapshot restore or
    // an earlier recovery writes a generation directory. Keep whichever this
    // generation had.
    let nested = generation_dir.join(format!("{gen_str}-Data.db")).exists();
    let source = if nested {
        generation_dir.clone()
    } else {
        table_dir.to_path_buf()
    };
    let prefix = format!("{gen_str}-");

    let quarantine = table_dir.join("quarantine");
    let preserved = quarantine.join(format!("{gen_str}-{}", stamp()));
    std::fs::create_dir_all(&preserved).map_err(|e| {
        ferrosa_common::Error::InvalidFormat(format!(
            "self-heal recovery: cannot create {} to preserve the corrupt generation: {e}",
            preserved.display()
        ))
    })?;
    let entries = std::fs::read_dir(&source).map_err(|e| {
        ferrosa_common::Error::InvalidFormat(format!(
            "self-heal recovery: cannot read {} to preserve the corrupt generation: {e}",
            source.display()
        ))
    })?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with(&prefix) {
            continue;
        }
        let to = preserved.join(&name);
        if to.exists() {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "self-heal recovery: refusing to overwrite {} while preserving the corrupt \
                 generation of {}",
                to.display(),
                source.display()
            )));
        }
        std::fs::rename(entry.path(), &to).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "self-heal recovery: cannot preserve the corrupt generation file {}: {e}",
                entry.path().display()
            ))
        })?;
    }
    if nested {
        // Now empty; the generation's new files may need this directory back.
        let _ = std::fs::remove_dir(&generation_dir);
    }

    let dest = if nested {
        generation_dir.clone()
    } else {
        table_dir.to_path_buf()
    };
    let staged = std::fs::read_dir(staging).map_err(|e| {
        ferrosa_common::Error::InvalidFormat(format!(
            "self-heal recovery: cannot read the verified copy {}: {e}",
            staging.display()
        ))
    })?;
    for entry in staged.flatten() {
        let name = entry.file_name();
        let to = dest.join(&name);
        if to.exists() {
            // Never overwrite: something else already holds this name. The
            // verified copy stays in staging, named by the caller's refusal.
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "self-heal recovery: refusing to overwrite {} with the recovered component",
                to.display()
            )));
        }
        std::fs::rename(entry.path(), &to).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "self-heal recovery: cannot publish the recovered component {}: {e}",
                to.display()
            ))
        })?;
    }
    sync_dir(&dest).map_err(|e| {
        ferrosa_common::Error::InvalidFormat(format!(
            "self-heal recovery: cannot sync {} after publishing the recovered generation: {e}",
            dest.display()
        ))
    })?;
    Ok(dest)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use object_store::ObjectStore;
    use serial_test::serial;

    use crate::engine::{StorageEngine, StorageEngineConfig};
    use crate::self_heal::test_fixtures::corrupt_one_generation;
    use crate::self_heal::{metrics, ClusterView, PeerHealthProbe, ReplicaAwareClusterView};
    use crate::upload::ObjectStoreConfig;
    use crate::TableId;

    use super::super::{SelfHealConfig, SelfHealController};

    const PREFIX: &str = "selfheal-recovery";

    fn table_id() -> TableId {
        TableId::new("test_ks", "test_table")
    }

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn key(name: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()))
    }

    fn row(value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn engine_config(dir: &Path) -> StorageEngineConfig {
        let mut config = StorageEngineConfig::test_config(dir);
        config.object_store = Some(ObjectStoreConfig {
            prefix: PREFIX.to_string(),
            ..ObjectStoreConfig::test_config()
        });
        config
    }

    /// Write `values` as one flushed generation each and upload them the way a
    /// running node does (flush → UploadManager → object store), so the store
    /// holds what a real node's store holds. Returns the engine and that store.
    async fn engine_with_uploaded_generations(
        dir: &Path,
        values: &[&str],
    ) -> (StorageEngine, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let engine = StorageEngine::new_with_upload_store(
            engine_config(dir),
            Arc::clone(&store),
            PREFIX.to_string(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap();
        let tid = table_id();
        engine.register_table(test_schema()).unwrap();
        for (i, value) in values.iter().enumerate() {
            let ts = 1_000 + i as i64;
            engine
                .write(&tid, &key(&format!("pk{i}")), row(value.as_bytes(), ts), ts)
                .unwrap();
            engine.flush(&tid).unwrap();
        }
        assert!(
            engine.sync_sstables_to_s3().await.unwrap() >= values.len(),
            "every flushed generation must have been uploaded to the store"
        );
        // Drop the flushed rows' commit-log segments, so the restart below reads
        // them from the SSTables alone (replay would otherwise resurrect them in
        // a memtable and hide which generation a value came from).
        let _ = engine.discard_completed_commit_log_segments();
        (engine, store)
    }

    /// Re-open the engine over the same data directory and store, after dropping
    /// the commit log so replay cannot resurrect rows the SSTables hold: the node
    /// restart the live incident describes, where the corrupt generation is
    /// excluded at load. Requires the default startup repair mode
    /// (`FERROSA_STARTUP_SMOKE_TEST` unset), under which a corrupt generation is
    /// excluded in place rather than moved to `quarantine/`.
    fn restart(dir: &Path, store: Arc<dyn ObjectStore>) -> StorageEngine {
        StorageEngine::new_with_upload_store(
            engine_config(dir),
            store,
            PREFIX.to_string(),
            &tokio::runtime::Handle::current(),
        )
        .unwrap()
    }

    /// The value of the single row of `pk`, or `None` when the row is not
    /// readable.
    fn value_of(engine: &StorageEngine, pk: &str) -> Option<Vec<u8>> {
        engine
            .read(&table_id(), &key(pk))
            .ok()
            .flatten()
            .and_then(|partition| partition.rows.first().cloned())
            .and_then(|r| r.cells.first().cloned())
            .and_then(|(_, cell)| cell.value)
    }

    struct NoPeers;
    impl PeerHealthProbe for NoPeers {
        fn healthy_peer_count(&self) -> usize {
            0
        }
    }

    /// The live cluster's posture: peers reachable, but none holds a healthy
    /// copy of this table — so quarantine (and the refill it needs) is refused.
    fn no_healthy_replica_cluster() -> Arc<dyn ClusterView> {
        Arc::new(ReplicaAwareClusterView::new(0, Arc::new(NoPeers)))
    }

    fn object_key(gen: u64, component: &str) -> object_store::path::Path {
        crate::upload::manager::sstable_object_key(
            PREFIX,
            &crate::upload::manager::hex_prefix_for(&gen.to_string()),
            &table_id().to_string(),
            &gen.to_string(),
            component,
        )
    }

    /// One self-heal tick against `engine`, the node's only remediation.
    fn tick(engine: &Arc<StorageEngine>) {
        let mut controller = SelfHealController::new(
            Arc::clone(engine),
            no_healthy_replica_cluster(),
            SelfHealConfig::default(),
        );
        controller.run_one_tick();
    }

    /// **RED.** On the tree before this change the tick escalates the issue
    /// (`EscalateReason::NoHealthyReplica`) and the generation stays excluded
    /// for the node's lifetime, so the final assertion fails. It fails on the
    /// BEHAVIOUR — the row is not readable — not on a missing symbol: every call
    /// in this test already exists on the base, so reverting the change leaves
    /// it failing an assertion rather than failing to compile.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn a_corrupt_generation_with_a_complete_store_copy_is_recovered_and_serves_its_values() {
        metrics::_reset_self_heal_metrics_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let (engine, store) = engine_with_uploaded_generations(dir.path(), &["v0", "v1"]).await;
        let table_dir = engine.table_sstable_dir(&table_id());
        drop(engine);

        // The store holds COMPLETE copies of both generations, uploaded before
        // the corruption; one local copy is then truncated on disk.
        let corrupt_gen = corrupt_one_generation(&table_dir);

        let engine = restart(dir.path(), store);
        engine.register_table(test_schema()).unwrap();
        assert_eq!(
            value_of(&engine, "pk1"),
            None,
            "precondition: generation {corrupt_gen} is excluded at load, so its row is unreadable"
        );
        assert_eq!(
            value_of(&engine, "pk0"),
            Some(b"v0".to_vec()),
            "precondition: the healthy generation still serves its row"
        );

        let engine = Arc::new(engine);
        tick(&engine);

        assert_eq!(
            value_of(&engine, "pk1"),
            Some(b"v1".to_vec()),
            "generation {corrupt_gen} was excluded as corrupt and the object store holds a \
             complete copy of it; recovery must re-admit the generation so its row reads back"
        );
        assert_eq!(
            value_of(&engine, "pk0"),
            Some(b"v0".to_vec()),
            "the healthy generation keeps serving its row"
        );
        assert_eq!(
            metrics::self_heal_metrics().recovered_generations_total,
            1,
            "the recovered generation is counted"
        );
        assert_eq!(metrics::self_heal_metrics().recovery_refused_total, 0);
    }

    /// Invariant (a): a generation the store cannot supply is left EXACTLY as it
    /// was — same bytes, still on disk, still excluded, still available for a
    /// later attempt — and the refusal is counted (never silent).
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn a_corrupt_generation_absent_from_the_store_is_left_exactly_as_it_was() {
        metrics::_reset_self_heal_metrics_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let (engine, store) = engine_with_uploaded_generations(dir.path(), &["v0", "v1"]).await;
        let table_dir = engine.table_sstable_dir(&table_id());
        drop(engine);

        let corrupt_gen = corrupt_one_generation(&table_dir);
        let data_db =
            StorageEngine::generation_component_path_for_test(&table_dir, corrupt_gen, "Data.db")
                .expect("the corrupt generation's Data.db");
        let before = std::fs::read(&data_db).unwrap();

        // The store's copy of this generation is gone (it was never uploaded
        // whole, or a lifecycle rule removed it).
        store
            .delete(&object_key(corrupt_gen, "Data.db"))
            .await
            .unwrap();
        store
            .delete(&object_key(corrupt_gen, "Partitions.db"))
            .await
            .unwrap();

        let engine = restart(dir.path(), store);
        engine.register_table(test_schema()).unwrap();
        let engine = Arc::new(engine);
        tick(&engine);

        assert_eq!(
            value_of(&engine, "pk1"),
            None,
            "with no store copy there is nothing to re-admit"
        );
        assert_eq!(
            std::fs::read(&data_db).unwrap(),
            before,
            "the corrupt generation must be byte-identical: it may be the only surviving \
             trace of this data, and a failed recovery must never lose it"
        );
        assert!(
            StorageEngine::list_generations_in_dir(&table_dir).contains(&corrupt_gen),
            "the corrupt generation stays on disk for a later attempt"
        );
        assert_eq!(metrics::self_heal_metrics().recovered_generations_total, 0);
        assert_eq!(
            metrics::self_heal_metrics().recovery_refused_total,
            1,
            "the refusal is counted, never silent"
        );
    }

    /// Invariant (b): a store copy that is itself truncated is NOT re-admitted —
    /// re-admission requires the loader's own validation to pass — and the
    /// corrupt local copy is left untouched.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn a_corrupt_generation_whose_store_copy_is_also_corrupt_is_not_re_admitted() {
        metrics::_reset_self_heal_metrics_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let (engine, store) = engine_with_uploaded_generations(dir.path(), &["v0", "v1"]).await;
        let table_dir = engine.table_sstable_dir(&table_id());
        drop(engine);

        let corrupt_gen = corrupt_one_generation(&table_dir);
        let data_db =
            StorageEngine::generation_component_path_for_test(&table_dir, corrupt_gen, "Data.db")
                .expect("the corrupt generation's Data.db");
        let before = std::fs::read(&data_db).unwrap();

        // The store's copy carries the same truncation (the flush that uploaded
        // it was itself the truncated one), so it fails validation too.
        store
            .put(
                &object_key(corrupt_gen, "Data.db"),
                object_store::PutPayload::from(bytes::Bytes::from(before.clone())),
            )
            .await
            .unwrap();

        let engine = restart(dir.path(), store);
        engine.register_table(test_schema()).unwrap();
        let engine = Arc::new(engine);
        tick(&engine);

        assert_eq!(
            value_of(&engine, "pk1"),
            None,
            "a store copy that still fails validation must never be re-admitted"
        );
        assert_eq!(
            std::fs::read(&data_db).unwrap(),
            before,
            "the corrupt local copy is left exactly as it was"
        );
        assert_eq!(metrics::self_heal_metrics().recovered_generations_total, 0);
        assert_eq!(
            metrics::self_heal_metrics().recovery_refused_total,
            1,
            "the refusal is counted, never silent"
        );
    }
}
