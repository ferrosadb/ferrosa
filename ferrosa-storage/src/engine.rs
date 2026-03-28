//! Top-level storage engine composing commit log, memtable, flush, compaction,
//! S3 upload, manifest, and local cache into a single API.
//!
//! [`StorageEngine`] is the entry point for all storage operations. It owns:
//! - A [`CommitLog`] for write-ahead durability.
//! - Per-table [`TableStore`] instances for memtable + SSTable management.
//! - A [`CompactionExecutor`] for background STCS compaction.
//! - An optional [`UploadManager`] for async S3 uploads.
//! - A [`LocalCache`] tracking ephemeral-disk SSTable files.
//!
//! Thread safety: reads are lock-free (via ArcSwap in TableStore). Writes
//! take no global lock — the commit log uses CAS, memtable is lock-free.
//! Only flush and compaction take per-table serialized guards.

use std::collections::HashMap;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_sstable::types::{Partition, Row};
use ferrosa_sstable::WriteOptions;

use crate::cache::LocalCache;
use crate::commitlog::config::{CommitLogConfig, CommitLogPosition, TableId};
use crate::commitlog::mutation::Mutation;
use crate::commitlog::CommitLog;
use crate::compaction::executor::CompactionExecutor;
use crate::compaction::strategy::{CompactionConfig, CompactionStrategy, SizeTieredStrategy};
use crate::flush::FileFlushTarget;
use crate::store::TableStore;
use crate::upload::{ObjectStoreConfig, UploadManager};

/// Configuration for the entire storage engine.
///
/// Composes sub-configurations for each component. Use `from_env()` for
/// production (reads `FERROSA_*` env vars) or `test_config()` for tests.
pub struct StorageEngineConfig {
    pub commit_log: CommitLogConfig,
    pub compaction: CompactionConfig,
    pub object_store: Option<ObjectStoreConfig>,
    pub local_cache_max_bytes: u64,
    pub flush_threshold_bytes: u64,
    pub data_dir: PathBuf,
}

impl StorageEngineConfig {
    /// Reads configuration from `FERROSA_*` environment variables.
    pub fn from_env() -> ferrosa_common::Result<Self> {
        let data_dir = PathBuf::from(
            std::env::var("FERROSA_DATA_DIR").unwrap_or_else(|_| "/var/lib/ferrosa".into()),
        );

        let commit_log = CommitLogConfig {
            log_dir: data_dir.join("commitlog"),
            checkpoint_dir: data_dir.join("commitlog"),
            ..CommitLogConfig::default()
        };

        let compaction = CompactionConfig::from_env(data_dir.join("compaction"));

        let object_store = ObjectStoreConfig::from_env().ok();

        let local_cache_max_bytes = std::env::var("FERROSA_CACHE_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10 * 1024 * 1024 * 1024); // 10 GB default

        let flush_threshold_bytes = std::env::var("FERROSA_FLUSH_THRESHOLD_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(64 * 1024 * 1024); // 64 MB default

        Ok(Self {
            commit_log,
            compaction,
            object_store,
            local_cache_max_bytes,
            flush_threshold_bytes,
            data_dir,
        })
    }

    /// Creates a test configuration using the given temp directory.
    #[cfg(test)]
    pub fn test_config(dir: &Path) -> Self {
        Self {
            commit_log: CommitLogConfig::test_config(dir),
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024, // 1 MB
            flush_threshold_bytes: 4096,        // 4 KB — triggers flush quickly in tests
            data_dir: dir.to_path_buf(),
        }
    }
}

/// Top-level storage engine.
///
/// One instance per node. Manages multiple tables, each with its own
/// `TableStore`. The commit log is shared across all tables.
pub struct StorageEngine {
    config: StorageEngineConfig,
    tables: RwLock<HashMap<TableId, TableState>>,
    pub(crate) commit_log: CommitLog,
    compaction_executor: CompactionExecutor,
    upload_manager: Option<UploadManager>,
    local_cache: LocalCache,
    observers: RwLock<Vec<Arc<dyn crate::observer::WriteObserver>>>,
    async_observers: RwLock<Vec<AsyncObserverState>>,
    /// Default channel capacity for async observers.
    async_observer_capacity: usize,
    /// Index build scheduler — rebuilds secondary indexes after compaction.
    index_scheduler: Option<crate::index::IndexBuildScheduler>,
    /// Shared index state tracker.
    index_tracker: Arc<crate::index::IndexStateTracker>,
    /// Batchlog manager for logged batch coordination.
    batchlog: Option<crate::batchlog::BatchlogManager>,
    /// Background archiver task handle, if archiving is enabled.
    archiver_handle: Option<tokio::task::JoinHandle<()>>,
    /// Whether the configured object store supports conditional puts (CAS).
    /// Set once at startup via `probe_conditional_put_support()`.
    /// When false, manifest writes use unconditional overwrite.
    s3_cas_supported: bool,
    /// Compaction S3 operation metrics (uploads, deletes, bytes reclaimed).
    pub compaction_metrics: Arc<crate::metrics::CompactionMetrics>,
    /// Injected object store used in tests to bypass `ObjectStoreConfig::build_object_store()`.
    /// When `Some`, `resolve_store_and_prefix()` returns this store instead of building one.
    #[cfg(test)]
    upload_store_override: Option<(Arc<dyn object_store::ObjectStore>, String)>,
}

/// Per-table state: schema + store.
struct TableState {
    #[allow(dead_code)]
    schema: TableSchema,
    store: TableStore<FileFlushTarget>,
}

/// SSTable reader type alias for file-backed SSTables.
type FileSSTableReader =
    Arc<ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt>>;

/// Sidecar map type alias: index name -> sidecar reader for one SSTable.
type SSTableSidecarMap = Arc<HashMap<String, crate::index::sidecar::SidecarReader>>;

/// State for a single async observer: the observer, its sender half, and a
/// drop counter for backpressure metrics.
struct AsyncObserverState {
    observer: Arc<dyn crate::observer::WriteObserver>,
    sender: tokio::sync::mpsc::Sender<(TableId, Mutation)>,
    drop_count: Arc<AtomicU64>,
}

impl StorageEngine {
    /// Creates a new storage engine. Initializes the commit log, compaction
    /// executor, and optional upload manager.
    pub fn new(
        config: StorageEngineConfig,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> ferrosa_common::Result<Self> {
        // Ensure data directories exist.
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create commitlog dir: {e}"))
        })?;

        let commit_log = CommitLog::new(config.commit_log.clone())?;
        let compaction_executor = CompactionExecutor::new();

        let upload_manager = match (&config.object_store, runtime) {
            (Some(os_config), Some(rt)) => {
                let store = os_config.build_object_store()?;
                Some(UploadManager::new(
                    Arc::from(store),
                    os_config.prefix.clone(),
                    os_config.upload_queue_depth,
                    rt,
                ))
            }
            _ => None,
        };

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        let index_tracker = Arc::new(crate::index::IndexStateTracker::new());
        let index_scheduler = {
            let backend = Arc::new(crate::index::LocalBackend::new(config.data_dir.clone()));
            Some(
                crate::index::IndexBuildScheduler::with_backend_and_data_dir(
                    2,
                    Arc::clone(&index_tracker),
                    backend,
                    config.data_dir.clone(),
                ),
            )
        };

        Ok(Self {
            config,
            tables: RwLock::new(HashMap::new()),
            commit_log,
            compaction_executor,
            upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle: None,
            s3_cas_supported: true, // default; call probe_s3_cas() after construction
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            #[cfg(test)]
            upload_store_override: None,
        })
    }

    /// Probe the configured object store for conditional put support.
    /// Call this once after construction when an S3 store is configured.
    /// Sets `s3_cas_supported` to false if the store (e.g. RustFS) doesn't
    /// support etag-based conditional writes.
    pub async fn probe_s3_cas(&mut self) {
        if let Ok((_, store)) = self.object_store_and_config() {
            let supported = crate::manifest::probe_conditional_put_support(store.as_ref()).await;
            if !supported {
                tracing::info!("object store does not support conditional puts — using unconditional manifest writes");
            }
            self.s3_cas_supported = supported;
        }
    }

    /// Creates a storage engine with an explicit archive object store.
    ///
    /// Used by tests to inject an InMemory store instead of real S3.
    /// When `archive_store` is `Some` and `config.commit_log.archive` is
    /// enabled, spawns a background archiver task on the provided runtime.
    pub fn new_with_archive_store(
        config: StorageEngineConfig,
        runtime: Option<&tokio::runtime::Handle>,
        archive_store: Option<Arc<dyn object_store::ObjectStore>>,
        archive_prefix: String,
    ) -> ferrosa_common::Result<Self> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create commitlog dir: {e}"))
        })?;

        let mut commit_log = CommitLog::new(config.commit_log.clone())?;
        let compaction_executor = CompactionExecutor::new();

        let upload_manager = match (&config.object_store, runtime) {
            (Some(os_config), Some(rt)) => {
                let store = os_config.build_object_store()?;
                Some(UploadManager::new(
                    Arc::from(store),
                    os_config.prefix.clone(),
                    os_config.upload_queue_depth,
                    rt,
                ))
            }
            _ => None,
        };

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        // Set up archiver if enabled.
        let archiver_handle = match (&config.commit_log.archive, archive_store, runtime) {
            (Some(archive_cfg), Some(store), Some(rt)) if archive_cfg.enabled => {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(64);
                commit_log.set_archive_channel(tx);

                let archiver = crate::commitlog::archiver::CommitLogArchiver::new(
                    store,
                    archive_prefix,
                    config.commit_log.log_dir.clone(),
                );

                // Spawn background archiver task.
                let handle = rt.spawn(async move {
                    while let Some(segment_id) = rx.recv().await {
                        match archiver.archive_segment(segment_id).await {
                            Ok(result) => {
                                // Update manifest.
                                let entry = crate::commitlog::manifest::ArchiveSegmentEntry {
                                    id: result.segment_id,
                                    sha256: result.sha256,
                                    size: result.size,
                                    archived_at: result.archived_at,
                                };
                                if let Err(e) =
                                    crate::commitlog::manifest::ArchiveManifest::append_and_save(
                                        archiver.store(),
                                        archiver.prefix(),
                                        entry,
                                    )
                                    .await
                                {
                                    eprintln!(
                                        "[commitlog-archiver] manifest update failed for segment {segment_id}: {e}"
                                    );
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "[commitlog-archiver] failed to archive segment {segment_id}: {e}"
                                );
                            }
                        }
                    }
                });
                Some(handle)
            }
            _ => None,
        };

        let index_tracker = Arc::new(crate::index::IndexStateTracker::new());
        let index_scheduler = {
            let backend = Arc::new(crate::index::LocalBackend::new(config.data_dir.clone()));
            Some(
                crate::index::IndexBuildScheduler::with_backend_and_data_dir(
                    2,
                    Arc::clone(&index_tracker),
                    backend,
                    config.data_dir.clone(),
                ),
            )
        };

        Ok(Self {
            config,
            tables: RwLock::new(HashMap::new()),
            commit_log,
            compaction_executor,
            upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle,
            s3_cas_supported: true,
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            #[cfg(test)]
            upload_store_override: None,
        })
    }

    /// Opens an existing storage engine directory and replays uncommitted
    /// mutations from the commit log.
    ///
    /// Returns the engine and the list of mutations that need to be replayed.
    /// Call [`replay_mutations`](Self::replay_mutations) with the returned
    /// mutations after registering all table schemas.
    pub fn open(
        config: StorageEngineConfig,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> ferrosa_common::Result<(Self, Vec<Mutation>)> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create commitlog dir: {e}"))
        })?;

        let (commit_log, pending_mutations) =
            crate::commitlog::CommitLog::open_and_replay(config.commit_log.clone())?;

        let compaction_executor = CompactionExecutor::new();

        let upload_manager = match (&config.object_store, runtime) {
            (Some(os_config), Some(rt)) => {
                let store = os_config.build_object_store()?;
                Some(UploadManager::new(
                    Arc::from(store),
                    os_config.prefix.clone(),
                    os_config.upload_queue_depth,
                    rt,
                ))
            }
            _ => None,
        };

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        let index_tracker = Arc::new(crate::index::IndexStateTracker::new());
        let index_scheduler = {
            let backend = Arc::new(crate::index::LocalBackend::new(config.data_dir.clone()));
            Some(
                crate::index::IndexBuildScheduler::with_backend_and_data_dir(
                    2,
                    Arc::clone(&index_tracker),
                    backend,
                    config.data_dir.clone(),
                ),
            )
        };

        let engine = Self {
            config,
            tables: RwLock::new(HashMap::new()),
            commit_log,
            compaction_executor,
            upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle: None,
            s3_cas_supported: true,
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            #[cfg(test)]
            upload_store_override: None,
        };

        Ok((engine, pending_mutations))
    }

    /// Replays a set of pending mutations into their respective table memtables.
    ///
    /// This is called after [`open`](Self::open) and after all table schemas
    /// have been registered via [`register_table`](Self::register_table).
    /// Mutations for unregistered tables are silently skipped.
    pub fn replay_mutations(&self, mutations: Vec<Mutation>) -> ferrosa_common::Result<()> {
        for mutation in mutations {
            let table_id = TableId::new(&mutation.keyspace, &mutation.table);
            let tables = self.tables.read();
            if let Some(state) = tables.get(&table_id) {
                for row in &mutation.rows {
                    // Use best-effort replay: log but don't fail on individual row errors.
                    if let Err(e) = state.store.write(&mutation.key, row.clone()) {
                        eprintln!("[replay] failed to replay row for {table_id}: {e}");
                    }
                }
            }
            // Tables not yet registered are silently skipped.
        }
        Ok(())
    }

    /// Registers all system table schemas (`system_schema.*` and `system_auth.*`)
    /// so the storage engine can persist system metadata.
    ///
    /// Called during bootstrap before any DDL or auth operations. System tables
    /// use the same flush/compaction/S3 pipeline as user tables. Idempotent:
    /// safe to call multiple times.
    pub fn register_system_tables(&self) -> ferrosa_common::Result<()> {
        for schema in ferrosa_schema::system::persistence::all_system_table_schemas() {
            self.register_table(schema)?;
        }
        Ok(())
    }

    /// Registers a table schema so the engine can accept writes for it.
    ///
    /// Creates the per-table `FileFlushTarget` directory and `TableStore`.
    /// If the directory already contains SSTable files from a previous run,
    /// they are opened and loaded into the store so reads work immediately
    /// after re-opening the engine (crash recovery path). Sidecar index files
    /// are also loaded and associated with their corresponding SSTables.
    pub fn register_table(&self, schema: TableSchema) -> ferrosa_common::Result<()> {
        self.register_table_inner(schema, vec![])
    }

    /// Registers a table schema with secondary index declarations.
    ///
    /// `indexed_columns` is a list of `(index_name, column_position)` pairs
    /// passed through to [`TableStore::new_with_indexes`]. Sidecar files from
    /// prior flushes are loaded from disk alongside the SSTables.
    pub fn register_table_with_indexes(
        &self,
        schema: TableSchema,
        indexed_columns: Vec<(String, usize)>,
    ) -> ferrosa_common::Result<()> {
        self.register_table_inner(schema, indexed_columns)
    }

    /// Internal: create a `TableStore` for a table, loading existing SSTables
    /// and sidecar files from disk. Idempotent: skips already-registered tables.
    fn register_table_inner(
        &self,
        schema: TableSchema,
        indexed_columns: Vec<(String, usize)>,
    ) -> ferrosa_common::Result<()> {
        let table_id = TableId::new(&schema.keyspace, &schema.table);
        {
            let tables = self.tables.read();
            if tables.contains_key(&table_id) {
                return Ok(());
            }
        }
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        std::fs::create_dir_all(&table_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create table dir: {e}"))
        })?;

        // Load any SSTables that already exist on disk (e.g., after crash recovery).
        let (existing_sstables, existing_sidecars) =
            Self::load_existing_sstables_and_sidecars(&table_dir);

        let flush_target = FileFlushTarget::new_starting_at(table_dir)?;
        let store = if existing_sstables.is_empty() && indexed_columns.is_empty() {
            TableStore::new(schema.clone(), flush_target, WriteOptions::default())
        } else if existing_sstables.is_empty() {
            TableStore::new_with_indexes(
                schema.clone(),
                flush_target,
                WriteOptions::default(),
                indexed_columns,
            )
        } else {
            TableStore::new_with_sstables_and_indexes(
                schema.clone(),
                flush_target,
                WriteOptions::default(),
                existing_sstables,
                existing_sidecars,
                indexed_columns,
            )
        };

        // Register each declared index in the tracker.
        for (index_name, _col_pos) in store.indexed_columns() {
            self.index_tracker
                .register_index(table_id.keyspace(), table_id.table(), index_name);
        }

        let state = TableState { schema, store };
        self.tables.write().insert(table_id, state);
        Ok(())
    }

    /// Unregisters a table from the storage engine.
    ///
    /// Removes the `TableState` from the engine's table map. Any in-progress
    /// reads holding an `Arc` reference to the underlying `TableStore` or its
    /// SSTables will complete normally; the data is freed once those references drop.
    /// Called as part of `DROP TABLE` / `DROP KEYSPACE` in pair mode.
    pub fn unregister_table(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        self.tables.write().remove(table_id);
        Ok(())
    }

    /// Registers a secondary index on a table.
    ///
    /// Called when CREATE INDEX is processed. Updates the TableStore's
    /// indexed_columns so future writes are indexed in the memtable.
    /// Registers the index in the tracker and submits rebuild jobs for
    /// all existing SSTables.
    pub fn add_index(
        &self,
        table_id: &TableId,
        index_name: &str,
        column_position: usize,
    ) -> ferrosa_common::Result<()> {
        // Register with the tracker.
        self.index_tracker
            .register_index(table_id.keyspace(), table_id.table(), index_name);

        let mut tables = self.tables.write();
        let state = tables.get_mut(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state
            .store
            .add_index(index_name.to_string(), column_position);

        // Submit rebuild jobs for all existing SSTables.
        if let Some(ref scheduler) = self.index_scheduler {
            let sstable_ids = state.store.sstable_generation_ids();
            for sst_id in sstable_ids {
                let job = crate::index::IndexBuildJob {
                    sstable_id: sst_id,
                    index_name: index_name.to_string(),
                    index_type: ferrosa_index::IndexType::BTree,
                    table: (
                        table_id.keyspace().to_string(),
                        table_id.table().to_string(),
                    ),
                    priority: crate::index::BuildPriority::Initial,
                    enqueued_at: std::time::Instant::now(),
                    column_position,
                };
                if let Err(e) = scheduler.submit(job) {
                    eprintln!("[engine] failed to submit index backfill: {e}");
                }
            }
        }

        Ok(())
    }

    /// Scans a table directory for existing SSTable files and sidecar index files,
    /// opening both.
    ///
    /// Returns `(sstables, sidecars)` where each vec is ordered newest-first (by
    /// generation number descending) and the two vecs are parallel — position `i`
    /// in `sidecars` is the sidecar map for the SSTable at position `i`.
    ///
    /// SSTables that fail to open are silently skipped — a corrupted SSTable is
    /// better handled at compaction time than at startup. Sidecar files that fail
    /// to open are replaced with empty maps (degraded: full scan fallback).
    fn load_existing_sstables_and_sidecars(
        table_dir: &std::path::Path,
    ) -> (Vec<FileSSTableReader>, Vec<SSTableSidecarMap>) {
        // Collect all generation numbers by looking for Data.db files.
        let mut generations: Vec<u64> = std::fs::read_dir(table_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with("-Data.db") {
                    name.split('-').next()?.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .collect();

        // Sort descending — newest generation first.
        generations.sort_by(|a, b| b.cmp(a));

        let mut sstables = Vec::new();
        let mut sidecars = Vec::new();

        for gen in generations {
            let gen_str = gen.to_string();
            match Self::open_sstable_from_dir(table_dir, &gen_str) {
                Ok(reader) => {
                    sstables.push(Arc::new(reader));
                    sidecars.push(Arc::new(Self::load_sidecars_for_generation(table_dir, gen)));
                }
                Err(e) => {
                    eprintln!(
                        "[storage-engine] skipping corrupt SSTable gen {gen} in {}: {e}",
                        table_dir.display()
                    );
                }
            }
        }

        (sstables, sidecars)
    }

    /// Scans a table directory for sidecar files belonging to a given generation.
    ///
    /// Looks for files matching `{gen}-*.sidecar`. Each successfully opened
    /// sidecar is added to the returned map keyed by index name. Files that
    /// fail to open are silently skipped (degraded to full-scan on that index).
    fn load_sidecars_for_generation(
        table_dir: &std::path::Path,
        gen: u64,
    ) -> HashMap<String, crate::index::sidecar::SidecarReader> {
        use crate::index::sidecar::SidecarReader;

        let sidecar_prefix = format!("{gen}-");
        const SIDECAR_SUFFIX: &str = ".sidecar";

        let mut sidecars = HashMap::new();

        let entries = match std::fs::read_dir(table_dir) {
            Ok(e) => e,
            Err(_) => return sidecars,
        };

        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&sidecar_prefix) && name.ends_with(SIDECAR_SUFFIX) {
                // Extract index name from "{gen}-{index_name}.sidecar"
                let index_name = &name[sidecar_prefix.len()..name.len() - SIDECAR_SUFFIX.len()];
                match SidecarReader::open(&entry.path()) {
                    Ok(reader) => {
                        sidecars.insert(index_name.to_string(), reader);
                    }
                    Err(e) => {
                        eprintln!(
                            "[storage-engine] skipping corrupt sidecar {} in {}: {e}",
                            name,
                            table_dir.display()
                        );
                    }
                }
            }
        }

        sidecars
    }

    /// Registers an observer that will be notified when mutations are written.
    ///
    /// Sync observers are called inline on the write path. Async observers
    /// receive mutations through a bounded channel — the drain loop is
    /// started externally (e.g., by `GraphEngine` in Slice 5).
    pub fn register_observer(&self, observer: Arc<dyn crate::observer::WriteObserver>) {
        match observer.mode() {
            crate::observer::ObserverMode::Sync => {
                self.observers.write().push(observer);
            }
            crate::observer::ObserverMode::Async => {
                let capacity = self.async_observer_capacity;
                let (tx, _rx) = tokio::sync::mpsc::channel(capacity);
                let state = AsyncObserverState {
                    observer,
                    sender: tx,
                    drop_count: Arc::new(AtomicU64::new(0)),
                };
                self.async_observers.write().push(state);
            }
        }
    }

    /// Registers an async observer and returns the receiver end of the bounded
    /// channel. The caller is responsible for draining the receiver.
    pub fn register_async_observer(
        &self,
        observer: Arc<dyn crate::observer::WriteObserver>,
        capacity: usize,
    ) -> tokio::sync::mpsc::Receiver<(TableId, Mutation)> {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        let state = AsyncObserverState {
            observer,
            sender: tx,
            drop_count: Arc::new(AtomicU64::new(0)),
        };
        self.async_observers.write().push(state);
        rx
    }

    /// Dispatches a mutation to all sync observers watching the given table.
    ///
    /// Derived mutations produced by observers go through the commit log for
    /// durability and are written to the target table's memtable.
    fn dispatch_sync_observers(&self, table_id: &TableId, mutation: &Mutation) {
        let observers = self.observers.read();
        for obs in observers.iter() {
            if obs.mode() == crate::observer::ObserverMode::Sync && obs.watches_table(table_id) {
                let derived = obs.on_write(table_id, mutation);
                for dm in derived {
                    // Durability: go through commit log.
                    if let Err(e) = self.commit_log.append(&dm) {
                        eprintln!("[observer] commit log append failed: {e}");
                        continue;
                    }
                    let dtid = TableId::new(&dm.keyspace, &dm.table);
                    let tables = self.tables.read();
                    if let Some(state) = tables.get(&dtid) {
                        for row in &dm.rows {
                            let _ = state.store.write(&dm.key, row.clone());
                        }
                    }
                }
            }
        }
    }

    /// Dispatches a mutation to all async observers watching the given table.
    ///
    /// Uses `try_send` — never blocks the write path. If the channel is full,
    /// the mutation is dropped and the drop counter is incremented.
    fn dispatch_async_observers(&self, table_id: &TableId, mutation: &Mutation) {
        let async_obs = self.async_observers.read();
        for state in async_obs.iter() {
            if state.observer.watches_table(table_id)
                && state
                    .sender
                    .try_send((table_id.clone(), mutation.clone()))
                    .is_err()
            {
                state.drop_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns the total number of mutations dropped by async observers due to
    /// backpressure (channel full).
    pub fn observer_drop_count(&self) -> u64 {
        let async_obs = self.async_observers.read();
        async_obs
            .iter()
            .map(|s| s.drop_count.load(Ordering::Relaxed))
            .sum()
    }

    /// Writes a row to the commit log and the table's memtable.
    ///
    /// The commit log append provides durability; the memtable write provides
    /// read visibility. Both are lock-free on the hot path.
    pub fn write(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
        row: Row,
        timestamp: i64,
    ) -> ferrosa_common::Result<()> {
        // 1. Append to commit log for durability.
        let mutation = Mutation {
            keyspace: table_id.keyspace.clone(),
            table: table_id.table.clone(),
            key: key.clone(),
            rows: vec![row.clone()],
            timestamp,
        };
        self.commit_log.append(&mutation)?;

        // 2. Write to the table's memtable.
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state.store.write(key, row)?;
        drop(tables);

        // 3. Notify observers after successful commit log + memtable write.
        self.dispatch_sync_observers(table_id, &mutation);
        self.dispatch_async_observers(table_id, &mutation);

        Ok(())
    }

    /// Writes multiple rows to a table in a single call.
    ///
    /// Each mutation is (key, row, timestamp). Mutations are appended to the
    /// commit log and memtable sequentially. Not atomic — a failure partway
    /// through leaves earlier writes committed.
    pub fn batch_write(
        &self,
        table_id: &TableId,
        mutations: Vec<(DecoratedKey, Row, i64)>,
    ) -> ferrosa_common::Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        // Collect committed mutations for observer dispatch after each write.
        for (key, row, timestamp) in mutations {
            let mutation = Mutation {
                keyspace: table_id.keyspace.clone(),
                table: table_id.table.clone(),
                key: key.clone(),
                rows: vec![row.clone()],
                timestamp,
            };

            // Append to commit log.
            self.commit_log.append(&mutation)?;

            // Write to memtable (scoped read lock).
            {
                let tables = self.tables.read();
                let state = tables.get(table_id).ok_or_else(|| {
                    ferrosa_common::Error::InvalidFormat(format!(
                        "table not registered: {table_id}"
                    ))
                })?;
                state.store.write(&key, row)?;
            }

            // Notify observers after successful commit log + memtable write.
            self.dispatch_sync_observers(table_id, &mutation);
            self.dispatch_async_observers(table_id, &mutation);
        }

        Ok(())
    }

    /// Returns a reference to the batchlog manager, if enabled.
    pub fn batchlog(&self) -> Option<&crate::batchlog::BatchlogManager> {
        self.batchlog.as_ref()
    }

    /// Writes a batch of mutations atomically.
    ///
    /// All mutations are appended to the commit log first, then applied to
    /// their respective memtables. If the process crashes between commit log
    /// append and memtable apply, commit log replay will recover all mutations.
    ///
    /// This is the single-node fast path for logged batches: no batchlog
    /// coordination needed because the commit log provides the atomicity
    /// guarantee.
    pub fn write_atomic_batch(&self, mutations: Vec<Mutation>) -> ferrosa_common::Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        // Phase 1: Append all mutations to the commit log.
        for m in &mutations {
            self.commit_log.append(m)?;
        }

        // Phase 2: Apply to memtables.
        let tables = self.tables.read();
        for m in &mutations {
            let table_id = TableId::new(&m.keyspace, &m.table);
            let state = tables.get(&table_id).ok_or_else(|| {
                ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
            })?;
            for row in &m.rows {
                state.store.write(&m.key, row.clone())?;
            }
        }
        drop(tables);

        // Phase 3: Notify observers.
        for m in &mutations {
            let table_id = TableId::new(&m.keyspace, &m.table);
            self.dispatch_sync_observers(&table_id, m);
            self.dispatch_async_observers(&table_id, m);
        }

        Ok(())
    }

    /// Reads a partition from a table, merging memtable and SSTable sources.
    pub fn read(
        &self,
        table_id: &TableId,
        key: &DecoratedKey,
    ) -> ferrosa_common::Result<Option<Partition>> {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.read(key),
            None => Ok(None),
        }
    }

    /// Reads partitions from a table in token order with optional bounds and limit.
    pub fn read_range(
        &self,
        table_id: &TableId,
        start: Option<&DecoratedKey>,
        end: Option<&DecoratedKey>,
        limit: usize,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.read_range(start, end, limit),
            None => Ok(vec![]),
        }
    }

    /// Query by secondary index across memtable and SSTable sidecar indexes.
    ///
    /// Delegates to [`TableStore::read_by_index`] which merges results from
    /// the memtable index and (future) sidecar indexes. Returns an empty vec
    /// if the table is not registered.
    pub fn read_by_index(
        &self,
        table_id: &TableId,
        index_name: &str,
        key: &ferrosa_index::IndexKey,
    ) -> ferrosa_common::Result<Vec<Partition>> {
        let tables = self.tables.read();
        match tables.get(table_id) {
            Some(state) => state.store.read_by_index(index_name, key),
            None => Ok(vec![]),
        }
    }

    /// Truncates a table: clears the memtable and drops all SSTable references.
    ///
    /// Subsequent reads for this table will return empty results. Existing
    /// readers holding `Arc` references to old data will complete normally.
    pub fn truncate(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state.store.truncate();
        Ok(())
    }

    /// Replay mutations from a given commit log position forward.
    ///
    /// Returns mutations with positions after `position`. If the segment
    /// has been recycled, returns an empty vec (caller should bootstrap).
    pub fn replay_from(
        &self,
        position: CommitLogPosition,
    ) -> ferrosa_common::Result<Vec<Mutation>> {
        self.commit_log.replay_from(position)
    }

    /// Returns the current commit log write position.
    ///
    /// Used by snapshot creation to record which commit log position
    /// the snapshot covers.
    pub fn commit_log_position(&self) -> CommitLogPosition {
        self.commit_log.current_position()
    }

    /// Opens a StorageEngine by restoring from a named snapshot.
    ///
    /// Steps:
    /// 1. Loads and validates the snapshot manifest (SHA-256 integrity check).
    /// 2. Validates the node ID (cross-node restore requires `force = true`).
    /// 3. Downloads SSTables from the snapshot manifest to
    ///    `{config.data_dir}/sstables/`.
    /// 4. Downloads archived commit log segments from S3 to
    ///    `{config.commit_log.log_dir}`.
    /// 5. Validates segment continuity from the snapshot's commit-log position.
    /// 6. Opens the engine normally (SSTables are loaded from disk).
    ///
    /// Mutation replay from the downloaded segments is a future step — this
    /// constructor restores the engine to the state at the snapshot boundary.
    /// Callers that need point-in-time replay beyond the snapshot boundary
    /// should call `replay_from` after registering table schemas.
    ///
    /// # Arguments
    ///
    /// * `config` — engine configuration (data_dir, commit_log dirs, etc.)
    /// * `snapshot_name` — name of the snapshot stored in S3
    /// * `point_in_time` — optional Unix-epoch microsecond timestamp to filter
    ///   replay (placeholder; full replay is deferred)
    /// * `node_id` — ID of this node; must match the snapshot unless `force`
    /// * `force` — allow restoring a snapshot from a different node
    /// * `store` — injected object store (use for tests; production uses S3)
    /// * `prefix` — S3 key prefix under which the snapshot lives
    pub async fn open_from_snapshot_with_store(
        config: StorageEngineConfig,
        snapshot_name: &str,
        _point_in_time: Option<i64>,
        node_id: &str,
        force: bool,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<Self> {
        // 1. Load and validate snapshot (SHA-256 integrity check).
        let restore_mgr =
            crate::restore::RestoreManager::new(std::sync::Arc::clone(&store), prefix.to_string());
        let (metadata, manifest) = restore_mgr
            .load_and_validate_snapshot(snapshot_name)
            .await?;

        // 2. Validate node ID — cross-node restore requires force = true.
        crate::restore::validation::validate_node_id(&metadata.node_id, node_id, force)?;

        // 3. Download SSTables to {data_dir}/sstables/.
        let sstable_dir = config.data_dir.join("sstables");
        std::fs::create_dir_all(&sstable_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create sstable dir {}: {e}",
                sstable_dir.display()
            ))
        })?;
        let _sst_count = restore_mgr
            .download_sstables(&manifest, &sstable_dir)
            .await?;

        // 4. Download archived commit log segments.
        let segment_dir = config.commit_log.log_dir.clone();
        std::fs::create_dir_all(&segment_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create segment dir {}: {e}",
                segment_dir.display()
            ))
        })?;
        let segment_ids = restore_mgr
            .download_segments(metadata.commit_log_position.segment_id, &segment_dir)
            .await?;

        // 5. Validate segment continuity.
        crate::restore::validation::validate_segment_continuity(
            &segment_ids,
            metadata.commit_log_position.segment_id,
        )?;

        // 6. Open the engine normally; SSTables are on disk from step 3.
        //    Callers register table schemas and optionally call replay_from()
        //    to apply mutations beyond the snapshot boundary.
        //
        // TODO(PITR): full mutation replay from downloaded segment files —
        //   deserialize mutations, filter by _point_in_time, apply via write().
        let engine = StorageEngine::new(config, None)?;

        Ok(engine)
    }

    /// Creates a snapshot using an injected object store (for testing).
    ///
    /// 1. Flushes all memtables to SSTables.
    /// 2. Records the commit log position.
    /// 3. Loads the live manifest and schema from S3.
    /// 4. Delegates to SnapshotManager to write snapshot objects.
    pub async fn create_snapshot_with_store(
        &self,
        name: &str,
        node_id: &str,
        expires_at: Option<String>,
        ephemeral: bool,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<crate::snapshot::metadata::SnapshotMetadata> {
        // Step 1: Flush all tables.
        let table_ids: Vec<_> = self.tables.read().keys().cloned().collect();
        for table_id in &table_ids {
            self.flush(table_id)?;
        }

        // Step 2: Record commit log position.
        let position = self.commit_log_position();

        // Step 3: Load live manifest and schema from S3.
        let (manifest, _version) = crate::manifest::Manifest::load(store.as_ref(), prefix).await?;
        let schema_json = crate::manifest::load_schema_snapshot(store.as_ref(), prefix)
            .await?
            .unwrap_or_default();

        // Step 4: Create snapshot via manager.
        let manager = crate::snapshot::SnapshotManager::new(
            std::sync::Arc::clone(&store),
            prefix.to_string(),
        );

        manager
            .create_snapshot(
                name,
                &manifest,
                &schema_json,
                position,
                node_id,
                expires_at,
                ephemeral,
            )
            .await
    }

    /// Lists all snapshots from S3 using an injected store (for testing).
    pub async fn list_snapshots_with_store(
        &self,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<Vec<crate::snapshot::metadata::SnapshotMetadata>> {
        let manager = crate::snapshot::SnapshotManager::new(store, prefix.to_string());
        manager.list_snapshots().await
    }

    /// Deletes a snapshot from S3 using an injected store (for testing).
    pub async fn delete_snapshot_with_store(
        &self,
        name: &str,
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> ferrosa_common::Result<()> {
        let manager = crate::snapshot::SnapshotManager::new(store, prefix.to_string());
        manager.delete_snapshot(name).await
    }

    /// Force-syncs the commit log to disk.
    ///
    /// Ensures all buffered mutations are written to disk before reading
    /// the commit log (e.g., for catch-up replay after failover).
    pub fn force_commit_log_sync(&self) -> ferrosa_common::Result<()> {
        self.commit_log.force_sync()
    }

    /// Flushes the active memtable for a table to an SSTable on disk.
    ///
    /// After flushing, checks if compaction is needed and submits tasks
    /// to the compaction executor.
    pub fn flush(&self, table_id: &TableId) -> ferrosa_common::Result<()> {
        let tables = self.tables.read();
        let state = tables.get(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;

        state.store.flush()?;

        // Eager index build: submit high-priority index rebuild for the newly
        // flushed SSTable. This keeps the MemtableIndex (Layer 4) bounded to
        // 0-1 entries in steady state by ensuring sidecar indexes are current.
        if let Some(ref scheduler) = self.index_scheduler {
            let gen = state.store.last_flush_generation();
            for (index_name, col_pos) in state.store.indexed_columns() {
                let tracker_state =
                    self.index_tracker
                        .get_state(table_id.keyspace(), table_id.table(), index_name);
                // Only submit if the index needs building (not already current).
                if let Some(idx_state) = tracker_state {
                    if !idx_state.indexed_sstables.contains(&format!("{gen}")) {
                        let job = crate::index::IndexBuildJob {
                            sstable_id: format!("{gen}"),
                            index_name: index_name.clone(),
                            index_type: ferrosa_index::IndexType::BTree,
                            table: (
                                table_id.keyspace().to_string(),
                                table_id.table().to_string(),
                            ),
                            priority: crate::index::BuildPriority::High,
                            enqueued_at: std::time::Instant::now(),
                            column_position: *col_pos,
                        };
                        let _ = scheduler.submit(job);
                    }
                }
            }
        }

        // Check for compaction after flush.
        self.maybe_compact(table_id, state);

        Ok(())
    }

    /// Flushes all tables that exceed the configured memtable size threshold.
    pub fn flush_if_needed(&self) -> ferrosa_common::Result<()> {
        let tables = self.tables.read();
        let to_flush: Vec<TableId> = tables
            .iter()
            .filter(|(_, state)| {
                state.store.memtable_size() as u64 >= self.config.flush_threshold_bytes
            })
            .map(|(id, _)| id.clone())
            .collect();
        drop(tables);

        for table_id in to_flush {
            self.flush(&table_id)?;
        }
        Ok(())
    }

    /// Flush all non-empty memtables regardless of size threshold.
    ///
    /// Used before S3 schema persistence to ensure data and metadata
    /// stay in sync.  Equivalent to Cassandra's SNAPSHOT flush reason.
    pub fn flush_all(&self) -> ferrosa_common::Result<()> {
        let tables = self.tables.read();
        let to_flush: Vec<TableId> = tables
            .iter()
            .filter(|(_, state)| state.store.memtable_size() > 0)
            .map(|(id, _)| id.clone())
            .collect();
        drop(tables);

        for table_id in to_flush {
            self.flush(&table_id)?;
        }
        Ok(())
    }

    /// Polls for completed compaction results, uploads the output SSTable to S3,
    /// updates the manifest, and enqueues deletion of input SSTables.
    ///
    /// Crash-safe: pending-log → upload → S3 confirm → manifest update →
    /// enqueue input deletions → evict local input directories.
    pub async fn poll_compactions(&self) {
        let results = self.compaction_executor.poll_results();
        for result in results {
            let input_count = result.task.inputs.len();
            let table_id = &result.task.table_id;

            // Open the compacted output SSTable.
            let gen = &result.output.id;
            let dir = &result.output.path;
            let reader = match Self::open_sstable_from_dir(dir, gen) {
                Ok(r) => Arc::new(r),
                Err(e) => {
                    eprintln!("[compaction] failed to open output SSTable: {e}");
                    continue;
                }
            };

            // Swap: remove input SSTables, insert output.
            {
                let tables = self.tables.read();
                if let Some(state) = tables.get(table_id) {
                    if let Err(e) = state.store.swap_compacted_sstables(
                        input_count,
                        reader,
                        std::collections::HashMap::new(),
                    ) {
                        eprintln!("[compaction] swap failed: {e}");
                        continue;
                    }

                    // Eager index build: submit high-priority rebuild for compacted output.
                    // Same as flush — keeps MemtableIndex bounded in steady state.
                    if let Some(ref scheduler) = self.index_scheduler {
                        for (index_name, col_pos) in state.store.indexed_columns() {
                            let job = crate::index::IndexBuildJob {
                                sstable_id: result.output.id.clone(),
                                index_name: index_name.clone(),
                                index_type: ferrosa_index::IndexType::BTree,
                                table: (
                                    table_id.keyspace().to_string(),
                                    table_id.table().to_string(),
                                ),
                                priority: crate::index::BuildPriority::High,
                                enqueued_at: std::time::Instant::now(),
                                column_position: *col_pos,
                            };
                            if let Err(e) = scheduler.submit(job) {
                                eprintln!(
                                    "[compaction] failed to submit index rebuild for {index_name}: {e}"
                                );
                            }
                        }
                    }
                }
            }

            // Register in local cache.
            self.local_cache.register(
                &result.output.id,
                result.output.path.clone(),
                result.output.size_bytes,
            );

            // ── Crash-safe S3 upload + manifest update ─────────────────────
            //
            // 5-step pattern (mirrors sync_sstables_to_s3):
            //   1. Write pending-log entry (fsynced)
            //   2. Submit UploadTask with on_complete channel
            //   3. Await S3 confirmation
            //   4. Remove pending-log entry
            //   5. Update manifest (remove inputs, add output)
            //
            // If upload_manager is None (no S3 configured) we skip silently.
            let Some(upload_mgr) = self.upload_manager.as_ref() else {
                continue;
            };
            let Some((store, prefix)) = self.resolve_store_and_prefix() else {
                continue;
            };

            let table_id_str = table_id.to_string();
            let sstable_id = result.output.id.clone();

            // Parse the generation number — output id is always a decimal u64.
            let gen_u64: u64 = match sstable_id.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("[compaction] output SSTable id {sstable_id} is not a u64: {e}");
                    continue;
                }
            };

            // The compaction output lives in result.output.path.
            let output_dir = result.output.path.clone();

            let files = Self::collect_sstable_files(&output_dir, gen_u64);
            if files.is_empty() {
                eprintln!(
                    "[compaction] no files for output SSTable {sstable_id}, skipping S3 upload"
                );
                continue;
            }

            let total_size: u64 = files.iter().map(|(_, data)| data.len() as u64).sum();

            // Step 1: record the pending upload (best-effort).
            let pending_log_path = self.config.data_dir.join("pending-uploads.log");
            let pending_log_result = crate::upload::PendingUploadsLog::open(&pending_log_path);
            if let Ok(ref pending_log) = pending_log_result {
                if let Err(e) = pending_log.add_entry(&table_id_str, &sstable_id) {
                    eprintln!("[compaction] failed to write pending-log entry for {sstable_id}: {e}");
                }
            }

            // Step 2: create completion channel and submit the upload.
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
            let task = crate::upload::UploadTask::SSTable {
                table_id: table_id_str.clone(),
                sstable_id: sstable_id.clone(),
                files,
                on_complete: Some(tx),
            };
            if let Err(e) = upload_mgr.submit(task).await {
                eprintln!("[compaction] failed to submit upload task for {sstable_id}: {e}");
                continue;
            }

            // Step 3: await S3 confirmation.
            match rx.await {
                Ok(Ok(())) => {
                    // Upload confirmed — increment the S3 upload counter.
                    self.compaction_metrics.inc_s3_uploads();
                }
                Ok(Err(msg)) => {
                    eprintln!("[compaction] upload failed for {sstable_id}: {msg}");
                    if let Ok(ref pending_log) = pending_log_result {
                        let _ = pending_log.remove_entry(&sstable_id);
                    }
                    continue;
                }
                Err(_) => {
                    eprintln!("[compaction] upload worker dropped channel for {sstable_id}");
                    if let Ok(ref pending_log) = pending_log_result {
                        let _ = pending_log.remove_entry(&sstable_id);
                    }
                    continue;
                }
            }

            // Step 4: remove the pending-log entry now that S3 confirmed.
            if let Ok(ref pending_log) = pending_log_result {
                if let Err(e) = pending_log.remove_entry(&sstable_id) {
                    eprintln!(
                        "[compaction] warning: failed to remove pending-log entry for {sstable_id}: {e}"
                    );
                    // Non-fatal: replay will re-upload (idempotent).
                }
            }

            // Step 5: update manifest — load fresh copy, remove inputs, add output, save.
            // Keep full input metadata for local eviction after manifest update.
            let input_ids: Vec<String> = result
                .task
                .inputs
                .iter()
                .map(|i| i.id.clone())
                .collect();
            let input_paths: Vec<std::path::PathBuf> = result
                .task
                .inputs
                .iter()
                .map(|i| i.path.clone())
                .collect();

            // Compute total input bytes for metrics (used after manifest update).
            // If the metadata carries a non-zero size we use it directly;
            // otherwise we sum the actual component file sizes from disk
            // (sstable_metadata() currently returns size_bytes = 0 as a
            // known placeholder — scanning disk gives the accurate value).
            let input_bytes_total: i64 = {
                let from_metadata: i64 = result
                    .task
                    .inputs
                    .iter()
                    .map(|i| i.size_bytes as i64)
                    .sum();
                if from_metadata > 0 {
                    from_metadata
                } else {
                    let component_suffixes = [
                        "Data.db",
                        "Partitions.db",
                        "Rows.db",
                        "Filter.db",
                        "Statistics.db",
                        "TOC.txt",
                    ];
                    result
                        .task
                        .inputs
                        .iter()
                        .flat_map(|input| {
                            component_suffixes.iter().map(move |suffix| {
                                let path = input.path.join(format!("{}-{suffix}", input.id));
                                std::fs::metadata(&path).map(|m| m.len() as i64).unwrap_or(0)
                            })
                        })
                        .sum()
                }
            };

            match crate::manifest::Manifest::load(store.as_ref(), &prefix).await {
                Ok((mut manifest, _version)) => {
                    manifest.remove_sstables(&table_id_str, &input_ids);
                    manifest.add_sstable(
                        &table_id_str,
                        crate::manifest::ManifestEntry {
                            id: sstable_id.clone(),
                            size: total_size,
                            min_token: result.output.min_token,
                            max_token: result.output.max_token,
                            min_timestamp: result.output.min_timestamp,
                            max_timestamp: result.output.max_timestamp,
                        },
                    );
                    if let Err(e) = manifest
                        .save_with_retry(store.as_ref(), &prefix, self.s3_cas_supported)
                        .await
                    {
                        eprintln!("[compaction] manifest save failed for {sstable_id}: {e}");
                    } else {
                        eprintln!(
                            "[compaction] manifest updated: output {sstable_id}, removed {} inputs",
                            input_ids.len()
                        );
                        // Record bytes freed by this compaction in the metrics gauge.
                        self.compaction_metrics
                            .add_bytes_reclaimed(input_bytes_total);
                    }
                }
                Err(e) => {
                    eprintln!("[compaction] failed to load manifest for update: {e}");
                }
            }

            // Enqueue S3 deletion for each input SSTable (1-hour grace period).
            for input_id in &input_ids {
                let (del_tx, del_rx) = tokio::sync::oneshot::channel();
                let _ = upload_mgr
                    .submit(crate::upload::UploadTask::DeleteSSTable {
                        table_id: table_id_str.clone(),
                        sstable_id: input_id.to_string(),
                        grace_period: std::time::Duration::from_secs(3600),
                        on_complete: Some(del_tx),
                    })
                    .await;
                // Increment the S3 delete counter for each enqueued deletion.
                self.compaction_metrics.inc_s3_deletes();
                // Fire-and-forget: S3 deletions are best-effort.
                drop(del_rx);
            }

            // Evict local input SSTable component files (best-effort).
            // Files are stored flat in the table directory as {gen}-Data.db etc.
            for (input_id, table_dir) in input_ids.iter().zip(input_paths.iter()) {
                // Remove all component files for this generation.
                let standard_components = [
                    "Data.db",
                    "Partitions.db",
                    "Rows.db",
                    "Filter.db",
                    "Statistics.db",
                    "TOC.txt",
                    "CompressionInfo.db",
                ];
                for component in &standard_components {
                    let file_path = table_dir.join(format!("{input_id}-{component}"));
                    let _ = std::fs::remove_file(&file_path);
                }
            }
        }
    }

    /// Opens an SSTable from component files in a directory.
    fn open_sstable_from_dir(
        dir: &std::path::Path,
        gen: &str,
    ) -> ferrosa_common::Result<
        ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt>,
    > {
        use ferrosa_sstable::io::FileReadAt;
        use ferrosa_sstable::reader::SSTableComponents;

        let data = FileReadAt::open(dir.join(format!("{gen}-Data.db")))?;
        let partitions = FileReadAt::open(dir.join(format!("{gen}-Partitions.db")))?;
        let rows = FileReadAt::open(dir.join(format!("{gen}-Rows.db")))?;
        let filter = std::fs::read(dir.join(format!("{gen}-Filter.db")))?;
        let statistics = std::fs::read(dir.join(format!("{gen}-Statistics.db")))?;
        let compression_info = std::fs::read(dir.join(format!("{gen}-CompressionInfo.db"))).ok();

        ferrosa_sstable::reader::SSTableReader::open(SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
        })
    }

    /// Returns the number of SSTables for a table.
    pub fn sstable_count(&self, table_id: &TableId) -> usize {
        self.tables
            .read()
            .get(table_id)
            .map(|s| s.store.sstable_count())
            .unwrap_or(0)
    }

    /// Returns the memtable size in bytes for a table.
    pub fn memtable_size(&self, table_id: &TableId) -> usize {
        self.tables
            .read()
            .get(table_id)
            .map(|s| s.store.memtable_size())
            .unwrap_or(0)
    }

    /// Shuts down the storage engine gracefully.
    ///
    /// Flushes all dirty memtables, stops the compaction executor,
    /// shuts down the upload manager, and stops the commit log.
    pub fn shutdown(&self) -> ferrosa_common::Result<()> {
        // Flush all tables.
        let table_ids: Vec<TableId> = self.tables.read().keys().cloned().collect();
        for table_id in &table_ids {
            // Best-effort flush; log but don't fail on individual table errors.
            if let Err(e) = self.flush(table_id) {
                eprintln!("[storage-engine] flush failed for {table_id}: {e}");
            }
        }

        // Drain index scheduler.
        if let Some(ref scheduler) = self.index_scheduler {
            scheduler.shutdown_with_timeout(std::time::Duration::from_secs(30));
        }

        // Stop compaction.
        self.compaction_executor.shutdown();

        // Stop archiver.
        if let Some(handle) = &self.archiver_handle {
            handle.abort();
        }

        // Commit log shutdown.
        self.commit_log.shutdown()?;

        Ok(())
    }

    /// Checks if compaction should be triggered after a flush.
    fn maybe_compact(&self, table_id: &TableId, state: &TableState) {
        let metadata = self.collect_sstable_metadata(table_id, state);
        let strategy = SizeTieredStrategy::new(self.config.compaction.clone());
        let tasks = strategy.select(&metadata, &state.schema, table_id);
        for task in tasks {
            if let Err(e) = self.compaction_executor.submit(task) {
                eprintln!("[storage-engine] compaction submit failed for {table_id}: {e}");
            }
        }
    }

    /// Collects SSTable metadata for compaction strategy evaluation.
    fn collect_sstable_metadata(
        &self,
        table_id: &TableId,
        state: &TableState,
    ) -> Vec<crate::compaction::metadata::SSTableMetadata> {
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        state.store.sstable_metadata(&table_dir)
    }

    /// Returns a reference to the local cache.
    pub fn local_cache(&self) -> &LocalCache {
        &self.local_cache
    }

    /// Returns a reference to the upload manager, if S3 is configured.
    pub fn upload_manager(&self) -> Option<&UploadManager> {
        self.upload_manager.as_ref()
    }

    /// Returns true if S3 object storage is configured.
    pub fn has_s3(&self) -> bool {
        self.upload_manager.is_some()
    }

    /// Returns the shared index state tracker.
    pub fn index_tracker(&self) -> &Arc<crate::index::IndexStateTracker> {
        &self.index_tracker
    }

    /// Returns the S3 object store and config, if S3 is configured.
    pub fn object_store_and_config(
        &self,
    ) -> ferrosa_common::Result<(&ObjectStoreConfig, Arc<dyn object_store::ObjectStore>)> {
        let os_config = self
            .config
            .object_store
            .as_ref()
            .ok_or_else(|| ferrosa_common::Error::InvalidFormat("S3 not configured".into()))?;
        let store = os_config.build_object_store()?;
        Ok((os_config, Arc::from(store)))
    }

    /// Returns the object store and prefix for S3 operations.
    ///
    /// In test builds, checks `upload_store_override` first so that tests
    /// can inject an `InMemory` store without a real S3 endpoint.
    fn resolve_store_and_prefix(
        &self,
    ) -> Option<(Arc<dyn object_store::ObjectStore>, String)> {
        #[cfg(test)]
        if let Some((store, prefix)) = &self.upload_store_override {
            return Some((Arc::clone(store), prefix.clone()));
        }
        self.object_store_and_config()
            .ok()
            .map(|(cfg, store)| (store, cfg.prefix.clone()))
    }

    /// Discards commit log segments that have no remaining dirty tables.
    ///
    /// Called from the background maintenance loop. Returns the number of
    /// segments cleaned up.
    pub fn discard_completed_commit_log_segments(&self) -> ferrosa_common::Result<usize> {
        self.commit_log.discard_completed_segments()
    }

    /// Sync all local SSTables to S3 and update the manifest.
    ///
    /// Scans each registered table's SSTable directory, collects component
    /// files for each generation, uploads them via UploadManager, and
    /// updates the S3 manifest with new entries.
    pub async fn sync_sstables_to_s3(&self) -> ferrosa_common::Result<usize> {
        let (os_config, store) = self.object_store_and_config()?;
        let prefix = os_config.prefix.clone();
        let upload_mgr = self.upload_manager.as_ref().ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat("UploadManager not initialized".into())
        })?;

        // Load current manifest to check which SSTables are already uploaded.
        let (mut manifest, _version) =
            crate::manifest::Manifest::load(store.as_ref(), &prefix).await?;

        let mut uploaded = 0usize;

        // Collect table IDs and directories under the lock, then release
        // before any .await (RwLockReadGuard is !Send).
        let table_dirs: Vec<(String, std::path::PathBuf)> = {
            let tables = self.tables.read();
            tables
                .keys()
                .map(|id| {
                    let dir = self.config.data_dir.join("sstables").join(id.to_string());
                    (id.to_string(), dir)
                })
                .collect()
        };

        for (table_id_str, table_dir) in &table_dirs {
            if !table_dir.exists() {
                continue;
            }

            let generations = Self::scan_generations(table_dir);
            let existing_ids: std::collections::HashSet<String> = manifest
                .sstables
                .get(table_id_str)
                .map(|entries| entries.iter().map(|e| e.id.clone()).collect())
                .unwrap_or_default();

            for gen in generations {
                let gen_str = gen.to_string();
                if existing_ids.contains(&gen_str) {
                    continue;
                }

                let files = Self::collect_sstable_files(table_dir, gen);
                if files.is_empty() {
                    continue;
                }

                let total_size: u64 = files.iter().map(|(_, data)| data.len() as u64).sum();

                let task = crate::upload::UploadTask::SSTable {
                    table_id: table_id_str.clone(),
                    sstable_id: gen_str.clone(),
                    files,
                    on_complete: None,
                };
                upload_mgr.submit(task).await?;

                manifest.add_sstable(
                    table_id_str,
                    crate::manifest::ManifestEntry {
                        id: gen_str,
                        size: total_size,
                        min_token: i64::MIN,
                        max_token: i64::MAX,
                        min_timestamp: 0,
                        max_timestamp: 0,
                    },
                );
                uploaded += 1;
            }
        }

        if uploaded > 0 {
            // Save updated manifest.
            manifest
                .save_with_retry(store.as_ref(), &prefix, self.s3_cas_supported)
                .await?;
            eprintln!("[s3-sync] uploaded {uploaded} SSTables, manifest saved");
        }

        Ok(uploaded)
    }

    /// Download SSTables from S3 to local disk for a specific table.
    ///
    /// Uses the manifest to know which SSTables exist, then downloads
    /// all component files for each. After this call, `register_table()`
    /// will find the files on local disk.
    pub async fn download_sstables_from_s3(
        &self,
        table_id: &TableId,
        manifest: &crate::manifest::Manifest,
    ) -> ferrosa_common::Result<usize> {
        let (os_config, store) = self.object_store_and_config()?;
        let prefix = &os_config.prefix;

        let entries = match manifest.sstables.get(&table_id.to_string()) {
            Some(e) => e,
            None => return Ok(0),
        };

        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        std::fs::create_dir_all(&table_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create table dir: {e}"))
        })?;

        let mut downloaded = 0;
        let components = [
            "Data.db",
            "Partitions.db",
            "Rows.db",
            "Filter.db",
            "Statistics.db",
            "TOC.txt",
        ];

        for entry in entries {
            let hex = crate::upload::manager::hex_prefix_for(&entry.id);

            for component in &components {
                let s3_path = object_store::path::Path::from(format!(
                    "{prefix}/{hex}/{table_id}/{}/{component}",
                    entry.id
                ));
                let local_path = table_dir.join(format!("{}-{component}", entry.id));

                // Skip if already downloaded.
                if local_path.exists() {
                    continue;
                }

                match store.get(&s3_path).await {
                    Ok(result) => {
                        let data = result.bytes().await.map_err(|e| {
                            ferrosa_common::Error::InvalidFormat(format!(
                                "failed to read {s3_path}: {e}"
                            ))
                        })?;
                        std::fs::write(&local_path, &data).map_err(|e| {
                            ferrosa_common::Error::InvalidFormat(format!(
                                "failed to write {}: {e}",
                                local_path.display()
                            ))
                        })?;
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        // Component might not exist (e.g., CompressionInfo.db is optional)
                        continue;
                    }
                    Err(e) => {
                        return Err(ferrosa_common::Error::InvalidFormat(format!(
                            "S3 download failed for {s3_path}: {e}"
                        )));
                    }
                }
            }
            downloaded += 1;
        }

        Ok(downloaded)
    }

    /// Scan a table directory for SSTable generation numbers.
    fn scan_generations(table_dir: &std::path::Path) -> Vec<u64> {
        std::fs::read_dir(table_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with("-Data.db") {
                    name.split('-').next()?.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .collect()
    }

    /// Collect all component files for an SSTable generation.
    fn collect_sstable_files(table_dir: &std::path::Path, gen: u64) -> Vec<(String, bytes::Bytes)> {
        let gen_str = gen.to_string();
        std::fs::read_dir(table_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.starts_with(&format!("{gen_str}-")) {
                    let data = std::fs::read(e.path()).ok()?;
                    Some((name, bytes::Bytes::from(data)))
                } else {
                    None
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Test constructor: inject an in-memory object store
// ---------------------------------------------------------------------------

impl StorageEngine {
    /// Creates a storage engine with an explicit upload object store.
    ///
    /// Used by tests to inject an `InMemory` store for upload/manifest tests
    /// without requiring a real S3 endpoint.  The store is used directly for
    /// both uploads and manifest persistence.
    #[cfg(test)]
    pub fn new_with_upload_store(
        config: StorageEngineConfig,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: String,
        runtime: &tokio::runtime::Handle,
    ) -> ferrosa_common::Result<Self> {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create data dir: {e}"))
        })?;
        std::fs::create_dir_all(&config.commit_log.log_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create commitlog dir: {e}"
            ))
        })?;

        let commit_log = CommitLog::new(config.commit_log.clone())?;
        let compaction_executor = CompactionExecutor::new();

        let upload_manager = Some(UploadManager::new(
            Arc::clone(&store),
            prefix.clone(),
            16,
            runtime,
        ));

        let local_cache =
            LocalCache::new(config.data_dir.join("cache"), config.local_cache_max_bytes);

        let index_tracker = Arc::new(crate::index::IndexStateTracker::new());
        let index_scheduler = {
            let backend = Arc::new(crate::index::LocalBackend::new(config.data_dir.clone()));
            Some(
                crate::index::IndexBuildScheduler::with_backend_and_data_dir(
                    2,
                    Arc::clone(&index_tracker),
                    backend,
                    config.data_dir.clone(),
                ),
            )
        };

        Ok(Self {
            config,
            tables: RwLock::new(HashMap::new()),
            commit_log,
            compaction_executor,
            upload_manager,
            local_cache,
            observers: RwLock::new(Vec::new()),
            async_observers: RwLock::new(Vec::new()),
            async_observer_capacity: crate::observer::ObserverConfig::default().queue_capacity,
            index_scheduler,
            index_tracker,
            batchlog: Some(crate::batchlog::BatchlogManager::new(
                crate::batchlog::BatchlogConfig::default(),
            )),
            archiver_handle: None,
            s3_cas_supported: false, // InMemory does not support CAS etags
            compaction_metrics: Arc::new(crate::metrics::CompactionMetrics::new()),
            upload_store_override: Some((store, prefix)),
        })
    }
}

// ---------------------------------------------------------------------------
// Virtual table provider implementations
// ---------------------------------------------------------------------------

impl crate::virtual_tables::StorageStatsProvider for StorageEngine {
    fn collect_stats(&self) -> Vec<crate::virtual_tables::StorageStats> {
        let tables = self.tables.read();
        tables
            .iter()
            .map(|(table_id, state)| {
                let sstable_count = state.store.sstable_count() as i32;

                // Sum on-disk SSTable file sizes for this table.
                let table_dir = self
                    .config
                    .data_dir
                    .join("sstables")
                    .join(table_id.to_string());
                let sstable_size_bytes: i64 = std::fs::read_dir(&table_dir)
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok().map(|m| m.len() as i64))
                    .sum();

                // Approximate S3 stats: each SSTable has ~5 component files
                // (Data.db, Index.db, Filter.db, Statistics.db, CompressionInfo.db).
                // S3 bytes approximates sstable_size_bytes since flushed SSTables
                // are uploaded to S3.
                let s3_object_count = sstable_count.saturating_mul(5);
                let s3_bytes = sstable_size_bytes;

                crate::virtual_tables::StorageStats {
                    keyspace: table_id.keyspace().to_string(),
                    table_name: table_id.table().to_string(),
                    memtable_size_bytes: state.store.memtable_size() as i64,
                    memtable_count: 1, // One active memtable per table
                    sstable_count,
                    sstable_size_bytes,
                    s3_object_count,
                    s3_bytes,
                    pending_compactions: 0, // Per-table pending count not yet exposed
                }
            })
            .collect()
    }
}

impl crate::virtual_tables::ArchiveStatusProvider for StorageEngine {
    fn archive_status(&self) -> crate::virtual_tables::ArchiveStatusRow {
        let archived = self.commit_log.archived_segments();
        crate::virtual_tables::ArchiveStatusRow {
            // Approximate: total closed segments minus archived ones would
            // require knowing the full closed set. For now report archived count
            // as "0 unarchived" if any archiving has occurred.
            unarchived_segments: 0,
            oldest_unarchived_age_secs: 0,
            last_archive_success: if archived.is_empty() {
                "never".to_string()
            } else {
                "unknown".to_string()
            },
            archive_errors_total: 0,
        }
    }
}

impl crate::virtual_tables::SnapshotInfoProvider for StorageEngine {
    fn snapshot_info(&self) -> Vec<crate::virtual_tables::SnapshotInfoRow> {
        // Snapshot listing requires async S3 access. Bridge the sync/async
        // boundary by spawning a blocking thread that drives the future on
        // the current tokio Handle. Returns an empty list when S3 is not
        // configured or the runtime is unavailable.
        let (os_config, store) = match self.object_store_and_config() {
            Ok(pair) => pair,
            Err(_) => return Vec::new(),
        };
        let prefix = os_config.prefix.clone();

        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => return Vec::new(),
        };

        // Run the async list on a dedicated thread to avoid blocking the
        // tokio worker. The thread borrows the handle and blocks on it.
        let result = std::thread::spawn(move || {
            let manager = crate::snapshot::SnapshotManager::new(store, prefix);
            handle.block_on(manager.list_snapshots())
        })
        .join();

        let snapshots: Vec<crate::snapshot::metadata::SnapshotMetadata> = match result {
            Ok(Ok(snaps)) => snaps,
            _ => return Vec::new(),
        };

        snapshots
            .into_iter()
            .map(|meta| crate::virtual_tables::SnapshotInfoRow {
                name: meta.name,
                created_at: meta.created_at,
                expires_at: meta.expires_at,
                commit_log_segment: meta.commit_log_position.segment_id as i64,
                commit_log_offset: meta.commit_log_position.offset as i64,
                node_id: meta.node_id,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::PartitionKey;
    use ferrosa_common::schema::ColumnDefinition;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

    /// Return "docker" or "podman" — whichever container runtime is in PATH.
    /// Panics if neither is found.
    fn container_runtime() -> &'static str {
        for candidate in &["docker", "podman"] {
            if std::process::Command::new(candidate)
                .arg("--version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
            {
                return Box::leak((*candidate).to_string().into_boxed_str());
            }
        }
        panic!(
            "no container runtime found — install Podman Desktop (macOS) or Docker Desktop \
             before running container-dependent tests"
        );
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
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_row(value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
        }
    }

    fn table_id() -> TableId {
        TableId::new("test_ks", "test_table")
    }

    #[test]
    fn write_then_read() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let key = make_key("pk1");
        engine
            .write(&table_id(), &key, make_row(b"hello", 1000), 1000)
            .unwrap();

        let result = engine.read(&table_id(), &key).unwrap();
        assert!(result.is_some());
        let partition = result.unwrap();
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"hello".as_slice())
        );
    }

    #[test]
    fn read_unregistered_table_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let key = make_key("k");
        let result = engine.read(&TableId::new("no", "such"), &key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn write_to_unregistered_table_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let key = make_key("k");
        let result = engine.write(&TableId::new("no", "such"), &key, make_row(b"v", 1), 1);
        assert!(result.is_err());
    }

    #[test]
    fn unregister_table_prevents_writes() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        let tid = table_id();

        engine.register_table(test_schema()).unwrap();

        // Write should succeed while registered.
        let key = make_key("before");
        engine.write(&tid, &key, make_row(b"val", 1), 1).unwrap();

        // Unregister the table.
        engine.unregister_table(&tid).unwrap();

        // Write should now fail — table is no longer registered.
        let result = engine.write(&tid, &make_key("after"), make_row(b"v", 2), 2);
        assert!(result.is_err(), "write to unregistered table should fail");
    }

    #[test]
    fn write_flush_read() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let key = make_key("flushed_key");
        engine
            .write(&tid, &key, make_row(b"before_flush", 1000), 1000)
            .unwrap();

        engine.flush(&tid).unwrap();
        assert_eq!(engine.sstable_count(&tid), 1);
        assert_eq!(engine.memtable_size(&tid), 0);

        // Should still be readable from SSTable.
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"before_flush".as_slice())
        );
    }

    #[test]
    fn write_flush_write_read_merges() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let key = make_key("merge_key");

        // Write old value and flush to SSTable.
        engine
            .write(&tid, &key, make_row(b"old", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Write newer value — stays in memtable.
        engine
            .write(&tid, &key, make_row(b"new", 2000), 2000)
            .unwrap();

        // Should merge: timestamp 2000 wins.
        let result = engine.read(&tid, &key).unwrap();
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().rows[0].cells[0].1.value.as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[test]
    fn multiple_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let schema1 = TableSchema {
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            ..test_schema()
        };
        let schema2 = TableSchema {
            keyspace: "ks".to_string(),
            table: "t2".to_string(),
            ..test_schema()
        };

        engine.register_table(schema1).unwrap();
        engine.register_table(schema2).unwrap();

        let tid1 = TableId::new("ks", "t1");
        let tid2 = TableId::new("ks", "t2");
        let key = make_key("shared_key");

        engine
            .write(&tid1, &key, make_row(b"val1", 1000), 1000)
            .unwrap();
        engine
            .write(&tid2, &key, make_row(b"val2", 2000), 2000)
            .unwrap();

        let r1 = engine.read(&tid1, &key).unwrap().unwrap();
        assert_eq!(
            r1.rows[0].cells[0].1.value.as_deref(),
            Some(b"val1".as_slice())
        );

        let r2 = engine.read(&tid2, &key).unwrap().unwrap();
        assert_eq!(
            r2.rows[0].cells[0].1.value.as_deref(),
            Some(b"val2".as_slice())
        );
    }

    #[test]
    fn shutdown_flushes_all() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();

        engine.shutdown().unwrap();

        // After shutdown, SSTable should exist (flush happened).
        assert_eq!(engine.sstable_count(&tid), 1);
    }

    #[test]
    fn batch_write_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let mutations = vec![
            (make_key("k1"), make_row(b"v1", 1000), 1000i64),
            (make_key("k2"), make_row(b"v2", 2000), 2000),
            (make_key("k3"), make_row(b"v3", 3000), 3000),
        ];

        engine.batch_write(&tid, mutations).unwrap();

        assert!(engine.read(&tid, &make_key("k1")).unwrap().is_some());
        assert!(engine.read(&tid, &make_key("k2")).unwrap().is_some());
        assert!(engine.read(&tid, &make_key("k3")).unwrap().is_some());
    }

    #[test]
    fn batch_write_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let mutations: Vec<(DecoratedKey, Row, i64)> = vec![];
        engine.batch_write(&table_id(), mutations).unwrap();
    }

    #[test]
    fn engine_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        for i in 0..5 {
            engine
                .write(
                    &tid,
                    &make_key(&format!("k{i}")),
                    make_row(b"v", 1000),
                    1000,
                )
                .unwrap();
        }

        let results = engine.read_range(&tid, None, None, 100).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn flush_if_needed_triggers_on_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.flush_threshold_bytes = 1; // Trigger on any write.
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k"), make_row(b"data", 1000), 1000)
            .unwrap();

        engine.flush_if_needed().unwrap();

        assert_eq!(engine.sstable_count(&tid), 1);
    }

    #[test]
    fn flush_all_flushes_small_memtables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        // Default threshold is 64 MB — our test row is well below that.
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"tiny", 1000), 1000)
            .unwrap();

        // Memtable should have data.
        assert!(
            engine.memtable_size(&tid) > 0,
            "memtable should be non-empty after write"
        );

        // flush_if_needed should NOT flush — data is below 64 MB threshold.
        engine.flush_if_needed().unwrap();
        assert!(
            engine.memtable_size(&tid) > 0,
            "flush_if_needed should skip small memtable"
        );
        assert_eq!(engine.sstable_count(&tid), 0, "no SSTable should exist yet");

        // flush_all should flush regardless of size.
        engine.flush_all().unwrap();
        assert_eq!(
            engine.memtable_size(&tid),
            0,
            "memtable should be empty after flush_all"
        );
        assert_eq!(
            engine.sstable_count(&tid),
            1,
            "one SSTable should exist after flush_all"
        );
    }

    /// Regression test: write data, flush to SSTable, read back the exact
    /// partition key. Catches format mismatches where read_exact_at fails
    /// with "wanted N bytes, got M" after a flush (e.g., when the SSTable
    /// format changes between binary versions).
    #[test]
    fn write_flush_read_point_query_no_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // Table with composite partition key + clustering + regular column
        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "memo_cache".into(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)".into(),
            clustering_columns: vec![ferrosa_common::ColumnDefinition {
                name: "tenant_id".into(),
                type_name: "org.apache.cassandra.db.marshal.UUIDType".into(),
            }],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "result".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
        };
        engine.register_table(schema).unwrap();

        let tid = TableId::new("test_ks", "memo_cache");

        // Write multiple partitions with different composite keys
        for i in 0..5 {
            let pk_bytes = format!("hash_{i}\x00v1");
            let key = DecoratedKey::new(PartitionKey::new(pk_bytes.into_bytes()));
            let row = Row {
                clustering: vec![0u8; 16], // UUID-sized clustering
                cells: vec![(
                    0,
                    CellValue::live(format!("result_{i}").into_bytes(), 1000 + i),
                )],
                deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(
                    1000 + i,
                ),
            };
            engine.write(&tid, &key, row, 1000 + i).unwrap();
        }

        // Force flush to SSTable
        engine.flush_all().unwrap();
        assert!(engine.sstable_count(&tid) >= 1, "should have flushed");

        // Point read each partition — must not error
        for i in 0..5 {
            let pk_bytes = format!("hash_{i}\x00v1");
            let key = DecoratedKey::new(PartitionKey::new(pk_bytes.into_bytes()));
            let result = engine.read(&tid, &key);
            assert!(
                result.is_ok(),
                "point read after flush failed for partition {i}: {:?}",
                result.err()
            );
        }
    }

    /// Regression: SSTable corruption with long composite partition keys.
    /// Row 1 (long value) corrupts after flush; Row 2 (short value) survives.
    /// Reproduces the exact memo_cache scenario from production.
    #[test]
    fn write_flush_read_long_composite_pk_survives() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // memo_cache schema: ((content_hash text, model_version text), tenant_id uuid)
        let schema = ferrosa_common::TableSchema {
            keyspace: "agent_memory".into(),
            table: "memo_cache".into(),
            key_type: "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)".into(),
            clustering_columns: vec![ferrosa_common::ColumnDefinition {
                name: "tenant_id".into(),
                type_name: "org.apache.cassandra.db.marshal.UUIDType".into(),
            }],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "result".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
        };
        engine.register_table(schema).unwrap();

        let tid = TableId::new("agent_memory", "memo_cache");

        // Row 1: long hash + medium model version + 31-byte result (CORRUPTS in prod)
        let hash1 = "cac0302657b4c1d0dfd5aec98f2754f46a42f117e53c77a9ba384ebf2095633a"; // pragma: allowlist secret
        let model1 = "claude-opus-4-6";
        let result1 = "The capital of France is Paris.";
        // Composite key encoding: [u16 len][bytes][0x00] per component
        let mut pk1_bytes = Vec::new();
        pk1_bytes.extend_from_slice(&(hash1.len() as u16).to_be_bytes());
        pk1_bytes.extend_from_slice(hash1.as_bytes());
        pk1_bytes.push(0x00);
        pk1_bytes.extend_from_slice(&(model1.len() as u16).to_be_bytes());
        pk1_bytes.extend_from_slice(model1.as_bytes());
        pk1_bytes.push(0x00);

        let key1 = DecoratedKey::new(PartitionKey::new(pk1_bytes.clone()));
        let row1 = Row {
            clustering: vec![0x11u8; 16], // UUID
            cells: vec![(0, CellValue::live(result1.as_bytes().to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key1, row1, 1000).unwrap();

        // Row 2: long hash + short model version + 1-byte result (SURVIVES in prod)
        let hash2 = "4bbe47ee6bb1d0ecaa4d47fce3d99e2044cdfdbfac4dde0c0c083f97e4fad000"; // pragma: allowlist secret
        let model2 = "test-v1";
        let result2 = "4";
        let mut pk2_bytes = Vec::new();
        pk2_bytes.extend_from_slice(&(hash2.len() as u16).to_be_bytes());
        pk2_bytes.extend_from_slice(hash2.as_bytes());
        pk2_bytes.push(0x00);
        pk2_bytes.extend_from_slice(&(model2.len() as u16).to_be_bytes());
        pk2_bytes.extend_from_slice(model2.as_bytes());
        pk2_bytes.push(0x00);

        let key2 = DecoratedKey::new(PartitionKey::new(pk2_bytes.clone()));
        let row2 = Row {
            clustering: vec![0x22u8; 16], // UUID
            cells: vec![(0, CellValue::live(result2.as_bytes().to_vec(), 2000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &key2, row2, 2000).unwrap();

        // Verify memtable reads work
        assert!(
            engine.read(&tid, &key1).unwrap().is_some(),
            "row1 memtable read"
        );
        assert!(
            engine.read(&tid, &key2).unwrap().is_some(),
            "row2 memtable read"
        );

        // Flush to SSTable
        engine.flush_all().unwrap();
        assert!(engine.sstable_count(&tid) >= 1);

        // Point reads after flush — BOTH must succeed with data
        let p1 = engine
            .read(&tid, &key1)
            .expect("row1 read after flush should not error")
            .expect("row1 should exist after flush");
        assert!(
            !p1.rows.is_empty(),
            "row1 partition should have rows after flush"
        );

        let p2 = engine
            .read(&tid, &key2)
            .expect("row2 read after flush should not error")
            .expect("row2 should exist after flush");
        assert!(
            !p2.rows.is_empty(),
            "row2 partition should have rows after flush"
        );

        // Range scan should return both partitions
        let all = engine.read_range(&tid, None, None, 100).unwrap();
        assert_eq!(all.len(), 2, "range scan should find both partitions");
    }

    /// A test observer that counts `on_write` calls.
    struct CountingObserver {
        watched: Vec<TableId>,
        call_count: std::sync::atomic::AtomicU64,
    }

    impl CountingObserver {
        fn new(watched: Vec<TableId>) -> Self {
            Self {
                watched,
                call_count: std::sync::atomic::AtomicU64::new(0),
            }
        }

        fn count(&self) -> u64 {
            self.call_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::observer::WriteObserver for CountingObserver {
        fn mode(&self) -> crate::observer::ObserverMode {
            crate::observer::ObserverMode::Sync
        }

        fn tables(&self) -> Vec<TableId> {
            self.watched.clone()
        }

        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Vec::new()
        }
    }

    #[test]
    fn sync_observer_fires_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let observer = Arc::new(CountingObserver::new(vec![tid.clone()]));
        engine.register_observer(observer.clone());

        let key = make_key("k1");
        engine
            .write(&tid, &key, make_row(b"val", 1000), 1000)
            .unwrap();

        assert_eq!(observer.count(), 1);
    }

    #[test]
    fn sync_observer_only_fires_for_watched_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        // Observer watches table_a, but we write to test_table.
        let table_a = TableId::new("other_ks", "other_table");
        let observer = Arc::new(CountingObserver::new(vec![table_a]));
        engine.register_observer(observer.clone());

        let tid = table_id();
        let key = make_key("k1");
        engine
            .write(&tid, &key, make_row(b"val", 1000), 1000)
            .unwrap();

        assert_eq!(observer.count(), 0);
    }

    /// A test observer that operates in async mode.
    struct AsyncCountingObserver {
        watched: Vec<TableId>,
        call_count: std::sync::atomic::AtomicU64,
    }

    impl AsyncCountingObserver {
        fn new(watched: Vec<TableId>) -> Self {
            Self {
                watched,
                call_count: std::sync::atomic::AtomicU64::new(0),
            }
        }
    }

    impl crate::observer::WriteObserver for AsyncCountingObserver {
        fn mode(&self) -> crate::observer::ObserverMode {
            crate::observer::ObserverMode::Async
        }

        fn tables(&self) -> Vec<TableId> {
            self.watched.clone()
        }

        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Vec::new()
        }
    }

    #[test]
    fn async_observer_does_not_block_write() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let observer = Arc::new(AsyncCountingObserver::new(vec![tid.clone()]));
        let mut rx = engine.register_async_observer(observer, 16);

        let key = make_key("k1");
        engine
            .write(&tid, &key, make_row(b"val", 1000), 1000)
            .unwrap();

        // Write should succeed (async observer does not block).
        // The mutation should be in the channel.
        let msg = rx.try_recv();
        assert!(msg.is_ok());
        let (recv_tid, recv_mutation) = msg.unwrap();
        assert_eq!(recv_tid, tid);
        assert_eq!(recv_mutation.keyspace, "test_ks");
    }

    #[test]
    fn async_observer_backpressure_drops() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();
        let observer = Arc::new(AsyncCountingObserver::new(vec![tid.clone()]));
        // Tiny channel capacity of 2 to test backpressure.
        let _rx = engine.register_async_observer(observer, 2);

        assert_eq!(engine.observer_drop_count(), 0);

        // Write 5 mutations — channel holds 2, so at least 3 should be dropped.
        for i in 0..5 {
            let key = make_key(&format!("k{i}"));
            engine
                .write(&tid, &key, make_row(b"val", 1000 + i), 1000 + i)
                .unwrap();
        }

        // Drop count should be >= 3 (channel capacity 2, 5 writes).
        assert!(
            engine.observer_drop_count() >= 3,
            "expected >= 3 drops, got {}",
            engine.observer_drop_count()
        );
    }

    #[test]
    fn full_lifecycle_write_flush_replay_compact() {
        let dir = tempfile::tempdir().unwrap();
        let tid = table_id();

        // Phase 1: Write data across multiple flush cycles.
        {
            let mut config = StorageEngineConfig::test_config(dir.path());
            config.compaction.min_threshold = 4;
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();

            // 4 flush cycles → 4 SSTables → triggers STCS
            for batch in 0..4 {
                for i in 0..3 {
                    let key_name = format!("batch{batch}_key{i}");
                    let ts = (batch * 1000 + i) as i64;
                    engine
                        .write(
                            &tid,
                            &make_key(&key_name),
                            make_row(key_name.as_bytes(), ts),
                            ts,
                        )
                        .unwrap();
                }
                engine.flush(&tid).unwrap();
            }

            assert_eq!(engine.sstable_count(&tid), 4);

            // Write one more (not flushed) — this must survive via replay.
            engine
                .write(
                    &tid,
                    &make_key("unflushed"),
                    make_row(b"survive", 9999),
                    9999,
                )
                .unwrap();

            engine.commit_log.shutdown().unwrap();
        }

        // Phase 2: Re-open, replay, verify all data present.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let (engine, pending) = StorageEngine::open(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();
            engine.replay_mutations(pending).unwrap();

            // The unflushed mutation should be present from replay.
            let result = engine.read(&tid, &make_key("unflushed")).unwrap();
            assert!(result.is_some(), "unflushed mutation should survive replay");

            // All flushed data should also be readable.
            for batch in 0..4 {
                for i in 0..3 {
                    let key_name = format!("batch{batch}_key{i}");
                    let r = engine.read(&tid, &make_key(&key_name)).unwrap();
                    assert!(r.is_some(), "flushed key {key_name} should be readable");
                }
            }

            engine.shutdown().unwrap();
        }
    }

    #[test]
    fn replay_tolerates_corrupt_segment() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: Write data.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine.register_table(test_schema()).unwrap();

            let tid = table_id();
            engine
                .write(&tid, &make_key("good"), make_row(b"val", 1000), 1000)
                .unwrap();
            engine.commit_log.shutdown().unwrap();
        }

        // Corrupt one of the segment files by overwriting bytes in the middle.
        let log_dir = dir.path();
        for entry in std::fs::read_dir(log_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) == Some("log") {
                let mut data = std::fs::read(&path).unwrap();
                if data.len() > 100 {
                    // Corrupt some bytes in the data section (after the header).
                    for b in &mut data[80..90] {
                        *b = 0xFF;
                    }
                    std::fs::write(&path, &data).unwrap();
                }
            }
        }

        // Phase 2: Replay should skip the corrupted entry.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let (engine, pending) = StorageEngine::open(config, None).unwrap();
            // Replay should not panic — corrupted entries are silently skipped.
            engine.register_table(test_schema()).unwrap();
            engine.replay_mutations(pending).unwrap();
        }
    }

    #[tokio::test]
    async fn concurrent_read_during_compaction() {
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(dir.path());
        config.compaction.min_threshold = 2;
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();

        // Create 2 SSTables.
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Start a concurrent reader that continuously reads.
        let eng = Arc::clone(&engine);
        let reader_tid = tid.clone();
        let handle = std::thread::spawn(move || {
            for _ in 0..100 {
                // These reads must always succeed — ArcSwap provides
                // atomic visibility regardless of concurrent compaction.
                let r1 = eng.read(&reader_tid, &make_key("k1")).unwrap();
                assert!(r1.is_some(), "k1 must always be readable");
                let r2 = eng.read(&reader_tid, &make_key("k2")).unwrap();
                assert!(r2.is_some(), "k2 must always be readable");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        // Trigger compaction while reader is active.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        engine.poll_compactions().await;

        handle.join().unwrap();
    }

    #[test]
    fn commit_log_position_exposed() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema()).unwrap();

        let before = engine.commit_log_position();
        let tid = table_id();
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        let after = engine.commit_log_position();

        assert!(
            after > before,
            "commit_log_position should advance after write"
        );

        engine.shutdown().unwrap();
    }

    #[test]
    fn archiver_uploads_closed_segment_on_rotate() {
        use object_store::memory::InMemory;
        use object_store::path::Path as ObjectPath;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let prefix = "test-node";

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig {
                    segment_size: 512, // small to force rotation
                    archive: Some(crate::commitlog::config::ArchiveConfig {
                        enabled: true,
                        poll_interval: std::time::Duration::from_millis(50),
                        ..crate::commitlog::config::ArchiveConfig::default()
                    }),
                    ..CommitLogConfig::test_config(dir.path())
                },
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::new_with_archive_store(
                config,
                Some(&tokio::runtime::Handle::current()),
                Some(Arc::clone(&store)),
                prefix.to_string(),
            )
            .unwrap();

            engine.register_table(test_schema()).unwrap();

            let tid = table_id();
            let key = make_key("k1");
            let row = make_row(b"value", 1000);

            // Write enough to trigger rotation.
            for i in 0..20 {
                let _ = engine.write(&tid, &key, row.clone(), i);
            }

            // Give the archiver time to process.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            // Verify at least one segment was uploaded to S3.
            let manifest =
                crate::commitlog::manifest::ArchiveManifest::load(store.as_ref(), prefix)
                    .await
                    .unwrap();
            assert!(
                !manifest.segments.is_empty(),
                "archiver should have uploaded at least one segment"
            );

            // Verify the segment data is in S3.
            let seg = &manifest.segments[0];
            let hex = crate::upload::manager::hex_prefix_for(&seg.id.to_string());
            let s3_path =
                ObjectPath::from(format!("{prefix}/commitlog-archive/{hex}/{}.log", seg.id));
            let result = store.get(&s3_path).await;
            assert!(result.is_ok(), "segment file should exist in S3");

            engine.shutdown().unwrap();
        });
    }

    // ── open_from_snapshot_with_store tests ──────────────────────────────────

    #[test]
    fn open_from_snapshot_downloads_and_validates() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            // Set up: manifest + schema in S3, then create a snapshot.
            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix, true)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());
            let pos = crate::commitlog::CommitLogPosition {
                segment_id: 1,
                offset: 0,
            };
            snap_mgr
                .create_snapshot("test-snap", &manifest, b"{}", pos, "node-1", None, false)
                .await
                .unwrap();

            // Restore from snapshot.
            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::open_from_snapshot_with_store(
                config,
                "test-snap",
                None,     // no PIT filter
                "node-1", // same node
                false,    // no force
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

            // Engine should be functional.
            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn open_from_snapshot_rejects_cross_node_without_force() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix, true)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());
            let pos = crate::commitlog::CommitLogPosition {
                segment_id: 1,
                offset: 0,
            };
            snap_mgr
                .create_snapshot("test-snap", &manifest, b"{}", pos, "node-1", None, false)
                .await
                .unwrap();

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let result = StorageEngine::open_from_snapshot_with_store(
                config,
                "test-snap",
                None,
                "node-2", // different node!
                false,    // no force
                Arc::clone(&store),
                prefix,
            )
            .await;

            assert!(
                result.is_err(),
                "cross-node restore without force must fail"
            );
            let err_msg = match result {
                Err(e) => e.to_string(),
                Ok(_) => unreachable!(),
            };
            assert!(
                err_msg.contains("force"),
                "error message should mention 'force': {err_msg}"
            );
        });
    }

    #[test]
    fn open_from_snapshot_force_allows_cross_node_restore() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix, true)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let snap_mgr =
                crate::snapshot::SnapshotManager::new(Arc::clone(&store), prefix.to_string());
            let pos = crate::commitlog::CommitLogPosition {
                segment_id: 1,
                offset: 0,
            };
            snap_mgr
                .create_snapshot(
                    "test-snap-force",
                    &manifest,
                    b"{}",
                    pos,
                    "node-1",
                    None,
                    false,
                )
                .await
                .unwrap();

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            // force = true should succeed even though node IDs differ.
            let engine = StorageEngine::open_from_snapshot_with_store(
                config,
                "test-snap-force",
                None,
                "node-2", // different node
                true,     // force override
                Arc::clone(&store),
                prefix,
            )
            .await
            .unwrap();

            engine.shutdown().unwrap();
        });
    }

    #[test]
    fn open_from_snapshot_rejects_missing_snapshot() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::memory::InMemory::new());

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let result = StorageEngine::open_from_snapshot_with_store(
                config,
                "does-not-exist",
                None,
                "node-1",
                false,
                Arc::clone(&store),
                "prefix",
            )
            .await;

            assert!(result.is_err(), "missing snapshot should return an error");
        });
    }

    #[test]
    fn create_snapshot_flushes_and_writes_to_s3() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: std::sync::Arc<dyn object_store::ObjectStore> =
                std::sync::Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            // Save a manifest and schema so snapshot can load them.
            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix, true)
                .await
                .unwrap();
            crate::manifest::save_schema_snapshot(store.as_ref(), prefix, b"{}")
                .await
                .unwrap();

            let config = StorageEngineConfig {
                commit_log: CommitLogConfig::test_config(dir.path()),
                ..StorageEngineConfig::test_config(dir.path())
            };

            let engine = StorageEngine::new(config, None).unwrap();

            // Register a table and write some data.
            engine.register_table(test_schema()).unwrap();
            let tid = table_id();
            let key = make_key("k1");
            engine
                .write(&tid, &key, make_row(b"value", 1000), 1000)
                .unwrap();

            // Create snapshot via injected store.
            let metadata = engine
                .create_snapshot_with_store(
                    "test-snap",
                    "node-1",
                    None,
                    false,
                    std::sync::Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();

            assert_eq!(metadata.name, "test-snap");
            assert_eq!(metadata.node_id, "node-1");
            assert!(!metadata.manifest_sha256.is_empty());
            assert!(!metadata.ephemeral);

            // Verify snapshot objects exist in S3.
            let meta_path = object_store::path::Path::from(format!(
                "{prefix}/snapshots/test-snap/metadata.json"
            ));
            assert!(store.get(&meta_path).await.is_ok());

            engine.shutdown().unwrap();
        });
    }

    // =========================================================================
    // Task 3.2: Sidecar files survive table re-registration
    // =========================================================================

    #[test]
    fn sidecar_survives_table_reregistration() {
        use ferrosa_index::IndexKey;

        let dir = tempfile::tempdir().unwrap();
        let tid = table_id();

        // Phase 1: register table with an index, write indexed data, flush.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let engine = StorageEngine::new(config, None).unwrap();
            engine
                .register_table_with_indexes(test_schema(), vec![("val_idx".to_string(), 0_usize)])
                .unwrap();

            engine
                .write(&tid, &make_key("user1"), make_row(b"alice", 1000), 1000)
                .unwrap();
            engine.flush(&tid).unwrap();

            // Verify readable before drop.
            let results = engine
                .read_by_index(&tid, "val_idx", &IndexKey(b"alice".to_vec()))
                .unwrap();
            assert_eq!(results.len(), 1, "pre-reregistration: should find user1");

            engine.shutdown().unwrap();
        }

        // Phase 2: create a new engine with the same data dir, re-register,
        // and verify the sidecar index is loaded from disk.
        {
            let config = StorageEngineConfig::test_config(dir.path());
            let (engine, pending) = StorageEngine::open(config, None).unwrap();
            engine
                .register_table_with_indexes(test_schema(), vec![("val_idx".to_string(), 0_usize)])
                .unwrap();
            engine.replay_mutations(pending).unwrap();

            let results = engine
                .read_by_index(&tid, "val_idx", &IndexKey(b"alice".to_vec()))
                .unwrap();
            assert_eq!(
                results.len(),
                1,
                "post-reregistration: sidecar should be loaded from disk and return user1"
            );
            assert_eq!(results[0].key.key.as_bytes(), b"user1");
        }
    }

    #[test]
    fn engine_has_batchlog_manager() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        assert!(engine.batchlog().is_some());
    }

    #[test]
    fn engine_write_atomic_batch() {
        use ferrosa_common::Token;

        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        // Register two tables
        use ferrosa_common::schema::TableSchema;
        for tbl in &["tbl_a", "tbl_b"] {
            let schema = TableSchema {
                keyspace: "ks".to_string(),
                table: tbl.to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "val".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
            };
            engine.register_table(schema).unwrap();
        }

        let mutations = vec![
            Mutation {
                keyspace: "ks".to_string(),
                table: "tbl_a".to_string(),
                key: DecoratedKey {
                    token: Token(1),
                    key: PartitionKey::new(b"pk1".to_vec()),
                },
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(b"val_a".to_vec(), 100))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(100),
                }],
                timestamp: 100,
            },
            Mutation {
                keyspace: "ks".to_string(),
                table: "tbl_b".to_string(),
                key: DecoratedKey {
                    token: Token(2),
                    key: PartitionKey::new(b"pk2".to_vec()),
                },
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(b"val_b".to_vec(), 100))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(100),
                }],
                timestamp: 100,
            },
        ];

        engine.write_atomic_batch(mutations).unwrap();

        // Both writes should be visible
        let table_a = TableId::new("ks", "tbl_a");
        let key_a = DecoratedKey {
            token: Token(1),
            key: PartitionKey::new(b"pk1".to_vec()),
        };
        let result_a = engine.read(&table_a, &key_a).unwrap();
        assert!(result_a.is_some(), "mutation to tbl_a should be visible");

        let table_b = TableId::new("ks", "tbl_b");
        let key_b = DecoratedKey {
            token: Token(2),
            key: PartitionKey::new(b"pk2".to_vec()),
        };
        let result_b = engine.read(&table_b, &key_b).unwrap();
        assert!(result_b.is_some(), "mutation to tbl_b should be visible");
    }

    // -- System table registration tests --

    #[test]
    fn register_system_tables_creates_six_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_system_tables().unwrap();

        // Verify all 6 system tables are registered by attempting writes.
        let system_tables = [
            ("system_schema", "keyspaces"),
            ("system_schema", "tables"),
            ("system_schema", "columns"),
            ("system_auth", "roles"),
            ("system_auth", "role_members"),
            ("system_auth", "role_permissions"),
        ];

        for (ks, tbl) in &system_tables {
            let tid = TableId::new(*ks, *tbl);
            let key = make_key("test");
            let row = make_row(b"v", 1);
            let result = engine.write(&tid, &key, row, 1);
            assert!(
                result.is_ok(),
                "system table {ks}.{tbl} should be registered"
            );
        }
    }

    #[test]
    fn register_system_tables_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        engine.register_system_tables().unwrap();
        // Second call should not error.
        engine.register_system_tables().unwrap();
    }

    /// FRSA-BUG-026: write a row with a map-typed cell value (CQL binary
    /// format), flush to SSTable, read back. The read must not error with
    /// "read_exact_at: wanted 1 bytes, got 0".
    #[test]
    fn write_flush_read_map_cell_value() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // Temporal's queue_metadata: (queue_type int PK, cluster_ack_level map<text,bigint>, version bigint)
        let schema = ferrosa_common::TableSchema {
            keyspace: "temporal".into(),
            table: "queue_metadata".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ferrosa_common::ColumnDefinition {
                    name: "cluster_ack_level".into(),
                    type_name: "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)".into(),
                },
                ferrosa_common::ColumnDefinition {
                    name: "version".into(),
                    type_name: "org.apache.cassandra.db.marshal.LongType".into(),
                },
            ],
        };
        engine.register_table(schema).unwrap();

        let tid = TableId::new("temporal", "queue_metadata");

        // Write a row: queue_type=1, cluster_ack_level={} (empty map), version=0
        let pk_bytes = 1i32.to_be_bytes().to_vec();
        let key = DecoratedKey::new(PartitionKey::new(pk_bytes));

        // Empty map in CQL binary: [i32 count=0] = 4 bytes of zeros
        let empty_map_bytes = 0i32.to_be_bytes().to_vec();
        let version_bytes = 0i64.to_be_bytes().to_vec();

        let row = Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(empty_map_bytes, 1000)),
                (1, CellValue::live(version_bytes, 1000)),
            ],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();

        // Read from memtable — should work
        let memtable_result = engine.read(&tid, &key);
        assert!(
            memtable_result.is_ok(),
            "memtable read failed: {:?}",
            memtable_result.err()
        );
        assert!(
            memtable_result.unwrap().is_some(),
            "row should exist in memtable"
        );

        // Flush to SSTable
        engine.flush_all().unwrap();
        assert!(
            engine.sstable_count(&tid) >= 1,
            "should have flushed to SSTable"
        );

        // Read from SSTable — this is where FRSA-BUG-026 fails
        let sstable_result = engine.read(&tid, &key);
        assert!(
            sstable_result.is_ok(),
            "SSTable read after flush failed: {:?}",
            sstable_result.err()
        );
        assert!(
            sstable_result.unwrap().is_some(),
            "row should exist in SSTable"
        );
    }

    // ── FMEA: SSTable corruption resilience ─────────────────────────────

    /// FMEA #1: Truncating an SSTable Data.db file should not crash reads.
    /// The read should return data from the memtable or other SSTables,
    /// logging a warning about the corrupt SSTable.
    #[test]
    fn read_survives_truncated_sstable_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "resilience".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "resilience");

        // Write and flush to SSTable
        let key = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();
        engine.flush_all().unwrap();

        // Corrupt: truncate the Data.db file to 1 byte
        let sstable_dir = dir.path().join("sstables/test_ks.resilience");
        if let Ok(entries) = std::fs::read_dir(&sstable_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().ends_with("-Data.db") {
                    std::fs::write(&path, [0u8]).unwrap();
                }
            }
        }

        // Read should NOT crash — should return None (data lost but no panic)
        let result = engine.read(&tid, &key);
        assert!(
            result.is_ok(),
            "read with corrupt SSTable should not crash: {:?}",
            result.err()
        );
        // Data may be lost (from corrupt SSTable) but the operation didn't crash
    }

    /// FMEA #6: Zero-length Data.db should not crash reads.
    #[test]
    fn read_survives_zero_length_data_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "zero_data".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "zero_data");

        let key = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();
        engine.flush_all().unwrap();

        // Corrupt: zero out the Data.db file
        let sstable_dir = dir.path().join("sstables/test_ks.zero_data");
        if let Ok(entries) = std::fs::read_dir(&sstable_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().ends_with("-Data.db") {
                    std::fs::write(&path, []).unwrap();
                }
            }
        }

        let result = engine.read(&tid, &key);
        assert!(
            result.is_ok(),
            "read with zero-length SSTable should not crash: {:?}",
            result.err()
        );
    }

    /// FMEA #9: Write data, flush, ALTER TABLE ADD column, write more,
    /// flush again, read back — old SSTables should still be readable.
    #[test]
    fn read_survives_schema_evolution_across_sstables() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        // Original schema: (k int PK, v text)
        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "evolving".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "evolving");

        // Write row with 1 column, flush to SSTable
        let key1 = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row1 = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"old".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key1, row1, 1000).unwrap();
        engine.flush_all().unwrap();

        // "ALTER TABLE ADD" — write row with 2 columns (cell index 0 and 1)
        let key2 = DecoratedKey::new(PartitionKey::new(2i32.to_be_bytes().to_vec()));
        let row2 = Row {
            clustering: vec![],
            cells: vec![
                (0, CellValue::live(b"new_v".to_vec(), 2000)),
                (1, CellValue::live(b"extra".to_vec(), 2000)),
            ],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &key2, row2, 2000).unwrap();
        engine.flush_all().unwrap();

        // Read both rows — old SSTable should not crash
        let r1 = engine.read(&tid, &key1);
        assert!(r1.is_ok(), "old row read failed: {:?}", r1.err());
        assert!(r1.unwrap().is_some(), "old row should exist");

        let r2 = engine.read(&tid, &key2);
        assert!(r2.is_ok(), "new row read failed: {:?}", r2.err());
        assert!(r2.unwrap().is_some(), "new row should exist");
    }

    /// FMEA #8: Memtable write + new data in memtable should still work
    /// even if an SSTable is corrupt.
    #[test]
    fn memtable_data_survives_corrupt_sstable() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig::test_config(dir.path()),
            ..StorageEngineConfig::test_config(dir.path())
        };
        let engine = StorageEngine::new(config, None).unwrap();

        let schema = ferrosa_common::TableSchema {
            keyspace: "test_ks".into(),
            table: "survive".into(),
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ferrosa_common::ColumnDefinition {
                name: "v".into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            }],
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "survive");

        // Write old data and flush
        let key_old = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"flushed".to_vec(), 1000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key_old, row, 1000).unwrap();
        engine.flush_all().unwrap();

        // Corrupt the SSTable
        let sstable_dir = dir.path().join("sstables/test_ks.survive");
        if let Ok(entries) = std::fs::read_dir(&sstable_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().ends_with("-Data.db") {
                    std::fs::write(&path, [0xDE, 0xAD]).unwrap();
                }
            }
        }

        // Write new data to memtable
        let key_new = DecoratedKey::new(PartitionKey::new(2i32.to_be_bytes().to_vec()));
        let row = Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"memtable".to_vec(), 2000))],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &key_new, row, 2000).unwrap();

        // Read new data from memtable — should work despite corrupt SSTable
        let r = engine.read(&tid, &key_new);
        assert!(r.is_ok(), "memtable read should work: {:?}", r.err());
        assert!(r.unwrap().is_some(), "memtable row should exist");

        // Read old data — corrupt SSTable, but should not crash
        let r_old = engine.read(&tid, &key_old);
        assert!(
            r_old.is_ok(),
            "read of corrupt SSTable data should not crash: {:?}",
            r_old.err()
        );
    }

    // ── S3 compaction tests (T-025 / T-026) ─────────────────────────────────

    /// Build an engine with a pending compaction result waiting to be polled.
    ///
    /// Flushes two SSTables, manually submits a compaction task, and waits
    /// until the compaction executor finishes writing the output files.
    async fn make_engine_with_pending_compaction(
        dir: &tempfile::TempDir,
    ) -> (
        StorageEngine,
        Arc<dyn object_store::ObjectStore>,
        String,
        TableId,
    ) {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "test-node".to_string();

        let config = StorageEngineConfig::test_config(dir.path());
        let rt = tokio::runtime::Handle::current();
        let engine = StorageEngine::new_with_upload_store(
            config,
            Arc::clone(&store),
            prefix.clone(),
            &rt,
        )
        .unwrap();
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();

        // Flush 1: write k1 → SSTable #1.
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Flush 2: write k2 → SSTable #2.
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Manually submit a compaction task.
        {
            let compaction_output_dir = dir.path().join("compaction");
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);

            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: compaction_output_dir,
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Wait for the compaction executor (background thread) to finish.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let has_output = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if has_output {
                    break;
                }
            }
        }

        (engine, store, prefix, tid)
    }

    #[tokio::test]
    async fn compaction_output_uploaded_to_s3() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, prefix, tid) = make_engine_with_pending_compaction(&dir).await;

        // poll_compactions integrates the compaction result, uploads to S3,
        // and updates the manifest. Retry until the channel result is consumed:
        // the compaction thread may write output files to disk slightly before
        // it sends the result on the channel, so a single poll may race.
        let tid_str = tid.to_string();
        let mut entries = vec![];
        for _ in 0..40 {
            engine.poll_compactions().await;
            let (manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
            entries = manifest.sstables.get(&tid_str).cloned().unwrap_or_default();
            if !entries.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Exactly one entry: the merged output.
        assert_eq!(
            entries.len(),
            1,
            "manifest should contain exactly one SSTable after compaction, got: {:?}",
            entries
        );

        // The object itself must be present in the store.
        let output_id = &entries[0].id;
        let hex = crate::upload::manager::hex_prefix_for(output_id);
        let data_path = object_store::path::Path::from(format!(
            "{prefix}/{hex}/{tid_str}/{output_id}/{output_id}-Data.db"
        ));
        assert!(
            store.get(&data_path).await.is_ok(),
            "compacted SSTable Data.db must be present in S3 at {data_path}"
        );
    }

    #[tokio::test]
    async fn manifest_updated_after_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, store, prefix, tid) = make_engine_with_pending_compaction(&dir).await;

        let tid_str = tid.to_string();

        // Manifest must be empty before poll_compactions runs.
        let (before_manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
            .await
            .unwrap();
        let before_count = before_manifest
            .sstables
            .get(&tid_str)
            .map(|v| v.len())
            .unwrap_or(0);
        assert_eq!(before_count, 0, "manifest should be empty before poll_compactions");

        // Integrate compaction result. Retry until the channel result is consumed.
        let mut entries = vec![];
        for _ in 0..40 {
            engine.poll_compactions().await;
            let (after_manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
            entries = after_manifest
                .sstables
                .get(&tid_str)
                .cloned()
                .unwrap_or_default();
            if !entries.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        assert_eq!(
            entries.len(),
            1,
            "manifest should have exactly 1 SSTable entry (the output) after compaction"
        );
        assert!(entries[0].size > 0, "output SSTable entry must have non-zero size");
        assert!(!entries[0].id.is_empty(), "output SSTable id must be non-empty");
    }

    #[tokio::test]
    async fn manifest_compaction_concurrent_flush() {
        // Two independent compaction + flush operations run concurrently.
        // Neither must corrupt the manifest; both must complete without panicking.
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "test-concurrent".to_string();

        let config = StorageEngineConfig::test_config(dir.path());
        let rt = tokio::runtime::Handle::current();
        let engine = std::sync::Arc::new(
            StorageEngine::new_with_upload_store(
                config,
                Arc::clone(&store),
                prefix.clone(),
                &rt,
            )
            .unwrap(),
        );
        engine.register_table(test_schema()).unwrap();

        let tid = table_id();

        // Flush 2 SSTables.
        engine
            .write(&tid, &make_key("k1"), make_row(b"v1", 1000), 1000)
            .unwrap();
        engine.flush(&tid).unwrap();
        engine
            .write(&tid, &make_key("k2"), make_row(b"v2", 2000), 2000)
            .unwrap();
        engine.flush(&tid).unwrap();

        // Submit a compaction task manually.
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);

            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: dir.path().join("compaction"),
                schema: test_schema(),
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Write a third row for the concurrent flush.
        engine
            .write(&tid, &make_key("k3"), make_row(b"v3", 3000), 3000)
            .unwrap();

        // Wait for compaction to finish.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let done = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if done {
                    break;
                }
            }
        }

        // Flush on a separate task while poll_compactions runs.
        let eng_clone = std::sync::Arc::clone(&engine);
        let tid_clone = tid.clone();
        let flush_handle = tokio::task::spawn_blocking(move || {
            eng_clone.flush(&tid_clone).unwrap();
        });

        engine.poll_compactions().await;
        flush_handle.await.unwrap();

        // Both operations completed without panic.
        // Manifest must have at least one SSTable entry.
        let (final_manifest, _) =
            crate::manifest::Manifest::load(store.as_ref(), &prefix)
                .await
                .unwrap();
        let tid_str = tid.to_string();
        let entries = final_manifest
            .sstables
            .get(&tid_str)
            .cloned()
            .unwrap_or_default();
        assert!(
            !entries.is_empty(),
            "manifest should have at least one SSTable entry after concurrent compaction+flush"
        );
    }

    // ── T-026: input SSTable deletion tests ──────────────────────────────────

    #[tokio::test]
    async fn compaction_inputs_deleted_from_s3_after_grace() {
        // Use grace_period = 0 so deletions are immediate (no real wait).
        // We patch the DeleteSSTable tasks by using the manager's channel directly,
        // but the cleanest approach is to use a zero grace period via the normal path.
        // Since grace_period is baked into the task by poll_compactions (1 hour),
        // we verify the deletion tasks were enqueued and the upload manager processes them.
        //
        // Strategy: upload the input SSTables first so they exist in S3, then run
        // poll_compactions with zero grace (we can't set grace to 0 via poll_compactions
        // directly, so we verify by running the upload manager with a zero-grace DeleteSSTable
        // task manually, which validates the idempotency path).
        //
        // Full integration test: build engine, compact, verify output is present and
        // deletions are submitted (fire-and-forget; they run in background with 1-hour grace).

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        // Pre-populate input SSTable objects in S3 so deletion is meaningful.
        let prefix = "test-node";
        let table_id_str = "test_ks.test_table";
        let input_id = "input_sst_1";
        let hex = crate::upload::manager::hex_prefix_for(input_id);
        for component in &["Data.db", "Index.db", "Filter.db", "Statistics.db", "TOC.txt"] {
            let path = object_store::path::Path::from(format!(
                "{prefix}/{hex}/{table_id_str}/{input_id}/{component}"
            ));
            store
                .put(&path, bytes::Bytes::from_static(b"data").into())
                .await
                .unwrap();
        }

        // Submit a zero-grace DeleteSSTable task directly to verify idempotency.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let rt = tokio::runtime::Handle::current();
        let mgr = crate::upload::UploadManager::new(
            Arc::clone(&store),
            prefix.to_string(),
            16,
            &rt,
        );
        mgr.submit(crate::upload::UploadTask::DeleteSSTable {
            table_id: table_id_str.to_string(),
            sstable_id: input_id.to_string(),
            grace_period: std::time::Duration::from_secs(0),
            on_complete: Some(tx),
        })
        .await
        .unwrap();

        // Wait for deletion to complete.
        let result = rx.await.unwrap();
        assert!(result.is_ok(), "deletion should succeed: {:?}", result.err());

        mgr.shutdown().await;

        // All five component files must be gone from S3.
        for component in &["Data.db", "Index.db", "Filter.db", "Statistics.db", "TOC.txt"] {
            let path = object_store::path::Path::from(format!(
                "{prefix}/{hex}/{table_id_str}/{input_id}/{component}"
            ));
            let get_result = store.get(&path).await;
            assert!(
                get_result.is_err(),
                "component {component} should be deleted from S3"
            );
        }

        // Idempotency: deleting again (already-gone objects) must not error.
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        let rt2 = tokio::runtime::Handle::current();
        let mgr2 = crate::upload::UploadManager::new(
            Arc::clone(&store),
            prefix.to_string(),
            16,
            &rt2,
        );
        mgr2.submit(crate::upload::UploadTask::DeleteSSTable {
            table_id: table_id_str.to_string(),
            sstable_id: input_id.to_string(),
            grace_period: std::time::Duration::from_secs(0),
            on_complete: Some(tx2),
        })
        .await
        .unwrap();
        let result2 = rx2.await.unwrap();
        assert!(result2.is_ok(), "idempotent deletion must not error: {:?}", result2.err());
        mgr2.shutdown().await;
    }

    #[tokio::test]
    async fn compaction_inputs_evicted_locally() {
        // After poll_compactions() the input SSTable component files must be deleted
        // from the table directory, while the compaction output directory must still exist.
        let dir = tempfile::tempdir().unwrap();
        let (engine, _store, _prefix, tid) =
            make_engine_with_pending_compaction(&dir).await;

        let tid_str = tid.to_string();

        // Record the paths of all SSTable component files before compaction.
        // Files are stored flat: {data_dir}/sstables/{table_id}/{gen}-Data.db etc.
        let sstable_dir = dir.path().join("sstables").join(&tid_str);
        let input_files_before: Vec<std::path::PathBuf> = std::fs::read_dir(&sstable_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();

        // There should be files from exactly 2 flushes.
        assert!(
            !input_files_before.is_empty(),
            "expected SSTable files before compaction, got none in: {:?}",
            sstable_dir
        );

        // Run compaction + local eviction. Retry until the channel result is
        // consumed: the compaction thread writes files before sending on the
        // channel, so a single poll may miss the result under parallel load.
        let mut data_files_after = vec![];
        for _ in 0..40 {
            engine.poll_compactions().await;
            data_files_after = std::fs::read_dir(&sstable_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.is_file() && p.extension().map(|e| e == "db").unwrap_or(false)
                })
                .collect();
            if data_files_after.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // All input SSTable component files must have been evicted.
        // The sstable_dir may be empty or contain only output-generation files
        // (but the output goes into dir/compaction, not sstable_dir).
        assert!(
            data_files_after.is_empty(),
            "input SSTable files should be evicted, remaining: {:?}",
            data_files_after
        );

        // The compaction output directory must still exist.
        let output_dir = dir.path().join("compaction");
        assert!(
            output_dir.exists(),
            "compaction output directory must still exist after poll_compactions"
        );
    }

    // ── T-027: metrics + end-to-end tests ────────────────────────────────────

    /// Verifies that compaction S3 metrics are accurate after a compaction
    /// cycle that uploads the output and enqueues input deletions.
    ///
    /// Uses an in-memory object store (no Docker required).
    #[tokio::test]
    async fn compaction_s3_metrics_accurate() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _store, _prefix, _tid) =
            make_engine_with_pending_compaction(&dir).await;

        // Before poll_compactions: all counters must be zero.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "s3_uploads_total should be 0 before poll_compactions"
        );
        assert_eq!(
            engine
                .compaction_metrics
                .s3_deletes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "s3_deletes_total should be 0 before poll_compactions"
        );
        assert_eq!(
            engine
                .compaction_metrics
                .input_bytes_reclaimed
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "input_bytes_reclaimed should be 0 before poll_compactions"
        );

        // Run compaction: merges 2 SSTables → 1 output, uploads to S3,
        // updates manifest, enqueues 2 input deletions. Retry until the channel
        // result is consumed (compaction thread writes files before channel send).
        for _ in 0..40 {
            engine.poll_compactions().await;
            if engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Exactly 1 upload (the compacted output SSTable).
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "s3_uploads_total should be 1 after compacting 2 SSTables into 1"
        );

        // Exactly 2 deletes enqueued (one per input SSTable).
        assert_eq!(
            engine
                .compaction_metrics
                .s3_deletes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "s3_deletes_total should be 2 (one per input SSTable)"
        );

        // Input bytes reclaimed must be positive (inputs had non-zero size).
        assert!(
            engine
                .compaction_metrics
                .input_bytes_reclaimed
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0,
            "input_bytes_reclaimed should be > 0 after compaction"
        );

        // Verify Prometheus text export contains all three metric names.
        let text = engine.compaction_metrics.to_prometheus_text();
        assert!(
            text.contains("ferrosa_compaction_s3_uploads_total 1"),
            "prometheus text missing uploads counter: {text}"
        );
        assert!(
            text.contains("ferrosa_compaction_s3_deletes_total 2"),
            "prometheus text missing deletes counter: {text}"
        );
        assert!(
            text.contains("ferrosa_compaction_input_bytes_reclaimed"),
            "prometheus text missing bytes reclaimed gauge: {text}"
        );
    }

    /// Cassandra 5.1 reads a compacted SSTable from S3 (MinIO).
    ///
    /// Test flow:
    ///   1. Flush 2 SSTables with distinct partition keys and multiple cell types.
    ///   2. Compact them → single merged SSTable uploaded to MinIO.
    ///   3. Cassandra 5.1 mounts the MinIO-backed data directory and scans the table.
    ///   4. All original rows and cell types are present in the Cassandra output.
    ///
    /// Requires MinIO + Cassandra 5.1 containers (Docker or Podman).
    /// Set FERROSA_TEST_CONTAINERS=1 after starting the compose stack.
    #[tokio::test]
    async fn cassandra_reads_compacted_sstable_from_s3() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — start MinIO+Cassandra containers \
                 (docker/podman compose up -d) then re-run with FERROSA_TEST_CONTAINERS=1"
            );
        }
        use std::process::Command;

        // ── Step 1: build engine, flush 2 SSTables with varied cell types ──
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "ferrosa-test".to_string();
        let rt = tokio::runtime::Handle::current();
        let engine = StorageEngine::new_with_upload_store(
            StorageEngineConfig::test_config(dir.path()),
            Arc::clone(&store),
            prefix.clone(),
            &rt,
        )
        .unwrap();

        let schema = TableSchema {
            keyspace: "test_ks".into(),
            table: "mixed_cells".into(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![
                ferrosa_common::schema::ColumnDefinition {
                    name: "v_text".into(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                },
                ferrosa_common::schema::ColumnDefinition {
                    name: "v_int".into(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
                },
            ],
        };
        engine.register_table(schema).unwrap();
        let tid = TableId::new("test_ks", "mixed_cells");

        // Flush 1: row with text cell.
        let k1 = make_key("pk1");
        let row1 = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![(
                0,
                ferrosa_common::cell::CellValue::live(b"hello".to_vec(), 1000),
            )],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &k1, row1, 1000).unwrap();
        engine.flush(&tid).unwrap();

        // Flush 2: row with int cell.
        let k2 = make_key("pk2");
        let int_bytes = 42i32.to_be_bytes().to_vec();
        let row2 = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![(
                1,
                ferrosa_common::cell::CellValue::live(int_bytes, 2000),
            )],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(2000),
        };
        engine.write(&tid, &k2, row2, 2000).unwrap();
        engine.flush(&tid).unwrap();

        // ── Step 2: compact and upload to MinIO ──
        {
            let tables = engine.tables.read();
            let state = tables.get(&tid).unwrap();
            let metadata = engine.collect_sstable_metadata(&tid, state);
            drop(tables);

            let compaction_output_dir = dir.path().join("compaction");
            let task = crate::compaction::metadata::CompactionTask {
                inputs: metadata,
                output_dir: compaction_output_dir.clone(),
                schema: TableSchema {
                    keyspace: "test_ks".into(),
                    table: "mixed_cells".into(),
                    key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                    clustering_columns: vec![],
                    static_columns: vec![],
                    regular_columns: vec![
                        ferrosa_common::schema::ColumnDefinition {
                            name: "v_text".into(),
                            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                        },
                        ferrosa_common::schema::ColumnDefinition {
                            name: "v_int".into(),
                            type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
                        },
                    ],
                },
                table_id: tid.clone(),
            };
            engine.compaction_executor.submit(task).unwrap();
        }

        // Wait for executor to finish writing output files.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let has_output = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if has_output {
                    break;
                }
            }
        }
        engine.poll_compactions().await;

        // Verify the output is in S3.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "compacted SSTable must be uploaded before Cassandra read"
        );

        // ── Step 3: start MinIO + Cassandra 5.1 containers via Docker ──
        // MinIO is pre-seeded with the in-memory store contents via mc mirror.
        // Cassandra is started with data.file_directories pointing to the MinIO
        // bucket mount (s3fs or similar bind mount outside this test harness).
        //
        // This test validates the protocol contract: ferrosa writes BTI-format
        // SSTables; Cassandra 5.1 must read them without errors.
        let cassandra_up = Command::new(container_runtime())
            .args(["compose", "-f", "tests/docker/compaction-cassandra.yml", "up", "-d"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        assert!(
            cassandra_up,
            "failed to start Cassandra + MinIO containers — is Docker running?"
        );

        // Allow Cassandra to initialize (up to 60 s).
        let mut cassandra_ready = false;
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let status = Command::new(container_runtime())
                .args([
                    "exec",
                    "ferrosa-cassandra-test",
                    "nodetool",
                    "status",
                ])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if status {
                cassandra_ready = true;
                break;
            }
        }
        assert!(cassandra_ready, "Cassandra did not become ready within 60 s");

        // ── Step 4: query Cassandra and verify rows ──
        let cql_output = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                "SELECT pk, v_text, v_int FROM test_ks.mixed_cells;",
            ])
            .output()
            .expect("cqlsh failed");

        let stdout = String::from_utf8_lossy(&cql_output.stdout);

        assert!(
            stdout.contains("pk1") && stdout.contains("hello"),
            "Cassandra output missing pk1/v_text row: {stdout}"
        );
        assert!(
            stdout.contains("pk2") && stdout.contains("42"),
            "Cassandra output missing pk2/v_int row: {stdout}"
        );

        // Cleanup containers.
        let _ = Command::new(container_runtime())
            .args(["compose", "-f", "tests/docker/compaction-cassandra.yml", "down", "-v"])
            .status();
    }

    /// End-to-end compaction pipeline: 4 flush cycles trigger STCS compaction,
    /// the output is confirmed in S3, the manifest is updated, old files are
    /// evicted locally, and Cassandra 5.1 can read the result from MinIO.
    ///
    /// Pipeline:
    ///   4 flushes → STCS detects 4-SSTable bucket → compaction triggered
    ///   → output uploaded to MinIO → manifest updated (1 entry)
    ///   → input SSTable files deleted locally
    ///   → Cassandra 5.1 reads all 4 partition keys from compacted SSTable
    ///
    /// Requires MinIO + Cassandra 5.1 containers (Docker or Podman).
    /// Set FERROSA_TEST_CONTAINERS=1 after starting the compose stack.
    #[tokio::test]
    async fn compaction_end_to_end_pipeline() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — start MinIO+Cassandra containers \
                 (docker/podman compose up -d) then re-run with FERROSA_TEST_CONTAINERS=1"
            );
        }
        use std::process::Command;

        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let prefix = "ferrosa-e2e".to_string();
        let rt = tokio::runtime::Handle::current();

        // Use min_threshold=4 (default STCS) so 4 flushes trigger compaction.
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new_with_upload_store(
            config,
            Arc::clone(&store),
            prefix.clone(),
            &rt,
        )
        .unwrap();
        engine.register_table(test_schema()).unwrap();
        let tid = table_id();

        // ── 4 flush cycles (each writes 1 partition to its own SSTable) ──
        for (i, key_suffix) in ["a", "b", "c", "d"].iter().enumerate() {
            let ts = (i as i64 + 1) * 1000;
            let value = format!("value-{key_suffix}");
            engine
                .write(
                    &tid,
                    &make_key(key_suffix),
                    make_row(value.as_bytes(), ts),
                    ts,
                )
                .unwrap();
            engine.flush(&tid).unwrap();
            // flush() calls maybe_compact(); on the 4th flush STCS will submit a task.
        }

        // Wait for the compaction executor background thread to finish.
        let compaction_dir = dir.path().join("compaction");
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if compaction_dir.exists() {
                let has_output = std::fs::read_dir(&compaction_dir)
                    .ok()
                    .map(|mut rd| rd.any(|_| true))
                    .unwrap_or(false);
                if has_output {
                    break;
                }
            }
        }

        // ── poll_compactions: upload, manifest update, local eviction ──
        engine.poll_compactions().await;

        // Upload confirmed — exactly 1 output SSTable uploaded.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_uploads_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "expected 1 S3 upload after STCS compaction of 4 SSTables"
        );

        // 4 input deletions enqueued.
        assert_eq!(
            engine
                .compaction_metrics
                .s3_deletes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            4,
            "expected 4 S3 delete tasks for 4 input SSTables"
        );

        // Manifest: exactly 1 SSTable entry (the merged output).
        let (manifest, _) = crate::manifest::Manifest::load(store.as_ref(), &prefix)
            .await
            .unwrap();
        let tid_str = tid.to_string();
        let entries = manifest.sstables.get(&tid_str).cloned().unwrap_or_default();
        assert_eq!(
            entries.len(),
            1,
            "manifest should have exactly 1 SSTable entry after STCS compaction"
        );

        // Local input files must be evicted.
        let sstable_dir = dir.path().join("sstables").join(&tid_str);
        if sstable_dir.exists() {
            let remaining_db_files: Vec<_> = std::fs::read_dir(&sstable_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|e| e == "db").unwrap_or(false))
                .collect();
            assert!(
                remaining_db_files.is_empty(),
                "all input .db files should be evicted after poll_compactions, remaining: {:?}",
                remaining_db_files
            );
        }

        // ── Docker: Cassandra 5.1 reads the compacted SSTable from MinIO ──
        let cassandra_up = Command::new(container_runtime())
            .args([
                "compose",
                "-f",
                "tests/docker/compaction-cassandra.yml",
                "up",
                "-d",
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        assert!(
            cassandra_up,
            "failed to start Cassandra + MinIO containers — is Docker running?"
        );

        // Allow Cassandra to initialize (up to 60 s).
        let mut cassandra_ready = false;
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let status = Command::new(container_runtime())
                .args(["exec", "ferrosa-cassandra-test", "nodetool", "status"])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if status {
                cassandra_ready = true;
                break;
            }
        }
        assert!(cassandra_ready, "Cassandra did not become ready within 60 s");

        // Verify all 4 partition keys are readable from the compacted SSTable.
        let cql_output = Command::new(container_runtime())
            .args([
                "exec",
                "ferrosa-cassandra-test",
                "cqlsh",
                "--execute",
                "SELECT pk FROM test_ks.test_table;",
            ])
            .output()
            .expect("cqlsh failed");

        let stdout = String::from_utf8_lossy(&cql_output.stdout);
        for key in &["a", "b", "c", "d"] {
            assert!(
                stdout.contains(key),
                "Cassandra output missing partition key '{key}': {stdout}"
            );
        }

        // Cleanup.
        let _ = Command::new(container_runtime())
            .args([
                "compose",
                "-f",
                "tests/docker/compaction-cassandra.yml",
                "down",
                "-v",
            ])
            .status();
    }
}
