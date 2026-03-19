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
    /// Optional index build scheduler — wiring to flush/compaction is deferred.
    #[allow(dead_code)]
    index_scheduler: Option<crate::index::IndexBuildScheduler>,
    /// Background archiver task handle, if archiving is enabled.
    archiver_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Per-table state: schema + store.
struct TableState {
    #[allow(dead_code)]
    schema: TableSchema,
    store: TableStore<FileFlushTarget>,
}

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
            index_scheduler: None,
            archiver_handle: None,
        })
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
            index_scheduler: None,
            archiver_handle,
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
            index_scheduler: None,
            archiver_handle: None,
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

    /// Registers a table schema so the engine can accept writes for it.
    ///
    /// Creates the per-table `FileFlushTarget` directory and `TableStore`.
    /// If the directory already contains SSTable files from a previous run,
    /// they are opened and loaded into the store so reads work immediately
    /// after re-opening the engine (crash recovery path).
    pub fn register_table(&self, schema: TableSchema) -> ferrosa_common::Result<()> {
        let table_id = TableId::new(&schema.keyspace, &schema.table);
        let table_dir = self
            .config
            .data_dir
            .join("sstables")
            .join(table_id.to_string());
        std::fs::create_dir_all(&table_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!("failed to create table dir: {e}"))
        })?;

        // Load any SSTables that already exist on disk (e.g., after crash recovery).
        let existing_sstables = Self::load_existing_sstables(&table_dir);

        let flush_target = FileFlushTarget::new_starting_at(table_dir)?;
        let store = if existing_sstables.is_empty() {
            TableStore::new(schema.clone(), flush_target, WriteOptions::default())
        } else {
            TableStore::new_with_sstables(
                schema.clone(),
                flush_target,
                WriteOptions::default(),
                existing_sstables,
            )
        };

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
    /// Called when `CREATE INDEX` is processed. Updates the `TableStore`'s
    /// `indexed_columns` so future writes are indexed in the memtable.
    /// The write lock on `self.tables` guarantees exclusive access to the
    /// `TableStore`, so `TableStore::add_index` can take `&mut self`.
    pub fn add_index(
        &self,
        table_id: &TableId,
        index_name: &str,
        column_position: usize,
    ) -> ferrosa_common::Result<()> {
        let mut tables = self.tables.write();
        let state = tables.get_mut(table_id).ok_or_else(|| {
            ferrosa_common::Error::InvalidFormat(format!("table not registered: {table_id}"))
        })?;
        state
            .store
            .add_index(index_name.to_string(), column_position);
        Ok(())
    }

    /// Scans a table directory for existing SSTable files and opens them.
    ///
    /// Returns readers ordered newest-first (by generation number descending).
    /// Files that fail to open are silently skipped — a corrupted SSTable is
    /// better handled at compaction time than at startup.
    fn load_existing_sstables(
        table_dir: &std::path::Path,
    ) -> Vec<Arc<ferrosa_sstable::reader::SSTableReader<ferrosa_sstable::io::FileReadAt>>> {
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

        generations
            .into_iter()
            .filter_map(|gen| {
                let gen_str = gen.to_string();
                match Self::open_sstable_from_dir(table_dir, &gen_str) {
                    Ok(reader) => Some(Arc::new(reader)),
                    Err(e) => {
                        eprintln!(
                            "[storage-engine] skipping corrupt SSTable gen {gen} in {}: {e}",
                            table_dir.display()
                        );
                        None
                    }
                }
            })
            .collect()
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

    /// Polls for completed compaction results and integrates them.
    pub fn poll_compactions(&self) {
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
            let tables = self.tables.read();
            if let Some(state) = tables.get(table_id) {
                if let Err(e) = state.store.swap_compacted_sstables(input_count, reader) {
                    eprintln!("[compaction] swap failed: {e}");
                }
            }

            // Register in local cache.
            self.local_cache.register(
                &result.output.id,
                result.output.path.clone(),
                result.output.size_bytes,
            );
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
            manifest.save_with_retry(store.as_ref(), &prefix).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::PartitionKey;
    use ferrosa_common::schema::ColumnDefinition;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

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

    #[test]
    fn concurrent_read_during_compaction() {
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
        std::thread::sleep(std::time::Duration::from_millis(10));
        engine.poll_compactions();

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
                .save_with_retry(store.as_ref(), prefix)
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

    #[test]
    fn list_and_delete_snapshots_via_engine() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store: std::sync::Arc<dyn object_store::ObjectStore> =
                std::sync::Arc::new(object_store::memory::InMemory::new());
            let prefix = "test-node";

            // Save manifest + schema so create_snapshot works.
            let manifest = crate::manifest::Manifest::new();
            manifest
                .save_with_retry(store.as_ref(), prefix)
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

            // Create two snapshots.
            engine
                .create_snapshot_with_store(
                    "snap-a",
                    "n1",
                    None,
                    false,
                    std::sync::Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();
            engine
                .create_snapshot_with_store(
                    "snap-b",
                    "n1",
                    None,
                    false,
                    std::sync::Arc::clone(&store),
                    prefix,
                )
                .await
                .unwrap();

            // List — should see both.
            let snaps = engine
                .list_snapshots_with_store(std::sync::Arc::clone(&store), prefix)
                .await
                .unwrap();
            assert_eq!(snaps.len(), 2);
            let names: Vec<&str> = snaps.iter().map(|s| s.name.as_str()).collect();
            assert!(names.contains(&"snap-a"));
            assert!(names.contains(&"snap-b"));

            // Delete one.
            engine
                .delete_snapshot_with_store("snap-a", std::sync::Arc::clone(&store), prefix)
                .await
                .unwrap();

            // List — should see only one.
            let snaps = engine
                .list_snapshots_with_store(std::sync::Arc::clone(&store), prefix)
                .await
                .unwrap();
            assert_eq!(snaps.len(), 1);
            assert_eq!(snaps[0].name, "snap-b");

            engine.shutdown().unwrap();
        });
    }

    // =========================================================================
    // Task 3.5: StorageEngine::add_index
    // =========================================================================

    fn test_schema_with_email() -> TableSchema {
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
                name: "email".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        }
    }

    #[test]
    fn engine_add_index_enables_memtable_indexing() {
        use ferrosa_index::IndexKey;

        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(test_schema_with_email()).unwrap();

        let tid = table_id();

        // Add the index via engine
        engine.add_index(&tid, "email_idx", 0).unwrap();

        // Write a row with the indexed column
        let key = make_key("user1");
        let row = Row {
            clustering: vec![0x00, 0x00, 0x00, 0x01],
            cells: vec![(
                0,
                ferrosa_common::cell::CellValue::live(b"alice@example.com".to_vec(), 1000),
            )],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000),
        };
        engine.write(&tid, &key, row, 1000).unwrap();

        // Read by index — should find the row
        let results = engine
            .read_by_index(&tid, "email_idx", &IndexKey(b"alice@example.com".to_vec()))
            .unwrap();
        assert_eq!(
            results.len(),
            1,
            "indexed write should be findable via engine.read_by_index"
        );
        assert_eq!(results[0].key.key.as_bytes(), b"user1");
    }

    #[test]
    fn engine_add_index_on_unregistered_table_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = StorageEngineConfig::test_config(dir.path());
        let engine = StorageEngine::new(config, None).unwrap();

        let result = engine.add_index(&TableId::new("no", "such"), "idx", 0);
        assert!(
            result.is_err(),
            "add_index on unregistered table should fail"
        );
    }
}
