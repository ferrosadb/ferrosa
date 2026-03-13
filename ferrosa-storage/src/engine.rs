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
use crate::commitlog::config::{CommitLogConfig, TableId};
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
    commit_log: CommitLog,
    compaction_executor: CompactionExecutor,
    upload_manager: Option<UploadManager>,
    local_cache: LocalCache,
    observers: RwLock<Vec<Arc<dyn crate::observer::WriteObserver>>>,
    async_observers: RwLock<Vec<AsyncObserverState>>,
    /// Default channel capacity for async observers.
    async_observer_capacity: usize,
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
        })
    }

    /// Registers a table schema so the engine can accept writes for it.
    ///
    /// Creates the per-table `FileFlushTarget` directory and `TableStore`.
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

        let flush_target = FileFlushTarget::new(table_dir)?;
        let store = TableStore::new(schema.clone(), flush_target, WriteOptions::default());

        let state = TableState { schema, store };
        self.tables.write().insert(table_id, state);
        Ok(())
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
            // Register the output SSTable in the local cache.
            self.local_cache.register(
                &result.output.id,
                result.output.path.clone(),
                result.output.size_bytes,
            );
        }
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
        _table_id: &TableId,
        state: &TableState,
    ) -> Vec<crate::compaction::metadata::SSTableMetadata> {
        // For now, return empty — full metadata collection requires
        // iterating the SSTable list and reading stats from each.
        // This will be wired up when SSTableReader exposes stats.
        let _ = state.store.sstable_count();
        Vec::new()
    }

    /// Returns a reference to the local cache.
    pub fn local_cache(&self) -> &LocalCache {
        &self.local_cache
    }

    /// Returns a reference to the upload manager, if S3 is configured.
    pub fn upload_manager(&self) -> Option<&UploadManager> {
        self.upload_manager.as_ref()
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
}
