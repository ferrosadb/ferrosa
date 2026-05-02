//! Single-node storage engine for Ferrosa.
//!
//! # Components
//!
//! - **Memtable**: in-memory write buffer (Part A)
//! - **Flush**: memtable → SSTable (Part A)
//! - **Merge**: read-path merge across sources (Part A)
//! - **TableStore**: lock-free per-table composition (Part A)
//! - **CommitLog**: write-ahead log for durability (Part B)
//! - **Compaction**: size-tiered compaction strategy (Part C)
//! - **Upload**: async S3-compatible upload via `object_store` (Part C)
//! - **Manifest**: S3 manifest with etag-based CAS (Part C)
//! - **Cache**: local disk cache with LRU eviction (Part C)
//! - **Engine**: top-level `StorageEngine` composing all components (Part C)
//! - **Accord**: per-shard conflict detection for Accord transactions

pub mod accord;
pub mod batchlog;
pub mod cache;
pub mod commitlog;
pub mod compaction;
pub mod data_store;
pub mod engine;
pub mod flush;
pub mod index;
pub mod manifest;
pub mod memtable;
pub mod merge;
pub mod metrics;
pub mod observer;
pub mod pin_config;
pub mod quarantine;
pub mod restore;
pub mod snapshot;
pub mod store;
pub mod subscription_observer;
pub mod timeseries;
pub mod upload;
pub mod virtual_tables;

pub use batchlog::{BatchlogConfig, BatchlogEntry, BatchlogManager};
pub use cache::LocalCache;
pub use commitlog::cdc::CdcReader;
pub use commitlog::{
    CommitLog, CommitLogConfig, CommitLogPosition, Mutation, SyncStrategyConfig, TableId,
};
pub use compaction::{
    CompactionConfig, CompactionExecutor, CompactionStrategy, SizeTieredStrategy,
};
pub use data_store::{DataStore, LocalDataStore};
pub use engine::{StorageEngine, StorageEngineConfig};
pub use flush::{FileFlushTarget, FlushTarget, InMemoryFlushTarget};
pub use manifest::{load_schema_snapshot, save_schema_snapshot, Manifest};
pub use memtable::eager_index::{EagerIndexBuilder, FlushCompleteEvent};
pub use memtable::sharded::ShardedBTreeMemtable;
#[cfg(feature = "skiplist-memtable")]
pub use memtable::skiplist::SkipListMemtable;
pub use memtable::Memtable;
pub use merge::merge_partitions;
pub use observer::{ObserverConfig, ObserverMode, WriteObserver};
pub use store::TableStore;
pub use subscription_observer::{
    SubscriptionConfig, SubscriptionFilter, SubscriptionId, SubscriptionObserver,
};
pub use upload::{ObjectStoreConfig, UploadManager};

/// Process-global span collector for tracing tests.
///
/// Installed once via `set_global_default` so tracing callsites are always
/// interned with an active subscriber. Eliminates callsite-caching flakiness
/// where a parallel test could intern a callsite as disabled before a
/// thread-local subscriber was installed.
#[cfg(test)]
pub(crate) mod test_span_collector {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};

    static NAMES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    static INSTALLED: OnceLock<()> = OnceLock::new();

    struct GlobalSpanCollector {
        next_id: AtomicU64,
    }

    impl tracing::Subscriber for GlobalSpanCollector {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            names()
                .lock()
                .unwrap()
                .push(span.metadata().name().to_string());
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::span::Id::from_u64(id)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn names() -> &'static Mutex<Vec<String>> {
        NAMES.get_or_init(|| Mutex::new(Vec::new()))
    }

    /// Ensure the global collector is installed. Idempotent.
    pub fn ensure_installed() {
        INSTALLED.get_or_init(|| {
            let collector = GlobalSpanCollector {
                next_id: AtomicU64::new(0),
            };
            let _ = tracing::subscriber::set_global_default(collector);
        });
    }

    /// Drain all recorded span names since the last call.
    pub fn drain_names() -> Vec<String> {
        names().lock().unwrap().drain(..).collect()
    }
}
