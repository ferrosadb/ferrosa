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

pub mod cache;
pub mod commitlog;
pub mod compaction;
pub mod engine;
pub mod flush;
pub mod manifest;
pub mod memtable;
pub mod merge;
pub mod store;
pub mod upload;

pub use cache::LocalCache;
pub use commitlog::{
    CommitLog, CommitLogConfig, CommitLogPosition, Mutation, SyncStrategyConfig, TableId,
};
pub use compaction::{
    CompactionConfig, CompactionExecutor, CompactionStrategy, SizeTieredStrategy,
};
pub use engine::{StorageEngine, StorageEngineConfig};
pub use flush::{FileFlushTarget, FlushTarget, InMemoryFlushTarget};
pub use manifest::Manifest;
pub use memtable::sharded::ShardedBTreeMemtable;
#[cfg(feature = "skiplist-memtable")]
pub use memtable::skiplist::SkipListMemtable;
pub use memtable::Memtable;
pub use merge::merge_partitions;
pub use store::TableStore;
pub use upload::{ObjectStoreConfig, UploadManager};
