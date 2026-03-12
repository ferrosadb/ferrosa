//! Single-node storage engine for Ferrosa.
//!
//! # Components
//!
//! - **Memtable**: in-memory write buffer (Part A)
//! - **Flush**: memtable → SSTable (Part A)
//! - **Merge**: read-path merge across sources (Part A)
//! - **TableStore**: lock-free composition (Part A)
//! - **CommitLog**: write-ahead log for durability (Part B)

pub mod commitlog;
pub mod compaction;
pub mod flush;
pub mod memtable;
pub mod merge;
pub mod store;

pub use commitlog::{
    CommitLog, CommitLogConfig, CommitLogPosition, Mutation, SyncStrategyConfig, TableId,
};
pub use flush::{FileFlushTarget, FlushTarget, InMemoryFlushTarget};
pub use memtable::sharded::ShardedBTreeMemtable;
pub use memtable::Memtable;
pub use merge::merge_partitions;
pub use store::TableStore;
