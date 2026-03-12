//! Single-node storage engine for Ferrosa.
//!
//! Accepts writes into an in-memory buffer (memtable), flushes to SSTables,
//! and merges reads across all sources. The read path is entirely wait-free
//! via lock-free atomic pointer swaps.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │ TableStore (ArcSwap<StoreView>)             │
//! │ ┌─────────────┐  ┌──────────┐  ┌─────────┐ │
//! │ │   Active    │  │ Flushing │  │ SSTables│ │
//! │ │  Memtable   │  │ Memtable │  │ (Vec)   │ │
//! │ └─────────────┘  └──────────┘  └─────────┘ │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! - **Write path**: `ArcSwap::load()` (wait-free) → memtable `put()` (one shard lock)
//! - **Read path**: `ArcSwap::load()` (wait-free) → check all sources → `merge_partitions()`
//! - **Flush path**: `Mutex` serializes flushes; two brief `ArcSwap::store()` calls

pub mod commitlog;
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
