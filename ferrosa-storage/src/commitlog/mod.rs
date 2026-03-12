//! Commit log (write-ahead log) for durability.
//!
//! The commit log records every mutation before it reaches the memtable.
//! On crash recovery, uncommitted mutations are replayed from segment
//! files to restore memtable state.

pub(crate) mod config;
pub(crate) mod descriptor;
pub(crate) mod mutation;
pub(crate) mod segment;
pub(crate) mod sync;

pub use config::{CommitLogConfig, CommitLogPosition, SyncStrategyConfig, TableId};
pub use mutation::Mutation;
