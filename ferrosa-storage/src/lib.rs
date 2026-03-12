//! Single-node storage engine for Ferrosa.
//!
//! Accepts writes into an in-memory buffer (memtable), flushes to SSTables,
//! and merges reads across all sources. The read path is entirely wait-free
//! via lock-free atomic pointer swaps.

pub mod flush;
pub mod memtable;
pub mod merge;
pub mod store;
