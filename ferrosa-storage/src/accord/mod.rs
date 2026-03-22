//! Accord transaction support for ferrosa-storage.
//!
//! This module provides the per-shard conflict detection index used by the
//! Accord distributed transaction protocol. The [`ConflictIndex`] tracks
//! in-flight transactions and provides O(1) single-key and O(log n) range
//! conflict lookups.

pub mod conflict_index;

pub use conflict_index::{ConflictIndex, ConflictIndexFull, InFlightWrite, TokenRange, TxnStatus};
