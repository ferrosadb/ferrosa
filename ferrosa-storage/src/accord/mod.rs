//! Accord transaction protocol support for ferrosa-storage.
//!
//! This module provides:
//!
//! - [`ConflictIndex`]: per-shard conflict detection with O(1) single-key
//!   and O(log n) range lookups for in-flight transactions.
//! - **Protocol log** ([`ProtocolLog`]): local-only log for PreAccepted,
//!   Accepted, and Committed entries. Never uploaded to S3.
//! - **Main commit log**: existing [`CommitLog`](crate::CommitLog) receives
//!   [`AccordAppliedEntry`] data after transaction execution.

pub mod conflict_index;
pub mod entries;
pub mod protocol_log;

pub use conflict_index::{ConflictIndex, ConflictIndexFull, InFlightWrite, TokenRange, TxnStatus};
pub use entries::{AccordAppliedEntry, AccordProtocolEntry};
pub use protocol_log::ProtocolLog;
