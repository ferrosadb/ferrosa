//! Accord transaction protocol support for ferrosa-storage.
//!
//! This module provides:
//!
//! - [`ConflictIndex`]: per-shard conflict detection with O(1) single-key
//!   and O(log n) range lookups for in-flight transactions.
//! - [`ProtocolLog`]: local-only log for PreAccepted, Accepted, and Committed
//!   entries. Never uploaded to S3.
//! - [`SyncWriter`]: fsync-before-ack wrapper ensuring durability before
//!   protocol replies are sent.
//! - [`check_write_gate`]: prevents non-transactional writes from bypassing
//!   Accord's ConflictIndex.
//! - [`CrashRecoveryReplay`]: replays persisted entries on startup to
//!   reconstruct Accord state after a crash.
//! - [`AccordSidecar`]: companion `.accord` files written alongside SSTables
//!   to record which transactions were applied in each SSTable.

pub mod conflict_index;
pub mod crash_recovery;
pub mod entries;
pub mod protocol_log;
pub mod read_2i;
pub mod sidecar;
pub mod sync_writer;
pub mod write_gate;

pub use conflict_index::{ConflictIndex, ConflictIndexFull, InFlightWrite, TokenRange, TxnStatus};
pub use crash_recovery::{
    CrashRecoveryReplay, ReplayedConflictEntry, ReplayedPhase, ReplayedTxnState,
};
pub use entries::{AccordAppliedEntry, AccordProtocolEntry};
pub use protocol_log::ProtocolLog;
pub use read_2i::{
    ConsistencyMode, DepWaitOutcome, IndexResult, LayerId, Read2iMerger, Read2iQuery,
};
pub use sidecar::{AccordSidecar, SidecarUploadManifest, SIDECAR_EXTENSION};
pub use sync_writer::{FileSyncWriter, MockSyncWriter, SyncWriteResult, SyncWriter};
pub use write_gate::{check_write_gate, check_write_gate_range, WriteGateDecision};
