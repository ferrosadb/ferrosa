//! Index build pipeline and staleness tracking.
//!
//! Manages per-index state tracking and background index build scheduling.
//! The [`IndexStateTracker`] tracks which SSTables have been indexed and which
//! are pending.

pub mod tracker;

pub use tracker::{IndexState, IndexStateTracker, IndexStatus};
