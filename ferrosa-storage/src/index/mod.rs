//! Index build pipeline and staleness tracking.
//!
//! Manages per-index state tracking and background index build scheduling.
//! The [`IndexStateTracker`] tracks which SSTables have been indexed and which
//! are pending, while the [`IndexBuildScheduler`] processes build jobs on
//! background worker threads following the same channel-based pattern as
//! [`CompactionExecutor`](crate::compaction::executor::CompactionExecutor).

pub mod scheduler;
pub mod tracker;
pub mod virtual_table;

pub use scheduler::{BuildPriority, IndexBuildJob, IndexBuildScheduler};
pub use tracker::{IndexState, IndexStateTracker, IndexStatus};
