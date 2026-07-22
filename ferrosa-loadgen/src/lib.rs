//! Load testing infrastructure for UCS compaction end-to-end verification.
//!
//! Provides property-based load generators with three profiles (read-heavy,
//! balanced, write-heavy), a thread-safe ground truth tracker, stats collection,
//! integrity verification, and orchestration.

pub mod cluster;
pub mod generator;
pub mod ground_truth;
pub mod integrity;
pub mod orchestrator;
pub mod profile;
pub mod resource_monitor;
pub mod scan_storm;
pub mod stats;
pub mod tui;

pub use ground_truth::GroundTruth;
pub use integrity::{IntegrityReport, IntegrityVerifier};
pub use orchestrator::{run_load_test, run_load_test_with_tui};
pub use profile::LoadProfile;
pub use resource_monitor::{ResourceMonitor, ResourceSummary};
pub use stats::{LoadStats, StatsCollector, StatsSnapshot};
