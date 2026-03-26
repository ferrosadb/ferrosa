//! Cascading time-series aggregation (RRD-style).
//!
//! Provides automatic consolidation of high-frequency time-series data into
//! coarser-grained tables using configurable aggregation functions. Consolidation
//! is driven by data timestamps, not wall clock.
//!
//! # Observability
//!
//! [`ConsolidationMetrics`] tracks windows consolidated, late arrivals, and
//! dropped tasks. Use [`ConsolidationMetrics::snapshot()`] to read a
//! point-in-time copy for Prometheus export or virtual table display.

pub mod aggregator;
pub mod config;
pub mod consolidation;
pub mod late_data;
pub mod ring;

// Re-export key types for convenience.
pub use aggregator::{
    ConsolidationMetrics, ConsolidationTask, ConsolidationWorker, MetricsSnapshot,
    TimeSeriesAggregator,
};
pub use config::ConsolidationConfig;
pub use consolidation::ConsolidationFn;
