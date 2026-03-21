//! Cascading time-series aggregation (RRD-style).
//!
//! Provides automatic consolidation of high-frequency time-series data into
//! coarser-grained tables using configurable aggregation functions. Consolidation
//! is driven by data timestamps, not wall clock.

pub mod aggregator;
pub mod config;
pub mod consolidation;
pub mod ring;
