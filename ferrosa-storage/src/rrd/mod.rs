//! RRD-style segmented tables: one immutable segment per time window, retention
//! by unlinking a whole segment, and no compaction at all.
//!
//! A table that sets `rrd.segment_window` stores its rows in non-overlapping
//! time-bounded segments. Because segments never overlap in time, row order
//! across segments is already the read order and there is nothing to merge, so
//! the table is exempt from compaction (see `crate::rrd::config::RrdConfig`).
//! Retention then stops being a row-level fact: a segment whose window ended
//! before `now - rrd.retain` is removed from the manifest and its file deleted.

pub mod config;

pub use config::RrdConfig;
