//! Cache-aligned ring buffer for time-series data points.
//!
//! Each `(table, partition_key)` gets its own [`RingBuffer`]. The buffer base is
//! cache-line-aligned; individual entries are packed for density.
//! `RingBuffer` is NOT `Sync` -- relies on DashMap per-shard lock for mutual exclusion.
