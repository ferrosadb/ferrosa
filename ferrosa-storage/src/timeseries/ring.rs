//! Ring buffer for time-series data points.
//!
//! Stub implementation for Sprint R-1 dependency. Will be replaced when
//! Sprint R-1's work is merged.

use smallvec::SmallVec;

/// Status returned by [`RingBuffer::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryStatus {
    /// The data point was inserted normally within the current window.
    Normal,
    /// The data point crossed a window boundary, triggering consolidation.
    BoundaryCrossed,
    /// The data point is late (before the current window boundary).
    LateData,
}

/// A single entry in the ring buffer.
#[derive(Debug, Clone)]
pub struct RingEntry {
    /// Timestamp in microseconds since epoch.
    pub timestamp: i64,
    /// Extracted numeric values (one per configured column).
    pub values: SmallVec<[f64; 8]>,
}

/// Circular buffer for time-series data within a single partition.
///
/// Tracks the current window boundary and supports efficient insertion
/// with automatic boundary crossing detection. Uses `last_access` for
/// LRU eviction of cold partitions.
pub struct RingBuffer {
    /// Entries stored in the ring.
    entries: Vec<RingEntry>,
    /// Maximum capacity of the ring.
    capacity: usize,
    /// Write position (wraps around).
    write_pos: usize,
    /// Number of valid entries (up to capacity).
    count: usize,
    /// Current window boundary timestamp (microseconds).
    boundary_ts: i64,
    /// Consolidation interval in microseconds.
    interval_micros: i64,
    /// Column indices being tracked.
    _column_indices: Vec<u16>,
    /// Last access time for LRU eviction.
    last_access: std::time::Instant,
}

impl RingBuffer {
    /// Create a new ring buffer with the given capacity and interval.
    pub fn new(capacity: usize, interval_micros: i64, column_indices: Vec<u16>) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            capacity,
            write_pos: 0,
            count: 0,
            boundary_ts: 0,
            interval_micros,
            _column_indices: column_indices,
            last_access: std::time::Instant::now(),
        }
    }

    /// Returns the current window boundary timestamp.
    pub fn boundary_ts(&self) -> i64 {
        self.boundary_ts
    }

    /// Returns the last access time (for LRU eviction).
    pub fn last_access(&self) -> std::time::Instant {
        self.last_access
    }

    /// Insert a data point into the ring buffer.
    ///
    /// Returns the boundary status indicating whether this insertion crossed
    /// a window boundary or represents late data.
    pub fn insert(&mut self, timestamp: i64, values: SmallVec<[f64; 8]>) -> BoundaryStatus {
        self.last_access = std::time::Instant::now();

        let entry = RingEntry { timestamp, values };

        // First entry: initialize boundary.
        if self.count == 0 && self.boundary_ts == 0 {
            // Align boundary to interval.
            self.boundary_ts = ((timestamp / self.interval_micros) + 1) * self.interval_micros;
            self.push_entry(entry);
            return BoundaryStatus::Normal;
        }

        // Check for late data (before current window start).
        let window_start = self.boundary_ts - self.interval_micros;
        if timestamp < window_start {
            // Still insert the entry, but flag as late.
            self.push_entry(entry);
            return BoundaryStatus::LateData;
        }

        // Check for boundary crossing.
        if timestamp >= self.boundary_ts {
            self.push_entry(entry);
            // Advance boundary.
            let new_boundary = ((timestamp / self.interval_micros) + 1) * self.interval_micros;
            self.boundary_ts = new_boundary;
            return BoundaryStatus::BoundaryCrossed;
        }

        // Normal insertion within current window.
        self.push_entry(entry);
        BoundaryStatus::Normal
    }

    /// Push an entry into the ring, wrapping if at capacity.
    fn push_entry(&mut self, entry: RingEntry) {
        if self.count < self.capacity {
            self.entries.push(entry);
            self.count += 1;
        } else {
            self.entries[self.write_pos] = entry;
        }
        self.write_pos = (self.write_pos + 1) % self.capacity;
    }

    /// Returns owned entries within the given timestamp range [start, end).
    pub fn window_owned(&self, start: i64, end: i64) -> Vec<RingEntry> {
        self.entries
            .iter()
            .filter(|e| e.timestamp >= start && e.timestamp < end)
            .cloned()
            .collect()
    }

    /// Returns the number of entries currently in the ring.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Returns true if the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_insert_normal() {
        let mut ring = RingBuffer::new(64, 10_000_000, vec![0]);
        let status = ring.insert(5_000_000, SmallVec::from_slice(&[42.0]));
        assert_eq!(status, BoundaryStatus::Normal);
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.boundary_ts(), 10_000_000);
    }

    #[test]
    fn ring_buffer_boundary_crossing() {
        let mut ring = RingBuffer::new(64, 10_000_000, vec![0]);
        ring.insert(5_000_000, SmallVec::from_slice(&[1.0]));
        let status = ring.insert(10_000_000, SmallVec::from_slice(&[2.0]));
        assert_eq!(status, BoundaryStatus::BoundaryCrossed);
    }

    #[test]
    fn ring_buffer_late_data() {
        let mut ring = RingBuffer::new(64, 10_000_000, vec![0]);
        // First entry at 15M -> boundary at 20M, window_start at 10M.
        ring.insert(15_000_000, SmallVec::from_slice(&[1.0]));
        // Cross boundary to 30M.
        ring.insert(25_000_000, SmallVec::from_slice(&[2.0]));
        // Late data at 5M (before window_start 20M).
        let status = ring.insert(5_000_000, SmallVec::from_slice(&[3.0]));
        assert_eq!(status, BoundaryStatus::LateData);
    }

    #[test]
    fn ring_buffer_window_owned() {
        let mut ring = RingBuffer::new(64, 10_000_000, vec![0]);
        ring.insert(1_000_000, SmallVec::from_slice(&[1.0]));
        ring.insert(5_000_000, SmallVec::from_slice(&[2.0]));
        ring.insert(15_000_000, SmallVec::from_slice(&[3.0]));

        let window = ring.window_owned(0, 10_000_000);
        assert_eq!(window.len(), 2);
    }
}
