//! Cache-aligned ring buffer for time-series data points.
//!
//! Each `(table, partition_key)` gets its own [`RingBuffer`]. The buffer base is
//! cache-line-aligned; individual entries are packed for density.
//! `RingBuffer` is NOT `Sync` -- relies on DashMap per-shard lock for mutual exclusion.

use std::time::Instant;

use smallvec::SmallVec;

/// Ring buffer entry. Packed for density -- cache-line alignment is on
/// the buffer base, not individual entries.
#[repr(C)]
#[derive(Clone)]
pub struct RingEntry {
    /// Timestamp in microseconds since epoch.
    pub timestamp: i64,
    /// Numeric values for aggregated columns. Inline for <= 8 columns.
    pub values: SmallVec<[f64; 8]>,
}

/// Cache-line-aligned ring buffer. Power-of-2 capacity for bitwise modulo.
///
/// NOT Sync -- relies on DashMap per-shard lock for mutual exclusion.
pub struct RingBuffer {
    /// Aligned allocation of ring entries.
    entries: Vec<RingEntry>,
    /// `capacity - 1` for bitwise modulo.
    capacity_mask: usize,
    /// Next write position. Count = `min(write_cursor, capacity)`.
    write_cursor: usize,
    /// Next consolidation boundary timestamp (microseconds).
    boundary_ts: i64,
    /// Consolidation interval in microseconds.
    interval_micros: i64,
    /// Which columns to extract from mutations (by column_index).
    column_indices: Vec<u16>,
    /// For LRU eviction.
    last_access: Instant,
}

/// Result of inserting into a ring buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryStatus {
    /// Normal insertion, no boundary crossed.
    Normal,
    /// Timestamp crossed the consolidation boundary.
    BoundaryCrossed,
    /// Timestamp is before the current boundary (late-arriving data).
    LateData,
}

impl RingBuffer {
    /// Create a new ring buffer with the given capacity (rounded up to power of 2).
    ///
    /// `interval_micros` is the consolidation interval in microseconds.
    /// `column_indices` specifies which column indexes to extract from mutations.
    pub fn new(capacity: usize, interval_micros: i64, column_indices: Vec<u16>) -> Self {
        assert!(capacity > 0, "ring buffer capacity must be > 0");
        assert!(interval_micros > 0, "interval must be positive");

        let capacity = capacity.next_power_of_two();
        let entries = Vec::with_capacity(capacity);

        Self {
            entries,
            capacity_mask: capacity - 1,
            write_cursor: 0,
            boundary_ts: 0,
            interval_micros,
            column_indices,
            last_access: Instant::now(),
        }
    }

    /// Returns the buffer capacity (always a power of 2).
    pub fn capacity(&self) -> usize {
        self.capacity_mask + 1
    }

    /// Returns the number of entries currently in the buffer.
    pub fn len(&self) -> usize {
        self.write_cursor.min(self.capacity())
    }

    /// Returns true if the buffer contains no entries.
    pub fn is_empty(&self) -> bool {
        self.write_cursor == 0
    }

    /// Returns the column indices this buffer extracts from mutations.
    pub fn column_indices(&self) -> &[u16] {
        &self.column_indices
    }

    /// Returns the interval in microseconds.
    pub fn interval_micros(&self) -> i64 {
        self.interval_micros
    }

    /// Returns the current boundary timestamp.
    pub fn boundary_ts(&self) -> i64 {
        self.boundary_ts
    }

    /// Touch the last_access time for LRU tracking.
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }

    /// Returns the last access time.
    pub fn last_access(&self) -> Instant {
        self.last_access
    }

    /// Insert an entry into the ring buffer.
    ///
    /// The slot is determined by `write_cursor & capacity_mask` (bitwise modulo).
    /// Returns the [`BoundaryStatus`] indicating whether a consolidation boundary
    /// was crossed or late data was detected.
    pub fn insert(&mut self, timestamp: i64, values: SmallVec<[f64; 8]>) -> BoundaryStatus {
        // Initialize boundary_ts from first write.
        if self.write_cursor == 0 {
            self.boundary_ts =
                (timestamp / self.interval_micros) * self.interval_micros + self.interval_micros;
        }

        let slot = self.write_cursor & self.capacity_mask;
        let entry = RingEntry { timestamp, values };

        if slot < self.entries.len() {
            self.entries[slot] = entry;
        } else {
            // First pass: push to fill the Vec up to capacity.
            self.entries.push(entry);
        }
        self.write_cursor += 1;
        self.last_access = Instant::now();

        // Check boundary.
        if timestamp < self.boundary_ts - self.interval_micros {
            BoundaryStatus::LateData
        } else if timestamp >= self.boundary_ts {
            let status = BoundaryStatus::BoundaryCrossed;
            // Advance boundary to cover the new timestamp.
            while self.boundary_ts <= timestamp {
                self.boundary_ts += self.interval_micros;
            }
            status
        } else {
            BoundaryStatus::Normal
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- RingEntry tests --

    #[test]
    fn ring_entry_construction() {
        let entry = RingEntry {
            timestamp: 1_700_000_000_000_000,
            values: SmallVec::from_slice(&[42.0, 37.5]),
        };
        assert_eq!(entry.timestamp, 1_700_000_000_000_000);
        assert_eq!(entry.values.len(), 2);
        assert!((entry.values[0] - 42.0).abs() < f64::EPSILON);
        assert!((entry.values[1] - 37.5).abs() < f64::EPSILON);
    }

    #[test]
    fn ring_entry_inline_up_to_8_columns() {
        let values: SmallVec<[f64; 8]> = (0..8).map(|i| i as f64).collect();
        let entry = RingEntry {
            timestamp: 1000,
            values,
        };
        assert_eq!(entry.values.len(), 8);
        assert!(!entry.values.spilled());
    }

    #[test]
    fn ring_entry_spills_beyond_8_columns() {
        let values: SmallVec<[f64; 8]> = (0..9).map(|i| i as f64).collect();
        let entry = RingEntry {
            timestamp: 1000,
            values,
        };
        assert_eq!(entry.values.len(), 9);
        assert!(entry.values.spilled());
    }

    // -- RingBuffer tests --

    #[test]
    fn ring_buffer_new_power_of_two() {
        let rb = RingBuffer::new(512, 300_000_000, vec![0, 1]);
        assert_eq!(rb.capacity(), 512);
        assert_eq!(rb.len(), 0);
        assert!(rb.is_empty());
    }

    #[test]
    fn ring_buffer_new_rounds_up_to_power_of_two() {
        let rb = RingBuffer::new(500, 300_000_000, vec![0]);
        assert_eq!(rb.capacity(), 512);
    }

    #[test]
    fn ring_buffer_insert_and_len() {
        let mut rb = RingBuffer::new(4, 300_000_000, vec![0]);
        assert_eq!(rb.len(), 0);

        rb.insert(1_000_000, SmallVec::from_slice(&[42.0]));
        assert_eq!(rb.len(), 1);

        rb.insert(2_000_000, SmallVec::from_slice(&[43.0]));
        assert_eq!(rb.len(), 2);
    }

    #[test]
    fn ring_buffer_insert_wraps_around() {
        let mut rb = RingBuffer::new(4, 300_000_000, vec![0]);

        for i in 0..6 {
            rb.insert(i as i64 * 1_000_000, SmallVec::from_slice(&[i as f64]));
        }
        assert_eq!(rb.len(), 4);
    }

    #[test]
    fn ring_buffer_boundary_ts_initialized_from_first_write() {
        let interval = 300_000_000_i64;
        let mut rb = RingBuffer::new(512, interval, vec![0]);

        rb.insert(100_000_000, SmallVec::from_slice(&[1.0]));
        assert_eq!(rb.boundary_ts(), 300_000_000);

        let mut rb2 = RingBuffer::new(512, interval, vec![0]);
        rb2.insert(400_000_000, SmallVec::from_slice(&[1.0]));
        assert_eq!(rb2.boundary_ts(), 600_000_000);
    }

    #[test]
    fn ring_buffer_detects_boundary_crossing() {
        let interval = 10_000_000_i64;
        let mut rb = RingBuffer::new(64, interval, vec![0]);

        let status = rb.insert(1_000_000, SmallVec::from_slice(&[1.0]));
        assert_eq!(status, BoundaryStatus::Normal);
        assert_eq!(rb.boundary_ts(), 10_000_000);

        let status = rb.insert(5_000_000, SmallVec::from_slice(&[2.0]));
        assert_eq!(status, BoundaryStatus::Normal);

        let status = rb.insert(10_000_000, SmallVec::from_slice(&[3.0]));
        assert_eq!(status, BoundaryStatus::BoundaryCrossed);
        assert_eq!(rb.boundary_ts(), 20_000_000);
    }

    #[test]
    fn ring_buffer_detects_late_data() {
        let interval = 10_000_000_i64;
        let mut rb = RingBuffer::new(64, interval, vec![0]);

        rb.insert(15_000_000, SmallVec::from_slice(&[1.0]));
        assert_eq!(rb.boundary_ts(), 20_000_000);

        let status = rb.insert(25_000_000, SmallVec::from_slice(&[2.0]));
        assert_eq!(status, BoundaryStatus::BoundaryCrossed);
        assert_eq!(rb.boundary_ts(), 30_000_000);

        let status = rb.insert(5_000_000, SmallVec::from_slice(&[3.0]));
        assert_eq!(status, BoundaryStatus::LateData);
    }
}
