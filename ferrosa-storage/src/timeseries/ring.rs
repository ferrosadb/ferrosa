//! Cache-aligned ring buffer for time-series data points.
//!
//! Each `(table, partition_key)` gets its own [`RingBuffer`]. The buffer base is
//! cache-line-aligned; individual entries are packed for density.
//! `RingBuffer` is NOT `Sync` -- relies on DashMap per-shard lock for mutual exclusion.

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

#[cfg(test)]
mod tests {
    use super::*;

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
        // SmallVec with 8 elements should still be inline (no heap allocation).
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
}
