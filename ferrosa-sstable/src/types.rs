//! SSTable-specific deserialized types.
//!
//! These types represent the in-memory view of data read from SSTables.
//! They live in `ferrosa-sstable` (not `ferrosa-common`) because they are
//! format-specific: fields like `DeletionTime` and `LivenessInfo` map directly
//! to on-disk SSTable encoding, not to the abstract CQL data model.
//!
//! # Key Types
//!
//! - [`DeletionTime`] — partition or row-level deletion marker
//! - [`LivenessInfo`] — primary key liveness (timestamp + TTL)
//! - [`Row`] — a deserialized row from an SSTable
//! - [`Partition`] — a deserialized partition from an SSTable

use ferrosa_common::{CellValue, DecoratedKey};

/// Partition-level or row-level deletion marker.
///
/// # Encoding
///
/// On disk, a live DeletionTime is a single `0x80` byte. A deleted
/// DeletionTime is 12 bytes: 8-byte i64 timestamp + 4-byte u32 local
/// deletion time.
///
/// ```
/// use ferrosa_sstable::types::DeletionTime;
///
/// let live = DeletionTime::LIVE;
/// assert!(live.is_live());
///
/// let deleted = DeletionTime::new(1000, 1700000000);
/// assert!(!deleted.is_live());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionTime {
    /// Microseconds since epoch. `i64::MIN` = live (not deleted).
    pub marked_for_delete_at: i64,
    /// Seconds since epoch. `u32::MAX` = live (not deleted).
    pub local_deletion_time: u32,
}

impl DeletionTime {
    /// Sentinel for live (not deleted) partitions and rows.
    pub const LIVE: DeletionTime = DeletionTime {
        marked_for_delete_at: i64::MIN,
        local_deletion_time: u32::MAX,
    };

    /// Create a deletion marker with the given timestamp and local deletion time.
    pub fn new(marked_for_delete_at: i64, local_deletion_time: u32) -> Self {
        Self {
            marked_for_delete_at,
            local_deletion_time,
        }
    }

    /// Returns true if this represents a live (not deleted) entry.
    pub fn is_live(&self) -> bool {
        self.marked_for_delete_at == i64::MIN && self.local_deletion_time == u32::MAX
    }
}

impl Default for DeletionTime {
    fn default() -> Self {
        Self::LIVE
    }
}

/// Primary key liveness info for a row.
///
/// Every CQL INSERT sets liveness on the primary key. If a row exists only
/// because of cell-level writes (UPDATE), it may have no liveness info.
///
/// ```
/// use ferrosa_sstable::types::LivenessInfo;
///
/// let no_liveness = LivenessInfo::NONE;
/// assert!(!no_liveness.has_timestamp());
///
/// let live = LivenessInfo::with_timestamp(1000);
/// assert!(live.has_timestamp());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessInfo {
    /// Microseconds since epoch. `i64::MIN` = no liveness.
    pub timestamp: i64,
    /// TTL in seconds. 0 = no TTL.
    pub ttl: i32,
    /// Local deletion time in seconds. `i32::MAX` = no expiry.
    pub local_deletion_time: i32,
}

impl LivenessInfo {
    /// No liveness information.
    pub const NONE: LivenessInfo = LivenessInfo {
        timestamp: i64::MIN,
        ttl: 0,
        local_deletion_time: i32::MAX,
    };

    /// Create liveness with a timestamp and no TTL.
    pub fn with_timestamp(timestamp: i64) -> Self {
        Self {
            timestamp,
            ttl: 0,
            local_deletion_time: i32::MAX,
        }
    }

    /// Create liveness with a timestamp, TTL, and expiry time.
    pub fn with_ttl(timestamp: i64, ttl: i32, local_deletion_time: i32) -> Self {
        Self {
            timestamp,
            ttl,
            local_deletion_time,
        }
    }

    /// Returns true if a timestamp is set.
    pub fn has_timestamp(&self) -> bool {
        self.timestamp != i64::MIN
    }

    /// Returns true if a TTL is set.
    pub fn has_ttl(&self) -> bool {
        self.ttl != 0
    }
}

impl Default for LivenessInfo {
    fn default() -> Self {
        Self::NONE
    }
}

/// A deserialized row from an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// Raw clustering key bytes.
    pub clustering: Vec<u8>,
    /// Column data: `(column_index, cell_value)` pairs.
    pub cells: Vec<(u16, CellValue)>,
    /// Row-level deletion.
    pub deletion: DeletionTime,
    /// Primary key liveness (set by INSERT, absent for UPDATE-only rows).
    pub primary_key_liveness: LivenessInfo,
}

/// A deserialized partition from an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    /// The partition's decorated key (key bytes + token).
    pub key: DecoratedKey,
    /// Partition-level deletion.
    pub deletion: DeletionTime,
    /// Static row (columns shared across all clustered rows).
    pub static_row: Option<Row>,
    /// Clustered rows in clustering order.
    pub rows: Vec<Row>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::PartitionKey;

    #[test]
    fn deletion_time_live() {
        assert!(DeletionTime::LIVE.is_live());
        assert!(DeletionTime::default().is_live());
    }

    #[test]
    fn deletion_time_deleted() {
        let dt = DeletionTime::new(1000, 1700000000);
        assert!(!dt.is_live());
        assert_eq!(dt.marked_for_delete_at, 1000);
        assert_eq!(dt.local_deletion_time, 1700000000);
    }

    #[test]
    fn liveness_none() {
        assert!(!LivenessInfo::NONE.has_timestamp());
        assert!(!LivenessInfo::NONE.has_ttl());
    }

    #[test]
    fn liveness_with_timestamp() {
        let li = LivenessInfo::with_timestamp(1000);
        assert!(li.has_timestamp());
        assert!(!li.has_ttl());
        assert_eq!(li.timestamp, 1000);
    }

    #[test]
    fn liveness_with_ttl() {
        let li = LivenessInfo::with_ttl(1000, 3600, 1700003600);
        assert!(li.has_timestamp());
        assert!(li.has_ttl());
        assert_eq!(li.ttl, 3600);
        assert_eq!(li.local_deletion_time, 1700003600);
    }

    #[test]
    fn row_construction() {
        let row = Row {
            clustering: vec![1, 2, 3],
            cells: vec![
                (0, CellValue::live(b"hello".to_vec(), 1000)),
                (1, CellValue::tombstone(2000, 1700000000)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        assert_eq!(row.cells.len(), 2);
        assert!(row.cells[0].1.is_live());
        assert!(row.cells[1].1.is_tombstone());
    }

    #[test]
    fn partition_construction() {
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"test".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![1],
                cells: vec![(0, CellValue::live(b"v".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
        };
        assert!(partition.deletion.is_live());
        assert!(partition.static_row.is_none());
        assert_eq!(partition.rows.len(), 1);
    }
}
