//! Cell values: the atomic unit of data in Cassandra/Ferrosa.
//!
//! A [`CellValue`] belongs to a specific column in a specific row and carries
//! a value along with metadata (timestamp, TTL, local deletion time) used for
//! last-write-wins conflict resolution and TTL expiration.
//!
//! Three cell states:
//! - **Live**: value + timestamp, no expiration
//! - **Expiring**: value + timestamp + TTL + local deletion time
//! - **Tombstone**: no value, marks deletion at a specific timestamp
//!
//! Sentinel constants: [`NO_TIMESTAMP`], [`NO_TTL`], [`NO_DELETION_TIME`].

/// Timestamp for a cell value (microseconds since epoch).
/// Matches Cassandra's `Cell.timestamp()`.
pub type Timestamp = i64;

/// Sentinel value indicating no timestamp has been set.
pub const NO_TIMESTAMP: Timestamp = i64::MIN;

/// Sentinel: cell has no TTL (lives forever unless explicitly deleted).
pub const NO_TTL: i32 = 0;

/// Sentinel: cell is not deleted. Cassandra uses `Integer.MAX_VALUE`.
pub const NO_DELETION_TIME: i32 = i32::MAX;

/// A single cell value within a row.
///
/// Cells are the atomic unit of data in Cassandra/Ferrosa. A cell belongs
/// to a specific column in a specific row, and carries a value along with
/// metadata (timestamp, TTL, deletion time) used for conflict resolution
/// and expiration.
///
/// Three cell states:
/// - **Live**: has a value and timestamp, no expiration
/// - **Expiring**: has a value, timestamp, TTL, and deletion time
/// - **Tombstone**: no value, marks deletion at a specific timestamp
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CellValue {
    /// The cell's bytes. `None` for tombstones.
    pub value: Option<Vec<u8>>,
    /// Microseconds since epoch. Used for last-write-wins conflict resolution.
    pub timestamp: Timestamp,
    /// Time-to-live in seconds. 0 means no expiration.
    pub ttl: i32,
    /// Local deletion time (seconds since epoch). `i32::MAX` means not deleted.
    pub local_deletion_time: i32,
}

impl CellValue {
    /// A live cell with a value and timestamp, no expiration.
    ///
    /// ```
    /// use ferrosa_common::CellValue;
    ///
    /// let cell = CellValue::live(b"hello".to_vec(), 1000);
    /// assert!(cell.is_live());
    /// assert!(!cell.is_tombstone());
    /// ```
    pub fn live(value: Vec<u8>, timestamp: Timestamp) -> Self {
        Self {
            value: Some(value),
            timestamp,
            ttl: NO_TTL,
            local_deletion_time: NO_DELETION_TIME,
        }
    }

    /// A cell with a TTL that will expire.
    pub fn expiring(
        value: Vec<u8>,
        timestamp: Timestamp,
        ttl: i32,
        local_deletion_time: i32,
    ) -> Self {
        Self {
            value: Some(value),
            timestamp,
            ttl,
            local_deletion_time,
        }
    }

    /// A tombstone marking deletion at the given timestamp.
    ///
    /// ```
    /// use ferrosa_common::CellValue;
    ///
    /// let cell = CellValue::tombstone(1000, 1700000000);
    /// assert!(cell.is_tombstone());
    /// assert!(cell.value.is_none());
    /// ```
    pub fn tombstone(timestamp: Timestamp, local_deletion_time: i32) -> Self {
        Self {
            value: None,
            timestamp,
            ttl: NO_TTL,
            local_deletion_time,
        }
    }

    pub fn is_live(&self) -> bool {
        self.value.is_some() && self.local_deletion_time == NO_DELETION_TIME
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    pub fn is_expiring(&self) -> bool {
        self.value.is_some() && self.ttl != NO_TTL
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_cell() {
        let cell = CellValue::live(b"hello".to_vec(), 1000);
        assert!(cell.is_live());
        assert!(!cell.is_tombstone());
        assert!(!cell.is_expiring());
        assert_eq!(cell.value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(cell.timestamp, 1000);
    }

    #[test]
    fn expiring_cell() {
        let cell = CellValue::expiring(b"temp".to_vec(), 1000, 3600, 1700000000);
        assert!(!cell.is_live()); // has deletion time set
        assert!(!cell.is_tombstone());
        assert!(cell.is_expiring());
        assert_eq!(cell.ttl, 3600);
    }

    #[test]
    fn tombstone() {
        let cell = CellValue::tombstone(1000, 1700000000);
        assert!(!cell.is_live());
        assert!(cell.is_tombstone());
        assert!(!cell.is_expiring());
        assert!(cell.value.is_none());
    }

    #[test]
    fn no_timestamp_sentinel() {
        assert_eq!(NO_TIMESTAMP, i64::MIN);
    }
}
