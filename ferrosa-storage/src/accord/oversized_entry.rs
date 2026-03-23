//! Oversized entry handling for Accord protocol log.
//!
//! Tests verifying that oversized entries are rejected without corrupting
//! the log or blocking other transactions.
//!
//! # A7.10 — Oversized Entry (3 tests)
//!
//! - `accord_oversized_entry_error` — entry exceeding max size is rejected
//! - `accord_oversized_entry_other_txns_unaffected` — other txns proceed
//! - `accord_entry_size_within_segment` — entries within size limit succeed

/// Maximum entry size for Accord protocol log entries (1 MB).
///
/// Entries larger than this are rejected to prevent segment corruption
/// and memory exhaustion. This limit applies to the serialized entry
/// including all fields and CRC.
pub const MAX_ENTRY_SIZE: usize = 1024 * 1024; // 1 MB

/// Error returned when an entry exceeds the maximum size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OversizedEntryError {
    /// Actual size of the entry in bytes.
    pub actual_size: usize,
    /// Maximum allowed size.
    pub max_size: usize,
}

impl std::fmt::Display for OversizedEntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "entry size {} exceeds maximum {} bytes",
            self.actual_size, self.max_size
        )
    }
}

impl std::error::Error for OversizedEntryError {}

/// Check if a serialized entry is within the size limit.
///
/// Returns `Ok(())` if the entry size is within bounds, or
/// `Err(OversizedEntryError)` if it exceeds `MAX_ENTRY_SIZE`.
pub fn check_entry_size(serialized: &[u8]) -> Result<(), OversizedEntryError> {
    if serialized.len() > MAX_ENTRY_SIZE {
        Err(OversizedEntryError {
            actual_size: serialized.len(),
            max_size: MAX_ENTRY_SIZE,
        })
    } else {
        Ok(())
    }
}

/// Check if a serialized entry is within a custom size limit.
pub fn check_entry_size_custom(
    serialized: &[u8],
    max_size: usize,
) -> Result<(), OversizedEntryError> {
    if serialized.len() > max_size {
        Err(OversizedEntryError {
            actual_size: serialized.len(),
            max_size,
        })
    } else {
        Ok(())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::entries::{AcceptedBallot, AccordProtocolEntry, Timestamp, TxnId};
    use crate::accord::protocol_log::ProtocolLog;

    fn ts(micros: u64, logical: u32) -> Timestamp {
        Timestamp {
            epoch_micros: micros,
            logical,
        }
    }

    fn txn(node: u64, micros: u64) -> TxnId {
        TxnId {
            node,
            timestamp: ts(micros, 0),
        }
    }

    // =======================================================================
    // A7.10-T1: accord_oversized_entry_error
    // =======================================================================

    /// An entry whose serialized form exceeds MAX_ENTRY_SIZE is rejected.
    #[test]
    fn accord_oversized_entry_error() {
        // Create an entry with a huge dependency list that pushes it over the limit.
        let huge_deps: Vec<TxnId> = (0..100_000).map(|i| txn(i, i * 10)).collect();

        let entry = AccordProtocolEntry::PreAccepted {
            txn_id: txn(1, 1000),
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: huge_deps,
        };

        let serialized = entry.serialize();
        assert!(
            serialized.len() > MAX_ENTRY_SIZE,
            "entry with 100K deps should exceed 1MB: actual={} bytes",
            serialized.len()
        );

        let result = check_entry_size(&serialized);
        assert!(result.is_err(), "oversized entry must be rejected");

        let err = result.unwrap_err();
        assert_eq!(err.max_size, MAX_ENTRY_SIZE);
        assert!(
            err.actual_size > MAX_ENTRY_SIZE,
            "error must report actual size > max"
        );
        assert!(
            err.to_string().contains("exceeds maximum"),
            "error message must describe the issue"
        );
    }

    // =======================================================================
    // A7.10-T2: accord_oversized_entry_other_txns_unaffected
    // =======================================================================

    /// Rejecting an oversized entry does not affect other transactions.
    /// The protocol log continues to work correctly for normal entries.
    #[test]
    fn accord_oversized_entry_other_txns_unaffected() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = ProtocolLog::new(dir.path());

        // Append a normal entry first.
        let normal_entry = AccordProtocolEntry::PreAccepted {
            txn_id: txn(1, 1000),
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
        };
        log.append(normal_entry.clone());
        assert_eq!(log.len(), 1);

        // Create an oversized entry and check it (but don't append if too big).
        let huge_deps: Vec<TxnId> = (0..100_000).map(|i| txn(i, i * 10)).collect();
        let oversized_entry = AccordProtocolEntry::PreAccepted {
            txn_id: txn(2, 2000),
            t0: ts(2000, 0),
            t: ts(2001, 0),
            deps: huge_deps,
        };

        let serialized = oversized_entry.serialize();
        let size_check = check_entry_size(&serialized);
        assert!(
            size_check.is_err(),
            "oversized entry must be rejected by size check"
        );

        // The log is unaffected — still has exactly 1 entry.
        assert_eq!(log.len(), 1, "log must not be corrupted by oversized check");

        // Append another normal entry after the rejection.
        let normal_entry2 = AccordProtocolEntry::Committed {
            txn_id: txn(3, 3000),
            t: ts(3001, 0),
            deps: vec![txn(1, 1000)],
        };
        log.append(normal_entry2);
        assert_eq!(
            log.len(),
            2,
            "normal entries must still be appendable after oversized rejection"
        );

        // Replay must return both normal entries in order.
        let replayed = log.replay();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].txn_id(), &txn(1, 1000));
        assert_eq!(replayed[1].txn_id(), &txn(3, 3000));
    }

    // =======================================================================
    // A7.10-T3: accord_entry_size_within_segment
    // =======================================================================

    /// Entries within the size limit pass validation and can be appended.
    #[test]
    fn accord_entry_size_within_segment() {
        // Normal entry with a few deps.
        let entry = AccordProtocolEntry::Accepted {
            txn_id: txn(1, 5000),
            t0: ts(5000, 0),
            t: ts(5001, 1),
            deps: vec![txn(2, 4000), txn(3, 4500)],
            accepted_ballot: AcceptedBallot { ballot: 1, node: 1 },
        };

        let serialized = entry.serialize();
        assert!(
            serialized.len() < MAX_ENTRY_SIZE,
            "normal entry should be well under 1MB: actual={} bytes",
            serialized.len()
        );

        let result = check_entry_size(&serialized);
        assert!(
            result.is_ok(),
            "entry within size limit must pass validation"
        );

        // Edge case: entry just barely under the limit (using custom limit).
        let custom_limit = serialized.len() + 1;
        let result2 = check_entry_size_custom(&serialized, custom_limit);
        assert!(result2.is_ok(), "entry just under limit must pass");

        // Edge case: entry exactly at the limit.
        let exact_limit = serialized.len();
        let result3 = check_entry_size_custom(&serialized, exact_limit);
        assert!(result3.is_ok(), "entry exactly at limit must pass");

        // Edge case: entry one byte over the limit.
        let too_small_limit = serialized.len() - 1;
        let result4 = check_entry_size_custom(&serialized, too_small_limit);
        assert!(
            result4.is_err(),
            "entry one byte over limit must be rejected"
        );

        // Verify the entry can be appended and replayed.
        let dir = tempfile::tempdir().unwrap();
        let mut log = ProtocolLog::new(dir.path());
        log.append(entry.clone());

        let replayed = log.replay();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0], entry);
    }
}
