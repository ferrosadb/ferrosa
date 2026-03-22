//! Write gate preventing non-transactional writes from bypassing Accord.
//!
//! When an Accord transaction is in-flight on a key, non-transactional
//! writes (regular INSERT/UPDATE via the CQL router without BEGIN
//! TRANSACTION) must be routed through Accord to maintain dependency
//! tracking. Without this gate, non-transactional writes become invisible
//! to concurrent Accord transactions, creating phantom dependency gaps.
//!
//! This is the primary mitigation for **FM16** (RPN 250) in the FMEA.

use super::conflict_index::ConflictIndex;

/// Result of checking the write gate.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteGateDecision {
    /// No in-flight Accord transactions on this key. Proceed normally.
    Allow,
    /// In-flight Accord transactions exist. The write must be routed
    /// through Accord to preserve dependency tracking.
    RouteThroughAccord,
}

/// Checks whether a non-transactional write should be allowed or must be
/// routed through Accord to maintain dependency tracking.
///
/// This prevents FM16: non-transactional writes bypassing Accord's
/// [`ConflictIndex`], making them invisible to concurrent Accord
/// transactions.
///
/// # Arguments
///
/// * `conflict_index` - The shard-local conflict index to query.
/// * `key` - The partition key bytes for the write.
///
/// # Returns
///
/// [`WriteGateDecision::Allow`] if no in-flight Accord transactions
/// touch this key, or [`WriteGateDecision::RouteThroughAccord`] if at
/// least one in-flight transaction exists on this key.
pub fn check_write_gate(conflict_index: &ConflictIndex, key: &[u8]) -> WriteGateDecision {
    if conflict_index.max_conflicting_timestamp(key).is_some() {
        WriteGateDecision::RouteThroughAccord
    } else {
        WriteGateDecision::Allow
    }
}

/// Same check as [`check_write_gate`] but for range operations
/// (e.g., token-range queries).
///
/// # Arguments
///
/// * `conflict_index` - The shard-local conflict index to query.
/// * `range` - The token range to check for overlapping in-flight
///   transactions.
pub fn check_write_gate_range(
    conflict_index: &ConflictIndex,
    range: &super::conflict_index::TokenRange,
) -> WriteGateDecision {
    if conflict_index
        .max_conflicting_range_timestamp(range)
        .is_some()
    {
        WriteGateDecision::RouteThroughAccord
    } else {
        WriteGateDecision::Allow
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::conflict_index::{InFlightWrite, TxnStatus};
    use ferrosa_common::accord::{Timestamp, TxnId};

    /// Helper: create a Timestamp with the given time value (other fields zero).
    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 1,
        }
    }

    /// Helper: create a TxnId from a Timestamp time value.
    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    /// Helper: create an InFlightWrite with given t0 time.
    fn write_entry(t0_time: u64) -> InFlightWrite {
        InFlightWrite {
            txn_id: txn(t0_time),
            t0: ts(t0_time),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: In-flight Accord txn blocks non-transactional write
    // -----------------------------------------------------------------------

    #[test]
    fn non_transactional_write_accord_gate() {
        let mut idx = ConflictIndex::new(100);
        let key = b"partition-K";

        // Register an in-flight Accord transaction on key K.
        idx.register(key, write_entry(10)).unwrap();

        // The write gate must route through Accord.
        let decision = check_write_gate(&idx, key);
        assert_eq!(
            decision,
            WriteGateDecision::RouteThroughAccord,
            "non-transactional write on key with in-flight Accord txn must be routed"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: Empty ConflictIndex allows writes
    // -----------------------------------------------------------------------

    #[test]
    fn write_gate_no_conflict_passes_through() {
        let idx = ConflictIndex::new(100);
        let key = b"partition-K";

        // No in-flight transactions — write should be allowed.
        let decision = check_write_gate(&idx, key);
        assert_eq!(
            decision,
            WriteGateDecision::Allow,
            "write on key with no in-flight txns must be allowed"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: Different keys are independent
    // -----------------------------------------------------------------------

    #[test]
    fn write_gate_concurrent_check() {
        let mut idx = ConflictIndex::new(100);
        let key_k = b"partition-K";
        let key_l = b"partition-L";

        // Register an in-flight Accord transaction on key K only.
        idx.register(key_k, write_entry(10)).unwrap();

        // Key K is gated.
        assert_eq!(
            check_write_gate(&idx, key_k),
            WriteGateDecision::RouteThroughAccord,
            "key K has in-flight txn, must be routed"
        );

        // Key L has no in-flight txn — independent, should be allowed.
        assert_eq!(
            check_write_gate(&idx, key_l),
            WriteGateDecision::Allow,
            "key L has no in-flight txn, must be allowed"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: Gate clears after Accord txn is applied and removed
    // -----------------------------------------------------------------------

    #[test]
    fn write_gate_after_accord_applied() {
        let mut idx = ConflictIndex::new(100);
        let key = b"partition-K";

        // Register an in-flight Accord transaction on key K.
        idx.register(key, write_entry(10)).unwrap();
        assert_eq!(
            check_write_gate(&idx, key),
            WriteGateDecision::RouteThroughAccord,
        );

        // Simulate the transaction being Applied — remove from index.
        idx.remove(&txn(10));

        // After removal the gate must allow the write.
        let decision = check_write_gate(&idx, key);
        assert_eq!(
            decision,
            WriteGateDecision::Allow,
            "after Accord txn is applied and removed, write must be allowed"
        );
    }
}
