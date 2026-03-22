//! Linearizable local reads for Accord.
//!
//! A **linearizable read** must observe all transactions that committed
//! before the read began. Before returning data, the read checks the
//! [`ConflictIndex`] for any in-flight transactions on the target key.
//! If there are pending (non-Applied) transactions, the read must wait
//! for all of them to reach the Applied state before proceeding.
//!
//! This module provides [`LinearizableReadManager`] which encapsulates
//! the conflict-check-and-wait logic.

use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_storage::accord::conflict_index::ConflictIndex;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Result of a linearizable read check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadResult {
    /// No conflicts detected; the read can proceed immediately.
    Ready,
    /// In-flight transactions must reach Applied before the read can proceed.
    /// Contains the set of transaction IDs that must be waited on.
    MustWait { pending_txn_ids: Vec<TxnId> },
}

/// Errors from the linearizable read path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// The read timed out waiting for pending transactions.
    Timeout { remaining_txn_ids: Vec<TxnId> },
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Timeout { remaining_txn_ids } => {
                write!(
                    f,
                    "linearizable read timeout: {} transactions still pending",
                    remaining_txn_ids.len()
                )
            }
        }
    }
}

impl std::error::Error for ReadError {}

// ---------------------------------------------------------------------------
// LinearizableReadManager
// ---------------------------------------------------------------------------

/// Manages linearizable read checks against the conflict index.
///
/// Before a local read can return, it must verify that no in-flight
/// Accord transactions are writing to the same key. If any are pending,
/// the read must wait for them to reach the Applied state.
pub struct LinearizableReadManager;

impl LinearizableReadManager {
    /// Create a new linearizable read manager.
    pub fn new() -> Self {
        Self
    }

    /// Check the conflict index for in-flight transactions on the given key.
    ///
    /// Returns `ReadResult::Ready` if no in-flight transactions exist,
    /// or `ReadResult::MustWait` with the list of pending transaction IDs
    /// that must reach Applied before the read can proceed.
    pub fn check_conflicts(&self, conflict_index: &ConflictIndex, key: &[u8]) -> ReadResult {
        // Use a far-future timestamp to capture all in-flight transactions.
        let far_future = Timestamp {
            epoch: u64::MAX,
            time: u64::MAX,
            seq: u32::MAX,
            node: u64::MAX,
        };

        let deps = conflict_index.deps_before_t0(key, &far_future);
        if deps.is_empty() {
            return ReadResult::Ready;
        }

        let mut pending_txn_ids: Vec<TxnId> = deps.into_iter().collect();
        // Sort for deterministic ordering in tests.
        pending_txn_ids.sort();

        ReadResult::MustWait { pending_txn_ids }
    }

    /// Re-check after transactions have been applied.
    ///
    /// Call this after the caller has waited for the pending transactions
    /// to be applied. Returns `ReadResult::Ready` if all transactions
    /// have been resolved, or `ReadResult::MustWait` if new transactions
    /// arrived while waiting.
    pub fn recheck_after_apply(&self, conflict_index: &ConflictIndex, key: &[u8]) -> ReadResult {
        self.check_conflicts(conflict_index, key)
    }
}

impl Default for LinearizableReadManager {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests — A4.6 (4 tests)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_storage::accord::conflict_index::{InFlightWrite, TxnStatus};

    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 1,
        }
    }

    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    fn write_entry(t0_time: u64) -> InFlightWrite {
        InFlightWrite {
            txn_id: txn(t0_time),
            t0: ts(t0_time),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: linearizable_read_dep_check
    // -----------------------------------------------------------------------

    #[test]
    fn linearizable_read_dep_check() {
        let mgr = LinearizableReadManager::new();
        let mut conflict_index = ConflictIndex::new(100);
        let key = b"users:alice";

        // Register an in-flight write on the key.
        conflict_index.register(key, write_entry(100)).unwrap();

        // Check: should detect the in-flight transaction.
        let result = mgr.check_conflicts(&conflict_index, key);
        match result {
            ReadResult::MustWait { pending_txn_ids } => {
                assert_eq!(pending_txn_ids.len(), 1);
                assert_eq!(pending_txn_ids[0], txn(100));
            }
            ReadResult::Ready => panic!("expected MustWait, got Ready"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 2: linearizable_read_no_conflict
    // -----------------------------------------------------------------------

    #[test]
    fn linearizable_read_no_conflict() {
        let mgr = LinearizableReadManager::new();
        let conflict_index = ConflictIndex::new(100);
        let key = b"users:bob";

        // No in-flight transactions: read should be ready immediately.
        let result = mgr.check_conflicts(&conflict_index, key);
        assert_eq!(result, ReadResult::Ready);
    }

    // -----------------------------------------------------------------------
    // Test 3: linearizable_read_waits_for_apply
    // -----------------------------------------------------------------------

    #[test]
    fn linearizable_read_waits_for_apply() {
        let mgr = LinearizableReadManager::new();
        let mut conflict_index = ConflictIndex::new(100);
        let key = b"orders:123";

        // Register an in-flight write.
        conflict_index.register(key, write_entry(200)).unwrap();

        // Initial check: must wait.
        let result = mgr.check_conflicts(&conflict_index, key);
        assert!(matches!(result, ReadResult::MustWait { .. }));

        // Simulate the transaction reaching Applied and being GC'd.
        conflict_index.mark_applied(&txn(200));
        conflict_index.gc_applied();

        // Re-check: should now be ready.
        let result = mgr.recheck_after_apply(&conflict_index, key);
        assert_eq!(result, ReadResult::Ready);
    }

    // -----------------------------------------------------------------------
    // Test 4: linearizable_read_multiple_pending
    // -----------------------------------------------------------------------

    #[test]
    fn linearizable_read_multiple_pending() {
        let mgr = LinearizableReadManager::new();
        let mut conflict_index = ConflictIndex::new(100);
        let key = b"accounts:savings";

        // Register multiple in-flight writes on the same key.
        conflict_index.register(key, write_entry(100)).unwrap();
        conflict_index.register(key, write_entry(200)).unwrap();
        conflict_index.register(key, write_entry(300)).unwrap();

        // Check: should report all three pending.
        let result = mgr.check_conflicts(&conflict_index, key);
        match &result {
            ReadResult::MustWait { pending_txn_ids } => {
                assert_eq!(pending_txn_ids.len(), 3);
                // Verify all three are present.
                assert!(pending_txn_ids.contains(&txn(100)));
                assert!(pending_txn_ids.contains(&txn(200)));
                assert!(pending_txn_ids.contains(&txn(300)));
            }
            ReadResult::Ready => panic!("expected MustWait, got Ready"),
        }

        // Apply and GC two of them. One should remain pending.
        conflict_index.mark_applied(&txn(100));
        conflict_index.mark_applied(&txn(200));
        conflict_index.gc_applied();

        let result2 = mgr.recheck_after_apply(&conflict_index, key);
        match &result2 {
            ReadResult::MustWait { pending_txn_ids } => {
                assert_eq!(pending_txn_ids.len(), 1);
                assert_eq!(pending_txn_ids[0], txn(300));
            }
            ReadResult::Ready => panic!("expected MustWait with 1 remaining, got Ready"),
        }

        // Apply the last one.
        conflict_index.mark_applied(&txn(300));
        conflict_index.gc_applied();

        let result3 = mgr.recheck_after_apply(&conflict_index, key);
        assert_eq!(result3, ReadResult::Ready);
    }
}
