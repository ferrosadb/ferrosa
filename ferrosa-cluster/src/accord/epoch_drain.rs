//! Epoch transition drain: safely drain in-flight transactions during
//! an epoch change.
//!
//! When the electorate epoch advances (member join/leave), all transactions
//! started in the old epoch must either complete or be aborted before the
//! new epoch fully takes effect. The drain period must exceed
//! `SkewMax + timeout` to account for clock skew and in-flight message
//! delivery delays.
//!
//! # Protocol
//!
//! 1. **Begin drain**: Mark the old epoch as draining. New transactions
//!    must use the new epoch.
//! 2. **Drain period**: Wait for `drain_duration` (default 30s). During
//!    this window, transactions from the old epoch are allowed to finish.
//! 3. **Timeout abort**: After the drain period, any remaining old-epoch
//!    transactions are aborted.
//! 4. **Cross-epoch**: Transactions that span the epoch boundary are
//!    allowed to finish if they were committed before the drain started.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ferrosa_common::accord::TxnId;

use super::epoch::Epoch;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default drain period: 30 seconds.
///
/// This exceeds `SkewMax` (typically 500ms-1s) plus the transaction
/// timeout (typically 10s) to ensure all in-flight transactions have
/// time to complete or be detected as timed out.
pub const DEFAULT_DRAIN_DURATION: Duration = Duration::from_secs(30);

/// Default SkewMax: 1 second.
pub const DEFAULT_SKEW_MAX: Duration = Duration::from_secs(1);

/// Default transaction timeout: 10 seconds.
pub const DEFAULT_TXN_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// DrainStatus
// ---------------------------------------------------------------------------

/// Status of a transaction relative to the epoch drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnDrainStatus {
    /// Transaction is from the current epoch — not affected by drain.
    CurrentEpoch,
    /// Transaction is from the old epoch and is still in-flight.
    Draining,
    /// Transaction was committed before drain started — allowed to finish.
    CrossEpoch,
    /// Transaction was aborted due to drain timeout.
    Aborted,
}

/// Result of drain completion check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainCheckResult {
    /// All old-epoch transactions have completed.
    Complete,
    /// Still draining — transactions remain.
    Pending { remaining: usize },
    /// No drain is active.
    NoDrain,
}

// ---------------------------------------------------------------------------
// EpochDrain
// ---------------------------------------------------------------------------

/// Manages epoch transition draining.
///
/// When an epoch transition occurs, this struct tracks which transactions
/// are from the old epoch and manages their orderly completion or timeout.
pub struct EpochDrain {
    /// The old epoch being drained (if any).
    draining_epoch: Option<Epoch>,
    /// The new (current) epoch.
    current_epoch: Epoch,
    /// Drain duration.
    drain_duration: Duration,
    /// Transactions from the old epoch that are still in-flight.
    in_flight: HashSet<TxnId>,
    /// Transactions that were committed before drain and are allowed
    /// to finish (cross-epoch).
    cross_epoch: HashSet<TxnId>,
    /// Transactions that were aborted due to drain timeout.
    aborted: HashSet<TxnId>,
    /// Per-transaction epoch tracking.
    txn_epochs: HashMap<TxnId, Epoch>,
    /// Whether the drain period has expired (simulated for testing).
    drain_expired: bool,
}

impl EpochDrain {
    /// Create a new drain manager at the given epoch.
    pub fn new(initial_epoch: Epoch) -> Self {
        Self {
            draining_epoch: None,
            current_epoch: initial_epoch,
            drain_duration: DEFAULT_DRAIN_DURATION,
            in_flight: HashSet::new(),
            cross_epoch: HashSet::new(),
            aborted: HashSet::new(),
            txn_epochs: HashMap::new(),
            drain_expired: false,
        }
    }

    /// Create with a custom drain duration.
    pub fn with_duration(initial_epoch: Epoch, duration: Duration) -> Self {
        let mut drain = Self::new(initial_epoch);
        drain.drain_duration = duration;
        drain
    }

    /// Current epoch.
    pub fn current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    /// Drain duration.
    pub fn drain_duration(&self) -> Duration {
        self.drain_duration
    }

    /// Whether a drain is currently active.
    pub fn is_draining(&self) -> bool {
        self.draining_epoch.is_some()
    }

    /// Begin an epoch transition.
    ///
    /// Marks the current epoch as draining and advances to the new epoch.
    /// All currently registered transactions from the old epoch are added
    /// to the in-flight set for drain tracking.
    ///
    /// # Panics
    /// Panics if a drain is already active.
    pub fn begin_transition(&mut self, new_epoch: Epoch) {
        assert!(
            self.draining_epoch.is_none(),
            "cannot begin transition while drain is active"
        );
        assert!(
            new_epoch > self.current_epoch,
            "new epoch must be greater than current"
        );

        let old_epoch = self.current_epoch;
        self.draining_epoch = Some(old_epoch);
        self.current_epoch = new_epoch;
        self.drain_expired = false;

        // Retroactively add all previously-registered transactions from the
        // old epoch to the in-flight set.
        for (&txn_id, &epoch) in &self.txn_epochs {
            if epoch == old_epoch {
                self.in_flight.insert(txn_id);
            }
        }
    }

    /// Register a new transaction and its epoch.
    pub fn register_txn(&mut self, txn_id: TxnId, epoch: Epoch) {
        self.txn_epochs.insert(txn_id, epoch);
        if let Some(draining_epoch) = self.draining_epoch {
            if epoch == draining_epoch {
                self.in_flight.insert(txn_id);
            }
        }
    }

    /// Mark a transaction as committed during the drain window.
    ///
    /// Committed transactions from the old epoch are marked as cross-epoch
    /// and allowed to finish.
    pub fn mark_committed(&mut self, txn_id: TxnId) {
        if self.in_flight.remove(&txn_id) {
            self.cross_epoch.insert(txn_id);
        }
    }

    /// Mark a transaction as completed (applied).
    pub fn mark_completed(&mut self, txn_id: &TxnId) {
        self.in_flight.remove(txn_id);
        self.cross_epoch.remove(txn_id);
        self.txn_epochs.remove(txn_id);
    }

    /// Get the drain status of a transaction.
    pub fn txn_status(&self, txn_id: &TxnId) -> TxnDrainStatus {
        if self.aborted.contains(txn_id) {
            return TxnDrainStatus::Aborted;
        }
        if self.cross_epoch.contains(txn_id) {
            return TxnDrainStatus::CrossEpoch;
        }
        if self.in_flight.contains(txn_id) {
            return TxnDrainStatus::Draining;
        }
        TxnDrainStatus::CurrentEpoch
    }

    /// Simulate the drain period expiring (for testing).
    ///
    /// In production this would be driven by a timer.
    pub fn expire_drain(&mut self) {
        self.drain_expired = true;
    }

    /// Abort all remaining in-flight transactions from the old epoch.
    ///
    /// Called after the drain period expires. Returns the list of aborted
    /// transaction IDs.
    ///
    /// Cross-epoch transactions (already committed) are NOT aborted.
    pub fn timeout_abort(&mut self) -> Vec<TxnId> {
        assert!(
            self.drain_expired,
            "cannot abort before drain period expires"
        );

        let aborted: Vec<TxnId> = self.in_flight.drain().collect();
        for txn_id in &aborted {
            self.aborted.insert(*txn_id);
            self.txn_epochs.remove(txn_id);
        }
        aborted
    }

    /// Check if the drain is complete.
    pub fn check_complete(&self) -> DrainCheckResult {
        match self.draining_epoch {
            None => DrainCheckResult::NoDrain,
            Some(_) => {
                let remaining = self.in_flight.len() + self.cross_epoch.len();
                if remaining == 0 {
                    DrainCheckResult::Complete
                } else {
                    DrainCheckResult::Pending { remaining }
                }
            }
        }
    }

    /// Finalize the drain: clear all drain state.
    ///
    /// Should only be called after `check_complete` returns `Complete`.
    pub fn finalize(&mut self) {
        self.draining_epoch = None;
        self.drain_expired = false;
        // aborted set is preserved for query purposes.
    }

    /// Number of in-flight old-epoch transactions.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Number of cross-epoch transactions still finishing.
    pub fn cross_epoch_count(&self) -> usize {
        self.cross_epoch.len()
    }

    /// Number of aborted transactions.
    pub fn aborted_count(&self) -> usize {
        self.aborted.len()
    }
}

// ===========================================================================
// Tests — 3 tests for A7.5
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 0,
        }
    }

    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    // -----------------------------------------------------------------------
    // Test 1: epoch_drain_period
    //   30s drain exceeds SkewMax + timeout.
    // -----------------------------------------------------------------------

    #[test]
    fn epoch_drain_period() {
        let drain = EpochDrain::new(1);

        // Verify default drain duration exceeds SkewMax + TxnTimeout.
        let min_drain = DEFAULT_SKEW_MAX + DEFAULT_TXN_TIMEOUT;
        assert!(
            drain.drain_duration() >= min_drain,
            "drain duration {:?} must exceed SkewMax + TxnTimeout = {:?}",
            drain.drain_duration(),
            min_drain
        );

        // Verify default is 30s.
        assert_eq!(
            drain.drain_duration(),
            Duration::from_secs(30),
            "default drain must be 30 seconds"
        );

        // Custom duration works.
        let custom = EpochDrain::with_duration(1, Duration::from_secs(60));
        assert_eq!(custom.drain_duration(), Duration::from_secs(60));

        // Verify drain lifecycle.
        let mut drain = EpochDrain::new(1);
        assert!(!drain.is_draining(), "no drain initially");

        // Register some txns in epoch 1.
        let t1 = txn(100);
        let t2 = txn(200);
        drain.register_txn(t1, 1);
        drain.register_txn(t2, 1);

        // Begin transition to epoch 2.
        drain.begin_transition(2);
        assert!(drain.is_draining());
        assert_eq!(drain.current_epoch(), 2);
        assert_eq!(drain.in_flight_count(), 2, "both txns from old epoch");

        // Complete both transactions.
        drain.mark_completed(&t1);
        drain.mark_completed(&t2);

        assert_eq!(drain.check_complete(), DrainCheckResult::Complete);
        drain.finalize();
        assert!(!drain.is_draining());
    }

    // -----------------------------------------------------------------------
    // Test 2: epoch_drain_timeout_abort
    //   Timeout aborts pending transactions.
    // -----------------------------------------------------------------------

    #[test]
    fn epoch_drain_timeout_abort() {
        let mut drain = EpochDrain::new(1);

        // Register transactions in epoch 1.
        let t1 = txn(100);
        let t2 = txn(200);
        let t3 = txn(300);
        drain.register_txn(t1, 1);
        drain.register_txn(t2, 1);
        drain.register_txn(t3, 1);

        // Begin transition to epoch 2.
        drain.begin_transition(2);
        assert_eq!(drain.in_flight_count(), 3);

        // Complete t1 normally.
        drain.mark_completed(&t1);
        assert_eq!(drain.in_flight_count(), 2);

        // Drain period expires.
        drain.expire_drain();

        // Timeout-abort remaining txns.
        let mut aborted = drain.timeout_abort();
        aborted.sort();
        assert_eq!(aborted.len(), 2, "t2 and t3 must be aborted");

        // Verify statuses.
        assert_eq!(drain.txn_status(&t1), TxnDrainStatus::CurrentEpoch);
        assert_eq!(drain.txn_status(&t2), TxnDrainStatus::Aborted);
        assert_eq!(drain.txn_status(&t3), TxnDrainStatus::Aborted);

        assert_eq!(drain.aborted_count(), 2);
        assert_eq!(drain.in_flight_count(), 0);

        // New transactions in epoch 2 are not affected.
        let t4 = txn(400);
        drain.register_txn(t4, 2);
        assert_eq!(
            drain.txn_status(&t4),
            TxnDrainStatus::CurrentEpoch,
            "epoch-2 txn must be CurrentEpoch"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: epoch_drain_cross_epoch_txn
    //   Cross-epoch txns (committed before drain) are allowed to finish.
    // -----------------------------------------------------------------------

    #[test]
    fn epoch_drain_cross_epoch_txn() {
        let mut drain = EpochDrain::new(1);

        let t1 = txn(100); // Will commit before drain expires.
        let t2 = txn(200); // Will NOT commit before drain expires.
        drain.register_txn(t1, 1);
        drain.register_txn(t2, 1);

        // Begin epoch transition.
        drain.begin_transition(2);
        assert_eq!(drain.in_flight_count(), 2);

        // t1 commits during the drain window — becomes cross-epoch.
        drain.mark_committed(t1);
        assert_eq!(drain.txn_status(&t1), TxnDrainStatus::CrossEpoch);
        assert_eq!(drain.in_flight_count(), 1, "t1 moved to cross-epoch");
        assert_eq!(drain.cross_epoch_count(), 1);

        // t2 remains in-flight.
        assert_eq!(drain.txn_status(&t2), TxnDrainStatus::Draining);

        // Drain period expires.
        drain.expire_drain();

        // Timeout-abort: only t2 is aborted. t1 (cross-epoch) is safe.
        let aborted = drain.timeout_abort();
        assert_eq!(aborted.len(), 1, "only t2 must be aborted");
        assert_eq!(aborted[0], t2);

        // t1 is still cross-epoch (allowed to finish).
        assert_eq!(drain.txn_status(&t1), TxnDrainStatus::CrossEpoch);
        assert_eq!(drain.txn_status(&t2), TxnDrainStatus::Aborted);

        // Drain not yet complete — t1 still finishing.
        assert_eq!(
            drain.check_complete(),
            DrainCheckResult::Pending { remaining: 1 }
        );

        // t1 finishes.
        drain.mark_completed(&t1);
        assert_eq!(drain.check_complete(), DrainCheckResult::Complete);

        // Finalize.
        drain.finalize();
        assert!(!drain.is_draining());
    }
}
