//! Transaction limit enforcement for Accord-style transactions.
//!
//! Enforces three categories of limits on transactions:
//! - **Concurrent**: max number of active transactions per connection (default 16)
//! - **Timeout**: max duration before auto-abort (default 10s)
//! - **Key count**: max partition keys touched per transaction (default 128)
//!
//! All limits are configurable via `TransactionLimits`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::CqlError;

/// Configurable limits for transactions on a connection.
#[derive(Debug, Clone)]
pub struct TransactionLimits {
    /// Maximum number of concurrent transactions per connection.
    pub max_concurrent: usize,
    /// Maximum duration before a transaction is auto-aborted.
    pub timeout: Duration,
    /// Maximum number of partition keys a single transaction may touch.
    pub max_keys: usize,
}

impl Default for TransactionLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 16,
            timeout: Duration::from_secs(10),
            max_keys: 128,
        }
    }
}

/// Tracks the state of a single in-flight transaction.
#[derive(Debug)]
pub struct TransactionState {
    /// When this transaction was started.
    start_time: Instant,
    /// Number of partition keys touched so far.
    key_count: usize,
    /// Reference to the shared limits configuration.
    limits: Arc<TransactionLimits>,
}

impl TransactionState {
    /// Create a new transaction state with the given limits.
    fn new(limits: Arc<TransactionLimits>) -> Self {
        Self {
            start_time: Instant::now(),
            key_count: 0,
            limits,
        }
    }

    /// Record that additional keys were touched. Returns an error if the key
    /// limit is exceeded or the transaction has timed out.
    pub fn record_keys(&mut self, count: usize) -> Result<(), CqlError> {
        self.check_timeout()?;
        self.key_count = self.key_count.saturating_add(count);
        if self.key_count > self.limits.max_keys {
            return Err(CqlError::Invalid(format!(
                "transaction touches {} keys, exceeding limit of {}",
                self.key_count, self.limits.max_keys
            )));
        }
        Ok(())
    }

    /// Check whether the transaction has exceeded its timeout.
    pub fn check_timeout(&self) -> Result<(), CqlError> {
        if self.start_time.elapsed() > self.limits.timeout {
            return Err(CqlError::Invalid(format!(
                "transaction timed out after {:?}",
                self.limits.timeout
            )));
        }
        Ok(())
    }

    /// Return the number of keys touched so far.
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// Return the instant when this transaction started.
    pub fn start_time(&self) -> Instant {
        self.start_time
    }
}

/// Per-connection transaction tracker.
///
/// Manages the set of active transactions and enforces the concurrent
/// transaction limit. Each connection should own one `TransactionTracker`.
#[derive(Debug)]
pub struct TransactionTracker {
    /// Current number of active transactions on this connection.
    active_count: AtomicUsize,
    /// Shared limits configuration.
    limits: Arc<TransactionLimits>,
}

impl TransactionTracker {
    /// Create a new tracker with the given limits.
    pub fn new(limits: Arc<TransactionLimits>) -> Self {
        Self {
            active_count: AtomicUsize::new(0),
            limits,
        }
    }

    /// Attempt to begin a new transaction. Returns a `TransactionState` on
    /// success, or an `Overloaded` error if the concurrent limit is reached.
    pub fn begin_transaction(&self) -> Result<TransactionState, CqlError> {
        // Atomically increment, checking the limit.
        loop {
            let current = self.active_count.load(Ordering::Acquire);
            if current >= self.limits.max_concurrent {
                return Err(CqlError::Overloaded(format!(
                    "too many concurrent transactions: {} (limit {})",
                    current, self.limits.max_concurrent
                )));
            }
            match self.active_count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(TransactionState::new(self.limits.clone())),
                Err(_) => continue, // CAS failed, retry
            }
        }
    }

    /// Mark a transaction as complete, decrementing the active count.
    pub fn end_transaction(&self) {
        let prev = self.active_count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(
            prev > 0,
            "end_transaction called with no active transactions"
        );
    }

    /// Return the current number of active transactions.
    pub fn active_count(&self) -> usize {
        self.active_count.load(Ordering::Acquire)
    }

    /// Return a reference to the limits configuration.
    pub fn limits(&self) -> &TransactionLimits {
        &self.limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_connection_limit() {
        let limits = Arc::new(TransactionLimits {
            max_concurrent: 16,
            ..TransactionLimits::default()
        });
        let tracker = TransactionTracker::new(limits);

        // Start 16 transactions — all should succeed.
        let mut txns: Vec<TransactionState> = Vec::new();
        for i in 0..16 {
            let txn = tracker
                .begin_transaction()
                .unwrap_or_else(|e| panic!("transaction {i} should succeed: {e}"));
            txns.push(txn);
        }
        assert_eq!(tracker.active_count(), 16);

        // 17th must fail with Overloaded.
        let err = tracker.begin_transaction().unwrap_err();
        assert!(
            matches!(err, CqlError::Overloaded(_)),
            "expected Overloaded error, got: {err:?}"
        );

        // End one transaction, then the next attempt should succeed.
        drop(txns.pop());
        tracker.end_transaction();
        assert_eq!(tracker.active_count(), 15);

        let txn = tracker
            .begin_transaction()
            .expect("should succeed after freeing a slot");
        txns.push(txn);
        assert_eq!(tracker.active_count(), 16);
    }

    #[test]
    fn transaction_timeout_abort() {
        let limits = Arc::new(TransactionLimits {
            timeout: Duration::from_millis(50),
            ..TransactionLimits::default()
        });
        let tracker = TransactionTracker::new(limits);
        let txn = tracker.begin_transaction().expect("should succeed");

        // Immediately after creation, timeout check should pass.
        assert!(txn.check_timeout().is_ok());

        // Wait for the timeout to expire.
        std::thread::sleep(Duration::from_millis(80));

        // Now the timeout check should fail.
        let err = txn.check_timeout().unwrap_err();
        assert!(
            matches!(err, CqlError::Invalid(ref msg) if msg.contains("timed out")),
            "expected timeout error, got: {err:?}"
        );
    }

    #[test]
    fn transaction_max_keys_limit() {
        let limits = Arc::new(TransactionLimits {
            max_keys: 128,
            ..TransactionLimits::default()
        });
        let tracker = TransactionTracker::new(limits);
        let mut txn = tracker.begin_transaction().expect("should succeed");

        // Record 128 keys — should be fine.
        txn.record_keys(128)
            .expect("128 keys should be within limit");
        assert_eq!(txn.key_count(), 128);

        // Recording even 1 more key should fail.
        let err = txn.record_keys(1).unwrap_err();
        assert!(
            matches!(err, CqlError::Invalid(ref msg) if msg.contains("exceeding limit")),
            "expected key limit error, got: {err:?}"
        );
    }

    #[test]
    fn transaction_max_keys_configurable() {
        // Configure a lower limit of 10 keys.
        let limits = Arc::new(TransactionLimits {
            max_keys: 10,
            ..TransactionLimits::default()
        });
        let tracker = TransactionTracker::new(limits.clone());
        let mut txn = tracker.begin_transaction().expect("should succeed");

        // 10 keys should succeed.
        txn.record_keys(10).expect("10 keys should be within limit");

        // 11th key should fail.
        let err = txn.record_keys(1).unwrap_err();
        assert!(
            matches!(err, CqlError::Invalid(ref msg) if msg.contains("exceeding limit of 10")),
            "expected key limit error with limit=10, got: {err:?}"
        );

        // Now configure a higher limit of 256 keys.
        let limits_large = Arc::new(TransactionLimits {
            max_keys: 256,
            ..TransactionLimits::default()
        });
        let tracker_large = TransactionTracker::new(limits_large);
        let mut txn_large = tracker_large.begin_transaction().expect("should succeed");

        // 256 keys should succeed.
        txn_large
            .record_keys(256)
            .expect("256 keys should be within limit");

        // 257th key should fail.
        let err = txn_large.record_keys(1).unwrap_err();
        assert!(
            matches!(err, CqlError::Invalid(ref msg) if msg.contains("exceeding limit of 256")),
            "expected key limit error with limit=256, got: {err:?}"
        );
    }

    #[test]
    fn transaction_default_limits() {
        let limits = TransactionLimits::default();
        assert_eq!(limits.max_concurrent, 16);
        assert_eq!(limits.timeout, Duration::from_secs(10));
        assert_eq!(limits.max_keys, 128);
    }

    #[test]
    fn transaction_record_keys_checks_timeout() {
        // Verify that record_keys also checks timeout, not just key count.
        let limits = Arc::new(TransactionLimits {
            timeout: Duration::from_millis(30),
            max_keys: 1000,
            ..TransactionLimits::default()
        });
        let tracker = TransactionTracker::new(limits);
        let mut txn = tracker.begin_transaction().expect("should succeed");

        std::thread::sleep(Duration::from_millis(60));

        let err = txn.record_keys(1).unwrap_err();
        assert!(
            matches!(err, CqlError::Invalid(ref msg) if msg.contains("timed out")),
            "expected timeout error from record_keys, got: {err:?}"
        );
    }

    #[test]
    fn transaction_tracker_end_updates_count() {
        let limits = Arc::new(TransactionLimits::default());
        let tracker = TransactionTracker::new(limits);

        assert_eq!(tracker.active_count(), 0);

        let _txn1 = tracker.begin_transaction().expect("should succeed");
        assert_eq!(tracker.active_count(), 1);

        let _txn2 = tracker.begin_transaction().expect("should succeed");
        assert_eq!(tracker.active_count(), 2);

        tracker.end_transaction();
        assert_eq!(tracker.active_count(), 1);

        tracker.end_transaction();
        assert_eq!(tracker.active_count(), 0);
    }
}
