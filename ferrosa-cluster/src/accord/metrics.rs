//! Accord observability metrics.
//!
//! [`AccordMetrics`] exposes atomic counters for key operational signals:
//! transaction lifecycle, conflict detection, clock skew, protocol log
//! pressure, dependency-wait latency, and deadlock detection. All counters
//! use relaxed atomic ordering — they are advisory telemetry, not
//! synchronization primitives.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// AccordMetrics
// ---------------------------------------------------------------------------

/// Atomic counters for Accord transaction observability.
///
/// Every field is `pub` so tests (and production dashboards) can read or
/// update counters directly. All updates use [`Ordering::Relaxed`] — the
/// counters are advisory and do not participate in consensus ordering.
pub struct AccordMetrics {
    // -- Transaction lifecycle -----------------------------------------------
    /// Number of transactions currently between PreAccept and Apply.
    pub txn_in_flight: AtomicI64,

    /// Number of recovery coordinators currently active.
    pub recovery_in_progress: AtomicI64,

    /// Total transactions committed via fast path (1 RTT).
    pub fast_path_commits: AtomicU64,

    /// Total transactions committed via slow path (2 RTT).
    pub slow_path_commits: AtomicU64,

    // -- Conflict index -----------------------------------------------------
    /// Current number of entries in the shard-local ConflictIndex.
    pub conflict_index_size: AtomicU64,

    // -- Reorder buffer -----------------------------------------------------
    /// Current depth of the per-shard reorder buffer.
    pub reorder_buffer_depth: AtomicU64,

    // -- Clock validation ---------------------------------------------------
    /// Maximum observed clock skew in nanoseconds across replicas.
    pub skew_max_ns: AtomicU64,

    // -- Protocol log -------------------------------------------------------
    /// Total bytes written to the on-disk protocol log since startup.
    pub protocol_log_size_bytes: AtomicU64,

    // -- Dependency wait ----------------------------------------------------
    /// Approximate p99 dep-wait duration in microseconds.
    ///
    /// Updated by the dep-wait subsystem after each completed wait. In this
    /// simplified implementation we track the single maximum observed wait
    /// as a proxy for p99.
    pub dep_wait_duration_p99_us: AtomicU64,

    // -- Deadlock detection -------------------------------------------------
    /// Total number of deadlocks detected (and broken) since startup.
    pub deadlock_detected: AtomicU64,
}

impl AccordMetrics {
    /// Create a new metrics instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            txn_in_flight: AtomicI64::new(0),
            recovery_in_progress: AtomicI64::new(0),
            fast_path_commits: AtomicU64::new(0),
            slow_path_commits: AtomicU64::new(0),
            conflict_index_size: AtomicU64::new(0),
            reorder_buffer_depth: AtomicU64::new(0),
            skew_max_ns: AtomicU64::new(0),
            protocol_log_size_bytes: AtomicU64::new(0),
            dep_wait_duration_p99_us: AtomicU64::new(0),
            deadlock_detected: AtomicU64::new(0),
        }
    }

    // -- Transaction lifecycle helpers ---------------------------------------

    /// Record that a new transaction entered the in-flight window.
    pub fn txn_started(&self) {
        self.txn_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a transaction left the in-flight window (committed or aborted).
    pub fn txn_finished(&self) {
        self.txn_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record that a recovery coordinator has been started.
    pub fn recovery_started(&self) {
        self.recovery_in_progress.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a recovery coordinator has completed.
    pub fn recovery_finished(&self) {
        self.recovery_in_progress.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a fast-path commit.
    pub fn record_fast_path(&self) {
        self.fast_path_commits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a slow-path commit.
    pub fn record_slow_path(&self) {
        self.slow_path_commits.fetch_add(1, Ordering::Relaxed);
    }

    /// Compute the fast-path ratio as a fraction in `[0.0, 1.0]`.
    ///
    /// Returns `0.0` if no transactions have committed yet.
    pub fn fast_path_ratio(&self) -> f64 {
        let fast = self.fast_path_commits.load(Ordering::Relaxed);
        let slow = self.slow_path_commits.load(Ordering::Relaxed);
        let total = fast + slow;
        if total == 0 {
            return 0.0;
        }
        fast as f64 / total as f64
    }

    // -- Conflict index helpers ---------------------------------------------

    /// Set the current conflict index size.
    pub fn set_conflict_index_size(&self, size: u64) {
        self.conflict_index_size.store(size, Ordering::Relaxed);
    }

    // -- Reorder buffer helpers ---------------------------------------------

    /// Set the current reorder buffer depth.
    pub fn set_reorder_buffer_depth(&self, depth: u64) {
        self.reorder_buffer_depth.store(depth, Ordering::Relaxed);
    }

    // -- Clock validation helpers -------------------------------------------

    /// Update the maximum observed skew (monotonically non-decreasing).
    pub fn update_skew_max_ns(&self, skew_ns: u64) {
        self.skew_max_ns.fetch_max(skew_ns, Ordering::Relaxed);
    }

    // -- Protocol log helpers -----------------------------------------------

    /// Add bytes to the protocol log size counter.
    pub fn add_protocol_log_bytes(&self, bytes: u64) {
        self.protocol_log_size_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    // -- Dependency wait helpers --------------------------------------------

    /// Record a completed dep-wait duration. Updates the p99 proxy if this
    /// duration exceeds the previous maximum.
    pub fn record_dep_wait_us(&self, duration_us: u64) {
        self.dep_wait_duration_p99_us
            .fetch_max(duration_us, Ordering::Relaxed);
    }

    // -- Deadlock helpers ---------------------------------------------------

    /// Record that a deadlock was detected and broken.
    pub fn record_deadlock(&self) {
        self.deadlock_detected.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for AccordMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests -- A6.9: Accord Observability Metrics (9 tests)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    // -----------------------------------------------------------------------
    // A6.9-T1: txn_in_flight increments and decrements
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_accord_txn_in_flight() {
        let m = AccordMetrics::new();
        assert_eq!(m.txn_in_flight.load(Ordering::Relaxed), 0);

        // Start 3 transactions.
        m.txn_started();
        m.txn_started();
        m.txn_started();
        assert_eq!(m.txn_in_flight.load(Ordering::Relaxed), 3);

        // Finish 2 of them.
        m.txn_finished();
        m.txn_finished();
        assert_eq!(m.txn_in_flight.load(Ordering::Relaxed), 1);

        // Finish the last one.
        m.txn_finished();
        assert_eq!(m.txn_in_flight.load(Ordering::Relaxed), 0);
    }

    // -----------------------------------------------------------------------
    // A6.9-T2: recovery_in_progress increments and decrements
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_accord_recovery_in_progress() {
        let m = AccordMetrics::new();
        assert_eq!(m.recovery_in_progress.load(Ordering::Relaxed), 0);

        m.recovery_started();
        m.recovery_started();
        assert_eq!(m.recovery_in_progress.load(Ordering::Relaxed), 2);

        m.recovery_finished();
        assert_eq!(m.recovery_in_progress.load(Ordering::Relaxed), 1);

        m.recovery_finished();
        assert_eq!(m.recovery_in_progress.load(Ordering::Relaxed), 0);
    }

    // -----------------------------------------------------------------------
    // A6.9-T3: fast_path_ratio tracks fast vs slow commits
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_accord_fast_path_ratio() {
        let m = AccordMetrics::new();

        // No commits yet: ratio is 0.
        assert_eq!(m.fast_path_ratio(), 0.0);

        // 3 fast-path, 1 slow-path -> 75%
        m.record_fast_path();
        m.record_fast_path();
        m.record_fast_path();
        m.record_slow_path();

        let ratio = m.fast_path_ratio();
        assert!(
            (ratio - 0.75).abs() < f64::EPSILON,
            "expected 0.75, got {}",
            ratio
        );

        // All fast-path -> 100%
        // Add 1 more fast: 4 fast, 1 slow = 80%
        m.record_fast_path();
        let ratio2 = m.fast_path_ratio();
        assert!(
            (ratio2 - 0.8).abs() < f64::EPSILON,
            "expected 0.8, got {}",
            ratio2
        );
    }

    // -----------------------------------------------------------------------
    // A6.9-T4: conflict_index_size updates on set
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_conflict_index_size() {
        let m = AccordMetrics::new();
        assert_eq!(m.conflict_index_size.load(Ordering::Relaxed), 0);

        m.set_conflict_index_size(42);
        assert_eq!(m.conflict_index_size.load(Ordering::Relaxed), 42);

        m.set_conflict_index_size(0);
        assert_eq!(m.conflict_index_size.load(Ordering::Relaxed), 0);
    }

    // -----------------------------------------------------------------------
    // A6.9-T5: reorder_buffer_depth updates on set
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_reorder_buffer_depth() {
        let m = AccordMetrics::new();
        assert_eq!(m.reorder_buffer_depth.load(Ordering::Relaxed), 0);

        m.set_reorder_buffer_depth(17);
        assert_eq!(m.reorder_buffer_depth.load(Ordering::Relaxed), 17);

        // Can grow and shrink.
        m.set_reorder_buffer_depth(100);
        assert_eq!(m.reorder_buffer_depth.load(Ordering::Relaxed), 100);

        m.set_reorder_buffer_depth(5);
        assert_eq!(m.reorder_buffer_depth.load(Ordering::Relaxed), 5);
    }

    // -----------------------------------------------------------------------
    // A6.9-T6: skew_max_ns is monotonically non-decreasing
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_skew_max_ns() {
        let m = AccordMetrics::new();
        assert_eq!(m.skew_max_ns.load(Ordering::Relaxed), 0);

        m.update_skew_max_ns(500);
        assert_eq!(m.skew_max_ns.load(Ordering::Relaxed), 500);

        // Smaller value does not decrease the max.
        m.update_skew_max_ns(100);
        assert_eq!(
            m.skew_max_ns.load(Ordering::Relaxed),
            500,
            "skew_max_ns must be monotonically non-decreasing"
        );

        // Larger value updates the max.
        m.update_skew_max_ns(1000);
        assert_eq!(m.skew_max_ns.load(Ordering::Relaxed), 1000);
    }

    // -----------------------------------------------------------------------
    // A6.9-T7: protocol_log_size_bytes accumulates
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_protocol_log_size_bytes() {
        let m = AccordMetrics::new();
        assert_eq!(m.protocol_log_size_bytes.load(Ordering::Relaxed), 0);

        m.add_protocol_log_bytes(4096);
        assert_eq!(m.protocol_log_size_bytes.load(Ordering::Relaxed), 4096);

        m.add_protocol_log_bytes(8192);
        assert_eq!(
            m.protocol_log_size_bytes.load(Ordering::Relaxed),
            4096 + 8192,
            "protocol_log_size_bytes must be cumulative"
        );
    }

    // -----------------------------------------------------------------------
    // A6.9-T8: dep_wait_duration_p99 tracks max observed wait
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_dep_wait_duration_p99() {
        let m = AccordMetrics::new();
        assert_eq!(m.dep_wait_duration_p99_us.load(Ordering::Relaxed), 0);

        m.record_dep_wait_us(100);
        assert_eq!(m.dep_wait_duration_p99_us.load(Ordering::Relaxed), 100);

        // Smaller duration does not lower the p99 proxy.
        m.record_dep_wait_us(50);
        assert_eq!(
            m.dep_wait_duration_p99_us.load(Ordering::Relaxed),
            100,
            "p99 proxy must not decrease"
        );

        // Larger duration updates it.
        m.record_dep_wait_us(500);
        assert_eq!(m.dep_wait_duration_p99_us.load(Ordering::Relaxed), 500);
    }

    // -----------------------------------------------------------------------
    // A6.9-T9: deadlock_detected counts up
    // -----------------------------------------------------------------------

    #[test]
    fn metrics_deadlock_detected() {
        let m = AccordMetrics::new();
        assert_eq!(m.deadlock_detected.load(Ordering::Relaxed), 0);

        m.record_deadlock();
        assert_eq!(m.deadlock_detected.load(Ordering::Relaxed), 1);

        m.record_deadlock();
        m.record_deadlock();
        assert_eq!(m.deadlock_detected.load(Ordering::Relaxed), 3);
    }
}
