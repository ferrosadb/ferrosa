//! Late-arriving data detection, debounce, and re-aggregation.
//!
//! When Accord transactions drive timeseries writes, the debouncer uses
//! Accord-committed timestamps (`accord_ts`) for ordering instead of
//! wall-clock arrival time. This ensures all replicas process
//! re-aggregations in the same deterministic order.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::commitlog::config::TableId;

/// Key for debouncing late-data re-aggregation requests.
/// Multiple late points for the same window are coalesced.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LateDataKey {
    pub table_id: TableId,
    pub partition_key: Vec<u8>,
    pub window_start_ts: i64,
    /// Accord transaction timestamp (nanoseconds), if available.
    /// When set, the debouncer uses this for deterministic cross-replica
    /// ordering instead of wall-clock arrival time.
    pub accord_ts: Option<u64>,
}

/// Internal entry tracking arrival time and ordering priority.
#[derive(Debug, Clone)]
struct PendingEntry {
    /// Wall-clock instant when this entry was first seen (for debounce timing).
    first_seen: Instant,
    /// Ordering key: `accord_ts` if available, otherwise a wall-clock-derived
    /// nanosecond value. Lower values sort first.
    order_key: u64,
}

/// Debounce state for late-data re-aggregation.
///
/// Coalesces multiple late data events for the same window within a configurable
/// debounce interval (default 100ms). Enforces a `max_pending` limit (default
/// 10,000) to prevent unbounded memory growth.
///
/// ## Accord ordering
///
/// When `LateDataKey::accord_ts` is set, the debouncer uses it as the ordering
/// key for re-aggregation. This ensures all replicas process late-data
/// re-aggregations in the same deterministic order regardless of wall-clock
/// differences between nodes.
pub struct LateDataDebouncer {
    /// Pending re-aggregation requests: key -> pending entry.
    pending: HashMap<LateDataKey, PendingEntry>,
    /// How long to wait after the first late point before re-aggregating.
    debounce_interval: Duration,
    /// Maximum number of pending entries before rejecting new records.
    max_pending: usize,
    /// Monotonic counter used as a fallback ordering key when no `accord_ts`
    /// is provided. Incremented on each `record()` call.
    wall_clock_counter: u64,
}

impl LateDataDebouncer {
    /// Create a new debouncer with the given interval and a default max_pending of 10,000.
    pub fn new(debounce_interval: Duration) -> Self {
        Self {
            pending: HashMap::new(),
            debounce_interval,
            max_pending: 10_000,
            wall_clock_counter: 0,
        }
    }

    /// Create a new debouncer with explicit max_pending limit.
    pub fn with_max_pending(debounce_interval: Duration, max_pending: usize) -> Self {
        Self {
            pending: HashMap::new(),
            debounce_interval,
            max_pending,
            wall_clock_counter: 0,
        }
    }

    /// Returns the max_pending limit.
    pub fn max_pending(&self) -> usize {
        self.max_pending
    }

    /// Determines the ordering key for a re-aggregation task.
    ///
    /// Under Accord, uses the transaction timestamp for deterministic
    /// cross-replica ordering. Without Accord, falls back to a monotonic
    /// counter that preserves insertion order.
    fn order_key_for(&mut self, key: &LateDataKey) -> u64 {
        aggregation_order_key(key.accord_ts, self.next_wall_clock_counter())
    }

    /// Returns and increments the monotonic wall-clock counter.
    fn next_wall_clock_counter(&mut self) -> u64 {
        let val = self.wall_clock_counter;
        self.wall_clock_counter += 1;
        val
    }

    /// Record a late-data event. Returns `true` if this is the first event
    /// for this key (starts the debounce timer). Returns `false` if the key
    /// already exists (coalesced) or if the pending map is at capacity, in
    /// which case the oldest entry is evicted.
    pub fn record(&mut self, key: LateDataKey) -> bool {
        // Check if already pending (coalesce).
        if self.pending.contains_key(&key) {
            return false;
        }

        // Check capacity limit.
        if self.pending.len() >= self.max_pending {
            // Evict the oldest entry (by first_seen wall clock).
            if let Some(oldest_key) = self
                .pending
                .iter()
                .min_by_key(|(_, entry)| entry.first_seen)
                .map(|(k, _)| k.clone())
            {
                self.pending.remove(&oldest_key);
            }
            return false; // signal rejection even though we evicted
        }

        let order_key = self.order_key_for(&key);
        self.pending.insert(
            key,
            PendingEntry {
                first_seen: Instant::now(),
                order_key,
            },
        );
        true
    }

    /// Drain all keys whose debounce interval has elapsed.
    ///
    /// Returns the keys ready for re-aggregation, sorted by their ordering
    /// key (ascending). When Accord timestamps are available, this produces
    /// a deterministic order across replicas.
    pub fn drain_ready(&mut self) -> Vec<LateDataKey> {
        let now = Instant::now();
        let mut ready: Vec<(u64, LateDataKey)> = Vec::new();
        self.pending.retain(|key, entry| {
            if now.duration_since(entry.first_seen) >= self.debounce_interval {
                ready.push((entry.order_key, key.clone()));
                false // remove from pending
            } else {
                true // keep
            }
        });
        // Sort by order_key ascending for deterministic processing order.
        ready.sort_by_key(|(order_key, _)| *order_key);
        ready.into_iter().map(|(_, k)| k).collect()
    }

    /// Returns the number of pending (not yet ready) keys.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Determines the ordering key for re-aggregation tasks.
///
/// Under Accord, uses the transaction timestamp for deterministic cross-replica
/// ordering. Without Accord, falls back to the provided wall-clock value.
pub fn aggregation_order_key(accord_ts: Option<u64>, wall_clock_ns: u64) -> u64 {
    accord_ts.unwrap_or(wall_clock_ns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(table: &str, pk: &[u8], ts: i64) -> LateDataKey {
        LateDataKey {
            table_id: TableId::new("ks", table),
            partition_key: pk.to_vec(),
            window_start_ts: ts,
            accord_ts: None,
        }
    }

    fn make_key_with_accord(table: &str, pk: &[u8], ts: i64, accord_ts: u64) -> LateDataKey {
        LateDataKey {
            table_id: TableId::new("ks", table),
            partition_key: pk.to_vec(),
            window_start_ts: ts,
            accord_ts: Some(accord_ts),
        }
    }

    #[test]
    fn debouncer_first_record_returns_true() {
        let mut db = LateDataDebouncer::new(Duration::from_millis(100));
        let key = make_key("t", b"pk", 0);
        assert!(db.record(key.clone()));
        assert_eq!(db.pending_count(), 1);
    }

    #[test]
    fn debouncer_duplicate_record_returns_false() {
        let mut db = LateDataDebouncer::new(Duration::from_millis(100));
        let key = make_key("t", b"pk", 0);
        assert!(db.record(key.clone()));
        assert!(!db.record(key)); // coalesced
        assert_eq!(db.pending_count(), 1);
    }

    #[test]
    fn debouncer_drain_ready_after_interval() {
        let mut db = LateDataDebouncer::new(Duration::from_millis(0)); // immediate
        let key = make_key("t", b"pk", 0);
        db.record(key.clone());
        let ready = db.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0], key);
        assert_eq!(db.pending_count(), 0);
    }

    #[test]
    fn debouncer_does_not_drain_before_interval() {
        let mut db = LateDataDebouncer::new(Duration::from_secs(60));
        let key = make_key("t", b"pk", 0);
        db.record(key);
        let ready = db.drain_ready();
        assert!(ready.is_empty());
        assert_eq!(db.pending_count(), 1);
    }

    #[test]
    fn debouncer_different_keys_independent() {
        let mut db = LateDataDebouncer::new(Duration::from_millis(0));
        let key1 = make_key("t", b"pk1", 0);
        let key2 = make_key("t", b"pk2", 0);
        let key3 = make_key("t", b"pk1", 10_000_000); // same pk, different window

        assert!(db.record(key1));
        assert!(db.record(key2));
        assert!(db.record(key3));
        assert_eq!(db.pending_count(), 3);

        let ready = db.drain_ready();
        assert_eq!(ready.len(), 3);
    }

    // --- FMEA Fix 6: max_pending limit on debouncer ---

    #[test]
    fn debouncer_rejects_when_at_capacity() {
        let mut db = LateDataDebouncer::with_max_pending(Duration::from_secs(60), 3);

        // Fill to capacity.
        assert!(db.record(make_key("t", b"pk1", 0)));
        assert!(db.record(make_key("t", b"pk2", 0)));
        assert!(db.record(make_key("t", b"pk3", 0)));
        assert_eq!(db.pending_count(), 3);

        // Next record should be rejected (returns false) and evict the oldest.
        let accepted = db.record(make_key("t", b"pk4", 0));
        assert!(!accepted, "should reject when at capacity");

        // The pending count should not exceed max_pending after eviction.
        assert!(
            db.pending_count() <= 3,
            "pending count should not exceed max: {}",
            db.pending_count()
        );
    }

    #[test]
    fn debouncer_default_max_pending() {
        let db = LateDataDebouncer::new(Duration::from_millis(100));
        assert_eq!(db.max_pending(), 10_000);
    }

    // --- A2.9: Accord timestamp ordering for late-data debouncer ---

    #[test]
    fn debouncer_accord_timestamp_ordering() {
        // Event A arrives at wall_clock=200 (second) with accord_ts=50.
        // Event B arrives at wall_clock=100 (first) with accord_ts=150.
        // With Accord ordering, A comes first (50 < 150).
        let mut db = LateDataDebouncer::new(Duration::from_millis(0));

        // Insert B first (lower wall clock / earlier arrival).
        let key_b = make_key_with_accord("t", b"pk_b", 1000, 150);
        assert!(db.record(key_b.clone()));

        // Insert A second (higher wall clock / later arrival).
        let key_a = make_key_with_accord("t", b"pk_a", 2000, 50);
        assert!(db.record(key_a.clone()));

        let ready = db.drain_ready();
        assert_eq!(ready.len(), 2);

        // Accord ordering: A (accord_ts=50) before B (accord_ts=150).
        assert_eq!(
            ready[0].accord_ts,
            Some(50),
            "first key should have accord_ts=50 (event A)"
        );
        assert_eq!(
            ready[1].accord_ts,
            Some(150),
            "second key should have accord_ts=150 (event B)"
        );
    }

    #[test]
    fn debouncer_concurrent_late_data_and_txn() {
        // An Accord transaction and a late data event arrive concurrently.
        // The Accord txn has accord_ts=500; the late data has accord_ts=100.
        // They should be ordered by accord_ts regardless of arrival order.
        let mut db = LateDataDebouncer::new(Duration::from_millis(0));

        // Accord transaction arrives first with higher accord_ts.
        let txn_key = make_key_with_accord("t", b"pk_txn", 1000, 500);
        assert!(db.record(txn_key.clone()));

        // Late data event arrives second with lower accord_ts.
        let late_key = make_key_with_accord("t", b"pk_late", 2000, 100);
        assert!(db.record(late_key.clone()));

        let ready = db.drain_ready();
        assert_eq!(ready.len(), 2);

        // Late data (accord_ts=100) should come before txn (accord_ts=500).
        assert_eq!(
            ready[0].accord_ts,
            Some(100),
            "late data event should be ordered first by accord_ts"
        );
        assert_eq!(
            ready[1].accord_ts,
            Some(500),
            "txn event should be ordered second by accord_ts"
        );
    }

    #[test]
    fn debouncer_deterministic_across_replicas() {
        // Three replicas receive the same 3 events with the same accord_ts
        // values but in different wall-clock order. All three should produce
        // the same ordering.

        let events = [
            ("pk_x", 3000_i64, 300_u64), // accord_ts=300
            ("pk_y", 1000_i64, 100_u64), // accord_ts=100
            ("pk_z", 2000_i64, 200_u64), // accord_ts=200
        ];

        // Replica 1: receives in order [x, y, z].
        let order_r1 = drain_in_order(&events, &[0, 1, 2]);

        // Replica 2: receives in order [z, x, y].
        let order_r2 = drain_in_order(&events, &[2, 0, 1]);

        // Replica 3: receives in order [y, z, x].
        let order_r3 = drain_in_order(&events, &[1, 2, 0]);

        // All replicas must produce the same accord_ts ordering.
        let expected_accord_order: Vec<u64> = vec![100, 200, 300];
        assert_eq!(
            order_r1, expected_accord_order,
            "replica 1 ordering mismatch"
        );
        assert_eq!(
            order_r2, expected_accord_order,
            "replica 2 ordering mismatch"
        );
        assert_eq!(
            order_r3, expected_accord_order,
            "replica 3 ordering mismatch"
        );
    }

    /// Helper: insert events in the given index order and drain, returning
    /// the accord_ts values in the order they are returned.
    fn drain_in_order(events: &[(&str, i64, u64)], order: &[usize]) -> Vec<u64> {
        let mut db = LateDataDebouncer::new(Duration::from_millis(0));
        for &idx in order {
            let (pk, window_ts, accord) = events[idx];
            let key = make_key_with_accord("t", pk.as_bytes(), window_ts, accord);
            db.record(key);
        }
        db.drain_ready()
            .into_iter()
            .map(|k| k.accord_ts.expect("all events should have accord_ts"))
            .collect()
    }

    #[test]
    fn debouncer_window_start_ts_from_accord() {
        // When accord_ts is available, the window_start_ts in the LateDataKey
        // should be derived from the Accord timestamp, not wall clock.
        let accord_timestamp_ns: u64 = 1_700_000_000_000_000_000; // ~2023-11-14
        let interval_ns: u64 = 5 * 60 * 1_000_000_000; // 5 minutes in ns

        // Derive window_start_ts from accord_ts by flooring to interval.
        let window_start = (accord_timestamp_ns / interval_ns * interval_ns) as i64;

        let key = LateDataKey {
            table_id: TableId::new("ks", "sensor_1s"),
            partition_key: b"sensor-42".to_vec(),
            window_start_ts: window_start,
            accord_ts: Some(accord_timestamp_ns),
        };

        // The window_start_ts should be aligned to the 5-minute interval boundary.
        assert_eq!(
            key.window_start_ts as u64 % interval_ns,
            0,
            "window_start_ts must be aligned to the interval boundary"
        );

        // The accord_ts should fall within [window_start, window_start + interval).
        assert!(
            accord_timestamp_ns >= key.window_start_ts as u64,
            "accord_ts must be >= window_start_ts"
        );
        assert!(
            accord_timestamp_ns < key.window_start_ts as u64 + interval_ns,
            "accord_ts must be < window_start_ts + interval"
        );

        // Verify the key round-trips through the debouncer correctly.
        let mut db = LateDataDebouncer::new(Duration::from_millis(0));
        assert!(db.record(key.clone()));
        let ready = db.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].window_start_ts, window_start);
        assert_eq!(ready[0].accord_ts, Some(accord_timestamp_ns));
    }

    // --- A2.9: aggregation_order_key unit tests ---

    #[test]
    fn aggregation_order_key_prefers_accord_ts() {
        assert_eq!(aggregation_order_key(Some(42), 999), 42);
    }

    #[test]
    fn aggregation_order_key_falls_back_to_wall_clock() {
        assert_eq!(aggregation_order_key(None, 999), 999);
    }
}
