//! Late-arriving data detection, debounce, and re-aggregation.

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
}

/// Debounce state for late-data re-aggregation.
///
/// Coalesces multiple late data events for the same window within a configurable
/// debounce interval (default 100ms).
pub struct LateDataDebouncer {
    /// Pending re-aggregation requests: key -> first_seen timestamp.
    pending: HashMap<LateDataKey, Instant>,
    /// How long to wait after the first late point before re-aggregating.
    debounce_interval: Duration,
}

impl LateDataDebouncer {
    /// Create a new debouncer with the given interval.
    pub fn new(debounce_interval: Duration) -> Self {
        Self {
            pending: HashMap::new(),
            debounce_interval,
        }
    }

    /// Record a late-data event. Returns `true` if this is the first event
    /// for this key (starts the debounce timer).
    pub fn record(&mut self, key: LateDataKey) -> bool {
        if let std::collections::hash_map::Entry::Vacant(e) = self.pending.entry(key) {
            e.insert(Instant::now());
            true
        } else {
            false // already pending, coalesced
        }
    }

    /// Drain all keys whose debounce interval has elapsed.
    /// Returns the keys ready for re-aggregation.
    pub fn drain_ready(&mut self) -> Vec<LateDataKey> {
        let now = Instant::now();
        let mut ready = Vec::new();
        self.pending.retain(|key, first_seen| {
            if now.duration_since(*first_seen) >= self.debounce_interval {
                ready.push(key.clone());
                false // remove from pending
            } else {
                true // keep
            }
        });
        ready
    }

    /// Returns the number of pending (not yet ready) keys.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(table: &str, pk: &[u8], ts: i64) -> LateDataKey {
        LateDataKey {
            table_id: TableId::new("ks", table),
            partition_key: pk.to_vec(),
            window_start_ts: ts,
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
}
