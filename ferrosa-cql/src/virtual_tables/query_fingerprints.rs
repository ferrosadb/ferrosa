//! Query fingerprints virtual table (T-30: O5.3).
//!
//! Maintains a `DashMap<u64, FingerprintEntry>` of parameterized query
//! fingerprints with count, total_duration, and last_seen. Ring buffer
//! capped at 10k entries (evict lowest count).
//!
//! Virtual table: `system_observability.query_fingerprints`

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};

/// Maximum number of fingerprint entries before eviction.
const MAX_ENTRIES: usize = 10_000;

/// A single fingerprint entry tracking statistics for a parameterized query.
pub struct FingerprintEntry {
    /// The parameterized query text (with literals replaced by `?`).
    pub query_text: String,
    /// Number of times this query has been executed.
    pub count: AtomicU64,
    /// Total execution duration in microseconds.
    pub total_duration_us: AtomicI64,
    /// Last seen timestamp (epoch millis).
    pub last_seen_ms: AtomicI64,
}

/// Concurrent tracker for query fingerprints.
pub struct QueryFingerprintTracker {
    entries: DashMap<u64, Arc<FingerprintEntry>>,
}

impl QueryFingerprintTracker {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Compute a fingerprint hash from parameterized query text.
    pub fn compute_hash(query: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        hasher.finish()
    }

    /// Record an execution of a query with the given fingerprint.
    pub fn record(&self, query: &str, duration_us: i64) {
        let hash = Self::compute_hash(query);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        if let Some(entry) = self.entries.get(&hash) {
            entry.count.fetch_add(1, Ordering::Relaxed);
            entry
                .total_duration_us
                .fetch_add(duration_us, Ordering::Relaxed);
            entry.last_seen_ms.store(now_ms, Ordering::Relaxed);
            return;
        }

        // Evict if at capacity — remove the entry with the lowest count.
        if self.entries.len() >= MAX_ENTRIES {
            self.evict_lowest();
        }

        let entry = Arc::new(FingerprintEntry {
            query_text: query.to_string(),
            count: AtomicU64::new(1),
            total_duration_us: AtomicI64::new(duration_us),
            last_seen_ms: AtomicI64::new(now_ms),
        });
        self.entries.insert(hash, entry);
    }

    /// Evict the entry with the lowest execution count.
    fn evict_lowest(&self) {
        let mut lowest_key = None;
        let mut lowest_count = u64::MAX;

        for entry in self.entries.iter() {
            let count = entry.value().count.load(Ordering::Relaxed);
            if count < lowest_count {
                lowest_count = count;
                lowest_key = Some(*entry.key());
            }
        }

        if let Some(key) = lowest_key {
            self.entries.remove(&key);
        }
    }

    /// Number of tracked fingerprints.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the tracker is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for QueryFingerprintTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Virtual table: `system_observability.query_fingerprints`
pub struct QueryFingerprintsTable {
    tracker: Arc<QueryFingerprintTracker>,
    columns: Vec<VirtualColumnDef>,
}

impl QueryFingerprintsTable {
    pub fn new(tracker: Arc<QueryFingerprintTracker>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "fingerprint_hash".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "query_text".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "count".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "total_duration_us".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "last_seen_ms".to_string(),
                data_type: DataType::BigInt,
            },
        ];
        Self { tracker, columns }
    }
}

impl VirtualTable for QueryFingerprintsTable {
    fn name(&self) -> &str {
        "query_fingerprints"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        self.tracker
            .entries
            .iter()
            .map(|entry| {
                let hash = *entry.key();
                let fp = entry.value();
                let cells = vec![
                    CellValue::live((hash as i64).to_be_bytes().to_vec(), 0),
                    CellValue::live(fp.query_text.as_bytes().to_vec(), 0),
                    CellValue::live(
                        (fp.count.load(Ordering::Relaxed) as i64)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        fp.total_duration_us
                            .load(Ordering::Relaxed)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                    CellValue::live(
                        fp.last_seen_ms
                            .load(Ordering::Relaxed)
                            .to_be_bytes()
                            .to_vec(),
                        0,
                    ),
                ];
                VirtualRow { cells }
            })
            .collect()
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_tracker_records_and_deduplicates() {
        let tracker = Arc::new(QueryFingerprintTracker::new());
        tracker.record("SELECT * FROM users WHERE id = ?", 100);
        tracker.record("SELECT * FROM users WHERE id = ?", 200);
        assert_eq!(tracker.len(), 1);

        let table = QueryFingerprintsTable::new(tracker.clone());
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        // Count should be 2
        let count_bytes = rows[0].cells[2].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(count_bytes.try_into().unwrap()), 2);
        // Total duration should be 300
        let dur_bytes = rows[0].cells[3].value.as_deref().unwrap();
        assert_eq!(i64::from_be_bytes(dur_bytes.try_into().unwrap()), 300);
    }

    #[test]
    fn fingerprint_tracker_different_queries() {
        let tracker = QueryFingerprintTracker::new();
        tracker.record("SELECT * FROM users", 10);
        tracker.record("INSERT INTO users VALUES (?)", 20);
        assert_eq!(tracker.len(), 2);
    }

    #[test]
    fn fingerprint_table_metadata() {
        let tracker = Arc::new(QueryFingerprintTracker::new());
        let table = QueryFingerprintsTable::new(tracker);
        assert_eq!(table.name(), "query_fingerprints");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 5);
    }

    #[test]
    fn fingerprint_eviction_at_capacity() {
        let tracker = QueryFingerprintTracker::new();
        // Fill up with MAX_ENTRIES queries
        for i in 0..MAX_ENTRIES {
            tracker.record(&format!("SELECT {i}"), 1);
        }
        assert_eq!(tracker.len(), MAX_ENTRIES);
        // Adding one more should evict, keeping at MAX_ENTRIES
        tracker.record("SELECT overflow", 1);
        assert!(tracker.len() <= MAX_ENTRIES);
    }
}
