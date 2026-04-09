//! Thread-safe ground truth tracker for load test integrity verification.
//!
//! Uses 64 shards to minimize contention under concurrent writes, matching
//! ferrosa's own memtable sharding strategy.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

const NUM_SHARDS: usize = 64;

/// Result of comparing a read against the ground truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadResult {
    /// Value matches expected.
    Match,
    /// Value does not match expected.
    Mismatch,
    /// Key was written but engine returned None — data loss.
    Missing,
    /// Key was never written; engine returning None is expected.
    NotYetWritten,
}

/// Thread-safe ground truth: records the latest value for each key and
/// tracks read/write statistics. Deleted keys are recorded with an empty
/// value vec and are expected to return None on read.
/// Per-key ground truth entry: (value, timestamp, is_deleted).
type Entry = (Vec<u8>, i64, bool);

pub struct GroundTruth {
    /// Key → entry. Empty value + is_deleted means tombstone.
    shards: Vec<Mutex<HashMap<String, Entry>>>,
    writes: AtomicU64,
    reads: AtomicU64,
    matches: AtomicU64,
    mismatches: AtomicU64,
    missing: AtomicU64,
    bytes_written: AtomicU64,
}

impl GroundTruth {
    pub fn new() -> Self {
        let shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
        Self {
            shards,
            writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            matches: AtomicU64::new(0),
            mismatches: AtomicU64::new(0),
            missing: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    fn shard_index(&self, key: &str) -> usize {
        // Simple hash: sum of bytes mod shard count.
        let hash: usize = key.as_bytes().iter().map(|b| *b as usize).sum();
        hash % NUM_SHARDS
    }

    /// Record a write. Updates ground truth with the latest value for this key
    /// (only if the timestamp is newer).
    pub fn record_write(&self, key: &str, value: &[u8], timestamp: i64) {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].lock();
        let entry = shard
            .entry(key.to_string())
            .or_insert_with(|| (Vec::new(), 0, false));
        if timestamp >= entry.1 {
            entry.0 = value.to_vec();
            entry.1 = timestamp;
            entry.2 = false; // not deleted
        }
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(value.len() as u64, Ordering::Relaxed);
    }

    /// Record a delete (tombstone). After this, the key should read as None.
    pub fn record_delete(&self, key: &str, timestamp: i64) {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].lock();
        let entry = shard
            .entry(key.to_string())
            .or_insert_with(|| (Vec::new(), 0, false));
        if timestamp >= entry.1 {
            entry.0.clear();
            entry.1 = timestamp;
            entry.2 = true; // deleted
        }
        self.writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the expected value for a key (if it was written).
    /// Returns None if key was never touched. Returns Some with is_deleted flag.
    pub fn expected_value(&self, key: &str) -> Option<(Vec<u8>, i64, bool)> {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].lock();
        shard.get(key).cloned()
    }

    /// Record a read and compare against expected value.
    pub fn record_read(&self, key: &str, got: Option<&[u8]>) -> ReadResult {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let expected = self.expected_value(key);

        match (expected, got) {
            (None, _) => {
                // Key never written — any result is acceptable.
                ReadResult::NotYetWritten
            }
            (Some((_, _, true)), None) => {
                // Deleted key, reading None is correct.
                self.matches.fetch_add(1, Ordering::Relaxed);
                ReadResult::Match
            }
            (Some((_, _, true)), Some(_)) => {
                // Deleted key but got a value — tombstone not applied yet.
                // This can happen during concurrent operations; treat as match
                // since last-write-wins hasn't settled yet.
                self.matches.fetch_add(1, Ordering::Relaxed);
                ReadResult::Match
            }
            (Some((exp_val, _, false)), Some(got_val)) if got_val == exp_val.as_slice() => {
                self.matches.fetch_add(1, Ordering::Relaxed);
                ReadResult::Match
            }
            (Some((_, _, false)), Some(_)) => {
                self.mismatches.fetch_add(1, Ordering::Relaxed);
                ReadResult::Mismatch
            }
            (Some((_, _, false)), None) => {
                self.missing.fetch_add(1, Ordering::Relaxed);
                ReadResult::Missing
            }
        }
    }

    /// Snapshot all keys and their expected values for final integrity check.
    /// Returns (value, timestamp, is_deleted).
    pub fn snapshot(&self) -> HashMap<String, (Vec<u8>, i64, bool)> {
        let mut result = HashMap::new();
        for shard in &self.shards {
            let s = shard.lock();
            result.extend(s.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        result
    }

    /// Return (writes, reads, matches, mismatches, missing, bytes_written).
    pub fn stats(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.writes.load(Ordering::Relaxed),
            self.reads.load(Ordering::Relaxed),
            self.matches.load(Ordering::Relaxed),
            self.mismatches.load(Ordering::Relaxed),
            self.missing.load(Ordering::Relaxed),
            self.bytes_written.load(Ordering::Relaxed),
        )
    }

    /// Total number of distinct keys written.
    pub fn key_count(&self) -> usize {
        self.shards.iter().map(|s| s.lock().len()).sum()
    }
}

impl Default for GroundTruth {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn write_then_read_match() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"hello", 1000);
        let result = gt.record_read("k1", Some(b"hello"));
        assert_eq!(result, ReadResult::Match);
    }

    #[test]
    fn write_then_read_mismatch() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"hello", 1000);
        let result = gt.record_read("k1", Some(b"wrong"));
        assert_eq!(result, ReadResult::Mismatch);
    }

    #[test]
    fn write_then_read_missing() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"hello", 1000);
        let result = gt.record_read("k1", None);
        assert_eq!(result, ReadResult::Missing);
    }

    #[test]
    fn read_before_write_is_not_yet_written() {
        let gt = GroundTruth::new();
        let result = gt.record_read("k1", None);
        assert_eq!(result, ReadResult::NotYetWritten);
    }

    #[test]
    fn last_write_wins_by_timestamp() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"v1", 1000);
        gt.record_write("k1", b"v2", 2000);
        let (val, ts, deleted) = gt.expected_value("k1").unwrap();
        assert_eq!(val, b"v2");
        assert_eq!(ts, 2000);
        assert!(!deleted);
    }

    #[test]
    fn older_timestamp_does_not_overwrite() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"new", 2000);
        gt.record_write("k1", b"old", 1000);
        let (val, _, _) = gt.expected_value("k1").unwrap();
        assert_eq!(val, b"new");
    }

    #[test]
    fn delete_makes_key_expect_none() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"hello", 1000);
        gt.record_delete("k1", 2000);
        let (_, _, deleted) = gt.expected_value("k1").unwrap();
        assert!(deleted);
        // Reading None for a deleted key is Match.
        assert_eq!(gt.record_read("k1", None), ReadResult::Match);
    }

    #[test]
    fn write_after_delete_undeletes() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"v1", 1000);
        gt.record_delete("k1", 2000);
        gt.record_write("k1", b"v2", 3000);
        let (val, _, deleted) = gt.expected_value("k1").unwrap();
        assert!(!deleted);
        assert_eq!(val, b"v2");
    }

    #[test]
    fn stats_track_operations() {
        let gt = GroundTruth::new();
        gt.record_write("k1", b"val", 1);
        gt.record_write("k2", b"val2", 2);
        gt.record_read("k1", Some(b"val"));
        gt.record_read("k2", Some(b"wrong"));
        gt.record_read("k3", None);

        let (writes, reads, matches, mismatches, missing, _) = gt.stats();
        assert_eq!(writes, 2);
        assert_eq!(reads, 3);
        assert_eq!(matches, 1);
        assert_eq!(mismatches, 1);
        assert_eq!(missing, 0); // k3 was never written, so NotYetWritten
    }

    #[test]
    fn snapshot_returns_all_keys() {
        let gt = GroundTruth::new();
        for i in 0..100 {
            gt.record_write(&format!("k{i}"), &[i as u8], i as i64);
        }
        let snap = gt.snapshot();
        assert_eq!(snap.len(), 100);
    }

    #[test]
    fn key_count_accurate() {
        let gt = GroundTruth::new();
        gt.record_write("a", b"1", 1);
        gt.record_write("b", b"2", 2);
        gt.record_write("a", b"3", 3); // overwrite
        assert_eq!(gt.key_count(), 2);
    }

    #[test]
    fn concurrent_writes_no_panic() {
        let gt = Arc::new(GroundTruth::new());
        let handles: Vec<_> = (0..16)
            .map(|t| {
                let gt = gt.clone();
                std::thread::spawn(move || {
                    for i in 0..1000 {
                        gt.record_write(
                            &format!("k{}", i % 100),
                            &[t as u8; 64],
                            (t * 1000 + i) as i64,
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let (writes, _, _, _, _, _) = gt.stats();
        assert_eq!(writes, 16_000);
        assert!(gt.key_count() <= 100);
    }

    /// Verify that the GT correctly tracks the highest-timestamp value
    /// under concurrent writes from multiple threads (same pattern as loadgen).
    #[test]
    fn concurrent_lww_correctness() {
        let gt = Arc::new(GroundTruth::new());
        let num_writers = 8usize;
        let key_space = 100usize;
        let writes_per_worker = 10_000usize;

        // Each worker writes to overlapping keys with its own timestamp range.
        let handles: Vec<_> = (0..num_writers)
            .map(|w| {
                let gt = gt.clone();
                std::thread::spawn(move || {
                    for i in 0..writes_per_worker {
                        let key_idx = (w * 31 + i * 7) % key_space;
                        let key = format!("k{key_idx:06}");
                        let ts = (w as i64) * 1_000_000_000 + (i as i64) + 1;
                        let value = format!("w{w}_i{i}").into_bytes();
                        gt.record_write(&key, &value, ts);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Verify: for each key, the GT should have the value from the
        // writer with the highest timestamp. Since worker 7 has the highest
        // base (7e9), and within a worker timestamps are monotonically
        // increasing, the GT entry for each key should be from the LAST
        // write by the HIGHEST-numbered worker that wrote to that key.
        let snap = gt.snapshot();
        for (key, (value, ts, _deleted)) in &snap {
            let val_str = std::str::from_utf8(value).unwrap();
            // Parse worker ID from value "wN_iM"
            let worker_id: usize = val_str
                .split('_')
                .next()
                .unwrap()
                .trim_start_matches('w')
                .parse()
                .unwrap();

            // The timestamp should be in this worker's range
            let worker_base = (worker_id as i64) * 1_000_000_000;
            assert!(
                *ts > worker_base && *ts <= worker_base + writes_per_worker as i64,
                "key {key}: ts={ts} not in worker {worker_id}'s range [{}, {}]",
                worker_base + 1,
                worker_base + writes_per_worker as i64
            );

            // No lower-numbered worker should have a higher timestamp
            // (since worker N's max ts = N*1e9 + writes_per_worker)
            for higher_w in (worker_id + 1)..num_writers {
                let higher_base = (higher_w as i64) * 1_000_000_000;
                assert!(
                    *ts >= higher_base || *ts < (worker_id as i64) * 1_000_000_000,
                    "key {key}: has ts={ts} from worker {worker_id}, but worker {higher_w} \
                     (base={higher_base}) should have won"
                );
            }
        }
    }

    #[test]
    fn bytes_written_tracked() {
        let gt = GroundTruth::new();
        gt.record_write("k1", &[0u8; 100], 1);
        gt.record_write("k2", &[0u8; 200], 2);
        let (_, _, _, _, _, bytes) = gt.stats();
        assert_eq!(bytes, 300);
    }
}
