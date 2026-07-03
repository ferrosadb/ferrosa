//! Per-index staleness tracking.
//!
//! [`IndexStateTracker`] maintains the build state for every registered
//! secondary index. It records which SSTables have been indexed, which are
//! pending, and derives a high-level [`IndexStatus`] for observability.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Composite key for an index: (keyspace, table, index_name).
type IndexKey = (String, String, String);

/// Status of a secondary index.
#[derive(Debug, Clone)]
pub enum IndexStatus {
    /// All known SSTables are indexed.
    Current,
    /// An index build is actively running.
    Building,
    /// The index has fallen behind: some SSTables are not yet indexed.
    Stale {
        /// How long the oldest pending SSTable has been waiting.
        lag: Duration,
        /// Number of SSTables awaiting indexing.
        pending_count: u32,
    },
    /// The last build attempt failed.
    Failed {
        /// Human-readable error description.
        error: String,
        /// When the next retry is scheduled.
        retry_at: Instant,
    },
}

/// Per-index build state.
#[derive(Debug, Clone)]
pub struct IndexState {
    /// Name of the index.
    pub index_name: String,
    /// (keyspace, table) the index belongs to.
    pub table: (String, String),
    /// Current status of this index.
    pub status: IndexStatus,
    /// SSTable IDs that have been successfully indexed.
    pub indexed_sstables: HashSet<String>,
    /// SSTable IDs awaiting indexing, in FIFO order.
    pub pending_sstables: VecDeque<String>,
    /// Total bytes of pending SSTables.
    pub pending_bytes: u64,
    /// Timestamp (from `Instant`) when the oldest pending SSTable was enqueued.
    pub oldest_pending_timestamp: Option<Instant>,
    /// Duration of the most recent successful build.
    pub last_build_duration: Option<Duration>,
    /// Total number of successful builds.
    pub total_builds: u64,
    /// Total number of failed build attempts.
    pub total_build_errors: u64,
}

impl IndexState {
    fn new(index_name: String, keyspace: String, table: String) -> Self {
        Self {
            index_name,
            table: (keyspace, table),
            status: IndexStatus::Current,
            indexed_sstables: HashSet::new(),
            pending_sstables: VecDeque::new(),
            pending_bytes: 0,
            oldest_pending_timestamp: None,
            last_build_duration: None,
            total_builds: 0,
            total_build_errors: 0,
        }
    }

    /// Recompute the status based on current pending/indexed state.
    fn recompute_status(&mut self) {
        if self.pending_sstables.is_empty() {
            // No work pending — check if we were in a failed state.
            if !matches!(self.status, IndexStatus::Failed { .. }) {
                self.status = IndexStatus::Current;
            }
        } else if let Some(oldest) = self.oldest_pending_timestamp {
            self.status = IndexStatus::Stale {
                lag: oldest.elapsed(),
                pending_count: self.pending_sstables.len() as u32,
            };
        }
    }
}

/// Thread-safe tracker for per-index build state.
///
/// Keyed by (keyspace, table, index_name). All methods acquire the internal
/// `RwLock` — callers should not hold references across await points.
pub struct IndexStateTracker {
    states: RwLock<HashMap<IndexKey, IndexState>>,
}

impl Default for IndexStateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexStateTracker {
    /// Creates an empty tracker with no registered indexes.
    pub fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a new index for tracking.
    ///
    /// If the index is already registered, this is a no-op.
    pub fn register_index(&self, keyspace: &str, table: &str, index_name: &str) {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        let mut states = self.states.write();
        states.entry(key).or_insert_with(|| {
            IndexState::new(
                index_name.to_string(),
                keyspace.to_string(),
                table.to_string(),
            )
        });
    }

    /// Removes every tracked index on `(keyspace, table)` — the DROP TABLE
    /// cascade counterpart of per-index [`remove_index`](Self::remove_index).
    ///
    /// Returns the number of entries removed.
    pub fn remove_table_indexes(&self, keyspace: &str, table: &str) -> usize {
        let mut states = self.states.write();
        let before = states.len();
        states.retain(|(ks, tbl, _), _| !(ks == keyspace && tbl == table));
        before - states.len()
    }

    /// Removes an index from tracking.
    ///
    /// Returns `true` if the index was present and removed.
    pub fn remove_index(&self, keyspace: &str, table: &str, index_name: &str) -> bool {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        self.states.write().remove(&key).is_some()
    }

    /// Marks an SSTable as pending indexing for the given index.
    ///
    /// Records the SSTable ID and byte count. Updates the status to `Stale`.
    pub fn mark_pending(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
        sstable_id: &str,
        bytes: u64,
    ) {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(&key) {
            // Don't add duplicates.
            if !state.pending_sstables.contains(&sstable_id.to_string())
                && !state.indexed_sstables.contains(sstable_id)
            {
                state.pending_sstables.push_back(sstable_id.to_string());
                state.pending_bytes += bytes;
                if state.oldest_pending_timestamp.is_none() {
                    state.oldest_pending_timestamp = Some(Instant::now());
                }
                state.recompute_status();
            }
        }
    }

    /// Marks an SSTable as successfully indexed.
    ///
    /// Moves the SSTable from pending to indexed and recomputes the status.
    pub fn mark_indexed(&self, keyspace: &str, table: &str, index_name: &str, sstable_id: &str) {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(&key) {
            // Remove from pending queue.
            state.pending_sstables.retain(|id| id != sstable_id);

            state.indexed_sstables.insert(sstable_id.to_string());
            state.total_builds += 1;

            // Reset oldest timestamp if no more pending.
            if state.pending_sstables.is_empty() {
                state.oldest_pending_timestamp = None;
                state.pending_bytes = 0;
            }

            state.recompute_status();
        }
    }

    /// Marks a build failure for the given index.
    ///
    /// Sets the status to `Failed` with a retry time.
    pub fn mark_failed(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
        error: String,
        retry_delay: Duration,
    ) {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(&key) {
            state.total_build_errors += 1;
            state.status = IndexStatus::Failed {
                error,
                retry_at: Instant::now() + retry_delay,
            };
        }
    }

    /// Returns a clone of the current state for a given index, if registered.
    pub fn get_state(&self, keyspace: &str, table: &str, index_name: &str) -> Option<IndexState> {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        self.states.read().get(&key).cloned()
    }

    /// Returns true when the index is registered and has no known pending or
    /// failed build work.
    pub fn is_current(&self, keyspace: &str, table: &str, index_name: &str) -> bool {
        self.get_state(keyspace, table, index_name)
            .is_some_and(|state| {
                matches!(state.status, IndexStatus::Current) && state.pending_sstables.is_empty()
            })
    }

    /// Returns the indexed and unindexed SSTable sets for a given index.
    ///
    /// Returns `(indexed, unindexed)` where unindexed is the set of pending
    /// SSTable IDs.
    pub fn get_coverage(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
    ) -> (HashSet<String>, HashSet<String>) {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        let states = self.states.read();
        match states.get(&key) {
            Some(state) => {
                let unindexed: HashSet<String> = state.pending_sstables.iter().cloned().collect();
                (state.indexed_sstables.clone(), unindexed)
            }
            None => (HashSet::new(), HashSet::new()),
        }
    }

    /// Returns a snapshot of all tracked index states.
    pub fn all_states(&self) -> Vec<IndexState> {
        self.states.read().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_starts_empty() {
        let tracker = IndexStateTracker::new();
        assert!(tracker.all_states().is_empty());
        assert!(tracker.get_state("ks", "tbl", "idx").is_none());
    }

    /// DROP TABLE cascade: `remove_table_indexes` sweeps every entry keyed on
    /// the dropped `(keyspace, table)` and only those (forge t_ae06e925).
    #[test]
    fn remove_table_indexes_sweeps_only_that_table() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx_a");
        tracker.register_index("ks", "tbl", "idx_b");
        tracker.register_index("ks", "other", "idx_c");
        tracker.register_index("ks2", "tbl", "idx_d");

        assert_eq!(tracker.remove_table_indexes("ks", "tbl"), 2);

        assert!(tracker.get_state("ks", "tbl", "idx_a").is_none());
        assert!(tracker.get_state("ks", "tbl", "idx_b").is_none());
        assert!(tracker.get_state("ks", "other", "idx_c").is_some());
        assert!(tracker.get_state("ks2", "tbl", "idx_d").is_some());

        // Idempotent: nothing left to remove.
        assert_eq!(tracker.remove_table_indexes("ks", "tbl"), 0);
    }

    #[test]
    fn tracker_register_and_mark_pending() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx_name");

        // After registration, status should be Current.
        let state = tracker.get_state("ks", "tbl", "idx_name").unwrap();
        assert!(matches!(state.status, IndexStatus::Current));

        // Mark an SSTable as pending.
        tracker.mark_pending("ks", "tbl", "idx_name", "sst-001", 1024);

        let state = tracker.get_state("ks", "tbl", "idx_name").unwrap();
        assert!(
            matches!(
                state.status,
                IndexStatus::Stale {
                    pending_count: 1,
                    ..
                }
            ),
            "expected Stale with pending_count=1, got {:?}",
            state.status
        );
        assert_eq!(state.pending_sstables.len(), 1);
        assert_eq!(state.pending_bytes, 1024);
        assert!(state.oldest_pending_timestamp.is_some());

        // Mark a second SSTable as pending.
        tracker.mark_pending("ks", "tbl", "idx_name", "sst-002", 2048);
        let state = tracker.get_state("ks", "tbl", "idx_name").unwrap();
        assert!(
            matches!(
                state.status,
                IndexStatus::Stale {
                    pending_count: 2,
                    ..
                }
            ),
            "expected Stale with pending_count=2, got {:?}",
            state.status
        );
        assert_eq!(state.pending_bytes, 3072);
    }

    #[test]
    fn tracker_mark_indexed_transitions_to_current() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");

        // Add pending SSTables.
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 500);
        tracker.mark_pending("ks", "tbl", "idx", "sst-002", 700);

        // Index the first one — should still be Stale.
        tracker.mark_indexed("ks", "tbl", "idx", "sst-001");
        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(
            matches!(
                state.status,
                IndexStatus::Stale {
                    pending_count: 1,
                    ..
                }
            ),
            "expected Stale with 1 pending, got {:?}",
            state.status
        );
        assert_eq!(state.total_builds, 1);
        assert!(state.indexed_sstables.contains("sst-001"));

        // Index the second one — should transition to Current.
        tracker.mark_indexed("ks", "tbl", "idx", "sst-002");
        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(
            matches!(state.status, IndexStatus::Current),
            "expected Current, got {:?}",
            state.status
        );
        assert_eq!(state.total_builds, 2);
        assert!(state.indexed_sstables.contains("sst-002"));
        assert!(state.pending_sstables.is_empty());
    }

    #[test]
    fn tracker_remove_index() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        assert_eq!(tracker.all_states().len(), 1);
        assert!(tracker.is_current("ks", "tbl", "idx"));

        let removed = tracker.remove_index("ks", "tbl", "idx");
        assert!(removed);
        assert!(tracker.all_states().is_empty());
        assert!(tracker.get_state("ks", "tbl", "idx").is_none());
        assert!(!tracker.is_current("ks", "tbl", "idx"));

        // Removing again returns false.
        let removed = tracker.remove_index("ks", "tbl", "idx");
        assert!(!removed);
    }

    #[test]
    fn is_current_tracks_pending_and_failed_work() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        assert!(tracker.is_current("ks", "tbl", "idx"));

        tracker.mark_pending("ks", "tbl", "idx", "sst-1", 1);
        assert!(!tracker.is_current("ks", "tbl", "idx"));

        tracker.mark_indexed("ks", "tbl", "idx", "sst-1");
        assert!(tracker.is_current("ks", "tbl", "idx"));

        tracker.mark_failed(
            "ks",
            "tbl",
            "idx",
            "boom".to_string(),
            Duration::from_secs(1),
        );
        assert!(!tracker.is_current("ks", "tbl", "idx"));
    }

    #[test]
    fn tracker_get_coverage() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");

        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100);
        tracker.mark_pending("ks", "tbl", "idx", "sst-002", 200);
        tracker.mark_indexed("ks", "tbl", "idx", "sst-001");

        let (indexed, unindexed) = tracker.get_coverage("ks", "tbl", "idx");
        assert!(indexed.contains("sst-001"));
        assert!(!indexed.contains("sst-002"));
        assert!(unindexed.contains("sst-002"));
        assert!(!unindexed.contains("sst-001"));
    }

    #[test]
    fn tracker_mark_pending_deduplicates() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");

        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100);
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100); // duplicate

        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert_eq!(state.pending_sstables.len(), 1);
        assert_eq!(state.pending_bytes, 100); // not doubled
    }

    #[test]
    fn tracker_mark_failed() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "sst-001", 100);

        tracker.mark_failed(
            "ks",
            "tbl",
            "idx",
            "disk full".to_string(),
            Duration::from_secs(60),
        );

        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(matches!(state.status, IndexStatus::Failed { .. }));
        assert_eq!(state.total_build_errors, 1);
    }
}
