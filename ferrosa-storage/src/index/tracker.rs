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
    /// When a generation of this index last finished building.
    pub last_progress: Option<Instant>,
    /// When the healer last resubmitted this index's pending generations.
    pub last_retry: Option<Instant>,
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
            last_progress: None,
            last_retry: None,
        }
    }

    /// Recompute the status based on current pending/indexed state.
    ///
    /// A failure is always about a pending generation, so with nothing
    /// pending the index is current: the failed generation was rebuilt, or
    /// retired by a compaction that carried its rows into a newer one. A
    /// pending generation keeps a recorded failure until it is resolved.
    fn recompute_status(&mut self) {
        if self.pending_sstables.is_empty() {
            self.oldest_pending_timestamp = None;
            self.pending_bytes = 0;
            if let IndexStatus::Failed { error, .. } = &self.status {
                tracing::warn!(
                    keyspace = %self.table.0,
                    table = %self.table.1,
                    index = %self.index_name,
                    last_error = %error,
                    "index backfill recovered: no generation is pending any more, the index \
                     is current and reads through it are served again"
                );
            }
            self.status = IndexStatus::Current;
        } else if matches!(self.status, IndexStatus::Failed { .. }) {
            // Still failed: the failed generation (or another) is pending.
        } else if let Some(oldest) = self.oldest_pending_timestamp {
            self.status = IndexStatus::Stale {
                lag: oldest.elapsed(),
                pending_count: self.pending_sstables.len() as u32,
            };
        }
    }
}

/// What [`IndexStateTracker::mark_failed`] made of a failed build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildFailure {
    /// The index was not failed before: the first failure of this outage.
    FirstFailure,
    /// The index was already failed; another attempt failed too.
    RepeatedFailure,
    /// The generation is no longer pending (a compaction retired it, or it
    /// was already built), so the failure does not leave the index short.
    NotPending,
}

/// One index whose pending generations the healer resubmits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueRetry {
    /// Keyspace of the index's table.
    pub keyspace: String,
    /// The index's table.
    pub table: String,
    /// The index.
    pub index_name: String,
    /// Its pending generations at the moment it was taken for retry.
    pub pending: Vec<String>,
    /// Why: the last build failed, or no generation has built for too long.
    pub failed: bool,
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
            state.last_progress = Some(Instant::now());
            state.recompute_status();
        }
    }

    /// Marks a failed build of generation `sstable_id` for the given index.
    ///
    /// Sets the status to `Failed` with a retry time, which the healer
    /// ([`Self::take_due_retries`]) acts on. A generation that is no longer
    /// pending is not a failure of the index: a build that lost a race with
    /// the compaction that retired its SSTable has nothing left to index, and
    /// recording it would hold the index failed with no work to retry
    /// (the 2026-10-04 restart outage). The caller logs every outcome.
    pub fn mark_failed(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
        sstable_id: &str,
        error: String,
        retry_delay: Duration,
    ) -> BuildFailure {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        let mut states = self.states.write();
        let Some(state) = states.get_mut(&key) else {
            return BuildFailure::NotPending;
        };
        if !state.pending_sstables.iter().any(|id| id == sstable_id) {
            return BuildFailure::NotPending;
        }
        state.total_build_errors += 1;
        let first = !matches!(state.status, IndexStatus::Failed { .. });
        state.status = IndexStatus::Failed {
            error,
            retry_at: Instant::now() + retry_delay,
        };
        if first {
            BuildFailure::FirstFailure
        } else {
            BuildFailure::RepeatedFailure
        }
    }

    /// Mark live generation `sstable_id` pending again, even if it was
    /// recorded as indexed: a rebuild found its sidecar cannot be (re)built,
    /// so the earlier record no longer vouches for it.
    pub fn reopen_pending(&self, keyspace: &str, table: &str, index_name: &str, sstable_id: &str) {
        let key = (
            keyspace.to_string(),
            table.to_string(),
            index_name.to_string(),
        );
        let mut states = self.states.write();
        if let Some(state) = states.get_mut(&key) {
            state.indexed_sstables.remove(sstable_id);
            if !state.pending_sstables.iter().any(|id| id == sstable_id) {
                state.pending_sstables.push_back(sstable_id.to_string());
                state
                    .oldest_pending_timestamp
                    .get_or_insert_with(Instant::now);
            }
            state.recompute_status();
        }
    }

    /// Account for generations that left the table's live set: `retired`
    /// were replaced by `replacement` (a compaction's output), or by nothing.
    ///
    /// For every index of the table, the retired generations stop being
    /// pending or indexed: no build will ever run for a file that is gone,
    /// so leaving them pending held the index "not current" forever. If any
    /// of them WAS pending, the replacement inherits that: it merged their
    /// sidecars, and theirs were missing, so its index is incomplete until a
    /// build of the replacement itself runs.
    pub fn retire_sstables(
        &self,
        keyspace: &str,
        table: &str,
        retired: &[String],
        replacement: Option<&str>,
    ) {
        if retired.is_empty() {
            return;
        }
        let mut states = self.states.write();
        for ((ks, tbl, _), state) in states.iter_mut() {
            if ks != keyspace || tbl != table {
                continue;
            }
            let pending_before = state.pending_sstables.len();
            state.pending_sstables.retain(|id| !retired.contains(id));
            let inherited = state.pending_sstables.len() != pending_before;
            for id in retired {
                state.indexed_sstables.remove(id);
            }
            if let Some(output) = replacement.filter(|_| inherited) {
                if !state.pending_sstables.iter().any(|id| id == output) {
                    state.pending_sstables.push_back(output.to_string());
                    state.indexed_sstables.remove(output);
                    state
                        .oldest_pending_timestamp
                        .get_or_insert_with(Instant::now);
                }
            }
            state.recompute_status();
        }
    }

    /// Take every index whose backfill is due for a retry, and stamp it as
    /// retried now.
    ///
    /// Due means pending generations remain and either the last build
    /// failed and its retry time has passed, or nothing has built (and
    /// nothing was retried) for `stall_after` — a build that was never
    /// queued, or whose job was lost, would otherwise wait forever. A taken
    /// index goes back from `Failed` to `Stale`, so a further failure is
    /// reported as a new attempt's.
    pub fn take_due_retries(&self, now: Instant, stall_after: Duration) -> Vec<DueRetry> {
        let mut states = self.states.write();
        let mut due = Vec::new();
        for ((keyspace, table, index_name), state) in states.iter_mut() {
            if state.pending_sstables.is_empty() {
                continue;
            }
            let failed = match state.status {
                IndexStatus::Failed { retry_at, .. } => {
                    if now < retry_at {
                        continue;
                    }
                    true
                }
                _ => {
                    let last_activity = [
                        state.oldest_pending_timestamp,
                        state.last_progress,
                        state.last_retry,
                    ]
                    .into_iter()
                    .flatten()
                    .max();
                    if last_activity.is_some_and(|at| now.duration_since(at) < stall_after) {
                        continue;
                    }
                    false
                }
            };
            state.last_retry = Some(now);
            if failed {
                state.status = IndexStatus::Stale {
                    lag: Duration::ZERO,
                    pending_count: state.pending_sstables.len() as u32,
                };
                state.recompute_status();
            }
            due.push(DueRetry {
                keyspace: keyspace.clone(),
                table: table.clone(),
                index_name: index_name.clone(),
                pending: state.pending_sstables.iter().cloned().collect(),
                failed,
            });
        }
        due
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

        tracker.mark_pending("ks", "tbl", "idx", "sst-2", 1);
        assert_eq!(
            tracker.mark_failed(
                "ks",
                "tbl",
                "idx",
                "sst-2",
                "boom".to_string(),
                Duration::from_secs(1),
            ),
            BuildFailure::FirstFailure
        );
        assert!(!tracker.is_current("ks", "tbl", "idx"));

        // The failed generation built on a retry: the failure is resolved.
        tracker.mark_indexed("ks", "tbl", "idx", "sst-2");
        assert!(tracker.is_current("ks", "tbl", "idx"));
    }

    /// A build that loses the race with the compaction retiring its SSTable
    /// fails to open the file. That is not a failure of the index, and must
    /// not hold it failed with nothing left to retry.
    #[test]
    fn a_failed_build_of_a_retired_generation_leaves_the_index_current() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "old-1", 1);
        tracker.mark_pending("ks", "tbl", "idx", "old-2", 1);

        tracker.retire_sstables(
            "ks",
            "tbl",
            &["old-1".to_string(), "old-2".to_string()],
            Some("merged"),
        );
        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert_eq!(
            state.pending_sstables,
            VecDeque::from(vec!["merged".to_string()]),
            "the output inherits its pending inputs' missing postings"
        );

        assert_eq!(
            tracker.mark_failed(
                "ks",
                "tbl",
                "idx",
                "old-1",
                "open data: No such file".to_string(),
                Duration::from_secs(60),
            ),
            BuildFailure::NotPending
        );
        tracker.mark_indexed("ks", "tbl", "idx", "merged");
        assert!(tracker.is_current("ks", "tbl", "idx"));
        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(
            !state.indexed_sstables.contains("old-1"),
            "retired generations leave the indexed set too"
        );
    }

    /// Retiring generations that were all indexed leaves the output to its
    /// merged sidecar: nothing becomes pending.
    #[test]
    fn retiring_indexed_generations_leaves_the_output_current() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "idx");
        tracker.register_index("ks", "other", "idx");
        tracker.mark_pending("ks", "tbl", "idx", "a", 1);
        tracker.mark_indexed("ks", "tbl", "idx", "a");
        tracker.mark_pending("ks", "other", "idx", "a", 1);

        tracker.retire_sstables("ks", "tbl", &["a".to_string()], Some("out"));

        assert!(tracker.is_current("ks", "tbl", "idx"));
        assert!(
            !tracker.is_current("ks", "other", "idx"),
            "another table's index with the same generation id is untouched"
        );
    }

    /// A failed index is retried once its retry time passes, and a pending
    /// one that made no progress for the stall bound is retried too.
    #[test]
    fn due_retries_cover_failed_and_stalled_backfills() {
        let tracker = IndexStateTracker::new();
        tracker.register_index("ks", "tbl", "failed_idx");
        tracker.register_index("ks", "tbl", "stalled_idx");
        tracker.register_index("ks", "tbl", "current_idx");
        tracker.mark_pending("ks", "tbl", "failed_idx", "g1", 1);
        tracker.mark_failed(
            "ks",
            "tbl",
            "failed_idx",
            "g1",
            "boom".to_string(),
            Duration::from_secs(60),
        );
        tracker.mark_pending("ks", "tbl", "stalled_idx", "g2", 1);
        let now = Instant::now();

        assert!(
            tracker
                .take_due_retries(now, Duration::from_secs(300))
                .is_empty(),
            "neither is due yet"
        );

        let later = now + Duration::from_secs(301);
        let mut due = tracker.take_due_retries(later, Duration::from_secs(300));
        due.sort_by(|a, b| a.index_name.cmp(&b.index_name));
        assert_eq!(due.len(), 2, "{due:?}");
        assert_eq!(
            (
                due[0].index_name.as_str(),
                due[0].failed,
                due[0].pending.clone()
            ),
            ("failed_idx", true, vec!["g1".to_string()])
        );
        assert_eq!(
            (due[1].index_name.as_str(), due[1].failed),
            ("stalled_idx", false)
        );
        assert!(
            tracker
                .take_due_retries(later, Duration::from_secs(300))
                .is_empty(),
            "a retried index is not retried again until the bound passes anew"
        );
        assert!(
            !matches!(
                tracker.get_state("ks", "tbl", "failed_idx").unwrap().status,
                IndexStatus::Failed { .. }
            ),
            "a retried index is no longer reported failed"
        );
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
            "sst-001",
            "disk full".to_string(),
            Duration::from_secs(60),
        );

        let state = tracker.get_state("ks", "tbl", "idx").unwrap();
        assert!(matches!(state.status, IndexStatus::Failed { .. }));
        assert_eq!(state.total_build_errors, 1);
    }
}
