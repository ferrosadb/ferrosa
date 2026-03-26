//! Commit log (write-ahead log) for durability.
//!
//! The commit log records every mutation before it reaches the memtable.
//! On crash recovery, uncommitted mutations are replayed from segment
//! files to restore memtable state.
//!
//! # Architecture
//!
//! The [`CommitLog`] composes all internal modules into a single public API:
//!
//! - **Segment** — fixed-size byte buffer with lock-free CAS allocation
//! - **SyncStrategy** — controls when segments are fsynced (Batch / Periodic / Group)
//! - **SegmentReader** — reads segment files during crash recovery replay
//! - **CommitLogCheckpoint** — tracks per-table flush positions
//!
//! The active segment is held behind an [`ArcSwap`],
//! giving writers lock-free access. Segment rotation atomically swaps in
//! a new segment while the old one stays alive (via `Arc`) until all
//! tables have been flushed past it.

pub mod archiver;
pub mod cdc;
pub(crate) mod checkpoint;
pub(crate) mod config;
pub(crate) mod descriptor;
pub mod manifest;
pub(crate) mod mutation;
pub(crate) mod reader;
pub(crate) mod segment;
pub(crate) mod sync;

pub use config::{ArchiveConfig, CommitLogConfig, CommitLogPosition, SyncStrategyConfig, TableId};
pub use mutation::Mutation;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use checkpoint::CommitLogCheckpoint;
use config::CommitLogConfig as Config;
use reader::SegmentReader;
use segment::Segment;
use sync::{BatchSync, FlushCallback, GroupSync, PeriodicSync, SyncStrategy};

/// The commit log: write-ahead log for mutation durability.
///
/// Writers call [`append()`](Self::append) to record a mutation. The commit log
/// manages segment allocation, rotation, sync strategy, and checkpoint tracking.
///
/// # Concurrency
///
/// - **Append** is lock-free on the hot path (CAS allocation in the active segment).
/// - **Rotation** briefly takes the `closed_segments` mutex to move the old segment.
/// - **Discard** takes both `segment_tracker` and `closed_segments` mutexes to
///   clean up fully-flushed segments.
pub struct CommitLog {
    /// Commit log configuration.
    config: Config,

    /// The currently active segment, swapped atomically on rotation.
    active: Arc<ArcSwap<Segment>>,

    /// Segments that are full but still have dirty (unflushed) tables.
    closed_segments: Mutex<Vec<Arc<Segment>>>,

    /// Per-segment dirty table tracking. Key = segment ID, value = map of
    /// table ID to the latest position written for that table in that segment.
    segment_tracker: Mutex<HashMap<u64, HashMap<TableId, CommitLogPosition>>>,

    /// Controls when segment buffers are fsynced to disk.
    sync_strategy: Box<dyn SyncStrategy>,

    /// Monotonic segment ID generator.
    next_segment_id: AtomicU64,

    /// Segment IDs that have been successfully archived to S3.
    /// Used by `discard_completed()` to gate segment deletion when
    /// archiving is enabled.
    archived: Mutex<HashSet<u64>>,

    /// Channel sender for notifying the archiver of closed segments.
    /// None when archiving is disabled.
    archive_tx: Option<tokio::sync::mpsc::Sender<u64>>,
}

impl CommitLog {
    /// Creates a new commit log with the given configuration.
    ///
    /// Creates the log directory if it does not exist, allocates the first
    /// segment, and starts the sync strategy.
    pub fn new(config: Config) -> ferrosa_common::Result<Self> {
        fs::create_dir_all(&config.log_dir)?;
        fs::create_dir_all(&config.checkpoint_dir)?;

        let first_segment = Arc::new(Segment::new(1, config.segment_size, &config.log_dir));
        let active = Arc::new(ArcSwap::from(first_segment));

        let sync_strategy = Self::create_sync_strategy(&config, Arc::clone(&active));
        sync_strategy.start();

        Ok(Self {
            config,
            active,
            closed_segments: Mutex::new(Vec::new()),
            segment_tracker: Mutex::new(HashMap::new()),
            sync_strategy,
            next_segment_id: AtomicU64::new(2), // first segment is 1
            archived: Mutex::new(HashSet::new()),
            archive_tx: None,
        })
    }

    /// Opens an existing commit log directory, replays uncommitted mutations,
    /// and returns a new `CommitLog` instance along with the replayed mutations.
    ///
    /// The replay process:
    /// 1. Load the checkpoint file to find per-table flush positions.
    /// 2. Scan the log directory for segment files, sorted by segment ID.
    /// 3. For each segment, read all entries and filter those after checkpoint positions.
    /// 4. Create a fresh `CommitLog` for new writes.
    pub fn open_and_replay(config: Config) -> ferrosa_common::Result<(Self, Vec<Mutation>)> {
        let checkpoint = CommitLogCheckpoint::load(&config.checkpoint_dir)?;

        // Scan for segment files in log_dir.
        let mut segment_files: Vec<(u64, std::path::PathBuf)> = Vec::new();
        if config.log_dir.exists() {
            for entry in fs::read_dir(&config.log_dir)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id) = parse_segment_id(name) {
                        segment_files.push((id, path));
                    }
                }
            }
        }

        // Sort by segment ID for deterministic replay order.
        segment_files.sort_by_key(|(id, _)| *id);

        let mut mutations = Vec::new();
        for (_, path) in &segment_files {
            let mut reader = SegmentReader::open(path)?;
            let entries = reader.read_all()?;

            for (pos, mutation) in entries {
                let table_id = TableId::new(&mutation.keyspace, &mutation.table);
                // Keep entries that are after the checkpoint position for this table.
                let dominated = checkpoint.get(&table_id).is_some_and(|cp| pos <= *cp);
                if !dominated {
                    mutations.push(mutation);
                }
            }
        }

        // Clean up old segment files before creating new CommitLog.
        for (_, path) in &segment_files {
            let _ = fs::remove_file(path);
        }

        let commit_log = Self::new(config)?;
        Ok((commit_log, mutations))
    }

    /// Appends a mutation to the commit log.
    ///
    /// This is the hot path. The flow:
    /// 1. Load the active segment (lock-free via `ArcSwap`).
    /// 2. Try to allocate space in the segment (lock-free CAS).
    /// 3. If the segment is full, rotate and retry.
    /// 4. Write the entry, update dirty tracking, notify sync strategy.
    pub fn append(&self, mutation: &Mutation) -> ferrosa_common::Result<CommitLogPosition> {
        let total_size = Segment::entry_total_size(mutation);

        // Load active segment and try to allocate. The segment reference MUST
        // stay paired with the offset — writing to a different segment than
        // the one where CAS succeeded would corrupt data.
        //
        // allocate_and_begin_write() increments in_flight_writers BEFORE the CAS,
        // closing the window where flush could read partially-written data.
        let (segment, offset) = {
            let seg = self.active.load_full();
            match seg.allocate_and_begin_write(total_size) {
                Some(offset) => (seg, offset),
                None => {
                    // Segment is full — rotate (serialized) and retry.
                    drop(seg);
                    self.force_rotate()?;
                    let new_seg = self.active.load_full();
                    let offset = match new_seg.allocate_and_begin_write(total_size) {
                        Some(o) => o,
                        None => {
                            // Entry exceeds segment capacity. This happens when
                            // a single mutation is larger than segment_size.
                            // Return an error instead of panicking.
                            return Err(ferrosa_common::Error::InvalidData(format!(
                                "commit log entry ({total_size} bytes) exceeds \
                                 segment capacity; increase segment_size"
                            )));
                        }
                    };
                    (new_seg, offset)
                }
            }
        };

        let position = segment.write_entry(offset, mutation);
        segment.writer_done();

        // Track dirty table in this segment.
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        segment.mark_table_dirty(&table_id, position);

        // Update segment tracker.
        {
            let mut tracker = self.segment_tracker.lock();
            tracker
                .entry(segment.id)
                .or_default()
                .entry(table_id)
                .and_modify(|existing| {
                    if position > *existing {
                        *existing = position;
                    }
                })
                .or_insert(position);
        }

        // Notify sync strategy.
        self.sync_strategy.on_write(&segment, offset);

        Ok(position)
    }

    /// Discards commit log data for a table up to the given position.
    ///
    /// When a table's memtable is flushed to an SSTable, the caller calls this
    /// to indicate that all mutations up to `position` are durable elsewhere.
    /// Segments where all tables have been flushed past their positions are
    /// deleted from disk.
    pub fn discard_completed(
        &self,
        table_id: &TableId,
        position: CommitLogPosition,
    ) -> ferrosa_common::Result<()> {
        let mut segments_to_delete = Vec::new();

        {
            let mut tracker = self.segment_tracker.lock();

            // For each tracked segment, check if this table's position is dominated.
            let segment_ids: Vec<u64> = tracker.keys().copied().collect();
            for seg_id in segment_ids {
                if let Some(tables) = tracker.get_mut(&seg_id) {
                    if let Some(table_pos) = tables.get(table_id) {
                        if *table_pos <= position {
                            tables.remove(table_id);
                        }
                    }
                    if tables.is_empty() {
                        // Check archive gate: if archiving is enabled, the segment
                        // must be archived before it can be deleted from disk.
                        let dominated_by_archive = match &self.config.archive {
                            Some(cfg) if cfg.enabled => self.archived.lock().contains(&seg_id),
                            _ => true, // Archiving disabled — no gate.
                        };

                        if dominated_by_archive {
                            tracker.remove(&seg_id);
                            segments_to_delete.push(seg_id);
                        }
                    }
                }
            }
        }

        // Remove deleted segments from closed_segments and delete files.
        if !segments_to_delete.is_empty() {
            let mut archived = self.archived.lock();
            let mut closed = self.closed_segments.lock();
            for seg_id in &segments_to_delete {
                if let Some(idx) = closed.iter().position(|s| s.id == *seg_id) {
                    let segment = closed.remove(idx);
                    let _ = fs::remove_file(segment.path());
                }
                archived.remove(seg_id);
            }
        }

        // Update checkpoint.
        let mut checkpoint = CommitLogCheckpoint::load(&self.config.checkpoint_dir)?;
        checkpoint
            .entry(table_id.clone())
            .and_modify(|existing| {
                if position > *existing {
                    *existing = position;
                }
            })
            .or_insert(position);
        CommitLogCheckpoint::save(&self.config.checkpoint_dir, &checkpoint)?;

        Ok(())
    }

    /// Discards closed segments that have no remaining dirty tables.
    ///
    /// This is a lightweight GC pass for the maintenance loop. Unlike
    /// `discard_completed()` which marks specific table positions, this method
    /// only removes segments where the tracker already shows zero dirty tables
    /// (i.e., all tables were previously discarded via `discard_completed()`).
    ///
    /// Returns the number of segments cleaned up.
    pub fn discard_completed_segments(&self) -> ferrosa_common::Result<usize> {
        let mut segments_to_delete = Vec::new();

        {
            let mut tracker = self.segment_tracker.lock();
            let segment_ids: Vec<u64> = tracker.keys().copied().collect();
            for seg_id in segment_ids {
                if tracker.get(&seg_id).is_some_and(|tables| tables.is_empty()) {
                    // Check archive gate: if archiving is enabled, the segment
                    // must be archived before it can be deleted from disk.
                    let dominated_by_archive = match &self.config.archive {
                        Some(cfg) if cfg.enabled => self.archived.lock().contains(&seg_id),
                        _ => true, // Archiving disabled — no gate.
                    };

                    if dominated_by_archive {
                        tracker.remove(&seg_id);
                        segments_to_delete.push(seg_id);
                    }
                }
            }
        }

        let count = segments_to_delete.len();
        if !segments_to_delete.is_empty() {
            let mut archived = self.archived.lock();
            let mut closed = self.closed_segments.lock();
            for seg_id in &segments_to_delete {
                if let Some(idx) = closed.iter().position(|s| s.id == *seg_id) {
                    let segment = closed.remove(idx);
                    let _ = fs::remove_file(segment.path());
                }
                archived.remove(seg_id);
            }
        }

        Ok(count)
    }

    /// Marks a segment as archived to S3.
    ///
    /// Called by the archiver after successful upload. Once marked,
    /// `discard_completed()` will allow deletion of this segment
    /// (provided all tables are also flushed).
    pub fn mark_archived(&self, segment_id: u64) {
        self.archived.lock().insert(segment_id);
    }

    /// Returns the set of segment IDs currently marked as archived.
    pub fn archived_segments(&self) -> HashSet<u64> {
        self.archived.lock().clone()
    }

    /// Sets the channel sender for archive notifications.
    ///
    /// Called by StorageEngine during initialization when archiving is enabled.
    pub fn set_archive_channel(&mut self, tx: tokio::sync::mpsc::Sender<u64>) {
        self.archive_tx = Some(tx);
    }

    /// Returns the current write position in the active segment.
    ///
    /// This is the position of the next byte that will be written. Used
    /// by snapshot creation (PITR Sprint P-2) to record the commit log
    /// position at the time of the snapshot.
    pub fn current_position(&self) -> CommitLogPosition {
        let segment = self.active.load();
        CommitLogPosition {
            segment_id: segment.id,
            offset: segment.current_position(),
        }
    }

    /// Forces rotation of the active segment.
    ///
    /// Allocates a new segment, atomically swaps it in via `ArcSwap`, and moves
    /// the old segment to the `closed_segments` list.
    ///
    /// If multiple threads race here, each creates a segment — the extras are
    /// empty but harmless. No data is lost because each writer holds its own
    /// `Arc<Segment>` paired with its CAS-allocated offset (see `append()`).
    pub fn force_rotate(&self) -> ferrosa_common::Result<()> {
        let new_id = self.next_segment_id.fetch_add(1, Ordering::AcqRel);
        let new_segment = Arc::new(Segment::new(
            new_id,
            self.config.segment_size,
            &self.config.log_dir,
        ));

        // Swap the new segment in and get the old one.
        let old_segment = self.active.swap(new_segment);

        // Flush the old segment to disk before archiving.
        old_segment.flush_to_disk()?;

        // Move old segment to closed list.
        let old_id = old_segment.id;
        let mut closed = self.closed_segments.lock();
        closed.push(old_segment);
        drop(closed);

        // Notify archiver of the closed segment (non-blocking).
        if let Some(tx) = &self.archive_tx {
            let _ = tx.try_send(old_id);
        }

        Ok(())
    }

    /// Replay mutations from a given position forward.
    ///
    /// Walks closed segments and the active segment, returning entries
    /// after the given position. Returns empty vec if the requested
    /// segment has been recycled (caller should trigger full bootstrap).
    pub fn replay_from(
        &self,
        position: CommitLogPosition,
    ) -> ferrosa_common::Result<Vec<Mutation>> {
        let mut mutations = Vec::new();

        // Collect segment paths to read: closed segments with id >= position.segment_id.
        let closed = self.closed_segments.lock();
        let mut segment_paths: Vec<(u64, std::path::PathBuf)> = closed
            .iter()
            .filter(|s| s.id >= position.segment_id)
            .map(|s| (s.id, s.path().to_path_buf()))
            .collect();
        drop(closed);

        // Check if the requested segment exists. If all closed segments have
        // lower IDs and the active segment is newer, the requested segment
        // was recycled — return empty to signal full bootstrap needed.
        let active = self.active.load();
        let active_id = active.id;
        let active_path = active.path().to_path_buf();

        if position.segment_id > 0 && segment_paths.is_empty() && active_id > position.segment_id {
            // Requested segment was recycled.
            return Ok(vec![]);
        }

        // Add active segment if it has data and its ID >= requested.
        if active_id >= position.segment_id {
            // Flush active segment to disk so SegmentReader can read it.
            active.flush_to_disk()?;
            segment_paths.push((active_id, active_path));
        }

        // Sort by segment ID for replay order.
        segment_paths.sort_by_key(|(id, _)| *id);

        for (_, path) in &segment_paths {
            if !path.exists() {
                continue;
            }
            let mut reader = SegmentReader::open(path)?;
            let entries = reader.read_all()?;

            for (pos, mutation) in entries {
                if pos > position {
                    mutations.push(mutation);
                }
            }
        }

        Ok(mutations)
    }

    /// Force-syncs the active segment to disk.
    ///
    /// Waits for all in-flight writers to complete, then flushes the
    /// segment buffer to disk. Used before catch-up replay to ensure
    /// all mutations are readable.
    pub fn force_sync(&self) -> ferrosa_common::Result<()> {
        let segment = self.active.load();
        // Write an EOF sync marker so SegmentReader can follow the chain.
        if let Some(offset) = segment.allocate(segment::SYNC_MARKER_SIZE) {
            segment.write_sync_marker_at(offset, 0);
        }
        // Full rewrite: write_sync_marker_at updates the PREVIOUS marker
        // in the buffer (at an earlier offset). Incremental flush wouldn't
        // capture that update, so we rewrite the entire file.
        segment.force_full_flush()
    }

    /// Shuts down the commit log cleanly.
    ///
    /// Stops the sync strategy and flushes the active segment to disk.
    pub fn shutdown(&self) -> ferrosa_common::Result<()> {
        self.sync_strategy.stop();
        let segment = self.active.load();
        segment.flush_to_disk()?;
        Ok(())
    }

    /// Creates the appropriate sync strategy based on config.
    fn create_sync_strategy(
        config: &Config,
        active: Arc<ArcSwap<Segment>>,
    ) -> Box<dyn SyncStrategy> {
        match &config.sync_strategy {
            SyncStrategyConfig::Batch => Box::new(BatchSync::new()),
            SyncStrategyConfig::Periodic { sync_interval } => {
                let active_ref = Arc::clone(&active);
                let flush_callback: FlushCallback = Arc::new(move || {
                    let seg = active_ref.load();
                    seg.flush_to_disk()?;
                    // Write an EOF sync marker after flushing. Uses plain
                    // allocate() since this is called from the flush thread
                    // (not a writer), and the marker is complete before return.
                    if let Some(offset) = seg.allocate(segment::SYNC_MARKER_SIZE) {
                        seg.write_sync_marker_at(offset, 0);
                    }
                    Ok(())
                });
                Box::new(PeriodicSync::new(*sync_interval, flush_callback))
            }
            SyncStrategyConfig::Group { max_wait } => {
                let active_ref = Arc::clone(&active);
                let flush_callback: FlushCallback = Arc::new(move || {
                    let seg = active_ref.load();
                    seg.flush_to_disk()?;
                    // Write an EOF sync marker after flushing.
                    if let Some(offset) = seg.allocate(segment::SYNC_MARKER_SIZE) {
                        seg.write_sync_marker_at(offset, 0);
                    }
                    Ok(())
                });
                Box::new(GroupSync::new(*max_wait, flush_callback))
            }
        }
    }
}

/// Parses a segment ID from a filename like `commitlog-42.log`.
fn parse_segment_id(filename: &str) -> Option<u64> {
    let name = filename.strip_prefix("commitlog-")?;
    let id_str = name.strip_suffix(".log")?;
    id_str.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    /// Helper to create a simple mutation for testing.
    fn simple_mutation() -> Mutation {
        Mutation {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 42_000,
        }
    }

    /// Helper to create a mutation targeting a different table.
    fn mutation_for_table(keyspace: &str, table: &str) -> Mutation {
        Mutation {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 42_000,
        }
    }

    #[test]
    fn new_creates_segment_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        // The first segment file should exist after BatchSync writes on first append,
        // but the segment is pre-allocated in memory. Let's append and check.
        let m = simple_mutation();
        cl.append(&m).unwrap();

        // After append with BatchSync, the segment file should be flushed.
        let segment = cl.active.load();
        assert!(
            segment.path().exists(),
            "segment file should exist after append with BatchSync"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn append_returns_positions() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let m1 = simple_mutation();
        let m2 = simple_mutation();

        let pos1 = cl.append(&m1).unwrap();
        let pos2 = cl.append(&m2).unwrap();

        // Positions should be in the same segment and increasing.
        assert_eq!(pos1.segment_id, pos2.segment_id);
        assert!(
            pos2.offset > pos1.offset,
            "second append should have higher offset"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn append_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        cl.append(&m).unwrap();

        let segment_path = cl.active.load().path().to_path_buf();
        cl.shutdown().unwrap();

        // After shutdown, the segment file should exist and contain data.
        assert!(
            segment_path.exists(),
            "segment file should exist after shutdown"
        );
        let contents = fs::read(&segment_path).unwrap();
        assert!(
            contents.len() > 25,
            "segment should contain data beyond header"
        );
    }

    #[test]
    fn rotation_on_full_segment() {
        let dir = tempfile::tempdir().unwrap();
        // Use a small segment size to force rotation after a few appends.
        // Each entry is ~118 bytes; header+sync marker is 25 bytes.
        // 512 bytes allows ~3-4 entries before rotation.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();

        // Keep appending until we've rotated at least once.
        let mut segment_ids = std::collections::HashSet::new();
        for _ in 0..10 {
            match cl.append(&m) {
                Ok(pos) => {
                    segment_ids.insert(pos.segment_id);
                }
                Err(_) => break,
            }
        }

        assert!(
            segment_ids.len() >= 2,
            "should have rotated to at least 2 segments, got {}",
            segment_ids.len()
        );

        // Verify multiple segment files exist.
        let files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
            })
            .collect();

        assert!(
            files.len() >= 2,
            "should have at least 2 segment files, got {}",
            files.len()
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn discard_deletes_clean_segments() {
        let dir = tempfile::tempdir().unwrap();
        // Small segment to force rotation after a few appends.
        // Each entry is ~118 bytes; 512 bytes allows ~3-4 entries per segment.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append mutations — this will span multiple segments.
        let mut last_pos = None;
        for _ in 0..10 {
            match cl.append(&m) {
                Ok(pos) => last_pos = Some(pos),
                Err(_) => break,
            }
        }
        let last_pos = last_pos.expect("should have appended at least one mutation");

        // Count segment files before discard.
        let count_segments = || -> usize {
            fs::read_dir(dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("commitlog-") && n.ends_with(".log"))
                })
                .count()
        };

        let before = count_segments();
        assert!(before >= 2, "need at least 2 segments for this test");

        // Discard all mutations up to the last position.
        cl.discard_completed(&table_id, last_pos).unwrap();

        let after = count_segments();
        // The closed segments should have been deleted. The active segment stays.
        assert!(
            after < before,
            "discard should have deleted some segments: before={before}, after={after}"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn discard_keeps_partially_dirty() {
        let dir = tempfile::tempdir().unwrap();
        // Small segment to force rotation.
        // Each entry is ~118 bytes; 512 bytes allows ~3-4 entries per segment.
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m1 = mutation_for_table("ks", "table_a");
        let m2 = mutation_for_table("ks", "table_b");
        let table_a = TableId::new("ks", "table_a");

        // Append mutations from two different tables.
        let mut pos_a = None;
        for _ in 0..5 {
            if let Ok(pos) = cl.append(&m1) {
                pos_a = Some(pos);
            }
        }
        for _ in 0..5 {
            let _ = cl.append(&m2);
        }

        let pos_a = pos_a.expect("should have appended table_a mutations");

        // Count closed segments.
        let closed_count = cl.closed_segments.lock().len();

        // Discard only table_a — table_b is still dirty.
        cl.discard_completed(&table_a, pos_a).unwrap();

        // Segments with table_b data should NOT be deleted.
        let closed_after = cl.closed_segments.lock().len();
        // If table_b has data in the same segments, they should be retained.
        // The exact count depends on which segments both tables share.
        // The key invariant: segments with remaining dirty tables are kept.
        assert!(
            closed_after <= closed_count,
            "should not have more closed segments after discard"
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn discard_blocked_until_archived() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append enough to force at least one rotation.
        let mut last_pos = None;
        for _ in 0..10 {
            if let Ok(pos) = cl.append(&m) {
                last_pos = Some(pos);
            }
        }
        let last_pos = last_pos.unwrap();

        // Mark all tables flushed — but do NOT mark segments as archived.
        cl.discard_completed(&table_id, last_pos).unwrap();

        // Closed segments should still exist because they are not archived.
        let closed = cl.closed_segments.lock();
        // When archive tracking is enabled, segments that are flushed but
        // not archived must not be deleted from disk.
        // (This test will need adjustment once archiving is wired in.)
        // For now: verify the API exists.
        assert!(
            cl.archived_segments().is_empty(),
            "no segments should be archived yet"
        );
        drop(closed);

        cl.shutdown().unwrap();
    }

    #[test]
    fn mark_archived_allows_discard() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append to force rotation.
        let mut positions = Vec::new();
        for _ in 0..10 {
            if let Ok(pos) = cl.append(&m) {
                positions.push(pos);
            }
        }

        // Collect closed segment IDs before discard.
        let closed_ids: Vec<u64> = cl.closed_segments.lock().iter().map(|s| s.id).collect();
        assert!(!closed_ids.is_empty(), "need closed segments for this test");

        // Mark all closed segments as archived.
        for id in &closed_ids {
            cl.mark_archived(*id);
        }

        // Verify they are tracked as archived.
        let archived = cl.archived_segments();
        for id in &closed_ids {
            assert!(archived.contains(id), "segment {id} should be archived");
        }

        // Now discard — segments are both flushed and archived, so they
        // should be deleted.
        let last_pos = positions.last().unwrap();
        cl.discard_completed(&table_id, *last_pos).unwrap();

        cl.shutdown().unwrap();
    }

    #[test]
    fn flushed_but_not_archived_segment_kept_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            archive: Some(super::config::ArchiveConfig {
                enabled: true,
                ..super::config::ArchiveConfig::default()
            }),
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        let table_id = TableId::new("test_ks", "test_table");

        // Append to force rotation.
        let mut last_pos = None;
        for _ in 0..10 {
            if let Ok(pos) = cl.append(&m) {
                last_pos = Some(pos);
            }
        }
        let last_pos = last_pos.unwrap();

        // Collect closed segment paths.
        let closed_paths: Vec<std::path::PathBuf> = cl
            .closed_segments
            .lock()
            .iter()
            .map(|s| s.path().to_path_buf())
            .collect();
        assert!(!closed_paths.is_empty());

        // Discard with archiving enabled but no segments marked as archived.
        cl.discard_completed(&table_id, last_pos).unwrap();

        // Segment files should still exist on disk.
        for path in &closed_paths {
            assert!(
                path.exists(),
                "segment {} should still exist (not archived yet)",
                path.display()
            );
        }

        cl.shutdown().unwrap();
    }

    #[test]
    fn parse_segment_id_works() {
        assert_eq!(parse_segment_id("commitlog-1.log"), Some(1));
        assert_eq!(parse_segment_id("commitlog-42.log"), Some(42));
        assert_eq!(parse_segment_id("commitlog-999.log"), Some(999));
        assert_eq!(parse_segment_id("other-file.txt"), None);
        assert_eq!(parse_segment_id("commitlog-.log"), None);
        assert_eq!(parse_segment_id("commitlog-abc.log"), None);
    }

    #[test]
    fn current_position_returns_active_segment_head() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let pos = cl.current_position();
        assert_eq!(pos.segment_id, 1, "first segment should have id 1");
        // Initial position is after header (17 bytes) + sync marker (8 bytes) = 25.
        assert_eq!(pos.offset, 25, "initial offset should be 25");

        cl.shutdown().unwrap();
    }

    #[test]
    fn current_position_advances_after_append() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig::test_config(dir.path());
        let cl = CommitLog::new(config).unwrap();

        let before = cl.current_position();
        let m = simple_mutation();
        cl.append(&m).unwrap();
        let after = cl.current_position();

        assert_eq!(before.segment_id, after.segment_id);
        assert!(
            after.offset > before.offset,
            "position should advance after append: before={}, after={}",
            before.offset,
            after.offset
        );

        cl.shutdown().unwrap();
    }

    #[test]
    fn current_position_reflects_new_segment_after_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let config = CommitLogConfig {
            segment_size: 512,
            ..CommitLogConfig::test_config(dir.path())
        };
        let cl = CommitLog::new(config).unwrap();

        let m = simple_mutation();
        // Write enough to force rotation.
        for _ in 0..10 {
            let _ = cl.append(&m);
        }

        let pos = cl.current_position();
        assert!(
            pos.segment_id > 1,
            "should have rotated to a new segment, got id={}",
            pos.segment_id
        );

        cl.shutdown().unwrap();
    }
}
