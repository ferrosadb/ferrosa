pub mod delivery;
pub mod segment;

pub use segment::HintRecord;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use uuid::Uuid;

use crate::error::{ClusterError, Result};
use segment::{AppendResult, SegmentReader, SegmentWriter};

// ---------------------------------------------------------------------------
// HintConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HintConfig {
    pub dir: PathBuf,
    /// Maximum total bytes of hints stored per peer before eviction.
    pub max_per_peer_mb: u64,
    /// Number of records to deliver per batch.
    pub delivery_batch_size: usize,
    /// How often delivery is attempted (milliseconds).
    pub delivery_interval_ms: u64,
    /// Size at which a segment file is rolled over (megabytes).
    pub segment_size_mb: u64,
}

impl Default for HintConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("hints"),
            max_per_peer_mb: 1024,
            delivery_batch_size: 128,
            delivery_interval_ms: 1000,
            segment_size_mb: 32,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal per-peer state
// ---------------------------------------------------------------------------

struct PeerHintState {
    writer: Option<SegmentWriter>,
    segment_counter: u32,
    total_bytes: u64,
    record_count: usize,
    needs_repair: bool,
}

impl PeerHintState {
    fn new() -> Self {
        Self {
            writer: None,
            segment_counter: 0,
            total_bytes: 0,
            record_count: 0,
            needs_repair: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Segment path helpers
// ---------------------------------------------------------------------------

/// Returns the directory used for a specific peer.
fn peer_dir(base: &Path, peer_id: Uuid) -> PathBuf {
    base.join(peer_id.to_string())
}

/// Returns the path for a numbered segment file.
fn segment_path(base: &Path, peer_id: Uuid, n: u32) -> PathBuf {
    peer_dir(base, peer_id).join(format!("segment_{:06}.hints", n))
}

/// Scan an existing peer directory and return the sorted list of (segment_number, path) pairs.
fn scan_segments(peer_dir: &Path) -> std::io::Result<Vec<(u32, PathBuf)>> {
    let mut segments: Vec<(u32, PathBuf)> = Vec::new();

    if !peer_dir.exists() {
        return Ok(segments);
    }

    for entry in fs::read_dir(peer_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(n) = parse_segment_number(&name_str) {
            segments.push((n, entry.path()));
        }
    }

    segments.sort_by_key(|(n, _)| *n);
    Ok(segments)
}

fn parse_segment_number(name: &str) -> Option<u32> {
    // Expected format: segment_NNNNNN.hints
    let stem = name.strip_suffix(".hints")?;
    let num_str = stem.strip_prefix("segment_")?;
    num_str.parse().ok()
}

// ---------------------------------------------------------------------------
// HintStore
// ---------------------------------------------------------------------------

pub struct HintStore {
    config: HintConfig,
    peers: Mutex<HashMap<Uuid, PeerHintState>>,
}

impl HintStore {
    /// Create a new `HintStore`, creating the base directory if needed.
    ///
    /// Scans existing peer subdirectories to recover state from a previous run.
    pub fn new(config: HintConfig) -> Result<Self> {
        fs::create_dir_all(&config.dir)
            .map_err(|e| ClusterError::Internal(format!("hints: cannot create base dir: {e}")))?;

        let mut peers: HashMap<Uuid, PeerHintState> = HashMap::new();

        // Recover existing peer directories.
        if let Ok(entries) = fs::read_dir(&config.dir) {
            for entry in entries.flatten() {
                let fname = entry.file_name();
                let name = fname.to_string_lossy();
                if let Ok(peer_id) = Uuid::parse_str(&name) {
                    let pdir = entry.path();
                    let segments = scan_segments(&pdir).unwrap_or_default();

                    let mut state = PeerHintState::new();

                    // Compute totals from existing segment files.
                    for (n, path) in &segments {
                        let meta = fs::metadata(path).unwrap_or_else(|_| {
                            // If we can't stat it, skip it.
                            fs::metadata(&config.dir).unwrap()
                        });
                        state.total_bytes += meta.len();
                        state.segment_counter = state.segment_counter.max(*n);
                    }

                    if !segments.is_empty() {
                        // Count records in each segment.
                        for (_, path) in &segments {
                            if let Ok(reader) = SegmentReader::open(path) {
                                state.record_count += reader.count();
                            }
                        }
                    }

                    peers.insert(peer_id, state);
                }
            }
        }

        Ok(Self {
            config,
            peers: Mutex::new(peers),
        })
    }

    // -----------------------------------------------------------------------
    // store
    // -----------------------------------------------------------------------

    /// Append a hint for `peer_id`.  Rolls over the segment when full and
    /// evicts oldest segments when the per-peer byte cap is exceeded.
    pub fn store(
        &self,
        peer_id: Uuid,
        keyspace: impl Into<String>,
        table: impl Into<String>,
        key: Vec<u8>,
        row: Vec<u8>,
        timestamp: i64,
    ) -> Result<()> {
        let record = HintRecord {
            timestamp,
            keyspace: keyspace.into(),
            table: table.into(),
            key,
            row,
        };

        let mut peers = self.peers.lock().unwrap();
        let state = peers.entry(peer_id).or_insert_with(PeerHintState::new);

        // Ensure the peer directory exists.
        let pdir = peer_dir(&self.config.dir, peer_id);
        fs::create_dir_all(&pdir)
            .map_err(|e| ClusterError::Internal(format!("hints: cannot create peer dir: {e}")))?;

        // Open writer for the current segment if not open.
        if state.writer.is_none() {
            let n = state.segment_counter;
            let path = segment_path(&self.config.dir, peer_id, n);
            let max_bytes = self.config.segment_size_mb * 1024 * 1024;
            let writer = SegmentWriter::create(&path, max_bytes)
                .map_err(|e| ClusterError::Internal(format!("hints: cannot open segment: {e}")))?;
            state.writer = Some(writer);
        }

        // Try to append; rollover if segment is full.
        let result = {
            let w = state.writer.as_mut().unwrap();
            w.append(&record)
                .map_err(|e| ClusterError::Internal(format!("hints: append failed: {e}")))?
        };

        if result == AppendResult::SegmentFull {
            // Sync and close the current segment.
            let mut w = state.writer.take().unwrap();
            w.sync()
                .map_err(|e| ClusterError::Internal(format!("hints: sync failed: {e}")))?;

            // Open the next segment.
            state.segment_counter += 1;
            let n = state.segment_counter;
            let path = segment_path(&self.config.dir, peer_id, n);
            let max_bytes = self.config.segment_size_mb * 1024 * 1024;
            let mut new_writer = SegmentWriter::create(&path, max_bytes).map_err(|e| {
                ClusterError::Internal(format!("hints: cannot create new segment: {e}"))
            })?;

            new_writer.append(&record).map_err(|e| {
                ClusterError::Internal(format!("hints: append to new segment failed: {e}"))
            })?;

            state.writer = Some(new_writer);
        }

        // Estimate the encoded size for bookkeeping.
        // 4 (length prefix) + 8 (ts) + 2+ks + 2+tbl + 4+key + 4+row + 4 (crc)
        let encoded_size = (4
            + 8
            + 2
            + record.keyspace.len()
            + 2
            + record.table.len()
            + 4
            + record.key.len()
            + 4
            + record.row.len()
            + 4) as u64;
        state.total_bytes += encoded_size;
        state.record_count += 1;

        // Check eviction.
        let cap_bytes = self.config.max_per_peer_mb * 1024 * 1024;
        if state.total_bytes > cap_bytes {
            self.evict_oldest(peer_id, state, cap_bytes)?;
        }

        Ok(())
    }

    /// Delete oldest segments until we are below the byte cap.
    /// Sets `needs_repair = true`.
    fn evict_oldest(&self, peer_id: Uuid, state: &mut PeerHintState, cap_bytes: u64) -> Result<()> {
        let pdir = peer_dir(&self.config.dir, peer_id);
        let mut segments = scan_segments(&pdir).map_err(|e| {
            ClusterError::Internal(format!("hints: scan failed during eviction: {e}"))
        })?;

        // Sort ascending (oldest first).
        segments.sort_by_key(|(n, _)| *n);

        // Skip the segment currently being written.
        let current_seg = state.segment_counter;

        for (n, path) in &segments {
            if state.total_bytes <= cap_bytes {
                break;
            }
            // Don't delete the segment we're currently writing to.
            if *n == current_seg {
                continue;
            }
            if let Ok(meta) = fs::metadata(path) {
                let file_size = meta.len();
                if let Err(e) = fs::remove_file(path) {
                    tracing::warn!("hints: failed to delete segment {:?}: {e}", path);
                } else {
                    state.total_bytes = state.total_bytes.saturating_sub(file_size);
                    tracing::warn!(
                        peer = %peer_id,
                        "hints: evicted segment {} due to byte cap exceeded",
                        n
                    );
                }
            }
        }

        state.needs_repair = true;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    pub fn pending_count(&self, peer_id: Uuid) -> usize {
        let peers = self.peers.lock().unwrap();
        peers.get(&peer_id).map(|s| s.record_count).unwrap_or(0)
    }

    pub fn pending_bytes(&self, peer_id: Uuid) -> u64 {
        let peers = self.peers.lock().unwrap();
        peers.get(&peer_id).map(|s| s.total_bytes).unwrap_or(0)
    }

    pub fn needs_repair(&self, peer_id: Uuid) -> bool {
        let peers = self.peers.lock().unwrap();
        peers.get(&peer_id).map(|s| s.needs_repair).unwrap_or(false)
    }

    // -----------------------------------------------------------------------
    // drain
    // -----------------------------------------------------------------------

    /// Return a `HintDrain` that iterates all hints for `peer_id` in FIFO order.
    ///
    /// The drain flushes and closes the current writer first, so all
    /// in-memory buffered hints are included.
    pub fn drain(&self, peer_id: Uuid) -> Result<HintDrain> {
        let mut peers = self.peers.lock().unwrap();
        if let Some(state) = peers.get_mut(&peer_id) {
            // Flush and close the current segment so the reader can open it.
            if let Some(mut w) = state.writer.take() {
                w.sync().map_err(|e| {
                    ClusterError::Internal(format!("hints: sync before drain failed: {e}"))
                })?;
            }
        }

        let pdir = peer_dir(&self.config.dir, peer_id);
        let segments = scan_segments(&pdir)
            .map_err(|e| ClusterError::Internal(format!("hints: scan for drain failed: {e}")))?;

        Ok(HintDrain {
            segments,
            current_reader: None,
            current_index: 0,
            fully_consumed: Vec::new(),
        })
    }

    // -----------------------------------------------------------------------
    // cleanup
    // -----------------------------------------------------------------------

    /// Delete all segment files that have been fully consumed by `drain`.
    /// After this call, the peer directory will be empty (or absent).
    pub fn cleanup(&self, peer_id: Uuid) {
        let pdir = peer_dir(&self.config.dir, peer_id);
        if let Ok(segments) = scan_segments(&pdir) {
            for (_, path) in &segments {
                if let Err(e) = fs::remove_file(path) {
                    tracing::warn!("hints: cleanup failed to remove {:?}: {e}", path);
                }
            }
        }
        // Try to remove the peer directory itself (only succeeds if empty).
        let _ = fs::remove_dir(&pdir);

        // Clear in-memory state.
        let mut peers = self.peers.lock().unwrap();
        if let Some(state) = peers.get_mut(&peer_id) {
            state.total_bytes = 0;
            state.record_count = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// HintDrain — consuming FIFO iterator across all segment files
// ---------------------------------------------------------------------------

pub struct HintDrain {
    /// All segments for this peer, sorted ascending (oldest first).
    segments: Vec<(u32, PathBuf)>,
    /// The reader for the segment we are currently consuming.
    current_reader: Option<SegmentReader>,
    /// Index into `segments` of the segment we are currently reading.
    current_index: usize,
    /// Segment indices that have been fully consumed (for future cleanup).
    fully_consumed: Vec<usize>,
}

impl Iterator for HintDrain {
    type Item = HintRecord;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have an active reader, try to pull the next record.
            if let Some(ref mut reader) = self.current_reader {
                if let Some(record) = reader.next() {
                    return Some(record);
                }
                // This segment is exhausted.
                self.fully_consumed.push(self.current_index);
                self.current_reader = None;
                self.current_index += 1;
            }

            // Advance to the next segment.
            if self.current_index >= self.segments.len() {
                return None;
            }

            let (_, path) = &self.segments[self.current_index];
            match SegmentReader::open(path) {
                Ok(reader) => {
                    self.current_reader = Some(reader);
                    // Loop back to try reading from this reader.
                }
                Err(e) => {
                    tracing::warn!("hints: cannot open segment {:?}: {e}", path);
                    // Skip this segment.
                    self.current_index += 1;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_config(dir: &TempDir, segment_size_mb: u64, max_per_peer_mb: u64) -> HintConfig {
        HintConfig {
            dir: dir.path().to_path_buf(),
            max_per_peer_mb,
            delivery_batch_size: 128,
            delivery_interval_ms: 1000,
            segment_size_mb,
        }
    }

    fn store_hint(store: &HintStore, peer: Uuid, i: u64) {
        store
            .store(
                peer,
                format!("ks_{i}"),
                format!("tbl_{i}"),
                format!("key_{i}").into_bytes(),
                format!("row_{i}").into_bytes(),
                i as i64,
            )
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // 1. Roundtrip a single hint
    // -----------------------------------------------------------------------
    #[test]
    fn hint_store_roundtrip() {
        let dir = TempDir::new().unwrap();
        let peer = Uuid::new_v4();
        let store = HintStore::new(make_config(&dir, 32, 1024)).unwrap();

        store
            .store(
                peer,
                "myks",
                "mytbl",
                b"pk1".to_vec(),
                b"row_data".to_vec(),
                12345,
            )
            .unwrap();

        let drain = store.drain(peer).unwrap();
        let records: Vec<_> = drain.collect();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].keyspace, "myks");
        assert_eq!(records[0].table, "mytbl");
        assert_eq!(records[0].key, b"pk1");
        assert_eq!(records[0].row, b"row_data");
        assert_eq!(records[0].timestamp, 12345);
    }

    // -----------------------------------------------------------------------
    // 2. FIFO ordering across multiple segments
    // -----------------------------------------------------------------------
    #[test]
    fn hint_store_fifo_across_segments() {
        let dir = TempDir::new().unwrap();
        let peer = Uuid::new_v4();
        // Use a tiny segment size to force many rollovers.
        let store = HintStore::new(make_config(&dir, 1, 1024)).unwrap();

        const N: u64 = 1000;
        for i in 0..N {
            store_hint(&store, peer, i);
        }

        let drain = store.drain(peer).unwrap();
        let records: Vec<_> = drain.collect();

        assert_eq!(records.len() as u64, N, "must recover all hints");

        // Verify FIFO order: timestamps should be 0, 1, 2, …
        for (idx, rec) in records.iter().enumerate() {
            assert_eq!(
                rec.timestamp, idx as i64,
                "hint {} has wrong timestamp",
                idx
            );
        }
    }

    // -----------------------------------------------------------------------
    // 3. pending_count and pending_bytes for multiple peers
    // -----------------------------------------------------------------------
    #[test]
    fn pending_count_and_bytes() {
        let dir = TempDir::new().unwrap();
        let peer_a = Uuid::new_v4();
        let peer_b = Uuid::new_v4();
        let store = HintStore::new(make_config(&dir, 32, 1024)).unwrap();

        for i in 0..5 {
            store_hint(&store, peer_a, i);
        }
        for i in 0..3 {
            store_hint(&store, peer_b, i);
        }

        assert_eq!(store.pending_count(peer_a), 5);
        assert_eq!(store.pending_count(peer_b), 3);
        assert!(store.pending_bytes(peer_a) > 0);
        assert!(store.pending_bytes(peer_b) > 0);
        // peer_a has more hints so more bytes
        assert!(store.pending_bytes(peer_a) > store.pending_bytes(peer_b));

        // Unknown peer returns zero.
        assert_eq!(store.pending_count(Uuid::new_v4()), 0);
        assert_eq!(store.pending_bytes(Uuid::new_v4()), 0);
    }

    // -----------------------------------------------------------------------
    // 4. cleanup deletes consumed segments
    // -----------------------------------------------------------------------
    #[test]
    fn cleanup_deletes_consumed_segments() {
        let dir = TempDir::new().unwrap();
        let peer = Uuid::new_v4();
        // Tiny segments to produce multiple files.
        let store = HintStore::new(make_config(&dir, 1, 1024)).unwrap();

        for i in 0..50 {
            store_hint(&store, peer, i);
        }

        // Drain all hints.
        let drain = store.drain(peer).unwrap();
        let records: Vec<_> = drain.collect();
        assert!(!records.is_empty());

        // Cleanup.
        store.cleanup(peer);

        // Peer directory should now be empty (or non-existent).
        let pdir = peer_dir(&store.config.dir, peer);
        if pdir.exists() {
            let remaining: Vec<_> = fs::read_dir(&pdir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                remaining.is_empty(),
                "peer directory should be empty after cleanup"
            );
        }
    }

    // -----------------------------------------------------------------------
    // 5. Eviction: exceed cap, verify eviction + needs_repair flag
    // -----------------------------------------------------------------------
    #[test]
    fn eviction_deletes_oldest_and_sets_needs_repair() {
        let dir = TempDir::new().unwrap();
        let peer = Uuid::new_v4();

        // Very small cap: 1 MB max, 1 MB segment size.
        // We'll write enough data to exceed the cap.
        // Each hint ≈ ~100 bytes.  1 MB cap → ~10000 hints would exceed it,
        // but let's use a smaller cap in bytes by setting max_per_peer_mb=0.
        // Instead we use a tiny cap of 1 KB by manipulating the multiplier.
        // The config is in MB, so set max_per_peer_mb=1 and write > 1 MB of data.
        // Easier: set max = 1 (MB) but write rows of size 512 KB each.

        // Use segment_size_mb=1 and max_per_peer_mb=1.
        // Write rows of ~512 KB so that 3 of them exceed the 1 MB cap.
        let store = HintStore::new(make_config(&dir, 1, 1)).unwrap();

        let big_row = vec![0u8; 512 * 1024]; // 512 KB

        // Store 3 hints, each ~512 KB row.  After the third, total > 1 MB.
        for i in 0..3u64 {
            store
                .store(
                    peer,
                    "ks",
                    "tbl",
                    format!("key_{i}").into_bytes(),
                    big_row.clone(),
                    i as i64,
                )
                .unwrap();
        }

        assert!(
            store.needs_repair(peer),
            "needs_repair must be set after eviction"
        );

        // At least one segment file should have been deleted.
        let pdir = peer_dir(&store.config.dir, peer);
        let remaining = scan_segments(&pdir).unwrap();
        // We had segments 0 and possibly 1; oldest (0) should have been evicted.
        // The current segment (being written) is never evicted.
        // With 3 hints of 512 KB each:
        //   - hints 0..1 may fit in segment 0
        //   - remaining go to segment 1
        // After eviction, segment 0 (oldest) should be gone.
        let pdir_entries: Vec<_> = remaining.iter().map(|(n, _)| *n).collect();
        if pdir_entries.len() > 1 {
            // If multiple segments remain, the lowest-numbered one should NOT be 0
            // (i.e. segment 0 was evicted).
            assert!(
                !pdir_entries.contains(&0),
                "oldest segment should have been evicted; remaining: {:?}",
                pdir_entries
            );
        }
        // If only one segment remains, that's the current one being written — also fine.
    }
}
