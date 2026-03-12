//! Segment: fixed-size byte buffer with lock-free CAS allocation.
//!
//! A `Segment` is the central building block of the commit log. Writers
//! concurrently allocate non-overlapping slices from the segment's buffer
//! using an atomic compare-and-swap loop — no locks on the hot path.
//!
//! # Allocation Protocol
//!
//! 1. Writer calls [`Segment::allocate(size)`](Segment::allocate) which performs
//!    an `AtomicU64::compare_exchange` loop on the write position.
//! 2. On success, the writer receives an exclusive byte offset.
//! 3. The writer calls [`Segment::write_entry()`](Segment::write_entry) to
//!    serialize the mutation at that offset.
//!
//! If `allocate()` would exceed segment capacity, it returns `None` — the
//! caller must trigger segment rotation.
//!
//! # Entry Format
//!
//! ```text
//! entry_size:   u32 (4 bytes) — size of payload only
//! size_crc:     u32 (4 bytes) — CRC32 of the 4 entry_size bytes
//! payload:      [u8; entry_size] — serialized Mutation
//! payload_crc:  u32 (4 bytes) — CRC32 of payload
//! ```
//!
//! Entry overhead: 12 bytes (4 + 4 + 4).
//!
//! # Sync Marker Format (8 bytes)
//!
//! ```text
//! next_marker_offset: u32 — absolute byte offset of next sync marker (0 = EOF)
//! marker_crc:         u32 — CRC32 of (segment_id as u64 || next_marker_offset as u32)
//! ```
//!
//! # Safety
//!
//! The segment buffer is wrapped in [`UnsafeCell`] because concurrent writers
//! write to non-overlapping slices after CAS allocation. This is safe because:
//! - Each `allocate()` call atomically claims a unique, non-overlapping range.
//! - No two threads can receive the same offset range.
//! - Each thread only writes to its exclusively-owned range.

// Items are used by later tasks (SyncStrategy, Reader, CommitLog); suppress
// dead-code warnings until those modules exist.
#![allow(dead_code)]

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::config::{CommitLogPosition, TableId};
use super::descriptor::{SegmentDescriptor, HEADER_SIZE};
use super::mutation::Mutation;

/// Entry overhead in bytes: entry_size (4) + size_crc (4) + payload_crc (4).
pub const ENTRY_OVERHEAD: usize = 12;

/// Sync marker size in bytes: next_marker_offset (4) + marker_crc (4).
pub const SYNC_MARKER_SIZE: usize = 8;

/// Initial write position: after the 17-byte header and 8-byte first sync marker.
const INITIAL_POSITION: u64 = (HEADER_SIZE + SYNC_MARKER_SIZE) as u64;

/// A fixed-size byte buffer with lock-free CAS allocation for commit log entries.
///
/// The segment is `Send + Sync` because concurrent access to the buffer is
/// mediated by the atomic position: each writer exclusively owns the byte
/// range returned by [`allocate()`](Self::allocate).
pub struct Segment {
    /// Segment identifier (monotonically increasing).
    pub id: u64,

    /// Pre-allocated buffer of `capacity` bytes, zeroed.
    ///
    /// # Safety
    ///
    /// Wrapped in `UnsafeCell` because concurrent writers access non-overlapping
    /// slices. The CAS allocation protocol guarantees exclusive ownership of
    /// each byte range — see module-level documentation.
    buffer: UnsafeCell<Vec<u8>>,

    /// Next write offset. Atomically advanced by `allocate()`.
    position: AtomicU64,

    /// Maximum usable bytes in the buffer.
    capacity: usize,

    /// When this segment was created (for age-based rotation).
    created_at: Instant,

    /// File path for this segment.
    path: PathBuf,

    /// Tables with uncommitted data in this segment.
    dirty_tables: Mutex<HashMap<TableId, CommitLogPosition>>,

    /// Number of writers currently between `allocate()` and `write_entry()`
    /// completion. `flush_to_disk()` waits for this to reach zero before
    /// reading the buffer, ensuring no partially-written entries are captured.
    in_flight_writers: AtomicU64,

    /// Persistent file handle for incremental flush. Opened on first flush,
    /// reused for subsequent appends.
    file_handle: Mutex<Option<std::fs::File>>,

    /// Byte position up to which data has been flushed to disk. Used by
    /// incremental flush to write only new bytes.
    last_flushed: AtomicU64,

    /// Absolute byte offset of the most recently written sync marker.
    /// Used to build the forward-linked marker chain: when a new marker is
    /// written, the previous marker's `next_marker_offset` is patched to
    /// point to the new one.
    last_sync_marker_offset: AtomicU64,
}

// SAFETY: The CAS allocation protocol guarantees that concurrent writers access
// non-overlapping byte ranges. The buffer is only written to through exclusively-
// owned offsets, and reads (flush_to_disk) only occur on the already-written
// portion of the buffer.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl Segment {
    /// Creates a new segment with the given ID and capacity.
    ///
    /// The buffer is pre-allocated and zeroed. The 17-byte header is written
    /// immediately, followed by an 8-byte sync marker (all zeros = EOF marker).
    pub fn new(id: u64, size: usize, dir: &Path) -> Self {
        assert!(
            size >= INITIAL_POSITION as usize + ENTRY_OVERHEAD,
            "segment size too small: need at least {} bytes",
            INITIAL_POSITION as usize + ENTRY_OVERHEAD
        );

        let mut buffer = vec![0u8; size];

        // Write the segment header (17 bytes).
        let descriptor = SegmentDescriptor::new(id);
        descriptor.write_to(&mut buffer[..HEADER_SIZE]);

        // Write the first sync marker at offset HEADER_SIZE (17).
        // All zeros = EOF marker (next_marker_offset = 0, marker_crc = CRC of
        // segment_id || 0).
        Self::write_sync_marker_to_buffer(&mut buffer, HEADER_SIZE, id, 0);

        let path = dir.join(format!("commitlog-{id}.log"));

        Segment {
            id,
            buffer: UnsafeCell::new(buffer),
            position: AtomicU64::new(INITIAL_POSITION),
            capacity: size,
            created_at: Instant::now(),
            path,
            dirty_tables: Mutex::new(HashMap::new()),
            in_flight_writers: AtomicU64::new(0),
            file_handle: Mutex::new(None),
            last_flushed: AtomicU64::new(INITIAL_POSITION),
            last_sync_marker_offset: AtomicU64::new(HEADER_SIZE as u64),
        }
    }

    /// Atomically allocates `entry_total_size` bytes from the segment buffer.
    ///
    /// Returns the start offset of the exclusively-owned byte range, or `None`
    /// if the allocation would exceed segment capacity.
    ///
    /// This is the hot path — no locks, just a CAS loop.
    pub fn allocate(&self, entry_total_size: usize) -> Option<u64> {
        loop {
            let current = self.position.load(Ordering::Acquire);
            let new_pos = current + entry_total_size as u64;

            if new_pos > self.capacity as u64 {
                return None;
            }

            match self.position.compare_exchange(
                current,
                new_pos,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(current),
                Err(_) => continue, // Another thread won; retry.
            }
        }
    }

    /// Atomically allocates space AND increments the in-flight writer counter.
    ///
    /// This is the same as `allocate()` but also tracks the writer for
    /// `flush_to_disk()` coordination. The caller MUST call `writer_done()`
    /// after writing the entry.
    ///
    /// Used by `CommitLog::append()` to close the window between allocation
    /// and entry write where flush could read partially-written data.
    pub fn allocate_and_begin_write(&self, entry_total_size: usize) -> Option<u64> {
        // Increment FIRST so that flush_to_disk() will wait for us even if
        // another thread interleaves between our CAS success and write_entry().
        self.in_flight_writers.fetch_add(1, Ordering::AcqRel);
        match self.allocate(entry_total_size) {
            Some(offset) => Some(offset),
            None => {
                // Allocation failed (segment full) — undo the increment.
                self.in_flight_writers.fetch_sub(1, Ordering::AcqRel);
                None
            }
        }
    }

    /// Decrements the in-flight writer count. Called after `write_entry()`
    /// completes. Paired with the increment in [`allocate()`](Self::allocate).
    pub fn writer_done(&self) {
        self.in_flight_writers.fetch_sub(1, Ordering::AcqRel);
    }

    /// Spins until all in-flight writers have completed their entries.
    /// Called by `flush_to_disk()` before reading the buffer.
    fn wait_for_writers(&self) {
        let mut spins = 0;
        while self.in_flight_writers.load(Ordering::Acquire) > 0 {
            spins += 1;
            if spins > 1000 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
    }

    /// Writes a mutation entry at the given offset.
    ///
    /// The entry format is: entry_size (u32) + size_crc (u32) + payload + payload_crc (u32).
    ///
    /// The caller MUST call `writer_begin()` before this method and
    /// `writer_done()` after it returns to coordinate with `flush_to_disk()`.
    ///
    /// # Safety Argument
    ///
    /// This method uses `unsafe` to obtain a mutable reference to the buffer
    /// slice at `offset`. This is safe because:
    /// - The `offset` was obtained from [`allocate()`](Self::allocate), which
    ///   guarantees exclusive ownership of the byte range `[offset, offset + total_size)`.
    /// - No other thread can write to or read from this range until the segment
    ///   is flushed (which requires all writers to have completed their entries).
    pub fn write_entry(&self, offset: u64, mutation: &Mutation) -> CommitLogPosition {
        let payload_size = mutation.serialized_size();
        let total_size = ENTRY_OVERHEAD + payload_size;
        let off = offset as usize;

        debug_assert!(
            off + total_size <= self.capacity,
            "write_entry: offset {off} + size {total_size} exceeds capacity {}",
            self.capacity
        );

        // SAFETY: offset range [off, off + total_size) is exclusively owned by
        // this thread after CAS allocation. No other thread accesses this range.
        let buf = unsafe { &mut *self.buffer.get() };

        // 1. Write entry_size (payload size as u32, big-endian).
        let entry_size_bytes = (payload_size as u32).to_be_bytes();
        buf[off..off + 4].copy_from_slice(&entry_size_bytes);

        // 2. Write size_crc (CRC32 of the 4 entry_size bytes).
        let size_crc = crc32fast::hash(&entry_size_bytes);
        buf[off + 4..off + 8].copy_from_slice(&size_crc.to_be_bytes());

        // 3. Write payload (serialized mutation).
        let payload_start = off + 8;
        let payload_end = payload_start + payload_size;
        mutation.serialize_into(&mut buf[payload_start..payload_end]);

        // 4. Write payload_crc (CRC32 of payload bytes).
        let payload_crc = crc32fast::hash(&buf[payload_start..payload_end]);
        buf[payload_end..payload_end + 4].copy_from_slice(&payload_crc.to_be_bytes());

        CommitLogPosition {
            segment_id: self.id,
            offset,
        }
    }

    /// Writes a sync marker at the current sync position.
    ///
    /// The sync marker contains `next_offset` (the absolute byte offset of
    /// the next sync marker, or 0 for EOF) and a CRC of the segment ID
    /// concatenated with the offset.
    pub fn write_sync_marker(&self, next_offset: u32) {
        let current_pos = self.position.load(Ordering::Acquire) as usize;

        // SAFETY: sync markers are written by the sync strategy, which
        // coordinates with writers. The marker is written at the current
        // position after all preceding entries are complete.
        let buf = unsafe { &mut *self.buffer.get() };

        Self::write_sync_marker_to_buffer(buf, current_pos, self.id, next_offset);

        // Advance position past the sync marker.
        self.position
            .fetch_add(SYNC_MARKER_SIZE as u64, Ordering::AcqRel);
    }

    /// Writes a sync marker at an explicitly allocated offset, linking it
    /// into the forward chain.
    ///
    /// Unlike `write_sync_marker()`, this does not advance the position — the
    /// offset was already reserved via `allocate()`.
    ///
    /// The previous sync marker's `next_marker_offset` is patched to point to
    /// this new marker, creating the forward-linked chain for crash recovery.
    pub fn write_sync_marker_at(&self, offset: u64, next_marker_offset: u32) {
        let buf = unsafe { &mut *self.buffer.get() };

        // Patch the previous marker to point to this one.
        let prev_offset = self.last_sync_marker_offset.load(Ordering::Acquire) as usize;
        Self::write_sync_marker_to_buffer(buf, prev_offset, self.id, offset as u32);

        // Write the new marker (EOF by default).
        Self::write_sync_marker_to_buffer(buf, offset as usize, self.id, next_marker_offset);

        // Update the tracker.
        self.last_sync_marker_offset
            .store(offset, Ordering::Release);
    }

    /// Internal helper to write a sync marker into a buffer at a given offset.
    fn write_sync_marker_to_buffer(
        buf: &mut [u8],
        offset: usize,
        segment_id: u64,
        next_marker_offset: u32,
    ) {
        // next_marker_offset: u32 (4 bytes)
        buf[offset..offset + 4].copy_from_slice(&next_marker_offset.to_be_bytes());

        // marker_crc: CRC32 of (segment_id as u64 || next_marker_offset as u32)
        let mut crc_input = [0u8; 12];
        crc_input[..8].copy_from_slice(&segment_id.to_be_bytes());
        crc_input[8..12].copy_from_slice(&next_marker_offset.to_be_bytes());
        let crc = crc32fast::hash(&crc_input);
        buf[offset + 4..offset + 8].copy_from_slice(&crc.to_be_bytes());
    }

    /// Flushes the written portion of the buffer to disk and fsyncs.
    ///
    /// Uses incremental flush: only writes bytes since the last flush.
    /// Waits for all in-flight writers to complete before reading the buffer.
    pub fn flush_to_disk(&self) -> ferrosa_common::Result<()> {
        // Snapshot position BEFORE waiting. This captures only data from
        // writers who have already allocated. New allocations after this
        // point will be flushed on the next call.
        let snapshot_pos = self.position.load(Ordering::Acquire) as usize;

        // Wait for all in-flight writers to complete their entries.
        // After this returns, buffer[0..snapshot_pos] is fully written.
        self.wait_for_writers();

        // SAFETY: We only read buffer[0..snapshot_pos]. All bytes in this range
        // have been fully written — wait_for_writers() ensures no in-flight writes.
        let buf = unsafe { &*self.buffer.get() };

        // File I/O happens under the file_handle mutex to prevent concurrent
        // flushers from writing duplicate bytes.
        let mut handle = self.file_handle.lock();
        let last_flushed = self.last_flushed.load(Ordering::Acquire) as usize;
        let current_pos = snapshot_pos;

        match handle.as_mut() {
            None => {
                // First flush: create file, write from beginning (header + sync marker + entries).
                if let Some(parent) = self.path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let f = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&self.path)?;
                *handle = Some(f);
                let file = handle.as_mut().unwrap();
                use std::io::Write;
                file.write_all(&buf[..current_pos])?;
                file.sync_all()?;
                self.last_flushed
                    .store(current_pos as u64, Ordering::Release);
            }
            Some(file) if current_pos > last_flushed => {
                // Incremental: append only new bytes.
                use std::io::Write;
                file.write_all(&buf[last_flushed..current_pos])?;
                file.sync_all()?;
                self.last_flushed
                    .store(current_pos as u64, Ordering::Release);
            }
            Some(_) => {
                // Nothing new to flush.
            }
        }

        Ok(())
    }

    /// Marks a table as having dirty (unflushed) data in this segment.
    pub fn mark_table_dirty(&self, table_id: &TableId, position: CommitLogPosition) {
        let mut dirty = self.dirty_tables.lock();
        dirty
            .entry(table_id.clone())
            .and_modify(|existing| {
                if position > *existing {
                    *existing = position;
                }
            })
            .or_insert(position);
    }

    /// Returns `true` if this segment is older than `max_age`.
    pub fn is_expired(&self, max_age: Duration) -> bool {
        self.created_at.elapsed() > max_age
    }

    /// Returns the current write position (next available offset).
    pub fn current_position(&self) -> u64 {
        self.position.load(Ordering::Acquire)
    }

    /// Returns the segment file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the segment capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Computes the total entry size for a mutation (payload + overhead).
    pub fn entry_total_size(mutation: &Mutation) -> usize {
        ENTRY_OVERHEAD + mutation.serialized_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use std::collections::HashSet;
    use std::sync::Arc;

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

    #[test]
    fn allocate_returns_sequential_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m = simple_mutation();
        let entry_size = Segment::entry_total_size(&m);

        let offset1 = segment.allocate(entry_size).unwrap();
        let offset2 = segment.allocate(entry_size).unwrap();

        assert_eq!(offset1, INITIAL_POSITION);
        assert_eq!(offset2, INITIAL_POSITION + entry_size as u64);
        // Non-overlapping: offset2 starts where offset1's range ends.
        assert!(offset2 >= offset1 + entry_size as u64);
    }

    #[test]
    fn allocate_returns_none_when_full() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segment: just enough for header + sync marker + one small entry.
        let min_size = INITIAL_POSITION as usize + ENTRY_OVERHEAD + 1;
        let segment = Segment::new(1, min_size, dir.path());

        // This should fail because it's bigger than remaining space.
        let result = segment.allocate(min_size);
        assert!(result.is_none());
    }

    #[test]
    fn write_entry_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m = simple_mutation();
        let payload_size = m.serialized_size();
        let total_size = Segment::entry_total_size(&m);

        let offset = segment.allocate(total_size).unwrap();
        let pos = segment.write_entry(offset, &m);

        assert_eq!(pos.segment_id, 1);
        assert_eq!(pos.offset, offset);

        // Read back raw bytes and verify CRCs.
        let buf = unsafe { &*segment.buffer.get() };
        let off = offset as usize;

        // Read entry_size.
        let entry_size = u32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        assert_eq!(entry_size as usize, payload_size);

        // Verify size_crc.
        let size_crc = u32::from_be_bytes([buf[off + 4], buf[off + 5], buf[off + 6], buf[off + 7]]);
        let expected_size_crc = crc32fast::hash(&(payload_size as u32).to_be_bytes());
        assert_eq!(size_crc, expected_size_crc);

        // Verify payload_crc.
        let payload_start = off + 8;
        let payload_end = payload_start + payload_size;
        let payload_crc = u32::from_be_bytes([
            buf[payload_end],
            buf[payload_end + 1],
            buf[payload_end + 2],
            buf[payload_end + 3],
        ]);
        let expected_payload_crc = crc32fast::hash(&buf[payload_start..payload_end]);
        assert_eq!(payload_crc, expected_payload_crc);

        // Deserialize payload and verify content.
        let deserialized = Mutation::deserialize_from(&buf[payload_start..payload_end]).unwrap();
        assert_eq!(deserialized.keyspace, m.keyspace);
        assert_eq!(deserialized.table, m.table);
        assert_eq!(deserialized.key, m.key);
        assert_eq!(deserialized.timestamp, m.timestamp);
    }

    #[test]
    fn concurrent_allocations_no_overlap() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Arc::new(Segment::new(1, 1024 * 1024, dir.path()));
        let alloc_size = 64;
        let threads = 8;
        let allocs_per_thread = 100;

        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let seg = Arc::clone(&segment);
                std::thread::spawn(move || {
                    let mut offsets = Vec::new();
                    for _ in 0..allocs_per_thread {
                        if let Some(offset) = seg.allocate(alloc_size) {
                            offsets.push(offset);
                        }
                    }
                    offsets
                })
            })
            .collect();

        let mut all_offsets = Vec::new();
        for handle in handles {
            all_offsets.extend(handle.join().unwrap());
        }

        // All offsets should be unique.
        let unique: HashSet<u64> = all_offsets.iter().copied().collect();
        assert_eq!(unique.len(), all_offsets.len(), "offsets must be unique");

        // All ranges should be non-overlapping.
        let mut sorted = all_offsets.clone();
        sorted.sort();
        for window in sorted.windows(2) {
            assert!(
                window[1] >= window[0] + alloc_size as u64,
                "overlapping ranges: {} and {} with size {}",
                window[0],
                window[1],
                alloc_size
            );
        }
    }

    #[test]
    fn entry_overhead_is_12_bytes() {
        assert_eq!(ENTRY_OVERHEAD, 12);
        // 4 (entry_size) + 4 (size_crc) + 4 (payload_crc)
        assert_eq!(4 + 4 + 4, ENTRY_OVERHEAD);
    }

    #[test]
    fn segment_starts_after_header_and_marker() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let offset = segment.allocate(ENTRY_OVERHEAD).unwrap();
        assert_eq!(offset, INITIAL_POSITION);
        assert_eq!(INITIAL_POSITION, (HEADER_SIZE + SYNC_MARKER_SIZE) as u64);
        assert_eq!(INITIAL_POSITION, 25);
    }

    #[test]
    fn flush_to_disk_writes_file() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let m = simple_mutation();
        let total_size = Segment::entry_total_size(&m);
        let offset = segment.allocate(total_size).unwrap();
        segment.write_entry(offset, &m);

        segment.flush_to_disk().unwrap();

        let path = segment.path();
        assert!(path.exists());
        let contents = fs::read(path).unwrap();
        let expected_len = segment.current_position() as usize;
        assert_eq!(contents.len(), expected_len);
    }

    #[test]
    fn mark_table_dirty_tracks_latest_position() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());
        let table = TableId::new("ks", "tbl");

        let pos1 = CommitLogPosition {
            segment_id: 1,
            offset: 25,
        };
        let pos2 = CommitLogPosition {
            segment_id: 1,
            offset: 100,
        };

        segment.mark_table_dirty(&table, pos1);
        segment.mark_table_dirty(&table, pos2);

        let dirty = segment.dirty_tables.lock();
        assert_eq!(dirty[&table], pos2);
    }

    #[test]
    fn is_expired_checks_age() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        // Should not be expired with a very long max age.
        assert!(!segment.is_expired(Duration::from_secs(3600)));

        // Should be expired with zero duration.
        assert!(segment.is_expired(Duration::ZERO));
    }

    #[test]
    fn header_is_valid_after_new() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(42, 4096, dir.path());

        let buf = unsafe { &*segment.buffer.get() };
        let descriptor = SegmentDescriptor::read_from(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(descriptor.segment_id, 42);
    }

    #[test]
    fn sync_marker_crc_is_valid() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let buf = unsafe { &*segment.buffer.get() };
        let marker_offset = HEADER_SIZE;

        // Read next_marker_offset (should be 0 = EOF).
        let next_offset = u32::from_be_bytes([
            buf[marker_offset],
            buf[marker_offset + 1],
            buf[marker_offset + 2],
            buf[marker_offset + 3],
        ]);
        assert_eq!(next_offset, 0);

        // Read and verify CRC.
        let stored_crc = u32::from_be_bytes([
            buf[marker_offset + 4],
            buf[marker_offset + 5],
            buf[marker_offset + 6],
            buf[marker_offset + 7],
        ]);

        let mut crc_input = [0u8; 12];
        crc_input[..8].copy_from_slice(&1u64.to_be_bytes());
        crc_input[8..12].copy_from_slice(&0u32.to_be_bytes());
        let expected_crc = crc32fast::hash(&crc_input);
        assert_eq!(stored_crc, expected_crc);
    }

    #[test]
    fn segment_path_format() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(42, 4096, dir.path());
        assert_eq!(
            segment.path().file_name().unwrap().to_str().unwrap(),
            "commitlog-42.log"
        );
    }
}
