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
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use ferrosa_common::key::DecoratedKey;
use ferrosa_sstable::types::Row;
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

static INCREMENTAL_FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static INCREMENTAL_FLUSH_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static FULL_FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static FULL_FLUSH_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNCS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static SYNC_WAIT_WRITERS_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_WAIT_WRITERS_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static SYNC_FILE_LOCK_WAIT_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_FILE_LOCK_WAIT_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static SYNC_WRITE_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_WRITE_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static SYNC_DATA_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_DATA_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static SYNC_PARENT_DIR_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_PARENT_DIR_MICROS_MAX: AtomicU64 = AtomicU64::new(0);

pub(crate) struct CommitLogFlushMetrics {
    pub incremental_flushes: u64,
    pub incremental_bytes: u64,
    pub full_flushes: u64,
    pub full_bytes: u64,
    pub syncs: u64,
    pub sync_micros_total: u64,
    pub sync_micros_max: u64,
    pub sync_wait_writers_micros_total: u64,
    pub sync_wait_writers_micros_max: u64,
    pub sync_file_lock_wait_micros_total: u64,
    pub sync_file_lock_wait_micros_max: u64,
    pub sync_write_micros_total: u64,
    pub sync_write_micros_max: u64,
    pub sync_data_micros_total: u64,
    pub sync_data_micros_max: u64,
    pub sync_parent_dir_micros_total: u64,
    pub sync_parent_dir_micros_max: u64,
}

pub(crate) fn flush_metrics() -> CommitLogFlushMetrics {
    CommitLogFlushMetrics {
        incremental_flushes: INCREMENTAL_FLUSHES_TOTAL.load(Ordering::Relaxed),
        incremental_bytes: INCREMENTAL_FLUSH_BYTES_TOTAL.load(Ordering::Relaxed),
        full_flushes: FULL_FLUSHES_TOTAL.load(Ordering::Relaxed),
        full_bytes: FULL_FLUSH_BYTES_TOTAL.load(Ordering::Relaxed),
        syncs: SYNCS_TOTAL.load(Ordering::Relaxed),
        sync_micros_total: SYNC_MICROS_TOTAL.load(Ordering::Relaxed),
        sync_micros_max: SYNC_MICROS_MAX.load(Ordering::Relaxed),
        sync_wait_writers_micros_total: SYNC_WAIT_WRITERS_MICROS_TOTAL.load(Ordering::Relaxed),
        sync_wait_writers_micros_max: SYNC_WAIT_WRITERS_MICROS_MAX.load(Ordering::Relaxed),
        sync_file_lock_wait_micros_total: SYNC_FILE_LOCK_WAIT_MICROS_TOTAL.load(Ordering::Relaxed),
        sync_file_lock_wait_micros_max: SYNC_FILE_LOCK_WAIT_MICROS_MAX.load(Ordering::Relaxed),
        sync_write_micros_total: SYNC_WRITE_MICROS_TOTAL.load(Ordering::Relaxed),
        sync_write_micros_max: SYNC_WRITE_MICROS_MAX.load(Ordering::Relaxed),
        sync_data_micros_total: SYNC_DATA_MICROS_TOTAL.load(Ordering::Relaxed),
        sync_data_micros_max: SYNC_DATA_MICROS_MAX.load(Ordering::Relaxed),
        sync_parent_dir_micros_total: SYNC_PARENT_DIR_MICROS_TOTAL.load(Ordering::Relaxed),
        sync_parent_dir_micros_max: SYNC_PARENT_DIR_MICROS_MAX.load(Ordering::Relaxed),
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn update_max_u64(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

fn observe_sync(duration: Duration) {
    let micros = duration_micros(duration);
    SYNC_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(&SYNC_MICROS_MAX, micros);
}

fn observe_wait_writers(duration: Duration) {
    let micros = duration_micros(duration);
    SYNC_WAIT_WRITERS_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(&SYNC_WAIT_WRITERS_MICROS_MAX, micros);
}

fn observe_phase(total: &AtomicU64, max: &AtomicU64, duration: Duration) {
    let micros = duration_micros(duration);
    total.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(max, micros);
}

#[cfg(all(
    target_os = "macos",
    any(feature = "macos-fullfsync", not(feature = "macos-standard-sync"))
))]
fn full_sync_file(file: &fs::File) -> ferrosa_common::Result<()> {
    use std::os::fd::AsRawFd;

    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    if rc == -1 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

fn sync_commitlog_file(file: &fs::File) -> ferrosa_common::Result<()> {
    #[cfg(all(
        target_os = "macos",
        any(feature = "macos-fullfsync", not(feature = "macos-standard-sync"))
    ))]
    {
        full_sync_file(file)
    }

    #[cfg(any(
        not(target_os = "macos"),
        all(
            target_os = "macos",
            feature = "macos-standard-sync",
            not(feature = "macos-fullfsync")
        )
    ))]
    {
        file.sync_data()?;
        Ok(())
    }
}

fn sync_parent_dir(path: &Path) -> ferrosa_common::Result<()> {
    if let Some(parent) = path.parent() {
        let dir = fs::File::open(parent)?;
        #[cfg(all(
            target_os = "macos",
            any(feature = "macos-fullfsync", not(feature = "macos-standard-sync"))
        ))]
        {
            if full_sync_file(&dir).is_err() {
                dir.sync_all()?;
            }
        }
        #[cfg(any(
            not(target_os = "macos"),
            all(
                target_os = "macos",
                feature = "macos-standard-sync",
                not(feature = "macos-fullfsync")
            )
        ))]
        {
            dir.sync_all()?;
        }
    }
    Ok(())
}

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
    ///
    /// Stores only the **offset** — `segment_id` is implicit in `self.id`.
    /// Using `DashMap<TableId, AtomicU64>` keeps the hot-path update
    /// (`mark_table_dirty`, called on every write) lock-free in steady
    /// state: a `get(table_id)` returns a shard-read-locked `Ref` and
    /// `fetch_max` advances the offset atomically. The previous
    /// `Mutex<HashMap<...>>` was a global serialization point for every
    /// append into the active segment.
    pub(crate) dirty_tables: DashMap<TableId, AtomicU64>,

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

#[derive(Default)]
struct SyncPhaseDurations {
    wait_writers: Duration,
    file_lock_wait: Duration,
    write: Duration,
    sync_data: Duration,
    parent_dir_sync: Duration,
}

fn slow_sync_warn_threshold() -> Duration {
    static THRESHOLD_MICROS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    Duration::from_micros(*THRESHOLD_MICROS.get_or_init(|| {
        std::env::var("FERROSA_COMMITLOG_SLOW_SYNC_WARN_MILLIS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.saturating_mul(1_000))
            .unwrap_or(250_000)
    }))
}

fn maybe_warn_slow_sync(
    segment_id: u64,
    written: usize,
    total: Duration,
    phases: &SyncPhaseDurations,
) {
    if total < slow_sync_warn_threshold() {
        return;
    }
    tracing::warn!(
        segment_id,
        written,
        total_ms = total.as_secs_f64() * 1_000.0,
        wait_writers_ms = phases.wait_writers.as_secs_f64() * 1_000.0,
        file_lock_wait_ms = phases.file_lock_wait.as_secs_f64() * 1_000.0,
        write_ms = phases.write.as_secs_f64() * 1_000.0,
        sync_data_ms = phases.sync_data.as_secs_f64() * 1_000.0,
        parent_dir_sync_ms = phases.parent_dir_sync.as_secs_f64() * 1_000.0,
        "commitlog: slow sync"
    );
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
            dirty_tables: DashMap::new(),
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
    fn wait_for_writers(&self) -> Duration {
        let started = Instant::now();
        let mut spins = 0;
        while self.in_flight_writers.load(Ordering::Acquire) > 0 {
            spins += 1;
            if spins > 1000 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
        started.elapsed()
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

    #[allow(clippy::too_many_arguments)]
    pub fn write_single_row_entry(
        &self,
        offset: u64,
        mutation_id: [u8; 16],
        keyspace: &str,
        table: &str,
        key: &DecoratedKey,
        row: &Row,
        timestamp: i64,
    ) -> CommitLogPosition {
        let payload_size = Mutation::serialized_size_for_single_row(keyspace, table, key, row);
        let total_size = ENTRY_OVERHEAD + payload_size;
        let off = offset as usize;

        debug_assert!(
            off + total_size <= self.capacity,
            "write_single_row_entry: offset {off} + size {total_size} exceeds capacity {}",
            self.capacity
        );

        let buf = unsafe { &mut *self.buffer.get() };

        let entry_size_bytes = (payload_size as u32).to_be_bytes();
        buf[off..off + 4].copy_from_slice(&entry_size_bytes);

        let size_crc = crc32fast::hash(&entry_size_bytes);
        buf[off + 4..off + 8].copy_from_slice(&size_crc.to_be_bytes());

        let payload_start = off + 8;
        let payload_end = payload_start + payload_size;
        Mutation::serialize_single_row_into(
            mutation_id,
            keyspace,
            table,
            key,
            row,
            timestamp,
            &mut buf[payload_start..payload_end],
        );

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
        let sync_start = Instant::now();
        let _span = tracing::info_span!("commitlog.sync", segment_id = self.id,).entered();
        let mut phases = SyncPhaseDurations::default();
        // Snapshot position BEFORE waiting. This captures only data from
        // writers who have already allocated. New allocations after this
        // point will be flushed on the next call.
        let snapshot_pos = self.position.load(Ordering::Acquire) as usize;

        // Wait for all in-flight writers to complete their entries.
        // After this returns, buffer[0..snapshot_pos] is fully written.
        phases.wait_writers = self.wait_for_writers();
        observe_wait_writers(phases.wait_writers);

        // SAFETY: We only read buffer[0..snapshot_pos]. All bytes in this range
        // have been fully written — wait_for_writers() ensures no in-flight writes.
        let buf = unsafe { &*self.buffer.get() };

        // File I/O happens under the file_handle mutex to prevent concurrent
        // flushers from writing duplicate bytes. `release_buffer()` also
        // takes this lock, so once we hold it the buffer's (ptr, len, cap)
        // is stable for the duration of this flush.
        let lock_start = Instant::now();
        let mut handle = self.file_handle.lock();
        phases.file_lock_wait = lock_start.elapsed();
        observe_phase(
            &SYNC_FILE_LOCK_WAIT_MICROS_TOTAL,
            &SYNC_FILE_LOCK_WAIT_MICROS_MAX,
            phases.file_lock_wait,
        );
        let last_flushed = self.last_flushed.load(Ordering::Acquire) as usize;
        let current_pos = snapshot_pos;

        // If `release_buffer()` ran before us, the data is already on disk
        // (force_rotate calls flush_to_disk first) and the buffer is empty.
        // Don't try to slice into a zero-length Vec — short-circuit. Without
        // this guard the test
        // `concurrent_writers_with_flushes_preserve_lww` flakes with
        // `range end index N out of range for slice of length 0`.
        if buf.len() < current_pos {
            return Ok(());
        }

        match handle.as_mut() {
            None => {
                // First flush: create file, write from beginning (header + sync marker + entries).
                if let Some(parent) = self.path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let created = !self.path.exists();
                let f = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&self.path)?;
                *handle = Some(f);
                let file = handle.as_mut().unwrap();
                use std::io::Write;
                let written = current_pos;
                let write_start = Instant::now();
                file.write_all(&buf[..current_pos])?;
                phases.write = write_start.elapsed();
                observe_phase(
                    &SYNC_WRITE_MICROS_TOTAL,
                    &SYNC_WRITE_MICROS_MAX,
                    phases.write,
                );
                let sync_data_start = Instant::now();
                sync_commitlog_file(file)?;
                phases.sync_data = sync_data_start.elapsed();
                observe_phase(
                    &SYNC_DATA_MICROS_TOTAL,
                    &SYNC_DATA_MICROS_MAX,
                    phases.sync_data,
                );
                if created {
                    let parent_sync_start = Instant::now();
                    sync_parent_dir(&self.path)?;
                    phases.parent_dir_sync = parent_sync_start.elapsed();
                    observe_phase(
                        &SYNC_PARENT_DIR_MICROS_TOTAL,
                        &SYNC_PARENT_DIR_MICROS_MAX,
                        phases.parent_dir_sync,
                    );
                }
                INCREMENTAL_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                INCREMENTAL_FLUSH_BYTES_TOTAL.fetch_add(written as u64, Ordering::Relaxed);
                SYNCS_TOTAL.fetch_add(1, Ordering::Relaxed);
                let total = sync_start.elapsed();
                observe_sync(total);
                maybe_warn_slow_sync(self.id, written, total, &phases);
                self.last_flushed
                    .store(current_pos as u64, Ordering::Release);
            }
            Some(file) if current_pos > last_flushed => {
                // Incremental: append only new bytes.
                use std::io::Write;
                let written = current_pos - last_flushed;
                let write_start = Instant::now();
                file.write_all(&buf[last_flushed..current_pos])?;
                phases.write = write_start.elapsed();
                observe_phase(
                    &SYNC_WRITE_MICROS_TOTAL,
                    &SYNC_WRITE_MICROS_MAX,
                    phases.write,
                );
                let sync_data_start = Instant::now();
                sync_commitlog_file(file)?;
                phases.sync_data = sync_data_start.elapsed();
                observe_phase(
                    &SYNC_DATA_MICROS_TOTAL,
                    &SYNC_DATA_MICROS_MAX,
                    phases.sync_data,
                );
                INCREMENTAL_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                INCREMENTAL_FLUSH_BYTES_TOTAL.fetch_add(written as u64, Ordering::Relaxed);
                SYNCS_TOTAL.fetch_add(1, Ordering::Relaxed);
                let total = sync_start.elapsed();
                observe_sync(total);
                maybe_warn_slow_sync(self.id, written, total, &phases);
                self.last_flushed
                    .store(current_pos as u64, Ordering::Release);
            }
            Some(_) => {
                // Nothing new to flush.
            }
        }

        Ok(())
    }

    /// Rewrites the entire segment file from scratch.
    ///
    /// Unlike `flush_to_disk` which only appends new bytes, this writes
    /// `buf[0..position]` to the file. Required when sync markers at earlier
    /// offsets have been updated after an incremental flush (e.g., during
    /// `force_sync` for catch-up replay).
    pub fn force_full_flush(&self) -> ferrosa_common::Result<()> {
        let sync_start = Instant::now();
        let mut phases = SyncPhaseDurations::default();
        let snapshot_pos = self.position.load(Ordering::Acquire) as usize;
        phases.wait_writers = self.wait_for_writers();
        observe_wait_writers(phases.wait_writers);

        let buf = unsafe { &*self.buffer.get() };
        let lock_start = Instant::now();
        let mut handle = self.file_handle.lock();
        phases.file_lock_wait = lock_start.elapsed();
        observe_phase(
            &SYNC_FILE_LOCK_WAIT_MICROS_TOTAL,
            &SYNC_FILE_LOCK_WAIT_MICROS_MAX,
            phases.file_lock_wait,
        );

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let created = !self.path.exists();
        let f = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.path)?;
        *handle = Some(f);
        let file = handle.as_mut().unwrap();
        use std::io::Write;
        let write_start = Instant::now();
        file.write_all(&buf[..snapshot_pos])?;
        phases.write = write_start.elapsed();
        observe_phase(
            &SYNC_WRITE_MICROS_TOTAL,
            &SYNC_WRITE_MICROS_MAX,
            phases.write,
        );
        let sync_data_start = Instant::now();
        sync_commitlog_file(file)?;
        phases.sync_data = sync_data_start.elapsed();
        observe_phase(
            &SYNC_DATA_MICROS_TOTAL,
            &SYNC_DATA_MICROS_MAX,
            phases.sync_data,
        );
        if created {
            let parent_sync_start = Instant::now();
            sync_parent_dir(&self.path)?;
            phases.parent_dir_sync = parent_sync_start.elapsed();
            observe_phase(
                &SYNC_PARENT_DIR_MICROS_TOTAL,
                &SYNC_PARENT_DIR_MICROS_MAX,
                phases.parent_dir_sync,
            );
        }
        FULL_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
        FULL_FLUSH_BYTES_TOTAL.fetch_add(snapshot_pos as u64, Ordering::Relaxed);
        SYNCS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let total = sync_start.elapsed();
        observe_sync(total);
        maybe_warn_slow_sync(self.id, snapshot_pos, total, &phases);
        self.last_flushed
            .store(snapshot_pos as u64, Ordering::Release);

        Ok(())
    }

    /// Closes the persistent file handle, releasing the file descriptor.
    ///
    /// Called after the segment's final flush when it moves to `closed_segments`.
    /// The segment data is fully on disk, so the handle is no longer needed.
    /// This prevents file descriptor leaks when many segments accumulate
    /// waiting for `discard_completed()` to clean them up.
    pub fn close_file_handle(&self) {
        let mut handle = self.file_handle.lock();
        *handle = None;
    }

    /// Marks a table as having dirty (unflushed) data in this segment.
    ///
    /// Lock-free in steady state: hits `DashMap::get` (per-shard read
    /// lock) and `AtomicU64::fetch_max`. Only the first write per
    /// (segment, table) pair pays the shard-write-lock cost of insert.
    pub fn mark_table_dirty(&self, table_id: &TableId, position: CommitLogPosition) {
        debug_assert_eq!(
            position.segment_id, self.id,
            "mark_table_dirty called with foreign segment_id"
        );
        if let Some(existing) = self.dirty_tables.get(table_id) {
            existing.fetch_max(position.offset, Ordering::Relaxed);
            return;
        }
        // First-time insert for this table — pays a one-shot shard write lock.
        // `or_insert_with` is atomic-enough: if a racing thread inserted
        // between our `get` and `entry`, we still `fetch_max` the existing
        // entry, never overwriting a higher offset.
        let entry = self
            .dirty_tables
            .entry(table_id.clone())
            .or_insert_with(|| AtomicU64::new(0));
        entry.fetch_max(position.offset, Ordering::Relaxed);
    }

    /// Removes a table's entry if its recorded offset is ≤ `up_to_offset`.
    /// Returns whether the segment is now empty of dirty tables.
    pub fn discard_table_if_dominated(&self, table_id: &TableId, up_to_offset: u64) -> bool {
        if let Some(entry) = self.dirty_tables.get(table_id) {
            if entry.load(Ordering::Relaxed) <= up_to_offset {
                drop(entry);
                self.dirty_tables.remove(table_id);
            }
        }
        self.dirty_tables.is_empty()
    }

    /// Unconditionally drop the table from the dirty set. Used by the
    /// commit-log discard path when the discard threshold's `segment_id`
    /// is strictly greater than this segment's id — meaning every offset
    /// here is dominated.
    pub fn discard_table_unconditional(&self, table_id: &TableId) -> bool {
        self.dirty_tables.remove(table_id);
        self.dirty_tables.is_empty()
    }

    /// Returns `true` when this segment has no dirty tables left.
    pub fn is_dirty_empty(&self) -> bool {
        self.dirty_tables.is_empty()
    }

    /// Returns a snapshot of (table, latest-position) pairs currently
    /// recorded as dirty in this segment.
    pub fn dirty_table_positions(&self) -> Vec<(TableId, CommitLogPosition)> {
        self.dirty_tables
            .iter()
            .map(|kv| {
                let offset = kv.value().load(Ordering::Relaxed);
                (
                    kv.key().clone(),
                    CommitLogPosition {
                        segment_id: self.id,
                        offset,
                    },
                )
            })
            .collect()
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

    /// The largest single entry (payload + overhead) that can ever fit in a
    /// fresh segment of `capacity` bytes.
    ///
    /// A fresh segment starts at [`INITIAL_POSITION`] (header + first sync
    /// marker); a closing sync marker is also reserved so `force_sync` can
    /// always append its EOF marker. Returns 0 when the capacity cannot hold
    /// even an empty entry — callers must treat that as "no entry fits".
    pub fn max_entry_size(capacity: usize) -> usize {
        let reserved = INITIAL_POSITION as usize + SYNC_MARKER_SIZE;
        capacity.saturating_sub(reserved)
    }

    /// Computes the total entry size for a single-row borrowed mutation.
    pub fn entry_total_size_single_row(
        keyspace: &str,
        table: &str,
        key: &DecoratedKey,
        row: &Row,
    ) -> usize {
        ENTRY_OVERHEAD + Mutation::serialized_size_for_single_row(keyspace, table, key, row)
    }

    /// Returns the current heap size of the buffer in bytes.
    ///
    /// Returns the segment capacity before [`release_buffer()`](Self::release_buffer)
    /// is called, and 0 after.  Used by [`CommitLog::closed_segments_total_bytes()`]
    /// to confirm that closed segments are not holding memory.
    pub fn buffer_bytes(&self) -> usize {
        // SAFETY: We only read the Vec's length field — no slice access.
        unsafe { &*self.buffer.get() }.len()
    }

    /// Drops the in-memory write buffer, retaining only segment metadata.
    ///
    /// Called by [`CommitLog::force_rotate()`] immediately after the segment
    /// has been fsynced to disk and moved to `closed_segments`.  Replacing
    /// the 32 MB buffer with an empty `Vec` caps closed-segment memory at
    /// ~200 bytes of metadata per segment regardless of GC lag.
    ///
    /// # Precondition
    ///
    /// Must only be called after:
    /// 1. [`flush_to_disk()`](Self::flush_to_disk) (all data is on disk), and
    /// 2. The segment is no longer the active segment (no new writes possible).
    ///
    /// Writing to or re-flushing the segment after this call is safe — the
    /// position has not advanced, so incremental flush finds nothing new to write.
    pub fn release_buffer(&self) {
        // Lock against concurrent `flush_to_disk()`. A periodic flush thread
        // can have captured `snapshot_pos` and a `&Vec<u8>` reference
        // BEFORE this method runs; if we replace the Vec without
        // serialising on the same lock, that thread reads a torn (ptr, len,
        // cap) tuple — observed in production as
        // `range end index N out of range for slice of length 0` at the
        // file.write_all(&buf[..current_pos]) site (see
        // tests::concurrent_writers_with_flushes_preserve_lww flake).
        //
        // SAFETY: With the lock held, no other thread is inside
        // `flush_to_disk()` (which also acquires `file_handle`). Writers
        // cannot reach this segment via `ArcSwap` once `force_rotate()` has
        // swapped in the new active segment, so no append path will read
        // the buffer either.
        let _handle = self.file_handle.lock();
        let buf = unsafe { &mut *self.buffer.get() };
        *buf = Vec::new();
        // Make any post-release flush a no-op: by setting last_flushed to
        // the current position, the `Some(_)` "nothing new" arm of
        // `flush_to_disk` is taken even if the file handle gets re-opened
        // somehow. The `None` arm is additionally guarded by a buffer
        // bounds check below.
        let pos = self.position.load(Ordering::Acquire);
        self.last_flushed.store(pos, Ordering::Release);
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
            mutation_id: [0x14u8; 16],
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

        let dirty = segment.dirty_table_positions();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0], (table.clone(), pos2));
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

    // -----------------------------------------------------------------------
    // Buffer release tests (P0 OOM fix)
    // -----------------------------------------------------------------------

    #[test]
    fn release_buffer_frees_memory() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        // Write an entry and flush so the segment has real data on disk.
        let m = simple_mutation();
        let total_size = Segment::entry_total_size(&m);
        let offset = segment.allocate(total_size).unwrap();
        segment.write_entry(offset, &m);
        segment.flush_to_disk().unwrap();

        // Buffer is live before release.
        assert_eq!(segment.buffer_bytes(), 4096);

        segment.release_buffer();

        // Buffer is gone after release; only metadata remains.
        assert_eq!(segment.buffer_bytes(), 0);
    }

    #[test]
    fn release_buffer_preserves_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(42, 4096, dir.path());
        let table = TableId::new("ks", "tbl");
        let pos = CommitLogPosition {
            segment_id: 42,
            offset: 25,
        };
        segment.mark_table_dirty(&table, pos);

        segment.release_buffer();

        // Metadata fields survive the release.
        assert_eq!(segment.id, 42);
        assert_eq!(
            segment.path().file_name().unwrap().to_str().unwrap(),
            "commitlog-42.log"
        );
        let dirty = segment.dirty_table_positions();
        assert_eq!(dirty.len(), 1, "dirty_tables must survive release_buffer");
        assert_eq!(dirty[0], (table.clone(), pos));
    }

    #[test]
    fn buffer_bytes_returns_capacity_before_release() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 8192, dir.path());
        assert_eq!(segment.buffer_bytes(), 8192);
    }

    /// Regression: a `flush_to_disk()` racing with `release_buffer()`
    /// must not panic on `&buf[..current_pos]` when the buffer has been
    /// emptied. Pre-fix the periodic flusher would observe a torn
    /// `(ptr, len, cap)` tuple — `len = 0` while the captured
    /// `current_pos` was still > 0 — and panic with
    /// `range end index N out of range for slice of length 0`.
    /// The fix serialises `release_buffer` against the file_handle
    /// lock and adds a `buf.len() < current_pos` short-circuit at the
    /// top of `flush_to_disk`.
    #[test]
    fn flush_after_release_buffer_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());
        let m = simple_mutation();
        let total_size = Segment::entry_total_size(&m);
        let offset = segment
            .allocate(total_size)
            .expect("allocate within capacity");
        segment.write_entry(offset, &m);

        // Mimic the force_rotate ordering: flush, close, release.
        segment.flush_to_disk().expect("first flush ok");
        segment.close_file_handle();
        segment.release_buffer();

        // A late periodic flush firing now must not panic; it is a no-op.
        segment
            .flush_to_disk()
            .expect("post-release flush must succeed (no-op)");
    }

    /// Stress variant of the above: many concurrent flushes hammering
    /// a segment that gets `release_buffer`'d mid-stream. Without the
    /// lock fix this panics within the first few iterations on macOS
    /// arm64 in debug.
    #[test]
    fn flush_to_disk_race_with_release_buffer_does_not_panic() {
        for _ in 0..16 {
            let dir = tempfile::tempdir().unwrap();
            let segment = Arc::new(Segment::new(1, 8192, dir.path()));
            let m = simple_mutation();
            let total_size = Segment::entry_total_size(&m);
            for _ in 0..32 {
                if let Some(off) = segment.allocate(total_size) {
                    segment.write_entry(off, &m);
                } else {
                    break;
                }
            }

            let mut handles = Vec::new();
            for _ in 0..4 {
                let s = Arc::clone(&segment);
                handles.push(std::thread::spawn(move || {
                    for _ in 0..200 {
                        let _ = s.flush_to_disk();
                    }
                }));
            }
            // Mimic force_rotate: flush, close, release, while the
            // periodic-flush threads above may still be in flight.
            std::thread::sleep(std::time::Duration::from_micros(50));
            segment.flush_to_disk().expect("rotate flush ok");
            segment.close_file_handle();
            segment.release_buffer();

            for h in handles {
                h.join().expect("flush thread must not panic");
            }
        }
    }
}
