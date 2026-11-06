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
//! # Sync Marker Chain: reserve-ahead, write-once
//!
//! A segment is a chain of sections. Each section starts with a head marker
//! whose `next_marker_offset` names the start of the following section (the
//! next head), or is 0 when the section runs to end of file.
//!
//! Every flush closes the current section, Cassandra-style:
//!
//! 1. Under `marker_link`, reserve the NEXT head slot `S` at the current
//!    allocation position.
//! 2. Write the CURRENT head (reserved by the previous flush, or the initial
//!    slot at `HEADER_SIZE`, and never yet on disk) with `next = S`.
//! 3. Write `[last_flushed, S)` to the file and sync it. `S` itself is not
//!    written; it becomes the new `last_flushed`.
//!
//! So each head marker is written exactly once, at or above the flush
//! watermark, and **no durable byte is ever rewritten**. A reader tailing the
//! open segment never sees a marker it already read change. The file of an
//! open segment always ends at an unwritten head slot; a reader stops there
//! (the marker lies beyond EOF) and resumes once a later flush writes it.
//!
//! When the segment cannot hold another marker (full) or is sealed for
//! rotation, the final head is written with `next = 0` (to EOF), the chain is
//! closed, and the segment accepts no more appends.
//!
//! The on-disk encoding did not change: segments written by older binaries,
//! which patched the previous marker in place and ended in a `next = 0`
//! marker, are read by the same [`SegmentReader`](super::reader::SegmentReader),
//! and an older binary reads this writer's segments.
//!
//! Compatibility is only ever a READ question, because no binary — old or new
//! — appends to a segment it did not create in the same process: segment files
//! are always created fresh (`truncate(true)`), and startup replays existing
//! segments, re-logs what it keeps into new ones, and deletes the old files. So
//! an older binary can never patch a marker in a segment this writer made.
//!
//! A torn head (bad CRC) ends the chain: the sections before it replay, and
//! the bytes after it are reported as rejected so the segment is retained
//! (`a_torn_section_head_is_rejected_not_followed`).
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

use super::config::{CommitLogPosition, CommitLogSyncMode, TableId};
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

/// Syncs issued, indexed by [`mode_index`]. Exported per mode so a dashboard
/// shows which call the node is really making, not only which it was told to.
static SYNCS_BY_MODE: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

fn mode_index(mode: CommitLogSyncMode) -> usize {
    match mode {
        CommitLogSyncMode::Full => 0,
        CommitLogSyncMode::Barrier => 1,
        CommitLogSyncMode::Fsync => 2,
    }
}

/// Commit-log and directory syncs issued in `mode` since the process started.
pub(crate) fn syncs_issued(mode: CommitLogSyncMode) -> u64 {
    SYNCS_BY_MODE[mode_index(mode)].load(Ordering::Relaxed)
}

/// An interrupted sync is reissued, as `std` does for `sync_all`. The bound
/// only stops a signal storm from spinning forever; it then fails loudly.
#[cfg(target_vendor = "apple")]
const MAX_EINTR_RETRIES: usize = 64;

#[cfg(target_vendor = "apple")]
fn apple_sync(file: &fs::File, mode: CommitLogSyncMode) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    for _ in 0..MAX_EINTR_RETRIES {
        // SAFETY: `fd` is owned by `file`, which outlives the call; none of
        // these calls reads or writes memory through a pointer.
        let rc = unsafe {
            match mode {
                CommitLogSyncMode::Full => libc::fcntl(fd, libc::F_FULLFSYNC),
                CommitLogSyncMode::Barrier => libc::fcntl(fd, libc::F_BARRIERFSYNC),
                // `File::sync_data` would be F_FULLFSYNC on Apple targets.
                CommitLogSyncMode::Fsync => libc::fsync(fd),
            }
        };
        if rc != -1 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
    Err(std::io::Error::other(format!(
        "commit-log {} sync interrupted {MAX_EINTR_RETRIES} times in a row",
        mode.name()
    )))
}

/// Makes `file` durable in `mode`. On non-Apple targets every mode is
/// `fdatasync`, which already flushes the device cache there.
fn sync_file(file: &fs::File, mode: CommitLogSyncMode) -> ferrosa_common::Result<()> {
    #[cfg(target_vendor = "apple")]
    apple_sync(file, mode)?;
    #[cfg(not(target_vendor = "apple"))]
    file.sync_data()?;
    SYNCS_BY_MODE[mode_index(mode)].fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// Makes a new segment's directory entry durable in `mode`.
///
/// A failure is returned, never retried another way. This used to fall back
/// from `F_FULLFSYNC` to `sync_all` on error, but `sync_all` is itself
/// `F_FULLFSYNC` on Apple targets: the fallback reissued the same call and
/// discarded the first error without a trace.
fn sync_parent_dir(path: &Path, mode: CommitLogSyncMode) -> ferrosa_common::Result<()> {
    if let Some(parent) = path.parent() {
        let dir = fs::File::open(parent)?;
        sync_file(&dir, mode)?;
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
    sealed: std::sync::atomic::AtomicBool,

    /// Persistent file handle for incremental flush. Opened on first flush,
    /// reused for subsequent appends.
    file_handle: Mutex<Option<std::fs::File>>,

    /// Byte position up to which data has been flushed AND synced. Every byte
    /// below it is durable and is never written again. Only advanced under
    /// `file_handle`, after the sync succeeds. While the chain is open it is
    /// always the offset of a reserved head slot that is not yet on disk.
    last_flushed: AtomicU64,

    /// Offset of the CURRENT section's head marker slot: reserved, not yet
    /// written to the buffer with its final value, never yet on disk. The
    /// next flush writes it with `next` = the slot it reserves.
    ///
    /// Only ever read or written under `marker_link`. It stays an atomic so
    /// the field carries no interior-mutability ceremony of its own; the lock
    /// is what makes the read-modify-write a single step.
    head_marker_offset: AtomicU64,

    /// Set (under `marker_link`) once the final head has been written with
    /// `next = 0`. After that no marker is ever written again and flushes
    /// carry the segment up to its allocation position.
    chain_closed: std::sync::atomic::AtomicBool,

    /// Serializes slot reservation together with writing the head that points
    /// at the reserved slot.
    ///
    /// The chain must come out in ALLOCATION order. Reserving the slot and
    /// linking the head to it under one lock makes "reserved earlier" and
    /// "sits earlier in the file" the same fact, so every `next` points
    /// forward to the very next head.
    ///
    /// It guards the eight-byte buffer writes only, not the file I/O, so
    /// concurrent flushers still sync in parallel.
    marker_link: Mutex<()>,

    /// The system call every sync of this segment issues.
    sync_mode: CommitLogSyncMode,
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
    /// immediately and the 8-byte slot at `HEADER_SIZE` is reserved as the
    /// first section's head; that marker is written by the first flush that
    /// closes a non-empty section. Syncs use the default, power-loss durable
    /// [`CommitLogSyncMode::Full`].
    pub fn new(id: u64, size: usize, dir: &Path) -> Self {
        Self::with_sync_mode(id, size, dir, CommitLogSyncMode::default())
    }

    /// Like [`new`](Self::new), syncing in `sync_mode`.
    pub fn with_sync_mode(id: u64, size: usize, dir: &Path, sync_mode: CommitLogSyncMode) -> Self {
        assert!(
            size >= INITIAL_POSITION as usize + ENTRY_OVERHEAD,
            "segment size too small: need at least {} bytes",
            INITIAL_POSITION as usize + ENTRY_OVERHEAD
        );

        let mut buffer = vec![0u8; size];

        // Write the segment header (17 bytes).
        let descriptor = SegmentDescriptor::new(id);
        descriptor.write_to(&mut buffer[..HEADER_SIZE]);
        // The slot at HEADER_SIZE stays zero: it is the first section's head,
        // written once by the flush that closes that section.

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
            sealed: std::sync::atomic::AtomicBool::new(false),
            file_handle: Mutex::new(None),
            last_flushed: AtomicU64::new(0),
            head_marker_offset: AtomicU64::new(HEADER_SIZE as u64),
            chain_closed: std::sync::atomic::AtomicBool::new(false),
            marker_link: Mutex::new(()),
            sync_mode,
        }
    }

    /// The system call this segment's syncs issue.
    pub fn sync_mode(&self) -> CommitLogSyncMode {
        self.sync_mode
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
        self.in_flight_writers.fetch_add(1, Ordering::SeqCst);
        if self.sealed.load(Ordering::SeqCst) {
            self.in_flight_writers.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        match self.allocate(entry_total_size) {
            Some(offset) => Some(offset),
            None => {
                // Allocation failed (segment full) — undo the increment.
                self.in_flight_writers.fetch_sub(1, Ordering::SeqCst);
                None
            }
        }
    }

    /// Prevent delayed holders of the old active Arc from allocating after
    /// rotation, then drain every admitted writer before the final snapshot.
    pub(crate) fn seal_writes(&self) {
        self.sealed.store(true, Ordering::SeqCst);
        while self.in_flight_writers.load(Ordering::SeqCst) != 0 {
            std::thread::yield_now();
        }
    }

    /// Decrements the in-flight writer count. Called after `write_entry()`
    /// completes. Paired with the increment in [`allocate()`](Self::allocate).
    pub fn writer_done(&self) {
        self.in_flight_writers.fetch_sub(1, Ordering::SeqCst);
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

    /// Closes the current section and returns the offset the flush must write
    /// up to (exclusive). Every byte below the returned offset is either
    /// already complete or belongs to a writer counted in `in_flight_writers`,
    /// so the caller must still `wait_for_writers` before reading the buffer.
    ///
    /// Three cases, all decided under `marker_link`:
    ///
    /// * **Chain closed** (final head already written with `next = 0`): the
    ///   last section runs to EOF, so the flush covers the whole allocation
    ///   position. No marker is written.
    /// * **Empty section** (nothing allocated since the head slot): there is
    ///   nothing to close. The flush end is the head slot itself, which leaves
    ///   any earlier, not yet durable section (a failed flush) to be written.
    ///   Without this, every idle periodic tick would burn eight bytes and a
    ///   sync.
    /// * **Otherwise** reserve the next slot `S` and write the current head
    ///   with `next = S`. The flush end is `S`: up to, not including, the new
    ///   slot, whose bytes are only written by the flush that closes ITS
    ///   section. If no slot fits, or the segment is sealed for rotation, the
    ///   head is written with `next = 0`, the chain is closed and the segment
    ///   is sealed so it never takes another append.
    ///
    /// Ordering. The head being written lies below `S`, and any flusher whose
    /// end exceeds the head obtained that end from this function after we
    /// release `marker_link`, so it observes the finished head bytes. Flushers
    /// holding an earlier end stop at or before the head and never read it.
    /// The new slot is never pointed at before this same flush writes it as
    /// the end of its range. The slot reservation itself goes through
    /// `allocate_and_begin_write`, so it counts as an in-flight writer while
    /// the head is written and also respects `sealed`.
    fn close_section(&self) -> ferrosa_common::Result<u64> {
        let _linking = self.marker_link.lock();
        if self.chain_closed.load(Ordering::Acquire) {
            return Ok(self.position.load(Ordering::Acquire));
        }
        let head = self.head_marker_offset.load(Ordering::Acquire);
        let head_end = head + SYNC_MARKER_SIZE as u64;
        let position = self.position.load(Ordering::Acquire);
        debug_assert!(
            position >= head_end,
            "allocation position {position} below the head slot end {head_end}"
        );
        if position == head_end {
            return Ok(head);
        }
        match self.allocate_and_begin_write(SYNC_MARKER_SIZE) {
            Some(slot) => {
                let linked = self.write_head_marker(head, slot);
                if linked.is_ok() {
                    self.head_marker_offset.store(slot, Ordering::Release);
                }
                self.writer_done();
                linked.map(|()| slot)
            }
            None => {
                // Full or sealed. A full segment cannot take an entry either
                // (every entry is larger than a marker); sealing says so
                // explicitly so no later append can land past the final head.
                self.sealed.store(true, Ordering::SeqCst);
                self.write_head_marker(head, 0)?;
                self.chain_closed.store(true, Ordering::Release);
                Ok(self.position.load(Ordering::Acquire))
            }
        }
    }

    /// Writes the head marker at `head` into the buffer, linking it to `next`
    /// (0 = runs to EOF). The caller holds `marker_link`.
    fn write_head_marker(&self, head: u64, next: u64) -> ferrosa_common::Result<()> {
        // SAFETY: the head slot was reserved by allocation (or is the initial
        // slot) and only `close_section`, under `marker_link`, writes it.
        // `release_buffer` also takes `marker_link`, so the Vec is stable.
        let buf = unsafe { &mut *self.buffer.get() };
        let next = u32::try_from(next).map_err(|_| {
            ferrosa_common::Error::InvalidData(format!(
                "commit-log segment {}: next marker offset {next} exceeds u32",
                self.id
            ))
        })?;
        let end = head as usize + SYNC_MARKER_SIZE;
        if end > buf.len() {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "commit-log segment {}: head marker {head}..{end} lies outside the \
                 {}-byte buffer (released before its chain was closed?)",
                self.id,
                buf.len()
            )));
        }
        Self::write_sync_marker_to_buffer(buf, head as usize, self.id, next);
        Ok(())
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

    /// Hold the real flush/file mutex while a deterministic cross-crate test runs.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn with_flush_held_for_test(&self, hold: impl FnOnce()) {
        let _file = self.file_handle.lock();
        hold();
    }

    /// Closes the current section and makes everything before the next head
    /// slot durable. This is the ONLY way bytes reach the segment file.
    ///
    /// Writes `[last_flushed, end)` at its own offset and syncs it, where
    /// `end` comes from [`close_section`](Self::close_section). Bytes below
    /// `last_flushed` are never written again. Waits for all in-flight
    /// writers before reading the buffer.
    pub fn flush_to_disk(&self) -> ferrosa_common::Result<()> {
        let end = self.close_section()?;
        self.write_through(end as usize)
    }

    fn write_through(&self, end: usize) -> ferrosa_common::Result<()> {
        let sync_start = Instant::now();
        let _span = tracing::info_span!("commitlog.sync", segment_id = self.id,).entered();
        // Every byte below `end` was allocated before `end` was chosen, so
        // its writer already counts in `in_flight_writers`. After this,
        // buffer[..end] is fully written.
        let mut phases = SyncPhaseDurations {
            wait_writers: self.wait_for_writers(),
            ..SyncPhaseDurations::default()
        };
        observe_wait_writers(phases.wait_writers);

        // File I/O happens under the file_handle mutex so concurrent flushers
        // never write the same range twice. `release_buffer()` also takes
        // this lock, so once we hold it the buffer's (ptr, len, cap) is
        // stable for the duration of this flush.
        let lock_start = Instant::now();
        let mut handle = self.file_handle.lock();
        phases.file_lock_wait = lock_start.elapsed();
        observe_phase(
            &SYNC_FILE_LOCK_WAIT_MICROS_TOTAL,
            &SYNC_FILE_LOCK_WAIT_MICROS_MAX,
            phases.file_lock_wait,
        );
        let start = self.last_flushed.load(Ordering::Acquire) as usize;
        if end <= start {
            // Already durable: a flusher with a later end wrote AND synced
            // this range under this lock before advancing the watermark (or
            // the segment was closed and released after its final flush).
            // `end` is never 0 (the header precedes the first head slot), so
            // a never-flushed segment (`start == 0`) always gets here with
            // something to write.
            return Ok(());
        }

        // SAFETY: we only read buffer[start..end]; wait_for_writers() above
        // ensured every writer of that range finished.
        let buf = unsafe { &*self.buffer.get() };
        if buf.len() < end {
            return Err(ferrosa_common::Error::InvalidData(format!(
                "commit-log segment {}: flush to {end} past the {}-byte buffer with only \
                 {start} bytes durable",
                self.id,
                buf.len()
            )));
        }

        let created = handle.is_none() && !self.path.exists();
        if handle.is_none() {
            if let Some(parent) = self.path.parent() {
                fs::create_dir_all(parent)?;
            }
            // Truncate only a file this segment has never made durable.
            let f = fs::OpenOptions::new()
                .create(true)
                .truncate(start == 0)
                .write(true)
                .open(&self.path)?;
            *handle = Some(f);
        }
        let Some(file) = handle.as_mut() else {
            unreachable!("file handle opened above");
        };

        // Positional write: the range lands at its own offset even if an
        // earlier attempt failed partway, so a retry can never misplace it.
        use std::os::unix::fs::FileExt;
        let write_start = Instant::now();
        file.write_all_at(&buf[start..end], start as u64)?;
        phases.write = write_start.elapsed();
        observe_phase(
            &SYNC_WRITE_MICROS_TOTAL,
            &SYNC_WRITE_MICROS_MAX,
            phases.write,
        );
        let sync_data_start = Instant::now();
        sync_file(file, self.sync_mode)?;
        phases.sync_data = sync_data_start.elapsed();
        observe_phase(
            &SYNC_DATA_MICROS_TOTAL,
            &SYNC_DATA_MICROS_MAX,
            phases.sync_data,
        );
        if created {
            let parent_sync_start = Instant::now();
            sync_parent_dir(&self.path, self.sync_mode)?;
            phases.parent_dir_sync = parent_sync_start.elapsed();
            observe_phase(
                &SYNC_PARENT_DIR_MICROS_TOTAL,
                &SYNC_PARENT_DIR_MICROS_MAX,
                phases.parent_dir_sync,
            );
        }
        let written = end - start;
        INCREMENTAL_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
        INCREMENTAL_FLUSH_BYTES_TOTAL.fetch_add(written as u64, Ordering::Relaxed);
        SYNCS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let total = sync_start.elapsed();
        observe_sync(total);
        maybe_warn_slow_sync(self.id, written, total, &phases);
        // Only now is [start, end) durable.
        self.last_flushed.store(end as u64, Ordering::Release);
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
    /// A fresh segment starts at [`INITIAL_POSITION`] (header + first head
    /// slot). One more marker slot is reserved on top. Reserve-ahead does not
    /// need it for correctness (a segment that cannot fit the next head closes
    /// its final section with `next = 0`), but it keeps the limit unchanged
    /// for callers that pre-flight batches against it. Returns 0 when the
    /// capacity cannot hold even an empty entry — callers must treat that as
    /// "no entry fits".
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
    /// is called, and 0 after.  Used by [`crate::commitlog::CommitLog::closed_segments_total_bytes`]
    /// to confirm that closed segments are not holding memory.
    pub fn buffer_bytes(&self) -> usize {
        // SAFETY: We only read the Vec's length field — no slice access.
        unsafe { &*self.buffer.get() }.len()
    }

    /// Drops the in-memory write buffer, retaining only segment metadata.
    ///
    /// Called by [`crate::commitlog::CommitLog::force_rotate`] immediately after the segment
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
        //
        // `marker_link` is taken too, because `close_section` writes head
        // markers into the buffer under it. Lock order is marker_link then
        // file_handle; flushers never hold both at once.
        //
        // `last_flushed` is left alone: it must never claim bytes durable
        // that are not. After the final flush the watermark already covers
        // everything a later flush could ask for (the chain is closed, or the
        // last section is empty), so a post-release flush is a no-op; one that
        // is not finds the buffer gone and fails loudly.
        let _linking = self.marker_link.lock();
        let _handle = self.file_handle.lock();
        let buf = unsafe { &mut *self.buffer.get() };
        *buf = Vec::new();
    }

    /// Offset up to which this segment's bytes are durable on disk.
    pub(crate) fn durable_watermark(&self) -> u64 {
        self.last_flushed.load(Ordering::Acquire)
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

    /// Flushes one entry through `mode` and checks the bytes reached the file
    /// and that the sync was issued, and counted, under that mode.
    fn flush_one_entry_in(mode: CommitLogSyncMode) {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::with_sync_mode(1, 4096, dir.path(), mode);
        assert_eq!(segment.sync_mode(), mode);
        let m = simple_mutation();
        let offset = segment.allocate(Segment::entry_total_size(&m)).unwrap();
        segment.write_entry(offset, &m);
        let before = syncs_issued(mode);

        segment.flush_to_disk().unwrap();

        assert!(syncs_issued(mode) > before, "{mode:?} sync was not counted");
        let on_disk = fs::read(segment.path()).unwrap();
        let buf = unsafe { &*segment.buffer.get() };
        // Everything up to the newly reserved (unwritten) head slot.
        let end = segment.current_position() as usize - SYNC_MARKER_SIZE;
        assert_eq!(segment.durable_watermark() as usize, end);
        assert_eq!(&on_disk[..], &buf[..end]);
    }

    #[test]
    fn a_full_mode_segment_flushes_and_counts_a_full_sync() {
        flush_one_entry_in(CommitLogSyncMode::Full);
    }

    #[test]
    fn a_barrier_mode_segment_flushes_and_counts_a_barrier_sync() {
        flush_one_entry_in(CommitLogSyncMode::Barrier);
    }

    #[test]
    fn an_fsync_mode_segment_flushes_and_counts_a_plain_fsync() {
        flush_one_entry_in(CommitLogSyncMode::Fsync);
    }

    #[test]
    fn a_segment_made_without_a_mode_uses_the_durable_default() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            Segment::new(1, 4096, dir.path()).sync_mode(),
            CommitLogSyncMode::Full
        );
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
        // The flush stops before the head slot it reserved for the next section.
        let expected_len = segment.current_position() as usize - SYNC_MARKER_SIZE;
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

    /// The marker bytes for `next` in segment `id`, as the reader checks them.
    fn marker_bytes(id: u64, next: u32) -> [u8; SYNC_MARKER_SIZE] {
        let mut out = [0u8; SYNC_MARKER_SIZE];
        Segment::write_sync_marker_to_buffer(&mut out, 0, id, next);
        out
    }

    fn mutation_n(n: i64) -> Mutation {
        Mutation {
            timestamp: n,
            ..simple_mutation()
        }
    }

    fn append(segment: &Segment, m: &Mutation) {
        let offset = segment
            .allocate_and_begin_write(Segment::entry_total_size(m))
            .expect("entry fits");
        segment.write_entry(offset, m);
        segment.writer_done();
    }

    fn replay(path: &Path) -> Vec<i64> {
        let mut reader = super::super::reader::SegmentReader::open(path).unwrap();
        let stamps = reader
            .read_all()
            .unwrap()
            .into_iter()
            .map(|(_, m)| m.timestamp)
            .collect();
        assert!(
            !reader.rejected_records(),
            "a cleanly written segment must not reject any bytes"
        );
        stamps
    }

    /// Two flushed sections, `[0, 1]` then `[2]`; returns the segment path and
    /// the offset of the second section's head marker.
    fn two_flushed_sections(dir: &Path) -> (std::path::PathBuf, u64) {
        let segment = Segment::new(7, 4096, dir);
        append(&segment, &mutation_n(0));
        append(&segment, &mutation_n(1));
        let second_head = segment.current_position();
        segment.flush_to_disk().unwrap();
        append(&segment, &mutation_n(2));
        segment.flush_to_disk().unwrap();
        assert_eq!(replay(segment.path()), vec![0, 1, 2]);
        (segment.path().to_path_buf(), second_head)
    }

    fn replay_with_rejection(path: &Path) -> (Vec<i64>, bool) {
        let mut reader = super::super::reader::SegmentReader::open(path).unwrap();
        let stamps = reader
            .read_all()
            .unwrap()
            .into_iter()
            .map(|(_, m)| m.timestamp)
            .collect();
        (stamps, reader.rejected_records())
    }

    /// PR #651 review: a reserve-ahead head whose CRC is torn must not be
    /// followed. The sections before it replay; the bytes after it are
    /// REJECTED (reported, so replay keeps the segment), never read as entries.
    #[test]
    fn a_torn_section_head_is_rejected_not_followed() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let (path, second_head) = two_flushed_sections(dir.path());
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut crc_byte = [0u8; 1];
        file.read_exact_at(&mut crc_byte, second_head + 5).unwrap();
        file.write_all_at(&[crc_byte[0] ^ 0xFF], second_head + 5)
            .unwrap();
        drop(file);

        let (stamps, rejected) = replay_with_rejection(&path);
        assert_eq!(
            stamps,
            vec![0, 1],
            "only the section before the torn head replays"
        );
        assert!(
            rejected,
            "the entries behind a torn head must be reported as rejected, not dropped silently"
        );
    }

    /// A crash mid-way through writing a head (the 8-byte marker half on
    /// disk) leaves the earlier sections intact and nothing to reject.
    #[test]
    fn a_half_written_section_head_ends_replay_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let (path, second_head) = two_flushed_sections(dir.path());
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(second_head + (SYNC_MARKER_SIZE as u64) / 2)
            .unwrap();
        drop(file);

        assert_eq!(replay_with_rejection(&path), (vec![0, 1], false));
    }

    #[test]
    fn the_first_head_is_written_once_by_the_flush_that_closes_its_section() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());
        // Reserved, not yet written: nothing is on disk before the first flush.
        assert!(
            !segment.path().exists(),
            "the first head must not reach disk before the flush that closes its section"
        );

        append(&segment, &mutation_n(1));
        let next_slot = segment.current_position();
        segment.flush_to_disk().unwrap();

        let on_disk = fs::read(segment.path()).unwrap();
        assert_eq!(
            &on_disk[HEADER_SIZE..HEADER_SIZE + SYNC_MARKER_SIZE],
            &marker_bytes(1, next_slot as u32),
            "the first head must link forward to the slot this flush reserved"
        );
        assert_eq!(
            on_disk.len() as u64,
            next_slot,
            "the flush stops before the newly reserved, still unwritten slot"
        );
        assert_eq!(segment.durable_watermark(), next_slot);
    }

    #[test]
    fn an_idle_flush_reserves_no_marker_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());
        append(&segment, &mutation_n(1));
        segment.flush_to_disk().unwrap();
        let position = segment.current_position();
        let bytes = fs::read(segment.path()).unwrap();
        for _ in 0..5 {
            segment.flush_to_disk().unwrap();
        }
        assert_eq!(segment.current_position(), position);
        assert_eq!(fs::read(segment.path()).unwrap(), bytes);
    }

    /// A reader tailing the open segment sees every entry in order across
    /// many flushes, and no byte it has seen on disk ever changes, in
    /// particular no marker it already followed.
    #[test]
    fn a_tailing_reader_sees_every_entry_and_no_seen_byte_changes() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(9, 64 * 1024, dir.path());
        append(&segment, &mutation_n(0));
        segment.flush_to_disk().unwrap();

        let mut reader = super::super::reader::SegmentReader::open(segment.path()).unwrap();
        let mut seen: Vec<i64> = Vec::new();
        let mut seen_bytes = fs::read(segment.path()).unwrap();
        while let Some((_, m)) = reader.next_entry().unwrap() {
            seen.push(m.timestamp);
        }
        let mut expected = vec![0];
        assert_eq!(seen, expected);

        let mut next = 1;
        for round in 0..20 {
            // A varying number of entries per section, including none.
            for _ in 0..(round % 4) {
                append(&segment, &mutation_n(next));
                expected.push(next);
                next += 1;
            }
            segment.flush_to_disk().unwrap();
            reader.refresh().unwrap();
            while let Some((_, m)) = reader.next_entry().unwrap() {
                seen.push(m.timestamp);
            }
            assert_eq!(seen, expected, "round {round}");
            let now = fs::read(segment.path()).unwrap();
            assert_eq!(
                &now[..seen_bytes.len()],
                &seen_bytes[..],
                "round {round}: bytes the reader already saw changed"
            );
            seen_bytes = now;
        }
        assert!(!reader.rejected_records());
    }

    /// Concurrent writers and flushers: every entry is replayable, and the
    /// file only ever grows by appending.
    #[test]
    fn concurrent_writers_and_flushers_keep_every_entry_and_never_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Arc::new(Segment::new(3, 4 * 1024 * 1024, dir.path()));
        const WRITERS: i64 = 4;
        const PER_WRITER: i64 = 200;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flushers: Vec<_> = (0..3)
            .map(|_| {
                let seg = Arc::clone(&segment);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut durable: Vec<u8> = Vec::new();
                    while !stop.load(Ordering::Acquire) {
                        seg.flush_to_disk().expect("flush");
                        let now = fs::read(seg.path()).expect("read segment");
                        assert!(now.len() >= durable.len(), "file shrank");
                        assert!(
                            now[..durable.len()] == durable[..],
                            "a durable byte was rewritten"
                        );
                        durable = now;
                    }
                })
            })
            .collect();
        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let seg = Arc::clone(&segment);
                std::thread::spawn(move || {
                    for i in 0..PER_WRITER {
                        append(&seg, &mutation_n(w * PER_WRITER + i));
                        if i % 7 == 0 {
                            seg.flush_to_disk().expect("writer flush");
                        }
                    }
                })
            })
            .collect();
        for h in writers {
            h.join().unwrap();
        }
        stop.store(true, Ordering::Release);
        for h in flushers {
            h.join().unwrap();
        }
        segment.flush_to_disk().unwrap();

        let mut stamps = replay(segment.path());
        stamps.sort_unstable();
        assert_eq!(stamps, (0..WRITERS * PER_WRITER).collect::<Vec<_>>());
    }

    /// A segment written by the previous binary: the initial marker and every
    /// force_sync marker were patched in place to point forward, and the last
    /// one is an EOF marker (`next = 0`) followed by entries that a plain
    /// flush appended after it. The current reader must replay all of it.
    #[test]
    fn a_segment_in_the_old_patched_layout_still_replays() {
        let dir = tempfile::tempdir().unwrap();
        let id = 77;
        let path = dir.path().join(format!("commitlog-{id}.log"));
        let mut file = vec![0u8; HEADER_SIZE];
        SegmentDescriptor::new(id).write_to(&mut file[..HEADER_SIZE]);

        let encode = |m: &Mutation| {
            let payload_size = m.serialized_size();
            let mut entry = vec![0u8; ENTRY_OVERHEAD + payload_size];
            let size_bytes = (payload_size as u32).to_be_bytes();
            entry[..4].copy_from_slice(&size_bytes);
            entry[4..8].copy_from_slice(&crc32fast::hash(&size_bytes).to_be_bytes());
            m.serialize_into(&mut entry[8..8 + payload_size]);
            let crc = crc32fast::hash(&entry[8..8 + payload_size]);
            entry[8 + payload_size..].copy_from_slice(&crc.to_be_bytes());
            entry
        };

        // Old layout: [hdr][M0][e0 e1][M1][e2][M2 = EOF][e3 e4], where the
        // old force_sync patched M0 -> M1 and M1 -> M2 after the fact.
        let sections: [&[i64]; 3] = [&[0, 1], &[2], &[3, 4]];
        let mut marker_offsets = Vec::new();
        for stamps in sections {
            marker_offsets.push(file.len());
            file.extend_from_slice(&[0u8; SYNC_MARKER_SIZE]);
            for &n in stamps {
                file.extend(encode(&mutation_n(n)));
            }
        }
        for pair in marker_offsets.windows(2) {
            file[pair[0]..pair[0] + SYNC_MARKER_SIZE]
                .copy_from_slice(&marker_bytes(id, pair[1] as u32));
        }
        let last = *marker_offsets.last().unwrap();
        file[last..last + SYNC_MARKER_SIZE].copy_from_slice(&marker_bytes(id, 0));
        fs::write(&path, &file).unwrap();

        assert_eq!(replay(&path), vec![0, 1, 2, 3, 4]);
    }

    /// The old layout's single-section case: a plain flush with the initial
    /// EOF marker and no force_sync at all.
    #[test]
    fn an_old_single_eof_section_segment_still_replays() {
        let dir = tempfile::tempdir().unwrap();
        let id = 78;
        let path = dir.path().join(format!("commitlog-{id}.log"));
        let segment = Segment::new(id, 4096, dir.path());
        append(&segment, &mutation_n(5));
        append(&segment, &mutation_n(6));
        // Build exactly what the old Segment::new + plain flush produced.
        let buf = unsafe { &*segment.buffer.get() };
        let mut file = buf[..segment.current_position() as usize].to_vec();
        file[HEADER_SIZE..HEADER_SIZE + SYNC_MARKER_SIZE].copy_from_slice(&marker_bytes(id, 0));
        fs::write(&path, &file).unwrap();
        assert_eq!(replay(&path), vec![5, 6]);
    }

    /// A segment that fills up closes its final section with `next = 0`,
    /// takes no further appends, and replays fully.
    #[test]
    fn a_full_segment_closes_its_final_section_to_eof_and_replays() {
        let dir = tempfile::tempdir().unwrap();
        let entry = Segment::entry_total_size(&mutation_n(0));
        // Room for the header, the first head, three entries, one more head
        // and part of a third: the second flush cannot reserve a slot.
        let capacity = INITIAL_POSITION as usize + 3 * entry + SYNC_MARKER_SIZE + 4;
        let segment = Segment::new(5, capacity, dir.path());
        append(&segment, &mutation_n(0));
        segment.flush_to_disk().unwrap();
        let second_head = segment.durable_watermark();
        append(&segment, &mutation_n(1));
        append(&segment, &mutation_n(2));
        let before_close = fs::read(segment.path()).unwrap();
        segment.flush_to_disk().unwrap();

        let on_disk = fs::read(segment.path()).unwrap();
        assert_eq!(&on_disk[..before_close.len()], &before_close[..]);
        let h = second_head as usize;
        assert_eq!(
            &on_disk[h..h + SYNC_MARKER_SIZE],
            &marker_bytes(5, 0),
            "the final head runs to EOF"
        );
        assert_eq!(on_disk.len() as u64, segment.current_position());
        assert!(
            segment.allocate_and_begin_write(ENTRY_OVERHEAD).is_none(),
            "a closed segment accepts no more appends"
        );
        // Later flushes are no-ops: nothing is ever rewritten.
        segment.flush_to_disk().unwrap();
        assert_eq!(fs::read(segment.path()).unwrap(), on_disk);
        assert_eq!(replay(segment.path()), vec![0, 1, 2]);
    }

    /// Rotation seals a segment with room left; its final flush closes the
    /// chain with `next = 0` too.
    #[test]
    fn a_sealed_segment_closes_its_final_section_to_eof() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(6, 64 * 1024, dir.path());
        append(&segment, &mutation_n(0));
        segment.flush_to_disk().unwrap();
        let head = segment.durable_watermark() as usize;
        append(&segment, &mutation_n(1));
        segment.seal_writes();
        segment.flush_to_disk().unwrap();
        segment.close_file_handle();
        segment.release_buffer();
        segment.flush_to_disk().unwrap();

        let on_disk = fs::read(segment.path()).unwrap();
        assert_eq!(&on_disk[head..head + SYNC_MARKER_SIZE], &marker_bytes(6, 0));
        assert_eq!(replay(segment.path()), vec![0, 1]);
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
                        s.flush_to_disk()
                            .expect("a flush racing release must not fail");
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
