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
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

/// Rewrites, in place, the sync marker that a forward flush skipped.
///
/// `write_all_at` is pwrite: it does not move the file cursor, so the
/// appending `write_all` that ran just before it keeps its place and the next
/// incremental flush still lands at the tail.
fn repair_marker_in_place(
    file: &fs::File,
    buf: &[u8],
    patch: &std::ops::Range<u64>,
) -> ferrosa_common::Result<usize> {
    use std::os::unix::fs::FileExt;
    let start = patch.start as usize;
    let end = patch.end as usize;
    if end > buf.len() {
        return Err(ferrosa_common::Error::InvalidData(format!(
            "commit-log marker repair {start}..{end} lies outside the {}-byte segment buffer",
            buf.len()
        )));
    }
    file.write_all_at(&buf[start..end], patch.start)?;
    Ok(end - start)
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

/// The segment's bytes, and the only place a view of them is formed.
///
/// Several threads write this buffer at once, each owning a disjoint byte
/// range, and std has no primitive that expresses that: the only way into an
/// `UnsafeCell<Vec<u8>>` is as a whole, `&mut Vec<u8>`, and two of those from
/// two writers overlap as *references* even when the bytes they touch do not.
/// That is undefined behaviour under Stacked and Tree Borrows — not a style
/// problem — and it was the append path's shape before this type existed.
///
/// The invariant that replaces it: every reference this type hands out covers
/// exactly one reservation, `[off, off + len)`, and reservation ownership — the
/// CAS in [`Segment::allocate`] returns each offset to one caller — is what
/// makes two of them disjoint. The base pointer is captured once, at
/// construction, because re-deriving it means forming a reference to the `Vec`
/// header that every writer would then have to coordinate on.
///
/// Deliberate follow-up, not smuggled in here: per-writer shards would let each
/// writer own its buffer outright, which removes this raw pointer and both
/// `unsafe impl`s below. That is a redesign of allocation, the flush watermark
/// and the marker chain at once, so it is named here rather than done.
struct SegmentBuffer {
    /// Owns the allocation. Only [`release`](Self::release) replaces it.
    storage: UnsafeCell<Vec<u8>>,
    /// `storage`'s allocation, fixed for the buffer's life: `new` allocates the
    /// whole capacity and nothing resizes it. `release` frees it and drops
    /// `len` to zero, which every accessor checks before touching `base`.
    base: NonNull<u8>,
    /// Allocated bytes, 0 once released.
    ///
    /// Atomic because `buffer_bytes()` reads it while writers hold `&mut` into
    /// the same allocation, where reading the `Vec`'s own length field is a data
    /// race.
    len: AtomicUsize,
    /// One past the highest byte whose write has completed.
    ///
    /// Published `Release` by the writer that filled it and read `Acquire` by a
    /// flusher, so a flusher that acts on this value is guaranteed to see the
    /// bytes below it and can never publish a length whose contents do not exist
    /// yet.
    written: AtomicUsize,
}

impl SegmentBuffer {
    fn new(bytes: Vec<u8>) -> Self {
        let mut bytes = bytes;
        // `as_mut_ptr`, not `as_ptr`: a pointer derived from a shared reference
        // carries read-only permission, and every writer below writes through
        // this one.
        let base = NonNull::new(bytes.as_mut_ptr()).expect("segment buffer is non-empty");
        let len = bytes.len();
        Self {
            storage: UnsafeCell::new(bytes),
            base,
            len: AtomicUsize::new(len),
            written: AtomicUsize::new(0),
        }
    }

    /// Allocated bytes; 0 once [`release`](Self::release) has run.
    fn allocated(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// One past the highest byte that has been written.
    fn written(&self) -> usize {
        self.written.load(Ordering::Acquire)
    }

    /// Publishes "every byte below `end` is written".
    fn publish(&self, end: usize) {
        self.written.fetch_max(end, Ordering::Release);
    }

    /// A mutable view of exactly `[off, off + len)` — never of the whole buffer.
    ///
    /// # Safety
    ///
    /// `[off, off + len)` must be owned by the caller: a reservation from
    /// [`Segment::allocate`], or marker bytes the caller reaches under
    /// `marker_link`. No other live view may overlap it.
    // The mutation is interior — that is the whole point of the `UnsafeCell` — and the caller's
    // reservation is the safety condition, so the lint's objection is answered by `unsafe` here
    // rather than by making the API hand out a raw pointer for every caller to dereference.
    #[allow(clippy::mut_from_ref)]
    unsafe fn range_mut(&self, off: usize, len: usize) -> &mut [u8] {
        // Also the guard that a write after `release` panics instead of
        // reaching a freed allocation. One load, and it is the atomic the
        // release path has to update anyway.
        assert!(
            off + len <= self.allocated(),
            "segment buffer range {off}..{} is outside the {}-byte buffer",
            off + len,
            self.allocated()
        );
        // SAFETY: the caller owns the range and `off + len` is inside the
        // allocation, so the pointer arithmetic stays in bounds and the view
        // does not overlap any other reservation's.
        unsafe { std::slice::from_raw_parts_mut(self.base.as_ptr().add(off), len) }
    }

    /// A read-only view of `[0, end)`.
    ///
    /// # Safety
    ///
    /// `end` must not exceed [`written`](Self::written) or the allocated
    /// length, and no writer may hold a mutable view of any part of `[0, end)`
    /// while this one lives, because the flusher forms it under `file_handle`
    /// after draining the append path.
    unsafe fn prefix(&self, end: usize) -> &[u8] {
        debug_assert!(end <= self.written(), "prefix past the published bytes");
        debug_assert!(end <= self.allocated(), "prefix past the allocation");
        // SAFETY: `end` is inside the allocation because the assertions above
        // bound it, and no mutable view overlaps the prefix because the caller
        // holds the lock that excludes the marker patch and the append path.
        unsafe { std::slice::from_raw_parts(self.base.as_ptr(), end) }
    }

    /// Frees the allocation.
    ///
    /// # Safety
    ///
    /// No writer and no flusher may still be able to reach the segment: every
    /// view is derived from `base`, which this frees. The caller holds
    /// `file_handle` — the lock a flusher takes before it forms its view — and
    /// the segment has been rotated out of the active slot.
    unsafe fn release(&self) {
        // SAFETY: exclusive by the caller's precondition. Replacing the `Vec`
        // drops the old allocation, which is exactly why `len` goes to zero
        // immediately after.
        unsafe { *self.storage.get() = Vec::new() };
        self.len.store(0, Ordering::Release);
    }
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
    /// A [`SegmentBuffer`], so a writer can only ever get a view of its own
    /// reserved range. See that type for why the bytes must not be reached as a
    /// whole while other writers hold ranges.
    buffer: SegmentBuffer,

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

    /// Byte position up to which data has been flushed to disk. Used by
    /// incremental flush to write only new bytes.
    last_flushed: AtomicU64,

    /// Absolute byte offset of the most recently written sync marker.
    /// Used to build the forward-linked marker chain: when a new marker is
    /// written, the previous marker's `next_marker_offset` is patched to
    /// point to the new one.
    ///
    /// Only ever read or written under `marker_link`. It stays an atomic so
    /// the field carries no interior-mutability ceremony of its own; the lock
    /// is what makes the read-modify-write a single step.
    last_sync_marker_offset: AtomicU64,

    /// Serializes marker allocation together with marker linking.
    ///
    /// The chain must come out in ALLOCATION order, and nothing weaker than
    /// one lock over both steps gets that. A swap on
    /// `last_sync_marker_offset` stops two callers from claiming the same
    /// predecessor, but it does not stop the caller who allocated the LATER
    /// offset from claiming first — and a marker whose `next` points
    /// backwards ends the chain as far as `SegmentReader` is concerned, which
    /// is silent loss of everything after it. Allocating under the same lock
    /// makes "claimed earlier" and "sits earlier in the file" the same fact.
    ///
    /// It guards the eight-byte buffer writes only, not the flush, so
    /// concurrent `force_sync` callers still fsync in parallel.
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

// SAFETY: `Segment`'s only field that is not `Send + Sync` on its own is its
// `SegmentBuffer` — `UnsafeCell<Vec<u8>>` and `NonNull<u8>` are deliberately
// neither. What discharges them is that buffer's invariant: every reference it
// hands out covers one reservation, the CAS in `allocate` returns each offset to
// exactly one writer so two writers' views cannot overlap, a reader only views
// the published prefix whose bytes are complete, and the one writer that reaches
// backwards (the marker patch) is synchronized against the flusher by
// `file_handle`; std has no disjoint-mutable-ranges primitive to do it instead.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}

impl Segment {
    /// Creates a new segment with the given ID and capacity.
    ///
    /// The buffer is pre-allocated and zeroed. The 17-byte header is written
    /// immediately, followed by an 8-byte sync marker (all zeros = EOF marker).
    /// Syncs use the default, power-loss durable [`CommitLogSyncMode::Full`].
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

        // Write the first sync marker at offset HEADER_SIZE (17).
        // All zeros = EOF marker (next_marker_offset = 0, marker_crc = CRC of
        // segment_id || 0).
        Self::write_sync_marker_into(
            &mut buffer[HEADER_SIZE..HEADER_SIZE + SYNC_MARKER_SIZE],
            id,
            0,
        );

        let path = dir.join(format!("commitlog-{id}.log"));

        // Everything up to the first marker exists, so it is the buffer's first
        // published length: a flusher that ran before any entry would write the
        // header and marker and stop there.
        let buffer = SegmentBuffer::new(buffer);
        buffer.publish(INITIAL_POSITION as usize);

        Segment {
            id,
            buffer,
            position: AtomicU64::new(INITIAL_POSITION),
            capacity: size,
            created_at: Instant::now(),
            path,
            dirty_tables: DashMap::new(),
            in_flight_writers: AtomicU64::new(0),
            sealed: std::sync::atomic::AtomicBool::new(false),
            file_handle: Mutex::new(None),
            last_flushed: AtomicU64::new(INITIAL_POSITION),
            last_sync_marker_offset: AtomicU64::new(HEADER_SIZE as u64),
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
    /// The view taken below covers `[offset, offset + total_size)` and nothing
    /// else. `offset` came from [`allocate()`](Self::allocate), whose CAS returns
    /// each offset to exactly one caller, so writer w owns `[off, off + len)`
    /// and no two writers' views overlap — the bytes are disjoint and so are the
    /// references. The length is published `Release` after the last byte is
    /// written, which pairs with the flusher's `Acquire` load, so a flush can
    /// only ever include bytes that exist.
    pub fn write_entry(&self, offset: u64, mutation: &Mutation) -> CommitLogPosition {
        let payload_size = mutation.serialized_size();
        let total_size = ENTRY_OVERHEAD + payload_size;
        let off = offset as usize;

        debug_assert!(
            off + total_size <= self.capacity,
            "write_entry: offset {off} + size {total_size} exceeds capacity {}",
            self.capacity
        );

        // SAFETY: this writer owns [off, off + total_size) — the reservation
        // came from `allocate`, so no other writer holds an overlapping range,
        // and the slice is scoped to those bytes rather than to the buffer.
        let buf = unsafe { self.buffer.range_mut(off, total_size) };

        // 1. Write entry_size (payload size as u32, big-endian).
        let entry_size_bytes = (payload_size as u32).to_be_bytes();
        buf[..4].copy_from_slice(&entry_size_bytes);

        // 2. Write size_crc (CRC32 of the 4 entry_size bytes).
        let size_crc = crc32fast::hash(&entry_size_bytes);
        buf[4..8].copy_from_slice(&size_crc.to_be_bytes());

        // 3. Write payload (serialized mutation).
        let payload_start = 8;
        let payload_end = payload_start + payload_size;
        mutation.serialize_into(&mut buf[payload_start..payload_end]);

        // 4. Write payload_crc (CRC32 of payload bytes).
        let payload_crc = crc32fast::hash(&buf[payload_start..payload_end]);
        buf[payload_end..payload_end + 4].copy_from_slice(&payload_crc.to_be_bytes());

        // The entry exists only once every byte above is down; `Release` is what
        // stops a flusher from believing otherwise.
        self.buffer.publish(off + total_size);

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

        // SAFETY: as `write_entry` — `allocate` returned this offset to this
        // writer alone, so the view covers exactly the bytes it owns.
        let buf = unsafe { self.buffer.range_mut(off, total_size) };

        let entry_size_bytes = (payload_size as u32).to_be_bytes();
        buf[..4].copy_from_slice(&entry_size_bytes);

        let size_crc = crc32fast::hash(&entry_size_bytes);
        buf[4..8].copy_from_slice(&size_crc.to_be_bytes());

        let payload_start = 8;
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

        self.buffer.publish(off + total_size);

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
        // Reserve the eight bytes rather than reading `position` and writing at
        // it: two callers that both read the same position would otherwise write
        // the same bytes, and a flusher could snapshot past a marker that has no
        // contents. Counting it in flight gives the marker the same ordering
        // guarantee an entry gets.
        let Some(offset) = self.allocate_and_begin_write(SYNC_MARKER_SIZE) else {
            // The segment cannot hold another marker; the chain ends where it
            // ends, exactly as `allocate_and_link_sync_marker` decides.
            return;
        };

        // SAFETY: because `allocate_and_begin_write` claimed exactly these eight
        // bytes for this call, no other writer's view can overlap them — the
        // reservation is what makes the range ours.
        let marker = unsafe { self.buffer.range_mut(offset as usize, SYNC_MARKER_SIZE) };
        Self::write_sync_marker_into(marker, self.id, next_offset);
        self.buffer.publish(offset as usize + SYNC_MARKER_SIZE);

        self.writer_done();
    }

    /// Which already-flushed bytes a backwards marker patch dirtied, if any.
    ///
    /// `write_sync_marker_at` patches the PREVIOUS marker so it points at the
    /// new one. That write goes backwards, and `flush_to_disk` only ever writes
    /// forward from `last_flushed` -- so a patch below that watermark would
    /// never reach disk.
    ///
    /// `force_sync` used to answer this by rewriting the whole segment:
    /// `open(O_TRUNC)` plus every byte accumulated so far plus a hardware cache
    /// flush, on every call. Inside one 32 MB segment that is quadratic in the
    /// number of batches, and it ran on every Accord apply. The region actually
    /// dirtied is SYNC_MARKER_SIZE bytes -- eight.
    ///
    /// `None` means the patch sits at or above the watermark and the ordinary
    /// incremental flush already covers it.
    pub(crate) fn marker_patch_to_rewrite(
        prev_offset: u64,
        already_flushed: u64,
    ) -> Option<std::ops::Range<u64>> {
        let end = prev_offset.saturating_add(SYNC_MARKER_SIZE as u64);
        // Any overlap at all with what is already on disk, not just full
        // containment: a marker straddling the watermark has its leading bytes
        // below it, and those are exactly the bytes a forward flush skips.
        (prev_offset < already_flushed).then_some(prev_offset..end)
    }

    /// Reserves an EOF sync marker and links it onto the end of the forward
    /// chain, as one step.
    ///
    /// Returns the offset of the PREVIOUS marker — the one this call patched,
    /// and the one the caller hands to
    /// [`flush_to_disk_repairing_marker`](Self::flush_to_disk_repairing_marker),
    /// because that backwards patch is the write an incremental flush would
    /// otherwise skip. Reading the tracker separately would name a different
    /// marker once a second writer has moved it. `None` means the segment
    /// cannot hold another marker and the chain ends where it ends.
    ///
    /// Reserving and linking are inseparable. Each is individually
    /// atomic already; doing them apart is what corrupts the chain, because a
    /// caller can win the allocation and lose the race to link, leaving a
    /// marker pointing backwards at one allocated before it. The
    /// `marker_link` field documents why nothing lock-free covers this.
    ///
    /// The marker counts as an in-flight writer for the whole window, exactly
    /// as `append` counts an entry: a flusher that snapshots a position past
    /// this allocation must wait for the eight bytes to exist, or it writes
    /// the hole and leaves an uncrossable marker on disk.
    pub fn allocate_and_link_sync_marker(&self) -> Option<u64> {
        let _linking = self.marker_link.lock();
        let offset = self.allocate_and_begin_write(SYNC_MARKER_SIZE)?;
        let prev_offset = self.write_sync_marker_at(offset, 0);
        self.writer_done();
        Some(prev_offset)
    }

    /// Writes a sync marker at an explicitly allocated offset, linking it
    /// into the forward chain.
    ///
    /// Unlike `write_sync_marker()`, this does not advance the position — the
    /// offset was already reserved via `allocate()`.
    ///
    /// The previous sync marker's `next_marker_offset` is patched to point to
    /// this new marker, creating the forward-linked chain for crash recovery.
    ///
    /// The caller must hold `marker_link`; see
    /// [`allocate_and_link_sync_marker`](Self::allocate_and_link_sync_marker).
    fn write_sync_marker_at(&self, offset: u64, next_marker_offset: u32) -> u64 {
        // The link write below goes backwards, into bytes a flush in progress may
        // be reading. `file_handle` is the lock a flusher takes before it forms
        // its view of the prefix, so holding it here is what keeps this
        // eight-byte patch out of that view. It does not serialise the fsyncs of
        // concurrent `force_sync` callers: this guard is released before their
        // repair flush re-acquires it.
        let _file = self.file_handle.lock();

        // SAFETY: because `offset` was reserved for this call by
        // `allocate_and_link_sync_marker`, and `marker_link` excludes every other
        // marker writer, this range is ours alone.
        let marker = unsafe { self.buffer.range_mut(offset as usize, SYNC_MARKER_SIZE) };

        // The new marker's own bytes go down BEFORE anything points at them.
        // A flusher racing us reads the prefix under this same file lock, so
        // the order these two writes land in is the order it can observe. A
        // predecessor pointing at eight bytes that are still zero is a marker
        // with a zero CRC, which a reader treats as the end of the log.
        Self::write_sync_marker_into(marker, self.id, next_marker_offset);
        self.buffer.publish(offset as usize + SYNC_MARKER_SIZE);

        let prev_offset = self.last_sync_marker_offset.load(Ordering::Acquire);
        // SAFETY: the previous marker is ours to patch because `marker_link` is
        // held, so no other writer is touching a marker, and because
        // `file_handle` keeps a flusher from holding a view over these eight
        // bytes while we write them.
        let prev = unsafe {
            self.buffer
                .range_mut(prev_offset as usize, SYNC_MARKER_SIZE)
        };
        Self::write_sync_marker_into(prev, self.id, offset as u32);

        self.last_sync_marker_offset
            .store(offset, Ordering::Release);

        prev_offset
    }

    /// Internal helper to write a sync marker into its own eight bytes.
    fn write_sync_marker_into(marker: &mut [u8], segment_id: u64, next_marker_offset: u32) {
        // next_marker_offset: u32 (4 bytes)
        marker[..4].copy_from_slice(&next_marker_offset.to_be_bytes());

        // marker_crc: CRC32 of (segment_id as u64 || next_marker_offset as u32)
        let mut crc_input = [0u8; 12];
        crc_input[..8].copy_from_slice(&segment_id.to_be_bytes());
        crc_input[8..12].copy_from_slice(&next_marker_offset.to_be_bytes());
        let crc = crc32fast::hash(&crc_input);
        marker[4..8].copy_from_slice(&crc.to_be_bytes());
    }

    /// Hold the real flush/file mutex while a deterministic cross-crate test runs.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn with_flush_held_for_test(&self, hold: impl FnOnce()) {
        let _file = self.file_handle.lock();
        hold();
    }

    /// Flushes the written portion of the buffer to disk and fsyncs.
    ///
    /// Uses incremental flush: only writes bytes since the last flush.
    /// Waits for all in-flight writers to complete before reading the buffer.
    pub fn flush_to_disk(&self) -> ferrosa_common::Result<()> {
        self.flush_inner(None)
    }

    /// Flushes, and repairs the sync marker at `prev_marker_offset` if the
    /// backwards patch [`write_sync_marker_at`](Self::write_sync_marker_at)
    /// made to it now sits behind the flush watermark.
    ///
    /// The repair is decided inside the flush, under the file lock, against
    /// the watermark the flush itself uses. Deciding it in the caller would be
    /// a race: between the caller's marker write and this call a concurrent
    /// flusher can carry those bytes to disk, or run past them with their
    /// pre-patch contents, and only the lock holder can tell which happened.
    pub fn flush_to_disk_repairing_marker(
        &self,
        prev_marker_offset: u64,
    ) -> ferrosa_common::Result<()> {
        self.flush_inner(Some(prev_marker_offset))
    }

    fn flush_inner(&self, repair_marker_at: Option<u64>) -> ferrosa_common::Result<()> {
        let sync_start = Instant::now();
        let _span = tracing::info_span!("commitlog.sync", segment_id = self.id,).entered();
        let mut phases = SyncPhaseDurations::default();
        // Snapshot position BEFORE waiting. This captures only data from
        // writers who have already allocated. New allocations after this
        // point will be flushed on the next call.
        let snapshot_pos = self.position.load(Ordering::Acquire) as usize;

        // Wait for all in-flight writers to complete their entries.
        phases.wait_writers = self.wait_for_writers();
        observe_wait_writers(phases.wait_writers);

        // File I/O happens under the file_handle mutex to prevent concurrent
        // flushers from writing duplicate bytes. `release_buffer()` and the
        // marker patch take this lock too, so once we hold it the buffer's
        // (ptr, len, cap) is stable for the duration of this flush and no
        // eight-byte marker write can land inside the prefix about to be read.
        let lock_start = Instant::now();
        let mut handle = self.file_handle.lock();
        phases.file_lock_wait = lock_start.elapsed();
        observe_phase(
            &SYNC_FILE_LOCK_WAIT_MICROS_TOTAL,
            &SYNC_FILE_LOCK_WAIT_MICROS_MAX,
            phases.file_lock_wait,
        );

        // If `release_buffer()` ran before us, the data is already on disk
        // (force_rotate calls flush_to_disk first) and the buffer is empty.
        // Don't try to slice into a freed allocation — short-circuit. Without
        // this guard the test
        // `concurrent_writers_with_flushes_preserve_lww` flakes with
        // `range end index N out of range for slice of length 0`.
        if self.buffer.allocated() == 0 {
            return Ok(());
        }

        // Publish only the bytes that exist. `snapshot_pos` is what was
        // *reserved*, and `allocate()` is public: a writer can hold a
        // reservation for a while before it has written a byte of it. The
        // flushable end is therefore the published prefix as well as the
        // snapshot. Taking the smaller of the two costs a slow writer one extra
        // incremental flush; taking the snapshot alone would write zeros over
        // its range and then move the watermark past it, so the entry would
        // never reach disk while the index said it was there.
        let current_pos = snapshot_pos.min(self.buffer.written());
        let last_flushed = self.last_flushed.load(Ordering::Acquire) as usize;
        let marker_patch = repair_marker_at
            .and_then(|prev| Self::marker_patch_to_rewrite(prev, last_flushed as u64));

        // SAFETY: because `current_pos <= written`, every byte below it has
        // been written and published; `wait_for_writers` drained the counted
        // writers, so nothing below `snapshot_pos` is mid-write, and entry
        // writers only touch ranges reserved at or above it. The file lock
        // excludes the marker patch — the one writer that reaches backwards
        // into this prefix — and `release_buffer`.
        let buf = unsafe { self.buffer.prefix(current_pos) };

        match handle.as_mut() {
            None => {
                // First flush: create file, write from beginning (header + sync marker + entries).
                // Nothing is on disk yet, so a marker patch anywhere in
                // `buf[..current_pos]` goes out with this write.
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
                // Incremental: append only new bytes, plus the eight bytes of
                // any marker the forward write would have skipped. Both land
                // before the single fsync below, so durability is unchanged.
                use std::io::Write;
                let mut written = current_pos - last_flushed;
                let write_start = Instant::now();
                file.write_all(&buf[last_flushed..current_pos])?;
                if let Some(patch) = marker_patch.as_ref() {
                    written += repair_marker_in_place(file, buf, patch)?;
                }
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
                INCREMENTAL_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                INCREMENTAL_FLUSH_BYTES_TOTAL.fetch_add(written as u64, Ordering::Relaxed);
                SYNCS_TOTAL.fetch_add(1, Ordering::Relaxed);
                let total = sync_start.elapsed();
                observe_sync(total);
                maybe_warn_slow_sync(self.id, written, total, &phases);
                self.last_flushed
                    .store(current_pos as u64, Ordering::Release);
            }
            Some(file) => {
                // Nothing new to append. A marker repair can still be owed:
                // a concurrent flusher can have carried this segment past the
                // patched marker already, writing its pre-patch bytes.
                let Some(patch) = marker_patch else {
                    return Ok(());
                };
                let write_start = Instant::now();
                let written = repair_marker_in_place(file, buf, &patch)?;
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
                INCREMENTAL_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
                INCREMENTAL_FLUSH_BYTES_TOTAL.fetch_add(written as u64, Ordering::Relaxed);
                SYNCS_TOTAL.fetch_add(1, Ordering::Relaxed);
                let total = sync_start.elapsed();
                observe_sync(total);
                maybe_warn_slow_sync(self.id, written, total, &phases);
            }
        }

        Ok(())
    }

    /// Rewrites the entire segment file from scratch.
    ///
    /// Unlike `flush_to_disk` which only appends new bytes, this writes
    /// `buf[0..position]` to the file.
    ///
    /// Nothing calls this any more. `force_sync` did, to carry the eight-byte
    /// backwards marker patch that a forward flush skips, which cost a whole
    /// `open(O_TRUNC)` plus every accumulated byte plus a hardware cache flush
    /// per call — quadratic within one segment. That is now
    /// [`flush_to_disk_repairing_marker`](Self::flush_to_disk_repairing_marker).
    /// A new caller wanting a full rewrite should say why the incremental path
    /// cannot express it.
    pub fn force_full_flush(&self) -> ferrosa_common::Result<()> {
        let sync_start = Instant::now();
        let mut phases = SyncPhaseDurations::default();
        let snapshot_pos = self.position.load(Ordering::Acquire) as usize;
        phases.wait_writers = self.wait_for_writers();
        observe_wait_writers(phases.wait_writers);

        let lock_start = Instant::now();
        let mut handle = self.file_handle.lock();
        phases.file_lock_wait = lock_start.elapsed();
        observe_phase(
            &SYNC_FILE_LOCK_WAIT_MICROS_TOTAL,
            &SYNC_FILE_LOCK_WAIT_MICROS_MAX,
            phases.file_lock_wait,
        );

        if self.buffer.allocated() == 0 {
            return Ok(());
        }
        // The same published-prefix bound as `flush_inner`: rewrite the bytes
        // that exist, not the range that was reserved.
        let end = snapshot_pos.min(self.buffer.written());
        // SAFETY: because `end <= written`, the prefix is written and published,
        // the counted writers below it are drained, and the file lock excludes
        // the marker patch and `release_buffer` from touching the same bytes.
        let buf = unsafe { self.buffer.prefix(end) };

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
        file.write_all(&buf[..end])?;
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
        FULL_FLUSHES_TOTAL.fetch_add(1, Ordering::Relaxed);
        FULL_FLUSH_BYTES_TOTAL.fetch_add(end as u64, Ordering::Relaxed);
        SYNCS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let total = sync_start.elapsed();
        observe_sync(total);
        maybe_warn_slow_sync(self.id, end, total, &phases);
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
        // An atomic, not the `Vec`'s length field: writers hold `&mut` into this
        // allocation while this reads, and reading the header would be a data
        // race against the append path.
        self.buffer.allocated()
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
        // can have captured `snapshot_pos` and formed a view of the prefix
        // BEFORE this method runs; the flush forms that view under this same
        // lock, so holding it here means no flusher is inside its view when the
        // allocation below is freed.
        //
        // Writers cannot reach this segment via `ArcSwap` once `force_rotate()`
        // has swapped in the new active segment, so no append path can hold a
        // range either; the assertion states that precondition rather than
        // assuming it silently.
        let _handle = self.file_handle.lock();
        debug_assert_eq!(
            self.in_flight_writers.load(Ordering::Acquire),
            0,
            "release_buffer: a writer is still in flight"
        );
        // SAFETY: `file_handle` excludes every flusher (they form their view
        // under it), the segment has been rotated out of the active slot, and
        // the assertion above holds on the one counter that tracks an append
        // between reservation and write. Every view into the allocation is
        // therefore gone.
        unsafe { self.buffer.release() };
        // Make any post-release flush a no-op: by setting last_flushed to
        // the current position, the `Some(_)` "nothing new" arm of
        // `flush_to_disk` is taken even if the file handle gets re-opened
        // somehow. The `None` arm is additionally guarded by the published
        // length check in `flush_inner`.
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

    /// The segment's written prefix, for assertions.
    ///
    /// Test-only: nothing outside this module reads the buffer as a whole, which
    /// is the point of `SegmentBuffer`.
    fn written_prefix(segment: &Segment) -> &[u8] {
        // SAFETY: the tests below are single-threaded at the point they read,
        // and `written()` bounds the view to bytes that exist.
        unsafe { segment.buffer.prefix(segment.buffer.written()) }
    }

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
        let buf = written_prefix(&segment);
        let end = segment.current_position() as usize;
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
        let buf = written_prefix(&segment);
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

        let buf = written_prefix(&segment);
        let descriptor = SegmentDescriptor::read_from(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(descriptor.segment_id, 42);
    }

    #[test]
    fn sync_marker_crc_is_valid() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 4096, dir.path());

        let buf = written_prefix(&segment);
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

    /// A reservation is a claim on bytes, not a claim that they exist yet.
    ///
    /// A flush that runs between `allocate()` and `write_entry()` must publish
    /// only the fully-written prefix. Publishing the whole reservation writes
    /// zeros over the entry, and the incremental watermark then starts past
    /// those bytes, so the entry is never written again: the index says the
    /// entry is at that offset and the file holds zeros there.
    ///
    /// `allocate()` is public and `engine.rs` writes through it, so this is a
    /// reachable window, not a synthetic one.
    #[test]
    fn a_flush_between_a_reservation_and_its_write_must_not_publish_unwritten_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let segment = Segment::new(1, 8192, dir.path());
        let m = simple_mutation();
        let total = Segment::entry_total_size(&m);

        let first = segment.allocate(total).expect("room for one entry");
        segment.write_entry(first, &m);
        segment.flush_to_disk().unwrap();

        // The window `allocate_and_begin_write()` covers with the in-flight
        // counter: the range is reserved, and no byte of it has been written.
        let second = segment.allocate(total).expect("room for a second entry");
        segment.flush_to_disk().unwrap();

        segment.write_entry(second, &m);
        segment.flush_to_disk().unwrap();

        let on_disk = fs::read(segment.path()).unwrap();
        let first_entry = &on_disk[first as usize..first as usize + total];
        let second_entry = &on_disk[second as usize..second as usize + total];
        assert_eq!(
            first_entry, second_entry,
            "the entry written after the intermediate flush is not on disk at its own \
             offset: that flush published a reserved range whose bytes did not exist yet"
        );
    }

    /// Every writer may hold a `&mut` to its own reserved range, and two
    /// writers' ranges are disjoint — but a `&mut` to the *whole* buffer on
    /// each thread means the two references overlap, which is UB under Stacked
    /// and Tree Borrows however disjoint the bytes they touch. This is the
    /// commit log's durability path, so the test drives it the way `append`
    /// does: one reservation per writer, all reservations taken before any
    /// write begins.
    ///
    /// The bytes are checked against the same writes done one at a time, so
    /// the assertion is "the concurrent result is byte-for-byte the serial
    /// result", not merely "nothing panicked".
    ///
    /// The comparison is over the buffer's published prefix, which is what both
    /// a flusher and this test read; the on-disk check is skipped under Miri,
    /// which has no `fdatasync`.
    #[test]
    fn concurrent_writers_take_disjoint_ranges_and_lose_no_bytes() {
        use std::sync::Barrier;

        const WRITERS: usize = 8;
        // Miri interprets one thread at a time and is orders of magnitude
        // slower; the aliasing violation needs only one preemption.
        let rounds = if cfg!(miri) { 4 } else { 16 };
        let total = Segment::entry_total_size(&simple_mutation());
        let capacity = 4096 + WRITERS * total;

        for _ in 0..rounds {
            // Reference: the same writes one at a time, so `expected` is the
            // concatenation of what each writer wrote and nothing else.
            let serial_dir = tempfile::tempdir().unwrap();
            let serial = Segment::new(1, capacity, serial_dir.path());
            let mut expected_offsets = Vec::new();
            for i in 0..WRITERS {
                let mut m = simple_mutation();
                m.timestamp = 1_000 + i as i64;
                let off = serial.allocate(total).expect("one block per writer");
                serial.write_entry(off, &m);
                expected_offsets.push(off);
            }
            let expected = written_prefix(&serial).to_vec();

            let dir = tempfile::tempdir().unwrap();
            let segment = Arc::new(Segment::new(1, capacity, dir.path()));
            let barrier = Arc::new(Barrier::new(WRITERS));
            let mut handles = Vec::new();
            for i in 0..WRITERS {
                let segment = Arc::clone(&segment);
                let barrier = Arc::clone(&barrier);
                handles.push(std::thread::spawn(move || {
                    let mut m = simple_mutation();
                    m.timestamp = 1_000 + i as i64;
                    let off = segment.allocate(total).expect("one block per writer");
                    // Everything is reserved before anything is written, so
                    // every writer owns its range at the same time.
                    barrier.wait();
                    segment.write_entry(off, &m);
                    (i, off)
                }));
            }
            // Reservations complete in scheduler order, not writer order, so
            // each block is checked against the offset its own writer took.
            let mut assigned: Vec<(usize, u64)> =
                handles.into_iter().map(|h| h.join().unwrap()).collect();
            assigned.sort_unstable();
            let mut offsets: Vec<u64> = assigned.iter().map(|(_, off)| *off).collect();
            offsets.sort_unstable();
            let mut expected_sorted = expected_offsets.clone();
            expected_sorted.sort_unstable();
            assert_eq!(offsets, expected_sorted, "reservations must stay disjoint");

            let actual = written_prefix(&segment);

            assert_eq!(
                actual.len(),
                expected.len(),
                "the concurrent segment published a different number of bytes"
            );
            for (i, off) in &assigned {
                let off = *off as usize;
                let serial_off = expected_offsets[*i] as usize;
                assert_eq!(
                    &actual[off..off + total],
                    &expected[serial_off..serial_off + total],
                    "writer {i}'s block at offset {off} does not hold that writer's entry"
                );
            }

            // And the buffer as a whole is the header followed by each writer's
            // block, laid down in reservation order — the concatenation, with
            // no interleaving and no gap.
            let mut blocks: Vec<(u64, usize)> =
                assigned.iter().map(|(i, off)| (*off, *i)).collect();
            blocks.sort_unstable();
            let mut concatenated = expected[..INITIAL_POSITION as usize].to_vec();
            for (_, i) in blocks {
                let serial_off = expected_offsets[i] as usize;
                concatenated.extend_from_slice(&expected[serial_off..serial_off + total]);
            }
            assert_eq!(
                actual, concatenated,
                "the segment is not the concatenation of what each writer wrote"
            );

            // The same bytes must reach the file through the flush path. Miri
            // stops at the sync syscall (`fdatasync` is unsupported there), so
            // that half is checked on a real target only.
            if !cfg!(miri) {
                segment.flush_to_disk().unwrap();
                let on_disk = fs::read(segment.path()).unwrap();
                assert_eq!(on_disk, concatenated, "the flush published different bytes");
            }
        }
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

#[cfg(test)]
mod marker_patch_tests {
    use super::*;

    #[test]
    fn a_patch_below_the_watermark_must_be_rewritten() {
        // The bytes are already on disk and the forward flush will not revisit
        // them, so eight bytes have to be written back.
        assert_eq!(
            Segment::marker_patch_to_rewrite(1_000, 5_000),
            Some(1_000..1_008)
        );
    }

    #[test]
    fn a_patch_at_or_above_the_watermark_needs_nothing_extra() {
        // Not yet flushed: the ordinary incremental flush carries it.
        assert_eq!(Segment::marker_patch_to_rewrite(5_000, 5_000), None);
        assert_eq!(Segment::marker_patch_to_rewrite(6_000, 5_000), None);
    }

    #[test]
    fn a_patch_straddling_the_watermark_is_still_rewritten() {
        // Its leading bytes are below the watermark, and those are precisely
        // the ones a forward flush skips.
        assert_eq!(
            Segment::marker_patch_to_rewrite(4_996, 5_000),
            Some(4_996..5_004)
        );
    }

    #[test]
    fn nothing_flushed_yet_means_nothing_to_rewrite() {
        assert_eq!(Segment::marker_patch_to_rewrite(0, 0), None);
    }

    #[test]
    fn the_rewrite_is_one_marker_wide_and_never_the_segment() {
        // The whole point: bounded at eight bytes regardless of how much the
        // segment has accumulated.
        let patch = Segment::marker_patch_to_rewrite(10, 32 * 1024 * 1024).expect("below mark");
        assert_eq!(patch.end - patch.start, SYNC_MARKER_SIZE as u64);
    }
}
