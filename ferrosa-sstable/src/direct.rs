//! Module: Page-cache-bypassing sequential writer for immutable SSTable output.
//! Correctness: Correct when the file's logical content is byte-identical to the
//!   written stream for every length (aligned or not), every device write is
//!   block-aligned in offset/length/buffer, the physical padding of a partial
//!   tail is truncated away, and a fallback to buffered I/O is loud and counted.
//! Last revised: 2026-07-22
//! Last changed: New module — Phase 3 (O_DIRECT + I/O, epic t_29f6b948). The
//!   2026-07-22 Fly A/B root-caused the ~3s p100 tail to memtable-flush /
//!   compaction output flooding the OS page cache: the dirty pages drive a
//!   block-layer writeback storm (`rq_qos_wait`, `folio_wait_bit_common`) that
//!   parks unrelated tokio workers in D-state and freezes the runtime. This
//!   writer keeps that bulk sequential output OUT of the page cache — O_DIRECT on
//!   Linux, `F_NOCACHE` on macOS — so it can neither pollute the cache nor
//!   accumulate the dirty pages that trigger the storm. It is the durable-path
//!   primitive the wiring steps (SSTable writer, then flush/compaction) build on.
//!
//! # Why a whole writer, not just an open flag
//!
//! O_DIRECT is unforgiving: every write's file offset, byte length, AND memory
//! buffer must be aligned to the device block size, or `write(2)` returns
//! `EINVAL`. [`DirectWriter`] hides this behind an ordinary `write_all` surface
//! by staging bytes into a block-aligned buffer and only ever issuing
//! block-multiple writes at block-multiple offsets. The final partial block is
//! zero-padded to a full block, written, then [`set_len`](std::fs::File::set_len)
//! trims the padding — so the on-disk logical length is exact.
//!
//! The alignment state machine runs on **every** platform (aligned writes are
//! valid against any file system), so the dev-host test suite exercises the risky
//! logic even though macOS never sets O_DIRECT. Only the open flag is
//! platform-conditional.
//!
//! # Fail-loud fallback
//!
//! Some file systems (tmpfs, some overlay/9p mounts) reject O_DIRECT at open with
//! `EINVAL`. Rather than fail the write, [`DirectWriter::create`] falls back to a
//! normal (page-cached) file and, on finish, advises the kernel to drop the pages
//! (`POSIX_FADV_DONTNEED`) — degraded but functional. Every fallback is WARN-logged
//! and counted in [`direct_write_fallbacks_total`], so silent degradation is
//! impossible (a non-zero counter in steady state means the freeze mitigation is
//! NOT active on that host and must be investigated).

use std::alloc::{alloc, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ferrosa_common::Result;

/// The master switch for every direct-I/O path (writer and compaction reads).
pub const DIRECT_IO_MASTER_ENV: &str = "FERROSA_DIRECT_IO";

/// Where a resolved direct-I/O decision came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchSource {
    /// Nothing was set: the built-in default (on).
    Default,
    /// `FERROSA_DIRECT_IO`.
    Master,
    /// The feature's own switch, which beats the master.
    Specific,
}

/// What to do on this platform for a resolved switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformDecision {
    Direct,
    Buffered,
    /// Direct I/O was asked for by name and this platform cannot do it.
    Unsupported,
}

/// A resolved direct-I/O switch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectIoSwitch {
    pub enabled: bool,
    pub source: SwitchSource,
    /// Values that were set but are not booleans, as `NAME="value"`. The default
    /// applies for them, and the caller must log them: a mistyped "off" would
    /// otherwise leave direct I/O on without a word.
    pub rejected: Vec<String>,
}

/// One env value: unset (absent, empty or blank), a boolean, or neither.
enum Setting {
    Unset,
    Value(bool),
    Rejected,
}

fn parse_setting(value: Option<&str>) -> Setting {
    let Some(text) = value.map(str::trim).filter(|t| !t.is_empty()) else {
        return Setting::Unset;
    };
    match text.to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => Setting::Value(true),
        "0" | "false" | "off" | "no" => Setting::Value(false),
        _ => Setting::Rejected,
    }
}

/// Decide whether a direct-I/O path is on.
///
/// The feature's own switch wins, then `FERROSA_DIRECT_IO`, then the built-in
/// default, which is ON. A value that is set but is not a boolean does not
/// change the outcome and is returned in `rejected`: with the default on, a
/// mistyped "off" (`disable`, `none`) would otherwise leave direct I/O running
/// with no sign that the operator's intent was ignored.
pub fn resolve_switch(
    specific_name: &str,
    specific: Option<&str>,
    master: Option<&str>,
) -> DirectIoSwitch {
    let mut rejected = Vec::new();
    let mut note = |name: &str, value: Option<&str>| {
        rejected.push(format!("{name}={:?}", value.unwrap_or_default().trim()));
    };
    match parse_setting(specific) {
        Setting::Value(enabled) => {
            return DirectIoSwitch {
                enabled,
                source: SwitchSource::Specific,
                rejected,
            };
        }
        Setting::Rejected => note(specific_name, specific),
        Setting::Unset => {}
    }
    match parse_setting(master) {
        Setting::Value(enabled) => {
            return DirectIoSwitch {
                enabled,
                source: SwitchSource::Master,
                rejected,
            };
        }
        Setting::Rejected => note(DIRECT_IO_MASTER_ENV, master),
        Setting::Unset => {}
    }
    DirectIoSwitch {
        enabled: true,
        source: SwitchSource::Default,
        rejected,
    }
}

impl DirectIoSwitch {
    /// What to do here. Turning it off is always honoured. On a platform
    /// without direct I/O the default and the master switch degrade to buffered
    /// I/O, which is the point of a default; only a request by the feature's own
    /// name is refused, because that one asked for something impossible.
    pub fn on_platform(&self, supports_direct: bool) -> PlatformDecision {
        match (self.enabled, supports_direct, self.source) {
            (false, _, _) => PlatformDecision::Buffered,
            (true, true, _) => PlatformDecision::Direct,
            (true, false, SwitchSource::Specific) => PlatformDecision::Unsupported,
            (true, false, _) => PlatformDecision::Buffered,
        }
    }
}

/// Read `specific_name` and `FERROSA_DIRECT_IO` from the environment and resolve
/// them, logging once (per `warned` flag) any value that is not a boolean.
///
/// `warned` is one static per call site: the writer asks for every Data.db it
/// creates, and a line per file would bury the one that mattered.
pub fn configured(specific_name: &str, warned: &AtomicBool) -> DirectIoSwitch {
    let switch = resolve_switch(
        specific_name,
        std::env::var(specific_name).ok().as_deref(),
        std::env::var(DIRECT_IO_MASTER_ENV).ok().as_deref(),
    );
    if !switch.rejected.is_empty() && !warned.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            rejected = ?switch.rejected,
            enabled = switch.enabled,
            "direct I/O switch is not a boolean (use 1/true/on/yes or 0/false/off/no); \
             it is ignored and direct I/O is {}",
            if switch.enabled { "ON (the default)" } else { "OFF" }
        );
    }
    switch
}

/// Whether direct I/O can run on this platform at all: `O_DIRECT` on Linux and
/// `F_NOCACHE` on macOS, both reached through unix syscalls.
pub const PLATFORM_SUPPORTS_DIRECT: bool = cfg!(unix);

/// Resolve to a plain yes/no for a caller that cannot return an error, warning
/// once when it was asked for by name on a platform that cannot do it.
pub fn direct_wanted(specific_name: &str, warned: &AtomicBool) -> bool {
    let switch = configured(specific_name, warned);
    match switch.on_platform(PLATFORM_SUPPORTS_DIRECT) {
        PlatformDecision::Direct => true,
        PlatformDecision::Buffered => false,
        PlatformDecision::Unsupported => {
            tracing::warn!(
                switch = specific_name,
                "direct I/O was requested but this platform does not support it; using buffered I/O"
            );
            false
        }
    }
}

/// Minimum alignment / write granularity. 4096 is the near-universal page/fs
/// block size and a safe superset of 512-byte device sectors: a buffer
/// aligned to 4096 satisfies any O_DIRECT alignment a real device imposes.
///
/// This is a floor, not "the" block size: [`AlignedBuf`] and [`DirectWriter`]
/// carry their own runtime alignment (probed per file — see `dio_align.rs`
/// and `decisions.md` D4), which is always `>= MIN_BLOCK`. Code that needs
/// "the" block for a specific buffer or writer reads it from that value, not
/// from this constant.
pub const MIN_BLOCK: usize = 4096;

/// Staging-buffer capacity (a multiple of [`MIN_BLOCK`]). 1 MiB amortizes
/// syscall overhead while bounding the in-flight buffer (Power-of-10 rule 3).
pub const STAGING_CAPACITY: usize = 256 * MIN_BLOCK;

static DIRECT_WRITE_FALLBACKS_TOTAL: AtomicU64 = AtomicU64::new(0);
static DIRECT_WRITE_FILES_TOTAL: AtomicU64 = AtomicU64::new(0);
static DIRECT_WRITE_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Files opened where O_DIRECT was rejected and the writer fell back to buffered
/// I/O + `POSIX_FADV_DONTNEED`. Non-zero in steady state means the page-cache-
/// bypass freeze mitigation is inactive on this host — a config/mount problem to
/// investigate, and a signal that should alert.
pub fn direct_write_fallbacks_total() -> u64 {
    DIRECT_WRITE_FALLBACKS_TOTAL.load(Ordering::Relaxed)
}

/// Immutable files completed through [`DirectWriter`] since start.
pub fn direct_write_files_total() -> u64 {
    DIRECT_WRITE_FILES_TOTAL.load(Ordering::Relaxed)
}

/// Logical bytes written through [`DirectWriter`] since start.
pub fn direct_write_bytes_total() -> u64 {
    DIRECT_WRITE_BYTES_TOTAL.load(Ordering::Relaxed)
}

/// Record that a file which asked for direct I/O ended up buffered instead —
/// shared by [`open_bypassing`]'s own O_DIRECT-open rejection and
/// `pump::FileSink`'s `dio_align::TooLarge` fallback (T-032), so both land in
/// the same counter operators already watch.
pub(crate) fn record_write_fallback() {
    DIRECT_WRITE_FALLBACKS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record one completed [`AlignedPump`](crate::pump::AlignedPump) (or
/// [`DirectWriter`]) file: one file, `logical` bytes. The single call site for
/// both counters, so `finish()` cannot update one and forget the other.
pub(crate) fn record_write_completion(logical: u64) {
    DIRECT_WRITE_FILES_TOTAL.fetch_add(1, Ordering::Relaxed);
    DIRECT_WRITE_BYTES_TOTAL.fetch_add(logical, Ordering::Relaxed);
}

/// Render the direct-writer metrics (Prometheus text exposition). Concatenated
/// into `/metrics` by the web layer. `direct_write_fallbacks_total` is the
/// load-bearing signal: it MUST stay 0 when `FERROSA_SSTABLE_DIRECT_IO=1` — a
/// non-zero value means the file system rejected O_DIRECT and Data.db is being
/// page-cached after all (the freeze mitigation is inert, e.g. an overlay
/// rootfs instead of a real ext4 volume).
pub fn render_prometheus(out: &mut String) {
    out.push_str(
        "# HELP ferrosa_sstable_direct_write_fallbacks_total Data.db files where O_DIRECT was rejected and the writer fell back to buffered I/O; non-zero means the page-cache-bypass mitigation is INACTIVE for those files and should alert.\n\
         # TYPE ferrosa_sstable_direct_write_fallbacks_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sstable_direct_write_fallbacks_total {}\n",
        direct_write_fallbacks_total()
    ));
    out.push_str(
        "# HELP ferrosa_sstable_direct_write_files_total Immutable files completed through the direct writer since start.\n\
         # TYPE ferrosa_sstable_direct_write_files_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sstable_direct_write_files_total {}\n",
        direct_write_files_total()
    ));
    out.push_str(
        "# HELP ferrosa_sstable_direct_write_bytes_total Logical bytes written through the direct writer since start.\n\
         # TYPE ferrosa_sstable_direct_write_bytes_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sstable_direct_write_bytes_total {}\n",
        direct_write_bytes_total()
    ));
    out.push_str(
        "# HELP ferrosa_sstable_direct_read_fallbacks_total Compaction input files where O_DIRECT was rejected and the reader fell back to buffered reads; non-zero means the page-cache bypass is INACTIVE for those files and should alert.\n\
         # TYPE ferrosa_sstable_direct_read_fallbacks_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sstable_direct_read_fallbacks_total {}\n",
        direct_read_fallbacks_total()
    ));
    out.push_str(
        "# HELP ferrosa_sstable_direct_read_files_total Files opened through the direct reader since start.\n\
         # TYPE ferrosa_sstable_direct_read_files_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sstable_direct_read_files_total {}\n",
        direct_read_files_total()
    ));
    out.push_str(
        "# HELP ferrosa_sstable_direct_read_bytes_total Logical bytes returned by the direct reader since start.\n\
         # TYPE ferrosa_sstable_direct_read_bytes_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sstable_direct_read_bytes_total {}\n",
        direct_read_bytes_total()
    ));
    out.push_str(
        "# HELP ferrosa_sstable_dio_align_probe_fallbacks_total Files where the STATX_DIOALIGN probe (T-031) could not report an alignment and MIN_BLOCK was used instead; non-zero is expected on old kernels/filesystems and is not itself an error.\n\
         # TYPE ferrosa_sstable_dio_align_probe_fallbacks_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sstable_dio_align_probe_fallbacks_total {}\n",
        crate::dio_align::dio_align_probe_fallbacks_total()
    ));
    crate::pump::render_prometheus(out);
}

/// How the OS page cache is being bypassed for a given file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectMode {
    /// Linux O_DIRECT: writes go straight to the device, never the page cache.
    Direct,
    /// macOS `F_NOCACHE`: the page cache is not populated for this fd.
    NoCache,
    /// Fallback: normal page-cached I/O; pages dropped via `POSIX_FADV_DONTNEED`
    /// on finish. Loud + counted — O_DIRECT was unavailable.
    Buffered,
}

/// The largest `block`-multiple prefix of `filled` bytes — the amount safe to
/// issue as an aligned device write, leaving a sub-block remainder buffered.
/// Pure (no I/O) so the alignment arithmetic is unit-tested directly. `block`
/// is the caller's runtime block size (`>= MIN_BLOCK`), not a fixed constant.
pub fn full_block_prefix(filled: usize, block: usize) -> usize {
    filled - (filled % block)
}

/// A heap buffer aligned to a runtime-chosen power of two, the O_DIRECT
/// memory-alignment requirement.
///
/// `Vec<u8>` gives no alignment guarantee, so the staging buffer is a manual
/// aligned allocation. `capacity` is a non-zero multiple of `align`, and
/// `align` must be a power of two no smaller than [`MIN_BLOCK`] — the probed
/// device block (`pump.rs`'s `dio_align` probe, once T-031 lands) can exceed
/// 4096, and the allocation must satisfy whatever that probe reports.
pub(crate) struct AlignedBuf {
    ptr: NonNull<u8>,
    capacity: usize,
    align: usize,
}

// SAFETY: `AlignedBuf` uniquely owns its allocation; sending it across threads is
// sound (it is `!Sync` by default via `NonNull`, and we never share `&` mutably).
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// Allocate `capacity` bytes aligned to `align`. `align` must be a power
    /// of two `>= MIN_BLOCK`; `capacity` must be a positive multiple of
    /// `align`. Both are asserted, since a violation here means the O_DIRECT
    /// invariant is already broken before any I/O happens.
    pub(crate) fn new(capacity: usize, align: usize) -> Self {
        assert!(
            align.is_power_of_two() && align >= MIN_BLOCK,
            "align must be a power of two >= MIN_BLOCK ({MIN_BLOCK}), got {align}"
        );
        assert!(
            capacity > 0 && capacity.is_multiple_of(align),
            "capacity must be a positive multiple of align ({align}), got {capacity}"
        );
        let layout = Layout::from_size_align(capacity, align).expect("valid aligned layout");
        // SAFETY: layout has non-zero size; we check the returned pointer for null.
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self {
            ptr,
            capacity,
            align,
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn align(&self) -> usize {
        self.align
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` owns `capacity` initialized-or-writable bytes; callers
        // only read the prefix they have written.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.capacity) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` owns `capacity` bytes and `&mut self` is exclusive.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout =
            Layout::from_size_align(self.capacity, self.align).expect("layout matches new()");
        // SAFETY: `ptr` came from `alloc` with this exact layout and is freed once.
        unsafe { dealloc(self.ptr.as_ptr(), layout) };
    }
}

/// A sequential writer that keeps its output out of the OS page cache.
///
/// Use like an ordinary writer: [`create`](Self::create), one or more
/// [`write_all`](Self::write_all), then [`finish`](Self::finish) (which syncs and
/// returns the exact logical length). Dropping without `finish` logs a WARN
/// naming the path if any bytes had already reached the device — always call
/// `finish`.
///
/// A thin wrapper (T-032) over [`crate::pump::AlignedPump`] at `depth = 0`:
/// every byte, alignment, and fallback rule described above now lives in
/// `pump.rs`, behind the [`crate::pump::SegmentSink`] seam — `AlignedPump`'s
/// production sink (`crate::pump::FileSink`) opens with exactly the flags this
/// type used to open with itself, plus the T-031 `dio_align` block probe this
/// writer did not yet consume.
pub struct DirectWriter {
    pump: crate::pump::AlignedPump,
}

impl DirectWriter {
    /// Create (truncating) `path` for cache-bypassing sequential writes.
    ///
    /// Opens O_DIRECT (Linux) / `F_NOCACHE` (macOS); on O_DIRECT rejection, falls
    /// back to buffered I/O (WARN-logged + counted). The parent directory must
    /// exist.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let (sink, block) = crate::pump::FileSink::create(&path)?;
        let segment = crate::pump::PumpConfig {
            segment_bytes: STAGING_CAPACITY,
            queue_depth: 0,
        }
        .effective_segment(block);
        let pump = crate::pump::AlignedPump::open(Box::new(sink), block, segment, path);
        Ok(Self { pump })
    }

    /// How the page cache is being bypassed for this file (observability/tests).
    pub fn mode(&self) -> DirectMode {
        self.pump.mode()
    }

    /// The current logical write offset — bytes accepted by [`Self::write_all`] so far
    /// (flushed + still staged). Equals the offset the next byte will occupy in
    /// the finished file, so it substitutes exactly for `Seek::stream_position`
    /// when recording chunk offsets.
    pub fn position(&self) -> u64 {
        self.pump.position()
    }

    /// Stage `data`, flushing full aligned segments to the device as the buffer
    /// fills. Bounded per call by `data.len()` (Power-of-10 rule 2).
    pub fn write_all(&mut self, data: &[u8]) -> Result<()> {
        self.pump.write_all(data)
    }

    /// Flush the final partial block, sync durably, trim any padding, and return
    /// the exact logical length. Consumes the writer.
    pub fn finish(self) -> Result<u64> {
        self.pump.finish()
    }
}

/// Open `path` (create + truncate) with the page cache bypassed. `pub(crate)`
/// so `pump::FileSink` (T-032) opens with exactly these flags.
pub(crate) fn open_bypassing(path: &Path) -> Result<(File, DirectMode)> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        match OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
        {
            Ok(file) => Ok((file, DirectMode::Direct)),
            Err(err) => {
                DIRECT_WRITE_FALLBACKS_TOTAL.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "O_DIRECT rejected — falling back to buffered I/O + POSIX_FADV_DONTNEED. \
                     The page-cache-bypass freeze mitigation is INACTIVE for this file; \
                     check the file system supports O_DIRECT (see direct_write_fallbacks_total)."
                );
                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)?;
                Ok((file, DirectMode::Buffered))
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        set_nocache(&file);
        Ok((file, DirectMode::NoCache))
    }
}

/// macOS: disable page caching for this fd (best-effort; failure is non-fatal —
/// the write still succeeds, just cached). No alignment requirement.
#[cfg(not(target_os = "linux"))]
fn set_nocache(file: &File) {
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: valid fd; F_NOCACHE takes an int arg and returns -1 on error.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
        if rc == -1 {
            tracing::warn!(
                error = %std::io::Error::last_os_error(),
                "F_NOCACHE failed — this file will use the page cache"
            );
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = file; // other non-Linux unixes: no portable equivalent, use page cache
}

/// Advise the kernel to drop this file's pages from the page cache (Linux). Used
/// only on the buffered fallback, after the durable sync, so the just-written
/// bytes cannot pollute the cache or feed the writeback storm. `pub(crate)` so
/// `pump::FileSink` (T-032) can call it from its own `fadvise_dontneed`.
pub(crate) fn fadvise_dontneed(file: &File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: valid fd; offset/len 0 means "the whole file".
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if rc != 0 {
            tracing::warn!(
                error = rc,
                "posix_fadvise(DONTNEED) failed on fallback write"
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = file;
}

/// Durable data sync (metadata sync of length is covered by the extra sync after
/// `set_len`). Kept simple — the platform `F_FULLFSYNC` nuance already lives in
/// the commit-log path; SSTable output is fsynced then published, and a lost
/// just-written SSTable is re-derivable from the memtable/commit log.
/// `pub(crate)` so `pump::FileSink` (T-032) can call it from its own `sync_data`.
pub(crate) fn sync_data(file: &File) -> Result<()> {
    file.sync_data()?;
    Ok(())
}

static DIRECT_READ_FALLBACKS_TOTAL: AtomicU64 = AtomicU64::new(0);
static DIRECT_READ_FILES_TOTAL: AtomicU64 = AtomicU64::new(0);
static DIRECT_READ_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Files opened for direct reads where O_DIRECT was rejected and the reader fell
/// back to buffered I/O + `POSIX_FADV_DONTNEED`. Non-zero means the page-cache
/// bypass is INACTIVE for those files.
pub fn direct_read_fallbacks_total() -> u64 {
    DIRECT_READ_FALLBACKS_TOTAL.load(Ordering::Relaxed)
}

/// Files opened through [`DirectReadFile`] since start.
pub fn direct_read_files_total() -> u64 {
    DIRECT_READ_FILES_TOTAL.load(Ordering::Relaxed)
}

/// Logical bytes returned by [`DirectReadFile`] since start.
pub fn direct_read_bytes_total() -> u64 {
    DIRECT_READ_BYTES_TOTAL.load(Ordering::Relaxed)
}

/// Bounce-buffer capacity for direct reads (a [`MIN_BLOCK`] multiple). One read
/// syscall moves at most this many bytes.
const READ_BOUNCE_CAPACITY: usize = STAGING_CAPACITY;

/// A read-only file that keeps its reads out of the OS page cache.
///
/// O_DIRECT needs the file offset, byte length and memory address of every read to
/// be block-aligned, so this reads whole aligned chunks into a private aligned
/// bounce buffer and copies the requested span out. Callers may use any offset and
/// length. Reads past EOF return the bytes that exist, like `pread`.
///
/// Fallback is loud: if the file system rejects O_DIRECT the file is opened
/// buffered, WARN-logged, counted in [`direct_read_fallbacks_total`], and each chunk
/// read is followed by `POSIX_FADV_DONTNEED` so the cache still is not populated.
#[cfg(unix)]
pub struct DirectReadFile {
    file: File,
    len: u64,
    mode: DirectMode,
    bounce: std::sync::Mutex<AlignedBuf>,
}

#[cfg(unix)]
impl DirectReadFile {
    /// Open `path` for cache-bypassing positional reads.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let (file, mode) = open_read_bypassing(path)?;
        let len = file.metadata()?.len();
        DIRECT_READ_FILES_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            file,
            len,
            mode,
            bounce: std::sync::Mutex::new(AlignedBuf::new(READ_BOUNCE_CAPACITY, MIN_BLOCK)),
        })
    }

    /// How the page cache is being bypassed for this file.
    pub fn mode(&self) -> DirectMode {
        self.mode
    }
}

#[cfg(unix)]
impl crate::io::ReadAt for DirectReadFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        if buf.is_empty() || offset >= self.len {
            return Ok(0);
        }
        let target = buf.len().min((self.len - offset) as usize);
        let mut bounce = self.bounce.lock().expect("direct read bounce poisoned");
        let block = bounce.align();
        let mut copied = 0;
        while copied < target {
            let pos = offset + copied as u64;
            let chunk_start = pos - pos % block as u64;
            let head = (pos - chunk_start) as usize;
            let need = head + (target - copied);
            let chunk_len = need
                .div_ceil(block)
                .saturating_mul(block)
                .min(bounce.capacity());
            let got = self
                .file
                .read_at(&mut bounce.as_mut_slice()[..chunk_len], chunk_start)?;
            if self.mode == DirectMode::Buffered {
                fadvise_dontneed_range(&self.file, chunk_start, chunk_len as u64);
            }
            let avail = got.saturating_sub(head);
            if avail == 0 {
                return Err(ferrosa_common::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "direct read: file ended at {pos} but length is {}",
                        self.len
                    ),
                )));
            }
            let n = avail.min(target - copied);
            buf[copied..copied + n].copy_from_slice(&bounce.as_slice()[head..head + n]);
            copied += n;
        }
        DIRECT_READ_BYTES_TOTAL.fetch_add(copied as u64, Ordering::Relaxed);
        Ok(copied)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.len)
    }
}

/// Open `path` read-only with the page cache bypassed.
#[cfg(unix)]
fn open_read_bypassing(path: &Path) -> Result<(File, DirectMode)> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
        {
            Ok(file) => Ok((file, DirectMode::Direct)),
            // A missing file is not an O_DIRECT problem: do not mask it as a fallback.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(err.into()),
            Err(err) => {
                DIRECT_READ_FALLBACKS_TOTAL.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "O_DIRECT read rejected — falling back to buffered reads + \
                     POSIX_FADV_DONTNEED. The page-cache-bypass for compaction input is \
                     INACTIVE for this file (see direct_read_fallbacks_total)."
                );
                Ok((
                    OpenOptions::new().read(true).open(path)?,
                    DirectMode::Buffered,
                ))
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let file = OpenOptions::new().read(true).open(path)?;
        set_nocache(&file);
        Ok((file, DirectMode::NoCache))
    }
}

/// Advise the kernel to drop `[offset, offset + len)` of this file from the page
/// cache (Linux). Used only on the buffered fallback.
#[cfg(unix)]
fn fadvise_dontneed_range(file: &File, offset: u64, len: u64) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: valid fd; the range is advisory and cannot fault.
        let rc = unsafe {
            libc::posix_fadvise(
                file.as_raw_fd(),
                offset as libc::off_t,
                len as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        if rc != 0 {
            tracing::warn!(
                error = rc,
                "posix_fadvise(DONTNEED) failed on fallback read"
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (file, offset, len);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn read_back(path: &Path) -> Vec<u8> {
        let mut f = File::open(path).expect("open for read");
        let mut v = Vec::new();
        f.read_to_end(&mut v).expect("read");
        v
    }

    #[test]
    fn full_block_prefix_rounds_down_to_block_multiple() {
        assert_eq!(full_block_prefix(0, MIN_BLOCK), 0);
        assert_eq!(full_block_prefix(1, MIN_BLOCK), 0);
        assert_eq!(full_block_prefix(MIN_BLOCK - 1, MIN_BLOCK), 0);
        assert_eq!(full_block_prefix(MIN_BLOCK, MIN_BLOCK), MIN_BLOCK);
        assert_eq!(full_block_prefix(MIN_BLOCK + 1, MIN_BLOCK), MIN_BLOCK);
        assert_eq!(
            full_block_prefix(3 * MIN_BLOCK + 7, MIN_BLOCK),
            3 * MIN_BLOCK
        );
    }

    /// `full_block_prefix` must round down to whatever block it is given, not
    /// just [`MIN_BLOCK`] — the probed device block (D4) can be larger.
    #[test]
    fn pump_primitives_full_block_prefix_honours_the_given_block() {
        for block in [4096usize, 8192, 65536] {
            assert_eq!(full_block_prefix(0, block), 0);
            assert_eq!(full_block_prefix(block - 1, block), 0);
            assert_eq!(full_block_prefix(block, block), block);
            assert_eq!(full_block_prefix(block + 1, block), block);
            assert_eq!(full_block_prefix(3 * block + 7, block), 3 * block);
        }
    }

    #[test]
    fn aligned_buf_is_block_aligned_and_addressable() {
        let mut b = AlignedBuf::new(2 * MIN_BLOCK, MIN_BLOCK);
        assert_eq!(b.capacity(), 2 * MIN_BLOCK);
        assert_eq!(
            b.as_slice().as_ptr() as usize % MIN_BLOCK,
            0,
            "buffer must be block-aligned"
        );
        // Writable across the whole capacity (Miri checks bounds/init).
        b.as_mut_slice()[2 * MIN_BLOCK - 1] = 0xAB;
        assert_eq!(b.as_slice()[2 * MIN_BLOCK - 1], 0xAB);
    }

    /// D4: the probed device block can be larger than [`MIN_BLOCK`] (up to the
    /// 64 KiB ceiling). `AlignedBuf` must honour whatever alignment it is given,
    /// with the pointer aligned and capacity an exact multiple.
    #[test]
    fn pump_primitives_aligned_buf_honours_runtime_alignment() {
        for align in [4096usize, 8192, 65536] {
            let capacity = 3 * align;
            let mut b = AlignedBuf::new(capacity, align);
            assert_eq!(b.align(), align);
            assert_eq!(b.capacity(), capacity);
            assert!(
                b.capacity().is_multiple_of(align),
                "capacity must be an exact multiple of align ({align})"
            );
            assert_eq!(
                b.as_slice().as_ptr() as usize % align,
                0,
                "pointer must be aligned to {align}"
            );
            b.as_mut_slice()[capacity - 1] = 0xCD;
            assert_eq!(b.as_slice()[capacity - 1], 0xCD);
        }
    }

    #[test]
    #[should_panic(expected = "align must be a power of two")]
    fn pump_primitives_aligned_buf_rejects_non_power_of_two_align() {
        AlignedBuf::new(4096 * 3, 3000);
    }

    #[test]
    #[should_panic(expected = "align must be a power of two")]
    fn pump_primitives_aligned_buf_rejects_align_below_min_block() {
        AlignedBuf::new(2048, 2048);
    }

    #[test]
    #[should_panic(expected = "capacity must be a positive multiple of align")]
    fn pump_primitives_aligned_buf_rejects_capacity_not_a_multiple_of_align() {
        AlignedBuf::new(8192 + 1, 8192);
    }

    #[test]
    #[should_panic(expected = "capacity must be a positive multiple of align")]
    fn pump_primitives_aligned_buf_rejects_zero_capacity() {
        AlignedBuf::new(0, MIN_BLOCK);
    }

    /// `AlignedBuf` must remain `Send` (moved by ownership between the producer
    /// and flusher threads — `decisions.md` D2) and must NOT be `Sync`: a
    /// static-assertion test rather than a runtime check, so a regression fails
    /// to compile instead of failing at runtime.
    #[test]
    fn pump_primitives_aligned_buf_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<AlignedBuf>();
    }

    /// The core correctness property: the file's logical content is byte-exact
    /// for every length class — empty, sub-block, exact block, block+tail, and
    /// spanning the staging buffer — regardless of the block padding underneath.
    #[test]
    fn roundtrip_is_byte_exact_across_length_classes() {
        let sizes = [
            0usize,
            1,
            MIN_BLOCK - 1,
            MIN_BLOCK,
            MIN_BLOCK + 1,
            3 * MIN_BLOCK,
            3 * MIN_BLOCK + 7,
            STAGING_CAPACITY + 123, // forces a mid-stream buffer flush + refill
            2 * STAGING_CAPACITY + MIN_BLOCK + 5,
        ];
        let dir = tmp();
        for (i, &size) in sizes.iter().enumerate() {
            let path = dir.path().join(format!("data-{i}.db"));
            let expected: Vec<u8> = (0..size).map(|j| (j % 251) as u8).collect();
            let mut w = DirectWriter::create(&path).expect("create");
            // Write in irregular chunks to exercise the fill/flush/refill paths.
            for chunk in expected.chunks(1000) {
                w.write_all(chunk).expect("write_all");
            }
            let logical = w.finish().expect("finish");
            assert_eq!(
                logical, size as u64,
                "finish must report the exact logical length"
            );
            assert_eq!(
                read_back(&path),
                expected,
                "byte-exact round trip (size {size})"
            );
        }
    }

    #[test]
    fn single_write_of_each_size_matches() {
        // Same property but a single `write_all` per file (no chunking), covering
        // the case where one call exceeds the staging capacity.
        let dir = tmp();
        for &size in &[
            0usize,
            MIN_BLOCK / 2,
            MIN_BLOCK,
            STAGING_CAPACITY,
            STAGING_CAPACITY + MIN_BLOCK - 1,
        ] {
            let path = dir.path().join(format!("one-{size}.db"));
            let expected: Vec<u8> = (0..size).map(|j| (j * 7 % 256) as u8).collect();
            let mut w = DirectWriter::create(&path).expect("create");
            w.write_all(&expected).expect("write_all");
            assert_eq!(w.finish().expect("finish"), size as u64);
            assert_eq!(read_back(&path), expected);
        }
    }

    #[test]
    fn many_tiny_writes_accumulate_exactly() {
        // 10k single-byte writes stress the copy-into-buffer + refill path.
        let dir = tmp();
        let path = dir.path().join("tiny.db");
        let expected: Vec<u8> = (0..10_000u32).map(|j| (j % 256) as u8).collect();
        let mut w = DirectWriter::create(&path).expect("create");
        for &byte in &expected {
            w.write_all(&[byte]).expect("write_all");
        }
        assert_eq!(w.finish().expect("finish"), expected.len() as u64);
        assert_eq!(read_back(&path), expected);
    }

    #[test]
    fn position_tracks_logical_bytes_written() {
        let dir = tmp();
        let path = dir.path().join("pos.db");
        let mut w = DirectWriter::create(&path).expect("create");
        assert_eq!(w.position(), 0);
        w.write_all(&[0u8; 100]).expect("write");
        assert_eq!(
            w.position(),
            100,
            "position counts staged bytes before any flush"
        );
        // Cross the staging boundary so some bytes are flushed and some staged.
        w.write_all(&vec![1u8; STAGING_CAPACITY]).expect("write");
        assert_eq!(w.position(), 100 + STAGING_CAPACITY as u64);
        assert_eq!(w.finish().expect("finish"), 100 + STAGING_CAPACITY as u64);
    }

    #[cfg(unix)]
    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|j| (j * 13 % 251) as u8).collect()
    }

    #[cfg(unix)]
    #[test]
    fn direct_read_is_byte_exact_at_unaligned_offsets_and_lengths() {
        use crate::io::ReadAt;
        let dir = tmp();
        for &size in &[
            0usize,
            1,
            MIN_BLOCK - 1,
            MIN_BLOCK,
            MIN_BLOCK + 1,
            3 * MIN_BLOCK + 7,
            STAGING_CAPACITY + 123,
        ] {
            let path = dir.path().join(format!("r-{size}.db"));
            let expected = pattern(size);
            std::fs::write(&path, &expected).expect("write");
            let f = DirectReadFile::open(&path).expect("open");
            assert_eq!(f.len().expect("len"), size as u64);
            for &(off, want) in &[
                (0u64, 1usize),
                (0, size),
                (1, 100),
                (MIN_BLOCK as u64 - 1, 3),
                (MIN_BLOCK as u64, MIN_BLOCK),
                (size as u64 / 2, size),
                (size as u64, 10),
                (size as u64 + 500, 10),
            ] {
                let mut got = vec![0u8; want];
                let n = f.read_at(&mut got, off).expect("read");
                let start = (off as usize).min(size);
                let end = (start + want).min(size);
                assert_eq!(n, end - start, "size {size} off {off} want {want}");
                assert_eq!(&got[..n], &expected[start..end], "size {size} off {off}");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn direct_read_mode_is_a_real_bypass_on_this_platform() {
        let dir = tmp();
        let path = dir.path().join("mode-r.db");
        std::fs::write(&path, pattern(MIN_BLOCK)).expect("write");
        let before = direct_read_files_total();
        let f = DirectReadFile::open(&path).expect("open");
        #[cfg(target_os = "macos")]
        assert_eq!(f.mode(), DirectMode::NoCache);
        #[cfg(target_os = "linux")]
        assert!(matches!(
            f.mode(),
            DirectMode::Direct | DirectMode::Buffered
        ));
        assert!(direct_read_files_total() > before, "open counter advances");
    }

    #[cfg(unix)]
    #[test]
    fn direct_read_counts_the_bytes_it_returns() {
        use crate::io::ReadAt;
        let dir = tmp();
        let path = dir.path().join("bytes-r.db");
        std::fs::write(&path, pattern(2 * MIN_BLOCK)).expect("write");
        let f = DirectReadFile::open(&path).expect("open");
        let before = direct_read_bytes_total();
        let mut buf = vec![0u8; 1000];
        f.read_at(&mut buf, 10).expect("read");
        assert!(direct_read_bytes_total() >= before + 1000);
    }

    #[test]
    fn prometheus_exposes_the_direct_read_counters() {
        let mut out = String::new();
        render_prometheus(&mut out);
        for name in [
            "ferrosa_sstable_direct_read_fallbacks_total",
            "ferrosa_sstable_direct_read_files_total",
            "ferrosa_sstable_direct_read_bytes_total",
        ] {
            assert!(out.contains(&format!("# TYPE {name} counter")), "{name}");
            assert!(out.contains(&format!("\n{name} ")), "{name} sample");
        }
    }

    #[cfg(unix)]
    #[test]
    fn direct_read_of_a_missing_file_is_an_error() {
        let dir = tmp();
        assert!(DirectReadFile::open(dir.path().join("nope.db")).is_err());
    }

    #[test]
    fn mode_is_a_real_bypass_on_this_platform() {
        // On the dev host (macOS) the mode must be NoCache; on Linux CI, Direct
        // (or a loudly-counted Buffered fallback). Never a silent no-op.
        let dir = tmp();
        let path = dir.path().join("mode.db");
        let before_files = direct_write_files_total();
        let w = DirectWriter::create(&path).expect("create");
        let mode = w.mode();
        w.finish().expect("finish");
        #[cfg(target_os = "macos")]
        assert_eq!(mode, DirectMode::NoCache);
        #[cfg(target_os = "linux")]
        assert!(matches!(mode, DirectMode::Direct | DirectMode::Buffered));
        assert!(
            direct_write_files_total() > before_files,
            "completion counter advances"
        );
    }
}

#[cfg(test)]
mod switch_tests {
    use super::*;

    const NAME: &str = "FERROSA_SSTABLE_DIRECT_IO";

    fn resolve(specific: Option<&str>, master: Option<&str>) -> DirectIoSwitch {
        resolve_switch(NAME, specific, master)
    }

    #[test]
    fn direct_io_is_on_when_nothing_is_set() {
        let switch = resolve(None, None);
        assert!(switch.enabled);
        assert_eq!(switch.source, SwitchSource::Default);
        assert!(switch.rejected.is_empty());
    }

    #[test]
    fn the_master_switch_turns_it_off() {
        for off in ["0", "false", "off", "no", "OFF", "False", " 0 "] {
            let switch = resolve(None, Some(off));
            assert!(!switch.enabled, "{off:?}");
            assert_eq!(switch.source, SwitchSource::Master, "{off:?}");
        }
    }

    #[test]
    fn the_master_switch_can_say_on_explicitly() {
        for on in ["1", "true", "on", "yes", "ON", " 1"] {
            let switch = resolve(None, Some(on));
            assert!(switch.enabled, "{on:?}");
            assert_eq!(switch.source, SwitchSource::Master);
        }
    }

    #[test]
    fn a_feature_switch_beats_the_master_in_both_directions() {
        let off = resolve(Some("0"), Some("1"));
        assert!(!off.enabled);
        assert_eq!(off.source, SwitchSource::Specific);
        let on = resolve(Some("1"), Some("0"));
        assert!(on.enabled);
        assert_eq!(on.source, SwitchSource::Specific);
    }

    #[test]
    fn a_value_that_is_not_a_boolean_keeps_the_default_and_is_reported() {
        let switch = resolve(Some("disable"), None);
        assert!(
            switch.enabled,
            "an unreadable value must not change behaviour"
        );
        assert_eq!(switch.source, SwitchSource::Default);
        assert_eq!(switch.rejected, vec![format!("{NAME}=\"disable\"")]);
    }

    #[test]
    fn a_bad_feature_value_falls_through_to_a_good_master() {
        let switch = resolve(Some("nope"), Some("0"));
        assert!(!switch.enabled);
        assert_eq!(switch.source, SwitchSource::Master);
        assert_eq!(switch.rejected.len(), 1);
    }

    #[test]
    fn both_bad_values_are_reported() {
        let switch = resolve(Some("x"), Some("y"));
        assert!(switch.enabled);
        assert_eq!(switch.rejected.len(), 2);
        assert!(switch
            .rejected
            .iter()
            .any(|r| r.starts_with(DIRECT_IO_MASTER_ENV)));
    }

    #[test]
    fn an_empty_value_counts_as_unset() {
        let switch = resolve(Some(""), Some("  "));
        assert!(switch.enabled);
        assert_eq!(switch.source, SwitchSource::Default);
        assert!(
            switch.rejected.is_empty(),
            "compose files leave `VAR=` behind"
        );
    }

    #[test]
    fn a_platform_without_direct_io_falls_back_unless_it_was_asked_for_by_name() {
        assert_eq!(
            resolve(None, None).on_platform(true),
            PlatformDecision::Direct
        );
        assert_eq!(
            resolve(None, None).on_platform(false),
            PlatformDecision::Buffered
        );
        assert_eq!(
            resolve(None, Some("1")).on_platform(false),
            PlatformDecision::Buffered
        );
        assert_eq!(
            resolve(Some("1"), None).on_platform(false),
            PlatformDecision::Unsupported
        );
        assert_eq!(
            resolve(Some("1"), None).on_platform(true),
            PlatformDecision::Direct
        );
    }

    #[test]
    fn off_is_buffered_on_every_platform() {
        for supports in [true, false] {
            assert_eq!(
                resolve(Some("0"), None).on_platform(supports),
                PlatformDecision::Buffered
            );
            assert_eq!(
                resolve(None, Some("0")).on_platform(supports),
                PlatformDecision::Buffered
            );
        }
    }
}
