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
use std::sync::atomic::{AtomicU64, Ordering};

use ferrosa_common::Result;

/// Alignment / write granularity. 4096 is the near-universal page/fs block size
/// and a safe superset of 512-byte device sectors: a buffer aligned to 4096
/// satisfies any O_DIRECT alignment a real device imposes.
pub const BLOCK: usize = 4096;

/// Staging-buffer capacity (a multiple of [`BLOCK`]). 1 MiB amortizes syscall
/// overhead while bounding the in-flight buffer (Power-of-10 rule 3).
pub const STAGING_CAPACITY: usize = 256 * BLOCK;

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

/// The largest [`BLOCK`]-multiple prefix of `filled` bytes — the amount safe to
/// issue as an aligned device write, leaving a sub-block remainder buffered.
/// Pure (no I/O) so the alignment arithmetic is unit-tested directly.
pub fn full_block_prefix(filled: usize) -> usize {
    filled - (filled % BLOCK)
}

/// A heap buffer aligned to [`BLOCK`], the O_DIRECT memory-alignment requirement.
///
/// `Vec<u8>` gives no alignment guarantee, so the staging buffer is a manual
/// aligned allocation. `capacity` is a non-zero multiple of [`BLOCK`].
struct AlignedBuf {
    ptr: NonNull<u8>,
    capacity: usize,
}

// SAFETY: `AlignedBuf` uniquely owns its allocation; sending it across threads is
// sound (it is `!Sync` by default via `NonNull`, and we never share `&` mutably).
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// Allocate `capacity` bytes aligned to [`BLOCK`]. `capacity` must be a
    /// non-zero multiple of [`BLOCK`].
    fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0 && capacity.is_multiple_of(BLOCK),
            "capacity must be a positive BLOCK multiple"
        );
        let layout = Layout::from_size_align(capacity, BLOCK).expect("valid aligned layout");
        // SAFETY: layout has non-zero size; we check the returned pointer for null.
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self { ptr, capacity }
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` owns `capacity` initialized-or-writable bytes; callers
        // only read the prefix they have written.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.capacity) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` owns `capacity` bytes and `&mut self` is exclusive.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, BLOCK).expect("layout matches new()");
        // SAFETY: `ptr` came from `alloc` with this exact layout and is freed once.
        unsafe { dealloc(self.ptr.as_ptr(), layout) };
    }
}

/// A sequential writer that keeps its output out of the OS page cache.
///
/// Use like an ordinary writer: [`create`](Self::create), one or more
/// [`write_all`](Self::write_all), then [`finish`](Self::finish) (which syncs and
/// returns the exact logical length). Dropping without `finish` discards any
/// buffered residual and does NOT sync — always call `finish`, mirroring the
/// existing `write_all` + `sync_data` shape in the SSTable writer.
pub struct DirectWriter {
    file: File,
    buf: AlignedBuf,
    /// Bytes staged in `buf` not yet flushed to the device (always `< BLOCK`
    /// after any flush; `<= capacity` transiently while filling).
    filled: usize,
    /// Physical bytes already written to the device (always a [`BLOCK`] multiple).
    physical: u64,
    mode: DirectMode,
}

impl DirectWriter {
    /// Create (truncating) `path` for cache-bypassing sequential writes.
    ///
    /// Opens O_DIRECT (Linux) / `F_NOCACHE` (macOS); on O_DIRECT rejection, falls
    /// back to buffered I/O (WARN-logged + counted). The parent directory must
    /// exist.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let (file, mode) = open_bypassing(path)?;
        Ok(Self {
            file,
            buf: AlignedBuf::new(STAGING_CAPACITY),
            filled: 0,
            physical: 0,
            mode,
        })
    }

    /// How the page cache is being bypassed for this file (observability/tests).
    pub fn mode(&self) -> DirectMode {
        self.mode
    }

    /// The current logical write offset — bytes accepted by [`write_all`] so far
    /// (flushed + still staged). Equals the offset the next byte will occupy in
    /// the finished file, so it substitutes exactly for `Seek::stream_position`
    /// when recording chunk offsets.
    pub fn position(&self) -> u64 {
        self.physical + self.filled as u64
    }

    /// Stage `data`, flushing full aligned blocks to the device as the buffer
    /// fills. Bounded per call by `data.len()` (Power-of-10 rule 2).
    pub fn write_all(&mut self, mut data: &[u8]) -> Result<()> {
        while !data.is_empty() {
            let space = self.buf.capacity() - self.filled;
            let n = space.min(data.len());
            let start = self.filled;
            self.buf.as_mut_slice()[start..start + n].copy_from_slice(&data[..n]);
            self.filled += n;
            data = &data[n..];
            if self.filled == self.buf.capacity() {
                self.flush_full_blocks()?;
            }
        }
        Ok(())
    }

    /// Write the largest block-multiple prefix of the staged bytes to the device
    /// and shift any sub-block remainder to the front of the buffer.
    fn flush_full_blocks(&mut self) -> Result<()> {
        let flush_len = full_block_prefix(self.filled);
        if flush_len == 0 {
            return Ok(());
        }
        write_at_offset(&mut self.file, &self.buf.as_slice()[..flush_len])?;
        let remainder = self.filled - flush_len;
        if remainder > 0 {
            self.buf
                .as_mut_slice()
                .copy_within(flush_len..self.filled, 0);
        }
        self.filled = remainder;
        self.physical += flush_len as u64;
        Ok(())
    }

    /// Flush the final partial block, sync durably, trim any padding, and return
    /// the exact logical length. Consumes the writer.
    pub fn finish(mut self) -> Result<u64> {
        self.flush_full_blocks()?;
        let tail = self.filled; // < BLOCK
        let logical = self.physical + tail as u64;
        if tail > 0 {
            // Zero-pad the partial block to a full aligned block, write it, then
            // truncate the padding off — the standard O_DIRECT tail technique.
            self.buf.as_mut_slice()[tail..BLOCK].fill(0);
            write_at_offset(&mut self.file, &self.buf.as_slice()[..BLOCK])?;
            self.physical += BLOCK as u64;
        }
        sync_data(&self.file)?;
        if tail > 0 {
            self.file.set_len(logical)?; // ftruncate off the zero padding
            sync_data(&self.file)?; // persist the new length
        }
        if self.mode == DirectMode::Buffered {
            // Degraded path used the page cache — drop the pages we just wrote so
            // they cannot drive the writeback storm this writer exists to avoid.
            fadvise_dontneed(&self.file);
        }
        DIRECT_WRITE_FILES_TOTAL.fetch_add(1, Ordering::Relaxed);
        DIRECT_WRITE_BYTES_TOTAL.fetch_add(logical, Ordering::Relaxed);
        Ok(logical)
    }
}

/// `write_all` at the file's current (sequential, block-aligned) offset. Split
/// out so the O_DIRECT invariant — buffer pointer + length + offset all aligned —
/// lives in one place. `std::io::Write::write_all` retries partial writes with a
/// suffix slice; because both the offset served and any partial count are block
/// multiples, the retried buffer pointer and length stay aligned.
fn write_at_offset(file: &mut File, block_aligned: &[u8]) -> Result<()> {
    use std::io::Write;
    debug_assert!(
        block_aligned.len().is_multiple_of(BLOCK),
        "device writes must be BLOCK-aligned"
    );
    file.write_all(block_aligned)?;
    Ok(())
}

/// Open `path` (create + truncate) with the page cache bypassed.
fn open_bypassing(path: &Path) -> Result<(File, DirectMode)> {
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
            Ok(file) => return Ok((file, DirectMode::Direct)),
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
                return Ok((file, DirectMode::Buffered));
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
/// bytes cannot pollute the cache or feed the writeback storm.
fn fadvise_dontneed(file: &File) {
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
fn sync_data(file: &File) -> Result<()> {
    file.sync_data()?;
    Ok(())
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
        assert_eq!(full_block_prefix(0), 0);
        assert_eq!(full_block_prefix(1), 0);
        assert_eq!(full_block_prefix(BLOCK - 1), 0);
        assert_eq!(full_block_prefix(BLOCK), BLOCK);
        assert_eq!(full_block_prefix(BLOCK + 1), BLOCK);
        assert_eq!(full_block_prefix(3 * BLOCK + 7), 3 * BLOCK);
    }

    #[test]
    fn aligned_buf_is_block_aligned_and_addressable() {
        let mut b = AlignedBuf::new(2 * BLOCK);
        assert_eq!(b.capacity(), 2 * BLOCK);
        assert_eq!(
            b.as_slice().as_ptr() as usize % BLOCK,
            0,
            "buffer must be BLOCK-aligned"
        );
        // Writable across the whole capacity (Miri checks bounds/init).
        b.as_mut_slice()[2 * BLOCK - 1] = 0xAB;
        assert_eq!(b.as_slice()[2 * BLOCK - 1], 0xAB);
    }

    /// The core correctness property: the file's logical content is byte-exact
    /// for every length class — empty, sub-block, exact block, block+tail, and
    /// spanning the staging buffer — regardless of the block padding underneath.
    #[test]
    fn roundtrip_is_byte_exact_across_length_classes() {
        let sizes = [
            0usize,
            1,
            BLOCK - 1,
            BLOCK,
            BLOCK + 1,
            3 * BLOCK,
            3 * BLOCK + 7,
            STAGING_CAPACITY + 123, // forces a mid-stream buffer flush + refill
            2 * STAGING_CAPACITY + BLOCK + 5,
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
            BLOCK / 2,
            BLOCK,
            STAGING_CAPACITY,
            STAGING_CAPACITY + BLOCK - 1,
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
