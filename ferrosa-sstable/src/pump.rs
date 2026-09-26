//! Module: Runtime tunables and the aligned SSTable write pump.
//! Correctness: An invalid or out-of-bounds env value never changes writer
//!   behavior silently — the default applies and the rejection is logged
//!   exactly once per process, the same rule `direct::configured` uses for
//!   the O_DIRECT switch. `effective_segment` always returns a positive
//!   multiple of the caller's block that is at least as large as the
//!   configured request. `AlignedPump` (depth 0 in this packet) issues one
//!   `SegmentSink::pwrite` per full, block-aligned segment and never a
//!   remainder shuffle (D5); `finish` always reports the exact logical
//!   length regardless of tail padding.
//! Last revised: 2026-09-26
//! Last changed: T-032. Adds the `SegmentSink` seam (`FileSink` as the
//!   production implementation, opening with the same flags
//!   `direct::open_bypassing` always used, now also consuming the T-031
//!   `dio_align` block probe), the synchronous (`depth = 0`) `AlignedPump`,
//!   and `test-support`-gated fault-injection sinks (`RecordingSink`,
//!   `FaultySink`, `GateSink`). `direct::DirectWriter` is now a thin wrapper
//!   over `AlignedPump` — see `ferrosa-suite/specs/sstable-write-pump/
//!   architecture.md` § `AlignedPump`.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use ferrosa_common::{Error, Result};

use crate::checksum::DigestCrc32;
use crate::direct::{full_block_prefix, AlignedBuf, DirectMode, MIN_BLOCK};

/// Env var for the aligned segment size in bytes, before rounding to a block
/// multiple. See [`PumpConfig::from_env`].
pub const SEGMENT_BYTES_ENV: &str = "FERROSA_SSTABLE_WRITE_SEGMENT_BYTES";

/// Env var for the number of segments that may be in flight to the flusher.
/// `0` selects the synchronous (depth-0) pump. See [`PumpConfig::from_env`].
pub const QUEUE_DEPTH_ENV: &str = "FERROSA_SSTABLE_WRITE_QUEUE_DEPTH";

const DEFAULT_SEGMENT_BYTES: usize = 1024 * 1024; // 1 MiB
const MIN_SEGMENT_BYTES: usize = 1;
const MAX_SEGMENT_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

const DEFAULT_QUEUE_DEPTH: usize = 3;
const MIN_QUEUE_DEPTH: usize = 0; // 0 = synchronous
const MAX_QUEUE_DEPTH: usize = 16;

static SEGMENT_BYTES_WARNED: AtomicBool = AtomicBool::new(false);
static QUEUE_DEPTH_WARNED: AtomicBool = AtomicBool::new(false);
static ROUNDING_WARNED: AtomicBool = AtomicBool::new(false);

/// Runtime tunables for the aligned write pump (`decisions.md` § Runtime
/// tunables). Read once when a writer opens; no restart is needed to pick up
/// a new value for the next writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PumpConfig {
    /// Requested segment size in bytes, before rounding to a block multiple.
    pub segment_bytes: usize,
    /// Segments that may be in flight to the flusher. `0` means synchronous.
    pub queue_depth: usize,
}

impl Default for PumpConfig {
    fn default() -> Self {
        Self {
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            queue_depth: DEFAULT_QUEUE_DEPTH,
        }
    }
}

impl PumpConfig {
    /// Read both tunables from the environment. A value that is set but does
    /// not parse as a `usize`, or falls outside its bounds, is rejected: the
    /// default applies and the rejection is logged once per process (never
    /// silently, per the standing order on silent failures).
    pub fn from_env() -> Self {
        Self {
            segment_bytes: resolve_env(
                SEGMENT_BYTES_ENV,
                std::env::var(SEGMENT_BYTES_ENV).ok().as_deref(),
                DEFAULT_SEGMENT_BYTES,
                MIN_SEGMENT_BYTES,
                MAX_SEGMENT_BYTES,
                &SEGMENT_BYTES_WARNED,
            ),
            queue_depth: resolve_env(
                QUEUE_DEPTH_ENV,
                std::env::var(QUEUE_DEPTH_ENV).ok().as_deref(),
                DEFAULT_QUEUE_DEPTH,
                MIN_QUEUE_DEPTH,
                MAX_QUEUE_DEPTH,
                &QUEUE_DEPTH_WARNED,
            ),
        }
    }

    /// Round `segment_bytes` up to a multiple of `block`, with a minimum of
    /// one block (`decisions.md` D5): `round_up(max(segment_bytes, block),
    /// block)`. Logs once per process, at INFO, when rounding changes the
    /// configured value — never at every open, or the one line that mattered
    /// would drown in identical repeats.
    pub fn effective_segment(&self, block: usize) -> usize {
        debug_assert!(block > 0, "block must be positive");
        let wanted = self.segment_bytes.max(block);
        let rounded = wanted.div_ceil(block) * block;
        if rounded != self.segment_bytes && !ROUNDING_WARNED.swap(true, Ordering::Relaxed) {
            tracing::info!(
                configured = self.segment_bytes,
                effective = rounded,
                block,
                "write pump segment size rounded up to a block multiple"
            );
        }
        rounded
    }
}

/// One parsed env value: unset (absent, empty, or blank), a `usize` within
/// `[min, max]`, or invalid (non-numeric or out of bounds). Pure — no env
/// access — so the parsing rules are unit-tested directly without
/// `std::env::set_var`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedBound {
    Unset,
    Value(usize),
    Invalid,
}

fn parse_usize_bounded(value: Option<&str>, min: usize, max: usize) -> ParsedBound {
    let Some(text) = value.map(str::trim).filter(|t| !t.is_empty()) else {
        return ParsedBound::Unset;
    };
    match text.parse::<usize>() {
        Ok(n) if n >= min && n <= max => ParsedBound::Value(n),
        _ => ParsedBound::Invalid,
    }
}

/// Resolve one tunable from an already-read env value: `(resolved, rejected)`.
/// `rejected` is true only when the value was set but unusable — unset and
/// empty are not rejections, they are the normal way to ask for the default.
fn resolve_bounded(value: Option<&str>, default: usize, min: usize, max: usize) -> (usize, bool) {
    match parse_usize_bounded(value, min, max) {
        ParsedBound::Value(n) => (n, false),
        ParsedBound::Unset => (default, false),
        ParsedBound::Invalid => (default, true),
    }
}

/// `resolve_bounded` plus the once-per-process WARN, matching the pattern
/// `direct::configured` uses for the O_DIRECT switch: the writer opens a
/// pump per SSTable, so a line per file would bury the one that mattered.
fn resolve_env(
    name: &str,
    value: Option<&str>,
    default: usize,
    min: usize,
    max: usize,
    warned: &AtomicBool,
) -> usize {
    let (resolved, rejected) = resolve_bounded(value, default, min, max);
    if rejected && !warned.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            var = name,
            value = ?value,
            default,
            min,
            max,
            "invalid value for write pump tunable; using the default"
        );
    }
    resolved
}

/// A destination for one aligned SSTable component's device writes, seamed out
/// so `AlignedPump` can be driven by the real filesystem ([`FileSink`]) or, in
/// tests, by a recording/fault-injecting/permit-gated double
/// (`test-support`-gated below). Every method is a whole-operation contract —
/// `pwrite` writes the entire `buf` or returns `Err`; a sink that silently
/// stores something other than what it was asked to is a bug the caller
/// cannot see except by checking the producer-side digest against what
/// actually landed (see the `FaultySink` tests below).
pub(crate) trait SegmentSink: Send {
    /// Write the whole of `buf` at `offset`, retrying any short device write
    /// internally. Never returns having written only part of `buf`.
    fn pwrite(&mut self, buf: &[u8], offset: u64) -> Result<()>;
    /// Durably sync data already written.
    fn sync_data(&mut self) -> Result<()>;
    /// Truncate (or extend) the file to exactly `len` bytes.
    fn set_len(&mut self, len: u64) -> Result<()>;
    /// Advise the kernel this file's pages are not needed (buffered fallback
    /// only; a no-op for a sink with nothing analogous, e.g. in tests).
    fn fadvise_dontneed(&mut self) -> Result<()>;
    /// How this sink bypasses (or does not bypass) the page cache.
    fn mode(&self) -> DirectMode;
}

/// Whether a short write of `n` bytes, made while more of `buf` remains,
/// violates the O_DIRECT alignment invariant. In direct mode every write
/// except the very last must itself be a block multiple, or retrying the
/// remainder would start at a misaligned offset; buffered/`NoCache` writes
/// have no such requirement and are always retried. Pure, so this one rule is
/// unit-tested directly without a real short write, which cannot be forced
/// portably against a real file.
fn short_write_violates_alignment(mode: DirectMode, n: usize, block: usize) -> bool {
    mode == DirectMode::Direct && !n.is_multiple_of(block)
}

/// Write the whole of `buf` to `file` at `offset`, retrying any short write
/// (`SegmentSink::pwrite`'s whole-buffer contract). `mode`/`block` decide
/// whether a short, non-block-multiple count is recoverable.
fn write_all_at(
    file: &mut std::fs::File,
    buf: &[u8],
    offset: u64,
    mode: DirectMode,
    block: usize,
) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut written = 0usize;
    while written < buf.len() {
        file.seek(SeekFrom::Start(offset + written as u64))?;
        let n = file.write(&buf[written..])?;
        if n == 0 {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "write returned 0 bytes at offset {} ({} bytes remaining)",
                    offset + written as u64,
                    buf.len() - written
                ),
            )));
        }
        written += n;
        if written < buf.len() && short_write_violates_alignment(mode, n, block) {
            return Err(Error::Io(io::Error::other(format!(
                "O_DIRECT short write of {n} bytes at offset {} is not a multiple of \
                 the block ({block}); cannot retry at an aligned offset",
                offset + (written - n) as u64
            ))));
        }
    }
    Ok(())
}

/// The production [`SegmentSink`]: an ordinary file, opened with exactly the
/// flags [`crate::direct::open_bypassing`] always used, plus the T-031
/// `dio_align` block probe `DirectWriter` did not yet consume before T-032.
pub(crate) struct FileSink {
    file: std::fs::File,
    mode: DirectMode,
    block: usize,
}

impl FileSink {
    /// Open `path` for cache-bypassing sequential writes and resolve the
    /// block to align every write to. On Linux, when the probed alignment
    /// exceeds [`crate::dio_align::MAX_BLOCK`], falls back to buffered I/O —
    /// loud (WARN) and counted in the same
    /// `direct_write_fallbacks_total` counter `open_bypassing`'s own
    /// O_DIRECT-rejection fallback uses. Returns the resolved block alongside
    /// the sink so the caller can size its segment.
    pub(crate) fn create(path: &Path) -> Result<(Self, usize)> {
        let (file, mode) = crate::direct::open_bypassing(path)?;
        match mode {
            DirectMode::Direct => match crate::dio_align::block_for(&file) {
                Ok(block) => Ok((Self { file, mode, block }, block)),
                Err(crate::dio_align::TooLarge(probed)) => {
                    crate::direct::record_write_fallback();
                    tracing::warn!(
                        path = %path.display(),
                        probed_block = probed,
                        max_block = crate::dio_align::MAX_BLOCK,
                        "probed O_DIRECT alignment exceeds the ceiling this pump can \
                         satisfy; falling back to buffered I/O + POSIX_FADV_DONTNEED"
                    );
                    let buffered = std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(path)?;
                    Ok((
                        Self {
                            file: buffered,
                            mode: DirectMode::Buffered,
                            block: MIN_BLOCK,
                        },
                        MIN_BLOCK,
                    ))
                }
            },
            DirectMode::Buffered | DirectMode::NoCache => Ok((
                Self {
                    file,
                    mode,
                    block: MIN_BLOCK,
                },
                MIN_BLOCK,
            )),
        }
    }
}

impl SegmentSink for FileSink {
    fn pwrite(&mut self, buf: &[u8], offset: u64) -> Result<()> {
        write_all_at(&mut self.file, buf, offset, self.mode, self.block)
    }

    fn sync_data(&mut self) -> Result<()> {
        crate::direct::sync_data(&self.file)
    }

    fn set_len(&mut self, len: u64) -> Result<()> {
        self.file.set_len(len)?;
        Ok(())
    }

    fn fadvise_dontneed(&mut self) -> Result<()> {
        crate::direct::fadvise_dontneed(&self.file);
        Ok(())
    }

    fn mode(&self) -> DirectMode {
        self.mode
    }
}

/// Wrap a [`SegmentSink`] error with the pump's path and the offset the
/// failing operation was at, so a fault surfaces with enough context to find
/// the file without a debugger (`architecture.md` acceptance criterion 4).
fn wrap_sink_error(path: &Path, offset: u64, err: Error) -> Error {
    Error::Io(io::Error::other(format!(
        "write pump: sink operation failed for {} at offset {offset}: {err}",
        path.display()
    )))
}

/// A synchronous (`depth = 0`) aligned SSTable-component writer: one
/// [`AlignedBuf`] segment, allocated once at [`open`](Self::open), filled by
/// [`write_all`](Self::write_all) and drained with exactly one
/// [`SegmentSink::pwrite`] per full segment (D5 — never a remainder
/// shuffle). [`finish`](Self::finish) pads and writes the final partial
/// block, syncs, trims the padding, and returns the exact logical length.
/// `depth >= 1` (a background flusher over bounded channels) is T-033.
pub(crate) struct AlignedPump {
    sink: Box<dyn SegmentSink>,
    current: AlignedBuf,
    /// Bytes staged in `current`, not yet handed to the sink. Only ever
    /// reaches `current.capacity()` transiently — that exact equality
    /// triggers an immediate flush back to 0 (`write_all`).
    filled: usize,
    /// Physical bytes already handed to `sink.pwrite`; always a multiple of
    /// `block`.
    physical: u64,
    /// `Digest.crc32`, fed on the producer side as bytes are accepted — never
    /// from what the sink reports back, so a sink that lies about what it
    /// stored cannot also lie about the digest (T-011 `checksum.rs`).
    digest: DigestCrc32,
    block: usize,
    mode: DirectMode,
    path: PathBuf,
    finished: bool,
    wrote_anything: bool,
}

impl AlignedPump {
    /// Open a synchronous pump over `sink`, staging into one `segment`-byte
    /// aligned buffer. `segment` must already be a positive multiple of
    /// `block` — callers pass it through
    /// [`PumpConfig::effective_segment`](super::PumpConfig::effective_segment),
    /// which guarantees this (D5).
    pub(crate) fn open(
        sink: Box<dyn SegmentSink>,
        block: usize,
        segment: usize,
        path: PathBuf,
    ) -> Self {
        debug_assert!(
            segment > 0 && segment.is_multiple_of(block),
            "segment must be a positive multiple of block"
        );
        let mode = sink.mode();
        Self {
            sink,
            current: AlignedBuf::new(segment, block),
            filled: 0,
            physical: 0,
            digest: DigestCrc32::new(),
            block,
            mode,
            path,
            finished: false,
            wrote_anything: false,
        }
    }

    /// How the page cache is being bypassed for this file.
    pub(crate) fn mode(&self) -> DirectMode {
        self.mode
    }

    /// The current logical write offset — bytes accepted by
    /// [`Self::write_all`] so far (flushed + still staged).
    pub(crate) fn position(&self) -> u64 {
        self.physical + self.filled as u64
    }

    /// The `Digest.crc32` of every byte accepted by [`Self::write_all`] so
    /// far — the producer-side checksum, independent of what the sink
    /// actually stored. Call before [`Self::finish`] consumes the pump.
    ///
    /// Only this packet's tests call it today (proving the fault-detection
    /// property `pump_sync_faulty_sink_silent_corruption_is_only_caught_by_digest_comparison`
    /// relies on); `DataSink` (T-038) is what wires it into the real
    /// publication-verification path (`publication-safety.md` M2/M3).
    #[allow(dead_code)]
    pub(crate) fn digest(&self) -> u32 {
        self.digest.clone().finalize()
    }

    /// Stage `data`, handing whole aligned segments to the sink as the buffer
    /// fills. Bounded per call by `data.len()` (Power-of-10 rule 2).
    pub(crate) fn write_all(&mut self, mut data: &[u8]) -> Result<()> {
        while !data.is_empty() {
            let space = self.current.capacity() - self.filled;
            let n = space.min(data.len());
            let start = self.filled;
            self.current.as_mut_slice()[start..start + n].copy_from_slice(&data[..n]);
            self.digest.update(&data[..n]);
            self.filled += n;
            data = &data[n..];
            if self.filled == self.current.capacity() {
                self.flush_segment()?;
            }
        }
        Ok(())
    }

    /// Hand the full, already-block-aligned segment to the sink in one
    /// `pwrite` and reset the buffer.
    fn flush_segment(&mut self) -> Result<()> {
        let offset = self.physical;
        self.sink
            .pwrite(self.current.as_slice(), offset)
            .map_err(|e| wrap_sink_error(&self.path, offset, e))?;
        self.wrote_anything = true;
        self.physical += self.filled as u64;
        self.filled = 0;
        Ok(())
    }

    /// Flush the final block-aligned prefix, zero-pad and write the last
    /// partial block, sync durably, trim any padding, and return the exact
    /// logical length. Consumes the pump.
    pub(crate) fn finish(mut self) -> Result<u64> {
        self.finished = true;
        let flush_len = full_block_prefix(self.filled, self.block);
        if flush_len > 0 {
            let offset = self.physical;
            self.sink
                .pwrite(&self.current.as_slice()[..flush_len], offset)
                .map_err(|e| wrap_sink_error(&self.path, offset, e))?;
            self.wrote_anything = true;
            self.physical += flush_len as u64;
        }
        let tail = self.filled - flush_len;
        let logical = self.physical + tail as u64;
        if tail > 0 {
            // Zero-pad the partial block to a full aligned block, write it, then
            // truncate the padding off — the standard O_DIRECT tail technique.
            let block = self.block;
            self.current.as_mut_slice()[flush_len + tail..flush_len + block].fill(0);
            let offset = self.physical;
            self.sink
                .pwrite(
                    &self.current.as_slice()[flush_len..flush_len + block],
                    offset,
                )
                .map_err(|e| wrap_sink_error(&self.path, offset, e))?;
            self.wrote_anything = true;
            self.physical += block as u64;
        }
        self.sink
            .sync_data()
            .map_err(|e| wrap_sink_error(&self.path, self.physical, e))?;
        if tail > 0 {
            self.sink
                .set_len(logical)
                .map_err(|e| wrap_sink_error(&self.path, logical, e))?;
            self.sink
                .sync_data()
                .map_err(|e| wrap_sink_error(&self.path, logical, e))?;
        }
        if self.mode == DirectMode::Buffered {
            // Degraded path used the page cache — drop the pages we just wrote
            // so they cannot drive the writeback storm this pump exists to avoid.
            self.sink
                .fadvise_dontneed()
                .map_err(|e| wrap_sink_error(&self.path, logical, e))?;
        }
        crate::direct::record_write_completion(logical);
        Ok(logical)
    }
}

impl Drop for AlignedPump {
    /// Mirrors the flusher's own drop contract (`architecture.md` § Drop
    /// without finish): if the pump is dropped without `finish` ever running,
    /// and it had already handed at least one segment to the sink, that is a
    /// caller bug worth a WARN naming the path — the file is left for the
    /// caller's own staging cleanup, exactly as `DirectWriter` always
    /// documented.
    fn drop(&mut self) {
        if !self.finished && self.wrote_anything {
            tracing::warn!(
                path = %self.path.display(),
                "AlignedPump dropped without finish(); on-disk content for this \
                 file is incomplete and unsynced"
            );
        }
    }
}

/// Test-only [`SegmentSink`] doubles (T-032). Compiled for this crate's own
/// unit/integration tests (`cfg(test)`) and, behind the `test-support`
/// feature, as part of the crate's compiled dev-support surface generally.
/// Everything here is `pub(crate)` — there is no cross-crate entry point yet,
/// since [`AlignedPump`] and [`SegmentSink`] are themselves crate-internal.
/// T-033/T-038 add the `pub` seam sibling crates (e.g. `ferrosa-storage`,
/// already wired with a `test-support`-featured dev-dependency on this crate)
/// need to inject faults through.
// The `test-support` feature compiles this module outside `cfg(test)` too
// (so it is part of the crate's compiled dev-support surface, matching
// `tests/support/mod.rs`'s `#[allow(dead_code)]` pattern for the same
// reason), but every item here is `pub(crate)`: with the feature on and
// `cfg(test)` off, nothing in the crate's own non-test code calls any of
// it — there is no cross-crate entry point until T-033/T-038 add one. That
// is a real, temporary gap in reachability, not a mistake to silence away
// with dead code the compiler should have caught; the crate's own
// `pump_sync_*` tests (`cfg(test)`) exercise every item here today.
#[cfg_attr(not(test), allow(dead_code))]
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod test_support {
    use super::{DirectMode, Error, Result, SegmentSink};
    use std::collections::HashMap;
    use std::io;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// One recorded [`SegmentSink::pwrite`] call: its offset, length, and the
    /// buffer's address — the raw material for the alignment assertions (L3).
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct RecordedWrite {
        pub offset: u64,
        pub len: usize,
        pub addr: usize,
    }

    #[derive(Default)]
    struct RecordingInner {
        writes: Vec<RecordedWrite>,
        bytes: Vec<u8>,
        sync_data_calls: usize,
        set_len_calls: Vec<u64>,
        fadvise_calls: usize,
    }

    /// A cloneable handle onto a [`RecordingSink`]'s (or a sink built on one,
    /// like [`FaultySink`]/[`GateSink`]) recorded state. Needed because
    /// `AlignedPump::open` takes the sink by `Box<dyn SegmentSink>`, so once a
    /// sink is handed to a pump the concrete type — and any inherent
    /// introspection method on it — is gone; this is the only way tests can
    /// still see what actually landed after `write_all`/`finish`.
    #[derive(Clone)]
    pub(crate) struct RecordingHandle(Arc<Mutex<RecordingInner>>);

    impl RecordingHandle {
        fn lock(&self) -> std::sync::MutexGuard<'_, RecordingInner> {
            self.0.lock().unwrap_or_else(|poison| poison.into_inner())
        }

        pub(crate) fn writes(&self) -> Vec<RecordedWrite> {
            self.lock().writes.clone()
        }

        /// The bytes actually stored — what a real disk would hold after
        /// every call, faulted or not.
        pub(crate) fn bytes(&self) -> Vec<u8> {
            self.lock().bytes.clone()
        }

        pub(crate) fn sync_data_calls(&self) -> usize {
            self.lock().sync_data_calls
        }

        pub(crate) fn set_len_calls(&self) -> Vec<u64> {
            self.lock().set_len_calls.clone()
        }

        pub(crate) fn fadvise_calls(&self) -> usize {
            self.lock().fadvise_calls
        }
    }

    /// An in-memory [`SegmentSink`] that reconstructs the file it would have
    /// produced and records every call for direct assertion — alignment,
    /// offset contiguity, and call counts — without touching disk. Returns a
    /// [`RecordingHandle`] alongside itself so tests can inspect state after
    /// the sink is boxed into an `AlignedPump`.
    pub(crate) struct RecordingSink {
        state: Arc<Mutex<RecordingInner>>,
        mode: DirectMode,
    }

    impl RecordingSink {
        pub(crate) fn new(mode: DirectMode) -> (Self, RecordingHandle) {
            let state = Arc::new(Mutex::new(RecordingInner::default()));
            (
                Self {
                    state: state.clone(),
                    mode,
                },
                RecordingHandle(state),
            )
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, RecordingInner> {
            self.state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
        }
    }

    impl SegmentSink for RecordingSink {
        fn pwrite(&mut self, buf: &[u8], offset: u64) -> Result<()> {
            let mut inner = self.lock();
            inner.writes.push(RecordedWrite {
                offset,
                len: buf.len(),
                addr: buf.as_ptr() as usize,
            });
            let end = offset as usize + buf.len();
            if inner.bytes.len() < end {
                inner.bytes.resize(end, 0);
            }
            inner.bytes[offset as usize..end].copy_from_slice(buf);
            Ok(())
        }

        fn sync_data(&mut self) -> Result<()> {
            self.lock().sync_data_calls += 1;
            Ok(())
        }

        fn set_len(&mut self, len: u64) -> Result<()> {
            let mut inner = self.lock();
            inner.set_len_calls.push(len);
            inner.bytes.truncate(len as usize);
            Ok(())
        }

        fn fadvise_dontneed(&mut self) -> Result<()> {
            self.lock().fadvise_calls += 1;
            Ok(())
        }

        fn mode(&self) -> DirectMode {
            self.mode
        }
    }

    /// One scripted fault, applied at a chosen call index (T-032 test spec).
    /// `Eio`/`Enospc`/`Panic`/`FsyncFail`/`SetLenFail` are hard failures the
    /// pump must surface as `Err` (or an actual panic). The rest are silent
    /// `SegmentSink` contract violations — the call still returns `Ok(())`,
    /// but what actually lands in `stored_bytes()` differs from what was
    /// asked for, and only a digest comparison against the producer-side
    /// `AlignedPump::digest()` can catch them.
    #[derive(Debug, Clone)]
    pub(crate) enum Fault {
        Eio,
        Enospc,
        ShortWrite(usize),
        DropSilently,
        WrongOffset(i64),
        Duplicate,
        StaleBytes,
        BitFlip(usize, u8),
        Panic,
        FsyncFail,
        SetLenFail,
    }

    fn injected_error(kind: &str, offset: u64) -> Error {
        Error::Io(io::Error::other(format!(
            "FaultySink: injected {kind} at offset {offset}"
        )))
    }

    /// A [`SegmentSink`] that plays back one [`Fault`] per scripted call
    /// index (a single counter shared across every method, in call order),
    /// otherwise delegating to an inner [`RecordingSink`].
    pub(crate) struct FaultySink {
        inner: RecordingSink,
        script: HashMap<usize, Fault>,
        call_index: usize,
    }

    impl FaultySink {
        pub(crate) fn new(mode: DirectMode) -> (Self, RecordingHandle) {
            let (inner, handle) = RecordingSink::new(mode);
            (
                Self {
                    inner,
                    script: HashMap::new(),
                    call_index: 0,
                },
                handle,
            )
        }

        /// Play `fault` back on the call (0-based, across every trait method
        /// in call order) at `call_index`.
        pub(crate) fn at(mut self, call_index: usize, fault: Fault) -> Self {
            self.script.insert(call_index, fault);
            self
        }

        fn next_fault(&mut self) -> Option<Fault> {
            let idx = self.call_index;
            self.call_index += 1;
            self.script.get(&idx).cloned()
        }
    }

    impl SegmentSink for FaultySink {
        fn pwrite(&mut self, buf: &[u8], offset: u64) -> Result<()> {
            match self.next_fault() {
                Some(Fault::Eio) => Err(injected_error("EIO", offset)),
                Some(Fault::Enospc) => Err(injected_error("ENOSPC", offset)),
                Some(Fault::Panic) => {
                    panic!("FaultySink: injected panic at pwrite offset {offset}")
                }
                Some(Fault::ShortWrite(n)) => {
                    // A defective sink that silently accepts fewer bytes than
                    // asked for while still reporting success — a real
                    // `SegmentSink` must never do this (FileSink retries
                    // internally instead); this exercises the digest check
                    // that catches a sink which does.
                    let n = n.min(buf.len());
                    self.inner.pwrite(&buf[..n], offset)
                }
                Some(Fault::DropSilently) => {
                    // Pretend success without storing the real bytes. The
                    // sentinel can never equal legitimate corpus data, so the
                    // digest mismatch is guaranteed, not probabilistic.
                    let sentinel = vec![0x5Au8; buf.len()];
                    self.inner.pwrite(&sentinel, offset)
                }
                Some(Fault::WrongOffset(delta)) => {
                    let bad_offset = (offset as i64 + delta).max(0) as u64;
                    self.inner.pwrite(buf, bad_offset)
                }
                Some(Fault::Duplicate) => {
                    // Store this segment correctly, then stomp on the
                    // immediately preceding segment's slot with it too —
                    // simulating a re-sent write landing at the wrong place.
                    self.inner.pwrite(buf, offset)?;
                    if let Some(prev_offset) = offset.checked_sub(buf.len() as u64) {
                        self.inner.pwrite(buf, prev_offset)?;
                    }
                    Ok(())
                }
                Some(Fault::StaleBytes) => {
                    let stale = vec![0xEEu8; buf.len()];
                    self.inner.pwrite(&stale, offset)
                }
                Some(Fault::BitFlip(byte, bit)) => {
                    let mut corrupted = buf.to_vec();
                    if let Some(b) = corrupted.get_mut(byte) {
                        *b ^= 1 << (bit % 8);
                    }
                    self.inner.pwrite(&corrupted, offset)
                }
                Some(Fault::FsyncFail) | Some(Fault::SetLenFail) | None => {
                    self.inner.pwrite(buf, offset)
                }
            }
        }

        fn sync_data(&mut self) -> Result<()> {
            match self.next_fault() {
                Some(Fault::FsyncFail) => Err(injected_error("fsync failure", 0)),
                Some(Fault::Panic) => panic!("FaultySink: injected panic at sync_data"),
                _ => self.inner.sync_data(),
            }
        }

        fn set_len(&mut self, len: u64) -> Result<()> {
            match self.next_fault() {
                Some(Fault::SetLenFail) => Err(injected_error("set_len failure", len)),
                Some(Fault::Panic) => panic!("FaultySink: injected panic at set_len"),
                _ => self.inner.set_len(len),
            }
        }

        fn fadvise_dontneed(&mut self) -> Result<()> {
            self.next_fault();
            self.inner.fadvise_dontneed()
        }

        fn mode(&self) -> DirectMode {
            self.inner.mode()
        }
    }

    /// A permit-gated [`SegmentSink`]: every call blocks until the test sends
    /// a permit, so multi-thread pump tests (T-033 onward) can pin exact
    /// interleavings. Every wait has a timeout (default 2s) that fails the
    /// test instead of hanging it — no wait here is ever unbounded.
    pub(crate) struct GateSink {
        inner: RecordingSink,
        permits: Receiver<()>,
        timeout: Duration,
    }

    impl GateSink {
        /// Build a gated sink, the [`Sender`] tests use to release it (one
        /// permit per blocked call), and a [`RecordingHandle`] onto its state.
        pub(crate) fn new(
            mode: DirectMode,
            timeout: Duration,
        ) -> (Self, Sender<()>, RecordingHandle) {
            let (inner, handle) = RecordingSink::new(mode);
            let (tx, rx) = mpsc::channel();
            (
                Self {
                    inner,
                    permits: rx,
                    timeout,
                },
                tx,
                handle,
            )
        }

        fn wait_for_permit(&self) -> Result<()> {
            self.permits.recv_timeout(self.timeout).map_err(|_| {
                Error::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "GateSink: no permit granted within {:?}; the caller is stuck \
                         waiting on a real device write forever",
                        self.timeout
                    ),
                ))
            })
        }
    }

    impl SegmentSink for GateSink {
        fn pwrite(&mut self, buf: &[u8], offset: u64) -> Result<()> {
            self.wait_for_permit()?;
            self.inner.pwrite(buf, offset)
        }

        fn sync_data(&mut self) -> Result<()> {
            self.wait_for_permit()?;
            self.inner.sync_data()
        }

        fn set_len(&mut self, len: u64) -> Result<()> {
            self.wait_for_permit()?;
            self.inner.set_len(len)
        }

        fn fadvise_dontneed(&mut self) -> Result<()> {
            self.wait_for_permit()?;
            self.inner.fadvise_dontneed()
        }

        fn mode(&self) -> DirectMode {
            self.inner.mode()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn pump_primitives_default_config_matches_the_documented_defaults() {
        let config = PumpConfig::default();
        assert_eq!(config.segment_bytes, 1024 * 1024);
        assert_eq!(config.queue_depth, 3);
    }

    /// `unset, empty, garbage, 0, 1, 4095, 4097, above max` for the segment
    /// size (bounds `[1, 16 MiB]`).
    #[test]
    fn pump_primitives_segment_bytes_parse_table() {
        let cases: &[(Option<&str>, ParsedBound)] = &[
            (None, ParsedBound::Unset),
            (Some(""), ParsedBound::Unset),
            (Some("   "), ParsedBound::Unset),
            (Some("garbage"), ParsedBound::Invalid),
            (Some("-1"), ParsedBound::Invalid),
            (Some("0"), ParsedBound::Invalid),
            (Some("1"), ParsedBound::Value(1)),
            (Some("4095"), ParsedBound::Value(4095)),
            (Some("4097"), ParsedBound::Value(4097)),
            (Some("16777216"), ParsedBound::Value(16 * 1024 * 1024)),
            (Some("16777217"), ParsedBound::Invalid),
            (Some("999999999999"), ParsedBound::Invalid),
        ];
        for (input, expected) in cases {
            let got = parse_usize_bounded(*input, MIN_SEGMENT_BYTES, MAX_SEGMENT_BYTES);
            assert_eq!(got, *expected, "input {input:?}");
        }
    }

    /// Same table shape for the queue depth (bounds `[0, 16]`, `0` valid and
    /// meaningful — synchronous mode, not "unset").
    #[test]
    fn pump_primitives_queue_depth_parse_table() {
        let cases: &[(Option<&str>, ParsedBound)] = &[
            (None, ParsedBound::Unset),
            (Some(""), ParsedBound::Unset),
            (Some("   "), ParsedBound::Unset),
            (Some("garbage"), ParsedBound::Invalid),
            (Some("-1"), ParsedBound::Invalid),
            (Some("0"), ParsedBound::Value(0)),
            (Some("1"), ParsedBound::Value(1)),
            (Some("4095"), ParsedBound::Invalid),
            (Some("4097"), ParsedBound::Invalid),
            (Some("16"), ParsedBound::Value(16)),
            (Some("17"), ParsedBound::Invalid),
        ];
        for (input, expected) in cases {
            let got = parse_usize_bounded(*input, MIN_QUEUE_DEPTH, MAX_QUEUE_DEPTH);
            assert_eq!(got, *expected, "input {input:?}");
        }
    }

    #[test]
    fn pump_primitives_resolve_bounded_falls_back_to_default_on_rejection() {
        let (value, rejected) = resolve_bounded(Some("not-a-number"), 1024, 1, 16 * 1024 * 1024);
        assert_eq!(value, 1024);
        assert!(rejected);

        let (value, rejected) = resolve_bounded(None, 1024, 1, 16 * 1024 * 1024);
        assert_eq!(value, 1024);
        assert!(!rejected, "unset is not a rejection");

        let (value, rejected) = resolve_bounded(Some("2048"), 1024, 1, 16 * 1024 * 1024);
        assert_eq!(value, 2048);
        assert!(!rejected);
    }

    #[test]
    fn pump_primitives_effective_segment_rounds_up_with_a_minimum_of_one_block() {
        assert_eq!(PumpConfig::default().effective_segment(4096), 1024 * 1024);
        let small = PumpConfig {
            segment_bytes: 1,
            queue_depth: 3,
        };
        assert_eq!(small.effective_segment(4096), 4096, "minimum one block");
        let exact = PumpConfig {
            segment_bytes: 8192,
            queue_depth: 3,
        };
        assert_eq!(exact.effective_segment(4096), 8192, "already a multiple");
        let over = PumpConfig {
            segment_bytes: 4097,
            queue_depth: 3,
        };
        assert_eq!(over.effective_segment(4096), 8192, "rounds up, not down");
    }

    proptest! {
        /// P7: for any configured size in `1..=16 MiB` and any probed block in
        /// `{4096, 8192, 65536}`, the effective segment is a positive multiple
        /// of the block, at least the block, at least the configured value,
        /// and never overshoots by a whole extra block.
        #[test]
        fn pump_primitives_effective_segment_rounding_property(
            configured in 1usize..=16 * 1024 * 1024,
            block in prop_oneof![Just(4096usize), Just(8192usize), Just(65536usize)],
        ) {
            let config = PumpConfig { segment_bytes: configured, queue_depth: 3 };
            let effective = config.effective_segment(block);
            prop_assert_eq!(effective % block, 0);
            prop_assert!(effective >= block);
            prop_assert!(effective >= configured);
            prop_assert!(effective < configured + block);
        }
    }
}

/// T-032: `SegmentSink` seam, `FileSink`, and the synchronous (`depth = 0`)
/// `AlignedPump`.
#[cfg(test)]
mod pump_sync_tests {
    use super::test_support::*;
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    fn open_recording(block: usize, segment: usize) -> (AlignedPump, RecordingHandle) {
        let (sink, handle) = RecordingSink::new(DirectMode::Direct);
        let pump = AlignedPump::open(Box::new(sink), block, segment, PathBuf::from("test.db"));
        (pump, handle)
    }

    /// Writes a deterministic byte pattern of `len` bytes through `pump` in
    /// irregular chunks (to exercise partial-buffer-fill paths, not just
    /// exact-segment writes) and returns the pattern for comparison.
    fn write_pattern(pump: &mut AlignedPump, len: usize) -> Vec<u8> {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        for chunk in data.chunks(4001) {
            pump.write_all(chunk).expect("write_all");
        }
        data
    }

    #[test]
    fn pump_sync_short_write_alignment_predicate() {
        // A short write that already completes the buffer needs no follow-up
        // offset, so it is never fatal regardless of multiple-ness — callers
        // only consult this predicate while more of the buffer remains.
        assert!(!short_write_violates_alignment(
            DirectMode::Direct,
            4096,
            4096
        ));
        assert!(short_write_violates_alignment(
            DirectMode::Direct,
            100,
            4096
        ));
        assert!(!short_write_violates_alignment(
            DirectMode::Buffered,
            100,
            4096
        ));
        assert!(!short_write_violates_alignment(
            DirectMode::NoCache,
            100,
            4096
        ));
        assert!(!short_write_violates_alignment(
            DirectMode::Direct,
            8192,
            4096
        ));
    }

    /// L3: for every probed block and every configured segment size, every
    /// `SegmentSink::pwrite` call is block-aligned in offset, length, and
    /// buffer address; calls are contiguous and non-overlapping; together
    /// they cover exactly `[0, round_up(len, block))`; `finish` reports the
    /// exact logical length; and the call count never exceeds
    /// `ceil(len / segment) + 1` (architecture.md acceptance criteria 2, 3).
    #[test]
    fn pump_sync_writes_are_block_aligned_contiguous_and_bounded() {
        for &block in &[4096usize, 8192, 65536] {
            for &requested in &[1usize, 4095, 4097, 1024 * 1024, 1024 * 1024 + 1] {
                let segment = PumpConfig {
                    segment_bytes: requested,
                    queue_depth: 0,
                }
                .effective_segment(block);
                for &len in &[
                    0usize,
                    1,
                    segment.saturating_sub(1).max(1),
                    segment,
                    segment + 1,
                    2 * segment + 37,
                ] {
                    let (mut pump, handle) = open_recording(block, segment);
                    let data = write_pattern(&mut pump, len);
                    let logical = pump.finish().unwrap_or_else(|e| {
                        panic!("block={block} segment={segment} len={len}: finish: {e}")
                    });
                    assert_eq!(
                        logical, len as u64,
                        "block={block} segment={segment} len={len}: exact logical length"
                    );

                    let writes = handle.writes();
                    let bound = len.div_ceil(segment) + 1;
                    assert!(
                        writes.len() <= bound,
                        "block={block} segment={segment} len={len}: {} writes exceeds bound {bound}",
                        writes.len()
                    );

                    let mut sorted = writes.clone();
                    sorted.sort_by_key(|w| w.offset);
                    let mut cursor = 0u64;
                    for w in &sorted {
                        assert_eq!(
                            w.offset, cursor,
                            "block={block} segment={segment} len={len}: writes must be contiguous"
                        );
                        assert_eq!(
                            w.offset % block as u64,
                            0,
                            "block={block} segment={segment} len={len}: offset must be block-aligned"
                        );
                        assert_eq!(
                            w.len % block,
                            0,
                            "block={block} segment={segment} len={len}: length must be block-aligned"
                        );
                        assert_eq!(
                            w.addr % block,
                            0,
                            "block={block} segment={segment} len={len}: buffer address must be block-aligned"
                        );
                        cursor += w.len as u64;
                    }
                    let expected_physical = (len as u64).div_ceil(block as u64) * block as u64;
                    assert_eq!(
                        cursor, expected_physical,
                        "block={block} segment={segment} len={len}: writes must cover [0, round_up(len, block))"
                    );

                    assert_eq!(
                        handle.bytes(),
                        data,
                        "block={block} segment={segment} len={len}: byte-exact after finish"
                    );
                }
            }
        }
    }

    /// `finish`'s exact call sequence (architecture.md § Finish): the tail
    /// write, `sync_data`, `set_len(logical)`, a second `sync_data`, and —
    /// only in `Buffered` mode — `fadvise_dontneed`.
    #[test]
    fn pump_sync_finish_calls_sync_set_len_and_fadvise_in_the_documented_order() {
        let block = 4096usize;
        let segment = 2 * block;
        let (sink, handle) = RecordingSink::new(DirectMode::Buffered);
        let mut pump = AlignedPump::open(Box::new(sink), block, segment, PathBuf::from("tail.db"));
        let data = write_pattern(&mut pump, segment + 10);
        let logical = pump.finish().expect("finish");
        assert_eq!(logical, data.len() as u64);
        assert_eq!(
            handle.sync_data_calls(),
            2,
            "one after the tail write, one after set_len"
        );
        assert_eq!(handle.set_len_calls(), vec![logical]);
        assert_eq!(
            handle.fadvise_calls(),
            1,
            "Buffered mode must drop the pages it just wrote"
        );
    }

    /// Byte identity against the real production path: `AlignedPump` driving
    /// a real `FileSink` (via `DirectWriter`) round-trips every byte for
    /// lengths around block/segment boundaries.
    #[test]
    fn pump_sync_matches_direct_writer_byte_for_byte_on_a_real_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        for &len in &[
            0usize,
            1,
            MIN_BLOCK - 1,
            MIN_BLOCK,
            MIN_BLOCK + 1,
            crate::direct::STAGING_CAPACITY - 1,
            crate::direct::STAGING_CAPACITY,
            crate::direct::STAGING_CAPACITY + 1,
            3 * crate::direct::STAGING_CAPACITY + 7,
        ] {
            let data: Vec<u8> = (0..len).map(|i| (i * 13 % 251) as u8).collect();
            let path = dir.path().join(format!("d-{len}.db"));
            let mut writer = crate::direct::DirectWriter::create(&path).expect("create");
            for chunk in data.chunks(997) {
                writer.write_all(chunk).expect("write_all");
            }
            let logical = writer.finish().expect("finish");
            assert_eq!(logical, len as u64, "len {len}");
            let on_disk = std::fs::read(&path).expect("read back");
            assert_eq!(on_disk, data, "len {len}");
        }
    }

    /// The producer-side `Digest.crc32` equals `crc32fast::hash` of exactly
    /// what was fed to `write_all`, independent of segment padding.
    #[test]
    fn pump_sync_digest_matches_crc32fast_hash_of_the_logical_bytes() {
        let block = 4096usize;
        let segment = 4 * block;
        let (mut pump, _handle) = open_recording(block, segment);
        let data = write_pattern(&mut pump, segment + 777);
        let digest = pump.digest();
        assert_eq!(digest, crc32fast::hash(&data));
        let logical = pump.finish().expect("finish");
        assert_eq!(logical, data.len() as u64);
    }

    /// L4: each hard-failure fault surfaces as `Err` naming the pump's path
    /// (never silently, and never as a panic outside `Fault::Panic` itself).
    #[test]
    fn pump_sync_faulty_sink_hard_failures_surface_as_err_naming_the_path() {
        let block = 4096usize;
        let segment = 3 * block;
        let path = PathBuf::from("fault.db");

        let (sink, _handle) = FaultySink::new(DirectMode::Direct);
        let sink = sink.at(0, Fault::Eio);
        let mut pump = AlignedPump::open(Box::new(sink), block, segment, path.clone());
        let err = pump.write_all(&vec![7u8; segment]).unwrap_err();
        assert!(err.to_string().contains("fault.db"), "{err}");
        assert!(err.to_string().contains("offset 0"), "{err}");

        let (sink, _handle) = FaultySink::new(DirectMode::Direct);
        let sink = sink.at(0, Fault::Enospc);
        let mut pump = AlignedPump::open(Box::new(sink), block, segment, path.clone());
        assert!(pump.write_all(&vec![7u8; segment]).is_err());

        // A write shorter than one block never reaches `flush_segment`, so
        // `finish` issues: pwrite (tail, idx 0), sync_data (idx 1), set_len
        // (idx 2), sync_data (idx 3), fadvise_dontneed (idx 4, Buffered only).
        let (sink, _handle) = FaultySink::new(DirectMode::Buffered);
        let sink = sink.at(1, Fault::FsyncFail);
        let mut pump = AlignedPump::open(Box::new(sink), block, segment, path.clone());
        pump.write_all(&[7u8; 10]).expect("write_all");
        let err = pump.finish().unwrap_err();
        assert!(err.to_string().contains("fault.db"), "{err}");

        let (sink, _handle) = FaultySink::new(DirectMode::Buffered);
        let sink = sink.at(2, Fault::SetLenFail);
        let mut pump = AlignedPump::open(Box::new(sink), block, segment, path);
        pump.write_all(&[7u8; 10]).expect("write_all");
        let err = pump.finish().unwrap_err();
        assert!(err.to_string().contains("fault.db"), "{err}");
    }

    #[test]
    fn pump_sync_faulty_sink_panic_propagates() {
        let block = 4096usize;
        let segment = 3 * block;
        let (sink, _handle) = FaultySink::new(DirectMode::Direct);
        let sink = sink.at(0, Fault::Panic);
        let mut pump = AlignedPump::open(Box::new(sink), block, segment, PathBuf::from("panic.db"));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pump.write_all(&vec![7u8; segment])
        }));
        assert!(result.is_err(), "expected the injected panic to propagate");
    }

    /// L4: `ShortWrite`/`DropSilently`/`WrongOffset`/`Duplicate`/`StaleBytes`/
    /// `BitFlip` are silent `SegmentSink` contract violations — `pwrite`
    /// still returns `Ok(())`, so the only way the caller catches them is by
    /// comparing the producer-side digest against a fresh digest of what the
    /// sink actually stored, exactly as the readback verification this pump
    /// exists for (`publication-safety.md` M2/M3) would.
    #[test]
    fn pump_sync_faulty_sink_silent_corruption_is_only_caught_by_digest_comparison() {
        let block = 4096usize;
        let segment = 2 * block;
        let data: Vec<u8> = (0..(3 * segment + 17)).map(|i| (i % 251) as u8).collect();

        let faults: &[(&str, Fault)] = &[
            ("short_write", Fault::ShortWrite(block)),
            ("drop_silently", Fault::DropSilently),
            ("wrong_offset", Fault::WrongOffset(-(block as i64))),
            ("duplicate", Fault::Duplicate),
            ("stale_bytes", Fault::StaleBytes),
            ("bit_flip", Fault::BitFlip(3, 2)),
        ];

        for (name, fault) in faults {
            let (sink, handle) = FaultySink::new(DirectMode::Direct);
            // Fault the SECOND pwrite call (index 1): `Duplicate` and
            // `WrongOffset` need a preceding real segment to collide with.
            let sink = sink.at(1, fault.clone());
            let mut pump =
                AlignedPump::open(Box::new(sink), block, segment, PathBuf::from("corrupt.db"));
            for chunk in data.chunks(577) {
                pump.write_all(chunk)
                    .unwrap_or_else(|e| panic!("{name}: write_all: {e}"));
            }
            let reported_digest = pump.digest();
            let logical = pump
                .finish()
                .unwrap_or_else(|e| panic!("{name}: a silent fault must not surface as Err: {e}"));
            assert_eq!(logical, data.len() as u64, "{name}");

            let actual_digest = crc32fast::hash(&handle.bytes());
            assert_ne!(
                reported_digest, actual_digest,
                "{name}: fault must be undetectable except via digest comparison"
            );
        }
    }

    /// `architecture.md` § Drop without finish: dropping a pump that already
    /// handed at least one segment to the sink, without calling `finish`,
    /// must not panic — only WARN — and must leave whatever was already
    /// flushed (nothing more, nothing less: the still-buffered partial
    /// segment is never padded/written, since only `finish` does that).
    #[test]
    fn pump_sync_drop_without_finish_does_not_panic_and_leaves_only_flushed_segments() {
        let block = 4096usize;
        let segment = 2 * block;
        let (sink, handle) = RecordingSink::new(DirectMode::Direct);
        {
            let mut pump = AlignedPump::open(
                Box::new(sink),
                block,
                segment,
                PathBuf::from("abandoned.db"),
            );
            pump.write_all(&vec![1u8; segment])
                .expect("write_all: full segment");
            pump.write_all(&[2u8; 10])
                .expect("write_all: partial segment");
            // Dropped here without `finish()`.
        }
        assert_eq!(
            handle.bytes().len(),
            segment,
            "only the fully-flushed segment should have reached the sink"
        );
    }

    #[test]
    fn pump_sync_gate_sink_times_out_without_a_permit() {
        let (sink, _tx, handle) = GateSink::new(DirectMode::Direct, Duration::from_millis(200));
        let mut pump = AlignedPump::open(Box::new(sink), 4096, 4096, PathBuf::from("gate.db"));
        let err = pump.write_all(&vec![9u8; 4096]).unwrap_err();
        assert!(err.to_string().contains("gate.db"), "{err}");
        assert!(
            handle.bytes().is_empty(),
            "no permit was ever sent; nothing should have been written"
        );
    }

    #[test]
    fn pump_sync_gate_sink_proceeds_once_a_permit_is_sent() {
        let (sink, tx, handle) = GateSink::new(DirectMode::Direct, Duration::from_secs(2));
        let mut pump = AlignedPump::open(Box::new(sink), 4096, 4096, PathBuf::from("gate-ok.db"));
        tx.send(()).expect("send permit");
        pump.write_all(&vec![9u8; 4096])
            .expect("write_all must proceed once a permit is granted");
        assert_eq!(handle.bytes().len(), 4096);
    }
}
