//! Module: O_DIRECT alignment probe via `STATX_DIOALIGN`.
//! Correctness: `resolve_block` is pure and total: for any mask/alignment
//!   input it returns exactly one of `Probed`, `Unsupported`, or `TooLarge`,
//!   and a `Probed`/`TooLarge` block is always a power of two `>= MIN_BLOCK`.
//!   `probe` never panics on a failing `statx(2)` call — a failure degrades to
//!   `Unsupported` and is logged once, never silently.
//! Last revised: 2026-09-26
//! Last changed: New module (T-031, sstable-write-pump). `decisions.md` D4:
//!   probe the true device alignment instead of assuming [`crate::direct::MIN_BLOCK`]
//!   is always sufficient. Nothing consumes this yet — `pump.rs` wires it into
//!   `AlignedPump`'s block choice in T-032.
//!
//! # Why gnu-only on Linux
//!
//! `libc` 0.2.186 (pinned in `Cargo.lock`) only defines the `statx` struct's
//! `stx_dio_mem_align` / `stx_dio_offset_align` fields, the `statx(2)` FFI
//! declaration, and `STATX_DIOALIGN` itself behind
//! `cfg(any(target_env = "gnu", target_os = "android", all(target_env =
//! "musl", musl_v1_2_3)))` (`libc-0.2.186/src/unix/linux_like/mod.rs:257-298,
//! 1615, 2177-2180`). `musl_v1_2_3` is a cfg the crate's build script sets
//! only when it detects musl >= 1.2.3 on the machine that *builds* the musl
//! target — not something guaranteed by targeting `*-unknown-linux-musl`
//! alone (an older musl toolchain, or a build environment where the probe
//! script cannot run, leaves it unset). Rather than depend on that detection
//! succeeding, [`probe`] compiles for `target_env = "gnu"` only; every other
//! target (musl, macOS, Windows, …) gets the `Unsupported`-returning stub, so
//! `cargo check --target x86_64-unknown-linux-musl` succeeds unconditionally.

use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::direct::MIN_BLOCK;

/// Ceiling on the probed device block (`decisions.md` D4). A probe above this
/// is not "a bigger block to use" — it means direct I/O is not usable for
/// this file at all, and the caller must fall back to buffered I/O.
pub const MAX_BLOCK: usize = 65536;

/// The outcome of resolving a probed (or absent) alignment to a block size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeResult {
    /// The device's required alignment, already `>= MIN_BLOCK` and a power of
    /// two, safe to allocate and write with.
    Probed(usize),
    /// The kernel could not report an alignment (old kernel, `STATX_DIOALIGN`
    /// absent from `stx_mask`, or both alignment fields zero). Callers use
    /// [`MIN_BLOCK`] and count the fallback.
    Unsupported,
    /// The probed alignment exceeds [`MAX_BLOCK`]. The caller must not use
    /// direct I/O for this file; the `usize` is the value that was refused,
    /// for logging.
    TooLarge(usize),
}

/// Resolve a `statx` alignment report to a block size (`decisions.md` D4).
/// Pure — no I/O — so every case in the resolution table is unit-tested
/// directly.
///
/// `mask_has_dioalign` is the caller's `stx_mask & STATX_DIOALIGN != 0`
/// check, done in the caller so this function needs no platform-specific
/// types. Unsupported when the mask bit is absent, or when both alignments
/// are zero (a filesystem that sets the bit but reports nothing usable, e.g.
/// some overlay mounts). Otherwise the block is
/// `max(MIN_BLOCK, mem_align, offset_align).next_power_of_two()`, capped at
/// [`MAX_BLOCK`].
pub fn resolve_block(mask_has_dioalign: bool, mem_align: u32, offset_align: u32) -> ProbeResult {
    if !mask_has_dioalign || (mem_align == 0 && offset_align == 0) {
        return ProbeResult::Unsupported;
    }
    let block = MIN_BLOCK
        .max(mem_align as usize)
        .max(offset_align as usize)
        .next_power_of_two();
    if block > MAX_BLOCK {
        ProbeResult::TooLarge(block)
    } else {
        ProbeResult::Probed(block)
    }
}

// Everything below is used only by the real gnu-Linux `probe` (the stub on
// every other target never calls it), so it is cfg-gated the same way —
// otherwise `-D warnings` turns "not reachable on this platform" into a
// dead-code build failure on macOS/musl/etc.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod linux_gnu_probe {
    use super::{ProbeResult, MIN_BLOCK};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    /// The block [`ProbeResult`] chose, or the value that will be logged in
    /// its place — used only to keep the once-per-device log line's shape
    /// the same across all three outcomes.
    pub(super) fn logged_block(result: ProbeResult) -> usize {
        match result {
            ProbeResult::Probed(block) | ProbeResult::TooLarge(block) => block,
            ProbeResult::Unsupported => MIN_BLOCK,
        }
    }

    pub(super) static STATX_FAILURE_WARNED: AtomicBool = AtomicBool::new(false);

    /// Devices already logged at INFO for their probed alignment. Bounded
    /// (Power-of-10 rule 3): this is a per-process operator log, not a
    /// correctness table, so past [`MAX_LOGGED_DEVICES`] distinct devices in
    /// one process the dedup simply stops (a handful of repeat INFO lines on
    /// an exotic host with dozens of volumes is a cosmetic cost, not a
    /// silent failure — the probe result itself is never affected).
    const MAX_LOGGED_DEVICES: usize = 64;
    pub(super) static LOGGED_DEVICES: Mutex<Vec<u64>> = Mutex::new(Vec::new());

    pub(super) fn device_id(major: u32, offset: u32) -> u64 {
        ((major as u64) << 32) | offset as u64
    }

    /// Log the probed alignment once per `st_dev`, cold path only (a lock
    /// per probe is fine: this runs once per file open, not per write).
    pub(super) fn log_once_per_device(
        dev: u64,
        mem_align: u32,
        offset_align: u32,
        result: ProbeResult,
    ) {
        let mut logged = LOGGED_DEVICES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if logged.contains(&dev) {
            return;
        }
        if logged.len() < MAX_LOGGED_DEVICES {
            logged.push(dev);
        }
        tracing::info!(
            device = dev,
            mem_align,
            offset_align,
            block = logged_block(result),
            "probed O_DIRECT alignment for device"
        );
    }
}
#[cfg(all(target_os = "linux", target_env = "gnu"))]
use linux_gnu_probe::{device_id, log_once_per_device, STATX_FAILURE_WARNED};

/// Probe `file`'s device for its true O_DIRECT alignment requirement via
/// `statx(2)` + `STATX_DIOALIGN` (Linux glibc only — see the module docs for
/// why musl is excluded). Never panics: a failing `statx` call is WARN-logged
/// once per process and resolved as [`ProbeResult::Unsupported`].
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn probe(file: &File) -> ProbeResult {
    use std::os::fd::AsRawFd;

    let mut stx = std::mem::MaybeUninit::<libc::statx>::zeroed();
    // SAFETY: `file.as_raw_fd()` is a valid, open fd for the duration of this
    // call. The pathname is an empty C string used only with AT_EMPTY_PATH,
    // which makes the kernel stat the fd itself rather than resolve a path.
    // `stx.as_mut_ptr()` points at a live, appropriately sized allocation the
    // kernel is permitted to write into.
    let rc = unsafe {
        libc::statx(
            file.as_raw_fd(),
            c"".as_ptr(),
            libc::AT_EMPTY_PATH,
            libc::STATX_DIOALIGN,
            stx.as_mut_ptr(),
        )
    };
    if rc != 0 {
        if !STATX_FAILURE_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                error = %std::io::Error::last_os_error(),
                "statx(STATX_DIOALIGN) failed; assuming O_DIRECT alignment is unsupported"
            );
        }
        return ProbeResult::Unsupported;
    }
    // SAFETY: rc == 0 means the kernel filled every field of `stx` (statx(2)
    // always fills the whole struct on success, using zero for anything not
    // covered by the requested mask), so reading it as initialized is sound.
    let stx = unsafe { stx.assume_init() };
    let result = resolve_block(
        stx.stx_mask & libc::STATX_DIOALIGN != 0,
        stx.stx_dio_mem_align,
        stx.stx_dio_offset_align,
    );
    log_once_per_device(
        device_id(stx.stx_dev_major, stx.stx_dev_minor),
        stx.stx_dio_mem_align,
        stx.stx_dio_offset_align,
        result,
    );
    result
}

/// Non-gnu-Linux and non-Linux platforms: `STATX_DIOALIGN` is either not in
/// `libc` for this target (musl without a detected `musl_v1_2_3`, see the
/// module docs) or does not exist on this OS at all. Always `Unsupported`.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn probe(_file: &File) -> ProbeResult {
    ProbeResult::Unsupported
}

static DIO_ALIGN_PROBE_FALLBACKS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Times [`block_for`] fell back to [`MIN_BLOCK`] because the probe reported
/// [`ProbeResult::Unsupported`]. Rendered in `direct::render_prometheus`
/// alongside the direct-writer metrics.
pub fn dio_align_probe_fallbacks_total() -> u64 {
    DIO_ALIGN_PROBE_FALLBACKS_TOTAL.load(Ordering::Relaxed)
}

/// [`block_for`]'s error: the probed alignment exceeds [`MAX_BLOCK`], so no
/// block this pump would allocate can satisfy the device's real requirement.
/// The caller must not open this file with O_DIRECT — fall back to buffered
/// I/O (the same loud, counted fallback `direct::open_bypassing` already uses
/// for a rejected O_DIRECT open) instead of silently writing at the wrong
/// alignment. Carries the refused block, already WARN-logged by
/// [`block_for`], for the caller's own log context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooLarge(pub usize);

/// The block a caller should allocate and align writes to for `file`.
///
/// `Probed` and `Unsupported` both resolve to a usable block (the probed
/// value, or the safe floor, counted in
/// [`dio_align_probe_fallbacks_total`]). See [`TooLarge`] for the refusal
/// case.
pub fn block_for(file: &File) -> Result<usize, TooLarge> {
    match probe(file) {
        ProbeResult::Probed(block) => Ok(block),
        ProbeResult::Unsupported => {
            DIO_ALIGN_PROBE_FALLBACKS_TOTAL.fetch_add(1, Ordering::Relaxed);
            Ok(MIN_BLOCK)
        }
        ProbeResult::TooLarge(block) => {
            tracing::warn!(
                block,
                max_block = MAX_BLOCK,
                "probed O_DIRECT alignment exceeds the block ceiling; \
                 direct I/O is not usable for this file, fall back to buffered I/O"
            );
            Err(TooLarge(block))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dio_align_resolve_block_missing_mask_is_unsupported() {
        assert_eq!(resolve_block(false, 4096, 4096), ProbeResult::Unsupported);
    }

    #[test]
    fn dio_align_resolve_block_both_zero_is_unsupported() {
        assert_eq!(resolve_block(true, 0, 0), ProbeResult::Unsupported);
    }

    #[test]
    fn dio_align_resolve_block_512_rounds_up_to_min_block() {
        assert_eq!(resolve_block(true, 512, 512), ProbeResult::Probed(4096));
    }

    #[test]
    fn dio_align_resolve_block_4096_is_min_block() {
        assert_eq!(resolve_block(true, 4096, 4096), ProbeResult::Probed(4096));
    }

    #[test]
    fn dio_align_resolve_block_16384_mem_beats_min_block() {
        assert_eq!(resolve_block(true, 16384, 4096), ProbeResult::Probed(16384));
    }

    #[test]
    fn dio_align_resolve_block_one_align_zero_still_resolves() {
        assert_eq!(resolve_block(true, 3000, 0), ProbeResult::Probed(4096));
    }

    #[test]
    fn dio_align_resolve_block_above_ceiling_is_too_large() {
        assert_eq!(
            resolve_block(true, 131072, 131072),
            ProbeResult::TooLarge(131072)
        );
    }

    #[test]
    fn dio_align_resolve_block_is_always_a_power_of_two_at_or_above_min_block() {
        for mem in [0u32, 1, 512, 3000, 4096, 5000, 16384, 60000] {
            for offset in [0u32, 1, 512, 3000, 4096, 5000, 16384, 60000] {
                if let ProbeResult::Probed(block) | ProbeResult::TooLarge(block) =
                    resolve_block(true, mem, offset)
                {
                    assert!(block.is_power_of_two(), "block {block} not a power of two");
                    assert!(block >= MIN_BLOCK, "block {block} below MIN_BLOCK");
                }
            }
        }
    }

    #[test]
    fn dio_align_block_for_never_panics_on_a_plain_file() {
        // Non-Linux-gnu builds always take the Unsupported stub; on Linux gnu
        // this exercises the real probe against an ordinary tempdir file.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("plain.db");
        let file = File::create(&path).expect("create");
        let result = block_for(&file);
        match result {
            Ok(block) => assert!(block.is_power_of_two() && block >= MIN_BLOCK),
            Err(TooLarge(block)) => {
                panic!("unexpected TooLarge({block}) for an ordinary tempdir file")
            }
        }
    }

    #[cfg(all(feature = "live-infra-tests", target_os = "linux"))]
    #[test]
    fn dio_align_probe_runs_on_a_real_linux_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = File::create(dir.path().join("probe.db")).expect("create");
        match probe(&file) {
            ProbeResult::Probed(block) => {
                assert!(block.is_power_of_two());
                assert!(block >= MIN_BLOCK);
            }
            ProbeResult::Unsupported => {}
            ProbeResult::TooLarge(block) => {
                panic!("unexpected TooLarge({block}) for an ordinary tempdir file")
            }
        }
    }

    #[cfg(all(feature = "live-infra-tests", not(target_os = "linux")))]
    #[test]
    fn dio_align_probe_runs_on_a_real_linux_file() {
        panic!(
            "dio_align_probe_runs_on_a_real_linux_file requires a Linux host: \
             STATX_DIOALIGN is a Linux-only statx(2) extension. Run with \
             `--features live-infra-tests` on a Linux box (e.g. fmem-dev or a \
             Linux CI job), not on macOS."
        );
    }
}
