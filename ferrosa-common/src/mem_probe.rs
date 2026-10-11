//! Optional process-wide allocation accounting for residency attribution.
//!
//! Reading a process's RSS at a phase boundary attributes a memory peak to a
//! PHASE, but not to the STRUCTURE that caused it. This module closes that gap:
//! it maintains a live-heap counter that any crate can read, so a phase boundary
//! can log exactly how many bytes were resident there.
//!
//! The counter is fed by the global allocator in the `ferrosa` binary, which
//! wraps jemalloc and records every `alloc`/`dealloc`/`realloc` when the
//! `alloc-probe` cargo feature is ON. When that feature is OFF — every
//! production build — the recording calls are empty and inline away, and the
//! process uses the plain jemalloc allocator, so nothing is paid.
//!
//! `installed()` reports which build this is, so a diagnostics path can log an
//! attributed number only when it is real rather than printing a silent zero.
//!
//! Last revised: 2026-10-10
//! Last changed: Added live-heap accounting for COMMIT residency attribution.

use std::sync::atomic::{AtomicI64, Ordering};

/// Live bytes currently requested from the global allocator, summed across
/// threads. Relaxed: a diagnostic read may race an in-flight allocation by a
/// few bytes, which cannot change which structure dominates a peak.
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
/// High-water mark of [`LIVE_BYTES`].
static PEAK_BYTES: AtomicI64 = AtomicI64::new(0);

/// Whether the counting global allocator is installed in this build.
///
/// `false` in every build without the `alloc-probe` feature: [`live_bytes`] then
/// returns `0` because nothing feeds it, and a caller that wants an honest
/// number must not report one. It is a `const`, so a guarded log path compiles
/// away entirely in a production build.
pub const fn installed() -> bool {
    cfg!(feature = "alloc-probe")
}

/// Record a fresh allocation of `bytes`.
#[inline]
pub fn record_alloc(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes as i64, Ordering::Relaxed) + bytes as i64;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

/// Record a release of `bytes`.
#[inline]
pub fn record_dealloc(bytes: usize) {
    LIVE_BYTES.fetch_sub(bytes as i64, Ordering::Relaxed);
}

/// Live host heap bytes, or `0` when the probe is not [`installed`].
#[inline]
pub fn live_bytes() -> i64 {
    if installed() {
        LIVE_BYTES.load(Ordering::Relaxed)
    } else {
        0
    }
}

/// High-water mark of [`live_bytes`], or `0` when the probe is not
/// [`installed`].
#[inline]
pub fn peak_bytes() -> i64 {
    if installed() {
        PEAK_BYTES.load(Ordering::Relaxed)
    } else {
        0
    }
}

/// Live heap in MiB, for a log line. `0.0` when the probe is not [`installed`].
#[inline]
pub fn live_mib() -> f64 {
    live_bytes() as f64 / (1024.0 * 1024.0)
}

/// High-water mark in MiB, for a log line. `0.0` when the probe is not
/// [`installed`].
#[inline]
pub fn peak_mib() -> f64 {
    peak_bytes() as f64 / (1024.0 * 1024.0)
}

/// Reset the high-water mark to the current live value, so the next peak reads
/// as "peak since this point" rather than "peak since process start".
#[inline]
pub fn reset_peak() {
    if installed() {
        PEAK_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_moves_live_and_peak_only_when_installed() {
        if installed() {
            let before = live_bytes();
            record_alloc(1_000_000);
            assert_eq!(live_bytes(), before + 1_000_000);
            assert!(peak_bytes() >= live_bytes());
            record_dealloc(1_000_000);
            assert_eq!(live_bytes(), before);
        } else {
            // A build without the allocator must report nothing rather than a
            // fabricated number: the counter is un-fed, so it stays zero.
            record_alloc(1_000_000);
            assert_eq!(live_bytes(), 0);
            assert_eq!(peak_bytes(), 0);
            record_dealloc(1_000_000);
        }
    }

    #[test]
    fn reset_peak_lowers_the_high_water_to_the_current_live_value() {
        if !installed() {
            return;
        }
        record_alloc(4_000_000);
        assert!(peak_bytes() >= live_bytes());
        reset_peak();
        assert_eq!(peak_bytes(), live_bytes());
        record_dealloc(4_000_000);
    }
}
