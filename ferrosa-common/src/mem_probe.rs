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
//! Last revised: 2026-10-13
//! Last changed: Added the allocation-SITE tag seam (`enter_site`/`record_site_keyed_alloc`),
//! so a counting allocator can charge each key-sized buffer to the call site that made it.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

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

// ── key-buffer attribution: WHICH site allocated a resident key ─────────────
//
// A whole-call peak (above) says the COMMIT path holds N key-sized buffers; it
// does not say WHICH allocation site put them there, so a fix is a guess. These
// counters close that gap: instrumented call sites tag themselves with
// [`enter_site`]/[`exit_site`], and a counting global allocator charges every
// key-sized allocation to [`current_site`] via [`record_site_keyed_alloc`].
//
// Scope: this attributes the *allocation* of a key-sized buffer, not its
// lifetime — `dealloc` carries no tag, so a site's retained SHARE of a peak is
// established by removing that site and re-measuring, not by these counters
// alone. Report both numbers together.
//
// Cost when `alloc-probe` is OFF (every production build): `installed()` is a
// `const fn`, so every entry point folds to `return` and the counters are dead
// stores rustc removes. Same discipline as [`record_alloc`].

/// Attribution sites for the COMMIT path's key-sized buffers. Index 0 is the
/// bucket for a key-sized allocation made with no site tagged — a nonzero value
/// there is a site the instrument does not (yet) know about.
pub const SITE_UNTAGGED: usize = 0;
/// The driver's per-key replica index (`per_key` in `drive_accord_core`).
pub const SITE_DRIVE_PER_KEY_INDEX: usize = 1;
/// The driver's own `write_set` entry key.
pub const SITE_DRIVE_WRITE_SET: usize = 2;
/// The `PreAcceptV2` wire key list built in `order_and_gate`.
pub const SITE_ORDER_PREACCEPT_KEYS: usize = 3;
/// The local self-vote key list built in `order_and_gate`.
pub const SITE_ORDER_SELF_VOTE_KEYS: usize = 4;
/// The state machine's conflict-index registration (`ConflictIndex::register`).
pub const SITE_SM_CONFLICT_INDEX: usize = 5;
/// A visited row's read key, streamed by `AppendedKeys::for_each_key`.
pub const SITE_APPENDED_ROW_READ_KEY: usize = 6;
/// Number of attribution sites, including [`SITE_UNTAGGED`].
pub const SITE_COUNT: usize = 7;
/// Human-readable site names, index-aligned with the `SITE_*` constants.
pub const SITE_NAMES: [&str; SITE_COUNT] = [
    "untagged",
    "drive.per_key_index",
    "drive.write_set",
    "order.preaccept_v2_keys",
    "order.self_vote_keys",
    "sm.conflict_index",
    "appended.row_read_key",
];

/// The site currently tagging its allocations, or [`SITE_UNTAGGED`].
static SITE_CURRENT: AtomicUsize = AtomicUsize::new(SITE_UNTAGGED);
/// Key-sized allocations charged to each site (cumulative over the window).
static SITE_KEYS: [AtomicI64; SITE_COUNT] = [
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
];
/// Bytes of those allocations, same index alignment.
static SITE_BYTES: [AtomicI64; SITE_COUNT] = [
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
    AtomicI64::new(0),
];

/// Tag allocations made until the matching [`exit_site`], returning the previous
/// tag so a nested scope restores correctly. One global tag, not a per-thread
/// one: the state machine runs its PreAccept on tokio's BLOCKING pool, so a
/// thread-local tag would be invisible exactly where site
/// [`SITE_SM_CONFLICT_INDEX`] is charged. The measured window must therefore be
/// single-threaded in the sense that matters — one commit at a time, which the
/// `serial` residency gates already guarantee.
#[inline]
pub fn enter_site(site: usize) -> usize {
    if installed() {
        SITE_CURRENT.swap(site, Ordering::Relaxed)
    } else {
        SITE_UNTAGGED
    }
}

/// Restore the tag [`enter_site`] returned.
#[inline]
pub fn exit_site(previous: usize) {
    if installed() {
        SITE_CURRENT.store(previous, Ordering::Relaxed);
    }
}

/// The site currently tagging its allocations.
#[inline]
pub fn current_site() -> usize {
    SITE_CURRENT.load(Ordering::Relaxed)
}

/// Charge one key-sized allocation to `site`. Called by the counting global
/// allocator for allocations at or above its key-size floor.
#[inline]
pub fn record_site_keyed_alloc(site: usize, bytes: usize) {
    if site < SITE_COUNT {
        SITE_KEYS[site].fetch_add(1, Ordering::Relaxed);
        SITE_BYTES[site].fetch_add(bytes as i64, Ordering::Relaxed);
    }
}

/// Zero every site counter and the current tag, so the next window reads alone.
#[inline]
pub fn reset_sites() {
    for (keys, bytes) in SITE_KEYS.iter().zip(SITE_BYTES.iter()) {
        keys.store(0, Ordering::Relaxed);
        bytes.store(0, Ordering::Relaxed);
    }
    SITE_CURRENT.store(SITE_UNTAGGED, Ordering::Relaxed);
}

/// Key-sized allocations charged to each site, index-aligned with [`SITE_NAMES`].
#[inline]
pub fn site_key_allocs() -> [i64; SITE_COUNT] {
    std::array::from_fn(|i| SITE_KEYS[i].load(Ordering::Relaxed))
}

/// Bytes of those allocations, index-aligned with [`SITE_NAMES`].
#[inline]
pub fn site_key_bytes() -> [i64; SITE_COUNT] {
    std::array::from_fn(|i| SITE_BYTES[i].load(Ordering::Relaxed))
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
