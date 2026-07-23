//! Bounded, shared thread pool for flush parallelism.
//!
//! The durability-barrier fsyncs (and, in later slices, sharded SSTable
//! writers) run on a single process-wide [`rayon::ThreadPool`] whose width is
//! CONFIGURABLE and capacity-aware, instead of each flush spawning its own
//! unbounded set of OS threads. This gives two properties the naive
//! `std::thread::scope` fan-out lacked:
//!
//! - the degree of flush parallelism is a knob (`FERROSA_FLUSH_PARALLELISM`,
//!   default = host `available_parallelism()`), so a larger (or heterogeneous
//!   burst) node can flush wider without a code change; and
//! - concurrency is BOUNDED across *all* concurrent flushes — the pool has a
//!   fixed thread count, so N simultaneous flushes still share W threads rather
//!   than spawning `components * N` threads.
//!
//! Correctness: the pool only bounds *how many* fsyncs/writes run at once; it
//! changes no durability ordering. Callers still barrier (join) their submitted
//! work before advancing any checkpoint — see `flush::fsync_components`.
//!
//! Last revised: 2026-07-23
//! Last changed: Introduced the bounded rayon flush pool (replaces per-flush
//! scoped threads) so flush parallelism is configurable and bounded.

use std::sync::OnceLock;

/// Hard sanity cap on flush parallelism. A capacity-aware default never
/// approaches this; it only guards against an absurd operator override.
const MAX_FLUSH_PARALLELISM: usize = 64;

static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

/// Resolve the flush pool width from an already-read env value, falling back to
/// host parallelism. Pure (takes the env value as an argument) so it is unit
/// testable without mutating the process environment — `std::env::set_var` in
/// one test races every other test that reads the same var (see the rust skill).
///
/// A value `>= 1` wins (clamped to [`MAX_FLUSH_PARALLELISM`]); `0`, a negative /
/// non-numeric value, or an absent var falls back to `available_parallelism()`.
pub(crate) fn parse_parallelism(env_val: Option<String>) -> usize {
    if let Some(v) = env_val {
        if let Ok(n) = v.trim().parse::<usize>() {
            if n >= 1 {
                return n.min(MAX_FLUSH_PARALLELISM);
            }
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, MAX_FLUSH_PARALLELISM)
}

/// The capacity-aware default width, reading `FERROSA_FLUSH_PARALLELISM`.
pub(crate) fn default_parallelism() -> usize {
    parse_parallelism(std::env::var("FERROSA_FLUSH_PARALLELISM").ok())
}

fn build_pool(width: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(width.clamp(1, MAX_FLUSH_PARALLELISM))
        .thread_name(|i| format!("ferrosa-flush-{i}"))
        .build()
        .expect("build flush fsync pool")
}

/// Initialize the shared flush pool at `width`. Idempotent: the first call
/// (typically `StorageEngine::new`, before any flush) wins; later calls are
/// ignored so the width stays stable for the process lifetime. Safe to call
/// before or after the first lazy [`pool`] access.
pub(crate) fn configure(width: usize) {
    if POOL.get().is_some() {
        return;
    }
    // A concurrent lazy `pool()` may win the race; then `set` returns Err and
    // the pool we just built is dropped (its threads join). Benign.
    let _ = POOL.set(build_pool(width));
}

/// The shared flush pool, lazily initialized to [`default_parallelism`] if
/// [`configure`] was never called (e.g. in unit tests).
pub(crate) fn pool() -> &'static rayon::ThreadPool {
    POOL.get_or_init(|| build_pool(default_parallelism()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::time::Duration;

    #[test]
    fn parse_parallelism_honors_valid_override() {
        assert_eq!(parse_parallelism(Some("4".to_string())), 4);
        assert_eq!(parse_parallelism(Some("  8 ".to_string())), 8);
    }

    #[test]
    fn parse_parallelism_caps_absurd_values() {
        assert_eq!(
            parse_parallelism(Some("100000".to_string())),
            MAX_FLUSH_PARALLELISM
        );
    }

    #[test]
    fn parse_parallelism_falls_back_on_invalid_or_absent() {
        // None, non-numeric, and sub-1 all fall back to host parallelism (>= 1).
        for v in [None, Some("garbage".to_string()), Some("0".to_string())] {
            let w = parse_parallelism(v);
            assert!(w >= 1, "fallback parallelism must be >= 1, got {w}");
            assert!(w <= MAX_FLUSH_PARALLELISM);
        }
    }

    #[test]
    fn pool_bounds_concurrent_tasks_to_width() {
        use rayon::prelude::*;
        // A width-2 pool must never run more than 2 tasks at once, no matter how
        // many are submitted — the bounded-executor property the naive
        // per-flush scoped-thread fan-out did not have.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let live = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        pool.install(|| {
            (0..8).into_par_iter().for_each(|_| {
                let cur = live.fetch_add(1, SeqCst) + 1;
                peak.fetch_max(cur, SeqCst);
                std::thread::sleep(Duration::from_millis(50));
                live.fetch_sub(1, SeqCst);
            });
        });
        let p = peak.load(SeqCst);
        assert!(
            p <= 2,
            "width-2 pool ran {p} tasks concurrently (must be <= 2)"
        );
        // With 8 tasks × 50ms on 2 threads the two workers reliably overlap, so
        // real parallelism did occur (guards against a width collapsing to 1).
        assert!(p >= 2, "width-2 pool never overlapped (peak={p})");
    }
}
