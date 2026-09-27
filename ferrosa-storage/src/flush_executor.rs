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
//! Last revised: 2026-09-27
//! Last changed: Flush-pool width now has a documented practical ceiling;
//!   failed pool creation retries serially and then returns an initialization
//!   error instead of panicking.

use std::sync::OnceLock;

static POOL: OnceLock<std::result::Result<rayon::ThreadPool, String>> = OnceLock::new();

/// Resolve the flush pool width from an already-read env value, falling back to
/// host parallelism. Pure (takes the env value as an argument) so it is unit
/// testable without mutating the process environment — `std::env::set_var` in
/// one test races every other test that reads the same var (see the rust skill).
///
/// A valid value in the configured range wins. Invalid values log an error and
/// fall back to the capacity-aware host width.
#[cfg(test)]
fn parse_parallelism_with(env_val: Option<String>, host_default: usize, cap: usize) -> usize {
    let value = env_val
        .map(Ok)
        .unwrap_or(Err(std::env::VarError::NotPresent));
    parse_parallelism_value(value, host_default, cap)
}

fn parse_parallelism_value(
    value: Result<String, std::env::VarError>,
    host_default: usize,
    cap: usize,
) -> usize {
    crate::runtime_tuning::parse_usize_env("FERROSA_FLUSH_PARALLELISM", value, host_default, 1, cap)
}

/// The capacity-aware default width, reading `FERROSA_FLUSH_PARALLELISM`.
pub(crate) fn default_parallelism() -> usize {
    let cap = crate::runtime_tuning::storage_runtime_tuning().max_flush_parallelism;
    let host_default = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, cap);
    parse_parallelism_value(
        std::env::var("FERROSA_FLUSH_PARALLELISM"),
        host_default,
        cap,
    )
}

fn build_pool(width: usize) -> std::result::Result<rayon::ThreadPool, String> {
    let cap = crate::runtime_tuning::storage_runtime_tuning().max_flush_parallelism;
    let width = width.clamp(1, cap);
    match rayon::ThreadPoolBuilder::new()
        .num_threads(width)
        .thread_name(|i| format!("ferrosa-flush-{i}"))
        .build()
    {
        Ok(pool) => Ok(pool),
        Err(error) if width > 1 => {
            tracing::error!(
                width,
                %error,
                "failed to create configured flush pool; retrying with one worker"
            );
            rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .thread_name(|i| format!("ferrosa-flush-{i}"))
                .build()
                .map_err(|fallback| {
                    tracing::error!(
                        %fallback,
                        "failed to create serial flush fallback pool"
                    );
                    format!("configured pool failed ({error}); serial fallback failed ({fallback})")
                })
        }
        Err(error) => {
            tracing::error!(%error, "failed to create serial flush pool");
            Err(error.to_string())
        }
    }
}

fn pool_for(width: usize) -> ferrosa_common::Result<&'static rayon::ThreadPool> {
    pool_result(POOL.get_or_init(|| build_pool(width)))
}

fn pool_result(
    result: &'static std::result::Result<rayon::ThreadPool, String>,
) -> ferrosa_common::Result<&'static rayon::ThreadPool> {
    match result {
        Ok(pool) => Ok(pool),
        Err(error) => Err(ferrosa_common::Error::InvalidFormat(format!(
            "flush worker pool initialization failed: {error}"
        ))),
    }
}

/// Initialize the shared flush pool at `width`. Idempotent: the first call
/// (typically `StorageEngine::new`, before any flush) wins; later calls are
/// ignored so the width stays stable for the process lifetime. Safe to call
/// before or after the first lazy [`pool`] access.
pub(crate) fn configure(width: usize) -> ferrosa_common::Result<()> {
    pool_for(width).map(|_| ())
}

/// The shared flush pool, lazily initialized to [`default_parallelism`] if
/// [`configure`] was never called (e.g. in unit tests).
pub(crate) fn pool() -> ferrosa_common::Result<&'static rayon::ThreadPool> {
    match POOL.get() {
        Some(result) => pool_result(result),
        None => pool_for(default_parallelism()),
    }
}

/// Current flush pool width (thread count). Used to bound how many SSTable
/// shards a single flush produces — no point making more shards than the pool
/// can encode concurrently.
pub(crate) fn width() -> usize {
    pool().map_or(1, |pool| pool.current_num_threads().max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::time::Duration;

    #[test]
    fn parse_parallelism_honors_valid_override() {
        assert_eq!(parse_parallelism_with(Some("4".to_string()), 2, 64), 4);
        assert_eq!(parse_parallelism_with(Some("  8 ".to_string()), 2, 64), 8);
    }

    #[test]
    fn parse_parallelism_uses_configured_ceiling() {
        const CONFIGURED_CEILING: usize = 256;
        assert_eq!(
            crate::runtime_tuning::parse_usize_env(
                "FERROSA_MAX_FLUSH_PARALLELISM",
                Ok("256".to_string()),
                64,
                1,
                CONFIGURED_CEILING,
            ),
            CONFIGURED_CEILING
        );
        assert_eq!(
            crate::runtime_tuning::parse_usize_env(
                "FERROSA_MAX_FLUSH_PARALLELISM",
                Ok("257".to_string()),
                64,
                1,
                CONFIGURED_CEILING,
            ),
            64
        );
        assert_eq!(
            parse_parallelism_with(Some("100".to_string()), 4, CONFIGURED_CEILING),
            100
        );
        assert_eq!(
            parse_parallelism_with(Some("1000".to_string()), 4, CONFIGURED_CEILING),
            4
        );
    }

    #[test]
    fn unreadable_flush_parallelism_falls_back_to_host_default() {
        let error = std::env::VarError::NotUnicode(std::ffi::OsString::from("unreadable"));
        assert_eq!(parse_parallelism_value(Err(error), 4, 64), 4);
    }

    #[test]
    fn parse_parallelism_falls_back_on_invalid_or_absent() {
        // Missing, malformed, and sub-1 values fall back to host parallelism.
        for v in [None, Some("garbage".to_string()), Some("0".to_string())] {
            let w = parse_parallelism_with(v, 4, 64);
            assert!(w >= 1, "fallback parallelism must be >= 1, got {w}");
            assert!(w <= 64);
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
