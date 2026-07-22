//! Subsystem runtime manager.
//!
//! Each subsystem gets its own tokio runtime so work on one path cannot
//! starve another.  The main runtime is supervisor-only.

use std::sync::Arc;
use std::time::Duration;

/// Resolve a positive-`usize` runtime tunable from an env value, falling back to
/// `default` when unset, unparseable, or non-positive.
///
/// Pure (the env read happens at the call site) so it is testable without racy
/// `set_var` in parallel tests.
fn resolve_positive_usize(env_val: Option<String>, default: usize) -> usize {
    env_val
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or(default)
}

/// Detected CPU parallelism, falling back to a single core.
fn detected_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Holds dedicated tokio runtimes for each subsystem.
///
/// Created once at startup and threaded through the initialization sequence.
/// Each runtime is `Arc`-wrapped so handles can be cheaply cloned into
/// subsystem components.
pub struct RuntimeManager {
    /// Raft consensus: openraft tasks, Raft lane IO, vote/heartbeat handlers.
    /// Must NEVER run bootstrap streaming, S3 sync, or data-path handlers.
    pub raft: Arc<tokio::runtime::Runtime>,
    /// Internode data path: read/write forwarding, bootstrap streaming, repair.
    pub data: Arc<tokio::runtime::Runtime>,
    /// Client CQL protocol path: accept loop, connection handlers, request dispatch.
    pub cql: Arc<tokio::runtime::Runtime>,
    /// Low-priority service work: seed retries, web/graph/sparql listeners,
    /// periodic maintenance coordinators, and one-shot warnings.
    pub background: Arc<tokio::runtime::Runtime>,
}

impl RuntimeManager {
    /// Build all subsystem runtimes.
    pub fn new() -> Self {
        fn worker_threads(env_key: &str, default: usize) -> usize {
            resolve_positive_usize(std::env::var(env_key).ok(), default)
        }
        fn max_blocking(env_key: &str, default: usize) -> usize {
            resolve_positive_usize(std::env::var(env_key).ok(), default)
        }

        // Explicit blocking-pool ceilings (t_88223ad0). Left unset, tokio defaults
        // to 512 blocking threads per runtime; a full-table ALLOW FILTERING scan
        // then admits hundreds of blocking producers that oversubscribe the cores
        // and starve raft heartbeats into a CheckQuorum step-down. These ceilings
        // are a cores-derived BACKSTOP; the tight per-scan CPU bound is the
        // ferrosa-sched pool (cores - reserved), which scan producers route
        // through (T0.3). The raft runtime is intentionally NOT capped — consensus
        // must never be throttled. cql keeps the default (client handlers are not
        // the scan-blocking offenders).
        let cores = detected_cores();
        let data_max_blocking = (cores * 8).max(8);
        let background_max_blocking = (cores * 2).max(4);

        let raft = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_RAFT_RUNTIME_THREADS", 8))
                .thread_name("raft-rt")
                .enable_all()
                .build()
                .expect("raft runtime"),
        );

        let data = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_DATA_RUNTIME_THREADS", 8))
                .max_blocking_threads(max_blocking("FERROSA_DATA_MAX_BLOCKING", data_max_blocking))
                .thread_name("data-rt")
                .enable_all()
                .build()
                .expect("data runtime"),
        );

        let cql = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_CQL_RUNTIME_THREADS", 8))
                .thread_name("cql-rt")
                .enable_all()
                .build()
                .expect("cql runtime"),
        );

        let background = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads("FERROSA_BACKGROUND_RUNTIME_THREADS", 2))
                .max_blocking_threads(max_blocking(
                    "FERROSA_BACKGROUND_MAX_BLOCKING",
                    background_max_blocking,
                ))
                .thread_name("background-rt")
                .enable_all()
                .build()
                .expect("background runtime"),
        );

        Self {
            raft,
            data,
            cql,
            background,
        }
    }

    /// Pin the subsystem runtimes to the process lifetime so they are never
    /// dropped on the `#[tokio::main]` async stack.
    ///
    /// Dropping a tokio `Runtime` from within an async context panics
    /// ("Cannot drop a runtime in a context where blocking is not allowed").
    /// `main` holds this `RuntimeManager` (and its `Arc<Runtime>` clones flow
    /// into `ModeController`, `PeerManager`, spawned tasks, …) as locals on the
    /// async stack. On *any* early-error return — a listener bind failing, a
    /// startup step returning `Err(_)` — those locals unwind and the last
    /// surviving `Arc<Runtime>` clone drops in async context, firing that panic
    /// *before* the real error is reported and masking it (issue #172, which
    /// fixed the same trap for the S3 upload runtime).
    ///
    /// Leaking one extra `Arc` clone of each runtime keeps every strong count
    /// ≥ 1 for the life of the process, so no `Runtime::drop` ever runs on the
    /// async stack. The runtimes must live until exit anyway; the OS reclaims
    /// them. Call this once, immediately after [`RuntimeManager::new`].
    pub fn leak_for_process_lifetime(&self) {
        std::mem::forget(self.raft.clone());
        std::mem::forget(self.data.clone());
        std::mem::forget(self.cql.clone());
        std::mem::forget(self.background.clone());
    }

    /// Graceful shutdown in reverse dependency order.
    #[allow(dead_code)] // Will be called from shutdown path.
    pub fn shutdown_all(self, timeout: Duration) {
        if let Ok(rt) = Arc::try_unwrap(self.data) {
            rt.shutdown_timeout(timeout);
        }
        if let Ok(rt) = Arc::try_unwrap(self.cql) {
            rt.shutdown_timeout(timeout);
        }
        if let Ok(rt) = Arc::try_unwrap(self.background) {
            rt.shutdown_timeout(timeout);
        }
        if let Ok(rt) = Arc::try_unwrap(self.raft) {
            rt.shutdown_timeout(timeout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T0.2 (t_88223ad0): the runtime tunable parser prefers a valid env value
    /// over the default and falls back safely on unset / non-positive /
    /// unparseable input. Pure — no `set_var`, so no cross-test env races.
    #[test]
    fn resolve_positive_usize_prefers_valid_env_over_default() {
        assert_eq!(resolve_positive_usize(Some("16".into()), 8), 16);
        assert_eq!(resolve_positive_usize(None, 8), 8, "unset uses default");
        assert_eq!(
            resolve_positive_usize(Some("0".into()), 8),
            8,
            "non-positive falls back to default"
        );
        assert_eq!(
            resolve_positive_usize(Some("garbage".into()), 8),
            8,
            "unparseable falls back to default"
        );
        assert_eq!(
            resolve_positive_usize(Some("  4  ".into()), 8),
            4,
            "surrounding whitespace is trimmed"
        );
    }

    /// The data/background blocking ceilings scale with cores and never collapse
    /// below their floors, so a small node still admits enough blocking I/O.
    #[test]
    fn blocking_ceilings_scale_with_cores_and_have_floors() {
        // Mirrors the derivation in `new()`.
        for cores in [1usize, 2, 4, 8, 16] {
            let data = (cores * 8).max(8);
            let background = (cores * 2).max(4);
            assert!(data >= 8, "data ceiling floors at 8 (cores={cores})");
            assert!(
                background >= 4,
                "background ceiling floors at 4 (cores={cores})"
            );
            assert!(
                data < 512,
                "data ceiling must be well below tokio's 512 default"
            );
            assert!(
                data >= background,
                "data path needs at least as much as background"
            );
        }
        assert_eq!(
            detected_cores().max(1),
            detected_cores(),
            "detected cores is >= 1"
        );
    }

    /// Regression (issue #172): dropping a `RuntimeManager` — and therefore its
    /// `Arc<Runtime>` fields — inside an async context must NOT panic once the
    /// runtimes have been pinned for the process lifetime. Before the fix, an
    /// early-error return from the async `#[tokio::main]` dropped the last
    /// `Arc<Runtime>` on the async stack, firing "Cannot drop a runtime in a
    /// context where blocking is not allowed" and masking the real error.
    ///
    /// This test runs on the default current-thread test runtime (an async
    /// context); the `drop(rm)` below would panic without
    /// `leak_for_process_lifetime`.
    #[tokio::test]
    async fn dropping_manager_in_async_context_does_not_panic_after_leak() {
        // Keep the runtimes tiny — we only care about the drop behavior.
        std::env::set_var("FERROSA_RAFT_RUNTIME_THREADS", "1");
        std::env::set_var("FERROSA_DATA_RUNTIME_THREADS", "1");
        std::env::set_var("FERROSA_CQL_RUNTIME_THREADS", "1");
        std::env::set_var("FERROSA_BACKGROUND_RUNTIME_THREADS", "1");

        let rm = RuntimeManager::new();
        rm.leak_for_process_lifetime();
        // Would panic in this async context if the leak did not keep a strong
        // ref alive; reaching the assertion means no panic occurred.
        drop(rm);

        std::env::remove_var("FERROSA_RAFT_RUNTIME_THREADS");
        std::env::remove_var("FERROSA_DATA_RUNTIME_THREADS");
        std::env::remove_var("FERROSA_CQL_RUNTIME_THREADS");
        std::env::remove_var("FERROSA_BACKGROUND_RUNTIME_THREADS");
    }
}
