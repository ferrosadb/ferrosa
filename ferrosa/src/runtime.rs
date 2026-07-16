//! Subsystem runtime manager.
//!
//! Each subsystem gets its own tokio runtime so work on one path cannot
//! starve another.  The main runtime is supervisor-only.

use std::sync::Arc;
use std::time::Duration;

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
            std::env::var(env_key)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|threads| *threads > 0)
                .unwrap_or(default)
        }

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
