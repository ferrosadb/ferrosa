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
