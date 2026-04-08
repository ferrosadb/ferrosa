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
}

impl RuntimeManager {
    /// Build all subsystem runtimes.
    pub fn new() -> Self {
        let raft = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .thread_name("raft-rt")
                .enable_all()
                .build()
                .expect("raft runtime"),
        );

        let data = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .thread_name("data-rt")
                .enable_all()
                .build()
                .expect("data runtime"),
        );

        Self { raft, data }
    }

    /// Graceful shutdown in reverse dependency order.
    #[allow(dead_code)] // Will be called from shutdown path.
    pub fn shutdown_all(self, timeout: Duration) {
        if let Ok(rt) = Arc::try_unwrap(self.data) {
            rt.shutdown_timeout(timeout);
        }
        if let Ok(rt) = Arc::try_unwrap(self.raft) {
            rt.shutdown_timeout(timeout);
        }
    }
}
