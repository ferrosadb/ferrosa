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
    pub raft: Arc<tokio::runtime::Runtime>,
    // Phase 2+: cql, data, s3, index, aux runtimes will be added here.
}

impl RuntimeManager {
    /// Build all subsystem runtimes.
    pub fn new() -> Self {
        let raft = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("raft-rt")
                .enable_all()
                .build()
                .expect("raft runtime"),
        );

        Self { raft }
    }

    /// Graceful shutdown in reverse dependency order.
    pub fn shutdown_all(self, timeout: Duration) {
        // Raft is the last to shut down — it must commit any in-flight entries.
        // Try to unwrap the Arc; if other references exist, just drop our handle.
        if let Ok(rt) = Arc::try_unwrap(self.raft) {
            rt.shutdown_timeout(timeout);
        }
    }
}
