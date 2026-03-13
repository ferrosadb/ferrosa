//! WriteObserver trait for reactive storage hooks.
//!
//! Observers receive mutations after they are committed to the write-ahead log
//! and memtable. They can produce derived mutations (e.g., adjacency index
//! entries for graph edges).
//!
//! # Observer Modes
//!
//! - **Sync:** `on_write` is called inline — the write path blocks until it returns.
//! - **Async:** Mutations are sent to a bounded channel and processed by a background task.
//!
//! # Contract
//!
//! `on_write` must be **non-blocking**. Do not perform async I/O, disk reads, or
//! network calls inside `on_write`.

use crate::commitlog::config::TableId;
use crate::commitlog::mutation::Mutation;

/// Determines how an observer is invoked on the write path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserverMode {
    /// `on_write` is called inline — the write path blocks until it returns.
    Sync,
    /// Mutations are sent to a bounded channel and processed by a background task.
    Async,
}

/// A reactive hook invoked when mutations are written to storage.
///
/// Implementations must be `Send + Sync` (shared across write threads).
/// The [`on_write`](WriteObserver::on_write) method must be non-blocking.
pub trait WriteObserver: Send + Sync {
    /// Returns the dispatch mode for this observer.
    fn mode(&self) -> ObserverMode;

    /// Returns the tables this observer is interested in.
    ///
    /// Only mutations targeting one of these tables will be dispatched.
    fn tables(&self) -> Vec<TableId>;

    /// Called when a mutation is committed to the write-ahead log and memtable.
    ///
    /// Returns zero or more derived mutations to be applied (e.g., index entries).
    /// Must be non-blocking — no async I/O, disk reads, or network calls.
    fn on_write(&self, table: &TableId, mutation: &Mutation) -> Vec<Mutation>;
}

/// Configuration for async observer dispatch.
#[derive(Debug, Clone)]
pub struct ObserverConfig {
    /// Bounded channel capacity for async observers (default 10,000).
    pub queue_capacity: usize,
    /// Batch drain interval in milliseconds (default 10).
    pub batch_interval_ms: u64,
}

impl Default for ObserverConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            batch_interval_ms: 10,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[test]
    fn observer_mode_values() {
        let sync = ObserverMode::Sync;
        let async_mode = ObserverMode::Async;
        assert_ne!(sync, async_mode);
        assert_eq!(sync, ObserverMode::Sync);
        assert_eq!(async_mode, ObserverMode::Async);
    }

    /// A test observer that counts how many times `on_write` is called.
    struct CountingObserver {
        mode: ObserverMode,
        watched: Vec<TableId>,
        call_count: AtomicU64,
    }

    impl CountingObserver {
        fn new(mode: ObserverMode, watched: Vec<TableId>) -> Self {
            Self {
                mode,
                watched,
                call_count: AtomicU64::new(0),
            }
        }

        fn count(&self) -> u64 {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl WriteObserver for CountingObserver {
        fn mode(&self) -> ObserverMode {
            self.mode
        }

        fn tables(&self) -> Vec<TableId> {
            self.watched.clone()
        }

        fn on_write(&self, _table: &TableId, _mutation: &Mutation) -> Vec<Mutation> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        }
    }

    #[test]
    fn counting_observer_tracks_calls() {
        use ferrosa_common::key::{DecoratedKey, PartitionKey};

        let table = TableId::new("ks", "tbl");
        let observer = CountingObserver::new(ObserverMode::Sync, vec![table.clone()]);

        assert_eq!(observer.count(), 0);

        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk".to_vec())),
            rows: vec![],
            timestamp: 1000,
        };

        let derived = observer.on_write(&table, &mutation);
        assert!(derived.is_empty());
        assert_eq!(observer.count(), 1);

        observer.on_write(&table, &mutation);
        assert_eq!(observer.count(), 2);
    }

    #[test]
    fn observer_is_object_safe() {
        let table = TableId::new("ks", "tbl");
        let observer = CountingObserver::new(ObserverMode::Sync, vec![table]);

        // Verify WriteObserver is object-safe by creating an Arc<dyn WriteObserver>.
        let dyn_observer: Arc<dyn WriteObserver> = Arc::new(observer);
        assert_eq!(dyn_observer.mode(), ObserverMode::Sync);
        assert_eq!(dyn_observer.tables().len(), 1);
    }

    #[test]
    fn observer_config_defaults() {
        let config = ObserverConfig::default();
        assert_eq!(config.queue_capacity, 10_000);
        assert_eq!(config.batch_interval_ms, 10);
    }
}
