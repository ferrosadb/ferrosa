//! Subscription management for SUBSCRIBE queries.
//!
//! Manages a registry of active subscriptions per connection, enforcing
//! per-connection limits (FMEA F5) and providing lifecycle operations.

use std::collections::HashMap;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::{GraphError, Result};

/// A handle to a running subscription.
pub struct SubscriptionHandle {
    /// Unique subscription ID within a connection.
    pub id: u16,
    /// Cancellation token — signal the task to stop gracefully.
    pub cancel: CancellationToken,
    /// Background task that periodically re-executes the query.
    pub task: tokio::task::JoinHandle<()>,
}

/// Registry of active subscriptions per connection.
///
/// Thread-safe: all methods acquire internal locks. The registry enforces
/// a per-connection limit (FMEA F5) to prevent resource exhaustion.
pub struct SubscriptionRegistry {
    subscriptions: Mutex<HashMap<u16, SubscriptionHandle>>,
    next_id: Mutex<u16>,
    max_per_connection: usize,
}

impl SubscriptionRegistry {
    /// Create a new registry with the given per-connection limit.
    pub fn new(max_per_connection: usize) -> Self {
        Self {
            subscriptions: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1),
            max_per_connection,
        }
    }

    /// Register a new subscription. Returns the assigned ID or an error if
    /// the per-connection limit has been reached (FMEA F5).
    pub fn register(
        &self,
        cancel: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    ) -> Result<u16> {
        let mut subs = self.subscriptions.lock();
        if subs.len() >= self.max_per_connection {
            // Cancel the task gracefully since we can't track it.
            cancel.cancel();
            return Err(GraphError::ResourceLimit(format!(
                "subscription limit reached ({} max per connection)",
                self.max_per_connection
            )));
        }

        let mut next = self.next_id.lock();
        let id = *next;
        *next = next.wrapping_add(1);
        // Skip 0 on wrap-around since 0 could be ambiguous.
        if *next == 0 {
            *next = 1;
        }
        drop(next);

        subs.insert(id, SubscriptionHandle { id, cancel, task });
        Ok(id)
    }

    /// Cancel a subscription by ID. Returns true if the subscription existed
    /// and was cancelled.
    pub fn cancel(&self, id: u16) -> bool {
        let mut subs = self.subscriptions.lock();
        if let Some(handle) = subs.remove(&id) {
            handle.cancel.cancel();
            true
        } else {
            false
        }
    }

    /// Cancel all subscriptions (called on disconnect cleanup).
    pub fn cancel_all(&self) {
        let mut subs = self.subscriptions.lock();
        for (_, handle) in subs.drain() {
            handle.cancel.cancel();
        }
    }

    /// Number of active subscriptions.
    pub fn count(&self) -> usize {
        self.subscriptions.lock().len()
    }
}

impl Drop for SubscriptionRegistry {
    fn drop(&mut self) {
        self.cancel_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a dummy task for testing — exits when the token is cancelled.
    fn dummy_task() -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let task = tokio::task::spawn(async move {
            token.cancelled().await;
        });
        (cancel, task)
    }

    #[tokio::test]
    async fn registry_register_and_cancel() {
        let registry = SubscriptionRegistry::new(8);

        let (cancel, task) = dummy_task();
        let id = registry.register(cancel, task).unwrap();
        assert_eq!(registry.count(), 1);

        let cancelled = registry.cancel(id);
        assert!(cancelled);
        assert_eq!(registry.count(), 0);

        // Cancelling again returns false.
        let cancelled_again = registry.cancel(id);
        assert!(!cancelled_again);
    }

    #[tokio::test]
    async fn registry_limit_enforcement() {
        let registry = SubscriptionRegistry::new(8);

        // Register 8 subscriptions.
        let mut ids = Vec::new();
        for _ in 0..8 {
            let (cancel, task) = dummy_task();
            let id = registry.register(cancel, task).unwrap();
            ids.push(id);
        }
        assert_eq!(registry.count(), 8);

        // 9th should fail (FMEA F5).
        let (cancel, task) = dummy_task();
        let result = registry.register(cancel, task);
        assert!(result.is_err());
        match result.unwrap_err() {
            GraphError::ResourceLimit(msg) => {
                assert!(msg.contains("subscription limit"));
            }
            other => panic!("expected ResourceLimit, got: {other:?}"),
        }
        assert_eq!(registry.count(), 8);

        // Clean up.
        for id in ids {
            registry.cancel(id);
        }
    }

    #[tokio::test]
    async fn registry_cancel_all() {
        let registry = SubscriptionRegistry::new(8);

        for _ in 0..5 {
            let (cancel, task) = dummy_task();
            registry.register(cancel, task).unwrap();
        }
        assert_eq!(registry.count(), 5);

        registry.cancel_all();
        assert_eq!(registry.count(), 0);
    }
}
