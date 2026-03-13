//! Per-connection subscription lifecycle management.
//!
//! Each CQL connection can hold up to `max_subscriptions` active streaming
//! subscriptions. A `SubscriptionHandle` tracks one subscription and carries
//! a `CancellationToken` that is cancelled when the subscription is removed
//! (via UNSUBSCRIBE) or when the connection is torn down.

use std::collections::HashMap;

use tokio_util::sync::CancellationToken;

/// A handle to a single active subscription.
pub struct SubscriptionHandle {
    pub stream_id: u16,
    pub cancel: CancellationToken,
}

impl SubscriptionHandle {
    #[cfg(test)]
    pub fn test(stream_id: u16) -> Self {
        Self {
            stream_id,
            cancel: CancellationToken::new(),
        }
    }
}

/// Per-connection subscription state.
pub struct SubscriptionState {
    max_subscriptions: usize,
    subscriptions: HashMap<u16, SubscriptionHandle>,
}

impl SubscriptionState {
    pub fn new(max_subscriptions: usize) -> Self {
        Self {
            max_subscriptions,
            subscriptions: HashMap::new(),
        }
    }

    pub fn active_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn add(&mut self, handle: SubscriptionHandle) -> Result<(), &'static str> {
        if self.subscriptions.len() >= self.max_subscriptions {
            return Err("maximum subscriptions per connection reached");
        }
        self.subscriptions.insert(handle.stream_id, handle);
        Ok(())
    }

    /// Cancel one or all subscriptions.
    pub fn cancel(&mut self, stream_id: Option<u16>) {
        match stream_id {
            Some(id) => {
                if let Some(handle) = self.subscriptions.remove(&id) {
                    handle.cancel.cancel();
                }
            }
            None => {
                // Cancel all
                for (_, handle) in self.subscriptions.drain() {
                    handle.cancel.cancel();
                }
            }
        }
    }

    /// Cancel all subscriptions (called on disconnect).
    pub fn cancel_all(&mut self) {
        self.cancel(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_state_tracks_active() {
        let mut state = SubscriptionState::new(8);
        let handle = SubscriptionHandle::test(1);
        assert!(state.add(handle).is_ok());
        assert_eq!(state.active_count(), 1);
    }

    #[test]
    fn subscription_state_enforces_max() {
        let mut state = SubscriptionState::new(2);
        state.add(SubscriptionHandle::test(1)).unwrap();
        state.add(SubscriptionHandle::test(2)).unwrap();
        assert!(state.add(SubscriptionHandle::test(3)).is_err());
    }

    #[test]
    fn unsubscribe_by_stream_id() {
        let mut state = SubscriptionState::new(8);
        state.add(SubscriptionHandle::test(42)).unwrap();
        state.cancel(Some(42));
        assert_eq!(state.active_count(), 0);
    }

    #[test]
    fn unsubscribe_all() {
        let mut state = SubscriptionState::new(8);
        state.add(SubscriptionHandle::test(1)).unwrap();
        state.add(SubscriptionHandle::test(2)).unwrap();
        state.cancel(None);
        assert_eq!(state.active_count(), 0);
    }

    #[test]
    fn cancellation_token_is_cancelled() {
        let mut state = SubscriptionState::new(8);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        state
            .add(SubscriptionHandle {
                stream_id: 1,
                cancel,
            })
            .unwrap();
        assert!(!cancel_clone.is_cancelled());
        state.cancel(Some(1));
        assert!(cancel_clone.is_cancelled());
    }
}
