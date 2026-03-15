//! Per-connection subscription lifecycle management.
//!
//! Each CQL connection can hold up to `max_subscriptions` active streaming
//! subscriptions. A `SubscriptionHandle` tracks one subscription and carries
//! a `CancellationToken` that is cancelled when the subscription is removed
//! (via UNSUBSCRIBE) or when the connection is torn down.
//!
//! The polling task (`run_subscription_poll`) re-executes the inner SELECT
//! at the specified interval and pushes result frames to the connection via
//! an `mpsc` channel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::ast::Statement;
use crate::router::{RequestContext, RouteResult, SharedState};

use ferrosa_schema::AuthContext;

/// A frame pushed from a subscription task to the connection writer.
pub struct SubscriptionPush {
    pub stream_id: i16,
    pub body: Bytes,
}

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

/// Spawn a polling subscription task.
///
/// Re-executes the inner SELECT every `interval` and sends result frames
/// through `push_tx`. Runs until the `cancel` token fires or the channel
/// closes (connection dropped).
#[allow(clippy::too_many_arguments)]
pub fn spawn_subscription_poll(
    stream_id: i16,
    interval: Duration,
    state: Arc<SharedState>,
    auth: AuthContext,
    keyspace: Option<String>,
    inner: Statement,
    push_tx: mpsc::Sender<SubscriptionPush>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        // SUBSCRIBE implies full table scan — set allow_filtering on the inner SELECT.
        let inner = match inner {
            Statement::Select(mut s) => {
                s.allow_filtering = true;
                Statement::Select(s)
            }
            other => other,
        };

        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // Skip the immediate first tick — first result at t+interval.
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let ctx = RequestContext {
                        auth: &auth,
                        current_keyspace: &keyspace,
                    };
                    match crate::router::route(&state, &ctx, inner.clone()).await {
                        Ok(RouteResult::Result(body)) => {
                            let push = SubscriptionPush {
                                stream_id,
                                body: body.freeze(),
                            };
                            if push_tx.send(push).await.is_err() {
                                break; // Connection closed
                            }
                        }
                        Ok(_) => {} // Unexpected result type; skip
                        Err(e) => {
                            tracing::debug!(stream_id, error = %e, "subscription query error");
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    tracing::debug!(stream_id, "subscription cancelled");
                    break;
                }
            }
        }
    });
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
