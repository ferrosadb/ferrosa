//! Per-request dispatch for multi-message streaming RPCs.
//!
//! When a coordinator initiates a streaming RPC (e.g., the ADR-020
//! `RangeReadStreamRequest`), it calls [`StreamRouter::register`] to
//! obtain a `mpsc::Receiver<Message>` keyed by a `request_id`. The
//! inbound message dispatch on the lane side decodes the `request_id`
//! from each arriving frame and calls [`StreamRouter::route`] to push
//! the frame to the right receiver. Termination is signalled by the
//! caller via [`StreamRouter::unregister`] (after observing a `Done`
//! or `Cancel` frame), or implicitly when the receiver is dropped —
//! subsequent `route` calls return [`RouteError::ChannelClosed`] and
//! the entry is cleaned up.
//!
//! [`StreamRouter`] is the routing leaf required by ADR-020. It is
//! paired with [`crate::idle_timeout::IdleTimeoutWatchdog`] on the
//! consumer side: the coordinator wraps the registered `Receiver` in
//! the watchdog and treats `IdleTimeoutElapsed` as "peer is genuinely
//! stuck — abort".

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use tokio::sync::mpsc;

use crate::message::Message;

/// Errors from [`StreamRouter::route`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    /// No receiver is currently registered for this `request_id`. The
    /// frame is dropped silently — this is the common case for a
    /// stale chunk that arrived after the caller cancelled or the
    /// request_id has been reused. Callers may log at debug level.
    NoRoute(u32),
    /// The receiver was registered but has since been dropped. The
    /// route entry has been removed; the caller should treat this as
    /// "consumer gave up" and (if it's the producer side) cancel the
    /// upstream operation.
    ChannelClosed(u32),
    /// The receiver's buffer is full. The route has been removed so
    /// the consumer observes channel close instead of returning a
    /// partial success after a dropped frame.
    ChannelFull(u32),
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRoute(id) => write!(f, "no stream route registered for request_id={id}"),
            Self::ChannelClosed(id) => write!(f, "stream consumer for request_id={id} dropped"),
            Self::ChannelFull(id) => write!(f, "stream buffer full for request_id={id}"),
        }
    }
}

impl std::error::Error for RouteError {}

/// Routes inbound streaming-RPC frames to per-`request_id`
/// receivers.
#[derive(Default)]
pub struct StreamRouter {
    routes: Mutex<HashMap<u32, mpsc::Sender<Message>>>,
}

impl StreamRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a receiver for `request_id`. Returns the consumer end
    /// of a bounded mpsc channel with the given `buffer` capacity.
    /// The caller owns the [`mpsc::Receiver`] and is responsible for
    /// draining it; back-pressure on a slow consumer surfaces as
    /// [`RouteError::ChannelFull`] and closes the route.
    ///
    /// If `request_id` is already registered, the prior registration
    /// is replaced and its sender is dropped — the previous consumer
    /// observes channel close on its next `recv`.
    ///
    /// # Panics
    /// Panics if `buffer == 0` (mpsc::channel rejects zero-capacity).
    pub fn register(&self, request_id: u32, buffer: usize) -> mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel(buffer);
        self.routes
            .lock()
            .expect("stream router mutex poisoned")
            .insert(request_id, tx);
        rx
    }

    /// Push an inbound `Message` onto the per-`request_id` receiver.
    /// Non-blocking: returns immediately on success, [`RouteError`]
    /// otherwise.
    ///
    /// `ChannelClosed` entries are removed from the routing table as
    /// a side-effect so future routes for the same id return
    /// `NoRoute` (and the table doesn't leak entries when consumers
    /// drop without explicit `unregister`).
    pub fn route(&self, request_id: u32, msg: Message) -> Result<(), RouteError> {
        let mut routes = self.routes.lock().expect("stream router mutex poisoned");
        let Some(sender) = routes.get(&request_id) else {
            return Err(RouteError::NoRoute(request_id));
        };
        match sender.try_send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                routes.remove(&request_id);
                Err(RouteError::ChannelClosed(request_id))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                routes.remove(&request_id);
                Err(RouteError::ChannelFull(request_id))
            }
        }
    }

    /// Explicitly remove the registration for `request_id`. The
    /// receiver observes channel close on its next `recv`. Idempotent
    /// — removing a non-existent id is a no-op.
    pub fn unregister(&self, request_id: u32) {
        self.routes
            .lock()
            .expect("stream router mutex poisoned")
            .remove(&request_id);
    }

    /// Returns the number of active registrations. Diagnostic /
    /// test-only.
    pub fn len(&self) -> usize {
        self.routes
            .lock()
            .expect("stream router mutex poisoned")
            .len()
    }

    /// Returns true when no registrations are active.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;

    fn chunk(tag: u8) -> Message {
        Message::RangeReadStreamChunk(Bytes::copy_from_slice(&[tag]))
    }

    #[tokio::test]
    async fn registered_route_delivers_message_to_receiver() {
        let router = StreamRouter::new();
        let mut rx = router.register(42, 4);

        router
            .route(42, chunk(0xAA))
            .expect("route to registered id");

        let got = rx.recv().await.expect("receiver gets the message");
        assert_eq!(got, chunk(0xAA));
    }

    #[test]
    fn route_to_unregistered_id_returns_no_route() {
        let router = StreamRouter::new();
        let err = router.route(7, chunk(1)).unwrap_err();
        assert_eq!(err, RouteError::NoRoute(7));
    }

    #[tokio::test]
    async fn dropped_receiver_surfaces_channel_closed_and_cleans_registration() {
        let router = StreamRouter::new();
        let rx = router.register(99, 4);
        assert_eq!(router.len(), 1, "registration present");

        drop(rx);

        let err = router.route(99, chunk(2)).unwrap_err();
        assert_eq!(err, RouteError::ChannelClosed(99));
        assert_eq!(
            router.len(),
            0,
            "ChannelClosed removed the entry as a side-effect"
        );
    }

    #[tokio::test]
    async fn full_buffer_returns_channel_full_and_closes_route() {
        let router = StreamRouter::new();
        let mut rx = router.register(11, 2);

        // Fill the buffer (capacity 2)
        router.route(11, chunk(1)).unwrap();
        router.route(11, chunk(2)).unwrap();

        let err = router.route(11, chunk(3)).unwrap_err();
        assert_eq!(err, RouteError::ChannelFull(11));
        assert_eq!(
            router.len(),
            0,
            "Full must close the route so the consumer fails rather than returning partial data"
        );
        assert_eq!(rx.recv().await, Some(chunk(1)));
        assert_eq!(rx.recv().await, Some(chunk(2)));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn unregister_drops_receiver_on_next_recv() {
        let router = StreamRouter::new();
        let mut rx = router.register(5, 4);

        router.unregister(5);

        assert_eq!(rx.recv().await, None, "unregister closes the channel");
        // And subsequent routes return NoRoute, not ChannelClosed —
        // the entry is gone.
        assert_eq!(
            router.route(5, chunk(0)).unwrap_err(),
            RouteError::NoRoute(5)
        );
    }

    #[tokio::test]
    async fn parallel_request_ids_do_not_cross_talk() {
        let router = Arc::new(StreamRouter::new());
        let mut rx_a = router.register(1, 4);
        let mut rx_b = router.register(2, 4);

        router.route(1, chunk(0xA1)).unwrap();
        router.route(2, chunk(0xB2)).unwrap();
        router.route(1, chunk(0xA3)).unwrap();

        assert_eq!(rx_a.recv().await.unwrap(), chunk(0xA1));
        assert_eq!(rx_b.recv().await.unwrap(), chunk(0xB2));
        assert_eq!(rx_a.recv().await.unwrap(), chunk(0xA3));
    }

    #[tokio::test]
    async fn re_registering_same_id_drops_prior_receiver() {
        let router = StreamRouter::new();
        let mut rx_old = router.register(13, 4);
        let _rx_new = router.register(13, 4);

        assert_eq!(rx_old.recv().await, None, "prior receiver observes close");
        assert_eq!(router.len(), 1, "only the new registration remains");
    }

    #[test]
    #[should_panic]
    fn register_with_zero_buffer_panics() {
        let router = StreamRouter::new();
        let _ = router.register(1, 0);
    }
}
