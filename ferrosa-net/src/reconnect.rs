use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::watch;
use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::rpc::client::RpcClient;

/// Exponential backoff with randomized jitter to prevent thundering herd.
///
/// Each call to [`Self::next_delay`] returns the current delay plus random
/// jitter (up to 25% of the delay), then doubles the base for the next call,
/// capped at `max`.  Call [`Self::reset`] to restart from `initial`.
pub struct ExponentialBackoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl ExponentialBackoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            current: initial,
        }
    }

    /// Returns the current delay with jitter, then doubles the base (capped at `max`).
    pub fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        // Add 0-25% random jitter to prevent synchronized retries
        let jitter_range = base.as_millis() as u64 / 4;
        if jitter_range > 0 {
            let jitter = rand::thread_rng().gen_range(0..=jitter_range);
            base.saturating_add(Duration::from_millis(jitter))
        } else {
            base
        }
    }

    /// Resets the backoff to `initial`.
    pub fn reset(&mut self) {
        self.current = self.initial;
    }
}

/// State of a single lane within a `PriorityPool`.
pub enum LaneState {
    /// The TCP connection is established and the client is usable.
    Connected(RpcClient),
    /// The connection is down; a background task is retrying with backoff.
    Reconnecting {
        attempt: u32,
        backoff: ExponentialBackoff,
    },
    /// All retry attempts have been exhausted.
    Failed,
}

/// Maximum number of reconnection attempts before moving to [`LaneState::Failed`].
///
/// At 10 s cap the tail attempts are spaced 10 s apart.  36 attempts ≈ 5 min
/// total worst-case exposure before the lane is declared permanently failed.
pub const MAX_RECONNECT_ATTEMPTS: u32 = 36;

/// Spawn a task that watches `alive_rx` and triggers `on_dead` whenever the
/// connection transitions from alive to dead.
///
/// This is a thin helper used by [`PriorityPool`] to monitor each lane.
pub(crate) fn spawn_alive_watcher(
    mut alive_rx: watch::Receiver<bool>,
    on_dead: impl Fn() + Send + 'static,
) {
    tokio::spawn(async move {
        loop {
            // Wait for the watch value to change.
            if alive_rx.changed().await.is_err() {
                // Sender dropped — connection object gone, nothing to monitor.
                break;
            }
            if !*alive_rx.borrow() {
                on_dead();
                break;
            }
        }
    });
}

/// Attempt to open a single connection, retrying with exponential backoff.
///
/// Returns `None` when `MAX_RECONNECT_ATTEMPTS` is reached.
pub(crate) async fn connect_with_retry(
    config: Arc<NetConfig>,
    local_host_id: Uuid,
    peer_addr: SocketAddr,
    lane: Lane,
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
) -> Option<RpcClient> {
    let mut backoff = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10));

    for attempt in 1..=MAX_RECONNECT_ATTEMPTS {
        let delay = backoff.next_delay();
        tracing::debug!(
            ?lane,
            attempt,
            ?delay,
            peer = %peer_addr,
            "reconnecting lane"
        );
        tokio::time::sleep(delay).await;

        let result = RpcClient::connect_with_tls(
            config.clone(),
            local_host_id,
            peer_addr,
            tls_connector.as_deref(),
        )
        .await;

        match result {
            Ok(client) => {
                tracing::info!(?lane, attempt, peer = %peer_addr, "lane reconnected");
                return Some(client);
            }
            Err(e) => {
                tracing::warn!(?lane, attempt, peer = %peer_addr, error = %e, "reconnect attempt failed");
            }
        }
    }

    tracing::error!(?lane, peer = %peer_addr, "lane reconnection exhausted all attempts");
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Assert delay is within [base, base + 25%] range (accounting for jitter).
    fn assert_in_range(actual: Duration, base_ms: u64) {
        let min = Duration::from_millis(base_ms);
        let max = Duration::from_millis(base_ms + base_ms / 4);
        assert!(
            actual >= min && actual <= max,
            "expected {actual:?} in [{min:?}, {max:?}]"
        );
    }

    #[test]
    fn backoff_sequence_caps_at_10s() {
        let mut b = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10));
        assert_in_range(b.next_delay(), 100);
        assert_in_range(b.next_delay(), 200);
        assert_in_range(b.next_delay(), 400);
        assert_in_range(b.next_delay(), 800);
        assert_in_range(b.next_delay(), 1600);
        assert_in_range(b.next_delay(), 3200);
        assert_in_range(b.next_delay(), 6400);
        assert_in_range(b.next_delay(), 10000); // capped
        assert_in_range(b.next_delay(), 10000); // stays capped
    }

    #[test]
    fn backoff_resets() {
        let mut b = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10));
        b.next_delay();
        b.next_delay();
        b.reset();
        assert_in_range(b.next_delay(), 100);
    }

    #[test]
    fn backoff_does_not_overflow_on_large_initial() {
        let mut b = ExponentialBackoff::new(
            Duration::from_secs(u64::MAX / 2),
            Duration::from_secs(u64::MAX),
        );
        // Should not panic — saturating_mul handles overflow.
        let _ = b.next_delay();
        let _ = b.next_delay();
    }
}
