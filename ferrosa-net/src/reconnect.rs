use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use tokio::sync::watch;
use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::rpc::client::RpcClient;
use crate::task_pool::TaskPool;

// ---------------------------------------------------------------------------
// Backoff + dormant constants
// ---------------------------------------------------------------------------

/// Initial delay between TCP-connect attempts (milliseconds).
/// Sequence: 1 s → 2 s → 4 s → 8 s → 16 s → 30 s (cap), then 30 s steady.
pub const BACKOFF_INITIAL_MS: u64 = 1_000;

/// Maximum delay between TCP-connect attempts (milliseconds).
pub const BACKOFF_CAP_MS: u64 = 30_000;

/// Maximum number of TCP-connect attempts per `connect_with_retry` invocation
/// before the call returns `None` and the caller receives `MarkFailed`.
/// At 30 s cap: 10 attempts ≈ ~3.5 min of total exposure per cycle.
pub const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Number of exhausted `connect_with_retry` cycles before the lane transitions
/// to the `Dormant` state.  One cycle ≈ 3.5 min, so after 3 cycles ≈ ~10 min.
pub const DORMANT_AFTER_EXHAUSTIONS: u32 = 3;

/// How long the lane waits between probe attempts while in the dormant state.
pub const DORMANT_PROBE_INTERVAL: Duration = Duration::from_secs(5 * 60); // 5 minutes

// ---------------------------------------------------------------------------
// Process-wide metrics counters
// ---------------------------------------------------------------------------

/// Number of lanes currently in the `Dormant` state across the process.
static DORMANT_PEER_COUNT: AtomicU64 = AtomicU64::new(0);

/// Total reconnect attempts fired across all lanes since process start.
static TOTAL_RECONNECT_ATTEMPTS: AtomicU64 = AtomicU64::new(0);

/// Returns the number of lanes currently in the `Dormant` state.
pub fn dormant_peer_count() -> u64 {
    DORMANT_PEER_COUNT.load(Ordering::Relaxed)
}

/// Returns the total number of reconnect attempts fired since process start.
pub fn total_reconnect_attempts() -> u64 {
    TOTAL_RECONNECT_ATTEMPTS.load(Ordering::Relaxed)
}

/// Increments the dormant peer counter by 1.
pub(crate) fn inc_dormant_peer_count() {
    DORMANT_PEER_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Decrements the dormant peer counter by 1 (saturating at 0).
pub(crate) fn dec_dormant_peer_count() {
    // Use a CAS loop to avoid wrapping below zero.
    let mut old = DORMANT_PEER_COUNT.load(Ordering::Relaxed);
    loop {
        if old == 0 {
            break;
        }
        match DORMANT_PEER_COUNT.compare_exchange_weak(
            old,
            old - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(cur) => old = cur,
        }
    }
}

/// Increments the total reconnect attempts counter by 1.
pub(crate) fn inc_total_reconnect_attempts() {
    TOTAL_RECONNECT_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// ExponentialBackoff
// ---------------------------------------------------------------------------

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
            let jitter = rand::rng().random_range(0..=jitter_range);
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

// ---------------------------------------------------------------------------
// LaneState
// ---------------------------------------------------------------------------

/// State of a single lane within a `PriorityPool`.
///
/// Transitions:
/// ```text
///   Idle ──────────────────────────────────────────► Connected
///   Connected ─(disconnect)──────────────────────► Reconnecting
///   Reconnecting ─(attempt ok)───────────────────► Connected
///   Reconnecting ─(MAX_RECONNECT_ATTEMPTS hit)───► Reconnecting (exhaustion_count+1)
///   Reconnecting ─(exhaustion_count == DORMANT_AFTER_EXHAUSTIONS)► Dormant
///   Dormant ─(probe ok)──────────────────────────► Connected
///   Dormant ─(probe fail)────────────────────────► Dormant (stays)
/// ```
pub enum LaneState {
    /// The TCP connection is established and the client is usable.
    Connected(RpcClient),
    /// The connection is down; a background task is retrying with backoff.
    Reconnecting {
        /// Current attempt index within this exhaustion cycle.
        attempt: u32,
        /// Number of times `connect_with_retry` has been exhausted without
        /// a successful connection.  When this reaches
        /// [`DORMANT_AFTER_EXHAUSTIONS`] the lane moves to [`LaneState::Dormant`].
        exhaustion_count: u32,
    },
    /// All retry cycles have been exhausted.  The lane probes the peer at most
    /// once every [`DORMANT_PROBE_INTERVAL`].
    Dormant,
}

// ---------------------------------------------------------------------------
// spawn_alive_watcher
// ---------------------------------------------------------------------------

/// Spawn a task that watches `alive_rx` and triggers `on_dead` whenever the
/// connection transitions from alive to dead.
///
/// This is a thin helper used by [`PriorityPool`] to monitor each lane.
pub(crate) fn spawn_alive_watcher(
    mut alive_rx: watch::Receiver<bool>,
    on_dead: impl Fn() + Send + 'static,
    task_pool: TaskPool,
) {
    task_pool.spawn(async move {
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

// ---------------------------------------------------------------------------
// connect_with_retry
// ---------------------------------------------------------------------------

/// Attempt to open a single connection, retrying with exponential backoff.
///
/// `peer_host` is resolved via DNS on every attempt so that container restarts
/// that assign a new IP are handled transparently.
///
/// Returns `None` when `MAX_RECONNECT_ATTEMPTS` is reached without success.
pub(crate) async fn connect_with_retry_cancelable(
    config: Arc<NetConfig>,
    local_host_id: Uuid,
    peer_host: &str,
    lane: Lane,
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    cancelled: Option<Arc<AtomicBool>>,
    task_pool: TaskPool,
) -> Option<RpcClient> {
    let mut backoff = ExponentialBackoff::new(
        Duration::from_millis(BACKOFF_INITIAL_MS),
        Duration::from_millis(BACKOFF_CAP_MS),
    );

    for attempt in 1..=MAX_RECONNECT_ATTEMPTS {
        if cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
        {
            tracing::debug!(?lane, peer = peer_host, "reconnect cancelled");
            return None;
        }
        let delay = backoff.next_delay();
        tracing::debug!(
            ?lane,
            attempt,
            ?delay,
            peer = peer_host,
            "reconnecting lane"
        );
        tokio::time::sleep(delay).await;
        if cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
        {
            tracing::debug!(?lane, peer = peer_host, "reconnect cancelled");
            return None;
        }
        inc_total_reconnect_attempts();

        // Re-resolve on every attempt so a container restart (new IP) is
        // handled without requiring a node restart on the connecting side.
        let peer_addr = match tokio::net::lookup_host(peer_host).await {
            Ok(mut addrs) => match addrs.next() {
                Some(addr) => addr,
                None => {
                    tracing::warn!(
                        ?lane,
                        attempt,
                        peer = peer_host,
                        "DNS resolved no addresses"
                    );
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(?lane, attempt, peer = peer_host, error = %e, "DNS resolution failed");
                continue;
            }
        };

        let result = RpcClient::connect_with_tls_on_pool(
            config.clone(),
            local_host_id,
            peer_addr,
            tls_connector.as_deref(),
            task_pool.clone(),
        )
        .await;

        match result {
            Ok(client) => {
                tracing::info!(?lane, attempt, peer = peer_host, %peer_addr, "lane reconnected");
                return Some(client);
            }
            Err(e) => {
                tracing::warn!(?lane, attempt, peer = peer_host, error = %e, "reconnect attempt failed");
            }
        }
    }

    tracing::error!(
        ?lane,
        peer = peer_host,
        "lane reconnection exhausted all attempts"
    );
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    fn backoff_sequence_caps_at_30s() {
        let mut b = ExponentialBackoff::new(
            Duration::from_millis(BACKOFF_INITIAL_MS),
            Duration::from_millis(BACKOFF_CAP_MS),
        );
        assert_in_range(b.next_delay(), 1_000);
        assert_in_range(b.next_delay(), 2_000);
        assert_in_range(b.next_delay(), 4_000);
        assert_in_range(b.next_delay(), 8_000);
        assert_in_range(b.next_delay(), 16_000);
        assert_in_range(b.next_delay(), 30_000); // capped
        assert_in_range(b.next_delay(), 30_000); // stays capped
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

    #[test]
    fn dormant_peer_counter_increments_and_decrements() {
        // Use a fresh counter baseline by reading current value.
        let before = dormant_peer_count();
        inc_dormant_peer_count();
        assert_eq!(dormant_peer_count(), before + 1);
        dec_dormant_peer_count();
        assert_eq!(dormant_peer_count(), before);
    }

    #[test]
    fn dormant_peer_counter_saturates_at_zero() {
        // Drive to 0 if not already, then try to decrement below — must not wrap.
        let cur = dormant_peer_count();
        for _ in 0..cur {
            dec_dormant_peer_count();
        }
        assert_eq!(dormant_peer_count(), 0);
        dec_dormant_peer_count(); // must not underflow
        assert_eq!(dormant_peer_count(), 0);
    }

    #[test]
    fn constants_are_sane() {
        const { assert!(BACKOFF_INITIAL_MS < BACKOFF_CAP_MS, "initial < cap") };
        const {
            assert!(
                MAX_RECONNECT_ATTEMPTS >= 5,
                "enough attempts to expose backoff"
            )
        };
        const { assert!(DORMANT_AFTER_EXHAUSTIONS >= 1, "must transition eventually") };
    }

    #[tokio::test]
    async fn cancelled_reconnect_exits_before_first_attempt() {
        let before = total_reconnect_attempts();
        let cancelled = Arc::new(AtomicBool::new(true));
        let start = std::time::Instant::now();

        let result = connect_with_retry_cancelable(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            "127.0.0.1:9",
            Lane::Data,
            None,
            Some(cancelled),
            TaskPool::current("test-reconnect"),
        )
        .await;

        assert!(result.is_none());
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "cancelled reconnect should not wait for backoff"
        );
        assert_eq!(
            total_reconnect_attempts(),
            before,
            "cancelled reconnect should not record an attempt"
        );
    }
}
