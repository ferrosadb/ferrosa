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

/// Default number of TCP-connect attempts per fast-phase `connect_with_retry`
/// invocation before it returns `None` and the caller receives `MarkFailed`.
/// At the 30 s cap: 10 attempts ≈ ~3.5 min of total exposure per cycle.
/// Tunable at run time via [`ENV_FAST_ATTEMPTS`]; read it through
/// [`reconnect_fast_attempts`].
pub const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Number of exhausted fast-phase cycles before the lane drops into the
/// indefinite slow-retry phase (`LaneState::Dormant`).  One cycle ≈ 3.5 min, so
/// the whole fast phase is ≈ ~10 min at defaults.
pub const DORMANT_AFTER_EXHAUSTIONS: u32 = 3;

/// Default interval between single-attempt probes while in the slow-retry
/// (dormant) phase.  Each probe wait gets up to 25% added jitter so peers do
/// not dial in lockstep.  Tunable at run time via [`ENV_SLOW_INTERVAL_MS`];
/// read it through [`slow_retry_interval`].
pub const DORMANT_PROBE_INTERVAL: Duration = Duration::from_secs(30);

/// Environment variable overriding [`MAX_RECONNECT_ATTEMPTS`] (positive integer).
pub const ENV_FAST_ATTEMPTS: &str = "FERROSA_NET_RECONNECT_FAST_ATTEMPTS";

/// Environment variable overriding [`DORMANT_PROBE_INTERVAL`], in milliseconds
/// (positive integer).
pub const ENV_SLOW_INTERVAL_MS: &str = "FERROSA_NET_RECONNECT_SLOW_INTERVAL_MS";

/// Parse a positive integer setting.  `None` (unset) yields `default`
/// silently; a present value that is not a positive integer yields `default`
/// and an `Err` carrying the rejected text so the caller can log it.
fn parse_positive(raw: Option<&str>, default: u64) -> (u64, Option<String>) {
    match raw {
        None => (default, None),
        Some(text) => match text.trim().parse::<u64>() {
            Ok(n) if n > 0 => (n, None),
            _ => (default, Some(text.to_owned())),
        },
    }
}

/// Read a positive integer from the environment, warning once per setting
/// (`warned`) when the value is unusable and the default is used instead.
fn env_positive(name: &'static str, default: u64, warned: &AtomicBool) -> u64 {
    let raw = std::env::var(name).ok();
    let (value, rejected) = parse_positive(raw.as_deref(), default);
    if let Some(text) = rejected {
        if !warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                env = name,
                value = %text,
                default,
                "ignoring invalid reconnect setting; using the default"
            );
        }
    }
    value
}

/// Fast-phase attempts per cycle: [`ENV_FAST_ATTEMPTS`] or
/// [`MAX_RECONNECT_ATTEMPTS`].
pub fn reconnect_fast_attempts() -> u32 {
    static WARNED: AtomicBool = AtomicBool::new(false);
    let n = env_positive(
        ENV_FAST_ATTEMPTS,
        u64::from(MAX_RECONNECT_ATTEMPTS),
        &WARNED,
    );
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// Slow-phase probe interval (before jitter): [`ENV_SLOW_INTERVAL_MS`] or
/// [`DORMANT_PROBE_INTERVAL`].
pub fn slow_retry_interval() -> Duration {
    static WARNED: AtomicBool = AtomicBool::new(false);
    let default_ms = DORMANT_PROBE_INTERVAL.as_millis() as u64;
    Duration::from_millis(env_positive(ENV_SLOW_INTERVAL_MS, default_ms, &WARNED))
}

/// `base` plus 0-25% random jitter.
pub(crate) fn with_jitter(base: Duration) -> Duration {
    let jitter_range = base.as_millis() as u64 / 4;
    if jitter_range == 0 {
        return base;
    }
    let jitter = rand::rng().random_range(0..=jitter_range);
    base.saturating_add(Duration::from_millis(jitter))
}

/// Delay before re-dial attempt number `failures` (0-based) of a peer that has
/// no pool: the fast-phase exponential backoff, capped at the slow-retry
/// interval, with jitter.
pub(crate) fn redial_delay(failures: u32) -> Duration {
    let initial = Duration::from_millis(BACKOFF_INITIAL_MS);
    let cap = slow_retry_interval();
    let scaled = initial.saturating_mul(1u32.checked_shl(failures).unwrap_or(u32::MAX));
    with_jitter(scaled.min(cap))
}

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
///   Reconnecting ─(fast attempts hit)────────────► Reconnecting (exhaustion_count+1)
///   Reconnecting ─(exhaustion_count == DORMANT_AFTER_EXHAUSTIONS)► Dormant
///   Dormant ─(probe ok)──────────────────────────► Connected
///   Dormant ─(probe fail)────────────────────────► Dormant (stays; probes forever)
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
    /// The fast retry cycles were exhausted.  The lane makes one connection
    /// attempt every [`slow_retry_interval`] (plus jitter) until the peer
    /// returns or the lane is shut down.
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
            // Check before waiting: `changed()` only reports changes made after
            // this receiver last saw the value, so a connection that died
            // before the watcher first ran (the actor starts a moment after
            // the client connects) would otherwise leave the lane `Connected`
            // on a dead client forever.
            if !*alive_rx.borrow_and_update() {
                on_dead();
                break;
            }
            if alive_rx.changed().await.is_err() {
                // Sender dropped — connection object gone, nothing to monitor.
                break;
            }
        }
    });
}

// ---------------------------------------------------------------------------
// connect_with_retry
// ---------------------------------------------------------------------------

/// Returns `true` when the optional cancel flag is set.
fn is_cancelled(cancelled: &Option<Arc<AtomicBool>>) -> bool {
    cancelled
        .as_ref()
        .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
}

/// Resolve `peer_host` and make one connection attempt.
///
/// `peer_host` is resolved on every attempt so that container restarts that
/// assign a new IP are handled transparently. Failures are returned, not
/// logged: callers log state edges, never individual attempts (a permanently
/// dead peer would otherwise write a line per attempt forever). Each call
/// counts one reconnect attempt in the process-wide metric.
async fn dial_once(
    config: Arc<NetConfig>,
    local_host_id: Uuid,
    peer_host: &str,
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    task_pool: TaskPool,
) -> Result<RpcClient, String> {
    inc_total_reconnect_attempts();
    // DNS is bounded here and the TCP connect and handshake inside
    // `RpcClient::connect_with_tls_on_pool`, each by its own config timeout, so
    // one dial takes at most connect_timeout*2 + handshake_timeout.
    let peer_addr = match tokio::time::timeout(
        config.connect_timeout,
        tokio::net::lookup_host(peer_host),
    )
    .await
    {
        Ok(Ok(mut addrs)) => addrs
            .next()
            .ok_or_else(|| "DNS resolved no addresses".to_owned())?,
        Ok(Err(e)) => return Err(format!("DNS resolution failed: {e}")),
        Err(_) => return Err("DNS resolution timed out".to_owned()),
    };
    RpcClient::connect_with_tls_on_pool(
        config,
        local_host_id,
        peer_addr,
        tls_connector.as_deref(),
        task_pool,
    )
    .await
    .map_err(|e| format!("connect to {peer_addr} failed: {e}"))
}

/// Fast phase: attempt to open a single connection, retrying with exponential
/// backoff for at most [`reconnect_fast_attempts`] attempts.
///
/// Returns `None` when the attempts are exhausted (or the lane was cancelled);
/// the lane actor then schedules another cycle or drops into slow-retry. Per
/// attempt detail is logged at DEBUG only; the lane actor logs the edges.
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

    for attempt in 1..=reconnect_fast_attempts() {
        if is_cancelled(&cancelled) {
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
        if is_cancelled(&cancelled) {
            tracing::debug!(?lane, peer = peer_host, "reconnect cancelled");
            return None;
        }

        match dial_once(
            config.clone(),
            local_host_id,
            peer_host,
            tls_connector.clone(),
            task_pool.clone(),
        )
        .await
        {
            Ok(client) => return Some(client),
            Err(reason) => {
                tracing::debug!(?lane, attempt, peer = peer_host, %reason, "reconnect attempt failed");
            }
        }
    }

    tracing::debug!(
        ?lane,
        peer = peer_host,
        "fast-phase reconnect cycle exhausted all attempts"
    );
    None
}

/// Slow phase: exactly one connection attempt, no backoff and no loop. The
/// caller schedules the next probe after [`slow_retry_interval`].
///
/// Returns `None` on failure or cancellation; failures are not logged here.
pub(crate) async fn connect_once_cancelable(
    config: Arc<NetConfig>,
    local_host_id: Uuid,
    peer_host: &str,
    lane: Lane,
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    cancelled: Option<Arc<AtomicBool>>,
    task_pool: TaskPool,
) -> Option<RpcClient> {
    if is_cancelled(&cancelled) {
        tracing::debug!(?lane, peer = peer_host, "slow-retry probe cancelled");
        return None;
    }
    match dial_once(config, local_host_id, peer_host, tls_connector, task_pool).await {
        Ok(client) => Some(client),
        Err(reason) => {
            tracing::debug!(?lane, peer = peer_host, %reason, "slow-retry probe failed");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

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

    // `DORMANT_PEER_COUNT` is a process-wide static metric. The two tests that
    // mutate it must not run concurrently (libtest parallelises by default) or
    // one test's absolute-value assertion races the other's mutation — a
    // shared-global-state race, not a timing flake. `#[serial]` keys them so they
    // run one at a time relative to each other.
    #[test]
    #[serial(dormant_peer_counter)]
    fn dormant_peer_counter_increments_and_decrements() {
        // Use a fresh counter baseline by reading current value.
        let before = dormant_peer_count();
        inc_dormant_peer_count();
        assert_eq!(dormant_peer_count(), before + 1);
        dec_dormant_peer_count();
        assert_eq!(dormant_peer_count(), before);
    }

    #[test]
    #[serial(dormant_peer_counter)]
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

    #[test]
    fn parse_positive_accepts_positive_integers_and_defaults_the_rest() {
        assert_eq!(parse_positive(None, 10), (10, None));
        assert_eq!(parse_positive(Some("25"), 10), (25, None));
        assert_eq!(parse_positive(Some(" 25 "), 10), (25, None));
        for bad in ["0", "-3", "abc", "", "1.5"] {
            assert_eq!(
                parse_positive(Some(bad), 10),
                (10, Some(bad.to_owned())),
                "{bad:?} must fall back to the default and be reported"
            );
        }
    }

    #[test]
    fn with_jitter_adds_at_most_a_quarter() {
        let base = Duration::from_secs(30);
        for _ in 0..200 {
            let d = with_jitter(base);
            assert!(d >= base && d <= base + base / 4, "{d:?}");
        }
        assert_eq!(with_jitter(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn redial_delay_backs_off_then_settles_at_the_slow_interval() {
        let cap = slow_retry_interval();
        assert_in_range(redial_delay(0), BACKOFF_INITIAL_MS);
        assert_in_range(redial_delay(1), 2 * BACKOFF_INITIAL_MS);
        assert_in_range(redial_delay(3), 8 * BACKOFF_INITIAL_MS);
        for failures in [10, 31, 32, 1000, u32::MAX] {
            assert_in_range(redial_delay(failures), cap.as_millis() as u64);
        }
    }

    #[test]
    fn slow_phase_defaults_are_sane() {
        const { assert!(DORMANT_PROBE_INTERVAL.as_secs() >= 1) };
        assert_eq!(slow_retry_interval(), DORMANT_PROBE_INTERVAL);
        assert_eq!(reconnect_fast_attempts(), MAX_RECONNECT_ATTEMPTS);
    }

    /// A connection that died before the watcher first polled (the lane actor
    /// is spawned a moment after the client connects) must still trigger
    /// `on_dead`. `watch::Receiver::changed` only reports changes made after
    /// the receiver was created, so a watcher that only waits on it leaves the
    /// lane `Connected` on a dead client forever.
    #[tokio::test]
    async fn alive_watcher_fires_when_connection_died_before_it_ran() {
        let (tx, _initial_rx) = watch::channel(true);
        tx.send(false).expect("receiver alive");
        // Subscribed after the death, exactly as `RpcClient::alive_rx` is.
        let rx = tx.subscribe();
        let fired = Arc::new(AtomicBool::new(false));
        let fired_in_watcher = Arc::clone(&fired);
        spawn_alive_watcher(
            rx,
            move || fired_in_watcher.store(true, Ordering::SeqCst),
            TaskPool::current("test-watcher"),
        );
        for _ in 0..50 {
            if fired.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            fired.load(Ordering::SeqCst),
            "on_dead never fired for a connection that was already dead"
        );
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
