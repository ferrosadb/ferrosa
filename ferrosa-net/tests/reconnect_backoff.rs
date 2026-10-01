/// Tests for P1-31: reconnect backoff, dormant state, and metrics.
///
/// TDD red→green record: these tests were written against the *old* code
/// (no dormant state, mark_failed() took no args, no BACKOFF_INITIAL_MS
/// constant) and were failing before the fix was applied.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ferrosa_net::codec::Lane;
use ferrosa_net::config::NetConfig;
use ferrosa_net::lane_actor::{spawn_lane_actor, ActorReconnectContext, LaneStatusReport};
use ferrosa_net::reconnect::{dormant_peer_count, LaneState};
use ferrosa_net::task_pool::TaskPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Test 1 — dead peer enters dormant after enough failed reconnect cycles
// ---------------------------------------------------------------------------

/// After DORMANT_AFTER_EXHAUSTIONS `MarkFailed` signals the lane must
/// transition to `Dormant`.  Time is paused so no real wall-clock waits.
#[tokio::test(start_paused = true)]
async fn dead_peer_enters_dormant_after_exhausted_reconnects() {
    use ferrosa_net::reconnect::DORMANT_AFTER_EXHAUSTIONS;

    let handle = spawn_lane_actor(
        Lane::Data,
        LaneState::Reconnecting {
            attempt: 0,
            exhaustion_count: 0,
        },
        |h| ActorReconnectContext {
            lane: Lane::Data,
            config: Arc::new(NetConfig::default()),
            local_host_id: Uuid::new_v4(),
            peer_host: "192.0.2.1:9999".to_owned(), // TEST-NET, never reachable
            tls_connector: None,
            cancelled: h.cancel_token(),
            handle: h,
            task_pool: TaskPool::current("test-lane"),
        },
    );

    // Drive the lane to exhaustion: send MarkFailed DORMANT_AFTER_EXHAUSTIONS
    // times, with sequential exhaustion_count values starting at 0.
    for i in 0..DORMANT_AFTER_EXHAUSTIONS {
        handle.mark_failed(i);
        // Let the actor process the command.
        tokio::task::yield_now().await;
        // Advance time past the inter-cycle delay (5 s) to unblock any spawned
        // sleep tasks.
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
    }

    // After DORMANT_AFTER_EXHAUSTIONS exhaustions the lane must be Dormant.
    let status = handle.query_status().await.unwrap();
    assert_eq!(
        status,
        LaneStatusReport::Dormant,
        "expected Dormant after {DORMANT_AFTER_EXHAUSTIONS} exhausted reconnect cycles"
    );

    // Process-wide counter must reflect at least this one dormant lane.
    assert!(
        dormant_peer_count() >= 1,
        "dormant_peer_count should be >= 1, got {}",
        dormant_peer_count()
    );

    handle.shutdown().await;
}

/// Advance the paused clock by `by`, then give the loopback sockets a moment of
/// real time: accepts and handshake bytes are delivered by the kernel in real
/// time and would otherwise be observed after the virtual timers that outlive
/// them.
async fn hop(by: Duration) {
    tokio::time::advance(by).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    std::thread::sleep(Duration::from_millis(1));
}

/// Listen on loopback, accept every connection and never speak. Returns the
/// address and the number of connections accepted so far.
async fn spawn_blackhole() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind blackhole");
    let addr = listener.local_addr().expect("blackhole address");
    let dials = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&dials);
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (stream, _) = listener.accept().await.expect("blackhole accept");
            held.push(stream);
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
    (addr, dials)
}

// ---------------------------------------------------------------------------
// Test 2 — dormant lane stays dormant and rate-limits probes
// ---------------------------------------------------------------------------

/// Once dormant, the lane must stay dormant (probing indefinitely) and fire at
/// most one connection attempt per slow-retry interval.
///
/// Updated for t_48d168ee: a probe used to be a whole `connect_with_retry`
/// cycle (up to `MAX_RECONNECT_ATTEMPTS` attempts) every five minutes, which
/// left a returning peer unreached for many minutes. A probe is now a single
/// attempt every `DORMANT_PROBE_INTERVAL` (+ up to 25% jitter). The original
/// intent, a bounded rate rather than an unbounded one, is kept: over N
/// intervals the lane may make at most N attempts, and at least one.
#[tokio::test(start_paused = true)]
async fn dormant_lane_rate_limits_probes() {
    use ferrosa_net::reconnect::{DORMANT_AFTER_EXHAUSTIONS, DORMANT_PROBE_INTERVAL};

    // A node that accepts TCP and never answers the handshake. Each accept is one
    // dial of the peer, so this counts the lane's probes at the peer. The
    // process-wide `total_reconnect_attempts` is bumped by every other test in
    // the process and cannot be asserted on.
    let (blackhole_addr, dials) = spawn_blackhole().await;

    let handle = spawn_lane_actor(
        Lane::Bulk,
        LaneState::Reconnecting {
            attempt: 0,
            exhaustion_count: 0,
        },
        |h| ActorReconnectContext {
            lane: Lane::Bulk,
            config: Arc::new(NetConfig::default()),
            local_host_id: Uuid::new_v4(),
            peer_host: blackhole_addr.to_string(),
            tls_connector: None,
            cancelled: h.cancel_token(),
            handle: h,
            task_pool: TaskPool::current("test-lane"),
        },
    );

    // Drive to dormant.
    for i in 0..DORMANT_AFTER_EXHAUSTIONS {
        handle.mark_failed(i);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
    }

    let status = handle.query_status().await.unwrap();
    assert_eq!(status, LaneStatusReport::Dormant, "must be Dormant first");

    // The loop above sent MarkFailed back to back, so it left two fast-phase
    // reconnect tasks running (each up to ~4 min of backoff). A real lane has
    // one cycle at a time. Let them finish before measuring, or their attempts
    // are counted as probes.
    for _ in 0..120 {
        hop(Duration::from_secs(5)).await;
    }

    // Advance ten probe intervals, a second at a time so each probe task runs.
    let dials_before = dials.load(Ordering::SeqCst);
    const INTERVALS: u64 = 10;
    for _ in 0..(DORMANT_PROBE_INTERVAL.as_secs() * INTERVALS) {
        hop(Duration::from_secs(1)).await;
    }
    let delta = dials.load(Ordering::SeqCst).saturating_sub(dials_before) as u64;

    // The lower bound is what actually guards the fix: `1..=INTERVALS` would
    // pass for a lane that probed ONCE and then gave up forever, which is the
    // bug this slow-retry phase exists to remove. Probe waits carry up to 25%
    // jitter and the next wait starts only after the previous dial fails, which
    // against this peer takes the 5 s handshake timeout. Worst case a cycle is
    // 30 * 1.25 + 5 = 42.5 s, so 300 s holds floor(300 / 42.5) = 7 dials. That
    // is the floor; the ceiling is 10 (no jitter). Without a bounded dial a
    // peer that never answers hangs the first probe and this saw exactly 1.
    const MIN_ATTEMPTS: u64 = INTERVALS * 4 / 5 - 1;
    assert!(
        (MIN_ATTEMPTS..=INTERVALS).contains(&delta),
        "dormant lane must keep probing about once per interval: expected \
         {MIN_ATTEMPTS}..={INTERVALS} attempts over {INTERVALS} intervals, saw {delta}"
    );

    // Lane must still be Dormant (probes failed, peer still down).
    let status = handle.query_status().await.unwrap();
    assert_eq!(
        status,
        LaneStatusReport::Dormant,
        "lane should remain Dormant while peer is unreachable"
    );

    handle.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 3 — exponential backoff sequence caps at 30s
// ---------------------------------------------------------------------------

/// The backoff schedule between individual TCP-connect attempts must be:
///   1s, 2s, 4s, 8s, 16s, 30s (capped), 30s, 30s …
///
/// Constants `BACKOFF_INITIAL_MS = 1000` and `BACKOFF_CAP_MS = 30000` must
/// exist and have exactly these values.
#[test]
fn exponential_backoff_caps_at_30s() {
    use ferrosa_net::reconnect::{ExponentialBackoff, BACKOFF_CAP_MS, BACKOFF_INITIAL_MS};

    assert_eq!(BACKOFF_INITIAL_MS, 1_000, "initial must be 1 s");
    assert_eq!(BACKOFF_CAP_MS, 30_000, "cap must be 30 s");

    let mut b = ExponentialBackoff::new(
        Duration::from_millis(BACKOFF_INITIAL_MS),
        Duration::from_millis(BACKOFF_CAP_MS),
    );

    // Assert each delay is in [base, base + 25%] (jitter window).
    let assert_range = |actual: Duration, expected_ms: u64| {
        let min = Duration::from_millis(expected_ms);
        let max = Duration::from_millis(expected_ms + expected_ms / 4);
        assert!(
            actual >= min && actual <= max,
            "expected delay in [{min:?}, {max:?}], got {actual:?}"
        );
    };

    assert_range(b.next_delay(), 1_000);
    assert_range(b.next_delay(), 2_000);
    assert_range(b.next_delay(), 4_000);
    assert_range(b.next_delay(), 8_000);
    assert_range(b.next_delay(), 16_000);
    assert_range(b.next_delay(), 30_000); // capped
    assert_range(b.next_delay(), 30_000); // stays capped
    assert_range(b.next_delay(), 30_000); // stays capped
}
