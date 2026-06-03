/// Tests for P1-31: reconnect backoff, dormant state, and metrics.
///
/// TDD red→green record: these tests were written against the *old* code
/// (no dormant state, mark_failed() took no args, no BACKOFF_INITIAL_MS
/// constant) and were failing before the fix was applied.
use std::sync::Arc;
use std::time::Duration;

use ferrosa_net::codec::Lane;
use ferrosa_net::config::NetConfig;
use ferrosa_net::lane_actor::{spawn_lane_actor, ActorReconnectContext, LaneStatusReport};
use ferrosa_net::reconnect::{dormant_peer_count, total_reconnect_attempts, LaneState};
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

// ---------------------------------------------------------------------------
// Test 2 — dormant lane stays dormant and rate-limits probes
// ---------------------------------------------------------------------------

/// Once dormant, the lane must stay dormant and fire at most one connection
/// attempt per probe interval.  We assert that `total_reconnect_attempts`
/// advances by at most `MAX_RECONNECT_ATTEMPTS` across one probe interval
/// (one `connect_with_retry` call), not the old unbounded rate.
#[tokio::test(start_paused = true)]
async fn dormant_lane_rate_limits_probes() {
    use ferrosa_net::reconnect::{DORMANT_AFTER_EXHAUSTIONS, DORMANT_PROBE_INTERVAL};

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
            peer_host: "192.0.2.2:9999".to_owned(),
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

    // Record attempt count, then advance one probe interval.
    let attempts_before = total_reconnect_attempts();
    tokio::time::advance(DORMANT_PROBE_INTERVAL + Duration::from_secs(1)).await;
    // Let spawned tasks run: the DormantProbe fires connect_with_retry.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    // The probe spawns connect_with_retry which runs up to MAX_RECONNECT_ATTEMPTS
    // individual attempts.  But we need time to advance past each attempt's
    // sleep too — with time paused, the connect_with_retry sleeps are instant.
    // Advance enough for all attempts in one probe cycle.
    use ferrosa_net::reconnect::MAX_RECONNECT_ATTEMPTS;
    // Each attempt sleeps up to BACKOFF_CAP_MS (30 s).
    for _ in 0..MAX_RECONNECT_ATTEMPTS {
        tokio::time::advance(Duration::from_secs(35)).await;
        tokio::task::yield_now().await;
    }

    let attempts_after = total_reconnect_attempts();
    let delta = attempts_after.saturating_sub(attempts_before);

    // At most one full connect_with_retry cycle per probe interval.
    assert!(
        delta <= MAX_RECONNECT_ATTEMPTS as u64,
        "dormant probe should fire at most one connect_with_retry cycle ({} attempts), fired {}",
        MAX_RECONNECT_ATTEMPTS,
        delta
    );

    // Lane must still be Dormant (probe failed, peer still down).
    // Allow extra time for the next probe to be scheduled (not yet fired).
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
