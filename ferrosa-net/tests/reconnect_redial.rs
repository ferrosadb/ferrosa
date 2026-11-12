//! A reconnecting lane must dial as soon as it is told the peer is back.
//!
//! Nightly pair smoke, 2026-10-23: node2 and node3 were stopped for ~20 s and
//! restarted on the same addresses. They dialled node1 inbound within a second,
//! but node1's outbound lanes to them were asleep in a 16 s reconnect backoff,
//! so a QUORUM write coordinated by node1 failed with "lane is reconnecting"
//! (received=1, required=2) although every replica was up. An inbound handshake
//! is proof the peer is back; `LaneHandle::redial_now` is how that proof reaches
//! the lane, and every wait in the reconnect ladder must honour it.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ferrosa_net::codec::Lane;
use ferrosa_net::config::NetConfig;
use ferrosa_net::lane_actor::{
    spawn_lane_actor, ActorReconnectContext, LaneHandle, LaneStatusReport,
};
use ferrosa_net::reconnect::{LaneState, DORMANT_AFTER_EXHAUSTIONS};
use ferrosa_net::task_pool::TaskPool;
use uuid::Uuid;

/// Advance the paused clock by `by`, then give loopback sockets a moment of
/// real time (accepts are delivered by the kernel in real time).
async fn hop(by: Duration) {
    tokio::time::advance(by).await;
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    std::thread::sleep(Duration::from_millis(1));
}

/// Listen on loopback, accept every connection and never speak. Each accept is
/// one dial by the lane. Returns the address and the dial count.
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

fn spawn_reconnecting_lane(peer: std::net::SocketAddr) -> LaneHandle {
    spawn_lane_actor(
        Lane::Data,
        LaneState::Reconnecting {
            attempt: 0,
            exhaustion_count: 0,
        },
        move |h| ActorReconnectContext {
            lane: Lane::Data,
            config: Arc::new(NetConfig::default()),
            local_host_id: Uuid::new_v4(),
            peer_host: peer.to_string(),
            tls_connector: None,
            cancelled: h.cancel_token(),
            handle: h,
            task_pool: TaskPool::current("test-redial"),
        },
    )
}

/// Hop one second at a time until the peer has seen `target` dials.
async fn hop_until_dials(dials: &AtomicUsize, target: usize, max_secs: u32) {
    for _ in 0..max_secs {
        if dials.load(Ordering::SeqCst) >= target {
            return;
        }
        hop(Duration::from_secs(1)).await;
    }
    panic!(
        "expected {target} dials within {max_secs} s, saw {}",
        dials.load(Ordering::SeqCst)
    );
}

/// Assert the lane dials once more within 500 ms of `redial_now`, far less
/// than the wait it was in.
async fn assert_redial_is_prompt(handle: &LaneHandle, dials: &AtomicUsize, waiting_in: &str) {
    let before = dials.load(Ordering::SeqCst);
    handle.redial_now();
    for _ in 0..5 {
        hop(Duration::from_millis(100)).await;
    }
    let after = dials.load(Ordering::SeqCst);
    assert_eq!(
        after,
        before + 1,
        "redial_now during {waiting_in} must dial within 500 ms (dials before={before}, after={after})"
    );
}

/// A served redial must not leave the lane dialling in a tight loop: once the
/// redial's dial fails (5 s handshake timeout here) the lane is back in an
/// ordinary wait for at least `quiet_secs`.
async fn assert_back_to_waiting(dials: &AtomicUsize, quiet_secs: u64) {
    hop(Duration::from_secs(6)).await;
    let after_timeout = dials.load(Ordering::SeqCst);
    for _ in 0..quiet_secs {
        hop(Duration::from_secs(1)).await;
    }
    assert_eq!(
        dials.load(Ordering::SeqCst),
        after_timeout,
        "after a served redial fails the lane must wait again, not keep dialling"
    );
}

#[tokio::test(start_paused = true)]
async fn redial_now_cuts_short_the_pause_between_reconnect_cycles() {
    let (addr, dials) = spawn_blackhole().await;
    let handle = spawn_reconnecting_lane(addr);

    // An exhausted fast cycle schedules the next one after a 5 s pause.
    handle.mark_failed(0);
    hop(Duration::from_secs(1)).await;
    assert_eq!(dials.load(Ordering::SeqCst), 0, "still inside the pause");

    assert_redial_is_prompt(&handle, &dials, "the inter-cycle pause").await;
    handle.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn redial_now_cuts_short_a_fast_phase_backoff() {
    let (addr, dials) = spawn_blackhole().await;
    let handle = spawn_reconnecting_lane(addr);
    handle.mark_failed(0);

    // Backoff runs 1, 2, 4, 8, 16 s (+ up to 25% jitter) between dials, each of
    // which hangs for the 5 s handshake timeout against the blackhole. After
    // the 4th dial times out the lane sleeps at least 16 s.
    hop_until_dials(&dials, 4, 120).await;
    hop(Duration::from_secs(7)).await;
    assert_eq!(
        dials.load(Ordering::SeqCst),
        4,
        "must be inside the 16 s backoff"
    );

    assert_redial_is_prompt(&handle, &dials, "a 16 s backoff").await;
    // Next backoff is the 30 s cap.
    assert_back_to_waiting(&dials, 20).await;
    handle.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn redial_now_cuts_short_the_slow_retry_interval() {
    let (addr, dials) = spawn_blackhole().await;
    let handle = spawn_reconnecting_lane(addr);
    for i in 0..DORMANT_AFTER_EXHAUSTIONS {
        handle.mark_failed(i);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(
        handle.query_status().await.unwrap(),
        LaneStatusReport::Dormant
    );
    // Let the stray fast cycles the loop above started run out (see
    // reconnect_backoff.rs), then wait for a probe and land mid-interval.
    for _ in 0..120 {
        hop(Duration::from_secs(5)).await;
    }
    let settled = dials.load(Ordering::SeqCst);
    hop_until_dials(&dials, settled + 1, 60).await;
    hop(Duration::from_secs(10)).await;
    let mid_interval = dials.load(Ordering::SeqCst);
    hop(Duration::from_secs(5)).await;
    assert_eq!(
        dials.load(Ordering::SeqCst),
        mid_interval,
        "must be inside the 30 s slow-retry interval"
    );

    assert_redial_is_prompt(&handle, &dials, "the slow-retry interval").await;
    assert_back_to_waiting(&dials, 20).await;
    handle.shutdown().await;
}
