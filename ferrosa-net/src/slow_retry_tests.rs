//! Lane reconnect must outlive any outage (t_48d168ee).
//!
//! These tests run the lane actor against fake peers that come and go on a
//! fixed loopback port while tokio's clock is paused, so an outage far longer
//! than the whole fast-retry budget costs milliseconds of wall time. The clock
//! is paused only after each connection is established. All of
//! them touch the process-wide reconnect counters, so they share one serial
//! key.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serial_test::serial;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;
use uuid::Uuid;

use crate::codec::{InternodeCodec, Lane};
use crate::config::NetConfig;
use crate::handshake::accept_handshake;
use crate::lane_actor::{spawn_lane_actor, ActorReconnectContext, LaneHandle, LaneStatusReport};
use crate::reconnect::{total_reconnect_attempts, LaneState};
use crate::rpc::client::RpcClient;
use crate::task_pool::TaskPool;

const MINUTE: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Fake peer: binds late, answers handshakes under a chosen host id, and can
// go away again after a fixed time.
// ---------------------------------------------------------------------------

struct FakePeer {
    accepted: Arc<AtomicUsize>,
}

impl FakePeer {
    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

/// Reserve a loopback port nothing listens on.
pub(crate) fn free_addr() -> SocketAddr {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let addr = probe.local_addr().expect("reserved port address");
    drop(probe);
    addr
}

/// Bind `addr` after `start_after` and answer handshakes as `id`; stop
/// listening and drop every connection after `up_for` (never if `None`).
fn spawn_fake_peer(
    addr: SocketAddr,
    id: Uuid,
    start_after: Duration,
    up_for: Option<Duration>,
) -> FakePeer {
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    tokio::spawn(async move {
        tokio::time::sleep(start_after).await;
        let listener = TcpListener::bind(addr).await.expect("fake peer bind");
        serve(listener, id, up_for, false, counter).await;
    });
    FakePeer { accepted }
}

/// Like [`spawn_fake_peer`] but on an already-bound listener, so the first
/// connection cannot race the bind.
fn spawn_bound_fake_peer(listener: TcpListener, id: Uuid, up_for: Duration) -> FakePeer {
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    tokio::spawn(serve(listener, id, Some(up_for), true, counter));
    FakePeer { accepted }
}

/// With `arm_on_first_accept` the `up_for` window starts at the first accepted
/// connection instead of at bind, so a paused clock cannot jump past it while
/// the first client is still mid-connect.
async fn serve(
    listener: TcpListener,
    id: Uuid,
    up_for: Option<Duration>,
    arm_on_first_accept: bool,
    accepted: Arc<AtomicUsize>,
) {
    let config = NetConfig::default();
    let mut deadline = match (up_for, arm_on_first_accept) {
        (Some(d), false) => Some(tokio::time::Instant::now() + d),
        _ => None,
    };
    let mut held = Vec::new();
    loop {
        let stream = match deadline {
            Some(deadline) => {
                tokio::select! {
                    res = listener.accept() => res.expect("fake peer accept").0,
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
            None => listener.accept().await.expect("fake peer accept").0,
        };
        if deadline.is_none() && arm_on_first_accept {
            deadline = up_for.map(|d| tokio::time::Instant::now() + d);
        }
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
        if accept_handshake(&mut framed, &config, id).await.is_ok() {
            accepted.fetch_add(1, Ordering::SeqCst);
            held.push(framed);
        }
    }
}

// ---------------------------------------------------------------------------
// Log capture: INFO and above from this crate.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<String>>>);

impl Captured {
    fn lines(&self) -> Vec<String> {
        self.0.lock().expect("log capture lock").clone()
    }

    fn count(&self, needle: &str) -> usize {
        self.lines().iter().filter(|l| l.contains(needle)).count()
    }
}

struct FieldText(String);

impl tracing::field::Visit for FieldText {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        // Writing into a String cannot fail.
        let _ = write!(self.0, "{}={:?} ", field.name(), value);
    }
}

impl<S: tracing::Subscriber> Layer<S> for Captured {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if *meta.level() > tracing::Level::INFO || !meta.target().starts_with("ferrosa_net") {
            return;
        }
        let mut text = FieldText(String::new());
        event.record(&mut text);
        self.0
            .lock()
            .expect("log capture lock")
            .push(format!("{} {}", meta.level(), text.0));
    }
}

fn capture_logs() -> (tracing::subscriber::DefaultGuard, Captured) {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::registry().with(captured.clone());
    (tracing::subscriber::set_default(subscriber), captured)
}

// ---------------------------------------------------------------------------
// Lane helpers
// ---------------------------------------------------------------------------

async fn connected_lane(addr: SocketAddr) -> LaneHandle {
    let config = Arc::new(NetConfig::default());
    let local_host_id = Uuid::new_v4();
    let client = RpcClient::connect_with_tls_on_pool(
        Arc::clone(&config),
        local_host_id,
        addr,
        None,
        TaskPool::current("slow-retry-test"),
    )
    .await
    .expect("initial connect to the fake peer");
    spawn_lane_actor(Lane::Data, LaneState::Connected(client), move |h| {
        ActorReconnectContext {
            lane: Lane::Data,
            config,
            local_host_id,
            peer_host: addr.to_string(),
            tls_connector: None,
            cancelled: h.cancel_token(),
            handle: h,
            task_pool: TaskPool::current("slow-retry-test"),
        }
    })
}

/// Advance the paused clock by `total` in `hop`-sized steps, pausing real time
/// between steps. Loopback readiness (FIN, accept, handshake bytes) is
/// delivered by the kernel in real time; hopping virtual time without ever
/// yielding real time lets a timer fire before an already-sent byte is
/// observed.
async fn run_for(total: Duration) {
    advance_hops(total, Duration::from_secs(5)).await;
}

pub(crate) async fn advance_hops(total: Duration, hop: Duration) {
    let mut elapsed = Duration::ZERO;
    while elapsed < total {
        let step = hop.min(total - elapsed);
        tokio::time::sleep(step).await;
        // Deliberate real-time wait inside a paused-clock test.
        std::thread::sleep(Duration::from_millis(2));
        elapsed += step;
    }
}

/// Advance virtual time one second at a time until `done` holds for the lane's
/// status, or `budget` of virtual time has passed. Returns whether it held.
async fn run_until(
    handle: &LaneHandle,
    budget: Duration,
    done: impl Fn(LaneStatusReport) -> bool,
) -> bool {
    let mut elapsed = Duration::ZERO;
    while elapsed < budget {
        if done(status(handle).await) {
            return true;
        }
        advance_hops(Duration::from_secs(1), Duration::from_secs(1)).await;
        elapsed += Duration::from_secs(1);
    }
    done(status(handle).await)
}

async fn status(handle: &LaneHandle) -> LaneStatusReport {
    handle.query_status().await.expect("lane actor alive")
}

/// Start a lane against a peer that drops it after one second; returns once
/// the lane has noticed and left `Connected`.
async fn lane_whose_peer_just_died(addr: SocketAddr, id: Uuid) -> LaneHandle {
    let listener = TcpListener::bind(addr).await.expect("bind first peer");
    let first = spawn_bound_fake_peer(listener, id, Duration::from_secs(1));
    let handle = connected_lane(addr).await;
    // Freeze the clock only after the connect: a paused clock jumps forward
    // whenever the runtime is idle, including while it waits on a loopback
    // handshake, and would fire the handshake timeout.
    tokio::time::pause();
    assert!(
        run_until(&handle, Duration::from_secs(30), |s| s
            != LaneStatusReport::Connected)
        .await,
        "lane must notice that its peer went away"
    );
    assert_eq!(first.accepted(), 1, "first peer served the initial connect");
    handle
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A connection that dies while nothing is subscribed to its liveness channel
/// (the lane actor subscribes a moment after the client is built, and a
/// reconnected client sits in the actor's mailbox first) must still read as
/// dead to a later subscriber. `watch::Sender::send` drops the value when no
/// receiver exists, which left such a lane `Connected` on a dead client
/// forever.
#[tokio::test]
#[serial(net_reconnect_counters)]
async fn client_death_with_no_subscriber_is_visible_to_a_late_subscriber() {
    let addr = free_addr();
    let listener = TcpListener::bind(addr).await.expect("bind peer");
    let _peer = spawn_bound_fake_peer(listener, Uuid::new_v4(), Duration::from_secs(1));
    let client = RpcClient::connect_with_tls_on_pool(
        Arc::new(NetConfig::default()),
        Uuid::new_v4(),
        addr,
        None,
        TaskPool::current("slow-retry-test"),
    )
    .await
    .expect("connect to the fake peer");
    tokio::time::pause(); // after the connect; see `lane_whose_peer_just_died`

    run_for(Duration::from_secs(30)).await;

    assert!(
        !*client.alive_rx().borrow(),
        "a subscriber that arrives after the connection died sees it alive"
    );
}

/// A peer that is down for far longer than the whole fast-retry budget (about
/// ten minutes at defaults) must still be reconnected soon after it returns.
/// Before the fix the lane retried on a five-minute probe cycle and never
/// converged within a bound.
#[tokio::test]
#[serial(net_reconnect_counters)]
async fn lane_reconnects_after_outage_longer_than_fast_budget() {
    let addr = free_addr();
    let peer_id = Uuid::new_v4();
    let handle = lane_whose_peer_just_died(addr, peer_id).await;

    let returned = spawn_fake_peer(addr, peer_id, 20 * MINUTE, None);

    run_for(20 * MINUTE).await;
    assert_ne!(status(&handle).await, LaneStatusReport::Connected);

    // Slow-retry interval is 30 s plus up to 25% jitter; two minutes is ample.
    assert!(
        run_until(&handle, 2 * MINUTE, |s| s == LaneStatusReport::Connected).await,
        "lane did not reconnect within two minutes of the peer returning"
    );
    assert!(returned.accepted() >= 1);
    handle.shutdown().await;
}

/// An hour of outage must produce one line when the lane drops into
/// slow-retry and one when it recovers, nothing in between.
#[tokio::test]
#[serial(net_reconnect_counters)]
async fn slow_retry_logs_the_drop_and_the_recovery_once_each() {
    let (_guard, logs) = capture_logs();
    let addr = free_addr();
    let peer_id = Uuid::new_v4();
    let handle = lane_whose_peer_just_died(addr, peer_id).await;
    let _returned = spawn_fake_peer(addr, peer_id, 60 * MINUTE, None);

    run_for(62 * MINUTE).await;
    assert_eq!(status(&handle).await, LaneStatusReport::Connected);

    let lines = logs.lines();
    assert_eq!(
        logs.count("entering slow-retry"),
        1,
        "expected one drop-into-slow-retry line, got: {lines:#?}"
    );
    assert_eq!(
        logs.count("reconnected after slow-retry"),
        1,
        "expected one recovery line, got: {lines:#?}"
    );
    let entered = lines
        .iter()
        .position(|l| l.contains("entering slow-retry"))
        .expect("drop line present");
    let recovered = lines
        .iter()
        .position(|l| l.contains("reconnected after slow-retry"))
        .expect("recovery line present");
    assert_eq!(
        recovered,
        entered + 1,
        "no INFO+ line may be written per slow-retry attempt, got: {lines:#?}"
    );
    handle.shutdown().await;
}

/// Shutting a lane down (what `remove_peer` does through `pool.shutdown`) must
/// end the retrying: no further attempts, no task left behind, and the peer
/// coming back afterwards is not connected to.
#[tokio::test]
#[serial(net_reconnect_counters)]
async fn shutdown_during_slow_retry_stops_attempts_and_leaks_no_task() {
    let addr = free_addr();
    let peer_id = Uuid::new_v4();
    let returned = spawn_fake_peer(addr, peer_id, 40 * MINUTE, None);
    let tasks_before_lane = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let first = TcpListener::bind(addr).await.expect("bind first peer");
    // The late peer would collide with the first bind, so this test hands the
    // first peer the port and lets the late peer bind after it is released.
    let first = spawn_bound_fake_peer(first, peer_id, Duration::from_secs(1));
    let handle = connected_lane(addr).await;
    tokio::time::pause(); // after the connect; see `lane_whose_peer_just_died`
    run_for(25 * MINUTE).await;
    assert_ne!(status(&handle).await, LaneStatusReport::Connected);
    assert_eq!(first.accepted(), 1);

    handle.shutdown().await;
    // One slow-retry interval (plus jitter) for in-flight sleeps to notice.
    run_for(2 * MINUTE).await;
    let attempts_settled = total_reconnect_attempts();
    let tasks_settled = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();

    run_for(30 * MINUTE).await;
    assert_eq!(
        total_reconnect_attempts(),
        attempts_settled,
        "a shut-down lane kept dialing"
    );
    assert_eq!(
        returned.accepted(),
        0,
        "a shut-down lane reconnected to the returning peer"
    );
    let tasks_after = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    assert!(
        tasks_after <= tasks_settled && tasks_settled <= tasks_before_lane,
        "retry tasks leaked: before lane {tasks_before_lane}, settled {tasks_settled}, later {tasks_after}"
    );
}

/// The address now answers as a different node (container IP reuse): the lane
/// must refuse it, say so once, and keep retrying until the real peer is back.
#[tokio::test]
#[serial(net_reconnect_counters)]
async fn reconnect_to_address_now_owned_by_another_node_is_refused() {
    let (_guard, logs) = capture_logs();
    let addr = free_addr();
    let peer_id = Uuid::new_v4();
    let handle = lane_whose_peer_just_died(addr, peer_id).await;

    let impostor = spawn_fake_peer(
        addr,
        Uuid::new_v4(),
        Duration::from_secs(30),
        Some(10 * MINUTE),
    );
    let real = spawn_fake_peer(addr, peer_id, 12 * MINUTE, None);

    run_for(5 * MINUTE).await;
    assert!(
        impostor.accepted() >= 1,
        "the lane never dialed the impostor, so nothing was refused"
    );
    assert_ne!(
        status(&handle).await,
        LaneStatusReport::Connected,
        "lane attached to a different node that took the address"
    );
    assert_eq!(
        logs.count("identity mismatch"),
        1,
        "refusal must be reported once, got: {:#?}",
        logs.lines()
    );

    run_for(10 * MINUTE).await;
    assert_eq!(
        status(&handle).await,
        LaneStatusReport::Connected,
        "lane did not reconnect to the real peer once it returned"
    );
    assert!(real.accepted() >= 1);
    handle.shutdown().await;
}
