//! Actor-based lane management for cancel-safe internode RPC.
//!
//! Each network lane (Raft, Data, Bulk) is owned by a single spawned task that
//! processes `LaneCommand`s sequentially via an mpsc channel.  Callers
//! interact through [`LaneHandle`], a thin Clone wrapper around the sender.
//!
//! This design eliminates the cancel-safety hazard of holding a `tokio::Mutex`
//! across `await` points (network round-trips).  The actor exclusively owns
//! [`LaneState`], so no mutex is needed.
//!
//! ## Reconnect lifecycle
//!
//! A lane never gives up on its peer. A short outage is retried quickly; a
//! long one is retried slowly and indefinitely, until the peer returns or the
//! lane is shut down (`remove_peer`).
//!
//! ```text
//! Connected ──(disconnect)──► Reconnecting(exhaustion_count=0)   fast phase:
//!                                    │                           exponential backoff,
//!                    reconnect_fast_attempts() reached           ≤ 30 s between attempts
//!                                    │
//!                                    ▼
//!                       exhaustion_count+1 < DORMANT_AFTER_EXHAUSTIONS?
//!                              yes │                  no │
//!                                  ▼                     ▼
//!                            Reconnecting              Dormant (slow-retry)
//!                         (exhaustion_count+1)    one attempt every slow_retry_interval()
//!                                                 (+ up to 25% jitter), forever
//!                                                       │ success
//!                                                       ▼
//!                                                   Connected
//! ```
//!
//! Logging is edge-only: one line on losing the connection, one on dropping
//! into slow-retry, one on recovery. Per-attempt detail is DEBUG, and the
//! attempts are counted in `reconnect::total_reconnect_attempts`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, Notify};
use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::{NetError, Result};
use crate::message::Message;
use crate::metrics;
use crate::reconnect::{
    connect_once_cancelable, connect_with_retry_cancelable, dec_dormant_peer_count,
    inc_dormant_peer_count, slow_retry_interval, spawn_alive_watcher, with_jitter, LaneState,
    DORMANT_AFTER_EXHAUSTIONS,
};
use crate::rpc::client::RpcClient;
use crate::task_pool::TaskPool;

/// Default channel capacity for lane actor commands. Read via
/// `lane_channel_capacity()` so operators can tune via
/// `FERROSA_LANE_CHANNEL_CAPACITY` without recompiling — the value was
/// raised 64 → 256 to fix the Raft starvation bug
/// (specs/in-process/bug-bulk-write-raft-starvation.md), and any
/// future workload-specific tuning shouldn't require another rebuild.
pub const DEFAULT_LANE_CHANNEL_CAPACITY: usize = 256;
const DATA_LANE_CAP_RETRY_DELAY: Duration = Duration::from_micros(250);

/// Resolved channel capacity. Looks up
/// `FERROSA_LANE_CHANNEL_CAPACITY` once per call; any value that
/// fails to parse or is zero falls back to
/// `DEFAULT_LANE_CHANNEL_CAPACITY`.
pub fn lane_channel_capacity() -> usize {
    std::env::var("FERROSA_LANE_CHANNEL_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_LANE_CHANNEL_CAPACITY)
}

pub const DEFAULT_LANE_PENDING_STREAM_CAPACITY: usize = 4096;

pub fn lane_pending_stream_capacity() -> usize {
    std::env::var("FERROSA_LANE_PENDING_STREAM_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_LANE_PENDING_STREAM_CAPACITY)
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands sent to a lane actor via [`LaneHandle`].
pub(crate) enum LaneCommand {
    /// Request/response RPC: send a message and wait for a reply.
    Send {
        msg: Message,
        timeout: Duration,
        reply: oneshot::Sender<Result<Message>>,
    },
    /// Fire-and-forget: send a message with no response expected.
    Fire {
        msg: Message,
        timeout: Duration,
        reply: oneshot::Sender<Result<()>>,
    },
    SendComplete {
        result: Result<Message>,
        reply: oneshot::Sender<Result<Message>>,
    },
    FireComplete {
        result: Result<()>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Internal wake-up used when a lane was blocked by the process-wide Data
    /// lane cap and no completion on this particular peer lane is guaranteed
    /// to arrive.
    DispatchPending,
    /// Replace the current RPC client (used after successful reconnect).
    SwapClient(RpcClient),
    /// Mark the current connection unusable and stop dispatching new work to it.
    ConnectionLost,
    /// Signal that one full `connect_with_retry` cycle was exhausted.
    ///
    /// Carries the `exhaustion_count` value at the time of spawning so the
    /// actor can detect stale signals from earlier reconnect tasks that raced
    /// with a successful connection.
    MarkFailed { exhaustion_count: u32 },
    /// Trigger a slow-retry probe attempt.  Sent by the dormant wake-up task
    /// after sleeping for one jittered [`slow_retry_interval`].
    DormantProbe,
    /// Query the current lane status.
    QueryStatus {
        reply: oneshot::Sender<LaneStatusReport>,
    },
    /// Gracefully shut down the actor loop.
    #[allow(dead_code)] // used in tests; part of actor API
    Shutdown,
}

enum PendingLaneCommand {
    Send {
        msg: Message,
        timeout: Duration,
        reply: oneshot::Sender<Result<Message>>,
        queued_at: Instant,
    },
    Fire {
        msg: Message,
        timeout: Duration,
        reply: oneshot::Sender<Result<()>>,
        queued_at: Instant,
    },
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Snapshot of a lane's current state, returned by [`LaneHandle::query_status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneStatusReport {
    Connected,
    Reconnecting,
    /// The lane is in the slow-retry phase: the fast reconnect cycles were
    /// exhausted. It probes the peer once per [`slow_retry_interval`] (plus
    /// jitter) for as long as the lane exists.
    Dormant,
    /// Kept for legacy callers; the actor never transitions to this in normal
    /// operation — use `Dormant` instead.
    Failed,
}

// ---------------------------------------------------------------------------
// LaneHandle
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Thread reaping
// ---------------------------------------------------------------------------

/// Wakes a lane actor's dedicated OS thread when the last [`LaneHandle`] is
/// dropped, so that a pool which is created but never installed (a dial that
/// times out, a cancelled `spawn_tracked`, a retry that supersedes it) does not
/// leave its thread — and its single-threaded tokio runtime — alive forever.
///
/// [`LaneHandle::shutdown`] remains the graceful path: it delivers
/// `LaneCommand::Shutdown` so the actor can drain. This is the backstop for the
/// path that never gets to call it. It is only safe because the actor's own
/// `ActorReconnectContext` holds a handle clone, so "sender count reaches zero"
/// can never be used as the signal — the reaper is explicit instead.
pub(crate) struct ThreadReaper {
    notify: Notify,
    cancelled: Arc<AtomicBool>,
    /// Live [`LaneHandle`] clones. Reaching zero is the only condition under
    /// which a dropped handle asks the thread to exit — a *temporary* clone
    /// dropped while the pool still holds one must not kill a healthy actor.
    handles: AtomicUsize,
}

impl ThreadReaper {
    fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            notify: Notify::new(),
            cancelled,
            handles: AtomicUsize::new(0),
        }
    }

    /// Register a new handle clone against this reaper.
    fn acquire(self: &Arc<Self>) -> Arc<HandleGuard> {
        self.handles.fetch_add(1, Ordering::AcqRel);
        Arc::new(HandleGuard {
            reaper: Arc::clone(self),
        })
    }

    /// Release a handle clone; wake the thread if it was the last one.
    fn release(&self) {
        if self.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.cancelled.store(true, Ordering::Release);
            self.notify.notify_one();
        }
    }

    /// Resolve once the last handle is dropped.
    pub(crate) async fn wait(&self) {
        self.notify.notified().await;
    }

    /// Whether the thread has been asked to exit.
    pub(crate) fn is_woken(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// One handle clone's registration, held inside its [`LaneHandle`]. Counting
/// lives here (not in the handle) because [`LaneHandle`] derives `Clone`: a
/// derived clone would copy the count, whereas `Arc::clone` of a guard shares
/// it. Releasing through the guard keeps the count exact under every clone.
pub(crate) struct HandleGuard {
    reaper: Arc<ThreadReaper>,
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        self.reaper.release();
    }
}

/// Cancel-safe handle for sending commands to a lane actor.
///
/// `Clone` is cheap (just an `mpsc::Sender` clone + a `Lane` copy).
/// All methods use `reserve().await` + `permit.send()` for cancel safety:
/// if a caller is cancelled between reserving a slot and sending the command,
/// the permit is simply dropped — no half-sent state.
///
/// For a pooled (Raft lane) actor the handle also carries a shared
/// `HandleGuard`, which drops exactly once — when the last handle goes away —
/// and wakes the actor's reaper. The actor's own `ActorReconnectContext` clone
/// deliberately carries **no** guard, so it does not hold the count open.
#[derive(Clone)]
pub struct LaneHandle {
    tx: mpsc::Sender<LaneCommand>,
    lane: Lane,
    default_timeout: Duration,
    cancelled: Arc<AtomicBool>,
    /// `None` for actors that run on a runtime owned elsewhere (a `TaskPool`);
    /// only the pooled Raft lane actor needs reaping.
    _guard: Option<Arc<HandleGuard>>,
}

impl LaneHandle {
    /// Which lane this handle targets.
    #[allow(dead_code)] // used in tests; part of actor API
    pub fn lane(&self) -> Lane {
        self.lane
    }
    pub fn cancel_token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    /// The thread reaper this actor is registered with, if it is a
    /// dedicated-thread (Raft lane) actor. Test-only accessor.
    #[cfg(test)]
    pub(crate) fn reaper(&self) -> Option<Arc<ThreadReaper>> {
        self._guard.as_ref().map(|guard| Arc::clone(&guard.reaper))
    }

    /// Send a request/response message through the lane actor.
    ///
    /// Uses `reserve().await` + `permit.send()` for cancel safety.
    /// Falls back to `lane.timeout()` when `timeout_override` is `None`.
    pub(crate) async fn send(
        &self,
        msg: Message,
        timeout_override: Option<Duration>,
    ) -> Result<Message> {
        let timeout = timeout_override.unwrap_or(self.default_timeout);
        let (reply_tx, reply_rx) = oneshot::channel();

        // Reserve a slot in the channel — cancel-safe because dropping the
        // permit before calling `permit.send()` simply releases the slot.
        let started = Instant::now();
        let permit = self
            .tx
            .reserve()
            .await
            .map_err(|_| NetError::LaneShutdown)?;
        metrics::record_lane_queue_wait(self.lane, started.elapsed());
        let capacity = self.tx.max_capacity();
        metrics::observe_lane_queue(
            self.lane,
            capacity.saturating_sub(self.tx.capacity()),
            capacity,
        );
        permit.send(LaneCommand::Send {
            msg,
            timeout,
            reply: reply_tx,
        });

        reply_rx.await.map_err(|_| NetError::LaneShutdown)?
    }

    /// Fire-and-forget a message through the lane actor.
    ///
    /// Uses `reserve().await` + `permit.send()` for cancel safety.
    pub(crate) async fn fire(
        &self,
        msg: Message,
        timeout_override: Option<Duration>,
    ) -> Result<()> {
        let timeout = timeout_override.unwrap_or(self.default_timeout);
        let (reply_tx, reply_rx) = oneshot::channel();

        let started = Instant::now();
        let permit = self
            .tx
            .reserve()
            .await
            .map_err(|_| NetError::LaneShutdown)?;
        metrics::record_lane_queue_wait(self.lane, started.elapsed());
        let capacity = self.tx.max_capacity();
        metrics::observe_lane_queue(
            self.lane,
            capacity.saturating_sub(self.tx.capacity()),
            capacity,
        );
        permit.send(LaneCommand::Fire {
            msg,
            timeout,
            reply: reply_tx,
        });

        reply_rx.await.map_err(|_| NetError::LaneShutdown)?
    }

    /// Attempt to swap in a new RPC client (best-effort, non-blocking).
    pub(crate) fn try_swap_client(&self, client: RpcClient) {
        if let Err(e) = self.tx.try_send(LaneCommand::SwapClient(client)) {
            tracing::error!(%e, "net: lane command send failed");
        }
    }

    /// Transition a connected lane to reconnecting after its TCP client dies.
    pub(crate) fn mark_reconnecting(&self) {
        if let Err(e) = self.tx.try_send(LaneCommand::ConnectionLost) {
            tracing::error!(%e, "net: lane command send failed");
        }
    }

    /// Signal that a `connect_with_retry` cycle was exhausted.
    ///
    /// `exhaustion_count` is the count that was current when the retry task
    /// was spawned; the actor uses it to discard stale signals.
    pub fn mark_failed(&self, exhaustion_count: u32) {
        if let Err(e) = self
            .tx
            .try_send(LaneCommand::MarkFailed { exhaustion_count })
        {
            tracing::error!(%e, "net: lane command send failed");
        }
    }

    /// Trigger a dormant probe attempt (best-effort, non-blocking).
    pub(crate) fn trigger_dormant_probe(&self) {
        if let Err(e) = self.tx.try_send(LaneCommand::DormantProbe) {
            tracing::error!(%e, "net: dormant probe command send failed");
        }
    }

    /// Query the current lane status.
    pub async fn query_status(&self) -> Result<LaneStatusReport> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let permit = self
            .tx
            .reserve()
            .await
            .map_err(|_| NetError::LaneShutdown)?;
        permit.send(LaneCommand::QueryStatus { reply: reply_tx });
        reply_rx.await.map_err(|_| NetError::LaneShutdown)
    }

    /// Request a graceful shutdown of the actor loop.
    #[allow(dead_code)] // used in tests; part of actor API
    pub async fn shutdown(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        let _ = self.tx.send(LaneCommand::Shutdown).await;
    }
}

// ---------------------------------------------------------------------------
// ActorReconnectContext
// ---------------------------------------------------------------------------

/// Everything needed to drive reconnection from within the actor.
///
/// Cloned into alive-watcher closures so a fresh reconnect can be kicked off
/// whenever the underlying TCP connection drops.
#[derive(Clone)]
pub struct ActorReconnectContext {
    pub lane: Lane,
    pub config: Arc<NetConfig>,
    pub local_host_id: Uuid,
    /// Peer address as a hostname:port or IP:port string.
    /// Stored as a string (not a resolved `SocketAddr`) so DNS is re-resolved on
    /// every reconnect attempt, allowing container restarts with new IPs to
    /// reconnect without requiring a restart on this side.
    pub peer_host: String,
    pub tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    pub handle: LaneHandle,
    pub cancelled: Arc<AtomicBool>,
    pub task_pool: TaskPool,
}

impl ActorReconnectContext {
    /// Spawn a background task that runs `connect_with_retry`.
    ///
    /// On success, delivers `SwapClient` to the actor.
    /// On exhaustion, calls `handle.mark_failed(exhaustion_count)`.
    pub(crate) fn spawn_reconnect(&self, exhaustion_count: u32) {
        let ctx = self.clone();
        self.task_pool.spawn(async move {
            if ctx.cancelled.load(Ordering::Relaxed) {
                return;
            }
            let result = connect_with_retry_cancelable(
                Arc::clone(&ctx.config),
                ctx.local_host_id,
                &ctx.peer_host,
                ctx.lane,
                ctx.tls_connector.clone(),
                Some(Arc::clone(&ctx.cancelled)),
                ctx.task_pool.clone(),
            )
            .await;

            if ctx.cancelled.load(Ordering::Relaxed) {
                return;
            }
            match result {
                Some(client) => {
                    ctx.handle.try_swap_client(client);
                }
                None => {
                    ctx.handle.mark_failed(exhaustion_count);
                }
            }
        });
    }

    /// Schedule a slow-retry probe after sleeping for one jittered
    /// [`slow_retry_interval`].
    ///
    /// The probe is triggered by sending `DormantProbe` to the actor, which
    /// then decides whether to fire a connection attempt.
    pub(crate) fn spawn_dormant_probe(&self) {
        let handle = self.handle.clone();
        let cancelled = Arc::clone(&self.cancelled);
        self.task_pool.spawn(async move {
            tokio::time::sleep(with_jitter(slow_retry_interval())).await;
            if cancelled.load(Ordering::Relaxed) {
                return;
            }
            handle.trigger_dormant_probe();
        });
    }
}

// ---------------------------------------------------------------------------
// Spawn + actor loop
// ---------------------------------------------------------------------------

/// Spawn a lane actor and return a [`LaneHandle`] for interacting with it.
///
/// The `ctx_builder` closure receives the freshly-created [`LaneHandle`] and
/// must return an [`ActorReconnectContext`].  This resolves the circular
/// dependency: the actor needs a handle to itself (via the reconnect context)
/// but the handle is only available after the channel is created.
pub fn spawn_lane_actor(
    lane: Lane,
    initial_state: LaneState,
    ctx_builder: impl FnOnce(LaneHandle) -> ActorReconnectContext,
) -> LaneHandle {
    spawn_lane_actor_with_timeout(lane, initial_state, lane.timeout(), ctx_builder)
}

pub fn spawn_lane_actor_with_timeout(
    lane: Lane,
    initial_state: LaneState,
    default_timeout: Duration,
    ctx_builder: impl FnOnce(LaneHandle) -> ActorReconnectContext,
) -> LaneHandle {
    let (tx, rx) = mpsc::channel(lane_channel_capacity());
    let handle = LaneHandle {
        tx,
        lane,
        default_timeout,
        cancelled: Arc::new(AtomicBool::new(false)),
        _guard: None,
    };
    let ctx = ctx_builder(handle.clone());
    ctx.task_pool
        .spawn(lane_actor_loop(lane, initial_state, rx, ctx.clone(), None));
    handle
}

pub(crate) fn spawn_lane_actor_on_pool_with_timeout(
    lane: Lane,
    initial_state: LaneState,
    default_timeout: Duration,
    task_pool: TaskPool,
    ctx_builder: impl FnOnce(LaneHandle) -> ActorReconnectContext,
) -> LaneHandle {
    let (tx, rx) = mpsc::channel(lane_channel_capacity());
    let handle = LaneHandle {
        tx,
        lane,
        default_timeout,
        cancelled: Arc::new(AtomicBool::new(false)),
        _guard: None,
    };
    let ctx = ctx_builder(handle.clone());
    task_pool.spawn(lane_actor_loop(lane, initial_state, rx, ctx, None));
    handle
}

/// Spawns a lane actor for the Raft lane, **assigned** to the fixed-width
/// [`crate::lane_thread_pool::LaneThreadPool`] rather than given a dedicated OS
/// thread per dial.
///
/// The isolation that motivated the dedicated thread (`0af43df8`: Raft
/// heartbeats must not be starved by data-path saturation) is preserved — the
/// pool threads run nothing but Raft lane actors, so a saturated `data-rt` still
/// cannot delay a heartbeat. What changes is the *shape*: the thread count is
/// now a tunable constant (`FERROSA_RAFT_LANE_POOL_THREADS`, default 2) instead
/// of one thread per dial that leaks whenever a pool is created and dropped
/// before `add_peer`.
///
/// The actor loop logic is identical to [`spawn_lane_actor`]; only the execution
/// context differs.
#[allow(dead_code)]
pub(crate) fn spawn_raft_lane_actor(
    lane: Lane,
    initial_state: LaneState,
    peer_label: String,
    ctx_builder: impl FnOnce(LaneHandle) -> ActorReconnectContext + Send + 'static,
) -> LaneHandle {
    spawn_raft_lane_actor_with_timeout(lane, initial_state, lane.timeout(), peer_label, ctx_builder)
}

pub(crate) fn spawn_raft_lane_actor_with_timeout(
    lane: Lane,
    initial_state: LaneState,
    default_timeout: Duration,
    _peer_label: String,
    ctx_builder: impl FnOnce(LaneHandle) -> ActorReconnectContext + Send + 'static,
) -> LaneHandle {
    let (tx, rx) = mpsc::channel(lane_channel_capacity());
    let cancelled = Arc::new(AtomicBool::new(false));
    let reaper = Arc::new(ThreadReaper::new(Arc::clone(&cancelled)));
    let handle = LaneHandle {
        tx,
        lane,
        default_timeout,
        cancelled,
        _guard: Some(reaper.acquire()),
    };
    // The context gets a handle clone WITHOUT a guard. Its `ctx_builder`
    // typically retains the handle inside `ActorReconnectContext`, and the
    // actor loop owns that context until it exits — so a counting guard here
    // would never drop and the count could never reach zero. Excluding the
    // context clone is what makes "all handles dropped" reachable; the loop
    // only exits on `Shutdown` or channel close, and the reaper is the backstop
    // for the pool that never gets to send either.
    let mut ctx_handle = handle.clone();
    ctx_handle._guard = None;
    let ctx = ctx_builder(ctx_handle);

    // Assign the actor to the pool instead of spawning a thread. A worker hosts
    // several actors concurrently (they are network-wait dominated), so the pool
    // may be narrower than the peer count. The reaper still races the loop so a
    // pool dropped before `add_peer` releases its actor slot promptly.
    crate::lane_thread_pool::lane_thread_pool().assign(async move {
        tokio::select! {
            _ = lane_actor_loop(lane, initial_state, rx, ctx, Some(Arc::clone(&reaper))) => {}
            _ = reaper.wait() => {}
        }
    });

    handle
}

/// The core actor loop.  Owns `LaneState` exclusively — no mutex required.
///
/// Processes commands sequentially from the mpsc receiver until `Shutdown`
/// is received or the channel is closed.
async fn lane_actor_loop(
    lane: Lane,
    mut state: LaneState,
    mut rx: mpsc::Receiver<LaneCommand>,
    ctx: ActorReconnectContext,
    reaper: Option<Arc<ThreadReaper>>,
) {
    let stream_limit = ctx.config.max_streams_per_lane.max(1);
    let pending_stream_capacity = lane_pending_stream_capacity();
    let mut in_flight_streams = 0usize;
    let mut pending_streams = VecDeque::new();
    let mut dispatch_retry_scheduled = false;

    // The node this lane was opened to. Every later reconnect must reach the
    // same node: a different one that took over the address (container IP
    // reuse) is refused rather than silently swapped in.
    let expected_peer = match &state {
        LaneState::Connected(client) => Some(client.peer_host_id()),
        LaneState::Reconnecting { .. } | LaneState::Dormant => None,
    };
    let mut identity_refusal_logged = false;

    // If initial state is Connected, attach an alive watcher immediately.
    if let LaneState::Connected(ref client) = state {
        let alive_rx = client.alive_rx();
        let watcher_ctx = ctx.clone();
        spawn_alive_watcher(
            alive_rx,
            move || {
                watcher_ctx.handle.mark_reconnecting();
                watcher_ctx.spawn_reconnect(0);
            },
            ctx.task_pool.clone(),
        );
    }

    loop {
        // Exit as soon as every external handle is gone. The reaper's flag is
        // *polled*, not awaited: `Notify::notified()` only registers a waiter
        // when it is first polled, and this thread's runtime may not poll it
        // promptly, so a release that lands before the first poll could be
        // missed and the thread stranded — exactly what the reaper exists to
        // prevent. One wake per 50 ms at idle, on the raft lane only.
        let cmd = tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(cmd) => cmd,
                // All senders gone. The raft actor's context holds one, so this
                // is the pooled/plain actors' exit path, unchanged.
                None => break,
            },
            _ = async {
                match &reaper {
                    Some(reaper) => loop {
                        if reaper.is_woken() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    },
                    None => std::future::pending::<()>().await,
                }
            } => break,
        };
        match cmd {
            LaneCommand::Send {
                msg,
                timeout,
                reply,
            } => {
                if pending_streams.len() >= pending_stream_capacity {
                    if lane == Lane::Data {
                        metrics::record_data_lane_rejected();
                    }
                    let _ = reply.send(Err(NetError::Overloaded));
                    continue;
                }
                push_pending_stream(
                    lane,
                    &mut pending_streams,
                    PendingLaneCommand::Send {
                        msg,
                        timeout,
                        reply,
                        queued_at: Instant::now(),
                    },
                );
                dispatch_retry_scheduled = dispatch_stream_window(
                    &state,
                    lane,
                    &ctx,
                    stream_limit,
                    &mut in_flight_streams,
                    &mut pending_streams,
                    dispatch_retry_scheduled,
                );
            }
            LaneCommand::Fire {
                msg,
                timeout,
                reply,
            } => {
                if pending_streams.len() >= pending_stream_capacity {
                    if lane == Lane::Data {
                        metrics::record_data_lane_rejected();
                    }
                    let _ = reply.send(Err(NetError::Overloaded));
                    continue;
                }
                push_pending_stream(
                    lane,
                    &mut pending_streams,
                    PendingLaneCommand::Fire {
                        msg,
                        timeout,
                        reply,
                        queued_at: Instant::now(),
                    },
                );
                dispatch_retry_scheduled = dispatch_stream_window(
                    &state,
                    lane,
                    &ctx,
                    stream_limit,
                    &mut in_flight_streams,
                    &mut pending_streams,
                    dispatch_retry_scheduled,
                );
            }
            LaneCommand::SendComplete { result, reply } => {
                in_flight_streams = in_flight_streams.saturating_sub(1);
                let _ = reply.send(result);
                dispatch_retry_scheduled = dispatch_stream_window(
                    &state,
                    lane,
                    &ctx,
                    stream_limit,
                    &mut in_flight_streams,
                    &mut pending_streams,
                    dispatch_retry_scheduled,
                );
            }
            LaneCommand::FireComplete { result, reply } => {
                in_flight_streams = in_flight_streams.saturating_sub(1);
                let _ = reply.send(result);
                dispatch_retry_scheduled = dispatch_stream_window(
                    &state,
                    lane,
                    &ctx,
                    stream_limit,
                    &mut in_flight_streams,
                    &mut pending_streams,
                    dispatch_retry_scheduled,
                );
            }
            LaneCommand::DispatchPending => {
                dispatch_retry_scheduled = false;
                dispatch_retry_scheduled = dispatch_stream_window(
                    &state,
                    lane,
                    &ctx,
                    stream_limit,
                    &mut in_flight_streams,
                    &mut pending_streams,
                    dispatch_retry_scheduled,
                );
            }
            LaneCommand::SwapClient(new_client) => {
                if let Some(expected) = expected_peer {
                    let answered_by = new_client.peer_host_id();
                    if answered_by != expected {
                        // One line per refusal episode, not per attempt: the
                        // retry machinery keeps dialing until the right node
                        // answers.
                        if !identity_refusal_logged {
                            identity_refusal_logged = true;
                            tracing::error!(
                                ?lane,
                                peer = %ctx.peer_host,
                                %expected,
                                %answered_by,
                                "reconnect refused: peer identity mismatch; the address is now \
                                 answered by a different node. Retrying until the expected node returns"
                            );
                        }
                        drop(new_client);
                        match &state {
                            LaneState::Dormant => ctx.spawn_dormant_probe(),
                            LaneState::Reconnecting {
                                exhaustion_count, ..
                            } => ctx.handle.mark_failed(*exhaustion_count),
                            LaneState::Connected(_) => {}
                        }
                        continue;
                    }
                }
                identity_refusal_logged = false;
                if matches!(state, LaneState::Dormant) {
                    dec_dormant_peer_count();
                    tracing::info!(
                        ?lane,
                        peer = %ctx.peer_host,
                        "lane reconnected after slow-retry"
                    );
                } else {
                    tracing::info!(?lane, peer = %ctx.peer_host, "lane reconnected");
                }
                let alive_rx = new_client.alive_rx();
                state = LaneState::Connected(new_client);
                let watcher_ctx = ctx.clone();
                spawn_alive_watcher(
                    alive_rx,
                    move || {
                        watcher_ctx.handle.mark_reconnecting();
                        watcher_ctx.spawn_reconnect(0);
                    },
                    ctx.task_pool.clone(),
                );
                dispatch_retry_scheduled = dispatch_stream_window(
                    &state,
                    lane,
                    &ctx,
                    stream_limit,
                    &mut in_flight_streams,
                    &mut pending_streams,
                    dispatch_retry_scheduled,
                );
            }
            LaneCommand::ConnectionLost => {
                if matches!(state, LaneState::Connected(_)) {
                    tracing::warn!(
                        ?lane,
                        peer = %ctx.peer_host,
                        "lane connection lost; entering reconnecting state"
                    );
                    state = LaneState::Reconnecting {
                        attempt: 0,
                        exhaustion_count: 0,
                    };
                    fail_pending_streams_reconnecting(&mut pending_streams);
                }
            }
            LaneCommand::MarkFailed { exhaustion_count } => {
                let current_exhaustion = match &state {
                    LaneState::Reconnecting {
                        exhaustion_count: ec,
                        ..
                    } => *ec,
                    // If the lane is already Connected or Dormant, this is a
                    // stale signal from a reconnect task that raced.
                    LaneState::Connected(_) | LaneState::Dormant => {
                        tracing::debug!(?lane, "ignoring MarkFailed: lane is not Reconnecting");
                        continue;
                    }
                };

                // Ignore stale signals from earlier exhaustion cycles.
                if exhaustion_count < current_exhaustion {
                    tracing::debug!(
                        ?lane,
                        signal_exhaustion = exhaustion_count,
                        current_exhaustion,
                        "ignoring stale MarkFailed signal"
                    );
                    continue;
                }

                let next_exhaustion = current_exhaustion + 1;

                if next_exhaustion >= DORMANT_AFTER_EXHAUSTIONS {
                    tracing::warn!(
                        ?lane,
                        peer = %ctx.peer_host,
                        exhaustion_count = next_exhaustion,
                        probe_interval = ?slow_retry_interval(),
                        "lane entering slow-retry: fast reconnect cycles exhausted; probing \
                         until the peer returns"
                    );
                    state = LaneState::Dormant;
                    inc_dormant_peer_count();
                    ctx.spawn_dormant_probe();
                } else {
                    tracing::warn!(
                        ?lane,
                        peer = %ctx.peer_host,
                        exhaustion_count = next_exhaustion,
                        remaining_before_dormant = DORMANT_AFTER_EXHAUSTIONS - next_exhaustion,
                        "lane reconnection exhausted, scheduling retry cycle"
                    );
                    state = LaneState::Reconnecting {
                        attempt: 0,
                        exhaustion_count: next_exhaustion,
                    };
                    let retry_ctx = ctx.clone();
                    ctx.task_pool.spawn(async move {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        retry_ctx.spawn_reconnect(next_exhaustion);
                    });
                }
            }
            LaneCommand::DormantProbe => {
                // Only act if still dormant; discard if we've already recovered.
                if !matches!(state, LaneState::Dormant) {
                    tracing::debug!(?lane, "ignoring DormantProbe: lane not dormant");
                    continue;
                }
                tracing::debug!(?lane, peer = %ctx.peer_host, "dormant probe firing");
                let probe_ctx = ctx.clone();
                ctx.task_pool.spawn(async move {
                    if probe_ctx.cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    let result = connect_once_cancelable(
                        Arc::clone(&probe_ctx.config),
                        probe_ctx.local_host_id,
                        &probe_ctx.peer_host,
                        probe_ctx.lane,
                        probe_ctx.tls_connector.clone(),
                        Some(Arc::clone(&probe_ctx.cancelled)),
                        probe_ctx.task_pool.clone(),
                    )
                    .await;
                    if probe_ctx.cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    match result {
                        Some(client) => {
                            probe_ctx.handle.try_swap_client(client);
                        }
                        None => {
                            // Probe failed; schedule the next one.
                            probe_ctx.spawn_dormant_probe();
                        }
                    }
                });
            }
            LaneCommand::QueryStatus { reply } => {
                let report = match &state {
                    LaneState::Connected(_) => LaneStatusReport::Connected,
                    LaneState::Reconnecting { .. } => LaneStatusReport::Reconnecting,
                    LaneState::Dormant => LaneStatusReport::Dormant,
                };
                let _ = reply.send(report);
            }
            LaneCommand::Shutdown => {
                if matches!(state, LaneState::Dormant) {
                    dec_dormant_peer_count();
                }
                tracing::info!(?lane, "lane actor: shutting down");
                break;
            }
        }
    }

    // Channel closed without explicit Shutdown — clean up dormant count.
    if matches!(state, LaneState::Dormant) {
        dec_dormant_peer_count();
    }
}

/// Dispatch queued lane commands while the per-peer/per-lane stream window has
/// capacity. The actor owns the window state, so this does not add a semaphore
/// or shared mutex on the hot path.
fn dispatch_stream_window(
    state: &LaneState,
    lane: Lane,
    ctx: &ActorReconnectContext,
    stream_limit: usize,
    in_flight_streams: &mut usize,
    pending_streams: &mut VecDeque<PendingLaneCommand>,
    retry_already_scheduled: bool,
) -> bool {
    let client = match state {
        LaneState::Connected(c) => c.clone(),
        LaneState::Reconnecting { .. } | LaneState::Dormant => {
            fail_pending_streams_reconnecting(pending_streams);
            metrics::observe_lane_pending_streams(lane, pending_streams.len());
            return false;
        }
    };
    let mut retry_scheduled = false;

    while *in_flight_streams < stream_limit {
        let Some(pending) = pending_streams.pop_front() else {
            break;
        };

        match pending {
            PendingLaneCommand::Send {
                msg,
                timeout,
                reply,
                queued_at,
            } => {
                metrics::record_lane_stream_wait(lane, queued_at.elapsed(), stream_limit);
                if !metrics::try_start_rpc(lane, ctx.config.data_lane_max_in_flight) {
                    pending_streams.push_front(PendingLaneCommand::Send {
                        msg,
                        timeout,
                        reply,
                        queued_at,
                    });
                    metrics::observe_lane_pending_streams(lane, pending_streams.len());
                    if !retry_already_scheduled {
                        schedule_dispatch_retry(ctx);
                    }
                    retry_scheduled = true;
                    break;
                }

                *in_flight_streams += 1;
                let client = client.clone();
                let complete_tx = ctx.handle.tx.clone();
                ctx.task_pool.spawn(async move {
                    let result = client.send_with_timeout(msg, lane, timeout).await;
                    if matches!(result, Err(NetError::Timeout(_))) {
                        metrics::record_rpc_timeout(lane);
                    }
                    metrics::finish_rpc(lane);
                    let _ = complete_tx
                        .send(LaneCommand::SendComplete { result, reply })
                        .await;
                });
            }
            PendingLaneCommand::Fire {
                msg,
                timeout,
                reply,
                queued_at,
            } => {
                metrics::record_lane_stream_wait(lane, queued_at.elapsed(), stream_limit);
                if !metrics::try_start_rpc(lane, ctx.config.data_lane_max_in_flight) {
                    pending_streams.push_front(PendingLaneCommand::Fire {
                        msg,
                        timeout,
                        reply,
                        queued_at,
                    });
                    metrics::observe_lane_pending_streams(lane, pending_streams.len());
                    if !retry_already_scheduled {
                        schedule_dispatch_retry(ctx);
                    }
                    retry_scheduled = true;
                    break;
                }

                *in_flight_streams += 1;
                let client = client.clone();
                let complete_tx = ctx.handle.tx.clone();
                ctx.task_pool.spawn(async move {
                    let result = match tokio::time::timeout(timeout, client.fire(msg, lane)).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(_elapsed) => {
                            Err(NetError::Timeout(format!("{lane:?} lane fire timeout")))
                        }
                    };
                    if matches!(result, Err(NetError::Timeout(_))) {
                        metrics::record_rpc_timeout(lane);
                    }
                    metrics::finish_rpc(lane);
                    let _ = complete_tx
                        .send(LaneCommand::FireComplete { result, reply })
                        .await;
                });
            }
        }
    }
    metrics::observe_lane_pending_streams(lane, pending_streams.len());
    retry_scheduled
}

fn push_pending_stream(
    lane: Lane,
    pending_streams: &mut VecDeque<PendingLaneCommand>,
    pending: PendingLaneCommand,
) {
    pending_streams.push_back(pending);
    metrics::observe_lane_pending_streams(lane, pending_streams.len());
}

fn schedule_dispatch_retry(ctx: &ActorReconnectContext) {
    let tx = ctx.handle.tx.clone();
    ctx.task_pool.spawn(async move {
        tokio::time::sleep(DATA_LANE_CAP_RETRY_DELAY).await;
        let _ = tx.send(LaneCommand::DispatchPending).await;
    });
}

fn fail_pending_streams_reconnecting(pending_streams: &mut VecDeque<PendingLaneCommand>) {
    while let Some(pending) = pending_streams.pop_front() {
        match pending {
            PendingLaneCommand::Send { reply, .. } => {
                let _ = reply.send(Err(NetError::Reconnecting));
            }
            PendingLaneCommand::Fire { reply, .. } => {
                let _ = reply.send(Err(NetError::Reconnecting));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn lane_handle_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<LaneHandle>();
    }

    /// The reaper must fire only when the *last* registered handle drops.
    ///
    /// A `LaneHandle` is cloned freely (into `PeerState`, in-flight commands,
    /// temporary borrows). If an intermediate drop woke the thread, a healthy
    /// raft lane would die the moment any caller released a clone.
    #[test]
    fn reaper_wakes_only_when_the_last_registered_handle_drops() {
        let reaper = Arc::new(ThreadReaper::new(Arc::new(AtomicBool::new(false))));

        let first = reaper.acquire();
        let second = reaper.acquire();

        drop(first);
        assert!(
            !reaper.is_woken(),
            "an outstanding handle must keep the actor thread alive"
        );

        drop(second);
        assert!(
            reaper.is_woken(),
            "dropping the last handle must ask the actor thread to exit"
        );
    }

    /// Dropping the last handle must actually tear the actor thread down — not
    /// merely flip a flag.
    ///
    /// Portable: it observes the reaper's own `Arc` (the thread holds one clone
    /// for its whole life, the spawn's `select!` holds another). When the thread
    /// stops, its clone is released and the count falls to exactly what the
    /// spawn scope still holds. `/proc/self/task` is the tempting instrument but
    /// is Linux-only and cannot run in the macOS dev loop, which is how this test
    /// went un-compiled until Linux CI caught the missing `handle` field.
    #[tokio::test]
    async fn dropping_the_last_handle_reaps_the_raft_lane_thread() {
        let handle = spawn_raft_lane_actor_with_timeout(
            Lane::Raft,
            LaneState::Reconnecting {
                attempt: 0,
                exhaustion_count: 0,
            },
            Duration::from_secs(30),
            "reapx".to_owned(),
            |h| ActorReconnectContext {
                lane: Lane::Raft,
                config: Arc::new(NetConfig::default()),
                local_host_id: Uuid::new_v4(),
                peer_host: "127.0.0.1:1".to_owned(),
                tls_connector: None,
                cancelled: h.cancel_token(),
                // The context holds a handle clone WITHOUT a counting guard, so
                // it does not keep the actor alive; dropping the returned
                // `handle` below is therefore the last release.
                handle: h,
                task_pool: TaskPool::current("reap-test"),
            },
        );

        let weak = Arc::downgrade(&handle.reaper().expect("raft actor carries a reaper"));
        assert!(
            weak.upgrade().is_some(),
            "the raft actor's reaper must be reachable before the drop"
        );

        drop(handle);

        // The actor's poll interval is 50 ms; give it a generous number of them.
        for _ in 0..100 {
            if weak.upgrade().is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "the raft actor thread was not reaped after the last handle dropped: \
             its reaper is still alive"
        );
    }

    #[test]
    fn lane_status_report_variants() {
        assert_eq!(LaneStatusReport::Connected, LaneStatusReport::Connected);
        assert_eq!(
            LaneStatusReport::Reconnecting,
            LaneStatusReport::Reconnecting
        );
        assert_eq!(LaneStatusReport::Dormant, LaneStatusReport::Dormant);
        assert_ne!(LaneStatusReport::Connected, LaneStatusReport::Dormant);
    }

    #[test]
    fn actor_reconnect_context_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ActorReconnectContext>();
    }

    #[tokio::test]
    async fn reconnecting_lane_returns_reconnecting_error() {
        let handle = spawn_lane_actor(
            Lane::Data,
            LaneState::Reconnecting {
                attempt: 1,
                exhaustion_count: 0,
            },
            |h| ActorReconnectContext {
                lane: Lane::Data,
                config: Arc::new(NetConfig::default()),
                local_host_id: Uuid::new_v4(),
                peer_host: "127.0.0.1:9999".to_owned(),
                tls_connector: None,
                cancelled: h.cancel_token(),
                handle: h,
                task_pool: TaskPool::current("test-lane"),
            },
        );

        assert_eq!(handle.lane(), Lane::Data);

        let result = handle
            .send(
                Message::Ping {
                    nonce: 1,
                    sent_at: 0,
                },
                None,
            )
            .await;
        assert!(
            matches!(result, Err(NetError::Reconnecting)),
            "expected Reconnecting, got {result:?}"
        );

        let status = handle.query_status().await.unwrap();
        assert_eq!(status, LaneStatusReport::Reconnecting);

        handle.shutdown().await;
    }

    // Drives a lane to `Dormant`, which mutates the process-wide
    // `DORMANT_PEER_COUNT` metric. Share the `dormant_peer_counter` serial key
    // with the counter assertions in `reconnect.rs` so this never increments the
    // global concurrently with their absolute-value checks.
    #[tokio::test]
    #[serial(dormant_peer_counter)]
    async fn mark_failed_transitions_through_exhaustion_to_dormant() {
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
                peer_host: "127.0.0.1:9999".to_owned(),
                tls_connector: None,
                cancelled: h.cancel_token(),
                handle: h,
                task_pool: TaskPool::current("test-lane"),
            },
        );

        let status = handle.query_status().await.unwrap();
        assert_eq!(status, LaneStatusReport::Reconnecting);

        // Drive to dormant by sending MarkFailed DORMANT_AFTER_EXHAUSTIONS times.
        for i in 0..DORMANT_AFTER_EXHAUSTIONS {
            handle.mark_failed(i);
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
        }

        let status = handle.query_status().await.unwrap();
        assert_eq!(
            status,
            LaneStatusReport::Dormant,
            "should be Dormant after {DORMANT_AFTER_EXHAUSTIONS} exhaustions"
        );

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_stops_actor() {
        let handle = spawn_lane_actor(
            Lane::Raft,
            LaneState::Reconnecting {
                attempt: 0,
                exhaustion_count: 0,
            },
            |h| ActorReconnectContext {
                lane: Lane::Raft,
                config: Arc::new(NetConfig::default()),
                local_host_id: Uuid::new_v4(),
                peer_host: "127.0.0.1:9999".to_owned(),
                tls_connector: None,
                cancelled: h.cancel_token(),
                handle: h,
                task_pool: TaskPool::current("test-lane"),
            },
        );

        handle.shutdown().await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let result = handle
            .send(
                Message::Ping {
                    nonce: 1,
                    sent_at: 0,
                },
                None,
            )
            .await;
        assert!(
            matches!(result, Err(NetError::LaneShutdown)),
            "expected LaneShutdown after shutdown, got {result:?}"
        );
    }

    #[tokio::test]
    async fn stale_mark_failed_ignored() {
        let handle = spawn_lane_actor(
            Lane::Raft,
            LaneState::Reconnecting {
                attempt: 0,
                exhaustion_count: 1,
            },
            |h| ActorReconnectContext {
                lane: Lane::Raft,
                config: Arc::new(NetConfig::default()),
                local_host_id: Uuid::new_v4(),
                peer_host: "127.0.0.1:9999".to_owned(),
                tls_connector: None,
                cancelled: h.cancel_token(),
                handle: h,
                task_pool: TaskPool::current("test-lane"),
            },
        );

        // Send a stale signal (exhaustion_count=0, current=1).
        handle.mark_failed(0);
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }

        // Lane should still be Reconnecting.
        let status = handle.query_status().await.unwrap();
        assert_eq!(
            status,
            LaneStatusReport::Reconnecting,
            "stale MarkFailed should be ignored"
        );

        handle.shutdown().await;
    }
}
