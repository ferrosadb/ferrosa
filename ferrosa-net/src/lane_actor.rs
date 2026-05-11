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
//! ```text
//! Connected ──(disconnect)──► Reconnecting(exhaustion_count=0)
//!                                    │
//!                    MAX_RECONNECT_ATTEMPTS reached
//!                                    │
//!                                    ▼
//!                       exhaustion_count+1 < DORMANT_AFTER_EXHAUSTIONS?
//!                              yes │                  no │
//!                                  ▼                     ▼
//!                            Reconnecting              Dormant
//!                         (exhaustion_count+1)    probe every DORMANT_PROBE_INTERVAL
//!                                                       │ success
//!                                                       ▼
//!                                                   Connected
//! ```

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::{NetError, Result};
use crate::message::Message;
use crate::reconnect::{
    connect_with_retry, dec_dormant_peer_count, inc_dormant_peer_count, spawn_alive_watcher,
    LaneState, DORMANT_AFTER_EXHAUSTIONS, DORMANT_PROBE_INTERVAL,
};
use crate::rpc::client::RpcClient;

/// Channel capacity for lane actor commands.
const LANE_CHANNEL_CAPACITY: usize = 256;

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
    /// Replace the current RPC client (used after successful reconnect).
    SwapClient(RpcClient),
    /// Signal that one full `connect_with_retry` cycle was exhausted.
    ///
    /// Carries the `exhaustion_count` value at the time of spawning so the
    /// actor can detect stale signals from earlier reconnect tasks that raced
    /// with a successful connection.
    MarkFailed { exhaustion_count: u32 },
    /// Trigger a dormant probe attempt.  Sent by the dormant wake-up task
    /// after sleeping for one [`DORMANT_PROBE_INTERVAL`].
    DormantProbe,
    /// Query the current lane status.
    QueryStatus {
        reply: oneshot::Sender<LaneStatusReport>,
    },
    /// Gracefully shut down the actor loop.
    #[allow(dead_code)] // used in tests; part of actor API
    Shutdown,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Snapshot of a lane's current state, returned by [`LaneHandle::query_status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneStatusReport {
    Connected,
    Reconnecting,
    /// The lane is dormant: all reconnect cycles were exhausted.
    /// Probes the peer at most once every [`DORMANT_PROBE_INTERVAL`].
    Dormant,
    /// Kept for legacy callers; the actor never transitions to this in normal
    /// operation — use `Dormant` instead.
    Failed,
}

// ---------------------------------------------------------------------------
// LaneHandle
// ---------------------------------------------------------------------------

/// Cancel-safe handle for sending commands to a lane actor.
///
/// `Clone` is cheap (just an `mpsc::Sender` clone + a `Lane` copy).
/// All methods use `reserve().await` + `permit.send()` for cancel safety:
/// if a caller is cancelled between reserving a slot and sending the command,
/// the permit is simply dropped — no half-sent state.
#[derive(Clone)]
pub struct LaneHandle {
    tx: mpsc::Sender<LaneCommand>,
    lane: Lane,
}

impl LaneHandle {
    /// Which lane this handle targets.
    #[allow(dead_code)] // used in tests; part of actor API
    pub fn lane(&self) -> Lane {
        self.lane
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
        let timeout = timeout_override.unwrap_or_else(|| self.lane.timeout());
        let (reply_tx, reply_rx) = oneshot::channel();

        // Reserve a slot in the channel — cancel-safe because dropping the
        // permit before calling `permit.send()` simply releases the slot.
        let permit = self.tx.reserve().await.map_err(|_| NetError::LaneFailed)?;
        permit.send(LaneCommand::Send {
            msg,
            timeout,
            reply: reply_tx,
        });

        reply_rx.await.map_err(|_| NetError::LaneFailed)?
    }

    /// Fire-and-forget a message through the lane actor.
    ///
    /// Uses `reserve().await` + `permit.send()` for cancel safety.
    pub(crate) async fn fire(
        &self,
        msg: Message,
        timeout_override: Option<Duration>,
    ) -> Result<()> {
        let timeout = timeout_override.unwrap_or_else(|| self.lane.timeout());
        let (reply_tx, reply_rx) = oneshot::channel();

        let permit = self.tx.reserve().await.map_err(|_| NetError::LaneFailed)?;
        permit.send(LaneCommand::Fire {
            msg,
            timeout,
            reply: reply_tx,
        });

        reply_rx.await.map_err(|_| NetError::LaneFailed)?
    }

    /// Attempt to swap in a new RPC client (best-effort, non-blocking).
    pub(crate) fn try_swap_client(&self, client: RpcClient) {
        if let Err(e) = self.tx.try_send(LaneCommand::SwapClient(client)) {
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
        let permit = self.tx.reserve().await.map_err(|_| NetError::LaneFailed)?;
        permit.send(LaneCommand::QueryStatus { reply: reply_tx });
        reply_rx.await.map_err(|_| NetError::LaneFailed)
    }

    /// Request a graceful shutdown of the actor loop.
    #[allow(dead_code)] // used in tests; part of actor API
    pub async fn shutdown(&self) {
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
}

impl ActorReconnectContext {
    /// Spawn a background task that runs `connect_with_retry`.
    ///
    /// On success, delivers `SwapClient` to the actor.
    /// On exhaustion, calls `handle.mark_failed(exhaustion_count)`.
    pub(crate) fn spawn_reconnect(&self, exhaustion_count: u32) {
        let ctx = self.clone();
        tokio::spawn(async move {
            let result = connect_with_retry(
                Arc::clone(&ctx.config),
                ctx.local_host_id,
                &ctx.peer_host,
                ctx.lane,
                ctx.tls_connector.clone(),
            )
            .await;

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

    /// Schedule a dormant probe after sleeping for [`DORMANT_PROBE_INTERVAL`].
    ///
    /// The probe is triggered by sending `DormantProbe` to the actor, which
    /// then decides whether to fire a connection attempt.
    pub(crate) fn spawn_dormant_probe(&self) {
        let handle = self.handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(DORMANT_PROBE_INTERVAL).await;
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
    let (tx, rx) = mpsc::channel(LANE_CHANNEL_CAPACITY);
    let handle = LaneHandle { tx, lane };
    let ctx = ctx_builder(handle.clone());
    tokio::spawn(lane_actor_loop(lane, initial_state, rx, ctx));
    handle
}

/// Spawns a lane actor on a **dedicated OS thread** with its own single-threaded
/// tokio runtime. Used for the Raft lane to guarantee heartbeat processing cannot
/// be starved by data-path saturation on the shared runtime.
///
/// The actor loop logic is identical to [`spawn_lane_actor`]; only the execution
/// context differs.
pub(crate) fn spawn_raft_lane_actor(
    lane: Lane,
    initial_state: LaneState,
    peer_label: String,
    ctx_builder: impl FnOnce(LaneHandle) -> ActorReconnectContext + Send + 'static,
) -> LaneHandle {
    let (tx, rx) = mpsc::channel(LANE_CHANNEL_CAPACITY);
    let handle = LaneHandle { tx, lane };
    let ctx = ctx_builder(handle.clone());

    std::thread::Builder::new()
        .name(format!("raft-lane-{peer_label}"))
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("raft lane runtime");

            rt.block_on(lane_actor_loop(lane, initial_state, rx, ctx));
        })
        .expect("spawn raft lane thread");

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
) {
    // If initial state is Connected, attach an alive watcher immediately.
    if let LaneState::Connected(ref client) = state {
        let alive_rx = client.alive_rx();
        let watcher_ctx = ctx.clone();
        spawn_alive_watcher(alive_rx, move || {
            watcher_ctx.spawn_reconnect(0);
        });
    }

    while let Some(cmd) = rx.recv().await {
        match cmd {
            LaneCommand::Send {
                msg,
                timeout,
                reply,
            } => {
                // Dispatch the RPC on a spawned task so the actor loop
                // can immediately process the next command. Without this,
                // every Send was awaited inline and the actor handled
                // one in-flight RPC per peer-lane at a time — turning
                // the underlying multiplexed transport (per-stream IDs
                // + DashMap of pending responses inside RpcClient) into
                // a single-pipelined channel. On a 32-thread NoSQLBench
                // workload this capped throughput at ~1 / RTT per peer.
                //
                // Connection failures are detected by the alive_watcher
                // attached on Connected and propagated via SwapClient /
                // MarkFailed — no need to mutate state from the spawned
                // task.
                dispatch_send(&state, lane, msg, timeout, reply);
            }
            LaneCommand::Fire {
                msg,
                timeout,
                reply,
            } => {
                dispatch_fire(&state, lane, msg, timeout, reply);
            }
            LaneCommand::SwapClient(new_client) => {
                // If we were dormant, decrement the dormant counter.
                if matches!(state, LaneState::Dormant) {
                    dec_dormant_peer_count();
                    tracing::info!(
                        ?lane,
                        peer = %ctx.peer_host,
                        "lane woke from dormant: reconnected"
                    );
                } else {
                    tracing::info!(?lane, peer = %ctx.peer_host, "lane actor: swapping in new client");
                }
                let alive_rx = new_client.alive_rx();
                state = LaneState::Connected(new_client);
                let watcher_ctx = ctx.clone();
                spawn_alive_watcher(alive_rx, move || {
                    watcher_ctx.spawn_reconnect(0);
                });
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
                    tracing::info!(
                        ?lane,
                        peer = %ctx.peer_host,
                        exhaustion_count = next_exhaustion,
                        probe_interval = ?DORMANT_PROBE_INTERVAL,
                        "lane entering dormant state"
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
                    tokio::spawn(async move {
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
                tokio::spawn(async move {
                    let result = connect_with_retry(
                        Arc::clone(&probe_ctx.config),
                        probe_ctx.local_host_id,
                        &probe_ctx.peer_host,
                        probe_ctx.lane,
                        probe_ctx.tls_connector.clone(),
                    )
                    .await;
                    match result {
                        Some(client) => {
                            probe_ctx.handle.try_swap_client(client);
                        }
                        None => {
                            // Probe exhausted; schedule the next probe.
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

/// Dispatch a Send command non-blocking: clone the client and spawn the
/// RPC + reply on a task so the actor loop returns immediately.
///
/// Connection-error reconnect is handled by the alive_watcher attached on
/// `LaneState::Connected`, not by mutating state from the spawned task.
fn dispatch_send(
    state: &LaneState,
    lane: Lane,
    msg: Message,
    timeout: Duration,
    reply: oneshot::Sender<Result<Message>>,
) {
    let client = match state {
        LaneState::Connected(c) => c.clone(),
        LaneState::Reconnecting { .. } | LaneState::Dormant => {
            let _ = reply.send(Err(NetError::Reconnecting));
            return;
        }
    };
    tokio::spawn(async move {
        let result = match tokio::time::timeout(timeout, client.send(msg, lane)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e),
            Err(_elapsed) => Err(NetError::Timeout(format!("{lane:?} lane send timeout"))),
        };
        let _ = reply.send(result);
    });
}

/// Dispatch a Fire command non-blocking. See `dispatch_send` for rationale.
fn dispatch_fire(
    state: &LaneState,
    lane: Lane,
    msg: Message,
    timeout: Duration,
    reply: oneshot::Sender<Result<()>>,
) {
    let client = match state {
        LaneState::Connected(c) => c.clone(),
        LaneState::Reconnecting { .. } | LaneState::Dormant => {
            let _ = reply.send(Err(NetError::Reconnecting));
            return;
        }
    };
    tokio::spawn(async move {
        let result = match tokio::time::timeout(timeout, client.fire(msg, lane)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_elapsed) => Err(NetError::Timeout(format!("{lane:?} lane fire timeout"))),
        };
        let _ = reply.send(result);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_handle_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<LaneHandle>();
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
                handle: h,
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

    #[tokio::test]
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
                handle: h,
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
                handle: h,
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
            matches!(result, Err(NetError::LaneFailed)),
            "expected LaneFailed after shutdown, got {result:?}"
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
                handle: h,
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
