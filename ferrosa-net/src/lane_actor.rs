//! Actor-based lane management for cancel-safe internode RPC.
//!
//! Each network lane (Raft, Data, Bulk) is owned by a single spawned task that
//! processes [`LaneCommand`]s sequentially via an mpsc channel.  Callers
//! interact through [`LaneHandle`], a thin Clone wrapper around the sender.
//!
//! This design eliminates the cancel-safety hazard of holding a `tokio::Mutex`
//! across `await` points (network round-trips).  The actor exclusively owns
//! [`LaneState`], so no mutex is needed.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::{NetError, Result};
use crate::message::Message;
use crate::reconnect::{connect_with_retry, spawn_alive_watcher, ExponentialBackoff, LaneState};
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
    /// Mark the lane as permanently failed (reconnect exhausted).
    MarkFailed,
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
pub(crate) enum LaneStatusReport {
    Connected,
    Reconnecting,
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
pub(crate) struct LaneHandle {
    tx: mpsc::Sender<LaneCommand>,
    lane: Lane,
}

impl LaneHandle {
    /// Which lane this handle targets.
    #[allow(dead_code)] // used in tests; part of actor API
    pub(crate) fn lane(&self) -> Lane {
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
            eprintln!("[net] lane command send failed: {e}");
        }
    }

    /// Mark the lane as permanently failed.
    pub(crate) fn mark_failed(&self) {
        if let Err(e) = self.tx.try_send(LaneCommand::MarkFailed) {
            eprintln!("[net] lane command send failed: {e}");
        }
    }

    /// Query the current lane status.
    pub(crate) async fn query_status(&self) -> Result<LaneStatusReport> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let permit = self.tx.reserve().await.map_err(|_| NetError::LaneFailed)?;
        permit.send(LaneCommand::QueryStatus { reply: reply_tx });
        reply_rx.await.map_err(|_| NetError::LaneFailed)
    }

    /// Request a graceful shutdown of the actor loop.
    #[allow(dead_code)] // used in tests; part of actor API
    pub(crate) async fn shutdown(&self) {
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
pub(crate) struct ActorReconnectContext {
    pub(crate) lane: Lane,
    pub(crate) config: Arc<NetConfig>,
    pub(crate) local_host_id: Uuid,
    /// Peer address as a hostname:port or IP:port string.
    /// Stored as a string (not a resolved `SocketAddr`) so DNS is re-resolved on
    /// every reconnect attempt, allowing container restarts with new IPs to
    /// reconnect without requiring a restart on this side.
    pub(crate) peer_host: String,
    pub(crate) tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    pub(crate) handle: LaneHandle,
}

impl ActorReconnectContext {
    /// Spawn a background task that runs `connect_with_retry`.
    ///
    /// On success, delivers a `SwapClient` command to the actor.
    /// On exhaustion, calls `handle.mark_failed()`.
    pub(crate) fn spawn_reconnect(&self) {
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
                    ctx.handle.mark_failed();
                }
            }
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
pub(crate) fn spawn_lane_actor(
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
            watcher_ctx.spawn_reconnect();
        });
    }

    while let Some(cmd) = rx.recv().await {
        match cmd {
            LaneCommand::Send {
                msg,
                timeout,
                reply,
            } => {
                let result = handle_send(&mut state, &ctx, lane, msg, timeout).await;
                let _ = reply.send(result);
            }
            LaneCommand::Fire {
                msg,
                timeout,
                reply,
            } => {
                let result = handle_fire(&mut state, &ctx, lane, msg, timeout).await;
                let _ = reply.send(result);
            }
            LaneCommand::SwapClient(new_client) => {
                tracing::info!(?lane, "lane actor: swapping in new client");
                let alive_rx = new_client.alive_rx();
                state = LaneState::Connected(new_client);
                let watcher_ctx = ctx.clone();
                spawn_alive_watcher(alive_rx, move || {
                    watcher_ctx.spawn_reconnect();
                });
            }
            LaneCommand::MarkFailed => {
                tracing::error!(?lane, "lane actor: marking lane as failed");
                state = LaneState::Failed;
            }
            LaneCommand::QueryStatus { reply } => {
                let report = match &state {
                    LaneState::Connected(_) => LaneStatusReport::Connected,
                    LaneState::Reconnecting { .. } => LaneStatusReport::Reconnecting,
                    LaneState::Failed => LaneStatusReport::Failed,
                };
                let _ = reply.send(report);
            }
            LaneCommand::Shutdown => {
                tracing::info!(?lane, "lane actor: shutting down");
                break;
            }
        }
    }
}

/// Handle a Send command: perform the RPC with timeout, trigger reconnect on error.
async fn handle_send(
    state: &mut LaneState,
    ctx: &ActorReconnectContext,
    lane: Lane,
    msg: Message,
    timeout: Duration,
) -> Result<Message> {
    match state {
        LaneState::Connected(client) => {
            let result = tokio::time::timeout(timeout, client.send(msg, lane)).await;
            match result {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(e)) => {
                    if matches!(&e, NetError::Io(_) | NetError::Protocol(_)) {
                        tracing::warn!(?lane, error = %e, "connection error, triggering reconnect");
                        *state = LaneState::Reconnecting {
                            attempt: 0,
                            backoff: ExponentialBackoff::new(
                                Duration::from_millis(500),
                                Duration::from_secs(30),
                            ),
                        };
                        ctx.spawn_reconnect();
                    }
                    Err(e)
                }
                Err(_elapsed) => Err(NetError::Timeout(format!("{lane:?} lane send timeout"))),
            }
        }
        LaneState::Reconnecting { .. } => Err(NetError::Reconnecting),
        LaneState::Failed => Err(NetError::LaneFailed),
    }
}

/// Handle a Fire command: send fire-and-forget with timeout, trigger reconnect on error.
async fn handle_fire(
    state: &mut LaneState,
    ctx: &ActorReconnectContext,
    lane: Lane,
    msg: Message,
    timeout: Duration,
) -> Result<()> {
    match state {
        LaneState::Connected(client) => {
            let result = tokio::time::timeout(timeout, client.fire(msg, lane)).await;
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    if matches!(&e, NetError::Io(_) | NetError::Protocol(_)) {
                        tracing::warn!(?lane, error = %e, "connection error, triggering reconnect");
                        *state = LaneState::Reconnecting {
                            attempt: 0,
                            backoff: ExponentialBackoff::new(
                                Duration::from_millis(500),
                                Duration::from_secs(30),
                            ),
                        };
                        ctx.spawn_reconnect();
                    }
                    Err(e)
                }
                Err(_elapsed) => Err(NetError::Timeout(format!("{lane:?} lane fire timeout"))),
            }
        }
        LaneState::Reconnecting { .. } => Err(NetError::Reconnecting),
        LaneState::Failed => Err(NetError::LaneFailed),
    }
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
        assert_eq!(LaneStatusReport::Failed, LaneStatusReport::Failed);
        assert_ne!(LaneStatusReport::Connected, LaneStatusReport::Failed);
    }

    #[test]
    fn actor_reconnect_context_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ActorReconnectContext>();
    }

    #[tokio::test]
    async fn spawn_lane_actor_with_failed_state_returns_lane_failed() {
        let handle = spawn_lane_actor(Lane::Raft, LaneState::Failed, |h| ActorReconnectContext {
            lane: Lane::Raft,
            config: Arc::new(NetConfig::default()),
            local_host_id: Uuid::new_v4(),
            peer_host: "127.0.0.1:9999".to_owned(),
            tls_connector: None,
            handle: h,
        });

        assert_eq!(handle.lane(), Lane::Raft);

        // Send should return LaneFailed for a Failed lane.
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
            "expected LaneFailed, got {result:?}"
        );

        // Fire should also return LaneFailed.
        let result = handle
            .fire(
                Message::Ping {
                    nonce: 2,
                    sent_at: 0,
                },
                None,
            )
            .await;
        assert!(
            matches!(result, Err(NetError::LaneFailed)),
            "expected LaneFailed, got {result:?}"
        );

        // Status should report Failed.
        let status = handle.query_status().await.unwrap();
        assert_eq!(status, LaneStatusReport::Failed);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn spawn_lane_actor_with_reconnecting_state() {
        let handle = spawn_lane_actor(
            Lane::Data,
            LaneState::Reconnecting {
                attempt: 1,
                backoff: ExponentialBackoff::new(
                    Duration::from_millis(100),
                    Duration::from_secs(10),
                ),
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
    async fn mark_failed_transitions_state() {
        let handle = spawn_lane_actor(
            Lane::Bulk,
            LaneState::Reconnecting {
                attempt: 1,
                backoff: ExponentialBackoff::new(
                    Duration::from_millis(100),
                    Duration::from_secs(10),
                ),
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

        // Initially reconnecting.
        let status = handle.query_status().await.unwrap();
        assert_eq!(status, LaneStatusReport::Reconnecting);

        // Mark failed.
        handle.mark_failed();

        // Give the actor a moment to process the command.
        tokio::task::yield_now().await;

        let status = handle.query_status().await.unwrap();
        assert_eq!(status, LaneStatusReport::Failed);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_stops_actor() {
        let handle = spawn_lane_actor(Lane::Raft, LaneState::Failed, |h| ActorReconnectContext {
            lane: Lane::Raft,
            config: Arc::new(NetConfig::default()),
            local_host_id: Uuid::new_v4(),
            peer_host: "127.0.0.1:9999".to_owned(),
            tls_connector: None,
            handle: h,
        });

        handle.shutdown().await;

        // After shutdown, the channel is closed so send should fail.
        // Give the actor loop a moment to exit.
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
}
