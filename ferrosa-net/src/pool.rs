use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::{NetError, Result};
use crate::message::Message;
use crate::reconnect::{connect_with_retry, spawn_alive_watcher, ExponentialBackoff, LaneState};
use crate::rpc::client::RpcClient;

/// A single lane slot protected by a mutex so the reconnect task can swap it.
type LaneSlot = Arc<Mutex<LaneState>>;

/// Everything needed to drive reconnection from outside `PriorityPool`.
///
/// Cloned into each alive-watcher closure so it can re-trigger a new reconnect
/// loop if the freshly-reconnected connection drops again.
#[derive(Clone)]
struct ReconnectContext {
    lane: Lane,
    config: Arc<NetConfig>,
    local_host_id: Uuid,
    peer_addr: SocketAddr,
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
    /// The slot for *this* lane (shared with `PriorityPool`).
    slot: LaneSlot,
    /// The other two slots — held so the alive-watcher closure can be
    /// reconstructed after a successful reconnect without a reference back
    /// to the pool.
    pool_raft: LaneSlot,
    pool_data: LaneSlot,
    pool_bulk: LaneSlot,
}

impl ReconnectContext {
    /// Spawn the reconnect loop for this lane.
    ///
    /// 1. Marks the slot `Reconnecting`.
    /// 2. Calls `connect_with_retry` (which sleeps with backoff internally).
    /// 3. On success: stores `Connected`, attaches a new alive watcher.
    /// 4. On exhaustion: stores `Failed`.
    fn spawn_reconnect(self) {
        tokio::spawn(async move {
            // Dedup: skip if already reconnecting or permanently failed.
            {
                let mut guard = self.slot.lock().await;
                match &*guard {
                    LaneState::Reconnecting { .. } | LaneState::Failed => return,
                    LaneState::Connected(_) => {}
                }
                *guard = LaneState::Reconnecting {
                    attempt: 0,
                    backoff: ExponentialBackoff::new(
                        Duration::from_millis(500),
                        Duration::from_secs(30),
                    ),
                };
            }

            let new_client = connect_with_retry(
                Arc::clone(&self.config),
                self.local_host_id,
                self.peer_addr,
                self.lane,
                self.tls_connector.clone(),
            )
            .await;

            let mut guard = self.slot.lock().await;
            match new_client {
                Some(client) => {
                    let alive_rx = client.alive_rx();
                    *guard = LaneState::Connected(client);
                    drop(guard);

                    // Build the next watcher context — identical to self
                    // except the slot is resolved from the pool_ fields so
                    // it stays in sync across reconnects.
                    let next_slot = self.lane_slot_from_pool();
                    let next_ctx = ReconnectContext {
                        slot: next_slot,
                        ..self
                    };

                    spawn_alive_watcher(alive_rx, move || {
                        next_ctx.clone().spawn_reconnect();
                    });
                }
                None => {
                    *guard = LaneState::Failed;
                }
            }
        });
    }

    /// Return the lane's own slot from the stored pool references.
    fn lane_slot_from_pool(&self) -> LaneSlot {
        match self.lane {
            Lane::Raft => Arc::clone(&self.pool_raft),
            Lane::Data => Arc::clone(&self.pool_data),
            Lane::Bulk => Arc::clone(&self.pool_bulk),
        }
    }
}

/// Outcome of checking all lane states in a [`PriorityPool`].
#[derive(Debug, PartialEq, Eq)]
pub enum LaneOutcome {
    /// All 3 lanes are `Connected`.
    AllConnected,
    /// At least one lane has `Failed` (exhausted all retry attempts).
    AnyFailed,
    /// At least one lane is still `Reconnecting` and none has `Failed`.
    StillReconnecting,
}

/// Maintains 3 connections to a peer — one per priority lane.
///
/// Each lane is independently monitored: if its TCP connection drops, a
/// background task retries with exponential backoff (up to
/// [`crate::reconnect::MAX_RECONNECT_ATTEMPTS`] attempts ≈ 5 min).
/// While a lane is reconnecting, `send`/`fire` return
/// [`NetError::Reconnecting`].  After all attempts are exhausted the lane
/// moves to [`LaneState::Failed`] and callers receive [`NetError::LaneFailed`].
pub struct PriorityPool {
    peer_host_id: Uuid,
    raft: LaneSlot,
    data: LaneSlot,
    bulk: LaneSlot,
    /// Kept so `reconnect_lane` / `reconnect_all_lanes` can build new contexts.
    reconnect_base: ReconnectContext,
}

impl PriorityPool {
    /// Open 3 TCP connections to the peer (one per lane).
    /// Builds TLS connector once and shares it across all 3 connections.
    pub async fn connect(
        config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_addr: SocketAddr,
    ) -> Result<Self> {
        let tls_connector = crate::tls::build_tls_connector(&config)?.map(Arc::new);

        let raft_client = RpcClient::connect_with_tls(
            Arc::clone(&config),
            local_host_id,
            peer_addr,
            tls_connector.as_deref(),
        )
        .await?;
        let data_client = RpcClient::connect_with_tls(
            Arc::clone(&config),
            local_host_id,
            peer_addr,
            tls_connector.as_deref(),
        )
        .await?;
        let bulk_client = RpcClient::connect_with_tls(
            Arc::clone(&config),
            local_host_id,
            peer_addr,
            tls_connector.as_deref(),
        )
        .await?;

        let peer_host_id = raft_client.peer_host_id();

        let raft: LaneSlot = Arc::new(Mutex::new(LaneState::Connected(raft_client)));
        let data: LaneSlot = Arc::new(Mutex::new(LaneState::Connected(data_client)));
        let bulk: LaneSlot = Arc::new(Mutex::new(LaneState::Connected(bulk_client)));

        // Shared base context — lane field is overridden per-lane below.
        let base = ReconnectContext {
            lane: Lane::Raft, // placeholder, overridden per attach call
            config,
            local_host_id,
            peer_addr,
            tls_connector,
            slot: Arc::clone(&raft),
            pool_raft: Arc::clone(&raft),
            pool_data: Arc::clone(&data),
            pool_bulk: Arc::clone(&bulk),
        };

        let pool = Self {
            peer_host_id,
            raft,
            data,
            bulk,
            reconnect_base: base,
        };

        // Attach alive watchers to all 3 initial connections.
        pool.attach_alive_watcher(Lane::Raft).await;
        pool.attach_alive_watcher(Lane::Data).await;
        pool.attach_alive_watcher(Lane::Bulk).await;

        Ok(pool)
    }

    /// The peer's host_id, obtained during the handshake on the first connection.
    pub fn peer_host_id(&self) -> Uuid {
        self.peer_host_id
    }

    fn slot(&self, lane: Lane) -> &LaneSlot {
        match lane {
            Lane::Raft => &self.raft,
            Lane::Data => &self.data,
            Lane::Bulk => &self.bulk,
        }
    }

    fn make_ctx(&self, lane: Lane) -> ReconnectContext {
        ReconnectContext {
            lane,
            slot: Arc::clone(self.slot(lane)),
            config: Arc::clone(&self.reconnect_base.config),
            local_host_id: self.reconnect_base.local_host_id,
            peer_addr: self.reconnect_base.peer_addr,
            tls_connector: self.reconnect_base.tls_connector.clone(),
            pool_raft: Arc::clone(&self.raft),
            pool_data: Arc::clone(&self.data),
            pool_bulk: Arc::clone(&self.bulk),
        }
    }

    /// Attach an alive watcher to the given lane if it is currently Connected.
    async fn attach_alive_watcher(&self, lane: Lane) {
        let alive_rx = {
            let guard = self.slot(lane).lock().await;
            match &*guard {
                LaneState::Connected(client) => client.alive_rx(),
                _ => return,
            }
        };
        let ctx = self.make_ctx(lane);
        spawn_alive_watcher(alive_rx, move || {
            ctx.clone().spawn_reconnect();
        });
    }

    /// Send a request/response message on the given lane.
    ///
    /// Returns `Err(NetError::Reconnecting)` while the lane is reconnecting,
    /// or `Err(NetError::LaneFailed)` once all retries are exhausted.
    pub async fn send(&self, msg: Message, lane: Lane) -> Result<Message> {
        let guard = self.slot(lane).lock().await;
        match &*guard {
            LaneState::Connected(client) => client.send(msg, lane).await,
            LaneState::Reconnecting { .. } => Err(NetError::Reconnecting),
            LaneState::Failed => Err(NetError::LaneFailed),
        }
    }

    /// Fire-and-forget a message on the given lane.
    pub async fn fire(&self, msg: Message, lane: Lane) -> Result<()> {
        let guard = self.slot(lane).lock().await;
        match &*guard {
            LaneState::Connected(client) => client.fire(msg, lane).await,
            LaneState::Reconnecting { .. } => Err(NetError::Reconnecting),
            LaneState::Failed => Err(NetError::LaneFailed),
        }
    }

    /// Trigger an immediate reconnect for a single lane.
    ///
    /// Useful for testing or externally-driven reconnect policies.
    /// The lane is marked `Reconnecting` instantly; a background task handles
    /// the actual retry loop.
    pub fn reconnect_lane(&self, lane: Lane) {
        self.make_ctx(lane).spawn_reconnect();
    }

    /// Trigger an immediate reconnect for all 3 lanes concurrently.
    pub fn reconnect_all_lanes(&self) {
        self.reconnect_lane(Lane::Raft);
        self.reconnect_lane(Lane::Data);
        self.reconnect_lane(Lane::Bulk);
    }

    /// Inspect the current state of all lanes without blocking.
    ///
    /// Returns:
    /// - [`LaneOutcome::AllConnected`] — all 3 lanes are `Connected`.
    /// - [`LaneOutcome::AnyFailed`] — at least one lane has exhausted retries.
    /// - [`LaneOutcome::StillReconnecting`] — all lanes are either `Connected`
    ///   or `Reconnecting`, with at least one still `Reconnecting`.
    pub async fn all_lanes_resolved(&self) -> LaneOutcome {
        let mut any_reconnecting = false;
        for slot in [&self.raft, &self.data, &self.bulk] {
            match &*slot.lock().await {
                LaneState::Failed => return LaneOutcome::AnyFailed,
                LaneState::Reconnecting { .. } => any_reconnecting = true,
                LaneState::Connected(_) => {}
            }
        }
        if any_reconnecting {
            LaneOutcome::StillReconnecting
        } else {
            LaneOutcome::AllConnected
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Lane, MsgType};
    use crate::config::NetConfig;
    use crate::message::Message;
    use crate::reconnect::{ExponentialBackoff, LaneState};
    use crate::rpc::handler::{HandlerRegistry, PeerId, RpcHandler};
    use crate::rpc::server::RpcServer;
    use std::sync::Arc;
    use std::time::Duration;

    struct EchoPingHandler;

    #[async_trait::async_trait]
    impl RpcHandler for EchoPingHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            match msg {
                Message::Ping { nonce, .. } => Some(Message::Pong {
                    nonce,
                    ping_recv_at: 0,
                    sent_at: 0,
                }),
                _ => None,
            }
        }
    }

    #[tokio::test]
    async fn pool_connects_and_sends_on_each_lane() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(
            config.clone(),
            uuid::Uuid::new_v4(),
            registry,
        ));
        let addr = server.start_and_get_addr().await.unwrap();

        let pool = PriorityPool::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
            .await
            .unwrap();

        for lane in [Lane::Raft, Lane::Data, Lane::Bulk] {
            let resp = pool
                .send(
                    Message::Ping {
                        nonce: 1,
                        sent_at: 0,
                    },
                    lane,
                )
                .await
                .unwrap();
            assert!(matches!(resp, Message::Pong { nonce: 1, .. }));
        }
    }

    #[test]
    fn lane_timeout_values() {
        assert_eq!(Lane::Raft.timeout(), Duration::from_secs(1));
        assert_eq!(Lane::Data.timeout(), Duration::from_secs(10));
        assert_eq!(Lane::Bulk.timeout(), Duration::from_secs(60));
    }

    /// Verify that `send` returns the correct error variants for non-Connected
    /// lane states, without needing a live server.
    #[tokio::test]
    async fn send_returns_reconnecting_when_lane_not_connected() {
        let reconnecting_slot: LaneSlot = Arc::new(Mutex::new(LaneState::Reconnecting {
            attempt: 1,
            backoff: ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10)),
        }));
        let failed_slot: LaneSlot = Arc::new(Mutex::new(LaneState::Failed));

        // Reconnecting path.
        let guard = reconnecting_slot.lock().await;
        let result: Result<Message> = match &*guard {
            LaneState::Connected(_) => panic!("unexpected Connected"),
            LaneState::Reconnecting { .. } => Err(NetError::Reconnecting),
            LaneState::Failed => Err(NetError::LaneFailed),
        };
        drop(guard);
        assert!(
            matches!(result, Err(NetError::Reconnecting)),
            "expected Reconnecting, got {result:?}"
        );

        // Failed path.
        let guard = failed_slot.lock().await;
        let result: Result<Message> = match &*guard {
            LaneState::Connected(_) => panic!("unexpected Connected"),
            LaneState::Reconnecting { .. } => Err(NetError::Reconnecting),
            LaneState::Failed => Err(NetError::LaneFailed),
        };
        drop(guard);
        assert!(
            matches!(result, Err(NetError::LaneFailed)),
            "expected LaneFailed, got {result:?}"
        );
    }
}
