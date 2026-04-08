use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::error::NetError;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::Result;
use crate::lane_actor::{
    spawn_lane_actor, spawn_raft_lane_actor, ActorReconnectContext, LaneHandle, LaneStatusReport,
};
use crate::message::Message;
use crate::reconnect::LaneState;
use crate::rpc::client::RpcClient;

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
/// Each lane is owned by a dedicated actor task that processes commands
/// sequentially via an mpsc channel.  This design eliminates the cancel-safety
/// hazard of holding a `tokio::Mutex` across `await` points (network
/// round-trips).
///
/// If a lane's TCP connection drops, the actor's alive watcher triggers a
/// background reconnect task.  While reconnecting, `send`/`fire` return
/// [`crate::error::NetError::Reconnecting`].  After all attempts are exhausted the lane
/// moves to `Failed` and callers receive [`crate::error::NetError::LaneFailed`].
pub struct PriorityPool {
    peer_host_id: Uuid,
    /// Resolved socket address from the initial connection.
    resolved_addr: SocketAddr,
    /// Peer's CQL broadcast address from handshake (if provided).
    peer_cql_broadcast: Option<String>,
    raft: LaneHandle,
    data: LaneHandle,
    bulk: LaneHandle,
}

impl PriorityPool {
    /// Open 3 TCP connections to the peer (one per lane).
    ///
    /// `peer_host` is a hostname:port or IP:port string.  It is resolved via
    /// DNS for the initial connection and stored as-is in each lane actor's
    /// reconnect context so that DNS is re-resolved on every reconnect attempt.
    /// This allows peers that restart with a new container IP (e.g. after
    /// `docker compose up --force-recreate`) to be reconnected automatically.
    ///
    /// Builds TLS connector once and shares it across all 3 connections.
    /// Create RpcClient on a specific runtime (or the caller's if None).
    async fn connect_client_on(
        config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_addr: SocketAddr,
        tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
        runtime: Option<&tokio::runtime::Runtime>,
    ) -> Result<RpcClient> {
        if let Some(rt) = runtime {
            let cfg = config;
            let tls = tls_connector;
            rt.spawn(async move {
                RpcClient::connect_with_tls(cfg, local_host_id, peer_addr, tls.as_deref()).await
            })
            .await
            .map_err(|e| NetError::Protocol(format!("runtime join error: {e}")))?
        } else {
            RpcClient::connect_with_tls(config, local_host_id, peer_addr, tls_connector.as_deref())
                .await
        }
    }

    /// Connect to a peer, creating one TCP connection per lane.
    ///
    /// Each client's IO loops are spawned on the provided runtime (or the
    /// caller's runtime if None). This keeps the main runtime free for
    /// supervision only.
    pub async fn connect(
        config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_host: &str,
        raft_runtime: Option<&tokio::runtime::Runtime>,
        data_runtime: Option<&tokio::runtime::Runtime>,
    ) -> Result<Self> {
        let peer_addr: SocketAddr = tokio::net::lookup_host(peer_host)
            .await
            .map_err(NetError::Io)?
            .next()
            .ok_or_else(|| NetError::Protocol(format!("no address resolved for {peer_host}")))?;

        let tls_connector = crate::tls::build_tls_connector(&config)?.map(Arc::new);

        let raft_client = Self::connect_client_on(
            Arc::clone(&config), local_host_id, peer_addr,
            tls_connector.clone(), raft_runtime,
        ).await?;
        let data_client = Self::connect_client_on(
            Arc::clone(&config), local_host_id, peer_addr,
            tls_connector.clone(), data_runtime,
        ).await?;
        let bulk_client = Self::connect_client_on(
            Arc::clone(&config), local_host_id, peer_addr,
            tls_connector.clone(), data_runtime,
        ).await?;

        let peer_host_id = raft_client.peer_host_id();
        let peer_cql_broadcast = raft_client.peer_cql_broadcast().map(String::from);
        let peer_host = peer_host.to_owned();

        // Spawn a lane actor for each connection.  The ctx_builder closure
        // captures the shared config/TLS state and wires up the reconnect
        // context with a handle back to the actor itself.
        let peer_label = format!("{}", peer_host_id.as_fields().0);
        let raft_config = Arc::clone(&config);
        let raft_peer_host = peer_host.clone();
        let raft_tls = tls_connector.clone();
        let raft = spawn_raft_lane_actor(
            Lane::Raft,
            LaneState::Connected(raft_client),
            peer_label,
            move |h| ActorReconnectContext {
                lane: Lane::Raft,
                config: raft_config,
                local_host_id,
                peer_host: raft_peer_host,
                tls_connector: raft_tls,
                handle: h,
            },
        );
        let data = spawn_lane_actor(Lane::Data, LaneState::Connected(data_client), |h| {
            ActorReconnectContext {
                lane: Lane::Data,
                config: Arc::clone(&config),
                local_host_id,
                peer_host: peer_host.clone(),
                tls_connector: tls_connector.clone(),
                handle: h,
            }
        });
        let bulk = spawn_lane_actor(Lane::Bulk, LaneState::Connected(bulk_client), |h| {
            ActorReconnectContext {
                lane: Lane::Bulk,
                config: Arc::clone(&config),
                local_host_id,
                peer_host,
                tls_connector: tls_connector.clone(),
                handle: h,
            }
        });

        Ok(Self {
            peer_host_id,
            resolved_addr: peer_addr,
            peer_cql_broadcast,
            raft,
            data,
            bulk,
        })
    }

    /// The peer's host_id, obtained during the handshake on the first connection.
    pub fn peer_host_id(&self) -> Uuid {
        self.peer_host_id
    }

    /// Peer's CQL broadcast address from the handshake, if provided.
    pub fn peer_cql_broadcast(&self) -> Option<&str> {
        self.peer_cql_broadcast.as_deref()
    }

    /// The socket address resolved during the initial connection.
    pub fn resolved_addr(&self) -> SocketAddr {
        self.resolved_addr
    }

    fn handle(&self, lane: Lane) -> &LaneHandle {
        match lane {
            Lane::Raft => &self.raft,
            Lane::Data => &self.data,
            Lane::Bulk => &self.bulk,
        }
    }

    /// Send a request/response message on the given lane.
    ///
    /// Returns `Err(NetError::Reconnecting)` while the lane is reconnecting,
    /// or `Err(NetError::LaneFailed)` once all retries are exhausted.
    pub async fn send(&self, msg: Message, lane: Lane) -> Result<Message> {
        self.handle(lane).send(msg, None).await
    }

    /// Send a request/response message on the given lane with a custom timeout.
    pub async fn send_with_timeout(
        &self,
        msg: Message,
        lane: Lane,
        timeout: Duration,
    ) -> Result<Message> {
        self.handle(lane).send(msg, Some(timeout)).await
    }

    /// Fire-and-forget a message on the given lane.
    pub async fn fire(&self, msg: Message, lane: Lane) -> Result<()> {
        self.handle(lane).fire(msg, None).await
    }

    /// Trigger an immediate reconnect for a single lane.
    ///
    /// No-op: the alive watcher attached to each lane actor handles
    /// reconnection automatically.  Retained for API compatibility.
    pub fn reconnect_lane(&self, _lane: Lane) {
        // Reconnection is driven by the alive watcher inside each lane actor.
    }

    /// Trigger an immediate reconnect for all 3 lanes concurrently.
    ///
    /// No-op: the alive watcher attached to each lane actor handles
    /// reconnection automatically.  Retained for API compatibility.
    pub fn reconnect_all_lanes(&self) {
        // Reconnection is driven by the alive watcher inside each lane actor.
    }

    /// Inspect the current state of all lanes.
    ///
    /// Returns:
    /// - [`LaneOutcome::AllConnected`] — all 3 lanes are `Connected`.
    /// - [`LaneOutcome::AnyFailed`] — at least one lane has exhausted retries.
    /// - [`LaneOutcome::StillReconnecting`] — all lanes are either `Connected`
    ///   or `Reconnecting`, with at least one still `Reconnecting`.
    pub async fn all_lanes_resolved(&self) -> LaneOutcome {
        let mut any_reconnecting = false;
        for handle in [&self.raft, &self.data, &self.bulk] {
            match handle.query_status().await {
                Ok(LaneStatusReport::Failed) | Err(_) => return LaneOutcome::AnyFailed,
                Ok(LaneStatusReport::Reconnecting) => any_reconnecting = true,
                Ok(LaneStatusReport::Connected) => {}
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
    use crate::error::NetError;
    use crate::lane_actor::{spawn_lane_actor, ActorReconnectContext};
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

        let pool = PriorityPool::connect(Arc::new(config), uuid::Uuid::new_v4(), &addr.to_string(), None, None)
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

    /// Connect to a test server, send Ping on each lane, verify Pong response.
    /// Validates the actor-based pool works end-to-end with real TCP connections.
    #[tokio::test]
    async fn pool_actor_send_and_fire() {
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

        let pool = PriorityPool::connect(Arc::new(config), uuid::Uuid::new_v4(), &addr.to_string(), None, None)
            .await
            .unwrap();

        // Send (request/response) on each lane.
        for lane in [Lane::Raft, Lane::Data, Lane::Bulk] {
            let resp = pool
                .send(
                    Message::Ping {
                        nonce: 42,
                        sent_at: 0,
                    },
                    lane,
                )
                .await
                .unwrap();
            assert!(
                matches!(resp, Message::Pong { nonce: 42, .. }),
                "expected Pong {{ nonce: 42 }} on {lane:?}, got {resp:?}"
            );
        }

        // Fire (fire-and-forget) on each lane.
        for lane in [Lane::Raft, Lane::Data, Lane::Bulk] {
            pool.fire(
                Message::Ping {
                    nonce: 99,
                    sent_at: 0,
                },
                lane,
            )
            .await
            .unwrap();
        }

        // All lanes should be connected.
        assert_eq!(pool.all_lanes_resolved().await, LaneOutcome::AllConnected);
    }

    #[test]
    fn lane_timeout_values() {
        assert_eq!(Lane::Raft.timeout(), Duration::from_secs(1));
        assert_eq!(Lane::Data.timeout(), Duration::from_secs(10));
        assert_eq!(Lane::Bulk.timeout(), Duration::from_secs(60));
    }

    /// Verify that `send` returns the correct error variants for non-Connected
    /// lane states, using actor-based approach.
    #[tokio::test]
    async fn send_returns_reconnecting_when_lane_not_connected() {
        // Spawn a lane actor in Reconnecting state.
        let handle = spawn_lane_actor(
            Lane::Raft,
            LaneState::Reconnecting {
                attempt: 1,
                backoff: ExponentialBackoff::new(
                    Duration::from_millis(100),
                    Duration::from_secs(10),
                ),
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

        // Send via LaneHandle: should return Reconnecting.
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

        // Mark the lane as failed.
        handle.mark_failed();
        // Give the actor a moment to process the command.
        tokio::task::yield_now().await;

        // Next send should return LaneFailed.
        let result = handle
            .send(
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

        handle.shutdown().await;
    }
}
