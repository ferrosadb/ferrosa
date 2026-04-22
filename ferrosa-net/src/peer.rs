use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::message::Message;
use crate::pool::{LaneOutcome, PriorityPool};
use crate::rpc::handler::PeerId;

/// Subscribe to peer lifecycle events.
pub trait PeerEventListener: Send + Sync {
    fn on_peer_connected(&self, peer: PeerId);
    fn on_peer_disconnected(&self, peer: PeerId);
    fn on_peer_suspected(&self, peer: PeerId);
    /// Called when a suspected peer successfully re-establishes all lanes.
    fn on_peer_recovered(&self, peer_id: uuid::Uuid);
    /// Called when all reconnection attempts for a suspected peer are exhausted.
    fn on_peer_failed(&self, peer_id: uuid::Uuid);
}

/// Manages all peer connections and runs failure detection.
pub struct PeerManager {
    config: Arc<NetConfig>,
    #[allow(dead_code)]
    local_host_id: uuid::Uuid,
    peers: RwLock<HashMap<uuid::Uuid, PeerState>>,
    listener: Arc<dyn PeerEventListener>,
    /// CQL broadcast addresses learned from peer handshakes.
    peer_cql_broadcasts: RwLock<HashMap<uuid::Uuid, String>>,
}

struct PeerState {
    pool: Option<Arc<PriorityPool>>, // None for unit-test entries (add_peer_entry)
    peer_id: PeerId,
    last_heartbeat: tokio::time::Instant,
    missed_heartbeats: u32,
}

impl PeerManager {
    pub fn new(
        config: Arc<NetConfig>,
        local_host_id: uuid::Uuid,
        listener: Arc<dyn PeerEventListener>,
    ) -> Self {
        Self {
            config,
            local_host_id,
            peers: RwLock::new(HashMap::new()),
            listener,
            peer_cql_broadcasts: RwLock::new(HashMap::new()),
        }
    }

    /// Add a connected peer with a real connection pool.
    ///
    /// If the pool's handshake received a CQL broadcast address from the peer,
    /// it is stored for system.peers.native_address lookups.
    pub async fn add_peer(&self, peer_id: PeerId, pool: PriorityPool) {
        let (host_id, _addr) = peer_id;
        // Extract the peer's CQL broadcast from the handshake before wrapping in Arc.
        if let Some(broadcast) = pool.peer_cql_broadcast() {
            self.peer_cql_broadcasts
                .write()
                .await
                .insert(host_id, broadcast.to_string());
        }
        let state = PeerState {
            pool: Some(Arc::new(pool)),
            peer_id,
            last_heartbeat: tokio::time::Instant::now(),
            missed_heartbeats: 0,
        };
        self.peers.write().await.insert(host_id, state);
        self.listener.on_peer_connected(peer_id);
    }

    /// Returns `true` if the PeerManager has an outbound connection to this peer.
    pub fn has_peer(&self, host_id: uuid::Uuid) -> bool {
        // Use try_read to avoid blocking the caller — if the lock is held,
        // conservatively return false (the peer will be connected shortly).
        self.peers
            .try_read()
            .map(|peers| peers.contains_key(&host_id))
            .unwrap_or(false)
    }

    /// Return the last known socket address for `host_id`, even if this peer
    /// currently has no active outbound pool.
    pub async fn peer_addr(&self, host_id: uuid::Uuid) -> Option<String> {
        let peers = self.peers.read().await;
        peers.get(&host_id).map(|state| state.peer_id.1.to_string())
    }

    /// Ensure there is an outbound connection pool for `host_id`.
    ///
    /// Uses the provided address string (IP:port or resolvable hostname:port)
    /// to establish the pool if one is not already present.
    pub async fn ensure_peer(&self, host_id: uuid::Uuid, addr: &str) -> crate::error::Result<()> {
        {
            let peers = self.peers.read().await;
            if let Some(state) = peers.get(&host_id) {
                if state.pool.is_some() {
                    return Ok(());
                }
            }
        }

        let resolved = addr
            .to_socket_addrs()
            .map_err(|e| {
                crate::error::NetError::Protocol(format!("invalid peer address '{addr}': {e}"))
            })?
            .next()
            .ok_or_else(|| {
                crate::error::NetError::Protocol(format!(
                    "peer address '{addr}' resolved to no socket addresses"
                ))
            })?;

        let pool = PriorityPool::connect(self.config.clone(), self.local_host_id, addr, None, None)
            .await?;
        self.add_peer((host_id, resolved), pool).await;
        Ok(())
    }

    /// Add a peer entry without a connection pool (for unit testing).
    pub async fn add_peer_entry(&self, peer_id: PeerId) {
        let (host_id, _addr) = peer_id;
        let state = PeerState {
            pool: None,
            peer_id,
            last_heartbeat: tokio::time::Instant::now(),
            missed_heartbeats: 0,
        };
        self.peers.write().await.insert(host_id, state);
        self.listener.on_peer_connected(peer_id);
    }

    /// Send a message to a peer on the specified lane.
    ///
    /// # Cancel Safety
    ///
    /// This method is cancel-safe. It delegates to the lane actor via
    /// `PriorityPool::send`, which uses `reserve`+`send` for enqueue and a oneshot
    /// for the response. Dropping the returned future before it resolves does not
    /// corrupt any shared state.
    pub async fn send(
        &self,
        host_id: uuid::Uuid,
        msg: Message,
        lane: Lane,
    ) -> crate::error::Result<Message> {
        // Clone the pool Arc and drop the read lock BEFORE the network
        // round-trip. Holding the lock across .await starves writers
        // (heartbeat loop, peer additions) and blocks all other sends.
        let pool = {
            let peers = self.peers.read().await;
            let state = peers.get(&host_id).ok_or_else(|| {
                crate::error::NetError::Protocol(format!("unknown peer: {host_id}"))
            })?;
            match &state.pool {
                Some(pool) => Arc::clone(pool),
                None => {
                    return Err(crate::error::NetError::Protocol(
                        "no connection pool".into(),
                    ))
                }
            }
        };
        pool.send(msg, lane).await
    }

    /// Send a message to a peer on the specified lane with a custom timeout.
    ///
    /// # Cancel Safety
    ///
    /// This method is cancel-safe. The timeout is managed inside the lane actor;
    /// dropping the returned future before it resolves does not orphan an in-flight
    /// request — the actor discards the response slot when the timeout fires.
    pub async fn send_with_timeout(
        &self,
        host_id: uuid::Uuid,
        msg: Message,
        lane: Lane,
        timeout: Duration,
    ) -> crate::error::Result<Message> {
        let pool = {
            let peers = self.peers.read().await;
            let state = peers.get(&host_id).ok_or_else(|| {
                crate::error::NetError::Protocol(format!("unknown peer: {host_id}"))
            })?;
            match &state.pool {
                Some(pool) => Arc::clone(pool),
                None => {
                    return Err(crate::error::NetError::Protocol(
                        "no connection pool".into(),
                    ))
                }
            }
        };
        pool.send_with_timeout(msg, lane, timeout).await
    }

    /// Fire-and-forget a message to a peer on the specified lane.
    ///
    /// Unlike [`send()`](Self::send), this does not wait for a response. Used for
    /// repair writes and other best-effort messages.
    pub async fn fire(
        &self,
        host_id: uuid::Uuid,
        msg: Message,
        lane: Lane,
    ) -> crate::error::Result<()> {
        let pool = {
            let peers = self.peers.read().await;
            let state = peers.get(&host_id).ok_or_else(|| {
                crate::error::NetError::Protocol(format!("unknown peer: {host_id}"))
            })?;
            match &state.pool {
                Some(pool) => Arc::clone(pool),
                None => {
                    return Err(crate::error::NetError::Protocol(
                        "no connection pool".into(),
                    ))
                }
            }
        };
        pool.fire(msg, lane).await
    }

    /// Heartbeat loop: sends Ping at configured interval, marks peers suspected
    /// after 3 missed heartbeats.
    ///
    /// Takes `Arc<Self>` so it can spawn per-peer tasks that call
    /// [`Self::record_heartbeat`] when a Pong is received.
    ///
    /// # Cancel Safety
    ///
    /// This method is cancel-safe. All mutable state is updated while holding
    /// the write lock with no `.await` inside the critical section. Per-peer
    /// Ping sends are dispatched via `tokio::spawn`, so dropping this future
    /// between ticks does not leave shared state inconsistent. Note that
    /// `PriorityPool::send` (used inside each spawned task) is itself not
    /// cancel-safe; wrapping it in `tokio::spawn` is what makes it safe here.
    pub async fn run_heartbeat_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.config.heartbeat_interval);
        loop {
            interval.tick().await;

            // Collect work under the write lock, then release before any I/O.
            let mut to_ping: Vec<(uuid::Uuid, Arc<PriorityPool>)> = Vec::new();
            let mut suspected: Vec<(PeerId, Option<Arc<PriorityPool>>)> = Vec::new();
            {
                let mut peers = self.peers.write().await;
                for (host_id, state) in peers.iter_mut() {
                    let elapsed = state.last_heartbeat.elapsed();
                    if elapsed >= self.config.heartbeat_timeout {
                        state.missed_heartbeats += 1;
                        if state.missed_heartbeats == 3 {
                            // Only push on the first detection (== 3) to avoid
                            // spawning a new monitor task on every subsequent tick.
                            tracing::warn!(
                                %host_id,
                                "peer suspected dead: {} missed heartbeats",
                                state.missed_heartbeats
                            );
                            let pool_arc = state.pool.as_ref().map(Arc::clone);
                            suspected.push((state.peer_id, pool_arc));
                        }
                    } else {
                        state.missed_heartbeats = 0;
                    }

                    if let Some(pool) = &state.pool {
                        to_ping.push((*host_id, Arc::clone(pool)));
                    }
                }
            } // write lock released

            // Notify listener and trigger reconnection outside the lock.
            for (peer_id, pool_opt) in suspected {
                self.listener.on_peer_suspected(peer_id);

                let (host_id, _addr) = peer_id;

                if let Some(pool) = pool_opt {
                    pool.reconnect_all_lanes();

                    let listener = Arc::clone(&self.listener);
                    tokio::spawn(async move {
                        for _ in 0u32..120 {
                            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                            match pool.all_lanes_resolved().await {
                                LaneOutcome::AllConnected => {
                                    listener.on_peer_recovered(host_id);
                                    return;
                                }
                                LaneOutcome::AnyFailed => {
                                    listener.on_peer_failed(host_id);
                                    return;
                                }
                                LaneOutcome::StillReconnecting => {}
                            }
                        }

                        listener.on_peer_failed(host_id);
                    });
                }
            }

            // Send Pings via request-response so Pong is delivered through the
            // pending map and record_heartbeat resets the miss counter.
            for (host_id, pool) in to_ping {
                let this = Arc::clone(&self);
                tokio::spawn(async move {
                    let nonce: u64 = rand::random();
                    let sent_at = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    if let Ok(Message::Pong { .. }) = pool
                        .send(Message::Ping { nonce, sent_at }, Lane::Raft)
                        .await
                    {
                        this.record_heartbeat(host_id).await;
                    }
                });
            }
        }
    }

    /// Called when Pong received -- reset heartbeat timer and missed counter.
    pub async fn record_heartbeat(&self, host_id: uuid::Uuid) {
        let mut peers = self.peers.write().await;
        if let Some(state) = peers.get_mut(&host_id) {
            state.last_heartbeat = tokio::time::Instant::now();
            state.missed_heartbeats = 0;
        }
    }

    /// Remove a peer and clean up all associated state (connection pool,
    /// CQL broadcast entry). Fires `on_peer_disconnected` if the peer existed.
    pub async fn remove_peer(&self, host_id: uuid::Uuid) {
        let removed = self.peers.write().await.remove(&host_id);
        self.peer_cql_broadcasts.write().await.remove(&host_id);
        if let Some(state) = removed {
            self.listener.on_peer_disconnected(state.peer_id);
        }
    }

    /// Store a peer's CQL broadcast address learned during handshake.
    pub async fn set_peer_cql_broadcast(&self, host_id: uuid::Uuid, addr: String) {
        self.peer_cql_broadcasts.write().await.insert(host_id, addr);
    }

    /// Retrieve a peer's CQL broadcast address (if known from handshake).
    pub async fn get_peer_cql_broadcast(&self, host_id: uuid::Uuid) -> Option<String> {
        self.peer_cql_broadcasts.read().await.get(&host_id).cloned()
    }

    /// Non-blocking version for synchronous contexts (e.g., system.peers query).
    /// Returns None if the lock is contended.
    pub fn get_peer_cql_broadcast_sync(&self, host_id: uuid::Uuid) -> Option<String> {
        self.peer_cql_broadcasts
            .try_read()
            .ok()
            .and_then(|guard| guard.get(&host_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::codec::MsgType;
    use crate::config::NetConfig;
    use crate::message::Message;
    use crate::rpc::handler::{HandlerRegistry, RpcHandler};
    use crate::rpc::server::RpcServer;

    struct TestListener {
        connected_count: AtomicUsize,
        suspected_count: AtomicUsize,
        disconnected_count: AtomicUsize,
        recovered_count: AtomicUsize,
        failed_count: AtomicUsize,
    }

    impl TestListener {
        fn new() -> Self {
            Self {
                connected_count: AtomicUsize::new(0),
                suspected_count: AtomicUsize::new(0),
                disconnected_count: AtomicUsize::new(0),
                recovered_count: AtomicUsize::new(0),
                failed_count: AtomicUsize::new(0),
            }
        }
    }

    impl PeerEventListener for TestListener {
        fn on_peer_connected(&self, _peer: PeerId) {
            self.connected_count.fetch_add(1, Ordering::Relaxed);
        }
        fn on_peer_disconnected(&self, _peer: PeerId) {
            self.disconnected_count.fetch_add(1, Ordering::Relaxed);
        }
        fn on_peer_suspected(&self, _peer: PeerId) {
            self.suspected_count.fetch_add(1, Ordering::Relaxed);
        }
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {
            self.recovered_count.fetch_add(1, Ordering::Relaxed);
        }
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {
            self.failed_count.fetch_add(1, Ordering::Relaxed);
        }
    }

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
    async fn peer_event_listener_receives_connected() {
        let config = Arc::new(NetConfig::default());
        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(config, uuid::Uuid::new_v4(), listener.clone());

        let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        pm.add_peer_entry(peer_id).await;

        assert_eq!(listener.connected_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn peer_event_listener_receives_suspected() {
        let config = Arc::new(NetConfig {
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(300),
            ..NetConfig::default()
        });
        let listener = Arc::new(TestListener::new());
        let pm = Arc::new(PeerManager::new(
            config,
            uuid::Uuid::new_v4(),
            listener.clone(),
        ));

        let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        pm.add_peer_entry(peer_id).await;

        let pm_clone = pm.clone();
        tokio::spawn(async move { pm_clone.run_heartbeat_loop().await });

        // Advance time past 3 missed heartbeats, yielding between each
        // interval tick so the spawned task can process.
        // With interval=100ms, timeout=300ms:
        //   t=0: first tick (immediate), elapsed ~0 => no miss
        //   t=100ms: elapsed=100ms < 300ms => no miss
        //   t=200ms: elapsed=200ms < 300ms => no miss
        //   t=300ms: elapsed=300ms >= 300ms => miss=1
        //   t=400ms: elapsed=400ms >= 300ms => miss=2
        //   t=500ms: elapsed=500ms >= 300ms => miss=3 => suspected!
        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }

        assert!(listener.suspected_count.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_keeps_peer_alive() {
        let config = Arc::new(NetConfig {
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(300),
            ..NetConfig::default()
        });
        let listener = Arc::new(TestListener::new());
        let pm = Arc::new(PeerManager::new(
            config,
            uuid::Uuid::new_v4(),
            listener.clone(),
        ));

        let host_id = uuid::Uuid::new_v4();
        let peer_id = (host_id, "127.0.0.1:7000".parse().unwrap());
        pm.add_peer_entry(peer_id).await;

        let pm_clone = pm.clone();
        tokio::spawn(async move { pm_clone.run_heartbeat_loop().await });

        // Simulate heartbeats every 90ms for 600ms
        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(90)).await;
            pm.record_heartbeat(host_id).await;
        }

        assert_eq!(listener.suspected_count.load(Ordering::Relaxed), 0);
    }

    /// Verify that suspecting a peer (with no real pool) fires on_peer_suspected
    /// but does not panic. The monitor task is skipped when pool is None.
    #[tokio::test(start_paused = true)]
    async fn suspected_peer_triggers_reconnection() {
        let config = Arc::new(NetConfig {
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(300),
            ..NetConfig::default()
        });
        let listener = Arc::new(TestListener::new());
        let pm = Arc::new(PeerManager::new(
            config,
            uuid::Uuid::new_v4(),
            listener.clone(),
        ));

        // add_peer_entry inserts a pool-less entry (simulates a peer whose pool
        // isn't wired up in tests). The heartbeat loop still fires on_peer_suspected.
        let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        pm.add_peer_entry(peer_id).await;

        let pm_clone = pm.clone();
        tokio::spawn(async move { pm_clone.run_heartbeat_loop().await });

        // Drive the loop until the peer is suspected.
        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
        }

        assert!(
            listener.suspected_count.load(Ordering::Relaxed) >= 1,
            "on_peer_suspected should have fired"
        );
        // No pool → no reconnect task → no recovered/failed callbacks.
        assert_eq!(
            listener.recovered_count.load(Ordering::Relaxed),
            0,
            "no pool means no reconnect and therefore no recovered event"
        );
    }

    #[tokio::test]
    async fn remove_peer_cleans_up_broadcast_map() {
        let config = Arc::new(NetConfig::default());
        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(config, uuid::Uuid::new_v4(), listener.clone());

        let host_id = uuid::Uuid::new_v4();
        let peer_id = (host_id, "127.0.0.1:7000".parse().unwrap());
        pm.add_peer_entry(peer_id).await;
        pm.set_peer_cql_broadcast(host_id, "10.0.0.1:9042".to_string())
            .await;

        assert!(pm.get_peer_cql_broadcast(host_id).await.is_some());
        assert!(pm.has_peer(host_id));

        pm.remove_peer(host_id).await;

        assert!(pm.get_peer_cql_broadcast(host_id).await.is_none());
        assert!(!pm.has_peer(host_id));
        assert_eq!(listener.disconnected_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn remove_peer_noop_for_unknown() {
        let config = Arc::new(NetConfig::default());
        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(config, uuid::Uuid::new_v4(), listener.clone());

        // Should not panic or fire disconnected.
        pm.remove_peer(uuid::Uuid::new_v4()).await;
        assert_eq!(listener.disconnected_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn fire_returns_error_for_unknown_peer() {
        let config = Arc::new(NetConfig::default());
        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(config, uuid::Uuid::new_v4(), listener);
        let result = pm
            .fire(
                uuid::Uuid::new_v4(),
                Message::Ping {
                    nonce: 1,
                    sent_at: 0,
                },
                Lane::Data,
            )
            .await;
        assert!(result.is_err(), "fire to unknown peer should fail");
    }

    #[tokio::test]
    async fn ensure_peer_connects_and_caches_pool() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };

        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();

        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(Arc::new(config), uuid::Uuid::new_v4(), listener.clone());

        pm.ensure_peer(server_id, &addr.to_string()).await.unwrap();

        assert!(pm.has_peer(server_id), "ensure_peer should cache the pool");
        assert_eq!(listener.connected_count.load(Ordering::Relaxed), 1);

        let resp = pm
            .send(
                server_id,
                Message::Ping {
                    nonce: 99,
                    sent_at: 0,
                },
                Lane::Data,
            )
            .await
            .unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 99, .. }));

        server.shutdown(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn peer_addr_returns_cached_peer_entry_address() {
        let config = Arc::new(NetConfig::default());
        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(config, uuid::Uuid::new_v4(), listener);

        let host_id = uuid::Uuid::new_v4();
        let addr = "127.0.0.1:9042".parse().unwrap();
        pm.add_peer_entry((host_id, addr)).await;

        assert_eq!(
            pm.peer_addr(host_id).await.as_deref(),
            Some("127.0.0.1:9042")
        );
    }

    #[tokio::test]
    async fn ensure_peer_replaces_entry_without_pool() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };

        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();

        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(Arc::new(config), uuid::Uuid::new_v4(), listener.clone());
        pm.add_peer_entry((server_id, addr)).await;

        pm.ensure_peer(server_id, &addr.to_string()).await.unwrap();

        let resp = pm
            .send(
                server_id,
                Message::Ping {
                    nonce: 7,
                    sent_at: 0,
                },
                Lane::Data,
            )
            .await
            .unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 7, .. }));
        assert_eq!(
            listener.connected_count.load(Ordering::Relaxed),
            2,
            "add_peer_entry plus ensure_peer should emit a second connected event when the pool is established"
        );

        server.shutdown(Duration::from_millis(50)).await;
    }
}
