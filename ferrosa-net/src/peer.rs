//! Bounded internode peer ownership and liveness management.
//!
//! Responsibility: own one connection pool per remote host, publish peer
//! metadata, and notify cluster formation of validated transport events.
//! Correctness: the local host is rejected before dialing, metadata mutation,
//! peer-map insertion, or listener callbacks.
//! Last revised: 2026-08-26.
//! Last changed: enforce self-peer rejection at the network admission boundary.

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::message::Message;
use crate::pool::{LaneOutcome, PriorityPool};
use crate::rpc::handler::PeerId;
use crate::task_pool::TaskPool;

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
    local_host_id: uuid::Uuid,
    peers: RwLock<HashMap<uuid::Uuid, Arc<PeerState>>>,
    listener: Arc<dyn PeerEventListener>,
    /// CQL broadcast addresses learned from peer handshakes.
    peer_cql_broadcasts: RwLock<HashMap<uuid::Uuid, String>>,
    /// Internode broadcast hostnames learned from peer handshakes. Used so the
    /// committed `NodeInfo.addr` is a re-resolvable hostname, not a frozen IP.
    peer_internode_broadcasts: RwLock<HashMap<uuid::Uuid, String>>,
    raft_runtime: OnceLock<Arc<tokio::runtime::Runtime>>,
    data_runtime: OnceLock<Arc<tokio::runtime::Runtime>>,
    started_at: tokio::time::Instant,
}

struct PeerState {
    pool: Option<Arc<PriorityPool>>, // None for unit-test entries (add_peer_entry)
    peer_id: PeerId,
    last_activity_ms: AtomicU64,
    missed_heartbeats: AtomicU32,
}

impl PeerState {
    fn new(peer_id: PeerId, pool: Option<Arc<PriorityPool>>, now_ms: u64) -> Self {
        Self {
            pool,
            peer_id,
            last_activity_ms: AtomicU64::new(now_ms),
            missed_heartbeats: AtomicU32::new(0),
        }
    }

    fn record_activity(&self, now_ms: u64) {
        self.last_activity_ms.store(now_ms, Ordering::Relaxed);
        self.missed_heartbeats.store(0, Ordering::Relaxed);
    }

    fn activity_elapsed(&self, now_ms: u64) -> Duration {
        Duration::from_millis(now_ms.saturating_sub(self.last_activity_ms.load(Ordering::Relaxed)))
    }
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
            peer_internode_broadcasts: RwLock::new(HashMap::new()),
            raft_runtime: OnceLock::new(),
            data_runtime: OnceLock::new(),
            started_at: tokio::time::Instant::now(),
        }
    }

    pub fn set_raft_runtime(&self, runtime: Arc<tokio::runtime::Runtime>) {
        let _ = self.raft_runtime.set(runtime);
    }

    pub fn set_data_runtime(&self, runtime: Arc<tokio::runtime::Runtime>) {
        let _ = self.data_runtime.set(runtime);
    }

    pub fn raft_runtime(&self) -> Option<Arc<tokio::runtime::Runtime>> {
        self.raft_runtime.get().cloned()
    }

    pub fn data_runtime(&self) -> Option<Arc<tokio::runtime::Runtime>> {
        self.data_runtime.get().cloned()
    }

    fn now_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    async fn pool_for_peer(
        &self,
        host_id: uuid::Uuid,
    ) -> crate::error::Result<(Arc<PeerState>, Arc<PriorityPool>)> {
        let peers = self.peers.read().await;
        let state =
            Arc::clone(peers.get(&host_id).ok_or_else(|| {
                crate::error::NetError::Protocol(format!("unknown peer: {host_id}"))
            })?);
        let pool = state
            .pool
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| crate::error::NetError::Protocol("no connection pool".into()))?;
        Ok((state, pool))
    }

    /// Add a connected peer with a real connection pool.
    ///
    /// If the pool's handshake received a CQL broadcast address from the peer,
    /// it is stored for system.peers.native_address lookups.
    pub async fn add_peer(&self, peer_id: PeerId, pool: PriorityPool) {
        let (host_id, _addr) = peer_id;
        if host_id == self.local_host_id {
            tracing::error!(
                peer = %host_id,
                "rejecting self before network peer admission"
            );
            pool.shutdown().await;
            return;
        }
        // Extract the peer's CQL broadcast from the handshake before wrapping in Arc.
        if let Some(broadcast) = pool.peer_cql_broadcast() {
            self.peer_cql_broadcasts
                .write()
                .await
                .insert(host_id, broadcast.to_string());
        }
        // Same for the internode broadcast hostname — committed into NodeInfo.addr
        // so the address re-resolves across container IP churn.
        if let Some(broadcast) = pool.peer_internode_broadcast() {
            self.peer_internode_broadcasts
                .write()
                .await
                .insert(host_id, broadcast.to_string());
        }
        let state = Arc::new(PeerState::new(peer_id, Some(Arc::new(pool)), self.now_ms()));
        let old_pool = self
            .peers
            .write()
            .await
            .insert(host_id, state)
            .and_then(|old| old.pool.clone());
        self.listener.on_peer_connected(peer_id);
        if let Some(pool) = old_pool {
            pool.shutdown().await;
        }
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

    /// Returns `true` only if there is an established outbound pool for this peer.
    pub fn has_live_peer(&self, host_id: uuid::Uuid) -> bool {
        self.peers
            .try_read()
            .map(|peers| {
                peers
                    .get(&host_id)
                    .map(|state| state.pool.is_some())
                    .unwrap_or(false)
            })
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
        if host_id == self.local_host_id {
            return Err(crate::error::NetError::Protocol(format!(
                "refusing to connect local host_id {host_id} as a peer"
            )));
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

        {
            let peers = self.peers.read().await;
            if let Some(state) = peers.get(&host_id) {
                if state.pool.is_some() && state.peer_id.1 == resolved {
                    return Ok(());
                }
            }
        }

        let raft_runtime = self.raft_runtime.get().cloned();
        let data_runtime = self.data_runtime.get().cloned();
        let pool = PriorityPool::connect(
            self.config.clone(),
            self.local_host_id,
            addr,
            raft_runtime,
            data_runtime,
        )
        .await?;
        self.add_peer((host_id, resolved), pool).await;
        Ok(())
    }

    /// Add a peer entry without a connection pool (for unit testing).
    pub async fn add_peer_entry(&self, peer_id: PeerId) {
        let (host_id, _addr) = peer_id;
        if host_id == self.local_host_id {
            tracing::error!(
                peer = %host_id,
                "rejecting self before network peer-entry admission"
            );
            return;
        }
        let state = Arc::new(PeerState::new(peer_id, None, self.now_ms()));
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
        let (state, pool) = self.pool_for_peer(host_id).await?;
        let resp = pool.send(msg, lane).await?;
        state.record_activity(self.now_ms());
        Ok(resp)
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
        let (state, pool) = self.pool_for_peer(host_id).await?;
        let resp = pool.send_with_timeout(msg, lane, timeout).await?;
        state.record_activity(self.now_ms());
        Ok(resp)
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
        let (state, pool) = self.pool_for_peer(host_id).await?;
        pool.fire(msg, lane).await?;
        state.record_activity(self.now_ms());
        Ok(())
    }

    /// Heartbeat loop: sends Ping at configured interval, marks peers suspected
    /// after 3 missed heartbeats.
    ///
    /// Takes `Arc<Self>` so it can spawn per-peer tasks that call
    /// [`Self::record_heartbeat`] when a Pong is received.
    ///
    /// # Cancel Safety
    ///
    /// This method is cancel-safe. Liveness state is updated with atomics while
    /// holding only the peer-map read lock. Per-peer
    /// Ping sends are dispatched via `tokio::spawn`, so dropping this future
    /// between ticks does not leave shared state inconsistent. Note that
    /// `PriorityPool::send` (used inside each spawned task) is itself not
    /// cancel-safe; wrapping it in `tokio::spawn` is what makes it safe here.
    pub async fn run_heartbeat_loop(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.config.heartbeat_interval);
        let task_pool =
            TaskPool::from_optional_runtime("peer-heartbeat", self.raft_runtime.get().cloned());
        loop {
            interval.tick().await;

            // Collect work under the read lock, then release before any I/O.
            let mut to_ping: Vec<(uuid::Uuid, Arc<PriorityPool>)> = Vec::new();
            let mut suspected: Vec<(PeerId, Option<Arc<PriorityPool>>)> = Vec::new();
            {
                let peers = self.peers.read().await;
                let now_ms = self.now_ms();
                for (host_id, state) in peers.iter() {
                    let elapsed = state.activity_elapsed(now_ms);
                    if elapsed >= self.config.heartbeat_timeout {
                        let missed_heartbeats =
                            state.missed_heartbeats.fetch_add(1, Ordering::Relaxed) + 1;
                        if missed_heartbeats == 3 {
                            // Only push on the first detection (== 3) to avoid
                            // spawning a new monitor task on every subsequent tick.
                            tracing::warn!(
                                %host_id,
                                "peer suspected dead: {} missed heartbeats",
                                missed_heartbeats
                            );
                            let pool_arc = state.pool.as_ref().map(Arc::clone);
                            suspected.push((state.peer_id, pool_arc));
                        }
                    } else {
                        state.missed_heartbeats.store(0, Ordering::Relaxed);
                    }

                    if let Some(pool) = &state.pool {
                        to_ping.push((*host_id, Arc::clone(pool)));
                    }
                }
            } // read lock released

            // Notify listener and trigger reconnection outside the lock.
            for (peer_id, pool_opt) in suspected {
                self.listener.on_peer_suspected(peer_id);

                let (host_id, _addr) = peer_id;

                if let Some(pool) = pool_opt {
                    pool.reconnect_all_lanes();

                    let listener = Arc::clone(&self.listener);
                    task_pool.spawn(async move {
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
                task_pool.spawn(async move {
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
        self.record_activity(host_id).await;
    }

    /// Called when recent successful peer traffic proves the connection is alive.
    pub async fn record_activity(&self, host_id: uuid::Uuid) {
        let peers = self.peers.read().await;
        if let Some(state) = peers.get(&host_id) {
            state.record_activity(self.now_ms());
        }
    }

    /// Remove a peer and clean up all associated state (connection pool,
    /// CQL broadcast entry). Fires `on_peer_disconnected` if the peer existed.
    pub async fn remove_peer(&self, host_id: uuid::Uuid) {
        let removed = self.peers.write().await.remove(&host_id);
        self.peer_cql_broadcasts.write().await.remove(&host_id);
        self.peer_internode_broadcasts
            .write()
            .await
            .remove(&host_id);
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

    /// Store a peer's internode broadcast hostname learned during handshake.
    pub async fn set_peer_internode_broadcast(&self, host_id: uuid::Uuid, addr: String) {
        self.peer_internode_broadcasts
            .write()
            .await
            .insert(host_id, addr);
    }

    /// Retrieve a peer's internode broadcast hostname (if known from handshake).
    pub async fn get_peer_internode_broadcast(&self, host_id: uuid::Uuid) -> Option<String> {
        self.peer_internode_broadcasts
            .read()
            .await
            .get(&host_id)
            .cloned()
    }

    /// Non-blocking version for synchronous contexts (e.g., the cluster-mode
    /// peer-connected planner). Returns None if the lock is contended.
    pub fn get_peer_internode_broadcast_sync(&self, host_id: uuid::Uuid) -> Option<String> {
        self.peer_internode_broadcasts
            .try_read()
            .ok()
            .and_then(|guard| guard.get(&host_id).cloned())
    }

    /// Return the UUIDs of all currently live peers (those with an active pool).
    ///
    /// Used by the Accord coordinator to build the replica set for an LWT
    /// transaction. Non-blocking: if the lock is contended, returns an empty
    /// vec and the caller should retry or fail loud.
    pub fn live_peer_ids(&self) -> Vec<uuid::Uuid> {
        self.peers
            .try_read()
            .map(|peers| {
                peers
                    .iter()
                    .filter(|(_, state)| state.pool.is_some())
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default()
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

    #[tokio::test]
    async fn self_peer_is_rejected_before_network_tracking_or_callback() {
        let config = Arc::new(NetConfig::default());
        let listener = Arc::new(TestListener::new());
        let local_host_id = uuid::Uuid::new_v4();
        let pm = PeerManager::new(config, local_host_id, listener.clone());

        pm.add_peer_entry((local_host_id, "127.0.0.1:7000".parse().unwrap()))
            .await;

        assert!(
            !pm.has_peer(local_host_id),
            "self must not enter the network peer map"
        );
        assert_eq!(
            listener.connected_count.load(Ordering::Relaxed),
            0,
            "self rejection must happen before the formation callback"
        );
        assert!(pm.get_peer_cql_broadcast(local_host_id).await.is_none());
        assert!(pm
            .get_peer_internode_broadcast(local_host_id)
            .await
            .is_none());
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

    #[tokio::test(start_paused = true)]
    async fn recent_peer_activity_keeps_peer_alive() {
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

        for _ in 0..6 {
            tokio::time::advance(Duration::from_millis(90)).await;
            pm.record_activity(host_id).await;
        }

        assert_eq!(
            listener.suspected_count.load(Ordering::Relaxed),
            0,
            "recent peer activity should count as liveness and avoid false dead-peer suspicion"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_send_refreshes_peer_activity() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(300),
            ..NetConfig::default()
        };

        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();

        let listener = Arc::new(TestListener::new());
        let pm = Arc::new(PeerManager::new(
            Arc::new(config),
            uuid::Uuid::new_v4(),
            listener.clone(),
        ));
        pm.ensure_peer(server_id, &addr.to_string()).await.unwrap();

        let pm_clone = pm.clone();
        tokio::spawn(async move { pm_clone.run_heartbeat_loop().await });

        for nonce in 0..6 {
            tokio::time::sleep(Duration::from_millis(90)).await;
            let resp = pm
                .send(server_id, Message::Ping { nonce, sent_at: 0 }, Lane::Data)
                .await
                .unwrap();
            assert!(matches!(resp, Message::Pong { .. }));
        }

        assert_eq!(
            listener.suspected_count.load(Ordering::Relaxed),
            0,
            "successful send/response traffic should refresh peer liveness"
        );

        pm.remove_peer(server_id).await;
        server.shutdown(Duration::from_millis(50)).await;
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

        pm.remove_peer(server_id).await;
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
        assert!(
            pm.has_live_peer(server_id),
            "ensure_peer should upgrade placeholder peers into a live outbound pool"
        );

        pm.remove_peer(server_id).await;
        server.shutdown(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn ensure_peer_reconnects_when_address_changes() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };

        let server_id = uuid::Uuid::new_v4();

        let registry1 = Arc::new(HandlerRegistry::new());
        registry1.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server1 = Arc::new(RpcServer::new(config.clone(), server_id, registry1));
        let addr1 = server1.start_and_get_addr().await.unwrap();

        let registry2 = Arc::new(HandlerRegistry::new());
        registry2.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server2 = Arc::new(RpcServer::new(config.clone(), server_id, registry2));
        let addr2 = server2.start_and_get_addr().await.unwrap();

        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(Arc::new(config), uuid::Uuid::new_v4(), listener);
        let addr1_str = addr1.to_string();
        let addr2_str = addr2.to_string();

        pm.ensure_peer(server_id, &addr1_str).await.unwrap();
        assert_eq!(
            pm.peer_addr(server_id).await.as_deref(),
            Some(addr1_str.as_str())
        );
        let old_pool = {
            let peers = pm.peers.read().await;
            peers
                .get(&server_id)
                .and_then(|state| state.pool.clone())
                .expect("first ensure_peer should install a pool")
        };

        pm.ensure_peer(server_id, &addr2_str).await.unwrap();

        assert_eq!(
            pm.peer_addr(server_id).await.as_deref(),
            Some(addr2_str.as_str())
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), old_pool.all_lanes_resolved())
                .await
                .expect("old pool lane status query should not hang after replacement"),
            LaneOutcome::AnyFailed,
            "replacing a peer address must shut down old lane actors so they stop reconnecting to stale IPs"
        );
        let resp = pm
            .send(
                server_id,
                Message::Ping {
                    nonce: 11,
                    sent_at: 0,
                },
                Lane::Data,
            )
            .await
            .unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 11, .. }));

        pm.remove_peer(server_id).await;
        server1.shutdown(Duration::from_millis(50)).await;
        server2.shutdown(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn has_live_peer_is_false_for_placeholder_entries() {
        let config = Arc::new(NetConfig::default());
        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(config, uuid::Uuid::new_v4(), listener);

        let peer_id = (uuid::Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        pm.add_peer_entry(peer_id).await;

        assert!(
            pm.has_peer(peer_id.0),
            "placeholder entries are still tracked"
        );
        assert!(
            !pm.has_live_peer(peer_id.0),
            "placeholder entries must not count as live outbound pools"
        );
    }
}
