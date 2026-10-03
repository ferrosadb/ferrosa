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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::NetError;
use crate::message::Message;
use crate::pool::{LaneOutcome, PriorityPool};
use crate::reconnect::{inc_total_reconnect_attempts, redial_delay};
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
    /// Serialises dead-pool replacement so concurrent requests share one dial.
    replace_lock: tokio::sync::Mutex<()>,
}

struct PeerState {
    pool: Option<Arc<PriorityPool>>, // None for unit-test entries (add_peer_entry)
    peer_id: PeerId,
    last_activity_ms: AtomicU64,
    missed_heartbeats: AtomicU32,
    /// Set only on the pool-less placeholder left by a failed pool
    /// replacement: the heartbeat loop re-dials such a peer until it answers
    /// or is removed (t_48d168ee).
    redial: Option<Arc<RedialState>>,
}

/// Schedule and bookkeeping for re-dialing a peer that has no pool.
struct RedialState {
    /// `now_ms` at or after which the next re-dial may start.
    next_due_ms: AtomicU64,
    /// Failed re-dials so far; drives [`redial_delay`].
    failures: AtomicU32,
    /// A re-dial task is running; at most one per peer.
    in_flight: AtomicBool,
    /// An identity mismatch was already reported for this outage.
    mismatch_reported: AtomicBool,
}

impl RedialState {
    fn new(now_ms: u64) -> Self {
        Self {
            next_due_ms: AtomicU64::new(now_ms.saturating_add(duration_ms(redial_delay(0)))),
            failures: AtomicU32::new(0),
            in_flight: AtomicBool::new(false),
            mismatch_reported: AtomicBool::new(false),
        }
    }

    /// Claim the right to run one re-dial now; `false` if not yet due or one
    /// is already running.
    fn claim_if_due(&self, now_ms: u64) -> bool {
        now_ms >= self.next_due_ms.load(Ordering::Relaxed)
            && self
                .in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
    }
}

fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Why a verified dial did not produce a pool.
enum DialFailure {
    /// The address answered, but as a different node than expected.
    IdentityMismatch(String),
    Other(NetError),
}

impl From<DialFailure> for NetError {
    fn from(failure: DialFailure) -> Self {
        match failure {
            DialFailure::IdentityMismatch(msg) => NetError::Protocol(msg),
            DialFailure::Other(e) => e,
        }
    }
}

/// Result of one re-dial attempt of a pool-less peer.
enum RedialOutcome {
    Installed,
    /// The peer was removed or replaced meanwhile; nothing more to do.
    Superseded,
    Failed(DialFailure),
}

impl PeerState {
    fn new(peer_id: PeerId, pool: Option<Arc<PriorityPool>>, now_ms: u64) -> Self {
        Self {
            pool,
            peer_id,
            last_activity_ms: AtomicU64::new(now_ms),
            missed_heartbeats: AtomicU32::new(0),
            redial: None,
        }
    }

    /// A pool-less placeholder that the heartbeat loop will keep re-dialing.
    fn awaiting_redial(peer_id: PeerId, now_ms: u64) -> Self {
        Self {
            redial: Some(Arc::new(RedialState::new(now_ms))),
            ..Self::new(peer_id, None, now_ms)
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
            replace_lock: tokio::sync::Mutex::new(()),
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

    /// This node's own host_id. Never present in the peer table by design.
    pub fn local_host_id(&self) -> uuid::Uuid {
        self.local_host_id
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

    /// Run `op` against the peer's current pool and record activity on success.
    ///
    /// Resolving the pool and using it are not atomic: [`Self::add_peer`] may
    /// swap in a replacement and shut the old pool down in between, so `op`
    /// then fails with [`NetError::LaneShutdown`] on a pool that is no longer
    /// registered. That says nothing about the peer, so the request is
    /// re-issued ONCE on the current pool. If the map still holds the same
    /// (dead) pool, [`Self::replace_dead_pool`] deregisters it and dials one
    /// replacement (bounded: a single attempt per request, no loop); if that
    /// fails the error is returned.
    async fn on_current_pool<T, F, Fut>(
        &self,
        host_id: uuid::Uuid,
        op: F,
    ) -> crate::error::Result<T>
    where
        F: Fn(Arc<PriorityPool>) -> Fut,
        Fut: std::future::Future<Output = crate::error::Result<T>>,
    {
        let (state, pool) = self.pool_for_peer(host_id).await?;
        let out = match op(Arc::clone(&pool)).await {
            Err(NetError::LaneShutdown) => {
                let (_, current) = self.pool_for_peer(host_id).await?;
                let current = if Arc::ptr_eq(&pool, &current) {
                    self.replace_dead_pool(host_id, &pool).await?
                } else {
                    tracing::debug!(
                        peer = %host_id,
                        "pool was replaced mid-request; re-issuing on the current pool"
                    );
                    current
                };
                let out = op(current).await?;
                // `state` may be the replaced entry; credit the live one.
                self.record_activity(host_id).await;
                return Ok(out);
            }
            other => other?,
        };
        state.record_activity(self.now_ms());
        Ok(out)
    }

    /// Deregister `dead` (a pool whose lane actors have exited) and dial a
    /// replacement through [`Self::ensure_peer`], which also verifies peer
    /// identity (t_a3df19a5).
    ///
    /// Serialised by `replace_lock` so concurrent requests that hit the same
    /// dead pool produce ONE dial: later arrivals find the pool already
    /// replaced and reuse it. Exactly one dial is attempted per call (no retry
    /// loop). The failure and the recovery are each logged once, by the
    /// request that performed the replacement. On failure the dead pool stays
    /// deregistered and the error is returned; the pool-less placeholder keeps
    /// the peer's address and is re-dialed by the heartbeat loop
    /// ([`Self::redial_peer`]) for as long as the peer stays registered.
    async fn replace_dead_pool(
        &self,
        host_id: uuid::Uuid,
        dead: &Arc<PriorityPool>,
    ) -> crate::error::Result<Arc<PriorityPool>> {
        let _guard = self.replace_lock.lock().await;
        // Errors here mean the peer was removed or deregistered meanwhile.
        let (state, current) = self.pool_for_peer(host_id).await?;
        if !Arc::ptr_eq(dead, &current) {
            return Ok(current); // another request already replaced it
        }
        let addr = state.peer_id.1;
        tracing::warn!(
            peer = %host_id, %addr,
            "peer's registered pool has dead lane actors; deregistering and re-dialing"
        );
        let placeholder = Arc::new(PeerState::awaiting_redial(state.peer_id, self.now_ms()));
        self.peers.write().await.insert(host_id, placeholder);
        dead.shutdown().await;
        if let Err(e) = self.ensure_peer(host_id, &addr.to_string()).await {
            tracing::error!(
                peer = %host_id, %addr, error = %e,
                "replacing the dead pool failed; the heartbeat loop keeps re-dialing it until the \
                 peer answers or is removed"
            );
            return Err(NetError::Protocol(format!(
                "replacing dead pool for peer {host_id} at {addr} failed: {e}"
            )));
        }
        tracing::info!(peer = %host_id, %addr, "replaced dead pool with a fresh connection");
        let (_, fresh) = self.pool_for_peer(host_id).await?;
        Ok(fresh)
    }

    /// Add a connected peer with a real connection pool.
    ///
    /// If the pool's handshake received a CQL broadcast address from the peer,
    /// it is stored for system.peers.native_address lookups.
    pub async fn add_peer(&self, peer_id: PeerId, pool: PriorityPool) {
        self.install_pool(peer_id, pool, None).await;
    }

    /// Register `pool` for `peer_id`, replacing (and shutting down) any
    /// previous pool. Returns `false` if the pool was refused and shut down.
    ///
    /// With `expect = Some(placeholder)` the install happens only if the map
    /// still holds exactly that placeholder: a re-dial that finished after the
    /// peer was removed (or replaced by a request-driven dial) must not
    /// resurrect or clobber it.
    async fn install_pool(
        &self,
        peer_id: PeerId,
        pool: PriorityPool,
        expect: Option<&Arc<PeerState>>,
    ) -> bool {
        let (host_id, _addr) = peer_id;
        if host_id == self.local_host_id {
            tracing::error!(
                peer = %host_id,
                "rejecting self before network peer admission"
            );
            pool.shutdown().await;
            return false;
        }
        // Extract the peer's broadcasts from the handshake before wrapping in Arc.
        let cql_broadcast = pool.peer_cql_broadcast().map(str::to_owned);
        let internode_broadcast = pool.peer_internode_broadcast().map(str::to_owned);
        // An unguarded install publishes the broadcasts before the peer is
        // visible. A guarded one must not touch them for a peer that may have
        // been removed, so it publishes after the guarded insert succeeds.
        if expect.is_none() {
            self.store_broadcasts(host_id, &cql_broadcast, &internode_broadcast)
                .await;
        }
        let state = Arc::new(PeerState::new(peer_id, Some(Arc::new(pool)), self.now_ms()));
        let old_pool = {
            let mut peers = self.peers.write().await;
            if let Some(expected) = expect {
                let still_registered = peers
                    .get(&host_id)
                    .is_some_and(|current| Arc::ptr_eq(current, expected));
                if !still_registered {
                    drop(peers);
                    if let Some(pool) = &state.pool {
                        pool.shutdown().await;
                    }
                    return false;
                }
            }
            peers
                .insert(host_id, Arc::clone(&state))
                .and_then(|old| old.pool.clone())
        };
        if expect.is_some() {
            self.store_broadcasts(host_id, &cql_broadcast, &internode_broadcast)
                .await;
        }
        self.listener.on_peer_connected(peer_id);
        if let Some(pool) = old_pool {
            pool.shutdown().await;
        }
        true
    }

    /// Record the CQL broadcast address (for system.peers.native_address) and
    /// the internode broadcast hostname (committed into NodeInfo.addr so the
    /// address re-resolves across container IP churn) learned in a handshake.
    async fn store_broadcasts(
        &self,
        host_id: uuid::Uuid,
        cql_broadcast: &Option<String>,
        internode_broadcast: &Option<String>,
    ) {
        if let Some(broadcast) = cql_broadcast {
            self.peer_cql_broadcasts
                .write()
                .await
                .insert(host_id, broadcast.clone());
        }
        if let Some(broadcast) = internode_broadcast {
            self.peer_internode_broadcasts
                .write()
                .await
                .insert(host_id, broadcast.clone());
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

    /// Capability bits (`handshake::CAP_*`) the peer advertised on its live
    /// outbound connection, or `None` when there is no live connection to it.
    /// A feature gated on a capability must check this before sending a
    /// message type an older peer does not know.
    pub async fn peer_capabilities(&self, host_id: uuid::Uuid) -> Option<u32> {
        let peers = self.peers.read().await;
        peers
            .get(&host_id)
            .and_then(|state| state.pool.as_ref())
            .map(|pool| pool.peer_capabilities())
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

        let pool = self
            .dial_verified(host_id, addr, resolved)
            .await
            .map_err(NetError::from)?;
        self.add_peer((host_id, resolved), pool).await;
        Ok(())
    }

    /// Dial `addr` and check that the node that answered is `host_id`.
    ///
    /// The handshake tells us who actually answered at `addr`. `host_id`
    /// came from the caller's topology view, which can be stale or wrong
    /// (a peer that has not registered yet, a re-resolved hostname that now
    /// points back at this node). Pooling the connection under the expected
    /// id would route X's requests to whoever owns the address; when that
    /// is this node, the stream handler sees `from == local` and its
    /// replies fail with "unknown peer: <local>" (t_b78e8e9a). Fail loud.
    async fn dial_verified(
        &self,
        host_id: uuid::Uuid,
        addr: &str,
        resolved: std::net::SocketAddr,
    ) -> Result<PriorityPool, DialFailure> {
        let raft_runtime = self.raft_runtime.get().cloned();
        let data_runtime = self.data_runtime.get().cloned();
        let pool = PriorityPool::connect(
            self.config.clone(),
            self.local_host_id,
            addr,
            raft_runtime,
            data_runtime,
        )
        .await
        .map_err(DialFailure::Other)?;
        let answered_by = pool.peer_host_id();
        if answered_by != host_id {
            pool.shutdown().await;
            let who = if answered_by == self.local_host_id {
                "this node itself"
            } else {
                "a different host"
            };
            return Err(DialFailure::IdentityMismatch(format!(
                "peer identity mismatch: expected {host_id} at {addr} ({resolved}) but the \
                 handshake was answered by {answered_by} ({who}); refusing to pool it"
            )));
        }
        Ok(pool)
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
        self.on_current_pool(host_id, |pool| {
            let msg = msg.clone();
            async move { pool.send(msg, lane).await }
        })
        .await
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
        self.on_current_pool(host_id, |pool| {
            let msg = msg.clone();
            async move { pool.send_with_timeout(msg, lane, timeout).await }
        })
        .await
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
        self.on_current_pool(host_id, |pool| {
            let msg = msg.clone();
            async move { pool.fire(msg, lane).await }
        })
        .await
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
            let mut to_redial: Vec<(uuid::Uuid, Arc<PeerState>)> = Vec::new();
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
                    } else if state
                        .redial
                        .as_ref()
                        .is_some_and(|redial| redial.claim_if_due(now_ms))
                    {
                        to_redial.push((*host_id, Arc::clone(state)));
                    }
                }
            } // read lock released

            // Re-dial peers whose pool could not be replaced. Each runs in its
            // own task (a dial can take a while) and `claim_if_due` admits at
            // most one per peer.
            for (host_id, placeholder) in to_redial {
                let this = Arc::clone(&self);
                task_pool.spawn(async move { this.redial_peer(host_id, placeholder).await });
            }

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

    /// One re-dial attempt for a pool-less peer, then reschedule or finish.
    ///
    /// Runs in the heartbeat loop's task pool; the caller holds the
    /// `in_flight` claim, released here. Logging is edge-only: the failure that
    /// created the placeholder was already reported by `replace_dead_pool`,
    /// each further failure is DEBUG (one ERROR for an identity mismatch per
    /// outage), and recovery is one INFO line.
    async fn redial_peer(&self, host_id: uuid::Uuid, placeholder: Arc<PeerState>) {
        let Some(redial) = placeholder.redial.as_ref() else {
            tracing::error!(peer = %host_id, "re-dial scheduled for a peer with no re-dial state");
            return;
        };
        let outcome = self.try_redial(host_id, &placeholder).await;
        match outcome {
            RedialOutcome::Installed => {
                let attempts = redial.failures.load(Ordering::Relaxed) + 1;
                tracing::info!(
                    peer = %host_id, attempts,
                    "peer re-dialed after losing its pool; connection restored"
                );
            }
            RedialOutcome::Superseded => {
                tracing::debug!(peer = %host_id, "re-dial dropped: peer removed or replaced");
            }
            RedialOutcome::Failed(failure) => {
                let failures = redial.failures.fetch_add(1, Ordering::Relaxed) + 1;
                match failure {
                    DialFailure::IdentityMismatch(msg) => {
                        if !redial.mismatch_reported.swap(true, Ordering::Relaxed) {
                            tracing::error!(
                                peer = %host_id, %msg,
                                "re-dial refused; retrying until the expected node answers"
                            );
                        }
                    }
                    DialFailure::Other(e) => {
                        tracing::debug!(peer = %host_id, failures, error = %e, "re-dial failed");
                    }
                }
                let due = self
                    .now_ms()
                    .saturating_add(duration_ms(redial_delay(failures)));
                redial.next_due_ms.store(due, Ordering::Relaxed);
            }
        }
        redial.in_flight.store(false, Ordering::Release);
    }

    async fn try_redial(&self, host_id: uuid::Uuid, placeholder: &Arc<PeerState>) -> RedialOutcome {
        if !self.is_registered(host_id, placeholder).await {
            return RedialOutcome::Superseded;
        }
        // Prefer the peer's advertised hostname: it re-resolves across IP churn.
        let target = match self.get_peer_internode_broadcast(host_id).await {
            Some(host) => host,
            None => placeholder.peer_id.1.to_string(),
        };
        inc_total_reconnect_attempts();
        let lookup = tokio::time::timeout(
            self.config.connect_timeout,
            tokio::net::lookup_host(&target),
        );
        let resolved = match lookup.await {
            Err(_) => {
                return RedialOutcome::Failed(DialFailure::Other(NetError::Timeout(format!(
                    "DNS resolution of '{target}'"
                ))))
            }
            Ok(Ok(mut addrs)) => match addrs.next() {
                Some(addr) => addr,
                None => {
                    return RedialOutcome::Failed(DialFailure::Other(NetError::Protocol(format!(
                        "peer address '{target}' resolved to no socket addresses"
                    ))))
                }
            },
            Ok(Err(e)) => return RedialOutcome::Failed(DialFailure::Other(NetError::Io(e))),
        };
        let pool = match self.dial_verified(host_id, &target, resolved).await {
            Ok(pool) => pool,
            Err(failure) => return RedialOutcome::Failed(failure),
        };
        if self
            .install_pool((host_id, resolved), pool, Some(placeholder))
            .await
        {
            RedialOutcome::Installed
        } else {
            RedialOutcome::Superseded
        }
    }

    /// Whether `placeholder` is still the registered entry for `host_id`.
    async fn is_registered(&self, host_id: uuid::Uuid, placeholder: &Arc<PeerState>) -> bool {
        self.peers
            .read()
            .await
            .get(&host_id)
            .is_some_and(|current| Arc::ptr_eq(current, placeholder))
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
            // Retire the pool's lane actors; dropping the map entry alone
            // leaves their tasks and reconnect watchers running forever
            // (t_b4d09b65).
            if let Some(pool) = &state.pool {
                pool.shutdown().await;
            }
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

    /// t_b4d09b65: removing a peer must retire its pool, or the lane-actor
    /// tasks (and their reconnect watchers) leak for the life of the process.
    #[tokio::test]
    async fn remove_peer_shuts_down_the_pool_lane_actors() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();
        let pm = PeerManager::new(
            Arc::new(config),
            uuid::Uuid::new_v4(),
            Arc::new(TestListener::new()),
        );
        pm.ensure_peer(server_id, &addr.to_string()).await.unwrap();
        let (_, pool) = pm.pool_for_peer(server_id).await.unwrap();
        assert_eq!(
            pool.all_lanes_resolved().await,
            LaneOutcome::AllConnected,
            "precondition: the pool is live while registered"
        );

        pm.remove_peer(server_id).await;

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), pool.all_lanes_resolved())
                .await
                .expect("lane status query must not hang after remove_peer"),
            LaneOutcome::AnyFailed,
            "remove_peer must shut the pool's lane actors down, not just drop the map entry"
        );
        server.shutdown(Duration::from_millis(50)).await;
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

    /// A ring entry whose address actually reaches a different node (stale
    /// address, or one that loops back to the dialer) must not be pooled under
    /// the expected host_id: requests addressed to X would be served by Y, and
    /// Y's stream replies would target the dialer's own unregistered host_id
    /// ("unknown peer: <self>", t_b78e8e9a).
    #[tokio::test]
    async fn ensure_peer_rejects_address_owned_by_another_host() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();

        let pm = PeerManager::new(
            Arc::new(config),
            uuid::Uuid::new_v4(),
            Arc::new(TestListener::new()),
        );
        let expected = uuid::Uuid::new_v4();
        let err = pm
            .ensure_peer(expected, &addr.to_string())
            .await
            .expect_err("address is owned by a different host_id");

        assert!(
            err.to_string().contains("identity mismatch"),
            "error must name the mismatch, got: {err}"
        );
        assert!(!pm.has_peer(expected), "mismatched pool must not be cached");
        assert!(!pm.has_peer(server_id));
        server.shutdown(Duration::from_millis(50)).await;
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

    /// A request that resolved the peer's pool just before `add_peer` replaced
    /// and shut it down must be re-issued on the replacement pool, not fail
    /// with `LaneShutdown`. Observed live as bursts of "lane permanently
    /// failed" during startup while reverse connections replaced pools.
    #[tokio::test]
    async fn request_in_flight_across_pool_replacement_lands_on_new_pool() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let mut servers = Vec::new();
        let mut addrs = Vec::new();
        for _ in 0..2 {
            let registry = Arc::new(HandlerRegistry::new());
            registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
            let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
            addrs.push(server.start_and_get_addr().await.unwrap().to_string());
            servers.push(server);
        }
        let pm = PeerManager::new(
            Arc::new(config),
            uuid::Uuid::new_v4(),
            Arc::new(TestListener::new()),
        );
        pm.ensure_peer(server_id, &addrs[0]).await.unwrap();

        let calls = std::sync::atomic::AtomicUsize::new(0);
        let resp = pm
            .on_current_pool(server_id, |pool| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                let (pm, addr) = (&pm, addrs[1].clone());
                async move {
                    if first {
                        // Replace the pool after this attempt already holds it.
                        pm.ensure_peer(server_id, &addr).await.unwrap();
                    }
                    pool.send(
                        Message::Ping {
                            nonce: 5,
                            sent_at: 0,
                        },
                        Lane::Data,
                    )
                    .await
                }
            })
            .await
            .expect("request must be re-issued on the replacement pool");
        assert!(matches!(resp, Message::Pong { nonce: 5, .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 2, "exactly one retry");

        pm.remove_peer(server_id).await;
        for s in servers {
            s.shutdown(Duration::from_millis(50)).await;
        }
    }

    /// t_a3df19a5: a registered pool whose lane actors died must be
    /// deregistered and replaced by a fresh dial, so the request succeeds
    /// instead of every later request failing until restart.
    #[tokio::test]
    async fn dead_registered_pool_is_replaced_and_request_succeeds() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap().to_string();
        let pm = PeerManager::new(
            Arc::new(config),
            uuid::Uuid::new_v4(),
            Arc::new(TestListener::new()),
        );
        pm.ensure_peer(server_id, &addr).await.unwrap();
        let (_, dead) = pm.pool_for_peer(server_id).await.unwrap();
        dead.shutdown().await; // stands in for a lane actor that died

        let calls = std::sync::atomic::AtomicUsize::new(0);
        let resp = pm
            .on_current_pool(server_id, |pool| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    pool.send(
                        Message::Ping {
                            nonce: 1,
                            sent_at: 0,
                        },
                        Lane::Data,
                    )
                    .await
                }
            })
            .await
            .expect("a dead pool must be replaced and the request re-issued");
        assert!(matches!(resp, Message::Pong { nonce: 1, .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 2, "exactly one retry");

        let (_, current) = pm.pool_for_peer(server_id).await.unwrap();
        assert!(
            !Arc::ptr_eq(&dead, &current),
            "the dead pool must have been deregistered and replaced"
        );
        assert_eq!(
            current.all_lanes_resolved().await,
            LaneOutcome::AllConnected
        );

        pm.remove_peer(server_id).await;
        server.shutdown(Duration::from_millis(50)).await;
    }

    /// If the replacement dial fails the request errors loudly (once, no
    /// retry loop) and the dead pool is NOT left registered.
    #[tokio::test]
    async fn failed_replacement_errors_and_deregisters_the_dead_pool() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap().to_string();
        let pm = PeerManager::new(
            Arc::new(config),
            uuid::Uuid::new_v4(),
            Arc::new(TestListener::new()),
        );
        pm.ensure_peer(server_id, &addr).await.unwrap();
        let (_, dead) = pm.pool_for_peer(server_id).await.unwrap();
        dead.shutdown().await;
        server.shutdown(Duration::from_millis(50)).await; // redial must fail

        let calls = std::sync::atomic::AtomicUsize::new(0);
        let err = pm
            .on_current_pool(server_id, |pool| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    pool.send(
                        Message::Ping {
                            nonce: 1,
                            sent_at: 0,
                        },
                        Lane::Data,
                    )
                    .await
                }
            })
            .await
            .expect_err("failed replacement must surface an error");
        assert!(
            err.to_string().contains("replac"),
            "error must say the replacement failed, got: {err}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no retry without a new pool"
        );
        assert!(
            !pm.has_live_peer(server_id),
            "the dead pool must not stay registered"
        );
        assert!(
            pm.has_peer(server_id),
            "the peer entry (and its address) is kept so a later ensure_peer can redial"
        );

        pm.remove_peer(server_id).await;
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

    // -- t_48d168ee: a peer left without a pool by a long outage must be
    // re-dialed on its own, and only while it is still wanted. These run on a
    // paused clock with real loopback sockets; `run_hops` yields real time
    // between virtual hops so kernel readiness is observed.

    use crate::slow_retry_tests::spawn_fake_peer;
    use crate::slow_retry_tests::{advance_hops, free_addr};
    use serial_test::serial;

    struct OutageFixture {
        pm: Arc<PeerManager>,
        config: NetConfig,
        peer_id: uuid::Uuid,
        addr: std::net::SocketAddr,
        listener: Arc<TestListener>,
    }

    fn registry_with_ping() -> Arc<HandlerRegistry> {
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        registry
    }

    /// A peer whose pool died while its node was down: the state a long
    /// outage leaves behind (placeholder entry, no pool, heartbeat running).
    async fn peer_left_pool_less_by_outage() -> OutageFixture {
        let addr = free_addr();
        let config = NetConfig {
            bind_addr: addr,
            ..NetConfig::default()
        };
        let peer_id = uuid::Uuid::new_v4();
        let server = Arc::new(RpcServer::new(
            config.clone(),
            peer_id,
            registry_with_ping(),
        ));
        server.start_and_get_addr().await.unwrap();
        let listener = Arc::new(TestListener::new());
        let pm = Arc::new(PeerManager::new(
            Arc::new(config.clone()),
            uuid::Uuid::new_v4(),
            listener.clone(),
        ));
        pm.ensure_peer(peer_id, &addr.to_string()).await.unwrap();
        let (_, dead) = pm.pool_for_peer(peer_id).await.unwrap();
        dead.shutdown().await;
        server.shutdown(Duration::from_millis(50)).await;

        let err = pm
            .send(
                peer_id,
                Message::Ping {
                    nonce: 1,
                    sent_at: 0,
                },
                Lane::Data,
            )
            .await
            .expect_err("the replacement dial must fail while the node is down");
        assert!(err.to_string().contains("replac"), "got: {err}");
        assert!(pm.has_peer(peer_id) && !pm.has_live_peer(peer_id));

        // Pause only now: a paused clock jumps forward whenever the runtime is
        // idle, including during the loopback handshakes above.
        tokio::time::pause();
        tokio::spawn(Arc::clone(&pm).run_heartbeat_loop());
        OutageFixture {
            pm,
            config,
            peer_id,
            addr,
            listener,
        }
    }

    async fn start_server_at(fx: &OutageFixture, id: uuid::Uuid) -> Arc<RpcServer> {
        let server = Arc::new(RpcServer::new(fx.config.clone(), id, registry_with_ping()));
        let bound = server.start_and_get_addr().await.unwrap();
        assert_eq!(bound, fx.addr);
        server
    }

    async fn live_within(pm: &PeerManager, id: uuid::Uuid, budget: Duration) -> bool {
        let mut elapsed = Duration::ZERO;
        while elapsed < budget {
            if pm.has_live_peer(id) {
                return true;
            }
            advance_hops(Duration::from_secs(1), Duration::from_secs(1)).await;
            elapsed += Duration::from_secs(1);
        }
        pm.has_live_peer(id)
    }

    /// The peer's node returns after an outage far longer than the lane
    /// fast-retry budget: nothing but the manager itself may bring it back.
    #[tokio::test]
    #[serial(net_reconnect_counters)]
    async fn pool_less_peer_is_redialed_when_its_node_returns() {
        let fx = peer_left_pool_less_by_outage().await;

        advance_hops(Duration::from_secs(20 * 60), Duration::from_secs(5)).await;
        assert!(!fx.pm.has_live_peer(fx.peer_id), "node is still down");

        let server = start_server_at(&fx, fx.peer_id).await;
        assert!(
            live_within(&fx.pm, fx.peer_id, Duration::from_secs(120)).await,
            "peer was not re-dialed within two minutes of its node returning"
        );
        let resp = fx
            .pm
            .send(
                fx.peer_id,
                Message::Ping {
                    nonce: 5,
                    sent_at: 0,
                },
                Lane::Data,
            )
            .await
            .unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 5, .. }));

        fx.pm.remove_peer(fx.peer_id).await;
        server.shutdown(Duration::from_millis(50)).await;
    }

    /// A peer removed on purpose must neither be re-dialed nor resurrected.
    #[tokio::test]
    #[serial(net_reconnect_counters)]
    async fn removed_pool_less_peer_is_not_redialed_or_resurrected() {
        let fx = peer_left_pool_less_by_outage().await;
        advance_hops(Duration::from_secs(60), Duration::from_secs(5)).await;

        fx.pm.remove_peer(fx.peer_id).await;
        let connected = fx.listener.connected_count.load(Ordering::Relaxed);

        // The node is back and would accept (and handshake) any dial. Counting
        // its accepts is peer-scoped, so unlike the process-wide attempt
        // counter it cannot be moved by other tests running in parallel.
        let node = spawn_fake_peer(fx.addr, fx.peer_id, Duration::ZERO, None);
        advance_hops(Duration::from_secs(10 * 60), Duration::from_secs(5)).await;

        assert!(!fx.pm.has_peer(fx.peer_id), "removed peer came back");
        assert_eq!(node.accepted(), 0, "a removed peer was dialed");
        assert_eq!(
            fx.listener.connected_count.load(Ordering::Relaxed),
            connected,
            "a removed peer was announced as connected again"
        );
    }

    /// The address now answers as a different node: it is dialed, refused, and
    /// never pooled under the expected id.
    #[tokio::test]
    #[serial(net_reconnect_counters)]
    async fn redial_refuses_an_address_owned_by_another_node() {
        let fx = peer_left_pool_less_by_outage().await;

        let impostor = spawn_fake_peer(fx.addr, uuid::Uuid::new_v4(), Duration::ZERO, None);
        advance_hops(Duration::from_secs(5 * 60), Duration::from_secs(5)).await;

        // Peer-scoped: the impostor completed handshakes, so the manager did
        // dial the address and then refused what answered.
        assert!(
            impostor.accepted() >= 1,
            "the manager never dialed the address, so nothing was refused"
        );
        assert!(
            !fx.pm.has_live_peer(fx.peer_id),
            "a different node was pooled under the expected peer id"
        );
        assert!(fx.pm.has_peer(fx.peer_id), "the peer entry must be kept");
        fx.pm.remove_peer(fx.peer_id).await;
    }

    /// A re-dial that completes after its peer was removed must be refused:
    /// no resurrected entry, no `on_peer_connected`, no stale broadcasts.
    #[tokio::test]
    async fn install_after_removal_is_refused_and_leaves_no_trace() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let server = Arc::new(RpcServer::new(
            config.clone(),
            server_id,
            registry_with_ping(),
        ));
        let addr = server.start_and_get_addr().await.unwrap();
        let listener = Arc::new(TestListener::new());
        let pm = PeerManager::new(
            Arc::new(config.clone()),
            uuid::Uuid::new_v4(),
            listener.clone(),
        );

        let placeholder = Arc::new(PeerState::awaiting_redial((server_id, addr), pm.now_ms()));
        pm.peers
            .write()
            .await
            .insert(server_id, Arc::clone(&placeholder));
        // The dial finishes ...
        let pool = PriorityPool::connect(
            Arc::new(config),
            uuid::Uuid::new_v4(),
            &addr.to_string(),
            None,
            None,
        )
        .await
        .unwrap();
        // ... but the peer was removed while it was in flight.
        pm.remove_peer(server_id).await;
        let disconnected = listener.disconnected_count.load(Ordering::Relaxed);

        let installed = pm
            .install_pool((server_id, addr), pool, Some(&placeholder))
            .await;

        assert!(!installed, "install must be refused for a removed peer");
        assert!(!pm.has_peer(server_id), "removed peer was resurrected");
        assert_eq!(listener.connected_count.load(Ordering::Relaxed), 0);
        assert_eq!(
            listener.disconnected_count.load(Ordering::Relaxed),
            disconnected
        );
        assert!(pm.get_peer_internode_broadcast(server_id).await.is_none());
        assert!(pm.get_peer_cql_broadcast(server_id).await.is_none());
        server.shutdown(Duration::from_millis(50)).await;
    }
}
