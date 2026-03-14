use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::message::Message;
use crate::pool::PriorityPool;
use crate::rpc::handler::PeerId;

/// Subscribe to peer lifecycle events.
pub trait PeerEventListener: Send + Sync {
    fn on_peer_connected(&self, peer: PeerId);
    fn on_peer_disconnected(&self, peer: PeerId);
    fn on_peer_suspected(&self, peer: PeerId);
}

/// Manages all peer connections and runs failure detection.
pub struct PeerManager {
    config: Arc<NetConfig>,
    #[allow(dead_code)]
    local_host_id: uuid::Uuid,
    peers: RwLock<HashMap<uuid::Uuid, PeerState>>,
    listener: Arc<dyn PeerEventListener>,
}

struct PeerState {
    pool: Option<PriorityPool>, // None for unit-test entries (add_peer_entry)
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
        }
    }

    /// Add a connected peer with a real connection pool.
    pub async fn add_peer(&self, peer_id: PeerId, pool: PriorityPool) {
        let (host_id, _addr) = peer_id;
        let state = PeerState {
            pool: Some(pool),
            peer_id,
            last_heartbeat: tokio::time::Instant::now(),
            missed_heartbeats: 0,
        };
        self.peers.write().await.insert(host_id, state);
        self.listener.on_peer_connected(peer_id);
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
    pub async fn send(
        &self,
        host_id: uuid::Uuid,
        msg: Message,
        lane: Lane,
    ) -> crate::error::Result<Message> {
        let peers = self.peers.read().await;
        let state = peers
            .get(&host_id)
            .ok_or_else(|| crate::error::NetError::Protocol(format!("unknown peer: {host_id}")))?;
        match &state.pool {
            Some(pool) => pool.send(msg, lane).await,
            None => Err(crate::error::NetError::Protocol(
                "no connection pool".into(),
            )),
        }
    }

    /// Heartbeat loop: sends Ping at configured interval, marks peers suspected
    /// after 3 missed heartbeats.
    pub async fn run_heartbeat_loop(&self) {
        let mut interval = tokio::time::interval(self.config.heartbeat_interval);
        loop {
            interval.tick().await;

            let mut peers = self.peers.write().await;
            let mut suspected = Vec::new();

            for (host_id, state) in peers.iter_mut() {
                let elapsed = state.last_heartbeat.elapsed();
                if elapsed >= self.config.heartbeat_timeout {
                    state.missed_heartbeats += 1;
                    if state.missed_heartbeats >= 3 {
                        tracing::warn!(
                            %host_id,
                            "peer suspected dead: {} missed heartbeats",
                            state.missed_heartbeats
                        );
                        suspected.push(state.peer_id);
                    }
                } else {
                    state.missed_heartbeats = 0;
                }

                // Send Ping via raft lane (fire-and-forget)
                if let Some(pool) = &state.pool {
                    let nonce = rand::random();
                    let _ = pool.fire(Message::Ping { nonce }, Lane::Raft).await;
                }
            }

            // Notify listener outside the iteration
            drop(peers);
            for peer_id in suspected {
                self.listener.on_peer_suspected(peer_id);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::NetConfig;

    struct TestListener {
        connected_count: AtomicUsize,
        suspected_count: AtomicUsize,
        disconnected_count: AtomicUsize,
    }

    impl TestListener {
        fn new() -> Self {
            Self {
                connected_count: AtomicUsize::new(0),
                suspected_count: AtomicUsize::new(0),
                disconnected_count: AtomicUsize::new(0),
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
}
