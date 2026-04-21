//! PeerEventListener and InboundPeerCallback trait implementations.

use std::sync::Arc;

use ferrosa_net::peer::PeerEventListener;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::InboundPeerCallback;

use crate::hints::delivery::HintDeliveryTask;
use crate::mode::DeploymentMode;

use super::ModeController;

impl PeerEventListener for ModeController {
    fn on_peer_connected(&self, peer: PeerId) {
        let (host_id, addr) = peer;
        tracing::info!(peer = %host_id, %addr, "peer connected");

        // Track this peer
        {
            let mut peers = self.connected_peers.lock();
            if !peers.iter().any(|(id, _)| *id == host_id) {
                if peers.len() >= super::MAX_CONNECTED_PEERS {
                    tracing::warn!(
                        cap = super::MAX_CONNECTED_PEERS,
                        "connected_peers at capacity — evicting oldest entry"
                    );
                    peers.remove(0);
                }
                peers.push((host_id, addr));
            }
        }

        // Hold the transition guard across mode-check-and-transition to prevent
        // two simultaneous peer connections from both triggering transition_to_pair.
        let guard_start = std::time::Instant::now();
        let _guard = self.transition_guard.lock();
        let current_mode = **self.mode.load();
        match current_mode {
            DeploymentMode::Standalone => {
                self.transition_to_pair(host_id, addr, false);
            }
            DeploymentMode::Pair => {
                // 2nd peer connecting while in pair mode → enter forming state
                let all_peers = self.connected_peers.lock().clone();
                if all_peers.len() >= 2 {
                    self.transition_to_forming(all_peers);
                }
            }
            DeploymentMode::Cluster => {
                tracing::info!(peer = %host_id, "new peer connected in cluster mode, triggering join");
                let cql_broadcast = self
                    .peer_manager
                    .load()
                    .as_ref()
                    .as_ref()
                    .and_then(|pm| pm.get_peer_cql_broadcast_sync(host_id));
                self.trigger_cluster_join(host_id, addr, cql_broadcast);
            }
            DeploymentMode::Forming => {
                tracing::info!(peer = %host_id, "peer connected during formation");
            }
            DeploymentMode::DegradedPair => {
                tracing::info!(peer = %host_id, "peer reconnected in degraded pair mode");
            }
            DeploymentMode::DegradedCluster => {
                // Check if quorum is restored using committed cluster size
                // (not the dynamic connected count) to prevent false quorum
                // restoration after network partitions.
                let connected = self.connected_peers.lock().len();
                let committed = self
                    .committed_cluster_size
                    .load(std::sync::atomic::Ordering::Relaxed);
                let total = if committed > 0 {
                    committed
                } else {
                    connected + 1
                };
                let quorum = (total / 2) + 1;
                if connected + 1 >= quorum {
                    tracing::info!(
                        connected,
                        committed_size = total,
                        quorum,
                        "quorum restored — transitioning back to Cluster"
                    );
                    self.mode.store(Arc::new(DeploymentMode::Cluster));
                    // Raft will resume accepting writes once leader is re-elected.
                    // Write path and DDL path are restored by the Raft leader
                    // election callback (already in transition_to_cluster's async task).
                }
            }
        }

        // Record how long the transition guard was held.
        drop(_guard);
        self.contention_metrics
            .record_guard_hold(guard_start.elapsed());
    }

    fn on_peer_disconnected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(peer = %host_id, "peer disconnected");

        // Remove from tracked peers
        {
            let mut peers = self.connected_peers.lock();
            peers.retain(|(id, _)| *id != host_id);
        }

        // Hold the transition guard to prevent a disconnect handler from racing
        // with a connect handler during mode transition.
        let _guard = self.transition_guard.lock();
        let current_mode = **self.mode.load();
        match current_mode {
            DeploymentMode::Pair => {
                self.transition_to_degraded();
            }
            DeploymentMode::Cluster => {
                // Check if remaining connected peers can form a quorum.
                // Use committed cluster size (not dynamic connected count)
                // to prevent premature quorum claims after partitions.
                let connected = self.connected_peers.lock().len();
                let committed = self
                    .committed_cluster_size
                    .load(std::sync::atomic::Ordering::Relaxed);
                let total = if committed > 0 {
                    committed
                } else {
                    connected + 1
                };
                let quorum = (total / 2) + 1;
                if connected + 1 < quorum {
                    tracing::warn!(
                        connected,
                        committed_size = total,
                        quorum,
                        "quorum lost — transitioning to DegradedCluster"
                    );
                    self.mode.store(Arc::new(DeploymentMode::DegradedCluster));
                    self.write_path
                        .store(Arc::new(crate::write_path::WritePath::unavailable()));
                    // DDL unavailable without quorum
                    self.ddl_path
                        .store(Arc::new(crate::ddl_path::DdlPath::Unavailable));
                }
                // If quorum intact, Raft handles it — no mode change needed.
            }
            _ => {}
        }
    }

    fn on_peer_suspected(&self, peer: PeerId) {
        let (host_id, _addr) = peer;
        tracing::warn!(peer = %host_id, "peer suspected dead (not transitioning)");
    }

    fn on_peer_recovered(&self, peer_id: uuid::Uuid) {
        tracing::info!(%peer_id, "peer recovered — scheduling hint delivery");

        // Only replay hints if there are any pending for this peer.
        if self.hint_store.pending_count(peer_id) == 0 {
            return;
        }

        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::warn!(%peer_id, "hint delivery skipped: peer_manager not set");
                return;
            }
        };

        let hint_store = self.hint_store.clone();
        let hint_config = self.hint_config.clone();

        self.spawn_tracked(async move {
            HintDeliveryTask::run(peer_id, hint_store, peer_manager, &hint_config).await;
        });
    }

    fn on_peer_failed(&self, peer_id: uuid::Uuid) {
        tracing::warn!(%peer_id, "peer failed — excluding from replica set");
    }
}

impl InboundPeerCallback for ModeController {
    fn on_inbound_peer(&self, peer_id: PeerId, cql_broadcast: Option<String>) {
        let (host_id, addr) = peer_id;
        tracing::info!(peer = %host_id, %addr, ?cql_broadcast, "inbound peer connected");

        // Store the peer's CQL broadcast address (from handshake) in PeerManager
        // so system.peers can return it instead of the container-internal IP.
        if let Some(ref broadcast) = cql_broadcast {
            if let Some(pm) = &**self.peer_manager.load() {
                let pm = pm.clone();
                let hid = host_id;
                let broadcast = broadcast.clone();
                tokio::spawn(async move {
                    pm.set_peer_cql_broadcast(hid, broadcast).await;
                });
            }
        }

        // Track this peer
        {
            let mut peers = self.connected_peers.lock();
            if !peers.iter().any(|(id, _)| *id == host_id) {
                if peers.len() >= super::MAX_CONNECTED_PEERS {
                    tracing::warn!(
                        cap = super::MAX_CONNECTED_PEERS,
                        "connected_peers at capacity — evicting oldest entry"
                    );
                    peers.remove(0);
                }
                peers.push((host_id, addr));
            }
        }

        let _guard = self.transition_guard.lock();
        let current_mode = **self.mode.load();
        match current_mode {
            DeploymentMode::Standalone => {
                // Inbound connection — we need a reverse outbound pool for sends.
                self.transition_to_pair(host_id, addr, true);
            }
            DeploymentMode::Pair => {
                let all_peers = self.connected_peers.lock().clone();
                if all_peers.len() >= 2 {
                    self.transition_to_cluster(all_peers);
                }
            }
            DeploymentMode::Cluster => {
                tracing::info!(peer = %host_id, "new inbound peer in cluster mode, triggering join");
                self.trigger_cluster_join(host_id, addr, cql_broadcast);
            }
            DeploymentMode::Forming => {
                tracing::info!(peer = %host_id, "inbound peer during formation");
            }
            DeploymentMode::DegradedPair | DeploymentMode::DegradedCluster => {
                tracing::info!(peer = %host_id, "inbound peer in degraded mode");
            }
        }
    }
}
