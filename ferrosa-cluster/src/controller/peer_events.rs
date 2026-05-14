//! PeerEventListener and InboundPeerCallback trait implementations.

use std::sync::Arc;

use ferrosa_net::peer::PeerEventListener;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::InboundPeerCallback;

use crate::hints::delivery::HintDeliveryTask;
use crate::mode::DeploymentMode;

use super::peer_plan::{self, PeerConnectPlanInput, PeerEventAction, PeerRecoveredPlanInput};
use super::ModeController;

#[cfg(test)]
pub(super) fn should_send_cluster_invite_after_join_trigger(
    join_enqueued: bool,
    last_sent: Option<std::time::Instant>,
    now: std::time::Instant,
) -> bool {
    // Join proposals and ClusterInvite delivery solve different problems.
    // A recreated existing member may already be present in the ring with
    // current metadata, so trigger_cluster_join() intentionally dedupes the
    // JoinNode/UpdateNodeInfo proposal. That peer can still be in pair mode,
    // though, and needs a fresh ClusterInvite to transition into cluster mode
    // and register Raft/Bulk/Data handlers.
    peer_plan::should_send_cluster_invite(join_enqueued, last_sent, now)
}

impl ModeController {
    fn execute_peer_event_plan(
        &self,
        actions: Vec<PeerEventAction>,
        all_peers_after_track: &[(uuid::Uuid, std::net::SocketAddr)],
        pair_transition_enters_cluster: bool,
    ) {
        let mut join_enqueued_invite = None;
        let mut invite_sent = false;

        for action in actions {
            match action {
                PeerEventAction::TrackPeer { .. } => {}
                PeerEventAction::TransitionToPair {
                    host_id,
                    addr,
                    inbound,
                } => self.transition_to_pair(host_id, addr, inbound),
                PeerEventAction::TransitionToForming => {
                    if pair_transition_enters_cluster {
                        self.transition_to_cluster(all_peers_after_track.to_vec());
                    } else {
                        self.transition_to_forming(all_peers_after_track.to_vec());
                    }
                }
                PeerEventAction::TriggerClusterJoin {
                    host_id,
                    addr,
                    cql_broadcast,
                } => {
                    tracing::info!(peer = %host_id, "new peer connected in cluster mode, triggering join");
                    if self.trigger_cluster_join(host_id, addr, cql_broadcast) {
                        join_enqueued_invite = Some(host_id);
                    }
                }
                PeerEventAction::SendClusterInvite { host_id, force } => {
                    invite_sent = true;
                    if force {
                        self.send_cluster_invite_to(host_id);
                    } else {
                        self.send_cluster_invite_to(host_id);
                    }
                }
                PeerEventAction::RestoreClusterMode => {
                    tracing::info!("quorum restored — transitioning back to Cluster");
                    self.mode.store(Arc::new(DeploymentMode::Cluster));
                    // Raft will resume accepting writes once leader is re-elected.
                    // Write path and DDL path are restored by the Raft leader
                    // election callback (already in transition_to_cluster's async task).
                }
                PeerEventAction::DeliverHints { host_id } => self.spawn_hint_delivery(host_id),
            }
        }

        if let Some(host_id) = join_enqueued_invite {
            if !invite_sent {
                self.send_cluster_invite_to(host_id);
            }
        }
    }

    fn spawn_hint_delivery(&self, peer_id: uuid::Uuid) {
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
}

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
        let all_peers = self.connected_peers.lock().clone();
        let cql_broadcast = if matches!(current_mode, DeploymentMode::Cluster) {
            self.peer_manager
                .load()
                .as_ref()
                .as_ref()
                .and_then(|pm| pm.get_peer_cql_broadcast_sync(host_id))
        } else {
            None
        };
        let plan = peer_plan::plan_peer_connected(PeerConnectPlanInput {
            mode: current_mode,
            host_id,
            addr,
            inbound: false,
            connected_peers_after_track: all_peers.clone(),
            committed_cluster_size: self
                .committed_cluster_size
                .load(std::sync::atomic::Ordering::Relaxed),
            join_enqueued: false,
            last_invite_sent: self.recent_reconnect_invites.lock().get(&host_id).copied(),
            now: std::time::Instant::now(),
            cql_broadcast,
        });
        self.execute_peer_event_plan(plan, &all_peers, false);

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
        tracing::info!(%peer_id, "peer recovered — planning invite and hint delivery");

        let pending_hint_count = self.hint_store.pending_count(peer_id);
        let peer_manager_available = self.peer_manager.load().is_some();
        let plan = peer_plan::plan_peer_recovered(PeerRecoveredPlanInput {
            mode: **self.mode.load(),
            host_id: peer_id,
            pending_hint_count,
            peer_manager_available,
        });
        self.execute_peer_event_plan(plan, &[], false);
    }

    fn on_peer_failed(&self, peer_id: uuid::Uuid) {
        tracing::warn!(%peer_id, "peer failed — excluding from replica set");
    }
}

impl InboundPeerCallback for ModeController {
    fn on_inbound_peer(&self, peer_id: PeerId, cql_broadcast: Option<String>) {
        let (host_id, addr) = peer_id;
        let reverse_addr = std::net::SocketAddr::new(addr.ip(), self.net_config.bind_addr.port());
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
        let all_peers = self.connected_peers.lock().clone();
        let plan = peer_plan::plan_peer_connected(PeerConnectPlanInput {
            mode: current_mode,
            host_id,
            addr: if matches!(current_mode, DeploymentMode::Cluster) {
                reverse_addr
            } else {
                addr
            },
            inbound: true,
            connected_peers_after_track: all_peers.clone(),
            committed_cluster_size: self
                .committed_cluster_size
                .load(std::sync::atomic::Ordering::Relaxed),
            join_enqueued: false,
            last_invite_sent: self.recent_reconnect_invites.lock().get(&host_id).copied(),
            now: std::time::Instant::now(),
            cql_broadcast,
        });
        self.execute_peer_event_plan(plan, &all_peers, true);
    }
}
