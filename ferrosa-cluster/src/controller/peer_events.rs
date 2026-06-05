//! PeerEventListener and InboundPeerCallback trait implementations.

use std::sync::Arc;

use ferrosa_net::peer::PeerEventListener;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::InboundPeerCallback;

use crate::hints::delivery::HintDeliveryTask;
use crate::mode::DeploymentMode;

use super::peer_plan::{self, PeerConnectPlanInput, PeerEventAction, PeerRecoveredPlanInput};
use super::ModeController;

pub(super) fn track_connected_peer(
    peers: &mut Vec<(uuid::Uuid, std::net::SocketAddr)>,
    host_id: uuid::Uuid,
    addr: std::net::SocketAddr,
    capacity: usize,
) {
    if let Some((_, existing_addr)) = peers.iter_mut().find(|(id, _)| *id == host_id) {
        *existing_addr = addr;
        return;
    }

    if peers.len() >= capacity {
        tracing::warn!(
            cap = capacity,
            "connected_peers at capacity — evicting oldest entry"
        );
        peers.remove(0);
    }
    peers.push((host_id, addr));
}

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

#[cfg(test)]
pub(super) fn should_refresh_outbound_peer_for_inbound(
    known_addr: Option<&str>,
    live: bool,
    inbound_reverse_addr: std::net::SocketAddr,
) -> bool {
    should_refresh_outbound_peer_for_inbound_impl(known_addr, live, inbound_reverse_addr)
}

#[cfg(test)]
pub(super) fn should_install_refreshed_outbound_peer(
    observed_addr: Option<&str>,
    current_addr: Option<&str>,
) -> bool {
    should_install_refreshed_outbound_peer_impl(observed_addr, current_addr)
}

fn should_refresh_outbound_peer_for_inbound_impl(
    known_addr: Option<&str>,
    live: bool,
    inbound_reverse_addr: std::net::SocketAddr,
) -> bool {
    let desired = inbound_reverse_addr.to_string();
    !matches!((known_addr, live), (Some(known), true) if known == desired)
}

fn should_install_refreshed_outbound_peer_impl(
    observed_addr: Option<&str>,
    current_addr: Option<&str>,
) -> bool {
    observed_addr == current_addr
}

async fn refresh_outbound_peer_for_inbound(
    peer_manager: Arc<ferrosa_net::peer::PeerManager>,
    net_config: Arc<ferrosa_net::config::NetConfig>,
    local_host_id: uuid::Uuid,
    raft_runtime: Option<Arc<tokio::runtime::Runtime>>,
    data_runtime: Option<Arc<tokio::runtime::Runtime>>,
    host_id: uuid::Uuid,
    reverse_addr: std::net::SocketAddr,
) {
    let known_addr = peer_manager.peer_addr(host_id).await;
    let live = peer_manager.has_live_peer(host_id);
    if !should_refresh_outbound_peer_for_inbound_impl(known_addr.as_deref(), live, reverse_addr) {
        return;
    }

    match ferrosa_net::pool::PriorityPool::connect(
        net_config,
        local_host_id,
        &reverse_addr.to_string(),
        raft_runtime,
        data_runtime,
    )
    .await
    {
        Ok(pool) => {
            let current_addr = peer_manager.peer_addr(host_id).await;
            if !should_install_refreshed_outbound_peer_impl(
                known_addr.as_deref(),
                current_addr.as_deref(),
            ) {
                tracing::info!(
                    peer = %host_id,
                    observed_addr = ?known_addr,
                    current_addr = ?current_addr,
                    attempted_addr = %reverse_addr,
                    "skipping stale inbound peer refresh; peer address changed while connecting"
                );
                return;
            }
            peer_manager.add_peer((host_id, reverse_addr), pool).await;
            tracing::info!(
                peer = %host_id,
                previous_addr = ?known_addr,
                new_addr = %reverse_addr,
                "inbound peer address refreshed outbound connection"
            );
        }
        Err(e) => {
            tracing::warn!(
                peer = %host_id,
                previous_addr = ?known_addr,
                new_addr = %reverse_addr,
                %e,
                "failed to refresh outbound connection from inbound peer address"
            );
        }
    }
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
                    internode_broadcast,
                } => {
                    tracing::info!(peer = %host_id, "new peer connected in cluster mode, triggering join");
                    if self.trigger_cluster_join(host_id, addr, cql_broadcast, internode_broadcast)
                    {
                        join_enqueued_invite = Some(host_id);
                    }
                }
                PeerEventAction::SendClusterInvite { host_id, force: _ } => {
                    invite_sent = true;
                    self.send_cluster_invite_to(host_id);
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
            track_connected_peer(&mut peers, host_id, addr, super::MAX_CONNECTED_PEERS);
        }

        // Hold the transition guard across mode-check-and-transition to prevent
        // two simultaneous peer connections from both triggering transition_to_pair.
        let guard_start = std::time::Instant::now();
        let _guard = self.transition_guard.lock();
        let current_mode = **self.mode.load();
        let all_peers = self.connected_peers.lock().clone();
        let (cql_broadcast, internode_broadcast) =
            if matches!(current_mode, DeploymentMode::Cluster) {
                let pm = self.peer_manager.load();
                let pm = pm.as_ref().as_ref();
                (
                    pm.and_then(|pm| pm.get_peer_cql_broadcast_sync(host_id)),
                    pm.and_then(|pm| pm.get_peer_internode_broadcast_sync(host_id)),
                )
            } else {
                (None, None)
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
            internode_broadcast,
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
    fn on_inbound_peer(
        &self,
        peer_id: PeerId,
        cql_broadcast: Option<String>,
        internode_broadcast: Option<String>,
    ) {
        let (host_id, addr) = peer_id;
        let reverse_addr = std::net::SocketAddr::new(addr.ip(), self.net_config.bind_addr.port());
        tracing::info!(peer = %host_id, %addr, ?cql_broadcast, ?internode_broadcast, "inbound peer connected");

        // Store the peer's CQL broadcast address (from handshake) in PeerManager
        // so system.peers can return it instead of the container-internal IP.
        if let Some(ref broadcast) = cql_broadcast {
            if let Some(pm) = &**self.peer_manager.load() {
                let pm = pm.clone();
                let hid = host_id;
                let broadcast = broadcast.clone();
                ferrosa_net::task_pool::TaskPool::current("peer-cql-broadcast").spawn(async move {
                    pm.set_peer_cql_broadcast(hid, broadcast).await;
                });
            }
        }

        // Store the peer's internode broadcast hostname so the outbound
        // on_peer_connected path (which reads it back) and committed membership
        // use the re-resolvable hostname instead of a frozen IP.
        if let Some(ref broadcast) = internode_broadcast {
            if let Some(pm) = &**self.peer_manager.load() {
                let pm = pm.clone();
                let hid = host_id;
                let broadcast = broadcast.clone();
                ferrosa_net::task_pool::TaskPool::current("peer-internode-broadcast").spawn(
                    async move {
                        pm.set_peer_internode_broadcast(hid, broadcast).await;
                    },
                );
            }
        }
        if let Some(pm) = &**self.peer_manager.load() {
            let pm = pm.clone();
            let net_config = self.net_config.clone();
            let local_host_id = self.local_host_id;
            let raft_runtime = self.raft_runtime.get().cloned();
            let data_runtime = self.data_runtime.get().cloned();
            self.spawn_tracked(refresh_outbound_peer_for_inbound(
                pm,
                net_config,
                local_host_id,
                raft_runtime,
                data_runtime,
                host_id,
                reverse_addr,
            ));
        }

        // Track this peer
        {
            let mut peers = self.connected_peers.lock();
            track_connected_peer(
                &mut peers,
                host_id,
                reverse_addr,
                super::MAX_CONNECTED_PEERS,
            );
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
            internode_broadcast,
        });
        self.execute_peer_event_plan(plan, &all_peers, true);
    }
}
