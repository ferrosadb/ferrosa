//! Operator commands: force_promote, switchover, transition_to_degraded.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

use crate::ddl_path::DdlPath;
use crate::error::{ClusterError, Result};
use crate::mode::DeploymentMode;
use crate::write_path::WritePath;

use super::{ClusterStateHolder, ModeController};

impl ModeController {
    /// Force-promote this node to standalone primary.
    ///
    /// Use when the peer is unreachable and the operator wants to resume writes.
    /// Subsequent peer reconnection will auto re-pair with this node as primary.
    pub fn force_promote(&self) -> Result<()> {
        let epoch = self.promote_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.write_path
            .store(Arc::new(WritePath::direct(self.storage.clone())));
        self.ddl_path.store(Arc::new(DdlPath::Direct {
            schema: self.schema.clone(),
            engine: self.storage.clone(),
        }));
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Standalone));
        self.force_mode_override(
            DeploymentMode::Standalone,
            "force promote to standalone primary",
        );
        self.force_promoted.store(true, Ordering::Release);
        *self.pair_context.lock() = None;
        self.connected_peers.lock().clear();
        tracing::info!(epoch, "force promoted to standalone primary");
        Ok(())
    }

    /// Operator downgrade: take this node out of a Raft cluster and into pair
    /// mode, replicating to one named peer.
    ///
    /// The automatic lifecycle never does this. `DeploymentMode` already
    /// documents that a multi-node Raft cluster does not become a pair again,
    /// because the shapes commit differently and silently swapping a quorum for
    /// a point-to-point primary accepts writes a quorum would have refused. So
    /// the move is an explicit operator decision, in the same class as
    /// [`Self::force_promote`] -- and like that one it is reachable only through
    /// this deliberate call, never from a timeout or a peer event.
    ///
    /// Refused, leaving the node untouched, when (P0-5):
    ///
    /// - no peer is named. The old action paired with `connected_peers.first()`,
    ///   so the replication target was whichever peer connected first.
    /// - any Raft group is still running on this node. While Raft runs the node
    ///   is a voter and a replica; installing the pair write path on top commits
    ///   writes a quorum never saw while the cluster still counts this node.
    ///   That is split brain. There is no supported way to stop Raft and shrink
    ///   membership to the named peer yet, so in a live cluster this refuses
    ///   outright (fail loud, never fake).
    /// - the token ring still holds a member other than this node and the named
    ///   peer: that member is still a replica of ranges this node would write
    ///   without it.
    /// - the named peer is not currently connected: a pair needs somewhere to
    ///   replicate. The message names the connected peers.
    /// - the T-300 jsonb guard refuses leaving standalone (checked inside the
    ///   pair transition, so it cannot be bypassed by going through here).
    ///
    /// On success the real pair machinery is installed (coordinator, DDL path,
    /// write path, pair state), not merely the mode -- so the node genuinely
    /// behaves as a pair afterwards rather than reporting pair mode while still
    /// writing through the cluster path.
    pub fn downgrade_to_pair(&self, named_peer: Option<Uuid>) -> Result<Uuid> {
        let Some(named_peer) = named_peer else {
            return Err(ClusterError::ModeTransitionRejected(
                "downgrade to pair requires a named peer to replicate to; pass the \
                 peer's host id"
                    .into(),
            ));
        };
        let running_groups = self.raft_groups.load().len();
        if running_groups > 0 {
            return Err(ClusterError::ModeTransitionRejected(format!(
                "downgrade to pair refused: Raft is running ({running_groups} group(s)); \
                 this node is still a voter and a replica, so committing point-to-point \
                 would split the cluster. Decommission the other members first; an \
                 in-place Raft shutdown is not supported yet"
            )));
        }
        if let Some(ring) = self.token_ring() {
            let allowed = [
                crate::raft::uuid_to_node_id(self.local_host_id),
                crate::raft::uuid_to_node_id(named_peer),
            ];
            let others: Vec<String> = ring
                .node_ids()
                .into_iter()
                .filter(|id| !allowed.contains(id))
                .map(|id| {
                    ring.get_node(id)
                        .map(|n| n.host_id.to_string())
                        .unwrap_or_else(|| format!("node_id {id}"))
                })
                .collect();
            if !others.is_empty() {
                return Err(ClusterError::ModeTransitionRejected(format!(
                    "downgrade to pair refused: the token ring still holds members other \
                     than this node and {named_peer}: {others:?}. They are still replicas; \
                     decommission them first"
                )));
            }
        }
        let peers = self.connected_peers.lock().clone();
        let Some((peer_host_id, peer_addr)) =
            peers.iter().copied().find(|(id, _)| *id == named_peer)
        else {
            let connected: Vec<String> = peers.iter().map(|(id, _)| id.to_string()).collect();
            return Err(ClusterError::ModeTransitionRejected(format!(
                "downgrade to pair requires the named peer {named_peer} to be a connected \
                 peer to replicate to; connected peers: {connected:?} — is the intended \
                 peer running and reachable?"
            )));
        };
        {
            // Hold `transition_guard` across the transition so the mode cannot
            // move underneath the check-and-install in `transition_to_pair`.
            let _guard = self.transition_guard.lock();
            self.transition_to_pair_operator_override(peer_host_id, peer_addr);
        }
        if **self.mode.load() != DeploymentMode::Pair {
            return Err(ClusterError::ModeTransitionRejected(
                "the pair transition was refused (see the node log); the node is                  unchanged"
                    .into(),
            ));
        }
        tracing::warn!(
            %peer_host_id,
            "OPERATOR ACTION: downgraded from cluster to pair mode. This node now              replicates point-to-point and no longer commits through Raft."
        );
        Ok(peer_host_id)
    }

    /// Returns the current promote epoch (Lamport counter).
    ///
    /// Used during reconnect handshake: the node with the higher epoch
    /// becomes primary. If equal, UUID comparison breaks the tie.
    pub fn promote_epoch(&self) -> u64 {
        self.promote_epoch.load(Ordering::SeqCst)
    }

    /// Set the promote epoch (used when receiving a higher epoch from a peer).
    pub fn set_promote_epoch(&self, epoch: u64) {
        self.promote_epoch.store(epoch, Ordering::SeqCst);
    }

    /// Initiate switchover: swap primary/secondary roles.
    ///
    /// Must be called on the current primary. Both nodes must be connected.
    pub async fn switchover(&self) -> Result<()> {
        let (role_arc, peer_host_id) = {
            let ctx = self.pair_context.lock();
            let ctx = ctx.as_ref().ok_or(ClusterError::ModeTransitionRejected(
                "switchover requires pair mode; current node is standalone".into(),
            ))?;
            (ctx.role.clone(), ctx.peer_host_id)
        };

        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                return Err(ClusterError::ModeTransitionRejected(
                    "peer manager not initialized; peer may be disconnected".into(),
                ));
            }
        };

        crate::pair::switchover::initiate_switchover(
            &peer_manager,
            self.local_host_id,
            peer_host_id,
            &role_arc,
        )
        .await
    }

    /// Transition to degraded pair state: writes unavailable, stale reads work.
    ///
    /// Preserves pair context (role, peer info) so recovery is automatic when
    /// the peer reconnects. Does NOT clear pair_context or connected_peers —
    /// unlike the old behavior which reset to Standalone and lost everything.
    pub(super) fn transition_to_degraded(&self) {
        // Preserve the PairCoordinator so local (stale) reads keep working.
        // Writes are rejected by WritePath::DegradedPair; reads delegate to
        // `coordinator.local_storage()` exactly as in normal pair mode.
        let degraded = match self.write_path.load().as_ref() {
            WritePath::Pair(coordinator) => WritePath::degraded_pair(coordinator.clone()),
            // If we're already degraded or not in pair mode, keep unavailable.
            _ => WritePath::unavailable(),
        };
        self.write_path.store(Arc::new(degraded));
        self.ddl_path.store(Arc::new(DdlPath::Unavailable));
        // Keep pair cluster state — the peer info is still valid for recovery.
        self.try_transition_mode(DeploymentMode::DegradedPair);
        // Do NOT clear pair_context — we need it for recovery on reconnect.
        // Do NOT clear connected_peers — the disconnected peer will be
        // removed by on_peer_disconnected, remaining peers stay tracked.
        tracing::warn!("mode transition: pair -> degraded-pair (peer lost, writes unavailable, pair context preserved)");
    }
}
