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
    /// Ben's rule (t_ad872ac7): "to become a pair an operator needs to bring one
    /// node down then do a downgrade — friction is the point." So the ONLY
    /// accepted shape is:
    ///
    /// 1. the operator has taken at least one member down, and every member
    ///    other than this node and the named peer is down (not connected);
    /// 2. the operator names the peer, and it is a connected cluster member;
    /// 3. this node stops its Raft group(s) BEFORE it installs the pair path,
    ///    so it is never a voter committing beside a pair write path (P0-5).
    ///
    /// Every shortcut is refused loudly, leaving the node untouched: no named
    /// peer (the old action used `connected_peers.first()`); no committed ring
    /// to check; a named peer outside the ring or not connected; no member
    /// down; any other member still up; the T-300 jsonb guard; no peer manager.
    ///
    /// On success the real pair machinery is installed (coordinator, DDL path,
    /// write path, pair state), not merely the mode. The member that was taken
    /// down must stay down: if it returns while the named peer still runs Raft,
    /// those two form a Raft majority beside this pair.
    pub async fn downgrade_to_pair(&self, named_peer: Option<Uuid>) -> Result<Uuid> {
        let refuse = |msg: String| Err(ClusterError::ModeTransitionRejected(msg));
        let Some(named_peer) = named_peer else {
            return refuse(
                "downgrade to pair requires a named peer to replicate to; pass the \
                 peer's host id"
                    .into(),
            );
        };
        let Some(ring) = self.token_ring() else {
            return refuse(
                "downgrade to pair refused: no committed token ring, so this node \
                 cannot check which cluster members are down"
                    .into(),
            );
        };
        let local = crate::raft::uuid_to_node_id(self.local_host_id);
        let peer_node = crate::raft::uuid_to_node_id(named_peer);
        if ring.get_node(peer_node).is_none() {
            return refuse(format!(
                "downgrade to pair refused: {named_peer} is not a cluster member"
            ));
        }
        let peers = self.connected_peers.lock().clone();
        let is_up = |host: Uuid| peers.iter().any(|(id, _)| *id == host);
        let others: Vec<Uuid> = ring
            .node_ids()
            .into_iter()
            .filter(|id| *id != local && *id != peer_node)
            .filter_map(|id| ring.get_node(id).map(|n| n.host_id))
            .collect();
        if others.is_empty() {
            return refuse(format!(
                "downgrade to pair refused: no member has been taken down. The ring \
                 holds only this node and {named_peer}; bring one node down first, \
                 then run the downgrade"
            ));
        }
        let still_up: Vec<String> = others
            .iter()
            .filter(|h| is_up(**h))
            .map(Uuid::to_string)
            .collect();
        if !still_up.is_empty() {
            return refuse(format!(
                "downgrade to pair refused: member(s) {still_up:?} still up. Every member \
                 other than this node and {named_peer} must be down first; a live member \
                 is a voter and replica this pair would write past"
            ));
        }
        let Some((peer_host_id, peer_addr)) =
            peers.iter().copied().find(|(id, _)| *id == named_peer)
        else {
            let connected: Vec<String> = peers.iter().map(|(id, _)| id.to_string()).collect();
            return refuse(format!(
                "downgrade to pair requires the named peer {named_peer} to be a connected \
                 peer to replicate to; connected peers: {connected:?} — is the intended \
                 peer running and reachable?"
            ));
        };
        // Check what the pair transition would refuse BEFORE stopping Raft, so a
        // refusal can never leave a node with no Raft and no pair path.
        if !self.leaving_standalone_permitted(DeploymentMode::Pair) {
            return refuse(
                "the T-300 jsonb guard refuses this transition (see the node log)".into(),
            );
        }
        if self.peer_manager.load().is_none() {
            return refuse("downgrade to pair refused: no peer manager installed".into());
        }

        self.stop_raft_for_downgrade().await?;
        {
            // Hold `transition_guard` across the transition so the mode cannot
            // move underneath the check-and-install in `transition_to_pair`.
            let _guard = self.transition_guard.lock();
            let current = **self.mode.load();
            if current != DeploymentMode::Cluster {
                return Err(ClusterError::ModeTransitionRejected(format!(
                    "downgrade to pair refused: this node is not a cluster member (mode is \
                     {current}); the node is unchanged"
                )));
            }
            let peers = self.connected_peers.lock().clone();
            let target = match peers.as_slice() {
                [] => {
                    return Err(ClusterError::ModeTransitionRejected(
                        "downgrade to pair requires a connected peer to replicate to; none is \
                         connected — is the intended peer running and reachable?"
                            .into(),
                    ))
                }
                [only] => *only,
                // Pair mode replicates to exactly one peer. With several
                // connected, picking one is an arbitrary choice the operator did
                // not make, and the others keep committing through Raft.
                many => {
                    let ids: Vec<String> = many.iter().map(|(id, _)| id.to_string()).collect();
                    return Err(ClusterError::ModeTransitionRejected(format!(
                        "downgrade to pair refused: more than one peer is connected ({}), so \
                         the pair target is ambiguous; remove the other members first. The \
                         node is unchanged",
                        ids.join(", ")
                    )));
                }
            };
            self.transition_to_pair_operator_override(target.0, target.1);
            target
        };
        if **self.mode.load() != DeploymentMode::Pair {
            tracing::error!(
                %peer_host_id,
                "OPERATOR ACTION FAILED: Raft was stopped but the pair transition was \
                 refused; this node serves no writes. Restart it to rejoin the cluster"
            );
            return Err(ClusterError::ModeTransitionRejected(
                "Raft was stopped but the pair transition was refused (see the node log); \
                 restart the node to rejoin the cluster"
                    .into(),
            ));
        }
        tracing::warn!(
            %peer_host_id,
            down = ?others,
            "OPERATOR ACTION: downgraded from cluster to pair mode. Raft is stopped on \
             this node; it replicates point-to-point to the named peer. The members \
             taken down must stay down."
        );
        Ok(peer_host_id)
    }

    /// Shut down every Raft group on this node and drop them, so it is no
    /// longer a voter. A failed shutdown aborts the downgrade.
    async fn stop_raft_for_downgrade(&self) -> Result<()> {
        let groups = self.raft_groups.load_full();
        for (id, raft) in groups.iter() {
            raft.shutdown().await.map_err(|e| {
                ClusterError::RaftError(format!(
                    "downgrade to pair: failed to stop Raft group {id:?}: {e}"
                ))
            })?;
            tracing::warn!(group = ?id, "OPERATOR ACTION: Raft group stopped for downgrade to pair");
        }
        self.raft_groups
            .store(Arc::new(std::collections::HashMap::new()));
        Ok(())
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
    ///
    /// Refused unless the peer is caught up: no data catch-up replay may be
    /// running or have failed, and the peer must confirm it converged to this
    /// node's schema (pushed here, so a missed ALTER is applied first).
    /// Promoting a peer that is behind makes it serve wrong answers -- a
    /// written value read back as null.
    pub async fn switchover(&self) -> Result<()> {
        let (role_arc, peer_host_id, catch_up) = {
            let ctx = self.pair_context.lock();
            let ctx = ctx.as_ref().ok_or(ClusterError::ModeTransitionRejected(
                "switchover requires pair mode; current node is standalone".into(),
            ))?;
            (ctx.role.clone(), ctx.peer_host_id, ctx.catch_up.clone())
        };
        if **role_arc.load() != crate::pair::PairRole::Primary {
            return Err(ClusterError::NotPrimary);
        }
        catch_up.ready()?;

        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                return Err(ClusterError::ModeTransitionRejected(
                    "peer manager not initialized; peer may be disconnected".into(),
                ));
            }
        };

        crate::pair::switchover::push_schema_and_confirm(&peer_manager, peer_host_id, &self.schema)
            .await?;

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
