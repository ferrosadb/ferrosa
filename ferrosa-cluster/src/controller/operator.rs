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
    /// Refused, leaving the node untouched, when:
    ///
    /// - `peer` is not a currently connected peer: a pair needs somewhere to
    ///   replicate, and a mode change alone would be a lie. The message names
    ///   the connected peers so the operator can see what was actually available.
    /// - the T-300 jsonb guard refuses leaving standalone (checked inside the
    ///   pair transition, so it cannot be bypassed by going through here).
    /// - no peer manager is installed.
    ///
    /// On success the real pair machinery is installed (coordinator, DDL path,
    /// write path, pair state), not merely the mode -- so the node genuinely
    /// behaves as a pair afterwards rather than reporting pair mode while still
    /// writing through the cluster path.
    pub fn downgrade_to_pair(&self) -> Result<Uuid> {
        let peers = self.connected_peers.lock().clone();
        let Some((peer_host_id, peer_addr)) = peers.first().copied() else {
            return Err(ClusterError::ModeTransitionRejected(
                "downgrade to pair requires a connected peer to replicate to; none is \
                 connected — is the intended peer running and reachable?"
                    .into(),
            ));
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
