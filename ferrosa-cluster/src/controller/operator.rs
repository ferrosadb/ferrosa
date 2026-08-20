//! Operator commands: force_promote, switchover, transition_to_degraded.

use std::sync::atomic::Ordering;
use std::sync::Arc;

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
