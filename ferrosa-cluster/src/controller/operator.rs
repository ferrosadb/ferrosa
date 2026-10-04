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
    /// 3. the members taken down are NOT a Raft majority of the voters on their
    ///    own, so they can never re-form the old cluster (t_47bbeb66);
    /// 4. the named peer durably records the dissolution and stops its Raft
    ///    group, and confirms (phase 1); nothing here changes before that;
    /// 5. this node durably records the dissolution, stops its Raft group(s),
    ///    and only then installs the pair path (phase 2).
    ///
    /// With both pair nodes durably out of Raft and the downed members a
    /// minority, no part of the old cluster can reach quorum again. The marker
    /// ([`super::dissolution`]) also keeps both nodes out of Raft across
    /// restarts and refuses former members as peers.
    ///
    /// Every shortcut is refused loudly, leaving the node untouched: the node is
    /// not a `Cluster` member (already `Pair`, or `Standalone`); no named
    /// peer (the old action used `connected_peers.first()`); no committed ring
    /// to check; a named peer outside the ring or not connected; no member
    /// down; any other member still up; the downed members a voter majority;
    /// no Raft group or Raft directory; the T-300 jsonb guard; no peer manager;
    /// the peer not confirming phase 1.
    pub async fn downgrade_to_pair(&self, named_peer: Option<Uuid>) -> Result<Uuid> {
        let Some(named_peer) = named_peer else {
            return Err(ClusterError::ModeTransitionRejected(
                "downgrade to pair requires a named peer to replicate to; pass the \
                 peer's host id"
                    .into(),
            ));
        };
        let checked = self.check_pair_dissolution(named_peer)?;

        // Phase 1: the peer leaves Raft durably first. A refusal or a lost
        // reply changes nothing here.
        let transport = self.pair_dissolve_transport();
        transport
            .request(super::dissolution::PairDissolveRequest {
                requester: self.local_host_id,
                peer: named_peer,
                former_voters: checked.voters.clone(),
            })
            .await?;

        // Phase 2: this node.
        self.dissolve_locally(named_peer, &checked).await?;
        tracing::warn!(
            peer = %named_peer,
            down = ?checked.down,
            "OPERATOR ACTION: downgraded from cluster to pair mode. Both pair nodes \
             recorded the dissolution and stopped Raft; the old cluster cannot reach \
             quorum again."
        );
        Ok(named_peer)
    }

    /// Phase 1 of the operator downgrade, served on the named peer: check the
    /// same preconditions from THIS node's view, then record the dissolution,
    /// stop Raft and become the other half of the pair.
    pub async fn accept_pair_dissolution(
        &self,
        req: super::dissolution::PairDissolveRequest,
    ) -> Result<()> {
        if req.peer != self.local_host_id {
            return Err(ClusterError::ModeTransitionRejected(format!(
                "pair dissolution addressed to {}, but this node is {}",
                req.peer, self.local_host_id
            )));
        }
        let checked = self.check_pair_dissolution(req.requester)?;
        if checked.voters != req.former_voters {
            return Err(ClusterError::ModeTransitionRejected(format!(
                "pair dissolution refused: the requester saw voters {:?}, this node sees \
                 {:?}",
                req.former_voters, checked.voters
            )));
        }
        self.dissolve_locally(req.requester, &checked).await?;
        tracing::warn!(
            partner = %req.requester,
            down = ?checked.down,
            "OPERATOR ACTION (from the partner): dissolved into a pair; recorded \
             durably and Raft stopped"
        );
        Ok(())
    }

    fn pair_dissolve_transport(&self) -> Arc<dyn super::dissolution::PairDissolveTransport> {
        if let Some(transport) = self.pair_dissolve_transport.load().as_ref() {
            return transport.clone();
        }
        let peer_manager = self
            .peer_manager
            .load()
            .as_ref()
            .clone()
            .expect("check_pair_dissolution requires a peer manager");
        Arc::new(super::dissolution::NetPairDissolveTransport { peer_manager })
    }

    /// Every precondition of a dissolution into a pair with `partner`, from
    /// this node's view. Pure check: changes nothing.
    fn check_pair_dissolution(&self, partner: Uuid) -> Result<CheckedDissolution> {
        let refuse = |msg: String| Err(ClusterError::ModeTransitionRejected(msg));
        let current = **self.mode.load();
        if current != DeploymentMode::Cluster {
            return refuse(format!(
                "downgrade to pair refused: this node is not a cluster member (mode is \
                 {current}); the node is unchanged"
            ));
        }
        let Some(ring) = self.token_ring() else {
            return refuse(
                "downgrade to pair refused: no committed token ring, so this node \
                 cannot check which cluster members are down"
                    .into(),
            );
        };
        let local = crate::raft::uuid_to_node_id(self.local_host_id);
        let partner_node = crate::raft::uuid_to_node_id(partner);
        if ring.get_node(partner_node).is_none() {
            return refuse(format!(
                "downgrade to pair refused: {partner} is not a cluster member"
            ));
        }
        let peers = self.connected_peers.lock().clone();
        let is_up = |host: Uuid| peers.iter().any(|(id, _)| *id == host);
        let down: Vec<Uuid> = ring
            .node_ids()
            .into_iter()
            .filter(|id| *id != local && *id != partner_node)
            .filter_map(|id| ring.get_node(id).map(|n| n.host_id))
            .collect();
        if down.is_empty() {
            return refuse(format!(
                "downgrade to pair refused: no member has been taken down. The ring \
                 holds only this node and {partner}; bring one node down first, \
                 then run the downgrade"
            ));
        }
        let still_up: Vec<String> = down
            .iter()
            .filter(|h| is_up(**h))
            .map(Uuid::to_string)
            .collect();
        if !still_up.is_empty() {
            return refuse(format!(
                "downgrade to pair refused: member(s) {still_up:?} still up. Every member \
                 other than this node and {partner} must be down first; a live member \
                 is a voter and replica this pair would write past"
            ));
        }
        let Some((_, partner_addr)) = peers.iter().copied().find(|(id, _)| *id == partner) else {
            let connected: Vec<String> = peers.iter().map(|(id, _)| id.to_string()).collect();
            return refuse(format!(
                "downgrade to pair requires the named peer {partner} to be a connected \
                 peer to replicate to; connected peers: {connected:?} — is the intended \
                 peer running and reachable?"
            ));
        };
        let voters = self.check_downed_voters_are_a_minority(local, partner_node)?;
        let Some(raft_dir) = super::cluster::configured_raft_dir(&self.config) else {
            return refuse(
                "downgrade to pair refused: no Raft directory configured, so the \
                 dissolution cannot be recorded durably"
                    .into(),
            );
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
        Ok(CheckedDissolution {
            partner_addr,
            voters,
            down,
            raft_dir,
        })
    }

    /// The Raft voter set, if the members other than this node and the partner
    /// cannot form a majority of it on their own. Once both pair nodes leave
    /// Raft for good, those members are all that is left of the old group;
    /// as a minority they can never commit again.
    fn check_downed_voters_are_a_minority(&self, local: u64, partner: u64) -> Result<Vec<u64>> {
        let Some(raft) = self.raft() else {
            return Err(ClusterError::ModeTransitionRejected(
                "downgrade to pair refused: no Raft group running, so the voter set \
                 cannot be checked"
                    .into(),
            ));
        };
        let voters: Vec<u64> = {
            let metrics = raft.metrics();
            let metrics = metrics.borrow();
            let mut ids: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
            ids.sort_unstable();
            ids
        };
        if !voters.contains(&local) || !voters.contains(&partner) {
            return Err(ClusterError::ModeTransitionRejected(format!(
                "downgrade to pair refused: this node ({local}) and the partner ({partner}) \
                 must both be Raft voters; voters are {voters:?}"
            )));
        }
        let downed = voters.len() - 2;
        let quorum = voters.len() / 2 + 1;
        if downed >= quorum {
            return Err(ClusterError::ModeTransitionRejected(format!(
                "downgrade to pair refused: the {downed} voters other than the pair are a \
                 majority of {} (quorum {quorum}); they could re-form the old cluster \
                 beside the pair. Decommission members until the pair plus the rest \
                 leaves them a minority",
                voters.len()
            )));
        }
        Ok(voters)
    }

    /// Record the dissolution durably, stop Raft, and install the pair path
    /// with `partner`. The marker is written FIRST: a crash after it leaves a
    /// node that refuses Raft on restart, never one that rejoins the old group.
    async fn dissolve_locally(&self, partner: Uuid, checked: &CheckedDissolution) -> Result<()> {
        let dissolution = super::dissolution::PairDissolution {
            pair: [self.local_host_id, partner],
            former_voters: checked.voters.clone(),
            dissolved_at_ms: super::dissolution::now_ms(),
        };
        super::dissolution::write_dissolution(&checked.raft_dir, &dissolution)?;
        self.dissolution
            .store(Arc::new(super::dissolution::DissolutionState::Dissolved(
                dissolution,
            )));
        self.stop_raft_for_downgrade().await?;
        {
            // Hold `transition_guard` across the transition so the mode cannot
            // move underneath the check-and-install in `transition_to_pair`.
            let _guard = self.transition_guard.lock();
            // Raft was stopped outside the guard (an await); re-check that
            // nothing moved this node out of Cluster meanwhile.
            let now = **self.mode.load();
            if now != DeploymentMode::Cluster {
                tracing::error!(
                    %now,
                    "OPERATOR ACTION FAILED: Raft was stopped to downgrade but the mode \
                     changed underneath it; not installing the pair path"
                );
                return Err(ClusterError::ModeTransitionRejected(format!(
                    "Raft was stopped but the mode changed to {now} before the pair \
                     transition; the dissolution is recorded, so restart the node to \
                     come up as half of the pair"
                )));
            }
            self.transition_to_pair_operator_override(partner, checked.partner_addr);
        }
        if **self.mode.load() != DeploymentMode::Pair {
            tracing::error!(
                %partner,
                "OPERATOR ACTION FAILED: Raft was stopped and the dissolution recorded, \
                 but the pair transition was refused; this node serves no writes"
            );
            return Err(ClusterError::ModeTransitionRejected(
                "Raft was stopped and the dissolution recorded, but the pair transition \
                 was refused (see the node log); restart the node to come up as half of \
                 the pair"
                    .into(),
            ));
        }
        Ok(())
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

/// The outcome of [`ModeController::check_pair_dissolution`].
struct CheckedDissolution {
    partner_addr: std::net::SocketAddr,
    /// Raft voters (openraft node ids), sorted.
    voters: Vec<u64>,
    /// Members taken down (host ids).
    down: Vec<Uuid>,
    raft_dir: std::path::PathBuf,
}
