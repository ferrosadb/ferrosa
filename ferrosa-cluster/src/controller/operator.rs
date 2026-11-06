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
    /// Operator-confirmed promotion; a former pair remains a logical pair.
    ///
    /// Use when the peer is unreachable and the operator wants to resume writes.
    /// Subsequent peer reconnection will auto re-pair with this node as primary.
    pub fn force_promote(&self) -> Result<()> {
        let _transition = self.transition_guard.lock();
        if matches!(
            self.mode(),
            DeploymentMode::Cluster | DeploymentMode::DegradedCluster | DeploymentMode::Forming
        ) {
            return Err(ClusterError::ModeTransitionRejected(
                "pair promotion cannot bypass Raft membership".into(),
            ));
        }
        self.refuse_promotion_over_a_connected_peer()?;
        if self.pair_context.lock().is_none() {
            if let Some(record) = crate::pair::authority::PairAuthority::read(
                self.storage.data_dir(),
                self.local_host_id,
            )? {
                // A restarted pair is still RF2, even before any peer reconnects.
                // The address is unknown until connection; no dial is attempted here.
                if self.peer_manager.load().is_none() {
                    return Err(ClusterError::PairWriteUnavailable);
                }
                self.transition_to_pair(
                    record.peer,
                    "0.0.0.0:0".parse().expect("literal address"),
                    false,
                );
                if self.pair_context.lock().is_none() {
                    return Err(ClusterError::PairWriteUnavailable);
                }
            }
        }
        let context = self
            .pair_context
            .lock()
            .as_ref()
            .map(|ctx| (ctx.role.clone(), ctx.peer_host_id));
        if let Some((role, peer)) = context {
            let coordinator = match &**self.write_path.load() {
                WritePath::Pair(coordinator) | WritePath::DegradedPair(coordinator) => {
                    coordinator.clone()
                }
                _ => return Err(ClusterError::PairWriteUnavailable),
            };
            let _authority_gate = coordinator.handoff_gate().try_write()
                .map_err(|_| ClusterError::ModeTransitionRejected("pair authority transfer or admitted write in progress; retry operator promotion".into()))?;
            let peer_manager = match &**self.peer_manager.load() {
                Some(peer_manager) => peer_manager.clone(),
                None => return Err(ClusterError::PairWriteUnavailable),
            };
            let ddl = Arc::new(
                crate::pair::ddl::DdlCoordinator::new(
                    role.clone(),
                    peer,
                    self.schema.clone(),
                    self.storage.clone(),
                    peer_manager,
                )
                .with_handoff_gate(coordinator.handoff_gate().clone()),
            );
            crate::pair::authority::PairAuthority::store(
                self.storage.data_dir(),
                self.local_host_id,
                peer,
                self.local_host_id,
            )?;
            let epoch = self.promote_epoch.fetch_add(1, Ordering::SeqCst) + 1;
            role.store(Arc::new(crate::pair::PairRole::Primary));
            self.write_path
                .store(Arc::new(WritePath::pair(coordinator.clone())));
            self.ddl_path.store(Arc::new(DdlPath::Pair(ddl)));
            self.force_mode_override(
                DeploymentMode::Pair,
                "operator-confirmed pair promotion; RF retained",
            );
            self.force_promoted.store(true, Ordering::Release);
            tracing::info!(
                epoch,
                "operator promoted pair survivor; ALL still requires follower"
            );
            return Ok(());
        }
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
        self.connected_peers.store(Arc::new(Vec::new()));
        tracing::info!(epoch, "force promoted to standalone primary");
        Ok(())
    }

    /// Promotion takes the operator's word that the peer is dead; a peer this node is still
    /// connected to is evidence it is not. Overriding a live primary leaves two nodes accepting
    /// writes for one key (`Topology/Pair.lean`, `unguarded_promotion_splits_brain`), so a connected
    /// peer is handed off with a switchover instead. Checked before anything changes.
    fn refuse_promotion_over_a_connected_peer(&self) -> Result<()> {
        let peer = match self.pair_context.lock().as_ref() {
            Some(ctx) => Some(ctx.peer_host_id),
            None => crate::pair::authority::PairAuthority::read(
                self.storage.data_dir(),
                self.local_host_id,
            )?
            .map(|record| record.peer),
        };
        let Some(peer) = peer else {
            return Ok(());
        };
        if self
            .connected_peers
            .load()
            .iter()
            .any(|(id, _)| *id == peer)
        {
            tracing::warn!(%peer, "refused operator promotion: the pair peer is still connected");
            return Err(ClusterError::ModeTransitionRejected(format!(
                "pair peer {peer} is still connected; promotion is only for a peer confirmed \
                 dead — use switchover to move authority to this node"
            )));
        }
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
        let _operation = self.raft_retirement_operation.try_lock().map_err(|_| {
            ClusterError::ModeTransitionRejected(
                "another Raft retirement operation is in progress".into(),
            )
        })?;
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
        let _operation = self.raft_retirement_operation.try_lock().map_err(|_| {
            ClusterError::ModeTransitionRejected(
                "another Raft retirement operation is in progress".into(),
            )
        })?;
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
        let peers = self.connected_peers.load_full();
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

    /// Record dissolution intent, drain Raft and its physical workers, then
    /// confirm retirement durably before installing Pair. An incomplete marker
    /// refuses both Raft and local/Pair write ownership after restart.
    async fn dissolve_locally(&self, partner: Uuid, checked: &CheckedDissolution) -> Result<()> {
        let mut dissolution = super::dissolution::PairDissolution {
            pair: [self.local_host_id, partner],
            former_voters: checked.voters.clone(),
            dissolved_at_ms: super::dissolution::now_ms(),
            retirement_complete: false,
        };
        {
            let _guard = self.transition_guard.lock();
            if **self.mode.load() != DeploymentMode::Cluster {
                return Err(ClusterError::ModeTransitionRejected(
                    "stale dissolution: this node no longer owns Cluster mode".into(),
                ));
            }
            super::dissolution::write_dissolution(&checked.raft_dir, &dissolution)?;
            self.dissolution
                .store(Arc::new(super::dissolution::DissolutionState::Unreadable(
                    "physical retirement is pending; Pair write ownership is not confirmed".into(),
                )));
        }
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
                    "Raft was stopped but the mode changed to {now}; retirement intent \
                     remains incomplete and requires verified recovery"
                )));
            }
            dissolution.retirement_complete = true;
            super::dissolution::write_dissolution(&checked.raft_dir, &dissolution)?;
            self.dissolution
                .store(Arc::new(super::dissolution::DissolutionState::Dissolved(
                    dissolution,
                )));
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
        self.physical_raft_workers
            .stop_and_drain(&groups)
            .await
            .map_err(|error| {
                self.consensus_health
                    .fail("raft_retirement_incomplete", format_args!("{error}"));
                ClusterError::RaftError(format!(
                    "downgrade to pair: physical retirement failed: {error}"
                ))
            })?;
        self.raft_groups
            .store(Arc::new(std::collections::HashMap::new()));
        Ok(())
    }

    /// Returns the process-local operator-promotion counter.
    /// This telemetry never elects a pair owner on reconnect.
    pub fn promote_epoch(&self) -> u64 {
        self.promote_epoch.load(Ordering::SeqCst)
    }

    /// Set promotion telemetry without changing pair ownership.
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
        let coordinator = match &**self.write_path.load() {
            WritePath::Pair(coordinator) => coordinator.clone(),
            _ => return Err(ClusterError::NotPrimary),
        };
        let _handoff = coordinator.handoff_gate().write().await;
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
            self.storage.data_dir(),
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

#[cfg(test)]
mod physical_handoff_tests {
    use super::*;
    use crate::raft::log_store::SledLogStore;
    use crate::raft::network::FerrosRaftNetworkFactory;
    use crate::raft::state_machine::FerrosStateMachine;
    use crate::raft::{FerrosRaft, FerrosRaftConfig, RaftCommand, RaftOp};
    use ferrosa_net::peer::{PeerEventListener, PeerManager};
    use ferrosa_storage::observer::{ObserverMode, WriteObserver};
    use ferrosa_storage::{
        Mutation, StorageEngine, StorageEngineConfig, SyncStrategyConfig, TableId,
    };
    use openraft::raft::AppendEntriesRequest;
    use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    struct NoopListener;
    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _: ferrosa_net::rpc::PeerId) {}
        fn on_peer_disconnected(&self, _: ferrosa_net::rpc::PeerId) {}
        fn on_peer_suspected(&self, _: ferrosa_net::rpc::PeerId) {}
        fn on_peer_recovered(&self, _: Uuid) {}
        fn on_peer_failed(&self, _: Uuid) {}
    }

    struct WriteWitness {
        key: Vec<u8>,
        reached: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }
    impl WriteObserver for WriteWitness {
        fn mode(&self) -> ObserverMode {
            ObserverMode::Sync
        }
        fn tables(&self) -> Vec<TableId> {
            vec![TableId::new("system_schema", "keyspaces")]
        }
        fn on_write(&self, _: &TableId, mutation: &Mutation) -> Vec<Mutation> {
            if mutation.key.key.as_bytes() == self.key {
                if let Some(reached) = self.reached.lock().take() {
                    let _ = reached.send(());
                }
            }
            Vec::new()
        }
    }

    fn keyspace(name: &str) -> ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
        ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
            name: name.into(),
            durable_writes: true,
            replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                strategy: "SimpleStrategy".into(),
                options: HashMap::from([("replication_factor".into(), "1".into())]),
            },
        }
    }
    fn entry(index: u64, name: &str) -> Entry<FerrosRaftConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(5, 99), index),
            payload: EntryPayload::Normal(RaftCommand {
                op: RaftOp::CreateKeyspace(keyspace(name)),
                schema_version: Uuid::new_v4(),
            }),
        }
    }

    /// Real follower commits, a real blocked WAL file, and a second queued SM
    /// command distinguish Core shutdown from physical worker retirement.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn physical_worker_retirement_precedes_pair_handoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageEngineConfig::test_config(&dir.path().join("data"));
        config.commit_log.sync_strategy = SyncStrategyConfig::default();
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        engine.register_system_tables().unwrap();
        let schema = crate::controller::tests::test_schema();
        let controller =
            ModeController::standalone_for_test(Arc::clone(&schema), Arc::clone(&engine));
        controller
            .force_mode_override(DeploymentMode::Cluster, "owned physical-retirement fixture");
        let peers = Arc::new(PeerManager::new(
            Arc::new(ferrosa_net::config::NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        controller
            .peer_manager
            .store(Arc::new(Some(Arc::clone(&peers))));
        let network = FerrosRaftNetworkFactory::new(
            peers,
            Arc::new(crate::raft::replication_health::ReplicationHealth::new()),
        );
        let raft_dir = dir.path().join("raft");
        let log = SledLogStore::new(&raft_dir).unwrap();
        let sm = FerrosStateMachine::with_side_effects(Arc::clone(&schema), Arc::clone(&engine));
        let physical = controller
            .physical_raft_workers
            .reserve(
                crate::raft::RaftGroupId::default_dc(),
                sm.physical_lifecycle(),
            )
            .unwrap();
        physical.bind_log(log.committed_reader());
        let raft = Arc::new(
            FerrosRaft::new(
                1,
                Arc::new(
                    openraft::Config {
                        enable_elect: false,
                        ..Default::default()
                    }
                    .validate()
                    .unwrap(),
                ),
                network,
                log,
                sm,
            )
            .await
            .unwrap(),
        );
        assert!(physical.install_raft(Arc::clone(&raft)));
        controller.set_raft_for_group(crate::raft::RaftGroupId::default_dc(), Arc::clone(&raft));

        let (reached, first_write) = tokio::sync::oneshot::channel();
        engine.register_observer(Arc::new(WriteWitness {
            key: b"held_effect".to_vec(),
            reached: parking_lot::Mutex::new(Some(reached)),
        }));
        let (release, released) = std::sync::mpsc::channel();
        let (locked, acquired) = tokio::sync::oneshot::channel();
        let held_engine = Arc::clone(&engine);
        let holder = std::thread::spawn(move || {
            held_engine.with_commit_log_flush_held_for_test(|| {
                locked.send(()).unwrap();
                released
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("fixture releases actual WAL mutex");
            })
        });
        acquired.await.unwrap();
        let membership = Entry {
            log_id: LogId::new(CommittedLeaderId::new(5, 99), 0),
            payload: EntryPayload::Membership(Membership::new(
                vec![BTreeSet::from([1, 99])],
                BTreeMap::from([(1, BasicNode::default()), (99, BasicNode::default())]),
            )),
        };
        raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(5, 99),
            prev_log_id: None,
            entries: vec![membership, entry(1, "held_effect")],
            leader_commit: Some(LogId::new(CommittedLeaderId::new(5, 99), 1)),
        })
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), first_write)
            .await
            .expect("exact canonical write entered")
            .unwrap();
        raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(5, 99),
            prev_log_id: Some(LogId::new(CommittedLeaderId::new(5, 99), 1)),
            entries: vec![entry(2, "queued_effect")],
            leader_commit: Some(LogId::new(CommittedLeaderId::new(5, 99), 2)),
        })
        .await
        .unwrap();
        assert!(!schema.snapshot().keyspaces.contains_key("queued_effect"));
        raft.shutdown().await.unwrap();
        let partner = Uuid::new_v4();
        let checked = CheckedDissolution {
            partner_addr: "127.0.0.1:1".parse().unwrap(),
            voters: vec![1, 99, 100],
            down: vec![Uuid::new_v4()],
            raft_dir,
        };
        let mut transition = Box::pin(controller.dissolve_locally(partner, &checked));
        let first_poll = futures::poll!(transition.as_mut());
        let waited = first_poll.is_pending();
        let stayed_cluster = **controller.mode.load() == DeploymentMode::Cluster;
        release.send(()).unwrap();
        holder.join().unwrap();
        match first_poll {
            std::task::Poll::Pending => transition.await.unwrap(),
            std::task::Poll::Ready(result) => result.unwrap(),
        }
        assert!(
            waited && stayed_cluster,
            "Core shutdown must not install Pair while old shared-engine work is held"
        );
        assert_eq!(**controller.mode.load(), DeploymentMode::Pair);
        assert!(schema.snapshot().keyspaces.contains_key("held_effect"));
        assert!(
            schema.snapshot().keyspaces.contains_key("queued_effect"),
            "committed queued suffix must land before Pair owns writes"
        );
    }
    #[tokio::test]
    async fn incomplete_retirement_refuses_local_write_ownership_after_restart() {
        for completion in [None, Some(false), Some(true)] {
            let dir = tempfile::tempdir().unwrap();
            let config = Arc::new(crate::ClusterConfig {
                raft_data_dir: Some(dir.path().join("raft")),
                ..Default::default()
            });
            let raft_dir = crate::controller::cluster::configured_raft_dir(&config).unwrap();
            std::fs::create_dir_all(&raft_dir).unwrap();
            let local = Uuid::new_v4();
            let partner = Uuid::new_v4();
            let mut record = serde_json::json!({"pair":[local,partner],"former_voters":[1,2,3],"dissolved_at_ms":1});
            if let Some(complete) = completion {
                record["retirement_complete"] = complete.into();
            }
            let path = crate::controller::dissolution::marker_path(&raft_dir);
            let bytes = serde_json::to_vec(&record).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            let engine = Arc::new(
                StorageEngine::new(
                    StorageEngineConfig::test_config(&dir.path().join("data")),
                    None,
                )
                .unwrap(),
            );
            let (controller, _) = ModeController::new(
                config,
                Arc::new(ferrosa_net::config::NetConfig::default()),
                local,
                engine,
                crate::controller::tests::test_schema(),
                Arc::new(ferrosa_net::rpc::HandlerRegistry::new()),
            );
            if completion == Some(true) {
                assert_eq!(
                    controller.mode(),
                    DeploymentMode::Standalone,
                    "confirmed retirement retains existing Pair restart path"
                );
                assert!(controller.consensus_health.is_healthy());
            } else {
                assert_eq!(controller.mode(), DeploymentMode::DegradedCluster, "intent-only or legacy unproven marker must not confer local/Pair write ownership");
                assert!(!controller.consensus_health.is_healthy());
                assert!(!controller.dissolution.load().admits_peer(local, partner));
            }
            assert_eq!(
                std::fs::read(path).unwrap(),
                bytes,
                "startup must preserve repair evidence"
            );
        }
    }
    #[tokio::test]
    async fn physical_stale_dissolution_cannot_rebind_confirmed_partner() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap(),
        );
        let controller =
            ModeController::standalone_for_test(crate::controller::tests::test_schema(), engine);
        controller.force_mode_override(DeploymentMode::Pair, "owned stale-retirement fixture");
        let old_partner = Uuid::new_v4();
        let dissolution = crate::controller::dissolution::PairDissolution {
            pair: [controller.local_host_id, old_partner],
            former_voters: vec![1, 2, 3],
            dissolved_at_ms: 1,
            retirement_complete: true,
        };
        let raft_dir = dir.path().join("raft");
        crate::controller::dissolution::write_dissolution(&raft_dir, &dissolution).unwrap();
        controller.dissolution.store(Arc::new(
            crate::controller::dissolution::DissolutionState::Dissolved(dissolution),
        ));
        let path = crate::controller::dissolution::marker_path(&raft_dir);
        let before = std::fs::read(&path).unwrap();
        let checked = CheckedDissolution {
            partner_addr: "127.0.0.1:1".parse().unwrap(),
            voters: vec![1, 2, 3],
            down: vec![Uuid::new_v4()],
            raft_dir,
        };
        assert!(controller
            .dissolve_locally(Uuid::new_v4(), &checked)
            .await
            .is_err());
        assert_eq!(
            std::fs::read(path).unwrap(),
            before,
            "stale checked operation must not rewrite durable partner authority"
        );
        assert_eq!(**controller.mode.load(), DeploymentMode::Pair);
        assert!(controller
            .dissolution
            .load()
            .admits_peer(controller.local_host_id, old_partner));
    }
}
