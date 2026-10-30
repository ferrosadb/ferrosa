//! Pair mode transition logic.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::message::Message;
use ferrosa_net::pool::PriorityPool;
use ferrosa_storage::CommitLogPosition;
use uuid::Uuid;

use crate::ddl_path::DdlPath;
use crate::mode::DeploymentMode;
use crate::pair::coordinator::{encode_mutation, PairCoordinator};
use crate::pair::ddl::{DdlCoordinator, PairDdlForwardHandler, PairSchemaSyncHandler};
use crate::pair::{PairRole, PairState};
use crate::state::PairClusterState;
use crate::write_path::WritePath;

use super::token::send_schema_sync_to_peer;
use super::{ClusterStateHolder, ModeController, PairContext};

/// Replay the local commit log to a rejoined peer. Returns `true` only when
/// every mutation was acknowledged by the peer.
async fn replay_commit_log_to_peer(
    storage: &ferrosa_storage::StorageEngine,
    pm: &ferrosa_net::peer::PeerManager,
    peer_host_id: Uuid,
) -> bool {
    let position = CommitLogPosition {
        segment_id: 0,
        offset: 0,
    };
    let mutations = match storage.replay_from(position) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(%e, "catch-up replay_from failed; catch-up FAILED");
            return false;
        }
    };
    if mutations.is_empty() {
        tracing::info!("no data to replay for catch-up");
        return true;
    }
    tracing::info!(count = mutations.len(), "replaying data to rejoined peer");
    for mutation in &mutations {
        let body = encode_mutation(mutation);
        match pm
            .send(peer_host_id, Message::PairWriteForward(body), Lane::Data)
            .await
        {
            Ok(Message::PairWriteAck(_)) => {}
            Ok(other) => {
                tracing::warn!(
                    response = ?other.msg_type(),
                    "catch-up replay: peer did not ack a mutation; catch-up FAILED"
                );
                return false;
            }
            Err(e) => {
                tracing::warn!(%e, "catch-up replay send failed; catch-up FAILED");
                return false;
            }
        }
    }
    tracing::info!("catch-up replay complete");
    true
}

impl ModeController {
    fn choose_pair_role(
        &self,
        peer_host_id: Uuid,
        _peer_addr: SocketAddr,
        _need_reverse: bool,
        was_promoted: bool,
    ) -> PairRole {
        match crate::pair::authority::PairAuthority::read(
            self.storage.data_dir(),
            self.local_host_id,
        ) {
            Ok(Some(record)) => {
                return if record.peer == peer_host_id
                    && record.primary == self.local_host_id
                    && (was_promoted
                        || self.pair_context.lock().as_ref().is_some_and(|ctx| {
                            ctx.peer_host_id == peer_host_id
                                && **ctx.role.load() == PairRole::Primary
                        }))
                {
                    PairRole::Primary
                } else {
                    PairRole::Secondary
                };
            }
            Err(error) => {
                tracing::error!(%error, "pair authority cannot be recovered; remaining read-only");
                return PairRole::Secondary;
            }
            Ok(None) => {}
        }
        if was_promoted || self.local_host_id <= peer_host_id {
            PairRole::Primary
        } else {
            PairRole::Secondary
        }
    }

    pub(super) fn transition_to_pair(
        &self,
        peer_host_id: Uuid,
        peer_addr: SocketAddr,
        need_reverse: bool,
    ) {
        self.transition_to_pair_inner(peer_host_id, peer_addr, need_reverse, false)
    }

    /// [`Self::transition_to_pair`], but an OPERATOR has waived the
    /// state-machine check that forbids `Cluster -> Pair`.
    ///
    /// Waiving that one check is the smallest possible override, which is the
    /// point. Entering pair mode is refused for a cluster member because the
    /// shapes commit differently, so the refusal has to be lifted for an
    /// operator to be able to downgrade at all. Everything else stays
    /// unconditional, because the machinery is what makes pair mode correct
    /// once you are in it:
    ///
    /// - the T-300 jsonb check still runs (no departure from standalone with
    ///   jsonb columns present);
    /// - a peer_manager is still required, because installing a pair write path
    ///   with no peer to replicate to would accept writes nothing could mirror;
    /// - the real pair coordinator/handlers/write path are still installed --
    ///   this does NOT simply declare `Pair` and leave a cluster write path in
    ///   place, which is the split brain T8b exists to prevent.
    ///
    /// The mode is then set by `force_mode_override`, which is deliberately
    /// unguarded and logs at WARN with both modes, so an override can never be
    /// mistaken for a normal transition when reading logs afterwards.
    pub(super) fn transition_to_pair_operator_override(
        &self,
        peer_host_id: Uuid,
        peer_addr: SocketAddr,
    ) {
        self.transition_to_pair_inner(peer_host_id, peer_addr, true, true)
    }

    fn transition_to_pair_inner(
        &self,
        peer_host_id: Uuid,
        peer_addr: SocketAddr,
        need_reverse: bool,
        operator_override: bool,
    ) {
        // Refuse before touching anything, not after.
        //
        // This function installs `WritePath::pair(...)` -- which accepts writes
        // as a pair primary -- well before it asks whether the mode change is
        // allowed. Checking only at the end is not a guard: a refused call
        // leaves `mode = Cluster` on a node whose write path has already become
        // a pair coordinator, so it accepts writes outside Raft while the rest
        // of the cluster keeps running consensus, and every mode-based check
        // still reports Cluster. That is the split brain T8b of
        // `specs/reference/cluster-formation-state-machine.md` exists to
        // prevent, made harder to see rather than easier.
        //
        // Callers hold `transition_guard`, so the mode cannot change between
        // this check and the commit below.
        let current = **self.mode.load();
        if !operator_override && !current.can_transition_to(DeploymentMode::Pair) {
            tracing::error!(
                %current,
                "refused illegal transition to pair; write path left untouched"
            );
            return;
        }

        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to pair: peer_manager not set");
                return;
            }
        };

        // Reconnect can race a switchover after peer loss moves us into
        // DegradedPair. Serialize the role decision and installation with the
        // existing writer/transfer gate; transition_guard alone does not cover
        // switchover. Refuse a busy synchronous callback without consuming
        // operator promotion state or restoring a withdrawn primary.
        let existing_coordinator = match &**self.write_path.load() {
            WritePath::Pair(c) | WritePath::DegradedPair(c) => Some(c.clone()),
            _ => None,
        };
        let _existing_handoff = match existing_coordinator.as_ref() {
            Some(coordinator) => {
                match coordinator.handoff_gate().try_write() {
                    Ok(guard) => Some(guard),
                    Err(_) => {
                        tracing::warn!("pair reconnect deferred: authority transfer or admitted write in progress");
                        return;
                    }
                }
            }
            None => None,
        };

        // Pair-role election must be deterministic across both directions of the
        // same peer relationship. During cluster bootstrap a joiner can receive
        // the seed's reverse inbound connection before its own outbound
        // `on_peer_connected()` fires; if role selection depends only on
        // connection direction, both sides can become primary and stale pair-mode
        // handlers will keep forwarding writes after cluster promotion.
        //
        // Keep force-promoted nodes primary, otherwise use host-id ordering so the
        // same two nodes always elect the same primary no matter which socket
        // wins the race.
        let was_promoted = self.force_promoted.swap(false, Ordering::AcqRel);
        let local_epoch = self.promote_epoch.load(Ordering::SeqCst);
        let role = self.choose_pair_role(peer_host_id, peer_addr, need_reverse, was_promoted);
        if crate::pair::authority::PairAuthority::read(self.storage.data_dir(), self.local_host_id)
            .ok()
            .flatten()
            .is_none()
        {
            let primary = if role == PairRole::Primary {
                self.local_host_id
            } else {
                peer_host_id
            };
            if let Err(error) = crate::pair::authority::PairAuthority::store(
                self.storage.data_dir(),
                self.local_host_id,
                peer_host_id,
                primary,
            ) {
                tracing::error!(%error, "cannot persist initial pair ownership; transition refused");
                self.write_path.store(Arc::new(WritePath::Unavailable));
                self.ddl_path.store(Arc::new(DdlPath::Unavailable));
                return;
            }
        }
        let role_arc = self
            .pair_context
            .lock()
            .as_ref()
            .filter(|ctx| ctx.peer_host_id == peer_host_id)
            .map(|ctx| ctx.role.clone())
            .unwrap_or_else(|| Arc::new(ArcSwap::from_pointee(role)));
        role_arc.store(Arc::new(role));

        let coordinator = match &**self.write_path.load() {
            WritePath::Pair(c) | WritePath::DegradedPair(c)
                if self
                    .pair_context
                    .lock()
                    .as_ref()
                    .is_some_and(|ctx| ctx.peer_host_id == peer_host_id) =>
            {
                c.clone()
            }
            _ => Arc::new(
                PairCoordinator::new(
                    role_arc.clone(),
                    peer_host_id,
                    self.storage.clone(),
                    peer_manager.clone(),
                )
                .with_strict_quorum(self.config.pair_quorum_strict),
            ),
        };

        // Register pair mode RPC handlers dynamically
        let write_fwd_handler = Arc::new(crate::pair::handler::PairWriteForwardHandler::new(
            role_arc.clone(),
            coordinator.clone(),
        ));
        self.registry
            .register(MsgType::PairWriteForward, write_fwd_handler);

        let role_swap_handler = Arc::new(crate::pair::switchover::RoleSwapHandler::durable(
            self.local_host_id,
            peer_host_id,
            role_arc.clone(),
            self.storage.data_dir().to_path_buf(),
            coordinator.handoff_gate().clone(),
        ));
        self.registry
            .register(MsgType::RoleSwap, role_swap_handler.clone());
        self.registry
            .register(MsgType::PairAuthority, role_swap_handler);

        // DDL coordination
        let ddl_coordinator = Arc::new(
            DdlCoordinator::new(
                role_arc.clone(),
                peer_host_id,
                self.schema.clone(),
                self.storage.clone(),
                peer_manager.clone(),
            )
            .with_handoff_gate(coordinator.handoff_gate().clone()),
        );

        let ddl_fwd_handler = Arc::new(PairDdlForwardHandler::new(
            role_arc.clone(),
            ddl_coordinator.clone(),
        ));
        self.registry
            .register(MsgType::PairDdlForward, ddl_fwd_handler);

        let schema_sync_handler = Arc::new(PairSchemaSyncHandler::new(
            self.schema.clone(),
            self.storage.clone(),
            role_arc.clone(),
        ));
        self.registry
            .register(MsgType::PairSchemaSync, schema_sync_handler);

        self.ddl_path
            .store(Arc::new(DdlPath::Pair(ddl_coordinator)));

        self.write_path
            .store(Arc::new(WritePath::pair(coordinator)));

        let pair_state = Arc::new(tokio::sync::RwLock::new(PairState::new(
            role,
            peer_host_id,
            peer_addr,
        )));
        self.cluster_state.store(Arc::new(ClusterStateHolder::Pair(
            PairClusterState::with_peer_manager(
                self.config.clone(),
                pair_state,
                peer_manager.clone(),
            ),
        )));

        // Store pair context for switchover/promote
        let catch_up = Arc::new(crate::pair::switchover::CatchUpGate::default());
        *self.pair_context.lock() = Some(PairContext {
            role: role_arc,
            peer_host_id,
            peer_addr,
            catch_up: catch_up.clone(),
        });

        if operator_override {
            self.force_mode_override(DeploymentMode::Pair, "operator downgrade: cluster -> pair");
        } else {
            self.try_transition_mode(DeploymentMode::Pair);
        }
        tracing::info!(
            %role,
            peer = %peer_host_id,
            promoted = was_promoted,
            promote_epoch = local_epoch,
            operator_override,
            "mode transition: -> pair"
        );

        // When triggered by an inbound peer connection, our peer_manager doesn't
        // have the peer registered — create a reverse outbound pool for RPC sends.
        if need_reverse {
            let pm = peer_manager.clone();
            let net_cfg = self.net_config.clone();
            let local_id = self.local_host_id;
            // Already the peer's internode address (`on_inbound_peer` passes
            // its resolved reverse address). Never `peer_addr.ip()` + our own
            // port: co-located nodes on distinct ports would dial themselves
            // (t_7c01df7e).
            let reverse_addr = peer_addr;
            if !self.admit_dial_target(peer_host_id, reverse_addr) {
                return;
            }
            let raft_rt = self.raft_runtime.get().cloned();
            let data_rt = self.data_runtime.get().cloned();
            self.spawn_tracked(async move {
                // Retry connecting to peer's RPC server with backoff.
                for attempt in 0..10 {
                    match PriorityPool::connect(
                        net_cfg.clone(),
                        local_id,
                        &reverse_addr.to_string(),
                        raft_rt.clone(),
                        data_rt.clone(),
                    )
                    .await
                    {
                        Ok(pool) => {
                            pm.add_peer((peer_host_id, reverse_addr), pool).await;
                            tracing::info!(%peer_host_id, %reverse_addr, attempt, "reverse connection established");
                            return;
                        }
                        Err(e) => {
                            if attempt < 9 {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            } else {
                                tracing::warn!(%e, attempt, "reverse connection to peer failed after retries");
                            }
                        }
                    }
                }
            });
        }

        // After force-promoted re-pairing, correct the peer's role and replay data.
        // `catch_up` stays in progress until every step is acknowledged, so a
        // switchover cannot promote the peer half caught up.
        if was_promoted {
            let pm = peer_manager;
            let storage = self.storage.clone();
            let schema = self.schema.clone();
            catch_up.begin();
            self.spawn_tracked(async move {
                // Retry sending RoleSwap until peer is ready (replaces 4s fixed sleep).
                for _attempt in 0..8 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if pm.has_peer(peer_host_id) {
                        break;
                    }
                }

                // Known pair members restart fenced. Never send legacy role assignments:
                // their unversioned grants cannot distinguish stale authority.
                // Schema before mutation replay: replayed cells are positional,
                // so they are only meaningful against the same table layout.
                if !send_schema_sync_to_peer(&pm, peer_host_id, &schema).await {
                    tracing::warn!(
                        "schema catch-up unconfirmed; not replaying data, catch-up FAILED"
                    );
                    catch_up.fail();
                    return;
                }

                // Force sync commit log to disk before replay.
                if let Err(e) = storage.force_commit_log_sync() {
                    tracing::warn!(%e, "failed to force commit log sync before catch-up replay");
                }

                if replay_commit_log_to_peer(&storage, &pm, peer_host_id).await {
                    catch_up.complete();
                } else {
                    catch_up.fail();
                }
            });
        } else if role == PairRole::Primary {
            // Normal pair rejoin (no force-promote): primary sends schema snapshot so
            // the secondary catches up on any schema changes made while it was offline.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let pm = peer_manager;
                let schema = self.schema.clone();
                handle.spawn(async move {
                    // Retry schema sync until peer's handler is ready.
                    for _attempt in 0..10 {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                        if pm.has_peer(peer_host_id) {
                            send_schema_sync_to_peer(&pm, peer_host_id, &schema).await;
                            return;
                        }
                    }
                    // Final attempt even if peer not confirmed
                    send_schema_sync_to_peer(&pm, peer_host_id, &schema).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod handoff_reconnect_tests {
    use super::super::tests::test_schema;
    use super::*;
    use crate::config::ClusterConfig;
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::PeerManager;
    use ferrosa_net::rpc::HandlerRegistry;
    use ferrosa_storage::StorageEngine;
    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }
    #[tokio::test]
    async fn verification_pair_reconnect_cannot_restore_primary_while_handoff_gate_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let local = Uuid::from_u128(1);
        let peer = Uuid::from_u128(2);
        let storage = test_storage(dir.path());
        let net = Arc::new(NetConfig::default());
        let (controller, _handles) = ModeController::new(
            Arc::new(ClusterConfig::default()),
            net.clone(),
            local,
            storage,
            test_schema(),
            Arc::new(HandlerRegistry::new()),
        );
        let manager = Arc::new(PeerManager::with_weak_listener(
            net,
            local,
            controller.as_peer_listener(),
        ));
        controller.set_peer_manager(manager);
        let addr = "127.0.0.1:7001".parse().unwrap();
        controller.transition_to_pair(peer, addr, false);
        let coordinator = match &**controller.write_path.load() {
            WritePath::Pair(c) => c.clone(),
            _ => panic!("initial pair must exist"),
        };
        let role = controller
            .pair_context
            .lock()
            .as_ref()
            .unwrap()
            .role
            .clone();
        assert_eq!(**role.load(), PairRole::Primary);
        let gate = coordinator.handoff_gate().clone();
        let handoff = gate.write().await;
        // Model a previously promoted primary after withdrawal and before the
        // durable peer-owner store in initiate_switchover. Reconnect must not
        // consume the promotion flag or restore Primary inside this interval.
        controller.force_promoted.store(true, Ordering::Release);
        role.store(Arc::new(PairRole::Secondary));
        // A disconnect during the handoff moves Pair to DegradedPair; only
        // that mode admits the reconnect transition (Pair -> Pair is refused).
        controller.transition_to_degraded();
        assert_eq!(
            controller.mode(),
            super::super::DeploymentMode::DegradedPair
        );
        controller.transition_to_pair(peer, addr, false);
        assert_eq!(
            **role.load(),
            PairRole::Secondary,
            "reconnect resurrected the transferring old primary"
        );
        assert!(
            controller.force_promoted.load(Ordering::Acquire),
            "blocked reconnect must not consume operator state"
        );
        crate::pair::authority::PairAuthority::store(dir.path(), local, peer, peer).unwrap();
        drop(handoff);
        // Positive control: reconnect works after transfer, retaining the
        // peer-owner assignment and the existing admission gate.
        controller.transition_to_pair(peer, addr, false);
        assert_eq!(**role.load(), PairRole::Secondary);
        let after = match &**controller.write_path.load() {
            WritePath::Pair(c) => c.clone(),
            _ => panic!("pair must remain installed"),
        };
        assert!(Arc::ptr_eq(after.handoff_gate(), &gate));
        controller.shutdown().await;
    }
}
