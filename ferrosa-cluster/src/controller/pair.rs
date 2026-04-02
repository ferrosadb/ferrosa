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

impl ModeController {
    pub(super) fn transition_to_pair(
        &self,
        peer_host_id: Uuid,
        peer_addr: SocketAddr,
        need_reverse: bool,
    ) {
        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to pair: peer_manager not set");
                return;
            }
        };

        // Role is determined by connection direction:
        //   need_reverse = true  → inbound connection → this node is Primary (seed)
        //   need_reverse = false → outbound connection → this node is Secondary (joiner)
        // Force-promoted nodes always stay primary regardless of direction.
        let was_promoted = self.force_promoted.swap(false, Ordering::AcqRel);
        let role = if was_promoted {
            PairRole::Primary
        } else {
            PairRole::from_connection_direction(need_reverse)
        };
        let role_arc = Arc::new(ArcSwap::from_pointee(role));

        let coordinator = Arc::new(PairCoordinator::new(
            role_arc.clone(),
            peer_host_id,
            self.storage.clone(),
            peer_manager.clone(),
        ));

        // Register pair mode RPC handlers dynamically
        let write_fwd_handler = Arc::new(crate::pair::handler::PairWriteForwardHandler::new(
            role_arc.clone(),
            coordinator.clone(),
        ));
        self.registry
            .register(MsgType::PairWriteForward, write_fwd_handler);

        let role_swap_handler = Arc::new(crate::pair::switchover::RoleSwapHandler::new(
            self.local_host_id,
            role_arc.clone(),
        ));
        self.registry.register(MsgType::RoleSwap, role_swap_handler);

        // DDL coordination
        let ddl_coordinator = Arc::new(DdlCoordinator::new(
            role_arc.clone(),
            peer_host_id,
            self.schema.clone(),
            self.storage.clone(),
            peer_manager.clone(),
        ));

        let ddl_fwd_handler = Arc::new(PairDdlForwardHandler::new(
            role_arc.clone(),
            ddl_coordinator.clone(),
        ));
        self.registry
            .register(MsgType::PairDdlForward, ddl_fwd_handler);

        let schema_sync_handler = Arc::new(PairSchemaSyncHandler::new(
            self.schema.clone(),
            self.storage.clone(),
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
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Pair(PairClusterState::new(
                self.config.clone(),
                pair_state,
            ))));

        // Store pair context for switchover/promote
        *self.pair_context.lock() = Some(PairContext {
            role: role_arc,
            peer_host_id,
            peer_addr,
        });

        self.mode.store(Arc::new(DeploymentMode::Pair));
        tracing::info!(
            %role,
            peer = %peer_host_id,
            promoted = was_promoted,
            "mode transition: standalone → pair"
        );

        // When triggered by an inbound peer connection, our peer_manager doesn't
        // have the peer registered — create a reverse outbound pool for RPC sends.
        if need_reverse {
            let pm = peer_manager.clone();
            let net_cfg = self.net_config.clone();
            let local_id = self.local_host_id;
            let internode_port = self.net_config.bind_addr.port();
            let reverse_addr = SocketAddr::new(peer_addr.ip(), internode_port);
            self.spawn_tracked(async move {
                // Small delay to let peer's RPC server be ready.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                match PriorityPool::connect(net_cfg, local_id, &reverse_addr.to_string()).await {
                    Ok(pool) => {
                        pm.add_peer((peer_host_id, reverse_addr), pool).await;
                        tracing::info!(%peer_host_id, %reverse_addr, "reverse connection established");
                    }
                    Err(e) => {
                        tracing::warn!(%e, "reverse connection to peer failed");
                    }
                }
            });
        }

        // After force-promoted re-pairing, correct the peer's role and replay data.
        if was_promoted {
            let local_id = self.local_host_id;
            let pm = peer_manager;
            let storage = self.storage.clone();
            let schema = self.schema.clone();
            self.spawn_tracked(async move {
                // Wait for reverse connection + peer pair transition + handler registration.
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;

                // Tell peer to become secondary.
                match pm
                    .send(
                        peer_host_id,
                        Message::RoleSwap {
                            new_primary: local_id,
                            new_secondary: peer_host_id,
                        },
                        Lane::Raft,
                    )
                    .await
                {
                    Ok(_) => tracing::info!("sent role correction to rejoined peer"),
                    Err(e) => {
                        tracing::warn!(%e, "failed to send role correction to peer");
                        return;
                    }
                }

                // Send schema snapshot before mutation replay.
                send_schema_sync_to_peer(&pm, peer_host_id, &schema).await;

                // Force sync commit log to disk before replay.
                if let Err(e) = storage.force_commit_log_sync() {
                    tracing::warn!(%e, "failed to force commit log sync before catch-up replay");
                }

                // Replay recent data to bring peer up to date.
                let position = CommitLogPosition {
                    segment_id: 0,
                    offset: 0,
                };
                match storage.replay_from(position) {
                    Ok(mutations) if !mutations.is_empty() => {
                        tracing::info!(count = mutations.len(), "replaying data to rejoined peer");
                        for mutation in &mutations {
                            let body = encode_mutation(mutation);
                            if let Err(e) = pm
                                .send(peer_host_id, Message::PairWriteForward(body), Lane::Data)
                                .await
                            {
                                tracing::warn!(%e, "catch-up replay send failed");
                                break;
                            }
                        }
                        tracing::info!("catch-up replay complete");
                    }
                    Ok(_) => tracing::info!("no data to replay for catch-up"),
                    Err(e) => tracing::warn!(%e, "catch-up replay_from failed"),
                }
            });
        } else if role == PairRole::Primary {
            // Normal pair rejoin (no force-promote): primary sends schema snapshot so
            // the secondary catches up on any schema changes made while it was offline.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let pm = peer_manager;
                let schema = self.schema.clone();
                handle.spawn(async move {
                    // Wait for peer to complete its pair transition and register handlers.
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    send_schema_sync_to_peer(&pm, peer_host_id, &schema).await;
                });
            }
        }
    }
}
