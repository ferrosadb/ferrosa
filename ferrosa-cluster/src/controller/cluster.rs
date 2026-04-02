//! Cluster mode transition logic: forming and full cluster.

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::message::Message;
use ferrosa_net::pool::PriorityPool;
use uuid::Uuid;

use crate::consistency::ConsistencyLevel;
use crate::coordinator::{ClusterCoordinator, RepairWriteHandler};
use crate::ddl_path::{execute_via_raft, ClusterDdlForwardHandler, DdlPath};
use crate::mode::DeploymentMode;
use crate::pair::ddl::DdlOperation;
use crate::pair::PairRole;
use crate::raft::handlers::{
    RaftAppendHandler, RaftSnapshotHandler, RaftVoteHandler, RangeReadHandler, ReadRequestHandler,
};
use crate::raft::log_store::SledLogStore;
use crate::raft::network::FerrosRaftNetworkFactory;
use crate::raft::state_machine::FerrosStateMachine;
use crate::raft::{uuid_to_node_id, FerrosRaft, NodeInfo, NodeState};
use crate::ring::TokenRing;
use crate::state::RaftClusterState;
use crate::write_path::WritePath;

use super::token::generate_deterministic_token;
use super::{ClusterStateHolder, ModeController};

impl ModeController {
    /// Transition from Pair to Forming: broadcast ClusterInvite and prepare
    /// for mesh formation. Does NOT initialize Raft — that happens in
    /// `transition_to_cluster` after all peers are connected.
    pub(super) fn transition_to_forming(&self, peers: Vec<(Uuid, SocketAddr)>) {
        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to forming: peer_manager not set");
                return;
            }
        };

        self.mode.store(Arc::new(DeploymentMode::Forming));
        // Block DDL during formation — prevents schema divergence (FMEA F3, RPN 378).
        // DDL will be re-enabled after Raft leader election in transition_to_cluster.
        self.ddl_path.store(Arc::new(DdlPath::Blocked));
        self.formation_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.seen_invite_initiators.lock().clear();
        tracing::info!(
            peer_count = peers.len(),
            epoch = self
                .formation_epoch
                .load(std::sync::atomic::Ordering::Relaxed),
            "mode transition: pair -> forming (broadcasting ClusterInvite)"
        );

        // Broadcast ClusterInvite to all connected peers so they discover each other.
        let local_id = self.local_host_id;
        let listen_addr = self.net_config.broadcast_addr;
        let peers_for_invite = peers.clone();
        let pm_clone = peer_manager.clone();
        self.spawn_tracked(async move {
            let invite = Message::ClusterInvite {
                initiator: local_id,
                peers: peers_for_invite
                    .iter()
                    .map(|(id, addr)| (*id, *addr))
                    .chain(std::iter::once((local_id, listen_addr)))
                    .collect(),
            };

            for (peer_id, _) in &peers_for_invite {
                if let Err(e) = pm_clone.fire(*peer_id, invite.clone(), Lane::Raft).await {
                    tracing::warn!(peer = %peer_id, %e, "failed to send ClusterInvite");
                }
            }
        });

        // Record when we entered Forming — the timeout check happens in the
        // Raft init background task (transition_to_cluster) and also in
        // on_peer_connected if mode is still Forming.
        // The actual timeout logic is inside transition_to_cluster's leader
        // election poll: if election doesn't complete in 30s AND formation
        // timeout is exceeded, the mode reverts to Pair.

        // Now proceed to cluster transition with all known peers.
        // In the future, this will wait for mesh completion before Raft init.
        // For now, proceed immediately (matches current behavior).
        self.transition_to_cluster(peers);
    }

    /// 4. TokenRing with deterministic initial token assignment
    /// 5. ClusterCoordinator for replica-aware writes
    /// 6. Swaps write path, DDL path, and cluster state atomically
    pub(super) fn transition_to_cluster(&self, peers: Vec<(Uuid, SocketAddr)>) {
        let peer_manager = match &**self.peer_manager.load() {
            Some(pm) => pm.clone(),
            None => {
                tracing::error!("cannot transition to cluster: peer_manager not set");
                return;
            }
        };

        // Capture whether this node was the seed (Primary) before we clear pair context.
        // Only the seed calls raft.initialize() — others wait for AppendEntries.
        let was_seed = self.role() == Some(PairRole::Primary);

        // 0. Ensure PeerManager has outbound connections to ALL peers.
        //
        // BUG FIX: When transitioning from pair → cluster, only the first peer
        // (from transition_to_pair) has a reverse outbound pool. The second peer
        // (which triggered this transition) may only have an inbound connection.
        // Raft needs to SEND to all peers, so we must create outbound pools for
        // any peer the PeerManager doesn't already know about.
        let net_cfg = self.net_config.clone();
        let local_id = self.local_host_id;
        let internode_port = self.net_config.bind_addr.port();
        for (peer_uuid, peer_addr) in &peers {
            if !peer_manager.has_peer(*peer_uuid) {
                let pm = peer_manager.clone();
                let cfg = net_cfg.clone();
                let uuid = *peer_uuid;
                let reverse_addr = SocketAddr::new(peer_addr.ip(), internode_port);
                tokio::spawn(async move {
                    match PriorityPool::connect(cfg, local_id, &reverse_addr.to_string()).await {
                        Ok(pool) => {
                            pm.add_peer((uuid, reverse_addr), pool).await;
                            tracing::info!(%uuid, %reverse_addr, "cluster: reverse connection established");
                        }
                        Err(e) => {
                            tracing::warn!(%uuid, %e, "cluster: reverse connection failed");
                        }
                    }
                });
            }
        }

        // 1. Create sled log store
        let raft_dir = if let Some(ref dir) = self.config.raft_data_dir {
            dir.clone()
        } else {
            let data_dir =
                std::env::var("FERROSA_DATA_DIR").unwrap_or_else(|_| "/var/lib/ferrosa".into());
            std::path::Path::new(&data_dir).join("raft")
        };
        let log_store = match SledLogStore::new(&raft_dir) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(%e, "failed to create Raft log store");
                return;
            }
        };

        // 2. Create state machine from current schema
        let mut state_machine =
            FerrosStateMachine::with_side_effects(self.schema.clone(), self.storage.clone());

        // 3. Create network factory
        let network_factory = FerrosRaftNetworkFactory::new(peer_manager.clone());
        let local_node_id = uuid_to_node_id(self.local_host_id);

        // Register node mappings for all peers
        for (peer_uuid, _addr) in &peers {
            let peer_node_id = uuid_to_node_id(*peer_uuid);
            network_factory.register_node(peer_node_id, *peer_uuid);
        }
        // Register self
        network_factory.register_node(local_node_id, self.local_host_id);

        // Capture the node_map Arc before the factory is consumed by FerrosRaft::new.
        // This shared map is used by DdlPath::Cluster to resolve leader NodeId → Uuid.
        let node_map_for_ddl = network_factory.node_map();
        // Clone peer_manager for DdlPath::Cluster forwarding (ClusterCoordinator
        // will consume `peer_manager` below).
        let peer_manager_for_ddl = peer_manager.clone();

        // 4. Build TokenRing with deterministic initial tokens
        let mut ring = TokenRing::new();

        // Add local node
        let broadcast = self.net_config.broadcast_addr.to_string();
        ring.add_node(
            local_node_id,
            NodeInfo {
                host_id: self.local_host_id,
                addr: broadcast,
                data_center: self.config.data_center.clone(),
                rack: self.config.rack.clone(),
                state: NodeState::Normal,
            },
        );

        // Add peers
        for (peer_uuid, addr) in &peers {
            let peer_node_id = uuid_to_node_id(*peer_uuid);
            ring.add_node(
                peer_node_id,
                NodeInfo {
                    host_id: *peer_uuid,
                    addr: addr.to_string(),
                    data_center: self.config.data_center.clone(),
                    rack: self.config.rack.clone(),
                    state: NodeState::Normal,
                },
            );
        }

        // Assign deterministic tokens to all nodes (256 per node).
        // Uses node_id XOR with index to produce deterministic, well-distributed tokens.
        let num_tokens = self.config.num_tokens as usize;
        let mut all_node_ids: Vec<u64> = vec![local_node_id];
        for (peer_uuid, _) in &peers {
            all_node_ids.push(uuid_to_node_id(*peer_uuid));
        }
        all_node_ids.sort_unstable(); // deterministic order

        for &nid in &all_node_ids {
            let tokens: Vec<i64> = (0..num_tokens)
                .map(|i| generate_deterministic_token(nid, i))
                .collect();
            ring.assign_tokens(nid, &tokens);
        }

        let ring_arc = Arc::new(ArcSwap::from_pointee(ring));

        // Seed the state machine with the initial topology so that
        // sync_ring() won't overwrite the ring with empty state.
        {
            let mut members = std::collections::BTreeMap::new();
            let mut token_map = std::collections::BTreeMap::new();
            let ring_snap = ring_arc.load();
            for &nid in &all_node_ids {
                if let Some(info) = ring_snap.get_node(nid) {
                    members.insert(nid, info.clone());
                }
            }
            for &nid in &all_node_ids {
                for tok in ring_snap.tokens_for_node(nid) {
                    token_map.insert(tok, nid);
                }
            }
            state_machine.seed_topology(members, token_map);
            state_machine.set_ring(ring_arc.clone());
        }

        // Expose the live ring snapshot for observability (web API, CLI).
        // We capture a snapshot of the ring at this point; it will be updated
        // by the Raft state machine as tokens are reassigned.
        {
            let ring_snapshot = Arc::new((**ring_arc.load()).clone());
            self.set_token_ring(ring_snapshot);
        }

        // 5. Create coordinator
        let coordinator = Arc::new(ClusterCoordinator::new(
            ring_arc.clone(),
            peer_manager,
            local_node_id,
            self.storage.clone(),
            3, // default RF
            ConsistencyLevel::Quorum,
        ));

        let repair_metrics_for_handler = coordinator.repair_metrics.clone();

        // 6. Swap write path — cluster coordinator handles replica routing
        self.write_path
            .store(Arc::new(WritePath::cluster(coordinator)));

        // DdlPath::Cluster needs the Raft instance — Raft initialization is async
        // and happens in a background task. Keep DDL on Direct path during the
        // transition window so standalone/pair DDL continues to work. Once Raft
        // is initialized and a leader is elected, the background task will:
        //   1. Swap DDL path to DdlPath::Cluster
        //   2. Replay the current local schema state through Raft so all
        //      followers converge on the same schema
        self.ddl_path.store(Arc::new(DdlPath::Direct {
            schema: self.schema.clone(),
            engine: self.storage.clone(),
        }));

        // Swap cluster state to Raft-based
        self.cluster_state
            .store(Arc::new(ClusterStateHolder::Cluster(
                RaftClusterState::new(ring_arc, local_node_id),
            )));

        // Clear pair context — no longer in pair mode
        *self.pair_context.lock() = None;

        self.mode.store(Arc::new(DeploymentMode::Cluster));

        tracing::info!(
            node_id = local_node_id,
            peers = peers.len(),
            "mode transition: pair -> cluster (raft init spawned)"
        );

        // Spawn background Raft initialization — Raft::new() is async and
        // must not block the PeerEventListener callback.
        let raft_instance_swap = self.raft_instance.clone();
        let ddl_path = self.ddl_path.clone();
        let mode_swap = self.mode.clone();
        let registry = self.registry.clone();
        let storage_for_handler = self.storage.clone();
        let repair_metrics = repair_metrics_for_handler;
        let cluster_name = self.config.cluster_name.clone();
        let schema_for_replay = self.schema.clone();
        self.spawn_tracked(async move {
            // Build openraft Config
            let raft_config = match (openraft::Config {
                cluster_name,
                heartbeat_interval: 300,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                max_payload_entries: 100,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
                ..Default::default()
            })
            .validate()
            {
                Ok(cfg) => Arc::new(cfg),
                Err(e) => {
                    tracing::error!(%e, "invalid raft config, staying in cluster mode without raft DDL");
                    return;
                }
            };

            // Create the Raft instance
            let raft = match FerrosRaft::new(
                local_node_id,
                raft_config,
                network_factory,
                log_store,
                state_machine,
            )
            .await
            {
                Ok(r) => r,
                Err(fatal) => {
                    tracing::error!(%fatal, "raft initialization failed (Fatal), DDL remains on direct path");
                    return;
                }
            };

            let raft_arc = Arc::new(raft);

            // Register Raft RPC handlers so peers can reach this node's Raft
            let append_handler = Arc::new(RaftAppendHandler::new((*raft_arc).clone()));
            registry.register(MsgType::RaftAppendEntries, append_handler);

            let vote_handler = Arc::new(RaftVoteHandler::new((*raft_arc).clone()));
            registry.register(MsgType::RaftVote, vote_handler);

            let snapshot_handler = Arc::new(RaftSnapshotHandler::new((*raft_arc).clone()));
            registry.register(MsgType::RaftInstallSnapshot, snapshot_handler);

            let repair_handler = Arc::new(RepairWriteHandler::new(
                storage_for_handler.clone(),
                repair_metrics,
            ));
            registry.register(MsgType::RepairWrite, repair_handler);

            let range_read_handler = Arc::new(RangeReadHandler::new(storage_for_handler.clone()));
            registry.register(MsgType::RangeReadRequest, range_read_handler);

            let read_handler = Arc::new(ReadRequestHandler::new(storage_for_handler));
            registry.register(MsgType::ReadRequest, read_handler);

            // Build initial membership: all known nodes including self
            let mut members = std::collections::BTreeMap::new();
            members.insert(
                local_node_id,
                openraft::BasicNode {
                    addr: String::new(),
                },
            );
            for (peer_uuid, addr) in &peers {
                let peer_node_id = uuid_to_node_id(*peer_uuid);
                members.insert(
                    peer_node_id,
                    openraft::BasicNode {
                        addr: addr.to_string(),
                    },
                );
            }

            // Only the seed (original Primary) calls initialize().
            // Non-seed nodes will receive their membership via AppendEntries
            // from the leader. This prevents CF-T17 (membership race from
            // independent initialize() calls with potentially different member lists).
            if was_seed {
                if let Err(e) = raft_arc.initialize(members).await {
                    // InitializeError::NotAllowed means the cluster was already
                    // initialized (e.g. from a prior run with persisted log).
                    // That is not fatal — the node will join the existing cluster.
                    tracing::warn!(%e, "raft initialize returned error (may be already initialized)");
                }
            } else {
                tracing::info!("non-seed node — skipping raft.initialize(), waiting for leader AppendEntries");
            }

            // Wait for leader election (poll with backoff, max ~30s)
            let mut leader = None;
            for attempt in 0..60 {
                if let Some(lid) = raft_arc.current_leader().await {
                    leader = Some(lid);
                    break;
                }
                let backoff =
                    std::time::Duration::from_millis(if attempt < 10 { 100 } else { 500 });
                tokio::time::sleep(backoff).await;
            }

            match leader {
                Some(lid) => {
                    tracing::info!(
                        leader = lid,
                        "raft leader elected, swapping DDL path to Cluster"
                    );
                    // Register the cluster DDL forward handler so that when a
                    // non-leader forwards a PairDdlForward to the leader, the
                    // leader proposes it through Raft rather than applying
                    // directly (which would bypass consensus).
                    let cluster_ddl_handler =
                        Arc::new(ClusterDdlForwardHandler::new(raft_arc.clone()));
                    registry.register(MsgType::PairDdlForward, cluster_ddl_handler);

                    ddl_path.store(Arc::new(DdlPath::Cluster {
                        raft: raft_arc.clone(),
                        peer_manager: peer_manager_for_ddl,
                        node_map: node_map_for_ddl,
                    }));

                    // Replay local schema state through Raft so all followers
                    // converge. Any DDL applied via the Direct path during the
                    // transition window is now proposed through consensus.
                    if lid == local_node_id {
                        tracing::info!("replaying local schema state through Raft for follower convergence");
                        let schema_snap = schema_for_replay.snapshot();
                        for (name, ks) in &schema_snap.keyspaces {
                            // Skip system keyspaces — they exist on all nodes.
                            if name.starts_with("system") {
                                continue;
                            }
                            let op = DdlOperation::CreateKeyspace(ks.clone());
                            if let Err(e) = execute_via_raft(&raft_arc, op).await {
                                tracing::warn!(%e, ks = %name, "schema replay: CreateKeyspace failed (may already exist)");
                            }
                        }
                        for ((ks, _tbl), table) in &schema_snap.tables {
                            if ks.starts_with("system") {
                                continue;
                            }
                            let op = DdlOperation::CreateTable(Box::new(table.clone()));
                            if let Err(e) = execute_via_raft(&raft_arc, op).await {
                                tracing::warn!(%e, "schema replay: CreateTable failed (may already exist)");
                            }
                        }
                    }
                }
                None => {
                    tracing::warn!(
                        "raft leader election timed out after 30s — reverting to Pair mode"
                    );
                    // Revert to Pair mode — formation failed. The Raft instance
                    // is stored but non-functional (no leader). Writes stay on
                    // Pair semantics with the original peer.
                    mode_swap.store(Arc::new(DeploymentMode::Pair));
                }
            }

            // Store the raft instance so it is accessible via controller.raft()
            raft_instance_swap.store(Arc::new(Some(raft_arc)));
        });
    }
}
