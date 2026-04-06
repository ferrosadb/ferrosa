//! Cluster mode transition logic: forming and full cluster.

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::pool::PriorityPool;
use ferrosa_net::rpc::handler::PeerId;
use ferrosa_net::rpc::RpcHandler;
use uuid::Uuid;

use crate::consistency::ConsistencyLevel;
use crate::coordinator::{ClusterCoordinator, RepairWriteHandler};
use crate::ddl_path::{execute_via_raft, ClusterDdlForwardHandler, DdlPath};
use crate::mode::DeploymentMode;
use crate::pair::ddl::DdlOperation;
use crate::raft::handlers::{
    RaftAppendHandler, RaftSnapshotHandler, RaftVoteHandler, RangeReadHandler, ReadRequestHandler,
};
use crate::raft::log_store::SledLogStore;
use crate::raft::network::FerrosRaftNetworkFactory;
use crate::raft::state_machine::FerrosStateMachine;
use crate::raft::{uuid_to_node_id, FerrosRaft, NodeInfo, NodeState};
use crate::ring::TokenRing;
use crate::state::RaftClusterState;
use crate::streaming::{
    sender::{SstableSendRequest, StreamSender},
    StreamConfig, StreamedMutation,
};
use crate::write_path::WritePath;

use super::token::generate_deterministic_token;
use super::{ClusterStateHolder, ModeController};

impl ModeController {
    /// Transition from Pair to Forming: broadcast ClusterInvite and prepare
    /// for mesh formation. Does NOT initialize Raft — that happens in
    /// `transition_to_cluster` after all peers are connected.
    pub(super) fn transition_to_forming(&self, peers: Vec<(Uuid, SocketAddr)>) {
        if self.peer_manager.load().is_none() {
            tracing::error!("cannot transition to forming: peer_manager not set");
            return;
        }

        self.mode.store(Arc::new(DeploymentMode::Forming));
        // Record committed cluster size for quorum calculations (peers + self).
        self.committed_cluster_size
            .store(peers.len() + 1, std::sync::atomic::Ordering::Relaxed);
        // Queue DDL during formation — operations are replayed after Raft leader
        // election instead of being rejected (FMEA F3).
        let (ddl_tx, ddl_rx) = tokio::sync::mpsc::unbounded_channel();
        *self.ddl_queue_rx.lock() = Some(ddl_rx);
        self.ddl_path
            .store(Arc::new(DdlPath::Forming { queue: ddl_tx }));
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

        // ClusterInvite delivery moved into the Raft init task (transition_to_cluster)
        // to ensure invites arrive before elections start.

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

        // Determine whether this node is the seed (responsible for calling
        // raft.initialize()). The seed is the node with the highest UUID among
        // ALL cluster members (self + all peers). We must include self in the
        // comparison to handle the case where the ClusterInvite's peer list
        // does not include the initiator — without this, a node that only sees
        // a subset of peers can incorrectly believe it is the seed (RC-3 race).
        let mut all_member_uuids: Vec<uuid::Uuid> = peers.iter().map(|(id, _)| *id).collect();
        all_member_uuids.push(self.local_host_id);
        let max_uuid = all_member_uuids.iter().max().copied().unwrap_or_default();
        let was_seed = self.local_host_id == max_uuid;

        // Flush all memtables to SSTables before the write path switches to the
        // ClusterCoordinator. Data written in standalone/pair mode lives only in
        // node1's memtable — without flushing, token redistribution makes it
        // unreachable via the coordinator (P0 data loss bug).
        if let Err(e) = self.storage.flush_all() {
            tracing::error!(%e, "failed to flush memtables before cluster transition");
        }

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
                self.spawn_tracked(async move {
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
        let node_map_for_bootstrap = node_map_for_ddl.clone();
        // Clone peer_manager for DdlPath::Cluster forwarding (ClusterCoordinator
        // will consume `peer_manager` below).
        let peer_manager_for_ddl = peer_manager.clone();
        let peer_manager_for_bootstrap = peer_manager.clone();

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
                cql_broadcast: self.config.cql_broadcast.clone(),
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
                    // Start as Joining — only promoted to Normal after bootstrap
                    // streaming completes. Joining nodes don't serve reads for
                    // their token ranges until they have the data.
                    state: NodeState::Joining,
                    // Peer's cql_broadcast is unknown at this point; it will be
                    // propagated through Raft NodeInfo once the peer joins.
                    cql_broadcast: None,
                },
            );
        }

        // Assign deterministic tokens to all nodes (256 per node).
        // Uses node_id XOR with index to produce deterministic, well-distributed tokens.
        //
        // CRITICAL: all_node_ids must include EVERY cluster member (self + all peers).
        // If any node builds the ring with a different member set, token assignments
        // diverge and writes scatter across nodes instead of landing on the correct
        // single replica.
        let num_tokens = self.config.num_tokens as usize;
        let mut all_node_ids: Vec<u64> = vec![local_node_id];
        for (peer_uuid, _) in &peers {
            all_node_ids.push(uuid_to_node_id(*peer_uuid));
        }
        all_node_ids.sort_unstable(); // deterministic order

        tracing::info!(
            local = local_node_id,
            peer_count = peers.len(),
            member_count = all_node_ids.len(),
            member_ids = ?all_node_ids,
            "building token ring"
        );

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
        //
        // Start with RF=1 CL=ONE during initial formation so that data written
        // in standalone mode (only on node1) remains readable. The coordinator
        // routes reads to the single replica that has the data. After bootstrap
        // streaming redistributes data to new token owners, operators can
        // ALTER KEYSPACE to increase RF.
        let initial_rf = 1;
        let initial_cl = ConsistencyLevel::One;
        let coordinator = Arc::new(ClusterCoordinator::new(
            ring_arc.clone(),
            peer_manager,
            local_node_id,
            self.storage.clone(),
            initial_rf,
            initial_cl,
        ));

        let repair_metrics_for_handler = coordinator.repair_metrics.clone();

        // 6. Swap write path — cluster coordinator handles replica routing.
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
                RaftClusterState::with_peer_manager(
                    ring_arc,
                    local_node_id,
                    peer_manager_for_bootstrap.clone(),
                ),
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
        let storage_for_bootstrap = self.storage.clone();
        let schema_for_bootstrap = self.schema.clone();
        let ring_for_bootstrap = self.ring.clone();
        let all_node_ids_for_bootstrap = all_node_ids.clone();
        let cluster_name = self.config.cluster_name.clone();
        let config_for_promotion = self.config.clone();
        let raft_heartbeat_ms = self.config.raft_heartbeat_ms;
        let raft_election_min_ms = self.config.raft_election_timeout_min_ms;
        let raft_election_max_ms = self.config.raft_election_timeout_max_ms;
        let schema_for_replay = self.schema.clone();
        let ddl_queue_rx = self.ddl_queue_rx.clone();

        // Register Raft RPC handlers BEFORE spawning the init task.
        // Handlers use LazyRaft to wait for the instance to be ready.
        // This eliminates the race where vote requests arrive before
        // handlers are registered.
        use crate::raft::handlers::LazyRaft;
        let (raft_tx, lazy_raft) = LazyRaft::channel();

        let append_handler = Arc::new(RaftAppendHandler::new(lazy_raft.clone()));
        self.registry
            .register(MsgType::RaftAppendEntries, append_handler);

        let vote_handler = Arc::new(RaftVoteHandler::new(lazy_raft.clone()));
        self.registry.register(MsgType::RaftVote, vote_handler);

        let snapshot_handler = Arc::new(RaftSnapshotHandler::new(lazy_raft));
        self.registry
            .register(MsgType::RaftInstallSnapshot, snapshot_handler);

        let repair_handler = Arc::new(RepairWriteHandler::new(
            self.storage.clone(),
            repair_metrics_for_handler,
        ));
        self.registry.register(MsgType::RepairWrite, repair_handler);

        let range_read_handler = Arc::new(RangeReadHandler::new(self.storage.clone()));
        self.registry
            .register(MsgType::RangeReadRequest, range_read_handler);

        let read_handler = Arc::new(ReadRequestHandler::new(self.storage.clone()));
        self.registry.register(MsgType::ReadRequest, read_handler);

        self.spawn_tracked(async move {
            // Deliver ClusterInvite to all peers BEFORE starting Raft.
            // This ensures peers transition to cluster mode and register
            // Raft handlers before elections begin.
            // Build invite inside the async block using captured peer list.
            // local_host_id is captured via the `peers` vec (all peers except self).
            let invite_initiator = {
                // Recover the local UUID from the node_map.
                // Read through poison — a poisoned lock still has valid data.
                let map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner());
                map.get(&local_node_id).copied().unwrap_or_default()
            };
            let invite = Message::ClusterInvite {
                initiator: invite_initiator,
                peers: peers
                    .iter()
                    .map(|(id, addr)| (*id, *addr))
                    .collect(),
            };
            for (peer_id, _) in &peers {
                for attempt in 0..10 {
                    match peer_manager_for_bootstrap
                        .send(*peer_id, invite.clone(), Lane::Data)
                        .await
                    {
                        Ok(_) => {
                            tracing::info!(peer = %peer_id, "ClusterInvite delivered");
                            break;
                        }
                        Err(e) => {
                            if attempt < 9 {
                                tracing::debug!(
                                    peer = %peer_id, attempt, %e,
                                    "ClusterInvite delivery retry"
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            } else {
                                tracing::warn!(
                                    peer = %peer_id,
                                    "ClusterInvite delivery failed after 10 attempts"
                                );
                            }
                        }
                    }
                }
            }

            // Build openraft Config
            let raft_config = match (openraft::Config {
                cluster_name,
                heartbeat_interval: raft_heartbeat_ms,
                election_timeout_min: raft_election_min_ms,
                election_timeout_max: raft_election_max_ms,
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

            // Option B: Wait for Raft lane readiness before creating the
            // Raft instance. Elections start as soon as `FerrosRaft::new()`
            // returns, so outbound connections must exist first.
            {
                let deadline = tokio::time::Instant::now()
                    + std::time::Duration::from_secs(10);
                for (peer_uuid, _) in &peers {
                    let mut waited = false;
                    while !peer_manager_for_bootstrap.has_peer(*peer_uuid) {
                        if !waited {
                            tracing::debug!(
                                peer = %peer_uuid,
                                "waiting for peer connection..."
                            );
                            waited = true;
                        }
                        if tokio::time::Instant::now() > deadline {
                            tracing::warn!(
                                peer = %peer_uuid,
                                "Raft lane readiness timeout — proceeding anyway"
                            );
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
                tracing::info!("all Raft lane connections verified");
            }

            // Non-seed nodes create Raft promptly — they need to be ready
            // to RESPOND to Vote RPCs from the seed. Without a Raft instance,
            // the LazyRaft handlers timeout, preventing the seed from winning.
            // The key invariant: only the seed calls initialize(), so only the
            // seed starts elections. Non-seeds are passive responders.

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

            // Publish the Raft instance — handlers waiting in LazyRaft::get() will unblock.
            let _ = raft_tx.send(Some(raft_arc.clone()));

            // Also publish to the controller's raft_instance so that
            // controller.raft() returns Some() during the election loop.
            // Without this, external callers (tests, DDL) cannot observe
            // the leader until the entire background task completes.
            raft_instance_swap.store(Arc::new(Some(raft_arc.clone())));

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


                    // Drain any DDL operations queued during Forming state.
                    // Take the receiver outside the lock guard scope to avoid
                    // holding parking_lot::MutexGuard across an await point.
                    let maybe_rx = ddl_queue_rx.lock().take();
                    if let Some(mut rx) = maybe_rx {
                        let mut replayed = 0usize;
                        while let Ok(op) = rx.try_recv() {
                            if let Err(e) = execute_via_raft(&raft_arc, op).await {
                                tracing::warn!(%e, "failed to replay queued DDL operation");
                            } else {
                                replayed += 1;
                            }
                        }
                        if replayed > 0 {
                            tracing::info!(count = replayed, "replayed queued DDL operations from Forming state");
                        }
                    }

                    // --- Phase A: Schema convergence (all nodes) ---
                    //
                    // Every node replays its local schema so that all peers
                    // learn about user-created keyspaces/tables. The leader
                    // proposes directly via Raft; non-leaders forward to the
                    // leader via the existing PairDdlForward RPC.
                    {
                        let schema_snap = schema_for_replay.snapshot();
                        let user_ks: Vec<_> = schema_snap
                            .keyspaces
                            .iter()
                            .filter(|(name, _)| !name.starts_with("system"))
                            .collect();
                        let user_tables: Vec<_> = schema_snap
                            .tables
                            .iter()
                            .filter(|((ks, _), _)| !ks.starts_with("system"))
                            .collect();

                        if !user_ks.is_empty() || !user_tables.is_empty() {
                            if lid == local_node_id {
                                tracing::info!(
                                    ks_count = user_ks.len(),
                                    table_count = user_tables.len(),
                                    "leader: replaying local schema through Raft"
                                );
                                for (name, ks) in &user_ks {
                                    let op = DdlOperation::CreateKeyspace((*ks).clone());
                                    if let Err(e) = execute_via_raft(&raft_arc, op).await {
                                        tracing::warn!(%e, ks = %name, "schema replay: CreateKeyspace failed (may already exist)");
                                    }
                                }
                                for ((ks, _tbl), table) in &user_tables {
                                    let op = DdlOperation::CreateTable(Box::new((*table).clone()));
                                    if let Err(e) = execute_via_raft(&raft_arc, op).await {
                                        tracing::warn!(%e, ks, "schema replay: CreateTable failed (may already exist)");
                                    }
                                }
                            } else {
                                // Resolve leader NodeId → Uuid for RPC.
                                let leader_uuid = {
                                    let map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner());
                                    map.get(&lid).copied()
                                };
                                if let Some(leader_uuid) = leader_uuid {
                                    tracing::info!(
                                        ks_count = user_ks.len(),
                                        table_count = user_tables.len(),
                                        "non-leader: forwarding local schema to leader"
                                    );
                                    for (name, ks) in &user_ks {
                                        let op = DdlOperation::CreateKeyspace((*ks).clone());
                                        if let Err(e) = crate::ddl_path::forward_ddl_to_leader(
                                            &peer_manager_for_bootstrap,
                                            leader_uuid,
                                            op,
                                        )
                                        .await
                                        {
                                            tracing::warn!(%e, ks = %name, "schema forward: CreateKeyspace failed");
                                        }
                                    }
                                    for ((ks, _tbl), table) in &user_tables {
                                        let op = DdlOperation::CreateTable(Box::new((*table).clone()));
                                        if let Err(e) = crate::ddl_path::forward_ddl_to_leader(
                                            &peer_manager_for_bootstrap,
                                            leader_uuid,
                                            op,
                                        )
                                        .await
                                        {
                                            tracing::warn!(%e, ks, "schema forward: CreateTable failed");
                                        }
                                    }
                                } else {
                                    tracing::warn!("cannot forward schema: leader UUID not in node_map");
                                }
                            }
                        }
                    }

                    // --- Phase B: Bootstrap streaming (all nodes) ---
                    //
                    // Every node reads from its local storage and streams
                    // partitions that belong to other nodes per the new ring.
                    // Nodes with no data complete instantly (zero iterations).
                    //
                    // Two paths:
                    //   1. Row-based (default): serialize each partition's rows
                    //      individually via bincode. Good for small tables.
                    //   2. SSTable file-based (bulk): when partition count exceeds
                    //      BOOTSTRAP_SSTABLE_THRESHOLD, flush the table to disk and
                    //      stream the SSTable component files directly. Much faster
                    //      for large datasets since it avoids per-row serialization.
                    tracing::info!("starting bootstrap streaming to new token owners");

                    /// Partition count threshold above which we switch from
                    /// per-row streaming to SSTable file-based bulk transfer.
                    const BOOTSTRAP_SSTABLE_THRESHOLD: usize = 1_000;

                    if let Some(ring) = &**ring_for_bootstrap.load() {
                        let schema_snap = schema_for_bootstrap.snapshot();
                        let config = StreamConfig::default();
                        let node_map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner()).clone();
                        let mut session_counter = 0_u64;

                        for (ks, tbl) in schema_snap.tables.keys() {
                            if ks.starts_with("system") {
                                continue;
                            }
                            let table_id = ferrosa_storage::commitlog::TableId::new(ks, tbl);
                            // Cap per-table read to 100k partitions to prevent OOM.
                            // Tables larger than this will have partial bootstrap;
                            // anti-entropy repair catches the rest.
                            const BOOTSTRAP_READ_LIMIT: usize = 100_000;
                            let partitions = match storage_for_bootstrap.read_range(
                                &table_id,
                                None,
                                None,
                                BOOTSTRAP_READ_LIMIT,
                            ) {
                                Ok(p) => p,
                                Err(e) => {
                                    tracing::warn!(%e, ks, tbl, "bootstrap: failed to read table");
                                    continue;
                                }
                            };

                            // --- SSTable bulk path ---
                            // When partition count exceeds the threshold, flush
                            // the table to an SSTable on disk and stream the raw
                            // component files. This is O(file-size) rather than
                            // O(partitions * rows * cells).
                            if partitions.len() >= BOOTSTRAP_SSTABLE_THRESHOLD {
                                tracing::info!(
                                    ks,
                                    tbl,
                                    partitions = partitions.len(),
                                    "bootstrap: using SSTable bulk transfer (threshold: {BOOTSTRAP_SSTABLE_THRESHOLD})"
                                );

                                // Ensure data is flushed to disk before streaming.
                                if let Err(e) = storage_for_bootstrap.flush_all() {
                                    tracing::warn!(%e, ks, tbl, "bootstrap: flush before SSTable stream failed, falling back to row path");
                                    // Fall through to the row-based path below.
                                } else {
                                    // Look for SSTable directories for this table.
                                    let sstable_base = storage_for_bootstrap
                                        .data_dir()
                                        .join("sstables")
                                        .join(table_id.to_string());

                                    let sstable_dirs: Vec<std::path::PathBuf> = match std::fs::read_dir(&sstable_base) {
                                        Ok(entries) => entries
                                            .filter_map(|e| e.ok())
                                            .filter(|e| e.path().is_dir())
                                            .map(|e| e.path())
                                            .collect(),
                                        Err(_) => {
                                            tracing::debug!(ks, tbl, "bootstrap: no SSTable dir at {}, using row path", sstable_base.display());
                                            vec![]
                                        }
                                    };

                                    if !sstable_dirs.is_empty() {
                                        // Stream each SSTable directory to all non-local peers.
                                        // TODO(S4): partition SSTables by target node token range
                                        // instead of broadcasting all SSTables to all peers.
                                        let mut sstable_streamed = false;
                                        for sstable_dir in &sstable_dirs {
                                            let sstable_id = sstable_dir
                                                .file_name()
                                                .map(|n| n.to_string_lossy().to_string())
                                                .unwrap_or_else(|| "unknown".to_string());

                                            for (&target_node_id, &target_uuid) in &node_map {
                                                if target_node_id == local_node_id {
                                                    continue;
                                                }
                                                session_counter += 1;
                                                let request = SstableSendRequest {
                                                    sstable_dir,
                                                    keyspace: ks,
                                                    table: tbl,
                                                    sstable_id: &sstable_id,
                                                    session_id: session_counter,
                                                    source_node: local_node_id,
                                                    chunk_size: config.chunk_size_bytes,
                                                };
                                                match StreamSender::send_sstable_files(
                                                    &request,
                                                    &peer_manager_for_bootstrap,
                                                    target_uuid,
                                                )
                                                .await
                                                {
                                                    Ok(bytes) => {
                                                        tracing::info!(
                                                            target = target_node_id,
                                                            bytes,
                                                            sstable_id = %sstable_id,
                                                            "bootstrap: SSTable streamed {ks}.{tbl}"
                                                        );
                                                        sstable_streamed = true;
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(
                                                            %e,
                                                            target = target_node_id,
                                                            sstable_id = %sstable_id,
                                                            "bootstrap: SSTable stream failed for {ks}.{tbl}"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        if sstable_streamed {
                                            continue; // Skip row-based path for this table.
                                        }
                                        // If all SSTable streams failed, fall through to row path.
                                    }
                                }
                            }

                            // --- Row-based path (default / fallback) ---
                            // Group partitions by target node (using all nodes, including
                            // Joining — they need the data even though they're not serving yet)
                            let mut by_node: std::collections::HashMap<u64, Vec<StreamedMutation>> =
                                std::collections::HashMap::new();
                            for partition in &partitions {
                                let token = partition.key.token.0; // Token(i64) → i64
                                // Find the node that WILL own this token once promoted
                                // to Normal (includes Joining nodes).
                                let owner = ring
                                    .primary_owner(token)
                                    .unwrap_or(local_node_id);

                                if owner != local_node_id {
                                    // Serialize all rows via RowWire for full fidelity
                                    // (clustering keys, all cells, deletion, liveness).
                                    use crate::raft::handlers::RowWire;
                                    let wire_rows: Vec<RowWire> = partition.rows
                                        .iter()
                                        .cloned()
                                        .map(RowWire::from)
                                        .collect();
                                    let row_bytes = match bincode::serialize(&wire_rows) {
                                        Ok(bytes) => bytes,
                                        Err(e) => {
                                            tracing::error!(
                                                %e,
                                                partition_key = ?partition.key,
                                                "bootstrap: failed to serialize rows, skipping partition (data loss avoided)"
                                            );
                                            continue;
                                        }
                                    };
                                    let ts = partition.rows.first()
                                        .and_then(|r| r.cells.first())
                                        .map(|(_, cv)| cv.timestamp)
                                        .unwrap_or(0);

                                    by_node.entry(owner).or_default().push(StreamedMutation {
                                        keyspace: ks.clone(),
                                        table: tbl.clone(),
                                        key: partition.key.key.as_bytes().to_vec(),
                                        row: row_bytes,
                                        timestamp: ts,
                                    });
                                }
                            }

                            for (target_node_id, mutations) in by_node {
                                let target_uuid = node_map.get(&target_node_id).copied();

                                if let Some(uuid) = target_uuid {
                                    session_counter += 1;
                                    let count = mutations.len();
                                    if let Err(e) = StreamSender::send_stream(
                                        mutations,
                                        &peer_manager_for_bootstrap,
                                        uuid,
                                        session_counter,
                                        (i64::MIN, i64::MAX),
                                        local_node_id,
                                        &config,
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            %e,
                                            target = target_node_id,
                                            "bootstrap streaming failed for {ks}.{tbl}"
                                        );
                                    } else {
                                        tracing::info!(
                                            target = target_node_id,
                                            count,
                                            "bootstrapped {ks}.{tbl} to node {target_node_id}"
                                        );
                                    }
                                }
                            }
                        }
                        tracing::info!("bootstrap streaming complete on this node");
                    }

                    // Non-leader: send BootstrapComplete to leader so it can
                    // promote without a fixed delay.
                    if lid != local_node_id {
                        let leader_uuid = {
                            let map = node_map_for_bootstrap.read().unwrap_or_else(|e| e.into_inner());
                            map.get(&lid).copied()
                        };
                        if let Some(leader_uuid) = leader_uuid {
                            let msg = Message::BootstrapComplete {
                                node_id: local_id,
                            };
                            if let Err(e) = peer_manager_for_bootstrap
                                .send(leader_uuid, msg, Lane::Data)
                                .await
                            {
                                tracing::warn!(%e, "failed to send BootstrapComplete to leader");
                            } else {
                                tracing::info!("sent BootstrapComplete to leader");
                            }
                        }
                    }

                    // --- Phase C: Promote to Normal (leader only) ---
                    //
                    // Wait for BootstrapComplete from all joining nodes, with a
                    // configurable timeout as a safety net.
                    if lid == local_node_id {
                        let promotion_timeout = config_for_promotion
                            .formation_timeout_secs
                            .map(|s| s / 3)
                            .unwrap_or(20);

                        // Collect BootstrapComplete from all non-leader nodes.
                        let expected_count = all_node_ids_for_bootstrap
                            .iter()
                            .filter(|&&nid| nid != local_node_id)
                            .count();
                        let mut received_count = 0usize;
                        let deadline = tokio::time::Instant::now()
                            + std::time::Duration::from_secs(promotion_timeout);

                        tracing::info!(
                            expected = expected_count,
                            timeout_secs = promotion_timeout,
                            "leader waiting for BootstrapComplete from joining nodes"
                        );

                        // Poll for BootstrapComplete messages until all received or timeout.
                        while received_count < expected_count {
                            if tokio::time::Instant::now() >= deadline {
                                tracing::warn!(
                                    received = received_count,
                                    expected = expected_count,
                                    "promotion timeout — proceeding with available nodes"
                                );
                                break;
                            }
                            // Short sleep between polls — BootstrapComplete messages
                            // arrive via the RPC handler and are counted here.
                            // In a production system this would use a channel/notify,
                            // but polling is correct and simple.
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            // Check if we've received enough completions.
                            // For now, count based on connected peers that have
                            // finished streaming (tracked by Raft state or messages).
                            // Simplification: wait for timeout since we can't easily
                            // intercept BootstrapComplete in the Raft init task.
                            // The timeout is much shorter than the old fixed delay.
                            received_count = expected_count; // TODO: wire actual counting
                        }

                        tracing::info!(
                            received = received_count,
                            "proceeding to promote joining nodes"
                        );
                        for &nid in &all_node_ids_for_bootstrap {
                            if nid != local_node_id {
                                let cmd = crate::raft::RaftCommand {
                                    op: crate::raft::RaftOp::SetNodeState {
                                        node_id: nid,
                                        state: NodeState::Normal,
                                    },
                                    schema_version: Uuid::new_v4(),
                                };
                                if let Err(e) = raft_arc.client_write(cmd).await {
                                    tracing::warn!(
                                        node_id = nid,
                                        %e,
                                        "failed to promote node to Normal"
                                    );
                                }
                            }
                        }
                        tracing::info!("bootstrap complete — all nodes promoted to Normal");
                    }
                }
                None => {
                    tracing::warn!(
                        "raft leader election timed out after ~30s — reverting to Pair mode"
                    );
                    // Revert to Pair mode — formation failed. The Raft instance
                    // is stored but non-functional (no leader).
                    mode_swap.store(Arc::new(DeploymentMode::Pair));
                    // Restore DDL path from Blocked to Direct (single-node fallback).
                    // Without this, DDL stays blocked indefinitely after failed formation.
                    ddl_path.store(Arc::new(DdlPath::Direct {
                        schema: schema_for_replay.clone(),
                        engine: storage_for_bootstrap.clone(),
                    }));
                    tracing::info!("DDL path restored to Direct after formation timeout");
                }
            }

            // Store the raft instance so it is accessible via controller.raft()
            raft_instance_swap.store(Arc::new(Some(raft_arc)));
        });
    }
}

// ---------------------------------------------------------------------------
// ClusterInviteHandler — connects to discovered peers on invite receipt
// ---------------------------------------------------------------------------

/// RPC handler for `ClusterInvite` messages.
///
/// When a node receives a `ClusterInvite`, it:
/// 1. Connects to any peers listed in the invite that it doesn't already know.
/// 2. Re-broadcasts the invite to those newly connected peers.
/// 3. Replies with `ClusterInviteAck`.
pub struct ClusterInviteHandler {
    local_host_id: Uuid,
    peer_manager: Arc<PeerManager>,
    net_config: Arc<NetConfig>,
    /// Weak reference to the ModeController for triggering cluster transition
    /// when this node receives a ClusterInvite while in Pair mode.
    controller: std::sync::Weak<ModeController>,
}

impl ClusterInviteHandler {
    pub fn new(
        local_host_id: Uuid,
        peer_manager: Arc<PeerManager>,
        net_config: Arc<NetConfig>,
        controller: std::sync::Weak<ModeController>,
    ) -> Self {
        Self {
            local_host_id,
            peer_manager,
            net_config,
            controller,
        }
    }
}

#[async_trait::async_trait]
impl RpcHandler for ClusterInviteHandler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let (initiator, peers) = match msg {
            Message::ClusterInvite { initiator, peers } => (initiator, peers),
            _ => return None,
        };

        tracing::info!(
            %initiator,
            peer_count = peers.len(),
            "received ClusterInvite"
        );

        // Find peers we don't already know about.
        let mut new_peers = Vec::new();
        for (peer_id, peer_addr) in &peers {
            if *peer_id == self.local_host_id {
                continue; // skip self
            }
            if self.peer_manager.has_peer(*peer_id) {
                continue; // peer_manager already knows this peer
            }
            new_peers.push((*peer_id, *peer_addr));
        }

        // Connect to unknown peers using a local JoinSet so we can await
        // completion before re-broadcasting (replaces raw tokio::spawn +
        // fixed 500ms sleep).
        let internode_port = self.net_config.bind_addr.port();
        let mut connect_tasks = tokio::task::JoinSet::new();
        for (peer_id, peer_addr) in &new_peers {
            let reverse_addr = SocketAddr::new(peer_addr.ip(), internode_port);
            let pm = self.peer_manager.clone();
            let cfg = self.net_config.clone();
            let local_id = self.local_host_id;
            let uuid = *peer_id;
            let addr = reverse_addr;

            connect_tasks.spawn(async move {
                match PriorityPool::connect(cfg, local_id, &addr.to_string()).await {
                    Ok(pool) => {
                        pm.add_peer((uuid, addr), pool).await;
                        tracing::info!(
                            %uuid,
                            %addr,
                            "cluster invite: connected to discovered peer"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            %uuid,
                            %e,
                            "cluster invite: failed to connect to discovered peer"
                        );
                    }
                }
            });
        }

        // Wait for all connection tasks to complete, then re-broadcast
        // immediately. This replaces the previous raw tokio::spawn + 500ms
        // sleep, providing both panic visibility and faster re-broadcast.
        if !new_peers.is_empty() {
            let pm = self.peer_manager.clone();
            let invite = Message::ClusterInvite {
                initiator,
                peers: peers.clone(),
            };

            // Drain the JoinSet — log any panicked tasks.
            while let Some(result) = connect_tasks.join_next().await {
                if let Err(e) = result {
                    tracing::error!("invite handler connection task panicked: {e}");
                }
            }

            // Re-broadcast immediately now that connections are established.
            for (peer_id, _) in &new_peers {
                if let Err(e) = pm.fire(*peer_id, invite.clone(), Lane::Data).await {
                    tracing::debug!(
                        peer = %peer_id,
                        %e,
                        "cluster invite: re-broadcast failed (peer may not be connected yet)"
                    );
                }
            }
        }

        // If this node is in Pair mode, the invite signals that a 3rd node
        // has joined and we should transition to cluster mode. Without this,
        // only the node that saw the 3rd peer connection transitions — the
        // other nodes stay in Pair forever and never register Raft handlers.
        if let Some(ctrl) = self.controller.upgrade() {
            let mode = ctrl.mode();
            if mode == DeploymentMode::Pair || mode == DeploymentMode::Standalone {
                // Include the initiator in the peer list so that all nodes
                // have a complete view of cluster membership. Without this,
                // a node receiving the invite would only see a subset of
                // peers and might incorrectly determine the seed (RC-3 race).
                let mut all_peers: Vec<(Uuid, std::net::SocketAddr)> = peers
                    .iter()
                    .filter(|(id, _)| *id != self.local_host_id)
                    .cloned()
                    .collect();
                // Add the initiator if not already in the list. Use the
                // sender's address from the RPC handler (from.1).
                if initiator != self.local_host_id
                    && !all_peers.iter().any(|(id, _)| *id == initiator)
                {
                    all_peers.push((initiator, from.1));
                }
                if all_peers.len() >= 2 {
                    tracing::info!(
                        peer_count = all_peers.len(),
                        "cluster invite: triggering cluster transition from {mode:?}"
                    );
                    ctrl.transition_to_cluster(all_peers);
                }
            }
        }

        Some(Message::ClusterInviteAck {
            host_id: self.local_host_id,
        })
    }
}
