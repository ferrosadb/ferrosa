//! Cluster membership operations: approve, join, decommission.

use std::sync::Arc;

use ferrosa_net::pool::PriorityPool;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::error::{ClusterError, Result};
use crate::raft::{uuid_to_node_id, NodeInfo, NodeState, RaftCommand, RaftOp};

use super::token::generate_deterministic_token;
use super::ModeController;

fn clear_pending_join(pending_joins: &Arc<Mutex<Vec<Uuid>>>, host_id: Uuid) {
    let mut pending = pending_joins.lock();
    pending.retain(|id| *id != host_id);
}

fn cluster_member_metadata_changed(
    current: &NodeInfo,
    addr: std::net::SocketAddr,
    cql_broadcast: Option<&str>,
) -> bool {
    if current.addr != addr.to_string() {
        return true;
    }

    match (&current.cql_broadcast, cql_broadcast) {
        (Some(current), Some(new)) => current != new,
        (None, Some(_)) => true,
        _ => false,
    }
}

impl ModeController {
    /// Record that a node has been approved to join the cluster.
    ///
    /// This mirrors the `ApproveNode` Raft command's effect on
    /// `RaftState.approved_nodes` so that the controller can perform
    /// synchronous approval checks in `handle_join_request`.
    pub fn approve_node(&self, host_id: Uuid) {
        self.approved_nodes.lock().insert(host_id);
    }

    /// Handle a join request from a new node.
    ///
    /// 1. Check `approved_nodes` unless `auto_join=true`.
    /// 2. Compute delta mutations (for now: empty — S3 bootstrap covers most data).
    /// 3. If delta needed, stream via `StreamSender`.
    /// 4. Generate deterministic tokens for the new node.
    /// 5. Propose `JoinNode` + `AssignTokens` via Raft.
    pub async fn handle_join_request(
        &self,
        peer_host_id: Uuid,
        peer_node_id: u64,
        _manifest_state: Option<()>, // placeholder for ManifestState
    ) -> Result<()> {
        // 1. Approval check — unless auto_join is enabled.
        //    Checked before Raft access so unapproved nodes are rejected fast.
        if !self.config.auto_join {
            let approved = self.approved_nodes.lock();
            if !approved.contains(&peer_host_id) {
                return Err(ClusterError::NotApproved(peer_host_id));
            }
        }

        let raft = self
            .raft()
            .ok_or_else(|| ClusterError::Internal("raft not initialized".into()))?;

        // 2. Delta computation — S3 bootstrap covers most data, so delta is empty for MVP.
        // In the future, compare manifest_state with current S3 state to compute delta.

        // 3. No delta streaming needed for MVP.

        // 4. Generate deterministic tokens for the new node.
        let num_tokens = self.config.num_tokens as usize;
        let tokens: Vec<i64> = (0..num_tokens)
            .map(|i| generate_deterministic_token(peer_node_id, i))
            .collect();

        // 5. Propose JoinNode via Raft.
        let node_info = NodeInfo {
            host_id: peer_host_id,
            addr: String::new(), // will be filled by the connecting peer
            data_center: self.config.data_center.clone(),
            rack: self.config.rack.clone(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };

        let join_cmd = RaftCommand {
            op: RaftOp::JoinNode(node_info),
            schema_version: Uuid::new_v4(),
        };
        raft.client_write(join_cmd)
            .await
            .map_err(|e| ClusterError::RaftError(format!("JoinNode proposal failed: {e}")))?;

        // Propose AssignTokens via Raft.
        let assign_cmd = RaftCommand {
            op: RaftOp::AssignTokens {
                node_id: peer_node_id,
                tokens,
            },
            schema_version: Uuid::new_v4(),
        };
        raft.client_write(assign_cmd)
            .await
            .map_err(|e| ClusterError::RaftError(format!("AssignTokens proposal failed: {e}")))?;

        tracing::info!(
            host_id = %peer_host_id,
            node_id = peer_node_id,
            "node join complete: JoinNode + AssignTokens committed"
        );

        Ok(())
    }

    /// Initiate decommission of a node.
    ///
    /// 1. Propose `LeaveNode` via Raft — removes the node from membership
    ///    and cleans up its tokens in the state machine.
    /// 2. Identify token ranges owned by the leaving node.
    /// 3. For each range, find new owner via `ring.replicas()` excluding the leaving node.
    /// 4. Stream data from leaving node to new owners via `StreamSender`.
    ///    (For MVP: the leaving node triggers its own streaming.)
    /// 5. After Raft commits the `LeaveNode`, the node is fully removed.
    pub async fn initiate_decommission(&self, host_id: Uuid) -> Result<()> {
        let raft = self
            .raft()
            .ok_or_else(|| ClusterError::Internal("raft not initialized".into()))?;

        let node_id = uuid_to_node_id(host_id);

        // 0. If this node is the Raft leader, wait for leadership to move.
        // The departing node shouldn't coordinate its own removal.
        // openraft 0.9 doesn't have transfer_leader(); instead, we proceed
        // with decommission and let Raft auto-elect after LeaveNode removes
        // this node from membership. The remaining nodes will elect a new
        // leader via normal Raft election timeout (~1-2s).
        if let Some(lid) = raft.current_leader().await {
            if lid == node_id {
                tracing::info!("decommissioning the leader — Raft will auto-elect after LeaveNode");
            }
        }

        // 1. Mark node as Leaving (still serves reads, but data streaming begins)
        let set_leaving = RaftCommand {
            op: RaftOp::SetNodeState {
                node_id,
                state: crate::raft::NodeState::Joining, // reuse Joining = not serving
            },
            schema_version: Uuid::new_v4(),
        };
        raft.client_write(set_leaving)
            .await
            .map_err(|e| ClusterError::RaftError(format!("SetNodeState failed: {e}")))?;

        // 2. Stream data from the leaving node to new token owners.
        // Read all user tables, find partitions whose primary owner is this node,
        // and stream them to the next replica.
        if let Some(ring) = &**self.ring.load() {
            let peer_manager = match &**self.peer_manager.load() {
                Some(pm) => pm.clone(),
                None => {
                    tracing::warn!("decommission: no peer_manager, skipping streaming");
                    return Err(ClusterError::Internal("peer_manager not set".into()));
                }
            };
            let schema_snap = self.schema.snapshot();
            let config = crate::streaming::StreamConfig::default();
            let mut session_counter = 0_u64;

            for (ks, tbl) in schema_snap.tables.keys() {
                if ks.starts_with("system") {
                    continue;
                }
                let table_id = ferrosa_storage::commitlog::TableId::new(ks, tbl);
                let partitions = match self.storage.read_range(&table_id, None, None, usize::MAX) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(%e, ks, tbl, "decommission: failed to read table");
                        continue;
                    }
                };

                for partition in &partitions {
                    let token = partition.key.token.0;
                    if ring.primary_owner(token) == Some(node_id) {
                        // This partition is owned by the leaving node — find next replica
                        let replicas = ring.replicas(token, 2);
                        let target = replicas.iter().find(|&&nid| nid != node_id);
                        if let Some(&target_nid) = target {
                            let target_uuid = ring
                                .get_node(target_nid)
                                .map(|n| n.host_id)
                                .unwrap_or_default();
                            // Serialize all rows via RowWire for full fidelity
                            // (clustering keys, all cells, deletion, liveness).
                            use crate::raft::handlers::RowWire;
                            let wire_rows: Vec<RowWire> =
                                partition.rows.iter().cloned().map(RowWire::from).collect();
                            let row_bytes = bincode::serialize(&wire_rows).unwrap_or_default();
                            let ts = partition
                                .rows
                                .first()
                                .and_then(|r| r.cells.first())
                                .map(|(_, cv)| cv.timestamp)
                                .unwrap_or(0);

                            session_counter += 1;
                            let mutations = vec![crate::streaming::StreamedMutation {
                                keyspace: ks.clone(),
                                table: tbl.clone(),
                                key: partition.key.key.as_bytes().to_vec(),
                                row: row_bytes,
                                timestamp: ts,
                            }];
                            if let Err(e) = crate::streaming::sender::StreamSender::send_stream(
                                mutations,
                                &peer_manager,
                                target_uuid,
                                session_counter,
                                (i64::MIN, i64::MAX),
                                node_id,
                                &config,
                            )
                            .await
                            {
                                tracing::warn!(%e, "decommission streaming failed for {ks}.{tbl}");
                            }
                        }
                    }
                }
            }
            tracing::info!("decommission streaming complete");
        }

        // 3. Propose LeaveNode via Raft — removes node from membership and tokens.
        let leave_cmd = RaftCommand {
            op: RaftOp::LeaveNode { node_id },
            schema_version: Uuid::new_v4(),
        };
        raft.client_write(leave_cmd)
            .await
            .map_err(|e| ClusterError::RaftError(format!("LeaveNode proposal failed: {e}")))?;

        tracing::info!(
            host_id = %host_id,
            node_id,
            "node decommission complete: data streamed + LeaveNode committed"
        );

        Ok(())
    }

    /// Trigger join admission for a peer that connected while in cluster mode.
    ///
    /// Checks de-duplication (via `pending_joins`), approval (via `approved_nodes`
    /// or `auto_join`), and spawns an async task to propose `JoinNode` +
    /// `AssignTokens` via Raft.
    pub(super) fn trigger_cluster_join(
        &self,
        host_id: Uuid,
        addr: std::net::SocketAddr,
        cql_broadcast: Option<String>,
    ) {
        let peer_manager = self.peer_manager.load().as_ref().as_ref().cloned();
        let has_outbound_peer = peer_manager
            .as_ref()
            .map(|pm| pm.has_live_peer(host_id))
            .unwrap_or(false);
        let peer_node_id = uuid_to_node_id(host_id);
        let existing_member = self
            .token_ring()
            .as_ref()
            .and_then(|ring| ring.get_node(peer_node_id).cloned());

        if let Some(existing) = existing_member.as_ref() {
            if !cluster_member_metadata_changed(existing, addr, cql_broadcast.as_deref())
                && has_outbound_peer
            {
                tracing::debug!(
                    peer = %host_id,
                    node_id = peer_node_id,
                    "peer already present in token ring with current metadata, skipping join trigger"
                );
                return;
            }
        }

        // Unapproved peers should be ignored without poisoning the pending set.
        if !self.config.auto_join {
            let approved = self.approved_nodes.lock();
            if !approved.contains(&host_id) {
                tracing::warn!(peer = %host_id, "peer not approved to join cluster, ignoring");
                return;
            }
        }

        // Track pending joins. Don't block retries — a previous attempt may
        // still be running, but repeated callbacks for the same host should
        // not enqueue duplicate JoinNode / AssignTokens proposals.
        let pending_joins = self.pending_joins.clone();
        {
            let mut pending = pending_joins.lock();
            if pending.contains(&host_id) {
                tracing::debug!(peer = %host_id, "peer join already pending, skipping duplicate trigger");
                return;
            }
            if pending.len() >= super::MAX_PENDING_JOINS {
                tracing::warn!(
                    cap = super::MAX_PENDING_JOINS,
                    "pending_joins at capacity — evicting oldest entry"
                );
                pending.remove(0);
            }
            pending.push(host_id);
        }

        // Capture state needed by the spawned task.
        let raft_groups = self.raft_groups.clone();
        let local_raft_group_id = crate::raft::RaftGroupId::for_dc(&self.config.data_center);
        let config_clone = self.config.clone();
        let existing_member = existing_member.clone();
        let cql_broadcast = cql_broadcast.clone();
        let peer_manager = peer_manager.clone();
        let net_config = self.net_config.clone();
        let local_host_id = self.local_host_id;
        let raft_runtime = self.raft_runtime.get().cloned();
        let data_runtime = self.data_runtime.get().cloned();
        // Ring is needed for resolving the Raft leader's u64 NodeId back to a
        // ferrosa Uuid when openraft asks us to forward a non-leader proposal.
        let ring_holder = self.ring.clone();

        self.spawn_tracked(async move {
            if let Some(pm) = peer_manager.as_ref() {
                let current_addr = pm.peer_addr(host_id).await;
                let desired_addr = addr.to_string();
                let needs_reverse_refresh =
                    !pm.has_live_peer(host_id) || current_addr.as_deref() != Some(&desired_addr);

                if needs_reverse_refresh {
                    match PriorityPool::connect(
                        net_config.clone(),
                        local_host_id,
                        &desired_addr,
                        raft_runtime.as_deref(),
                        data_runtime.as_deref(),
                    )
                    .await
                    {
                        Ok(pool) => {
                            pm.add_peer((host_id, addr), pool).await;
                            tracing::info!(
                                peer = %host_id,
                                %addr,
                                previous_addr = ?current_addr,
                                "cluster member reverse connection established before join refresh"
                            );
                        }
                        Err(e) => {
                            clear_pending_join(&pending_joins, host_id);
                            tracing::warn!(
                                peer = %host_id,
                                %addr,
                                previous_addr = ?current_addr,
                                %e,
                                "failed to establish reverse connection for cluster member"
                            );
                            return;
                        }
                    }
                }
            }

            let mut raft = None;
            for attempt in 0..60 {
                let groups = raft_groups.load();
                if let Some(r) = groups.get(&local_raft_group_id) {
                    raft = Some(r.clone());
                    break;
                }
                // Backward-compat: when only one group is installed, accept it
                // regardless of DC name (covers single-DC tests where the
                // configured `data_center` may differ from what bootstrap used).
                if groups.len() == 1 {
                    raft = groups.values().next().cloned();
                    break;
                }
                drop(groups);
                let backoff =
                    std::time::Duration::from_millis(if attempt < 10 { 100 } else { 500 });
                tokio::time::sleep(backoff).await;
            }
            let Some(raft) = raft else {
                clear_pending_join(&pending_joins, host_id);
                tracing::warn!(
                    peer = %host_id,
                    "raft not initialized yet, cannot admit peer"
                );
                return;
            };

            let mut leader = None;
            for attempt in 0..60 {
                if let Some(lid) = raft.current_leader().await {
                    leader = Some(lid);
                    break;
                }
                let backoff =
                    std::time::Duration::from_millis(if attempt < 10 { 100 } else { 500 });
                tokio::time::sleep(backoff).await;
            }
            let Some(leader) = leader else {
                clear_pending_join(&pending_joins, host_id);
                tracing::warn!(
                    peer = %host_id,
                    "raft leader not elected yet, cannot admit peer"
                );
                return;
            };

            if let Some(existing) = existing_member {
                let refresh_cmd = RaftCommand {
                    op: RaftOp::UpdateNodeInfo(NodeInfo {
                        host_id,
                        addr: addr.to_string(),
                        data_center: existing.data_center,
                        rack: existing.rack,
                        state: existing.state,
                        cql_broadcast: cql_broadcast.or(existing.cql_broadcast),
                    }),
                    schema_version: Uuid::new_v4(),
                };

                let refresh_result = raft.client_write(refresh_cmd.clone()).await;
                clear_pending_join(&pending_joins, host_id);

                let outcome = match refresh_result {
                    Ok(_) => Ok(()),
                    Err(raft_err) => {
                        Err(crate::raft_forward::classify_client_write_error(&raft_err))
                    }
                };
                let was_local_ok = outcome.is_ok();

                let dispatch_pm = peer_manager.clone();
                let dispatch_ring = ring_holder.clone();
                let dispatch_result = crate::raft_forward::dispatch_propose_outcome(
                    outcome,
                    refresh_cmd,
                    move |leader_node_id| {
                        (**dispatch_ring.load())
                            .as_ref()
                            .and_then(|ring| ring.get_node(leader_node_id).map(|n| n.host_id))
                    },
                    move |leader_uuid, cmd| {
                        let pm = dispatch_pm.clone();
                        async move {
                            match pm.as_ref() {
                                Some(pm) => {
                                    crate::raft_forward::forward_raft_command_to_leader(
                                        pm.as_ref(),
                                        leader_uuid,
                                        cmd,
                                    )
                                    .await
                                }
                                None => Err(crate::error::ClusterError::Internal(
                                    "raft forward: peer_manager not set, cannot forward to leader"
                                        .into(),
                                )),
                            }
                        }
                    },
                )
                .await;

                match dispatch_result {
                    Ok(()) => {
                        if was_local_ok {
                            tracing::info!(
                                peer = %host_id,
                                node_id = peer_node_id,
                                leader,
                                "peer metadata refreshed via on_peer_connected"
                            );
                        } else {
                            tracing::info!(
                                peer = %host_id,
                                node_id = peer_node_id,
                                leader,
                                "peer metadata refresh forwarded to raft leader"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer = %host_id,
                            leader,
                            %e,
                            "UpdateNodeInfo refresh did not converge — will retry on next reconnect"
                        );
                    }
                }
                return;
            }

            // Propose JoinNode via Raft.
            let node_info = NodeInfo {
                host_id,
                addr: addr.to_string(),
                data_center: config_clone.data_center.clone(),
                rack: config_clone.rack.clone(),
                state: NodeState::Normal,
                cql_broadcast,
            };

            let join_cmd = RaftCommand {
                op: RaftOp::JoinNode(node_info),
                schema_version: Uuid::new_v4(),
            };
            if let Err(e) = raft.client_write(join_cmd).await {
                clear_pending_join(&pending_joins, host_id);
                tracing::warn!(peer = %host_id, leader, %e, "JoinNode proposal failed");
                return;
            }

            // Propose AssignTokens via Raft.
            let num_tokens = config_clone.num_tokens as usize;
            let tokens: Vec<i64> = (0..num_tokens)
                .map(|i| generate_deterministic_token(peer_node_id, i))
                .collect();

            let assign_cmd = RaftCommand {
                op: RaftOp::AssignTokens {
                    node_id: peer_node_id,
                    tokens,
                },
                schema_version: Uuid::new_v4(),
            };
            if let Err(e) = raft.client_write(assign_cmd).await {
                clear_pending_join(&pending_joins, host_id);
                tracing::warn!(peer = %host_id, leader, %e, "AssignTokens proposal failed");
                return;
            }

            clear_pending_join(&pending_joins, host_id);

            tracing::info!(
                peer = %host_id,
                node_id = peer_node_id,
                leader,
                "peer admitted to cluster via on_peer_connected"
            );
        });
    }
}
