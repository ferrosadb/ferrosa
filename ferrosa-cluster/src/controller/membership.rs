//! Cluster membership operations: approve, join, decommission.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_net::pool::PriorityPool;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::error::{ClusterError, Result};
use crate::raft::{uuid_to_node_id, NodeInfo, NodeState, RaftCommand, RaftOp};
use crate::ring::TokenRing;

use super::cluster::peer_topology;
use super::token::generate_deterministic_token;
use super::ModeController;

fn clear_pending_join(pending_joins: &Arc<Mutex<Vec<Uuid>>>, host_id: Uuid) {
    let mut pending = pending_joins.lock();
    pending.retain(|id| *id != host_id);
}

/// Resolve the value stored in `NodeInfo.addr` for a peer.
///
/// Prefers the peer's advertised internode-broadcast HOSTNAME so the committed
/// membership/token-ring entry re-resolves on every reconnect (handling
/// container IP churn). Falls back to the observed connection `SocketAddr` (a
/// resolved IP literal) only when no hostname was advertised.
pub(super) fn node_info_addr(
    addr: std::net::SocketAddr,
    internode_broadcast: Option<&str>,
) -> String {
    match internode_broadcast {
        Some(host) if !host.trim().is_empty() => host.to_string(),
        _ => addr.to_string(),
    }
}

fn cluster_member_metadata_changed(
    current: &NodeInfo,
    desired_addr: &str,
    cql_broadcast: Option<&str>,
) -> bool {
    if current.addr != desired_addr {
        return true;
    }

    match (&current.cql_broadcast, cql_broadcast) {
        (Some(current), Some(new)) => current != new,
        (None, Some(_)) => true,
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_existing_member_metadata_to_ring(
    ring_holder: &Arc<ArcSwap<Option<Arc<TokenRing>>>>,
    peer_node_id: u64,
    existing: &NodeInfo,
    host_id: Uuid,
    addr: std::net::SocketAddr,
    cql_broadcast: Option<String>,
    internode_broadcast: Option<String>,
) -> bool {
    let desired_addr = node_info_addr(addr, internode_broadcast.as_deref());
    let desired_cql_broadcast = cql_broadcast.or_else(|| existing.cql_broadcast.clone());
    if !cluster_member_metadata_changed(existing, &desired_addr, desired_cql_broadcast.as_deref()) {
        return false;
    }

    let ring_snapshot = ring_holder.load();
    let Some(current_ring) = (**ring_snapshot).as_ref() else {
        return false;
    };

    let mut updated_ring = (**current_ring).clone();
    updated_ring.add_node(
        peer_node_id,
        NodeInfo {
            host_id,
            addr: desired_addr,
            data_center: existing.data_center.clone(),
            rack: existing.rack.clone(),
            state: existing.state,
            cql_broadcast: desired_cql_broadcast,
        },
    );
    ring_holder.store(Arc::new(Some(Arc::new(updated_ring))));
    true
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
        //
        // The committed entry must carry THE PEER's announced dc/rack, never
        // ours: this node authors the joiner's identity, so stamping it with
        // the local topology would record a remote-DC node as a local-DC
        // replica in every follower's `state.members` / ring, and LOCAL_*
        // quorums would then count it as local. A peer that announced nothing
        // (an older peer mid-rolling-upgrade) still falls back to the local
        // topology, which is the behaviour before DC advertisement existed.
        let (peer_dc, peer_rack) = peer_topology(
            peer_host_id,
            &self.peer_dcs_snapshot(),
            &self.peer_racks_snapshot(),
            &self.config.data_center,
            &self.config.rack,
        );
        let node_info = NodeInfo {
            host_id: peer_host_id,
            addr: String::new(), // will be filled by the connecting peer
            data_center: peer_dc,
            rack: peer_rack,
            state: NodeState::Normal,
            cql_broadcast: None,
        };

        // Topology commands change no schema, so they must carry the node's
        // CURRENT schema version — never a freshly minted random one (bug:
        // schema_version diverges after restart, t_7dda9421). The state
        // machine's `schema_changed` gate keeps the committed version where the
        // last real DDL put it; a random version here would be wrong even though
        // that gate currently ignores it.
        let schema_version = self.schema.snapshot().version;

        let join_cmd = RaftCommand {
            op: RaftOp::JoinNode(node_info),
            schema_version,
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
            schema_version,
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

    /// Decommission this node (P0-2).
    ///
    /// 1. Mark it `Leaving` through Raft (out of `replicas()`, never promoted
    ///    back by the restart pass).
    /// 2. Stream every local partition of every range it REPLICATES (not only
    ///    the ranges it is primary for) to each node that becomes a replica of
    ///    that range, per keyspace replication strategy. Each batch must come
    ///    back verified by the receiver (count, checksum, applied).
    /// 3. Only then propose `LeaveNode`.
    ///
    /// Any read error, stream failure or short apply aborts before step 3: the
    /// node stays `Leaving` with all its data and the error says what failed.
    /// See [`super::data_movement::decommission_verified`].
    ///
    /// Only the leaving node can run this. It streams its OWN storage; run on
    /// another node it would stream that node's copies and call the leaving
    /// node's data moved. Removing a dead node needs streaming from the
    /// surviving replicas, which this does not do, so it is refused.
    pub async fn initiate_decommission(&self, host_id: Uuid) -> Result<()> {
        let raft = self
            .raft()
            .ok_or_else(|| ClusterError::Internal("raft not initialized".into()))?;
        if host_id != self.local_host_id {
            return Err(ClusterError::ModeTransitionRejected(format!(
                "decommission of {host_id} must run on that node: it streams the leaving \
                 node's own data. This node is {}",
                self.local_host_id
            )));
        }
        let node_id = uuid_to_node_id(host_id);
        let ring = self
            .token_ring()
            .ok_or_else(|| ClusterError::Internal("decommission: no token ring".into()))?;
        let peer_manager = self
            .peer_manager
            .load()
            .as_ref()
            .clone()
            .ok_or_else(|| ClusterError::Internal("peer_manager not set".into()))?;
        let tables = super::data_movement::replicated_tables(&self.schema.snapshot())?;
        let storage = self.storage.clone();
        let scan = move |table: &ferrosa_storage::TableId| {
            use futures::StreamExt as _;
            storage
                .range_iter(table, None, None)
                .map(|r| r.map(Arc::unwrap_or_clone))
                .boxed()
        };
        // Membership / data-movement commands change no schema, so this proposer
        // carries the node's CURRENT schema version rather than minting one
        // (bug: schema_version diverges after restart, t_7dda9421).
        let proposer = super::data_movement::ForwardingProposer {
            raft,
            peer_manager: peer_manager.clone(),
            ring: self.ring.clone(),
            schema_version: self.schema.snapshot().version,
        };
        let streamer = super::data_movement::StreamSenderStreamer {
            peer_manager,
            ring: ring.clone(),
            source_node: node_id,
        };
        super::data_movement::decommission_verified(
            &proposer, &ring, node_id, &tables, scan, &streamer,
        )
        .await
        .map(|_| ())
    }

    /// Trigger join admission for a peer that connected while in cluster mode.
    ///
    /// Checks de-duplication (via `pending_joins`), approval (via `approved_nodes`
    /// or `auto_join`), and spawns an async task to propose `JoinNode` +
    /// `AssignTokens` via Raft.
    ///
    /// Returns `true` only when a join/metadata refresh was newly enqueued.
    /// Cluster-mode reconnect handling uses this to avoid re-sending
    /// `ClusterInvite` on every duplicate reconnect for an already-known live
    /// member; unbounded invite storms churn lanes and make LOCAL_QUORUM reads
    /// time out even while containers look healthy.
    /// Is this node a VOTER in this node's own Raft group?
    ///
    /// The token ring records placement membership; only the Raft group's voter set
    /// decides who participates in consensus. A peer can be in one and absent from the
    /// other, and treating ring presence as proof of membership is what stopped the
    /// promotion that reconciles them (CL-76).
    /// `Some(true/false)` when this node owns a Raft group, `None` when it does not
    /// (yet). `None` is NOT "not a voter": a node that has not published its group
    /// cannot tell, and treating that as absence turned the duplicate-join guard into a
    /// join loop for every ring member (two regression tests caught it).
    fn local_group_voter_state(&self, peer_node_id: u64) -> Option<bool> {
        self.raft().map(|raft| {
            raft.metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == peer_node_id)
        })
    }

    pub(super) fn trigger_cluster_join(
        &self,
        host_id: Uuid,
        addr: std::net::SocketAddr,
        cql_broadcast: Option<String>,
        internode_broadcast: Option<String>,
    ) -> bool {
        // The address committed to membership/the ring is the advertised
        // internode-broadcast HOSTNAME when present (re-resolvable across IP
        // churn), else the observed connection IP.
        let desired_addr = node_info_addr(addr, internode_broadcast.as_deref());
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
            // CL-76: an entry in the TOKEN RING is placement membership, not a vote.
            // A same-DC peer that is in the ring but absent from our Raft group's voter
            // set still has to be proposed: otherwise the leader never promotes it, the
            // group can never reach `expected_cluster_size`, and a data center that
            // formed from an incomplete peer set stays permanently unable to accept
            // clients. This is the guard the debug logs name —
            // "peer already present in token ring with current metadata, skipping join
            // trigger" — firing on a peer that was in the ring and NOT a voter.
            // Only an AUTHORITATIVE voter set may bypass the guard: a group that exists
            // and does not contain the peer. No group, or the peer already a voter, keeps
            // the ring guard exactly as it was.
            if self.local_group_voter_state(peer_node_id) == Some(false) {
                tracing::info!(
                    peer = %host_id,
                    node_id = peer_node_id,
                    "peer is in the token ring but not a voter in this DC's Raft group: \
                     proposing the join so the leader promotes it"
                );
            } else if !cluster_member_metadata_changed(
                existing,
                &desired_addr,
                cql_broadcast.as_deref(),
            ) && has_outbound_peer
            {
                tracing::debug!(
                    peer = %host_id,
                    node_id = peer_node_id,
                    "peer already present in token ring with current metadata, skipping join trigger"
                );
                return false;
            }
        }

        // Unapproved peers should be ignored without poisoning the pending set.
        if !self.config.auto_join {
            let approved = self.approved_nodes.lock();
            if !approved.contains(&host_id) {
                tracing::warn!(peer = %host_id, "peer not approved to join cluster, ignoring");
                return false;
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
                return false;
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
        let internode_broadcast = internode_broadcast.clone();
        let peer_manager = peer_manager.clone();
        let net_config = self.net_config.clone();
        let local_host_id = self.local_host_id;
        let raft_runtime = self.raft_runtime.get().cloned();
        let data_runtime = self.data_runtime.get().cloned();
        // Ring is needed for resolving the Raft leader's u64 NodeId back to a
        // ferrosa Uuid when openraft asks us to forward a non-leader proposal.
        let ring_holder = self.ring.clone();
        // Topology commands built in this task carry the CURRENT schema version
        // (they change no schema); read it from the shared registry here.
        let schema = self.schema.clone();
        let admission_node_map = self.raft_node_map.load_full();
        let admission_cancel = self.cancel.clone();
        let admission_gate = self.membership_admission.clone();
        let (announced_dc, announced_rack) = peer_topology(
            host_id,
            &self.peer_dcs_snapshot(),
            &self.peer_racks_snapshot(),
            &self.config.data_center,
            &self.config.rack,
        );

        self.spawn_tracked(async move {
            let schema_version = schema.snapshot().version;
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
                        raft_runtime.clone(),
                        data_runtime.clone(),
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

            if let Some(existing) = existing_member.as_ref() {
                if apply_existing_member_metadata_to_ring(
                    &ring_holder,
                    peer_node_id,
                    existing,
                    host_id,
                    addr,
                    cql_broadcast.clone(),
                    internode_broadcast.clone(),
                ) {
                    tracing::info!(
                        peer = %host_id,
                        node_id = peer_node_id,
                        %addr,
                        "peer metadata applied optimistically to local token ring before Raft convergence"
                    );
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

            let is_voter = raft.metrics().borrow().membership_config.membership().voter_ids().any(|id|id==peer_node_id);
            let needs_admission = existing_member.as_ref().map_or(!is_voter, |info| info.state==NodeState::Joining || (info.state==NodeState::Normal && !is_voter));
            if needs_admission {
                let peer_dc=existing_member.as_ref().map(|n|n.data_center.clone()).unwrap_or(announced_dc);
                if peer_dc != config_clone.data_center {
                    tracing::warn!(peer=%host_id, dc=%peer_dc, "peer must be admitted by its own DC's membership leader");
                    clear_pending_join(&pending_joins,host_id);
                    return;
                }
                let result = if leader != uuid_to_node_id(local_host_id) {
                    let leader_host = ring_holder.load().as_ref().as_ref().and_then(|r|r.get_node(leader).map(|n|n.host_id));
                    match (peer_manager.as_ref(),leader_host) {
                        (Some(pm),Some(target))=>control::request(pm,target,crate::membership::MembershipOp::AddVoter { host_id,addr }).await,
                        _=>Err(ClusterError::Internal("verified admission cannot resolve the current leader".into())),
                    }
                } else {
                    match (peer_manager.as_ref(),admission_node_map.as_ref()) {
                        (Some(pm),Some(map))=> {
                            let info=NodeInfo { host_id,addr:desired_addr.clone(),data_center:peer_dc,rack:existing_member.as_ref().map(|n|n.rack.clone()).unwrap_or(announced_rack),state:NodeState::Joining,cql_broadcast:cql_broadcast.clone().or_else(||existing_member.as_ref().and_then(|n|n.cql_broadcast.clone())) };
                            control::Admission { gate:admission_gate,raft:raft.clone(),node_map:map.clone(),pm:pm.clone(),ring:ring_holder.clone(),schema:schema.clone(),num_tokens:config_clone.num_tokens as usize,cancel:admission_cancel }.run(info,addr).await
                        }
                        _=>Err(ClusterError::Internal("verified admission has no network membership map".into())),
                    }
                };
                if let Err(error)=result { tracing::error!(peer=%host_id,%error,"verified membership admission failed; no unverified replica promotion"); }
                clear_pending_join(&pending_joins,host_id);
                return;
            }

            if let Some(existing) = existing_member {
                let refresh_cmd = RaftCommand {
                    op: RaftOp::UpdateNodeInfo(NodeInfo {
                        host_id,
                        addr: desired_addr.clone(),
                        data_center: existing.data_center,
                        rack: existing.rack,
                        state: existing.state,
                        cql_broadcast: cql_broadcast.or(existing.cql_broadcast),
                    }),
                    schema_version,
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
                addr: desired_addr.clone(),
                data_center: config_clone.data_center.clone(),
                rack: config_clone.rack.clone(),
                state: NodeState::Normal,
                cql_broadcast,
            };

            let join_cmd = RaftCommand {
                op: RaftOp::JoinNode(node_info),
                schema_version,
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
                schema_version,
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

        true
    }
}

/// The Raft op that takes a node out of `replicas()` while decommission
/// streams its data to the new owners.
///
/// It must be `Leaving`, never `Joining`. Both are excluded from `replicas()`,
/// but `Joining` means "pre-bootstrap, promote me when ready": the restart
/// recovery pass (`promote_joining_members`, CL-29) promotes every `Joining`
/// member back to `Normal`. A decommission marked `Joining` was therefore
/// undone by any node restarting mid-drain: the draining node served and took
/// writes again, and writes it took after its stream passed their partition
/// were lost at `LeaveNode`.
pub(crate) fn decommission_drain_op(node_id: u64) -> RaftOp {
    RaftOp::SetNodeState {
        node_id,
        state: NodeState::Leaving,
    }
}

#[cfg(test)]
mod decommission_drain_tests {
    use std::collections::BTreeMap;

    use super::decommission_drain_op;
    use crate::raft::{NodeState, RaftOp};
    use crate::repair::coordinator::promote_joining_members;

    /// A node being decommissioned must not be promoted back to `Normal` by
    /// the restart recovery pass, and must stay out of `replicas()`.
    #[test]
    fn a_draining_node_is_not_promoted_by_the_recovery_pass() {
        let draining = 2_u64;
        let RaftOp::SetNodeState { node_id, state } = decommission_drain_op(draining) else {
            panic!("decommission must set the node state");
        };
        assert_eq!(node_id, draining);
        assert!(
            !state.is_voter(),
            "a draining node must not serve as a voter"
        );

        let members: BTreeMap<u64, NodeState> = [
            (1, NodeState::Normal),
            (draining, state),
            (3, NodeState::Normal),
        ]
        .into();
        assert!(
            promote_joining_members(&members, &[draining].into()).is_empty(),
            "the recovery promote pass would undo the decommission of node {draining} \
             (drain state {state:?})"
        );
    }
}

#[cfg(test)]
mod streaming_contract_tests {
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use uuid::Uuid;

    use crate::raft::{uuid_to_node_id, NodeInfo, NodeState};
    use crate::ring::TokenRing;

    #[test]
    fn decommission_streaming_must_not_materialize_all_partitions() {
        let source = include_str!("membership.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production membership source must be present");
        let unbounded_range_read = concat!("read_range", "(&table_id, None, None, usize::MAX)");
        assert!(
            !source.contains(unbounded_range_read),
            "decommission must stream partitions instead of materializing every partition with usize::MAX"
        );
    }

    #[test]
    fn node_info_addr_prefers_advertised_internode_hostname_over_resolved_ip() {
        // When a peer advertises an internode-broadcast hostname, the committed
        // NodeInfo.addr must be that re-resolvable hostname, NOT the startup-frozen
        // IP literal observed on the connection. This is the core fix for stale
        // membership after container IP churn.
        let observed: std::net::SocketAddr = "10.89.1.176:7000".parse().unwrap();
        assert_eq!(
            super::node_info_addr(observed, Some("node1:7000")),
            "node1:7000",
            "advertised internode hostname must win over the resolved connection IP"
        );
    }

    #[test]
    fn node_info_addr_falls_back_to_socket_addr_without_advertised_hostname() {
        let observed: std::net::SocketAddr = "10.89.1.176:7000".parse().unwrap();
        assert_eq!(super::node_info_addr(observed, None), "10.89.1.176:7000");
        assert_eq!(
            super::node_info_addr(observed, Some("  ")),
            "10.89.1.176:7000"
        );
    }

    #[test]
    fn existing_member_metadata_refresh_commits_hostname_when_internode_broadcast_present() {
        // The ring update path must store the hostname (not the resolved IP) so
        // re-resolution at connect time tracks the live container IP.
        let host_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let node_id = uuid_to_node_id(host_id);
        let mut ring = TokenRing::new();
        ring.add_node(
            node_id,
            NodeInfo {
                host_id,
                addr: "10.89.1.151:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            },
        );
        ring.assign_tokens(node_id, &[100, 200]);
        let existing = ring.get_node(node_id).unwrap().clone();
        let ring_holder = Arc::new(ArcSwap::from_pointee(Some(Arc::new(ring))));

        let changed = super::apply_existing_member_metadata_to_ring(
            &ring_holder,
            node_id,
            &existing,
            host_id,
            // Observed connection IP differs from the previous committed IP…
            "10.89.1.201:7000".parse().unwrap(),
            None,
            // …but the peer advertises a stable hostname, which must be stored.
            Some("node1:7000".to_string()),
        );

        assert!(changed);
        let updated = ring_holder
            .load()
            .as_ref()
            .as_ref()
            .unwrap()
            .get_node(node_id)
            .unwrap()
            .clone();
        assert_eq!(
            updated.addr, "node1:7000",
            "committed NodeInfo.addr must be the advertised hostname, not the resolved IP"
        );
        assert_eq!(
            ring_holder
                .load()
                .as_ref()
                .as_ref()
                .unwrap()
                .tokens_for_node(node_id),
            vec![100, 200]
        );
    }

    #[test]
    fn existing_member_metadata_refresh_updates_local_ring_snapshot_without_losing_tokens() {
        let host_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let node_id = uuid_to_node_id(host_id);
        let mut ring = TokenRing::new();
        ring.add_node(
            node_id,
            NodeInfo {
                host_id,
                addr: "10.89.1.151:17000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19043".to_string()),
            },
        );
        ring.assign_tokens(node_id, &[100, 200]);
        let existing = ring.get_node(node_id).unwrap().clone();
        let ring_holder = Arc::new(ArcSwap::from_pointee(Some(Arc::new(ring))));

        let changed = super::apply_existing_member_metadata_to_ring(
            &ring_holder,
            node_id,
            &existing,
            host_id,
            "10.89.1.201:17000".parse().unwrap(),
            Some("127.0.0.1:19045".to_string()),
            None,
        );

        assert!(changed);
        let updated = ring_holder
            .load()
            .as_ref()
            .as_ref()
            .unwrap()
            .get_node(node_id)
            .unwrap()
            .clone();
        assert_eq!(updated.addr, "10.89.1.201:17000");
        assert_eq!(updated.cql_broadcast.as_deref(), Some("127.0.0.1:19045"));
        assert_eq!(
            ring_holder
                .load()
                .as_ref()
                .as_ref()
                .unwrap()
                .tokens_for_node(node_id),
            vec![100, 200]
        );
    }
}

#[cfg(test)]
#[path = "membership_verification_tests.rs"]
mod membership_verification_tests;

#[path = "membership_control.rs"]
pub(super) mod control;
