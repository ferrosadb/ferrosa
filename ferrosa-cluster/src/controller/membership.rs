//! Cluster membership operations: approve, join, decommission.

use uuid::Uuid;

use crate::error::{ClusterError, Result};
use crate::raft::{uuid_to_node_id, NodeInfo, NodeState, RaftCommand, RaftOp};

use super::token::generate_deterministic_token;
use super::ModeController;

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

        // 1. Propose LeaveNode via Raft — this removes the node from membership
        //    and cleans up its tokens in the state machine.
        let leave_cmd = RaftCommand {
            op: RaftOp::LeaveNode { node_id },
            schema_version: Uuid::new_v4(),
        };
        raft.client_write(leave_cmd)
            .await
            .map_err(|e| ClusterError::RaftError(format!("LeaveNode proposal failed: {e}")))?;

        // 2-4. In the full implementation, we would:
        //    - Query the ring for tokens owned by this node before removal
        //    - Find new owners for each range
        //    - Stream data to new owners
        //    For the MVP, S3 provides durability so data is not lost when a node
        //    leaves. The remaining nodes will pick up the ranges via the updated
        //    token map.

        tracing::info!(
            host_id = %host_id,
            node_id,
            "node decommission complete: LeaveNode committed"
        );

        Ok(())
    }

    /// Trigger join admission for a peer that connected while in cluster mode.
    ///
    /// Checks de-duplication (via `pending_joins`), approval (via `approved_nodes`
    /// or `auto_join`), and spawns an async task to propose `JoinNode` +
    /// `AssignTokens` via Raft.
    pub(super) fn trigger_cluster_join(&self, host_id: Uuid, addr: std::net::SocketAddr) {
        // De-duplicate: skip if already pending.
        {
            let mut pending = self.pending_joins.lock();
            if pending.contains(&host_id) {
                tracing::info!(peer = %host_id, "peer already pending join, skipping");
                return;
            }
            pending.push(host_id);
        }

        // Capture state needed by the spawned task.
        let approved_nodes = self.approved_nodes.lock().clone();
        let peer_node_id = uuid_to_node_id(host_id);
        let raft_instance = self.raft_instance.clone();
        let config_clone = self.config.clone();

        self.spawn_tracked(async move {
            // Check approval before touching Raft.
            if !config_clone.auto_join && !approved_nodes.contains(&host_id) {
                tracing::warn!(
                    peer = %host_id,
                    "peer not approved to join cluster, ignoring"
                );
                return;
            }

            let raft = match &**raft_instance.load() {
                Some(r) => r.clone(),
                None => {
                    tracing::warn!(
                        peer = %host_id,
                        "raft not initialized yet, cannot admit peer"
                    );
                    return;
                }
            };

            // Propose JoinNode via Raft.
            let node_info = NodeInfo {
                host_id,
                addr: addr.to_string(),
                data_center: config_clone.data_center.clone(),
                rack: config_clone.rack.clone(),
                state: NodeState::Normal,
            };

            let join_cmd = RaftCommand {
                op: RaftOp::JoinNode(node_info),
                schema_version: Uuid::new_v4(),
            };
            if let Err(e) = raft.client_write(join_cmd).await {
                tracing::warn!(peer = %host_id, %e, "JoinNode proposal failed");
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
                tracing::warn!(peer = %host_id, %e, "AssignTokens proposal failed");
                return;
            }

            tracing::info!(
                peer = %host_id,
                node_id = peer_node_id,
                "peer admitted to cluster via on_peer_connected"
            );
        });
    }
}
