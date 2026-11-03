//! Versioned membership control over the existing forwarding envelope.
//! Raw Raft-command forwarding remains delegated to the existing handler.
use super::super::{data_movement, ModeController};
use crate::{
    error::{ClusterError, Result},
    membership::{MembershipChanger, MembershipNetwork, MembershipOp},
    raft::{uuid_to_node_id, FerrosRaft, NodeInfo, NodeState, RaftCommand, RaftOp},
    ring::{strategy::ReplicationStrategy, TokenRing},
};
use bytes::Bytes;
use ferrosa_net::{
    codec::Lane,
    message::Message,
    peer::PeerManager,
    rpc::{handler::PeerId, RpcHandler},
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock, Weak},
    time::Duration,
};
use uuid::Uuid;

const MAGIC: &[u8] = b"FMOP\x01";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum ControlError {
    NotReady,
    Rejected(String),
}
impl From<String> for ControlError {
    fn from(value: String) -> Self {
        Self::Rejected(value)
    }
}
impl From<&str> for ControlError {
    fn from(value: &str) -> Self {
        Self::Rejected(value.into())
    }
}

pub(crate) async fn request(pm: &PeerManager, target: Uuid, op: MembershipOp) -> Result<()> {
    let mut body = MAGIC.to_vec();
    body.extend(bincode::serialize(&op).map_err(|e| ClusterError::Internal(e.to_string()))?);
    let reply = pm
        .send(
            target,
            Message::ClusterMembershipForward(Bytes::from(body)),
            // Control only enqueues bootstrap; payload transfer uses repair/Bulk.
            // Formation invitations may occupy the ordinary Data lane.
            Lane::Raft,
        )
        .await
        .map_err(ClusterError::Net)?;
    let Message::ClusterMembershipForwardAck(body) = reply else {
        return Err(ClusterError::Internal(
            "membership control: unexpected response".into(),
        ));
    };
    let payload = body
        .strip_prefix(MAGIC)
        .ok_or_else(|| ClusterError::NotImplemented {
            feature: "peer does not support versioned verified membership control".into(),
        })?;
    let result: std::result::Result<(), ControlError> =
        bincode::deserialize(payload).map_err(|e| ClusterError::Internal(e.to_string()))?;
    result.map_err(|error| match error {
        ControlError::NotReady => ClusterError::NotLeader { leader_id: None },
        ControlError::Rejected(message) => ClusterError::DataMovementUnverified(message),
    })
}

pub(crate) struct Handler {
    controller: Weak<ModeController>,
    legacy: Arc<dyn RpcHandler>,
}
impl Handler {
    pub(crate) fn new(controller: Weak<ModeController>, legacy: Arc<dyn RpcHandler>) -> Self {
        Self { controller, legacy }
    }
}
#[async_trait::async_trait]
impl RpcHandler for Handler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let Message::ClusterMembershipForward(body) = &msg else {
            return self.legacy.handle(from, msg).await;
        };
        if !body.starts_with(b"FMOP") {
            let guarded = self
                .controller
                .upgrade()
                .and_then(|controller| {
                    crate::raft_forward_codec::decode_forwarded_command(body)
                        .ok()
                        .map(|command| match command.op {
                            RaftOp::JoinNode(_) | RaftOp::AssignTokens { .. } => true,
                            RaftOp::SetNodeState {
                                node_id,
                                state: NodeState::Normal,
                            } => {
                                let voter = controller.raft().is_some_and(|r| {
                                    r.metrics()
                                        .borrow()
                                        .membership_config
                                        .membership()
                                        .voter_ids()
                                        .any(|id| id == node_id)
                                });
                                !voter
                                    || !controller
                                        .token_ring()
                                        .is_some_and(|r| r.bootstrap_complete(node_id))
                            }
                            RaftOp::RecordBootstrapComplete { node_id, .. } => {
                                node_id != uuid_to_node_id(from.0)
                                    || !controller.token_ring().is_some_and(|r| {
                                        r.get_node(node_id)
                                            .is_some_and(|n| n.state == NodeState::Joining)
                                    })
                            }
                            RaftOp::UpdateNodeInfo(info) => !controller
                                .token_ring()
                                .and_then(|r| r.get_node(uuid_to_node_id(info.host_id)).cloned())
                                .is_some_and(|existing| {
                                    existing.state == info.state
                                        && existing.data_center == info.data_center
                                }),
                            _ => false,
                        })
                })
                .unwrap_or(true);
            if guarded {
                let result = crate::raft_forward::process_forwarded_command(body, |_| async {
                    Err("legacy ownership/promotion requires verified membership admission".into())
                })
                .await;
                return Some(Message::ClusterMembershipForwardAck(result));
            }
            return self.legacy.handle(from, msg).await;
        }
        tracing::info!(sender=%from.0,"versioned membership control received");
        let result: std::result::Result<(),ControlError> = async {
            let payload = body.strip_prefix(MAGIC).ok_or_else(|| "unsupported membership control version".to_string())?;
            if payload.len() > 64 * 1024 { return Err("membership control exceeds metadata size limit".into()); }
            let op: MembershipOp = bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let controller = self.controller.upgrade().ok_or_else(|| "controller stopped".to_string())?;
            match op {
                MembershipOp::AddVoter { host_id, addr } => {
                    let raft = controller.raft().ok_or_else(|| "Raft not initialized".to_string())?;
                    if raft.current_leader().await != Some(uuid_to_node_id(controller.local_host_id)) { return Err("membership admission requires the current leader".into()); }
                    let sender_is_member = controller.token_ring().and_then(|r| r.get_node(uuid_to_node_id(from.0)).cloned()).is_some_and(|n| n.state.is_voter());
                    if from.0 != host_id && !sender_is_member { return Err("membership admission sender is not an existing voter or the joining node".into()); }
                    if !controller.config.auto_join && !controller.approved_nodes.lock().contains(&host_id) { return Err("joining node is not approved".into()); }
                    let pm=controller.peer_manager.load_full();
                    let cql=pm.as_ref().as_ref().and_then(|pm|pm.get_peer_cql_broadcast_sync(host_id));
                    let broadcast=pm.as_ref().as_ref().and_then(|pm|pm.get_peer_internode_broadcast_sync(host_id));
                    controller.trigger_cluster_join(host_id,addr,cql,broadcast);
                    Ok(())
                }
                MembershipOp::BootstrapData { host_id } => {
                    if host_id != controller.local_host_id { return Err("bootstrap target must be this node".into()); }
                    let raft = controller.raft().ok_or_else(|| "joining Raft not initialized".to_string())?;
                    let Some(leader)=raft.current_leader().await else { return Err(ControlError::NotReady); };
                    if leader != uuid_to_node_id(from.0) { return Err("only the current Raft leader may request bootstrap".into()); }
                    launch_bootstrap(&controller).map_err(|e|ControlError::Rejected(e.to_string()))
                }
                _ => Err("membership operation is not supported by verified admission".into()),
            }
        }.await;
        let mut ack = MAGIC.to_vec();
        ack.extend(bincode::serialize(&result).expect("membership result serializes"));
        Some(Message::ClusterMembershipForwardAck(Bytes::from(ack)))
    }
}

/// Legacy DDL envelopes must not provide an alternate unchecked join route.
pub(crate) struct DdlHandler(pub Arc<dyn RpcHandler>);
fn legacy_ddl_join(message: &Message) -> bool {
    let Message::PairDdlForward(body) = message else {
        return false;
    };
    let operation = crate::pair::ddl::DdlOperation::from_bytes(body)
        .or_else(|_| crate::pair::ddl::DdlEnvelope::from_bytes(body).map(|envelope| envelope.op));
    matches!(operation, Ok(crate::pair::ddl::DdlOperation::JoinNode(_)))
}
#[async_trait::async_trait]
impl RpcHandler for DdlHandler {
    async fn handle(&self, from: PeerId, message: Message) -> Option<Message> {
        if legacy_ddl_join(&message) {
            return None;
        }
        self.0.handle(from, message).await
    }
    async fn handle_outcome(
        &self,
        from: PeerId,
        message: Message,
    ) -> ferrosa_net::rpc::handler::HandlerOutcome {
        if legacy_ddl_join(&message) {
            return ferrosa_net::rpc::handler::HandlerOutcome::Fail {
                kind: ferrosa_net::rpc::error_reply::RemoteFailureKind::HandlerFailed,
                detail: "legacy DDL join requires versioned verified membership admission".into(),
            };
        }
        self.0.handle_outcome(from, message).await
    }
}

struct Network(Arc<RwLock<HashMap<u64, Uuid>>>);
impl MembershipNetwork for Network {
    fn register_node(&self, id: u64, host: Uuid) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, host);
    }
    fn unregister_node(&self, id: u64) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }
    fn contains(&self, id: u64) -> bool {
        self.0
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&id)
    }
}

/// Until joint placement exists, admission may add replicas but never evict one.
fn monotonic(
    ring: &TokenRing,
    info: &NodeInfo,
    schema: &ferrosa_schema::SchemaSnapshot,
) -> Result<()> {
    let existing: Vec<_> = ring
        .node_ids()
        .into_iter()
        .filter(|id| *id != uuid_to_node_id(info.host_id))
        .filter_map(|id| ring.get_node(id))
        .filter(|n| {
            n.state.is_voter() || matches!(n.state, NodeState::Learner { owns_tokens: true })
        })
        .collect();
    for (table, strategy) in data_movement::replicated_tables(schema)? {
        let allowed = match strategy {
            ReplicationStrategy::Simple { replication_factor } => {
                replication_factor > existing.len()
            }
            ReplicationStrategy::NetworkTopology { dc_rf } => {
                let rf = dc_rf.get(&info.data_center).copied().unwrap_or(0);
                rf == 0
                    || rf
                        > existing
                            .iter()
                            .filter(|n| n.data_center == info.data_center)
                            .count()
            }
        };
        if !allowed {
            return Err(ClusterError::NotImplemented { feature: format!("populated join would evict existing replicas of {table}; joint placement or write fencing is required") });
        }
    }
    Ok(())
}

async fn serialized<T>(
    gate: &tokio::sync::Mutex<()>,
    operation: impl std::future::Future<Output = T>,
) -> T {
    let _guard = gate.lock().await;
    operation.await
}

pub(crate) struct Admission {
    pub gate: Arc<tokio::sync::Mutex<()>>,
    pub raft: Arc<FerrosRaft>,
    pub node_map: Arc<RwLock<HashMap<u64, Uuid>>>,
    pub pm: Arc<PeerManager>,
    pub ring: Arc<arc_swap::ArcSwap<Option<Arc<TokenRing>>>>,
    pub schema: Arc<ferrosa_schema::Schema>,
    pub num_tokens: usize,
    pub cancel: tokio_util::sync::CancellationToken,
}
impl Admission {
    pub async fn run(&self, info: NodeInfo, addr: SocketAddr) -> Result<()> {
        serialized(&self.gate, self.run_locked(info, addr)).await
    }
    async fn run_locked(&self, mut info: NodeInfo, addr: SocketAddr) -> Result<()> {
        let node_id = uuid_to_node_id(info.host_id);
        tracing::info!(peer=%info.host_id,"verified learner admission started");
        let ring = self
            .ring
            .load_full()
            .as_ref()
            .clone()
            .ok_or_else(|| ClusterError::Internal("admission has no ring".into()))?;
        let snapshot = self.schema.snapshot();
        monotonic(&ring, &info, &snapshot)?;
        let changer = MembershipChanger::for_dc(
            info.data_center.clone(),
            self.raft.clone(),
            Arc::new(Network(self.node_map.clone())),
        )
        .with_node_metadata_defaults(info.data_center.clone(), info.rack.clone());
        changer
            .add_bootstrapping_learner(info.clone(), addr)
            .await
            .map_err(|e| ClusterError::RaftError(e.to_string()))?;
        info.state = NodeState::Joining;
        for op in [RaftOp::AssignTokens {
            node_id,
            tokens: crate::controller::token::deterministic_tokens_for_node(
                node_id,
                self.num_tokens,
            ),
        }] {
            self.raft
                .client_write(RaftCommand {
                    op,
                    schema_version: snapshot.version,
                })
                .await
                .map_err(|e| ClusterError::RaftError(e.to_string()))?;
        }
        tracing::info!(peer=%info.host_id,"requesting joiner-local verified data bootstrap");
        let dispatch = async {
            loop {
                match request(
                    &self.pm,
                    info.host_id,
                    MembershipOp::BootstrapData {
                        host_id: info.host_id,
                    },
                )
                .await
                {
                    Err(ClusterError::NotLeader { leader_id: None }) => {
                        tokio::time::sleep(Duration::from_millis(25)).await
                    }
                    result => return result,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), dispatch)
            .await
            .map_err(|_| {
                ClusterError::DataMovementUnverified(
                    "joiner has not published its leader identity; bootstrap was not authorized"
                        .into(),
                )
            })??;
        // Bootstrap is streamed and asynchronous. The ack only starts it; only
        // its committed verification record permits membership promotion.
        let wait = async {
            loop {
                if self
                    .ring
                    .load()
                    .as_ref()
                    .as_ref()
                    .is_some_and(|r| r.bootstrap_complete(node_id))
                {
                    return Ok::<(), ClusterError>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        };
        tokio::select! {
            _=self.cancel.cancelled()=>return Err(ClusterError::Internal("admission cancelled".into())),
            result=tokio::time::timeout(Duration::from_secs(300),wait)=>result.map_err(|_|ClusterError::DataMovementUnverified("joining data bootstrap did not verify before admission deadline; node stays Joining".into()))??,
        }
        let ring = self
            .ring
            .load_full()
            .as_ref()
            .clone()
            .ok_or_else(|| ClusterError::Internal("admission lost ring".into()))?;
        let final_schema = self.schema.snapshot();
        if final_schema.version != snapshot.version {
            return Err(ClusterError::DataMovementUnverified(
                "schema changed during joining data verification; retry bootstrap before promotion"
                    .into(),
            ));
        }
        monotonic(&ring, &info, &final_schema)?;
        changer
            .promote_learner_to_voter(info.host_id)
            .await
            .map_err(|e| ClusterError::RaftError(e.to_string()))
    }
}

struct BootstrapLease(Arc<std::sync::atomic::AtomicBool>);
impl Drop for BootstrapLease {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

fn launch_bootstrap(controller: &ModeController) -> Result<()> {
    let host = controller.local_host_id;
    let raft = controller
        .raft()
        .ok_or_else(|| ClusterError::Internal("bootstrap has no Raft".into()))?;
    let pm = controller
        .peer_manager
        .load_full()
        .as_ref()
        .clone()
        .ok_or_else(|| ClusterError::Internal("bootstrap has no peers".into()))?;
    let ring_holder = controller.ring.clone();
    let storage = controller.storage.clone();
    let schema = controller.schema.clone();
    let cancel = controller.cancel.clone();
    let claimed = controller.membership_bootstrap.clone();
    if claimed
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .is_err()
    {
        return Ok(());
    }
    let lease = BootstrapLease(claimed);
    controller.spawn_tracked(async move {
        let _lease=lease;
        tracing::info!(%host,"joiner-local bootstrap task started");
        let work=async {
            let id=uuid_to_node_id(host);
            // The control lane can overtake the metadata append. Wait for the
            // exact joining identity and tokens before planning verified ranges.
            let ring=loop {
                if let Some(ring)=ring_holder.load_full().as_ref().clone().filter(|r|r.get_node(id).is_some_and(|n|n.state==NodeState::Joining) && !r.tokens_for_node(id).is_empty()) { break ring; }
                tokio::time::sleep(Duration::from_millis(25)).await;
            };
            tracing::info!(%host,"joining token ownership applied; planning verified ranges");
            let snapshot=schema.snapshot();
            let tables=data_movement::replicated_tables(&snapshot)?;
            let executor=data_movement::bootstrap_executor(storage,pm.clone(),&ring,id);
            let proposer=data_movement::ForwardingProposer { raft, peer_manager:pm, ring:ring_holder, schema_version:snapshot.version };
            data_movement::bootstrap_data_verified(&proposer,&ring,id,&tables,&executor).await.map(|_|())
        };
        tokio::select! {
            _=cancel.cancelled()=>{},
            result=tokio::time::timeout(Duration::from_secs(300),work)=>match result { Ok(Ok(()))=>{}, other=>tracing::error!(?other,"joining bootstrap failed; replica remains Joining") },
        }

    });
    Ok(())
}

#[cfg(test)]
#[path = "membership_control_tests.rs"]
mod tests;
