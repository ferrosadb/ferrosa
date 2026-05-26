//! Forwarding Raft proposals from a non-leader to the elected leader.
//!
//! When a non-leader node calls [`openraft::Raft::client_write`], openraft
//! returns a `ForwardToLeader` hint instead of replicating the entry.  For
//! cluster-mode membership refreshes (e.g. `UpdateNodeInfo` triggered by
//! `on_peer_connected`) the proposal must still land on the leader so that
//! followers like the joining node get a corrected `addr` in the committed
//! membership snapshot — silently dropping the proposal lets the broken
//! `addr: ""` entry persist and the cluster never converges.
//!
//! This module mirrors the DDL forwarding path
//! (`crate::ddl_path::forward_ddl_to_leader`):
//!
//! 1. Non-leader serialises the [`crate::raft::RaftCommand`] with bincode and
//!    sends it as [`Message::ClusterMembershipForward`] on [`Lane::Data`].
//! 2. The leader runs [`ClusterMembershipForwardHandler`], deserialises the
//!    command, calls `client_write` locally, and replies with a
//!    [`Message::ClusterMembershipForwardAck`] carrying the bincode-serialised
//!    [`ForwardAckBody`].
//!
//! Unlike the DDL forwarder we use bincode (not JSON) because `RaftCommand`
//! already round-trips through bincode via the openraft log codec.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use crate::error::{ClusterError, Result};
use crate::raft::{FerrosRaft, RaftCommand};

/// Outcome of a Raft `client_write` call as seen by code that needs to react
/// to a [`openraft::error::ForwardToLeader`] hint.
///
/// Decouples the membership refresh path from openraft's nested error types
/// so the dispatch logic can be exercised without spinning up a full Raft.
#[derive(Debug)]
pub enum ProposeError {
    /// openraft asked us to forward to the named leader.  `None` means no
    /// leader is currently elected — the caller should retry later.
    Forward { leader_node_id: Option<u64> },
    /// Any other Raft failure — surfaced as a string for logging.
    Other(String),
}

/// Inspect the error returned by [`openraft::Raft::client_write`] and
/// classify it for the caller.
pub fn classify_client_write_error(
    err: &openraft::error::RaftError<
        u64,
        openraft::error::ClientWriteError<u64, openraft::BasicNode>,
    >,
) -> ProposeError {
    if let Some(fwd) = err.forward_to_leader() {
        return ProposeError::Forward {
            leader_node_id: fwd.leader_id,
        };
    }
    ProposeError::Other(err.to_string())
}

/// Dispatch a [`ProposeError`] outcome: forward to the leader if openraft
/// asked us to, otherwise surface a meaningful error.
///
/// Pulled out of [`crate::controller`] so the
/// classify-then-look-up-then-forward chain can be exercised without
/// constructing a real openraft instance — tests inject any combination of
/// `outcome`, `resolve_leader`, and `forward` they need.
pub async fn dispatch_propose_outcome<R, F, Fut>(
    outcome: std::result::Result<(), ProposeError>,
    cmd: RaftCommand,
    resolve_leader: R,
    forward: F,
) -> Result<()>
where
    R: FnOnce(u64) -> Option<uuid::Uuid>,
    F: FnOnce(uuid::Uuid, RaftCommand) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    match outcome {
        Ok(()) => Ok(()),
        Err(ProposeError::Forward {
            leader_node_id: Some(node_id),
        }) => match resolve_leader(node_id) {
            Some(uuid) => forward(uuid, cmd).await,
            None => Err(ClusterError::Internal(format!(
                "raft forward: leader node_id={node_id} not in token ring"
            ))),
        },
        Err(ProposeError::Forward {
            leader_node_id: None,
        }) => Err(ClusterError::Internal(
            "raft forward: no leader currently elected".into(),
        )),
        Err(ProposeError::Other(msg)) => Err(ClusterError::RaftError(msg)),
    }
}

/// Body of a [`Message::ClusterMembershipForwardAck`].
///
/// `Ok` indicates the leader applied the command via `client_write`.  `Err`
/// carries the leader's stringified error so the non-leader can surface it
/// to logs without trying to deserialise foreign error types across versions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ForwardAckBody {
    Ok,
    Err(String),
}

/// Forward `cmd` to the Raft leader so that the leader can call
/// `client_write` locally.
///
/// On success the leader has applied (or at minimum committed) the command;
/// on `Err` the caller should log and let the next `on_peer_connected` retry
/// the refresh.
pub async fn forward_raft_command_to_leader(
    peer_manager: &PeerManager,
    leader_uuid: uuid::Uuid,
    cmd: RaftCommand,
) -> Result<()> {
    let body = bincode::serialize(&cmd).map_err(|e| {
        ClusterError::Internal(format!("forward_raft_command_to_leader serialize: {e}"))
    })?;
    let body = Bytes::from(body);

    let resp = match peer_manager
        .send(
            leader_uuid,
            Message::ClusterMembershipForward(body.clone()),
            Lane::Data,
        )
        .await
    {
        Ok(resp) => resp,
        Err(e)
            if e.to_string().contains("unknown peer")
                || e.to_string().contains("no connection pool") =>
        {
            let addr = peer_manager.peer_addr(leader_uuid).await.ok_or_else(|| {
                ClusterError::Internal(format!(
                    "raft forward: missing address for leader {leader_uuid}"
                ))
            })?;
            peer_manager
                .ensure_peer(leader_uuid, &addr)
                .await
                .map_err(ClusterError::Net)?;
            peer_manager
                .send(
                    leader_uuid,
                    Message::ClusterMembershipForward(body),
                    Lane::Data,
                )
                .await
                .map_err(ClusterError::Net)?
        }
        Err(e) => return Err(ClusterError::Net(e)),
    };

    match resp {
        Message::ClusterMembershipForwardAck(body) => decode_ack(&body),
        other => Err(ClusterError::Internal(format!(
            "unexpected response to ClusterMembershipForward: {:?}",
            other.msg_type()
        ))),
    }
}

fn decode_ack(body: &Bytes) -> Result<()> {
    if body.is_empty() {
        return Ok(());
    }
    let ack: ForwardAckBody = bincode::deserialize(body).map_err(|e| {
        ClusterError::Internal(format!("ClusterMembershipForwardAck decode failed: {e}"))
    })?;
    match ack {
        ForwardAckBody::Ok => Ok(()),
        ForwardAckBody::Err(msg) => Err(ClusterError::RaftError(msg)),
    }
}

fn encode_ack(body: &ForwardAckBody) -> Bytes {
    Bytes::from(bincode::serialize(body).expect("ForwardAckBody must serialize"))
}

/// Decode `body` as a [`RaftCommand`], invoke `propose`, and produce the
/// bincode-serialized [`ForwardAckBody`] that should be wrapped in a
/// [`Message::ClusterMembershipForwardAck`].
///
/// Extracted from [`ClusterMembershipForwardHandler::handle`] so the
/// decode-and-respond logic can be unit-tested without spinning up an openraft
/// instance.
pub(crate) async fn process_forwarded_command<F, Fut>(body: &[u8], propose: F) -> Bytes
where
    F: FnOnce(RaftCommand) -> Fut,
    Fut: Future<Output = std::result::Result<(), String>>,
{
    let cmd: RaftCommand = match bincode::deserialize(body) {
        Ok(c) => c,
        Err(e) => return encode_ack(&ForwardAckBody::Err(format!("decode: {e}"))),
    };
    let ack = match propose(cmd).await {
        Ok(()) => ForwardAckBody::Ok,
        Err(msg) => ForwardAckBody::Err(msg),
    };
    encode_ack(&ack)
}

/// RPC handler registered on the Raft leader to apply forwarded
/// [`RaftCommand`]s.
///
/// Non-leader nodes that hit `ForwardToLeader` from their local
/// [`openraft::Raft::client_write`] send the proposal here via
/// [`Message::ClusterMembershipForward`].  This handler decodes it, calls
/// `client_write` locally (which succeeds because we are the leader), and
/// replies with a [`Message::ClusterMembershipForwardAck`].
pub struct ClusterMembershipForwardHandler {
    raft: Arc<FerrosRaft>,
}

impl ClusterMembershipForwardHandler {
    pub fn new(raft: Arc<FerrosRaft>) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl RpcHandler for ClusterMembershipForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::ClusterMembershipForward(b) => b,
            _ => return None,
        };
        let raft = self.raft.clone();
        let ack_bytes = process_forwarded_command(&body, move |cmd| async move {
            raft.client_write(cmd)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
        .await;
        Some(Message::ClusterMembershipForwardAck(ack_bytes))
    }
}

/// Startup handler for membership forwards received before this node has
/// entered cluster mode.
///
/// Returning an explicit nack keeps callers from waiting for the RPC timeout
/// and, more importantly, prevents "no handler registered" drops during rolling
/// restarts. `transition_to_cluster` replaces this with
/// [`LazyClusterMembershipForwardHandler`] once the Raft initialization path is
/// active.
pub struct ClusterMembershipForwardUnavailableHandler;

#[async_trait]
impl RpcHandler for ClusterMembershipForwardUnavailableHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let Message::ClusterMembershipForward(_) = msg else {
            return None;
        };
        Some(Message::ClusterMembershipForwardAck(encode_ack(
            &ForwardAckBody::Err(
                "ClusterMembershipForward: node has not entered cluster mode".to_string(),
            ),
        )))
    }
}

/// RPC handler registered before Raft initialization completes.
///
/// Reconnect metadata refreshes can be forwarded while a recreated node is
/// still building its Raft handle. Returning "no handler" in that window makes
/// peer metadata convergence depend on a later reconnect. This mirrors the
/// lazy Raft Append/Vote handlers: the message is handled immediately, then the
/// proposal waits for the Raft handle or returns an explicit ack error.
pub struct LazyClusterMembershipForwardHandler {
    raft: crate::raft::handlers::LazyRaft,
}

impl LazyClusterMembershipForwardHandler {
    pub fn new(raft: crate::raft::handlers::LazyRaft) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl RpcHandler for LazyClusterMembershipForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::ClusterMembershipForward(b) => b,
            _ => return None,
        };
        let Some(raft) = self.raft.get().await else {
            let ack = encode_ack(&ForwardAckBody::Err(
                "ClusterMembershipForward: raft not initialized".to_string(),
            ));
            return Some(Message::ClusterMembershipForwardAck(ack));
        };
        let ack_bytes = process_forwarded_command(&body, move |cmd| async move {
            raft.client_write(cmd)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
        .await;
        Some(Message::ClusterMembershipForwardAck(ack_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use ferrosa_net::codec::MsgType;
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::PeerEventListener;
    use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
    use ferrosa_net::rpc::server::RpcServer;
    use ferrosa_net::rpc::HandlerRegistry;
    use uuid::Uuid;

    use crate::raft::{NodeInfo, NodeState, RaftOp};

    struct NoopListener;
    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
    }

    fn encoded_ack(body: &ForwardAckBody) -> Bytes {
        Bytes::from(bincode::serialize(body).expect("ack must serialize"))
    }

    struct EchoOkHandler;

    #[async_trait::async_trait]
    impl RpcHandler for EchoOkHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            match msg {
                Message::ClusterMembershipForward(_) => Some(Message::ClusterMembershipForwardAck(
                    encoded_ack(&ForwardAckBody::Ok),
                )),
                _ => None,
            }
        }
    }

    struct EchoErrHandler;

    #[async_trait::async_trait]
    impl RpcHandler for EchoErrHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            match msg {
                Message::ClusterMembershipForward(_) => Some(Message::ClusterMembershipForwardAck(
                    encoded_ack(&ForwardAckBody::Err("simulated leader rejection".into())),
                )),
                _ => None,
            }
        }
    }

    async fn start_rpc_server(
        msg_type: MsgType,
        handler: Arc<dyn RpcHandler>,
    ) -> (Arc<RpcServer>, std::net::SocketAddr, Uuid) {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(msg_type, handler);
        let server = Arc::new(RpcServer::new(config, server_id, registry));
        let addr = server.start_and_get_addr().await.unwrap();
        (server, addr, server_id)
    }

    fn sample_command() -> RaftCommand {
        RaftCommand {
            op: RaftOp::UpdateNodeInfo(NodeInfo {
                host_id: Uuid::new_v4(),
                addr: "10.0.0.5:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            }),
            schema_version: Uuid::new_v4(),
        }
    }

    #[test]
    fn forward_ack_body_roundtrip() {
        let ok_bytes = encoded_ack(&ForwardAckBody::Ok);
        decode_ack(&ok_bytes).unwrap();

        let err_bytes = encoded_ack(&ForwardAckBody::Err("boom".into()));
        match decode_ack(&err_bytes) {
            Err(ClusterError::RaftError(msg)) => assert_eq!(msg, "boom"),
            other => panic!("expected RaftError(\"boom\"), got {other:?}"),
        }
    }

    #[test]
    fn forward_ack_body_empty_payload_is_treated_as_ok() {
        // Older receivers may emit Bytes::new() for success; preserve that contract.
        decode_ack(&Bytes::new()).unwrap();
    }

    #[tokio::test]
    async fn forward_raft_command_to_leader_round_trips_via_existing_peer() {
        let (server, addr, leader_uuid) =
            start_rpc_server(MsgType::ClusterMembershipForward, Arc::new(EchoOkHandler)).await;

        let peer_manager = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        peer_manager.add_peer_entry((leader_uuid, addr)).await;

        forward_raft_command_to_leader(&peer_manager, leader_uuid, sample_command())
            .await
            .unwrap();

        assert!(
            peer_manager.has_peer(leader_uuid),
            "raft forward path should reconnect and cache the leader peer"
        );

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn dispatch_propose_outcome_invokes_forwarder_when_leader_is_known() {
        use std::sync::Mutex;

        let captured: Arc<Mutex<Option<Uuid>>> = Arc::new(Mutex::new(None));
        let captured_clone = captured.clone();
        let leader_uuid = Uuid::from_u128(0xABCDEF);

        let result = dispatch_propose_outcome(
            Err(ProposeError::Forward {
                leader_node_id: Some(42),
            }),
            sample_command(),
            |node_id| {
                assert_eq!(node_id, 42);
                Some(leader_uuid)
            },
            move |uuid, _cmd| {
                let captured = captured_clone.clone();
                async move {
                    *captured.lock().unwrap() = Some(uuid);
                    Ok(())
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(*captured.lock().unwrap(), Some(leader_uuid));
    }

    #[tokio::test]
    async fn dispatch_propose_outcome_returns_internal_when_leader_uuid_unknown() {
        let result = dispatch_propose_outcome(
            Err(ProposeError::Forward {
                leader_node_id: Some(99),
            }),
            sample_command(),
            |_node_id| None,
            |_uuid, _cmd| async move {
                panic!("forwarder must not run when leader UUID cannot be resolved")
            },
        )
        .await;

        match result {
            Err(ClusterError::Internal(m)) => assert!(m.contains("99")),
            other => panic!("expected Internal error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dispatch_propose_outcome_returns_internal_when_no_leader_elected() {
        let result = dispatch_propose_outcome(
            Err(ProposeError::Forward {
                leader_node_id: None,
            }),
            sample_command(),
            |_| panic!("resolver must not run without a leader hint"),
            |_uuid, _cmd| async move { panic!("forwarder must not run") },
        )
        .await;

        assert!(matches!(result, Err(ClusterError::Internal(_))));
    }

    #[tokio::test]
    async fn dispatch_propose_outcome_passes_through_ok_outcomes() {
        let result = dispatch_propose_outcome(
            Ok(()),
            sample_command(),
            |_| panic!("resolver must not run on Ok"),
            |_uuid, _cmd| async move { panic!("forwarder must not run on Ok") },
        )
        .await;

        assert!(result.is_ok());
    }

    #[test]
    fn classify_client_write_error_extracts_leader_node_id() {
        use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
        use openraft::BasicNode;

        let err: RaftError<u64, ClientWriteError<u64, BasicNode>> =
            RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::new(
                42,
                BasicNode {
                    addr: "10.0.0.42:7000".into(),
                },
            )));

        match classify_client_write_error(&err) {
            ProposeError::Forward {
                leader_node_id: Some(42),
            } => {}
            other => panic!("expected Forward(Some(42)), got {other:?}"),
        }
    }

    #[test]
    fn classify_client_write_error_handles_empty_forward_hint() {
        use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
        use openraft::BasicNode;

        let err: RaftError<u64, ClientWriteError<u64, BasicNode>> =
            RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::<
                u64,
                BasicNode,
            >::empty()));

        match classify_client_write_error(&err) {
            ProposeError::Forward {
                leader_node_id: None,
            } => {}
            other => panic!("expected Forward(None), got {other:?}"),
        }
    }

    #[test]
    fn classify_client_write_error_falls_back_to_other_for_non_forward_errors() {
        use openraft::error::{ClientWriteError, EmptyMembership, RaftError};
        use openraft::BasicNode;

        let err: RaftError<u64, ClientWriteError<u64, BasicNode>> = RaftError::APIError(
            ClientWriteError::ChangeMembershipError(EmptyMembership {}.into()),
        );

        match classify_client_write_error(&err) {
            ProposeError::Other(msg) => assert!(!msg.is_empty()),
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_forwarded_command_emits_ok_ack_when_propose_succeeds() {
        let cmd = sample_command();
        let body = bincode::serialize(&cmd).unwrap();

        let bytes = process_forwarded_command(&body, |received: RaftCommand| async move {
            // The handler must deserialise the wire bytes back into the same op shape.
            match received.op {
                RaftOp::UpdateNodeInfo(info) => assert_eq!(info.addr, "10.0.0.5:7000"),
                other => panic!("unexpected op: {other:?}"),
            }
            Ok::<(), String>(())
        })
        .await;

        let decoded: ForwardAckBody = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded, ForwardAckBody::Ok);
    }

    #[tokio::test]
    async fn process_forwarded_command_emits_err_ack_when_propose_fails() {
        let body = bincode::serialize(&sample_command()).unwrap();

        let bytes = process_forwarded_command(&body, |_cmd: RaftCommand| async move {
            Err::<(), String>("not leader after all".to_string())
        })
        .await;

        let decoded: ForwardAckBody = bincode::deserialize(&bytes).unwrap();
        match decoded {
            ForwardAckBody::Err(m) => assert!(m.contains("not leader after all")),
            other => panic!("expected Err ack, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_forwarded_command_emits_err_ack_on_malformed_payload() {
        let bytes =
            process_forwarded_command(b"not-a-valid-bincode-raftcommand", |_cmd| async move {
                Ok::<(), String>(())
            })
            .await;

        let decoded: ForwardAckBody = bincode::deserialize(&bytes).unwrap();
        match decoded {
            ForwardAckBody::Err(m) => assert!(m.starts_with("decode: ")),
            other => panic!("expected decode-error Err ack, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn forward_raft_command_to_leader_surfaces_leader_error() {
        let (server, addr, leader_uuid) =
            start_rpc_server(MsgType::ClusterMembershipForward, Arc::new(EchoErrHandler)).await;

        let peer_manager = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ));
        peer_manager.add_peer_entry((leader_uuid, addr)).await;

        let err = forward_raft_command_to_leader(&peer_manager, leader_uuid, sample_command())
            .await
            .expect_err("leader rejection must propagate");
        match err {
            ClusterError::RaftError(m) => assert!(m.contains("simulated leader rejection")),
            other => panic!("expected RaftError, got {other:?}"),
        }

        server.shutdown(std::time::Duration::from_millis(50)).await;
    }
}
