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
    // Cap'n Proto is chosen ONLY for a peer that advertised `CAP_RAFT_SCHEMA_CAPNP`;
    // a peer on the previous build is sent the bincode body it already understands.
    // The receiver tells the two apart by the frame's versioned wire magic, and an
    // op the frame does not cover keeps the bincode body either way.
    let capnp_allowed = peer_manager
        .peer_capabilities(leader_uuid)
        .await
        .is_some_and(|caps| caps & ferrosa_net::handshake::CAP_RAFT_SCHEMA_CAPNP != 0);
    let body =
        crate::raft_forward_codec::encode_forwarded_command(&cmd, capnp_allowed).map_err(|e| {
            ClusterError::Internal(format!("forward_raft_command_to_leader serialize: {e}"))
        })?;

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
    let cmd: RaftCommand = match crate::raft_forward_codec::decode_forwarded_command(body) {
        Ok(c) => c,
        Err(e) => return encode_ack(&ForwardAckBody::Err(e.to_string())),
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

    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use ferrosa_net::codec::MsgType;
    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::PeerEventListener;
    use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
    use ferrosa_net::rpc::server::RpcServer;
    use ferrosa_net::rpc::HandlerRegistry;
    use ferrosa_schema::metadata::{
        CachingParams, ClusteringOrder, ColumnKind, ColumnMask, ColumnMetadata, KeyspaceMetadata,
        KeyspaceUpdates, ReplicationParams, TableFlag, TableMetadata, TableParams, TableUpdates,
    };
    use indexmap::IndexMap;
    use uuid::Uuid;

    use crate::raft::{DataMovementEvidence, NodeInfo, NodeState, RaftCommand, RaftOp};
    use crate::raft_forward_codec::{
        decode_forwarded_command, encode_forwarded_command, ForwardFrameError,
        FORWARD_RAFT_FRAME_MAGIC, FORWARD_RAFT_FRAME_VERSION,
    };

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

    // =====================================================================
    // Forwarded schema/membership command frame (CAP_RAFT_SCHEMA_CAPNP).
    //
    // The frame is added ALONGSIDE the shipped bincode body; the receiver
    // dual-reads. Values are asserted, never counts — a frame that decodes to
    // the wrong keyspace/table/column is the failure these tests exist to catch.
    // =====================================================================

    /// Build a `[magic][version]`-prefixed Cap'n Proto `ForwardedRaftCommand`
    /// frame **by hand** from the generated schema, independent of
    /// `raft_forward_codec`'s production encoder.
    ///
    /// This is a captured fixture, not a re-implementation: naming no production
    /// seam is what lets the red test below compile against the pre-change tree
    /// and fail on the handler's behaviour instead of on a missing symbol. The
    /// magic/version literals are pinned here so a change to either constant is a
    /// deliberate two-place edit.
    fn hand_built_create_keyspace_frame(cmd: &RaftCommand) -> Bytes {
        use capnp::message::Builder;
        use capnp::serialize;

        let RaftOp::CreateKeyspace(ks) = &cmd.op else {
            panic!("fixture expects a CreateKeyspace command");
        };

        let mut message = Builder::new_default();
        {
            let mut root = message
                .init_root::<ferrosa_net::protocol::envelope_capnp::forwarded_raft_command::Builder>(
                );
            root.set_magic(0x4652_5746); // == FORWARD_RAFT_FRAME_MAGIC
            root.set_version(1); // == FORWARD_RAFT_FRAME_VERSION
            root.set_schema_version(cmd.schema_version.as_bytes());
            let mut out = root.init_op().init_create_keyspace();
            out.set_name(ks.name.as_str());
            out.set_durable_writes(ks.durable_writes);
            let mut replication = out.init_replication();
            replication.set_strategy(ks.replication.strategy.as_str());
            let mut options = replication.init_options(ks.replication.options.len() as u32);
            for (idx, (key, value)) in ks.replication.options.iter().enumerate() {
                let mut pair = options.reborrow().get(idx as u32);
                pair.set_key(key.as_str());
                pair.set_value(value.as_str());
            }
        }

        let words = serialize::write_message_to_words(&message);
        let mut body = Vec::with_capacity(5 + words.len());
        body.extend_from_slice(&0x4652_5746u32.to_be_bytes());
        body.push(1u8);
        body.extend_from_slice(&words);
        Bytes::from(body)
    }

    fn sample_keyspace() -> KeyspaceMetadata {
        let mut options = HashMap::new();
        options.insert("dc1".to_string(), "3".to_string());
        options.insert("dc2".to_string(), "2".to_string());
        KeyspaceMetadata {
            name: "app".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "NetworkTopologyStrategy".to_string(),
                options,
            },
        }
    }

    fn sample_column(
        name: &str,
        kind: ColumnKind,
        position: i32,
        column_type: &str,
        clustering_order: ClusteringOrder,
        mask: Option<ColumnMask>,
    ) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position,
            column_type: column_type.to_string(),
            clustering_order,
            mask,
        }
    }

    fn sample_table_params() -> TableParams {
        let mut compaction = HashMap::new();
        compaction.insert(
            "class".to_string(),
            "SizeTieredCompactionStrategy".to_string(),
        );
        compaction.insert("max_threshold".to_string(), "32".to_string());
        let mut compression = HashMap::new();
        compression.insert(
            "sstable_compression".to_string(),
            "ZstdCompressor".to_string(),
        );
        TableParams {
            bloom_filter_fp_chance: 0.02,
            caching: CachingParams {
                keys: "ALL".to_string(),
                rows_per_partition: "128".to_string(),
            },
            comment: "orders by customer".to_string(),
            compaction,
            compression,
            crc_check_chance: 0.9,
            default_time_to_live: 3600,
            gc_grace_seconds: 86_400,
            max_index_interval: 1024,
            min_index_interval: 64,
            memtable_flush_period_in_ms: 500,
            speculative_retry: "99PERCENTILE".to_string(),
            additional_write_policy: "99PERCENTILE".to_string(),
            cdc: true,
            read_repair: "BLOCKING".to_string(),
            allow_auto_snapshot: false,
            incremental_backups: true,
        }
    }

    /// The richest sample of the framed DDL vocabulary: a table whose columns,
    /// key order, flags, extensions and params all carry distinct values, so a
    /// field the codec drops or misroutes shows up as a mismatch (not a count).
    fn sample_table() -> TableMetadata {
        let mut columns = IndexMap::new();
        let pk = sample_column(
            "customer_id",
            ColumnKind::PartitionKey,
            0,
            "uuid",
            ClusteringOrder::None,
            None,
        );
        let ck = sample_column(
            "order_ts",
            ColumnKind::Clustering,
            0,
            "timestamp",
            ClusteringOrder::Desc,
            None,
        );
        let total = sample_column(
            "total",
            ColumnKind::Regular,
            0,
            "decimal",
            ClusteringOrder::None,
            None,
        );
        let note = sample_column(
            "note",
            ColumnKind::Static,
            0,
            "text",
            ClusteringOrder::None,
            Some(ColumnMask {
                function_name: "mask_default".to_string(),
                arguments: vec!["***".to_string()],
            }),
        );
        columns.insert(pk.name.clone(), pk);
        columns.insert(ck.name.clone(), ck);
        columns.insert(total.name.clone(), total);
        columns.insert(note.name.clone(), note);

        let mut extensions = HashMap::new();
        extensions.insert("graph.type".to_string(), "vertex".to_string());

        TableMetadata {
            keyspace: "app".to_string(),
            name: "orders".to_string(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["customer_id".to_string()],
            clustering_key: vec![("order_ts".to_string(), ClusteringOrder::Desc)],
            params: sample_table_params(),
            flags: HashSet::from([TableFlag::Compound, TableFlag::Counter]),
            extensions,
            is_system: false,
        }
    }

    fn assert_table_params_eq(actual: &TableParams, expected: &TableParams) {
        assert_eq!(
            actual.bloom_filter_fp_chance,
            expected.bloom_filter_fp_chance
        );
        assert_eq!(actual.caching.keys, expected.caching.keys);
        assert_eq!(
            actual.caching.rows_per_partition,
            expected.caching.rows_per_partition
        );
        assert_eq!(actual.comment, expected.comment);
        assert_eq!(actual.compaction, expected.compaction);
        assert_eq!(actual.compression, expected.compression);
        assert_eq!(actual.crc_check_chance, expected.crc_check_chance);
        assert_eq!(actual.default_time_to_live, expected.default_time_to_live);
        assert_eq!(actual.gc_grace_seconds, expected.gc_grace_seconds);
        assert_eq!(actual.max_index_interval, expected.max_index_interval);
        assert_eq!(actual.min_index_interval, expected.min_index_interval);
        assert_eq!(
            actual.memtable_flush_period_in_ms,
            expected.memtable_flush_period_in_ms
        );
        assert_eq!(actual.speculative_retry, expected.speculative_retry);
        assert_eq!(
            actual.additional_write_policy,
            expected.additional_write_policy
        );
        assert_eq!(actual.cdc, expected.cdc);
        assert_eq!(actual.read_repair, expected.read_repair);
        assert_eq!(actual.allow_auto_snapshot, expected.allow_auto_snapshot);
        assert_eq!(actual.incremental_backups, expected.incremental_backups);
    }

    fn assert_table_metadata_eq(actual: &TableMetadata, expected: &TableMetadata) {
        assert_eq!(actual.keyspace, expected.keyspace);
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.id, expected.id);
        assert_eq!(actual.is_system, expected.is_system);
        assert_eq!(actual.partition_key, expected.partition_key);
        assert_eq!(actual.clustering_key, expected.clustering_key);
        assert_eq!(actual.flags, expected.flags);
        assert_eq!(actual.extensions, expected.extensions);
        assert_eq!(
            actual.columns.len(),
            expected.columns.len(),
            "column count differs"
        );
        for (name, expected_column) in &expected.columns {
            let actual_column = actual
                .columns
                .get(name)
                .unwrap_or_else(|| panic!("column {name} missing after round-trip"));
            assert_eq!(actual_column, expected_column, "column {name} differs");
        }
        assert_table_params_eq(&actual.params, &expected.params);
    }

    fn assert_node_info_eq(actual: &NodeInfo, expected: &NodeInfo) {
        assert_eq!(actual.host_id, expected.host_id);
        assert_eq!(actual.addr, expected.addr);
        assert_eq!(actual.data_center, expected.data_center);
        assert_eq!(actual.rack, expected.rack);
        assert_eq!(actual.state, expected.state);
        assert_eq!(actual.cql_broadcast, expected.cql_broadcast);
    }

    /// Decode the handler's ack, requiring success; a refusal is surfaced with
    /// its own text so the failing arm names itself.
    fn require_ok_ack(ack: &Bytes) -> ForwardAckBody {
        let decoded: ForwardAckBody = bincode::deserialize(ack).expect("ack must be bincode");
        if let ForwardAckBody::Err(message) = &decoded {
            panic!("handler refused the forwarded command: {message}");
        }
        decoded
    }

    /// Drive the real handler entry point and return the command it proposed.
    async fn propose_through_handler(body: &[u8]) -> RaftCommand {
        let proposed: Arc<Mutex<Option<RaftCommand>>> = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&proposed);
        let ack = process_forwarded_command(body, |received: RaftCommand| {
            let sink = Arc::clone(&sink);
            async move {
                *sink.lock().unwrap() = Some(received);
                Ok::<(), String>(())
            }
        })
        .await;
        require_ok_ack(&ack);
        let taken = {
            let mut guard = proposed.lock().unwrap();
            guard.take()
        };
        taken.expect("the handler must have proposed the forwarded command")
    }

    /// RED as written: `process_forwarded_command` bincode-decodes the body, so a
    /// Cap'n Proto frame is refused with `decode: ...` and `propose` never runs.
    /// The VALUES assertion therefore never holds — the failure is the handler's
    /// decode behaviour, not a symbol added by this change. The fixture names
    /// only generated schema builders and the magic literal, so it compiles
    /// against the pre-change tree.
    #[tokio::test]
    async fn forwarded_capnp_frame_proposes_the_schema_values() {
        let cmd = RaftCommand {
            op: RaftOp::CreateKeyspace(sample_keyspace()),
            schema_version: Uuid::new_v4(),
        };
        let schema_version = cmd.schema_version;
        let body = hand_built_create_keyspace_frame(&cmd);

        let proposed = propose_through_handler(&body).await;

        match proposed.op {
            RaftOp::CreateKeyspace(ks) => {
                assert_eq!(ks.name, "app");
                assert!(ks.durable_writes);
                assert_eq!(ks.replication.strategy, "NetworkTopologyStrategy");
                assert_eq!(
                    ks.replication.options.get("dc1").map(String::as_str),
                    Some("3")
                );
                assert_eq!(
                    ks.replication.options.get("dc2").map(String::as_str),
                    Some("2")
                );
            }
            other => panic!("expected CreateKeyspace, got {other:?}"),
        }
        assert_eq!(proposed.schema_version, schema_version);
    }

    /// The production encoder and the handler's decoder agree on a rich
    /// `TableMetadata`. (Regression guard written AFTER the encoder — the red
    /// that produced the code is `forwarded_capnp_frame_proposes_the_schema_values`.)
    #[tokio::test]
    async fn forwarded_frame_round_trips_a_rich_table_metadata() {
        let table = sample_table();
        let cmd = RaftCommand {
            op: RaftOp::CreateTable(Box::new(table.clone())),
            schema_version: Uuid::new_v4(),
        };
        let schema_version = cmd.schema_version;

        let body = encode_forwarded_command(&cmd, true).expect("capnp frame encodes");
        assert_eq!(
            u32::from_be_bytes([body[0], body[1], body[2], body[3]]),
            FORWARD_RAFT_FRAME_MAGIC,
            "the framed body must carry the versioned magic"
        );
        assert_eq!(body[4], FORWARD_RAFT_FRAME_VERSION);

        let proposed = propose_through_handler(&body).await;
        match proposed.op {
            RaftOp::CreateTable(decoded) => assert_table_metadata_eq(&decoded, &table),
            other => panic!("expected CreateTable, got {other:?}"),
        }
        assert_eq!(proposed.schema_version, schema_version);

        // The decoder independently agrees on the same values.
        let direct = decode_forwarded_command(&body).expect("frame decodes");
        match direct.op {
            RaftOp::CreateTable(decoded) => assert_table_metadata_eq(&decoded, &table),
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    /// Round-trip one op through the production encoder and decoder, returning the
    /// decoded op; also asserts the leader's schema version survives.
    fn round_trip_op(op: RaftOp) -> RaftOp {
        let cmd = RaftCommand {
            op,
            schema_version: Uuid::new_v4(),
        };
        let body = encode_forwarded_command(&cmd, true).expect("capnp frame encodes");
        let decoded = decode_forwarded_command(&body).expect("frame decodes");
        assert_eq!(decoded.schema_version, cmd.schema_version);
        decoded.op
    }

    /// The membership ops the same forward path carries round-trip by value.
    #[test]
    fn forwarded_frame_round_trips_the_membership_ops() {
        let joiner = NodeInfo {
            host_id: Uuid::new_v4(),
            addr: "10.0.0.7:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack2".to_string(),
            state: NodeState::Joining,
            cql_broadcast: Some("10.0.0.7:9042".to_string()),
        };
        let update = NodeInfo {
            host_id: joiner.host_id,
            addr: "10.0.0.7:7100".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack3".to_string(),
            state: NodeState::Learner { owns_tokens: true },
            cql_broadcast: None,
        };
        let approve_host = Uuid::new_v4();

        match round_trip_op(RaftOp::JoinNode(joiner.clone())) {
            RaftOp::JoinNode(info) => assert_node_info_eq(&info, &joiner),
            other => panic!("expected JoinNode, got {other:?}"),
        }
        match round_trip_op(RaftOp::UpdateNodeInfo(update.clone())) {
            RaftOp::UpdateNodeInfo(info) => assert_node_info_eq(&info, &update),
            other => panic!("expected UpdateNodeInfo, got {other:?}"),
        }
        match round_trip_op(RaftOp::ApproveNode {
            host_id: approve_host,
        }) {
            RaftOp::ApproveNode { host_id } => assert_eq!(host_id, approve_host),
            other => panic!("expected ApproveNode, got {other:?}"),
        }
        match round_trip_op(RaftOp::SetNodeState {
            node_id: 42,
            state: NodeState::Learner { owns_tokens: false },
        }) {
            RaftOp::SetNodeState { node_id, state } => {
                assert_eq!(node_id, 42);
                assert_eq!(state, NodeState::Learner { owns_tokens: false });
            }
            other => panic!("expected SetNodeState, got {other:?}"),
        }
        match round_trip_op(RaftOp::RecordBootstrapComplete {
            node_id: 7,
            evidence: DataMovementEvidence {
                sessions: 5,
                ranges: 12,
                partitions: 41,
            },
        }) {
            RaftOp::RecordBootstrapComplete { node_id, evidence } => {
                assert_eq!(node_id, 7);
                assert_eq!(evidence.sessions, 5);
                assert_eq!(evidence.ranges, 12);
                assert_eq!(evidence.partitions, 41);
            }
            other => panic!("expected RecordBootstrapComplete, got {other:?}"),
        }
    }

    /// The remaining framed DDL vocabulary (drop / alter) round-trips by value,
    /// including the `Option` fields a `has_*` pointer distinguishes from empty.
    #[test]
    fn forwarded_frame_round_trips_drop_and_alter_ops() {
        let add_columns = vec![sample_column(
            "status",
            ColumnKind::Regular,
            0,
            "text",
            ClusteringOrder::None,
            None,
        )];
        let updates = TableUpdates {
            params: Some(sample_table_params()),
            add_columns,
            drop_columns: vec!["legacy_note".to_string()],
            extensions: Some(HashMap::from([(
                "graph.label".to_string(),
                "Order".to_string(),
            )])),
        };

        let alter_keyspace = KeyspaceUpdates {
            replication: Some(ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "3".to_string())]),
            }),
            durable_writes: Some(false),
        };

        match round_trip_op(RaftOp::DropKeyspace("app".to_string())) {
            RaftOp::DropKeyspace(name) => assert_eq!(name, "app"),
            other => panic!("expected DropKeyspace, got {other:?}"),
        }
        match round_trip_op(RaftOp::DropTable {
            keyspace: "app".to_string(),
            table: "orders".to_string(),
        }) {
            RaftOp::DropTable { keyspace, table } => {
                assert_eq!(keyspace, "app");
                assert_eq!(table, "orders");
            }
            other => panic!("expected DropTable, got {other:?}"),
        }
        match round_trip_op(RaftOp::AlterKeyspace {
            name: "app".to_string(),
            updates: alter_keyspace,
        }) {
            RaftOp::AlterKeyspace { name, updates } => {
                assert_eq!(name, "app");
                assert_eq!(updates.durable_writes, Some(false));
                let replication = updates.replication.expect("replication present");
                assert_eq!(replication.strategy, "SimpleStrategy");
                assert_eq!(
                    replication
                        .options
                        .get("replication_factor")
                        .map(String::as_str),
                    Some("3")
                );
            }
            other => panic!("expected AlterKeyspace, got {other:?}"),
        }
        match round_trip_op(RaftOp::AlterTable {
            keyspace: "app".to_string(),
            table: "orders".to_string(),
            updates: Box::new(updates),
        }) {
            RaftOp::AlterTable {
                keyspace,
                table,
                updates,
            } => {
                assert_eq!(keyspace, "app");
                assert_eq!(table, "orders");
                assert_eq!(updates.drop_columns, vec!["legacy_note".to_string()]);
                assert_eq!(updates.add_columns.len(), 1);
                assert_eq!(updates.add_columns[0].name, "status");
                assert_eq!(updates.add_columns[0].column_type, "text");
                assert_eq!(
                    updates
                        .extensions
                        .as_ref()
                        .and_then(|e| e.get("graph.label")),
                    Some(&"Order".to_string())
                );
                let params = updates.params.as_ref().expect("params present");
                assert_table_params_eq(params, &sample_table_params());
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }

    /// The `AlterKeyspace` `Option`s must distinguish `None` from `Some` — a
    /// `has_*` pointer, not an empty-string sentinel.
    #[tokio::test]
    async fn forwarded_frame_preserves_alter_keyspace_none_options() {
        let updates = KeyspaceUpdates {
            replication: None,
            durable_writes: None,
        };
        let cmd = RaftCommand {
            op: RaftOp::AlterKeyspace {
                name: "app".to_string(),
                updates,
            },
            schema_version: Uuid::new_v4(),
        };

        let body = encode_forwarded_command(&cmd, true).expect("capnp frame encodes");
        let decoded = decode_forwarded_command(&body).expect("frame decodes");
        match decoded.op {
            RaftOp::AlterKeyspace { updates, .. } => {
                assert!(
                    updates.replication.is_none(),
                    "None replication must stay None"
                );
                assert!(
                    updates.durable_writes.is_none(),
                    "None durable_writes must stay None"
                );
            }
            other => panic!("expected AlterKeyspace, got {other:?}"),
        }
    }

    /// Mixed-version dual-read: a peer on the previous build sends the shipped
    /// bincode body, and the new handler must still apply it.
    ///
    /// Regression guard, not a red: the pre-change handler already decoded
    /// bincode, so this was green before the frame landed. It exists to keep it
    /// green — a frame-only handler would silently break every older peer.
    #[tokio::test]
    async fn bincode_body_still_decodes_through_the_forward_handler() {
        let table = sample_table();
        let cmd = RaftCommand {
            op: RaftOp::CreateTable(Box::new(table.clone())),
            schema_version: Uuid::new_v4(),
        };
        let body = encode_forwarded_command(&cmd, false).expect("bincode body encodes");
        assert_ne!(
            u32::from_be_bytes([body[0], body[1], body[2], body[3]]),
            FORWARD_RAFT_FRAME_MAGIC,
            "a bincode body must not carry the frame magic"
        );

        let proposed = propose_through_handler(&body).await;
        match proposed.op {
            RaftOp::CreateTable(decoded) => assert_table_metadata_eq(&decoded, &table),
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    /// Fail loud: a corrupt frame that carries the magic is refused by name, and
    /// an unknown frame version is refused as well. Neither falls back to bincode
    /// and neither proposes a half-parsed command.
    ///
    /// The arms that refuse: `ForwardFrameError::Malformed` for the corrupt
    /// payload, `ForwardFrameError::UnsupportedVersion` for the wrong prefix
    /// version. Both are surfaced as an ack `Err`.
    #[tokio::test]
    async fn unreadable_forward_frame_is_refused_by_name() {
        let cmd = RaftCommand {
            op: RaftOp::CreateKeyspace(sample_keyspace()),
            schema_version: Uuid::new_v4(),
        };
        let mut corrupt = hand_built_create_keyspace_frame(&cmd);
        // Carry the magic, then truncate the Cap'n Proto message so it cannot parse.
        corrupt.truncate(9);

        let proposed: Arc<Mutex<Option<RaftCommand>>> = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&proposed);
        let ack = process_forwarded_command(&corrupt, |received: RaftCommand| {
            let sink = Arc::clone(&sink);
            async move {
                *sink.lock().unwrap() = Some(received);
                Ok::<(), String>(())
            }
        })
        .await;
        assert!(
            proposed.lock().unwrap().is_none(),
            "a corrupt frame must not be proposed"
        );
        match bincode::deserialize::<ForwardAckBody>(&ack).expect("ack is bincode") {
            ForwardAckBody::Err(message) => assert!(
                message.contains("forwarded raft frame"),
                "the refusal must name the frame, got: {message}"
            ),
            ForwardAckBody::Ok => panic!("a corrupt frame must not be accepted"),
        }

        // An unknown frame VERSION is refused by name too, and is not retried as
        // bincode even though the payload bytes after the prefix are irrelevant.
        let mut wrong_version = hand_built_create_keyspace_frame(&cmd).to_vec();
        wrong_version[4] = FORWARD_RAFT_FRAME_VERSION + 1;
        let err = decode_forwarded_command(&wrong_version).expect_err("wrong version refused");
        assert_eq!(
            err,
            ForwardFrameError::UnsupportedVersion {
                version: u16::from(FORWARD_RAFT_FRAME_VERSION + 1)
            }
        );
        assert!(err.to_string().contains("forwarded raft frame"));
    }

    /// A body whose bytes are not a frame decodes as bincode (dual-read); only a
    /// body that DOES carry the magic is treated as a frame.
    #[test]
    fn body_without_the_magic_is_decoded_as_bincode() {
        let cmd = sample_command();
        let bincode_body = bincode::serialize(&cmd).expect("bincode encodes");
        let decoded = decode_forwarded_command(&bincode_body).expect("legacy body decodes");
        match decoded.op {
            RaftOp::UpdateNodeInfo(info) => assert_eq!(info.addr, "10.0.0.5:7000"),
            other => panic!("expected UpdateNodeInfo, got {other:?}"),
        }
    }

    /// The capability gate: without `CAP_RAFT_SCHEMA_CAPNP` the sender emits the
    /// shipped bincode body, and the wire magic is absent so the receiver knows.
    #[test]
    fn encode_falls_back_to_bincode_without_the_capability() {
        let cmd = RaftCommand {
            op: RaftOp::CreateKeyspace(sample_keyspace()),
            schema_version: Uuid::new_v4(),
        };
        let without = encode_forwarded_command(&cmd, false).expect("bincode encodes");
        assert_ne!(
            u32::from_be_bytes([without[0], without[1], without[2], without[3]]),
            FORWARD_RAFT_FRAME_MAGIC
        );
        let with = encode_forwarded_command(&cmd, true).expect("capnp encodes");
        assert_eq!(
            u32::from_be_bytes([with[0], with[1], with[2], with[3]]),
            FORWARD_RAFT_FRAME_MAGIC
        );
    }

    /// An op the family does not cover keeps the bincode body even when the peer
    /// advertised the capability, and the frame refuses to carry it.
    #[test]
    fn non_framed_op_stays_bincode_even_with_capnp_allowed() {
        let cmd = RaftCommand {
            op: RaftOp::DropRole("analyst".to_string()),
            schema_version: Uuid::new_v4(),
        };
        let body = encode_forwarded_command(&cmd, true).expect("bincode encodes");
        assert_ne!(
            u32::from_be_bytes([body[0], body[1], body[2], body[3]]),
            FORWARD_RAFT_FRAME_MAGIC,
            "a non-framed op must keep the bincode body"
        );
        let decoded = decode_forwarded_command(&body).expect("bincode decodes");
        match decoded.op {
            RaftOp::DropRole(name) => assert_eq!(name, "analyst"),
            other => panic!("expected DropRole, got {other:?}"),
        }
        assert_eq!(
            crate::raft_forward_codec::encode_frame(&cmd).expect_err("not framed"),
            ForwardFrameError::NotFramed { op: "DropRole" }
        );
    }
}
