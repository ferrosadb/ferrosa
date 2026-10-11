//! Outage 2026-10-10 (F2, decision D2): a Raft RPC handler that cannot answer
//! must say so.
//!
//! The append, vote, pre-vote and install-snapshot handlers returned `None`
//! (no reply) when the request failed to decode, when Raft was not initialised
//! yet, or when openraft returned an error. The sender then held its stream
//! slot until its own deadline and retried blind. During the outage that was
//! the install-snapshot path: every refused chunk cost the leader a full
//! timeout before it tried again.
//!
//! These tests run the real handlers behind a real `RpcServer` over loopback
//! TCP and assert what the requesting `RpcClient` receives.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, CommittedLeaderId, LogId, RaftNetwork, RaftNetworkFactory, SnapshotMeta, Vote};

use ferrosa_cluster::raft::handlers::{
    LazyRaft, RaftAppendHandler, RaftPreVoteHandler, RaftSnapshotHandler, RaftVoteHandler,
};
use ferrosa_cluster::raft::log_store::SledLogStore;
use ferrosa_cluster::raft::state_machine::FerrosStateMachine;
use ferrosa_cluster::raft::{FerrosRaft, FerrosRaftConfig};
use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::config::NetConfig;
use ferrosa_net::error::NetError;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::client::RpcClient;
use ferrosa_net::rpc::error_reply::RemoteFailureKind;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_net::rpc::{HandlerRegistry, RpcHandler};

/// How long the requester waits. A handler that answers nothing makes the
/// request fail here with `Timeout`; an error reply arrives in milliseconds.
const REQUEST_DEADLINE: Duration = Duration::from_secs(3);

// ---------------------------------------------------------------------------
// A Raft node with no peers: every outbound RPC is Unreachable.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NoPeers;
impl std::fmt::Display for NoPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("test node has no peers")
    }
}
impl std::error::Error for NoPeers {}

struct NoPeersNetwork;

impl RaftNetworkFactory<FerrosRaftConfig> for NoPeersNetwork {
    type Network = NoPeersNetwork;
    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        NoPeersNetwork
    }
}

impl RaftNetwork<FerrosRaftConfig> for NoPeersNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<FerrosRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&NoPeers)))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<FerrosRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(&NoPeers)))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&NoPeers)))
    }
}

/// An uninitialised single Raft node, published through a `LazyRaft`.
async fn ready_lazy_raft(dir: &tempfile::TempDir) -> (LazyRaft, Arc<FerrosRaft>) {
    let config = Arc::new(
        openraft::Config {
            cluster_name: "handler-error-replies".to_string(),
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );
    let log_store = SledLogStore::new(&dir.path().join("raft")).expect("sled log store");
    let raft = FerrosRaft::new(1, config, NoPeersNetwork, log_store, FerrosStateMachine::new())
        .await
        .expect("FerrosRaft::new");
    let raft = Arc::new(raft);
    let (tx, lazy) = LazyRaft::channel();
    tx.send(Some(Arc::clone(&raft))).expect("publish raft");
    // Keep the sender alive for the test's lifetime: a dropped sender would
    // make `LazyRaft::get` report "not ready".
    std::mem::forget(tx);
    (lazy, raft)
}

/// A `LazyRaft` whose publisher is gone: Raft never becomes ready.
fn never_ready_lazy_raft() -> LazyRaft {
    let (tx, lazy) = LazyRaft::channel();
    drop(tx);
    lazy
}

fn handler_for(msg_type: MsgType, lazy: LazyRaft) -> Arc<dyn RpcHandler> {
    match msg_type {
        MsgType::RaftAppendEntries => Arc::new(RaftAppendHandler::new(lazy)),
        MsgType::RaftVote => Arc::new(RaftVoteHandler::new(lazy)),
        MsgType::RaftPreVote => Arc::new(RaftPreVoteHandler::new(lazy)),
        MsgType::RaftInstallSnapshot => Arc::new(RaftSnapshotHandler::new(lazy)),
        other => panic!("no Raft handler for {other:?}"),
    }
}

fn request(msg_type: MsgType, body: Bytes) -> Message {
    match msg_type {
        MsgType::RaftAppendEntries => Message::RaftAppendEntries(body),
        MsgType::RaftVote => Message::RaftVote(body),
        MsgType::RaftPreVote => Message::RaftPreVote(body),
        MsgType::RaftInstallSnapshot => Message::RaftInstallSnapshot(body),
        other => panic!("not a Raft request type: {other:?}"),
    }
}

/// Serve `handler` for `msg_type` on loopback and send it `msg`.
async fn send_to_handler(
    msg_type: MsgType,
    handler: Arc<dyn RpcHandler>,
    msg: Message,
) -> (Result<Message, NetError>, Duration) {
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    };
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(msg_type, handler);
    let server = Arc::new(RpcServer::new(config, uuid::Uuid::new_v4(), registry));
    let addr = server.start_and_get_addr().await.expect("bind rpc server");
    let client = RpcClient::connect(Arc::new(NetConfig::default()), uuid::Uuid::new_v4(), addr)
        .await
        .expect("connect");
    let started = Instant::now();
    let result = client
        .send_with_timeout(msg, Lane::Raft, REQUEST_DEADLINE)
        .await;
    (result, started.elapsed())
}

fn expect_remote_failure(
    result: &Result<Message, NetError>,
    elapsed: Duration,
    want_type: MsgType,
    want_kind: RemoteFailureKind,
) -> String {
    match result {
        Err(NetError::RemoteHandlerFailed {
            msg_type,
            kind,
            detail,
        }) if *msg_type == want_type && *kind == want_kind => {
            assert!(
                elapsed < REQUEST_DEADLINE / 2,
                "the error reply took {elapsed:?}; it must not wait out the deadline"
            );
            detail.clone()
        }
        other => panic!(
            "{want_type:?}: expected an error reply RemoteHandlerFailed({want_kind:?}) after a \
             failed request, got {other:?} after {elapsed:?}"
        ),
    }
}

const RAFT_REQUESTS: [MsgType; 4] = [
    MsgType::RaftInstallSnapshot,
    MsgType::RaftAppendEntries,
    MsgType::RaftVote,
    MsgType::RaftPreVote,
];

/// T-F2-3, plus the matching append, vote and pre-vote cases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_handler_decode_failure_sends_error_reply() {
    let dir = tempfile::tempdir().unwrap();
    let (lazy, _raft) = ready_lazy_raft(&dir).await;
    for msg_type in RAFT_REQUESTS {
        let malformed = Bytes::from_static(&[0xff, 0x01]);
        let (result, elapsed) = send_to_handler(
            msg_type,
            handler_for(msg_type, lazy.clone()),
            request(msg_type, malformed),
        )
        .await;
        let detail = expect_remote_failure(
            &result,
            elapsed,
            msg_type,
            RemoteFailureKind::RequestDecodeFailed,
        );
        assert!(
            detail.contains("deserialize"),
            "{msg_type:?}: detail must name the decode failure: {detail}"
        );
    }
}

/// T-F2-4. openraft refuses the chunk (an offset into a snapshot this node
/// never began receiving), and the sender is told why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_handler_install_error_sends_error_reply() {
    let dir = tempfile::tempdir().unwrap();
    let (lazy, _raft) = ready_lazy_raft(&dir).await;
    let req: InstallSnapshotRequest<FerrosRaftConfig> = InstallSnapshotRequest {
        vote: Vote::new_committed(5, 2),
        meta: SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(5, 2), 40)),
            last_membership: Default::default(),
            snapshot_id: "never-begun".to_string(),
        },
        offset: 4096,
        data: vec![1, 2, 3],
        done: false,
    };
    let body = Bytes::from(bincode::serialize(&req).unwrap());
    let (result, elapsed) = send_to_handler(
        MsgType::RaftInstallSnapshot,
        handler_for(MsgType::RaftInstallSnapshot, lazy),
        Message::RaftInstallSnapshot(body),
    )
    .await;
    let detail = expect_remote_failure(
        &result,
        elapsed,
        MsgType::RaftInstallSnapshot,
        RemoteFailureKind::HandlerFailed,
    );
    assert!(
        detail.contains("install_snapshot"),
        "detail must name the failed operation: {detail}"
    );
}

/// T-F2-5, plus the matching append, vote and pre-vote cases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_handler_raft_not_ready_sends_error_reply() {
    for msg_type in RAFT_REQUESTS {
        let (result, elapsed) = send_to_handler(
            msg_type,
            handler_for(msg_type, never_ready_lazy_raft()),
            request(msg_type, Bytes::from_static(b"unread")),
        )
        .await;
        let detail = expect_remote_failure(&result, elapsed, msg_type, RemoteFailureKind::NotReady);
        assert!(
            detail.contains("not ready"),
            "{msg_type:?}: detail must say Raft is not ready: {detail}"
        );
    }
}
