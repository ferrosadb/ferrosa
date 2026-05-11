//! Raft network adapter wrapping ferrosa-net's [`PeerManager`].
//!
//! [`FerrosRaftNetworkFactory`] implements openraft's `RaftNetworkFactory` and
//! creates [`FerrosRaftNetwork`] instances, one per target node.  Each
//! `FerrosRaftNetwork` serialises openraft RPCs with bincode, wraps them in the
//! appropriate [`Message`] variant, and sends them over [`Lane::Raft`] through
//! the `PeerManager`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::error::NetError;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;

use super::FerrosRaftConfig;

/// Delay before handing a transient reconnecting lane back to OpenRaft as
/// `Unreachable`.
///
/// OpenRaft's replication core enters `backoff_drain_events` after an
/// `Unreachable` error. Returning Ferrosa's local `NetError::Reconnecting`
/// immediately can turn a transient lane reconnect into a hot event-drain loop
/// on one runtime worker. Sleeping here makes the Raft network future itself
/// carry the reconnect backoff instead of fast-failing into OpenRaft.
const RAFT_RECONNECTING_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(500);

// ---------------------------------------------------------------------------
// FerrosRaftNetworkFactory
// ---------------------------------------------------------------------------

/// Factory that creates per-node [`FerrosRaftNetwork`] instances.
///
/// Maintains a mapping from openraft `u64` node IDs to ferrosa [`Uuid`] host
/// IDs so that the underlying [`PeerManager`] (which works with `Uuid`) can be
/// driven by openraft (which works with `u64`).
pub struct FerrosRaftNetworkFactory {
    peer_manager: Arc<PeerManager>,
    /// Maps openraft u64 node IDs to ferrosa Uuid host IDs.
    ///
    /// Uses `std::sync::RwLock` rather than `tokio::sync::RwLock` because the
    /// critical section is a trivial `HashMap::insert` — no async work is done
    /// under the lock.  This allows `register_node` to be called from both sync
    /// and async contexts without `block_on` / `block_in_place` gymnastics.
    node_map: Arc<RwLock<HashMap<u64, Uuid>>>,
}

impl FerrosRaftNetworkFactory {
    /// Create a new factory backed by `peer_manager`.
    pub fn new(peer_manager: Arc<PeerManager>) -> Self {
        Self {
            peer_manager,
            node_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a node ID -> host ID mapping.
    ///
    /// Must be called before openraft asks the factory to create a network
    /// client for a given node ID.  Safe to call from both sync and async
    /// contexts.
    pub fn register_node(&self, node_id: u64, host_id: Uuid) {
        self.node_map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(node_id, host_id);
    }

    /// Async version of `register_node` — kept for API compatibility.
    pub async fn register_node_async(&self, node_id: u64, host_id: Uuid) {
        self.register_node(node_id, host_id);
    }

    /// Return a clone of the `Arc` pointing to the node_id → host_id mapping.
    ///
    /// Callers such as [`crate::ddl_path::DdlPath::Cluster`] can hold this
    /// reference to resolve a Raft leader `u64` NodeId to the `Uuid` needed by
    /// [`ferrosa_net::peer::PeerManager`] without duplicating the map.
    pub fn node_map(&self) -> Arc<std::sync::RwLock<std::collections::HashMap<u64, Uuid>>> {
        Arc::clone(&self.node_map)
    }

    /// Look up the host UUID for a given openraft node ID.
    fn resolve_host_id(&self, node_id: u64) -> Option<Uuid> {
        self.node_map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&node_id)
            .copied()
    }
}

impl RaftNetworkFactory<FerrosRaftConfig> for FerrosRaftNetworkFactory {
    type Network = FerrosRaftNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> FerrosRaftNetwork {
        // Resolve the UUID from the sync map.
        let target_host_id = self.resolve_host_id(target).unwrap_or_else(|| {
            // openraft does not allow returning an error here.  Return a
            // nil UUID; the first RPC will fail with Unreachable which will
            // trigger openraft's backoff logic.
            tracing::error!(node_id = target, "no host_id registered for Raft node");
            Uuid::nil()
        });

        FerrosRaftNetwork {
            peer_manager: Arc::clone(&self.peer_manager),
            target_host_id,
        }
    }
}

// ---------------------------------------------------------------------------
// FerrosRaftNetwork
// ---------------------------------------------------------------------------

/// A single-target network connection for Raft RPCs.
///
/// All methods serialise the request with bincode, forward it via
/// [`PeerManager::send`] on [`Lane::Raft`], and deserialise the response.
pub struct FerrosRaftNetwork {
    peer_manager: Arc<PeerManager>,
    target_host_id: Uuid,
}

/// Serialise `value` with bincode, wrapping any error as an openraft
/// [`RPCError`] (specifically a [`NetworkError`]).
///
/// openraft's error types are large by design (dictated by the crate).
#[allow(clippy::result_large_err)]
fn encode<T: serde::Serialize>(
    value: &T,
) -> Result<Bytes, RPCError<u64, BasicNode, RaftError<u64>>> {
    bincode::serialize(value)
        .map(Bytes::from)
        .map_err(|e| RPCError::Network(NetworkError::new(&*e)))
}

/// Deserialise `bytes` with bincode into `T`, mapping errors to openraft's
/// [`NetworkError`] (the response was received but was malformed).
///
/// openraft's error types are large by design (dictated by the crate).
#[allow(clippy::result_large_err)]
fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, RPCError<u64, BasicNode, RaftError<u64>>> {
    bincode::deserialize(bytes).map_err(|e| RPCError::Network(NetworkError::new(&*e)))
}

/// Deserialise `bytes` with bincode into `T` for InstallSnapshot error variant.
///
/// openraft's error types are large by design (dictated by the crate).
#[allow(clippy::result_large_err)]
fn decode_snapshot<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>> {
    bincode::deserialize(bytes).map_err(|e| RPCError::Network(NetworkError::new(&*e)))
}

/// Map a [`ferrosa_net::error::NetError`] to openraft's [`Unreachable`] so
/// that openraft backs off before retrying the node.
fn net_error_to_unreachable(
    e: ferrosa_net::error::NetError,
) -> RPCError<u64, BasicNode, RaftError<u64>> {
    RPCError::Unreachable(Unreachable::new(&e))
}

/// Same mapping for the InstallSnapshot error variant.
fn net_error_to_unreachable_snapshot(
    e: ferrosa_net::error::NetError,
) -> RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>> {
    RPCError::Unreachable(Unreachable::new(&e))
}

async fn backoff_transient_raft_reconnect(e: &NetError) {
    if matches!(e, NetError::Reconnecting) {
        tokio::time::sleep(RAFT_RECONNECTING_ERROR_BACKOFF).await;
    }
}

impl RaftNetwork<FerrosRaftConfig> for FerrosRaftNetwork {
    /// Forward an `AppendEntries` RPC to the target node.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<FerrosRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = encode(&rpc)?;
        let response = match self
            .peer_manager
            .send(
                self.target_host_id,
                Message::RaftAppendEntries(payload),
                Lane::Raft,
            )
            .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!(target = %self.target_host_id, %e, "AppendEntries send failed");
                backoff_transient_raft_reconnect(&e).await;
                return Err(net_error_to_unreachable(e));
            }
        };

        match response {
            Message::RaftAppendResponse(bytes) => decode(&bytes),
            other => {
                tracing::error!(target = %self.target_host_id, msg_type = ?other.msg_type(), "AppendEntries got unexpected response type");
                Err(RPCError::Network(NetworkError::new(&UnexpectedResponse(
                    format!("expected RaftAppendResponse, got {:?}", other.msg_type()),
                ))))
            }
        }
    }

    /// Forward an `InstallSnapshot` RPC chunk to the target node.
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<FerrosRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let payload = bincode::serialize(&rpc)
            .map(Bytes::from)
            .map_err(|e| RPCError::Network(NetworkError::new(&*e)))?;

        // W4.13 (Sprint 4): InstallSnapshot rides on the Bulk lane,
        // not the Raft lane.  Sending a 100 MB snapshot down Lane::Raft
        // pushed AppendEntries p99 from ~5 ms to ~600 ms, well over
        // election_timeout_min, causing avoidable elections.
        // See `crate::raft::snapshot_transport::snapshot_lane`.
        let response = match self
            .peer_manager
            .send(
                self.target_host_id,
                Message::RaftInstallSnapshot(payload),
                crate::raft::snapshot_transport::snapshot_lane(),
            )
            .await
        {
            Ok(response) => response,
            Err(e) => {
                backoff_transient_raft_reconnect(&e).await;
                return Err(net_error_to_unreachable_snapshot(e));
            }
        };

        match response {
            // The install-snapshot response is encoded the same way: bincode of
            // `InstallSnapshotResponse`.  We reuse the same message type for the
            // response body because the codec is symmetric.
            Message::RaftAppendResponse(bytes) => decode_snapshot(&bytes),
            other => Err(RPCError::Network(NetworkError::new(&UnexpectedResponse(
                format!(
                    "expected RaftAppendResponse (snapshot ack), got {:?}",
                    other.msg_type()
                ),
            )))),
        }
    }

    /// Forward a `Vote` RPC to the target node.
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = encode(&rpc)?;
        let response = match self
            .peer_manager
            .send(self.target_host_id, Message::RaftVote(payload), Lane::Raft)
            .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::debug!(target = %self.target_host_id, %e, "Vote send failed");
                backoff_transient_raft_reconnect(&e).await;
                return Err(net_error_to_unreachable(e));
            }
        };

        match response {
            Message::RaftVoteResponse(bytes) => decode(&bytes),
            other => {
                tracing::error!(target = %self.target_host_id, msg_type = ?other.msg_type(), "Vote got unexpected response type");
                Err(RPCError::Network(NetworkError::new(&UnexpectedResponse(
                    format!("expected RaftVoteResponse, got {:?}", other.msg_type()),
                ))))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: UnexpectedResponse
// ---------------------------------------------------------------------------

/// A local error type for unexpected response message variants.
#[derive(Debug)]
struct UnexpectedResponse(String);

impl std::fmt::Display for UnexpectedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unexpected response: {}", self.0)
    }
}

impl std::error::Error for UnexpectedResponse {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    use ferrosa_net::config::NetConfig;
    use ferrosa_net::peer::PeerEventListener;
    use ferrosa_net::rpc::handler::PeerId;

    // Minimal no-op listener for PeerManager construction.
    struct NoopListener;
    impl PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _: PeerId) {}
        fn on_peer_disconnected(&self, _: PeerId) {}
        fn on_peer_suspected(&self, _: PeerId) {}
        fn on_peer_recovered(&self, _: uuid::Uuid) {}
        fn on_peer_failed(&self, _: uuid::Uuid) {}
    }

    fn make_peer_manager() -> Arc<PeerManager> {
        let config = Arc::new(NetConfig::default());
        Arc::new(PeerManager::new(
            config,
            Uuid::new_v4(),
            Arc::new(NoopListener),
        ))
    }

    // -- reconnecting errors are not fast-failed into OpenRaft -------------

    #[tokio::test]
    async fn transient_reconnecting_error_is_backed_off_before_openraft_error() {
        let start = tokio::time::Instant::now();
        backoff_transient_raft_reconnect(&ferrosa_net::error::NetError::Reconnecting).await;
        assert!(
            start.elapsed() >= RAFT_RECONNECTING_ERROR_BACKOFF,
            "Reconnecting lanes must be delayed before returning Unreachable to OpenRaft"
        );
    }

    // -- factory_creates_network -------------------------------------------

    #[tokio::test]
    async fn factory_creates_network() {
        let pm = make_peer_manager();
        let mut factory = FerrosRaftNetworkFactory::new(Arc::clone(&pm));

        let host_id = Uuid::new_v4();
        let node_id: u64 = 1;
        factory.register_node_async(node_id, host_id).await;

        let node = BasicNode {
            addr: "127.0.0.1:7001".to_string(),
        };
        let network = factory.new_client(node_id, &node).await;

        assert_eq!(network.target_host_id, host_id);
    }

    // -- factory_unknown_node_yields_nil -----------------------------------

    #[tokio::test]
    async fn factory_unknown_node_yields_nil() {
        let pm = make_peer_manager();
        let mut factory = FerrosRaftNetworkFactory::new(pm);

        // No registration — factory should log an error and return a nil UUID.
        let node = BasicNode {
            addr: "127.0.0.1:7001".to_string(),
        };
        let network = factory.new_client(999, &node).await;
        assert_eq!(network.target_host_id, Uuid::nil());
    }

    // -- node_map_registration ---------------------------------------------

    #[tokio::test]
    async fn node_map_registration() {
        let pm = make_peer_manager();
        let factory = FerrosRaftNetworkFactory::new(pm);

        let ids: Vec<(u64, Uuid)> = (0..5).map(|i| (i, Uuid::new_v4())).collect();

        for (node_id, host_id) in &ids {
            factory.register_node_async(*node_id, *host_id).await;
        }

        // Verify all mappings are present and correct.
        let map = factory.node_map.read().expect("lock");
        for (node_id, host_id) in &ids {
            assert_eq!(
                map.get(node_id),
                Some(host_id),
                "node_id {node_id} should map to {host_id}"
            );
        }
        assert_eq!(map.len(), 5);
    }

    // -- node_map_overwrite ------------------------------------------------

    #[tokio::test]
    async fn node_map_overwrite() {
        let pm = make_peer_manager();
        let factory = FerrosRaftNetworkFactory::new(pm);

        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        factory.register_node_async(1, first).await;
        factory.register_node_async(1, second).await;

        let map = factory.node_map.read().expect("lock");
        assert_eq!(
            map.get(&1),
            Some(&second),
            "second registration should overwrite the first"
        );
    }

    // -- encode_decode_roundtrip -------------------------------------------

    #[test]
    fn encode_decode_vote_request_roundtrip() {
        use openraft::{CommittedLeaderId, LogId, Vote};

        let req = VoteRequest {
            vote: Vote::new(1, 42),
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 0), 5)),
        };

        let bytes = bincode::serialize(&req).expect("serialize");
        let decoded: VoteRequest<u64> = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(decoded.vote, req.vote);
        assert_eq!(decoded.last_log_id, req.last_log_id);
    }

    // -- addr parsing sanity -----------------------------------------------

    #[test]
    fn socket_addr_parse() {
        // BasicNode stores addr as a String; verify our test addresses parse.
        let addr = "127.0.0.1:7001";
        assert!(SocketAddr::from_str(addr).is_ok());
    }
}
