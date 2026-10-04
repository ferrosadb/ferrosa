//! Fencing a cluster that an operator dissolved into a pair (t_47bbeb66).
//!
//! The operator downgrade (t_ad872ac7) turns two cluster members A and B into
//! a pair after the other members were taken down. Stopping Raft on A alone
//! did not fence the old cluster: B kept its Raft group, so a downed member C
//! that came back formed a Raft majority with B and committed beside the pair.
//! A restart of A or B did the same, because Raft restarts from its persisted
//! log.
//!
//! The fence has three parts, all fail-loud:
//!
//! 1. **Two-phase dissolution.** A asks B ([`PairDissolveRequest`]) to record
//!    the dissolution and stop its Raft group. A changes nothing unless B
//!    confirms; then A does the same itself.
//! 2. **A durable marker** ([`PairDissolution`], `dissolved-into-pair.json` in
//!    the Raft directory, written atomically and fsynced) on both A and B. A
//!    node holding it never starts Raft again, starts `Standalone` rather than
//!    as a returning cluster member, and admits only its pair partner as a
//!    peer: a former member that reconnects is refused and logged, so it can
//!    never pull the pair back into cluster formation.
//! 3. **A majority rule**, checked by the downgrade: the members taken down
//!    must not be a Raft majority of the voter set on their own. With A and B
//!    permanently out of Raft, the old group can then never reach quorum
//!    again, whoever comes back.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use crate::error::{ClusterError, Result};

/// File name of the durable dissolution marker, in the Raft directory.
pub const DISSOLUTION_MARKER: &str = "dissolved-into-pair.json";

/// The durable record that this node's cluster was dissolved into a pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairDissolution {
    /// The two nodes that became the pair (host ids).
    pub pair: [Uuid; 2],
    /// Voters of the dissolved Raft group (openraft node ids).
    pub former_voters: Vec<u64>,
    /// Wall-clock milliseconds when this node recorded it.
    pub dissolved_at_ms: u64,
}

impl PairDissolution {
    /// The pair partner of `local`, if `local` is one of the pair.
    pub fn partner_of(&self, local: Uuid) -> Option<Uuid> {
        match self.pair {
            [a, b] if a == local => Some(b),
            [a, b] if b == local => Some(a),
            _ => None,
        }
    }
}

/// What a node knows about a dissolution of its cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DissolutionState {
    /// No marker: an ordinary node.
    None,
    /// The cluster was dissolved into a pair.
    Dissolved(PairDissolution),
    /// A marker exists but cannot be read. Treated as dissolved with an unknown
    /// partner: no Raft, no peers, until an operator repairs it. Guessing "not
    /// dissolved" from a damaged marker is the failure this exists to prevent.
    Unreadable(String),
}

impl DissolutionState {
    /// Why this node must not start Raft, if it must not.
    pub fn raft_refusal(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::Dissolved(d) => Some(format!(
                "this node's cluster was dissolved into the pair {} + {} (former voters \
                 {:?}); it never runs Raft again. Remove {DISSOLUTION_MARKER} only to \
                 rebuild the cluster deliberately",
                d.pair[0], d.pair[1], d.former_voters
            )),
            Self::Unreadable(e) => Some(format!(
                "{DISSOLUTION_MARKER} exists but cannot be read ({e}); refusing Raft and \
                 every peer until an operator repairs or removes it"
            )),
        }
    }

    /// Whether `peer` may be admitted as a connected peer of `local`.
    pub fn admits_peer(&self, local: Uuid, peer: Uuid) -> bool {
        match self {
            Self::None => true,
            Self::Dissolved(d) => d.partner_of(local) == Some(peer),
            Self::Unreadable(_) => false,
        }
    }
}

/// Path of the marker in `raft_dir`.
pub fn marker_path(raft_dir: &Path) -> PathBuf {
    raft_dir.join(DISSOLUTION_MARKER)
}

/// Read the marker. Absent is `None`; present but unreadable is `Unreadable`.
pub fn read_dissolution(raft_dir: &Path) -> DissolutionState {
    let path = marker_path(raft_dir);
    match std::fs::read(&path) {
        Ok(bytes) => match serde_json::from_slice::<PairDissolution>(&bytes) {
            Ok(d) => DissolutionState::Dissolved(d),
            Err(e) => DissolutionState::Unreadable(format!("{}: {e}", path.display())),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DissolutionState::None,
        Err(e) => DissolutionState::Unreadable(format!("{}: {e}", path.display())),
    }
}

/// Write the marker durably: staged file, fsync, rename, fsync the directory.
pub fn write_dissolution(raft_dir: &Path, dissolution: &PairDissolution) -> Result<()> {
    let io = |what: &str, e: std::io::Error| {
        ClusterError::Internal(format!(
            "pair dissolution marker in {}: {what}: {e}",
            raft_dir.display()
        ))
    };
    std::fs::create_dir_all(raft_dir).map_err(|e| io("create dir", e))?;
    let body = serde_json::to_vec_pretty(dissolution)
        .map_err(|e| ClusterError::Internal(format!("encode pair dissolution marker: {e}")))?;
    let staging = raft_dir.join(format!("{DISSOLUTION_MARKER}.partial"));
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&staging).map_err(|e| io("create", e))?;
        file.write_all(&body).map_err(|e| io("write", e))?;
        file.sync_all().map_err(|e| io("fsync", e))?;
    }
    std::fs::rename(&staging, marker_path(raft_dir)).map_err(|e| io("rename", e))?;
    std::fs::File::open(raft_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| io("fsync dir", e))?;
    Ok(())
}

/// Phase 1 request: `requester` asks `peer` to dissolve into a pair with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairDissolveRequest {
    pub requester: Uuid,
    pub peer: Uuid,
    /// The voter set the requester checked the majority rule against.
    pub former_voters: Vec<u64>,
}

/// Phase 1 reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PairDissolveReply {
    /// The peer recorded the dissolution durably and stopped its Raft group.
    Done,
    /// The peer refused; nothing on it changed unless the reason says so.
    Refused(String),
}

/// How the downgrade reaches the named peer for phase 1.
#[async_trait]
pub trait PairDissolveTransport: Send + Sync {
    /// Ok only when the peer replied [`PairDissolveReply::Done`].
    async fn request(&self, req: PairDissolveRequest) -> Result<()>;
}

/// Production transport over the internode connection.
pub struct NetPairDissolveTransport {
    pub peer_manager: Arc<ferrosa_net::peer::PeerManager>,
}

#[async_trait]
impl PairDissolveTransport for NetPairDissolveTransport {
    async fn request(&self, req: PairDissolveRequest) -> Result<()> {
        let peer = req.peer;
        // An older node does not know the type byte and would drop the whole
        // connection; it cannot dissolve, so the downgrade is refused.
        match self.peer_manager.peer_capabilities(peer).await {
            Some(caps) if caps & ferrosa_net::handshake::CAP_PAIR_DISSOLVE != 0 => {}
            other => {
                return Err(ClusterError::ModeTransitionRejected(format!(
                    "peer {peer} cannot take part in a pair dissolution (capabilities \
                     {other:?}); upgrade it first"
                )))
            }
        }
        let body = bincode::serialize(&req)
            .map_err(|e| ClusterError::Internal(format!("encode PairDissolve: {e}")))?;
        let reply = self
            .peer_manager
            .send(peer, Message::PairDissolve(Bytes::from(body)), Lane::Data)
            .await
            .map_err(ClusterError::Net)?;
        let Message::PairDissolveAck(bytes) = reply else {
            return Err(ClusterError::Internal(format!(
                "PairDissolve to {peer}: unexpected reply {:?}",
                reply.msg_type()
            )));
        };
        match bincode::deserialize::<PairDissolveReply>(&bytes) {
            Ok(PairDissolveReply::Done) => Ok(()),
            Ok(PairDissolveReply::Refused(reason)) => Err(ClusterError::ModeTransitionRejected(
                format!("peer {peer} refused the pair dissolution: {reason}"),
            )),
            Err(e) => Err(ClusterError::Internal(format!(
                "PairDissolve to {peer}: undecodable reply: {e}"
            ))),
        }
    }
}

/// Serves phase 1 on the named peer.
pub struct PairDissolveHandler {
    pub controller: std::sync::Weak<super::ModeController>,
}

#[async_trait]
impl RpcHandler for PairDissolveHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let Message::PairDissolve(bytes) = msg else {
            return None;
        };
        let reply = match (
            bincode::deserialize::<PairDissolveRequest>(&bytes),
            self.controller.upgrade(),
        ) {
            (Err(e), _) => PairDissolveReply::Refused(format!("undecodable request: {e}")),
            (_, None) => PairDissolveReply::Refused("node is shutting down".into()),
            (Ok(req), Some(controller)) => match controller.accept_pair_dissolution(req).await {
                Ok(()) => PairDissolveReply::Done,
                Err(e) => PairDissolveReply::Refused(e.to_string()),
            },
        };
        match bincode::serialize(&reply) {
            Ok(body) => Some(Message::PairDissolveAck(Bytes::from(body))),
            Err(e) => {
                tracing::error!("PairDissolveAck: failed to encode reply: {e}");
                None
            }
        }
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dissolution() -> PairDissolution {
        PairDissolution {
            pair: [Uuid::from_u128(1), Uuid::from_u128(2)],
            former_voters: vec![1, 2, 3],
            dissolved_at_ms: 7,
        }
    }

    #[test]
    fn marker_round_trips_and_absent_is_none() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_dissolution(dir.path()), DissolutionState::None);
        write_dissolution(dir.path(), &dissolution()).unwrap();
        assert_eq!(
            read_dissolution(dir.path()),
            DissolutionState::Dissolved(dissolution())
        );
    }

    /// A damaged marker must not read as "not dissolved".
    #[test]
    fn a_corrupt_marker_is_unreadable_not_absent() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(marker_path(dir.path()), b"{not json").unwrap();
        let state = read_dissolution(dir.path());
        assert!(
            matches!(state, DissolutionState::Unreadable(_)),
            "{state:?}"
        );
        assert!(state.raft_refusal().is_some());
        assert!(!state.admits_peer(Uuid::from_u128(1), Uuid::from_u128(2)));
    }

    #[test]
    fn a_dissolved_node_admits_only_its_partner() {
        let state = DissolutionState::Dissolved(dissolution());
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        assert!(state.admits_peer(a, b));
        assert!(state.admits_peer(b, a));
        assert!(!state.admits_peer(a, Uuid::from_u128(3)));
        assert!(state.raft_refusal().is_some());
        assert!(DissolutionState::None.raft_refusal().is_none());
    }
}
