//! Serve, and fetch, the committed snapshot a quarantined node repairs from.
//!
//! A node whose Raft group is quarantined runs no Raft, so the leader's
//! ordinary replication cannot reach it. It pulls instead: it asks a healthy
//! peer of its own group for a snapshot whose last log id reaches a minimum
//! index, verifies what comes back, and installs it (see
//! `ModeController::drive_raft_repair`).
//!
//! The server answers only from state it is entitled to serve. It refuses when
//! its own admission is closed, because a quarantined node's snapshot is
//! exactly the unhonoured state the repair is escaping. Its snapshot is always
//! a committed prefix: a state machine only applies committed entries. When the
//! current snapshot is too old it asks its Raft to build one and waits a
//! bounded time for it.
//!
//! Wire: `RaftRepairSnapshotRequest` / `RaftRepairSnapshotResponse`, bincode
//! bodies, sent only to a peer that advertised `CAP_RAFT_REPAIR_SNAPSHOT`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ferrosa_net::message::Message;
use ferrosa_net::rpc::handler::{HandlerOutcome, PeerId, RpcHandler};
use openraft::{BasicNode, SnapshotMeta};
use serde::{Deserialize, Serialize};

use super::handlers::{unexpected_message, LazyRaft};
use super::quarantine::RecoveryAdmission;
use super::state_machine::SharedSnapshotSlot;
use crate::config::cluster_config_wire::deserialize_bounded;

/// How long a server waits for its Raft to build a covering snapshot.
pub(crate) const DEFAULT_BUILD_WAIT: Duration = Duration::from_secs(30);

/// A quarantined node's request: a snapshot of `group` through at least
/// `minimum_index`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RepairSnapshotRequest {
    pub(crate) group: String,
    pub(crate) minimum_index: u64,
}

/// The server's answer.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RepairSnapshotResponse {
    Snapshot {
        meta: SnapshotMeta<u64, BasicNode>,
        bytes: Vec<u8>,
    },
    Refused {
        reason: String,
    },
}

impl RepairSnapshotRequest {
    pub(crate) fn encode(&self) -> Result<Message, String> {
        bincode::serialize(self)
            .map(|body| Message::RaftRepairSnapshotRequest(Bytes::from(body)))
            .map_err(|error| format!("encode repair snapshot request: {error}"))
    }

    pub(crate) fn decode(body: &[u8]) -> Result<Self, String> {
        deserialize_bounded(body)
            .map_err(|error| format!("decode repair snapshot request: {error}"))
    }
}

impl RepairSnapshotResponse {
    pub(crate) fn encode(&self) -> Result<Message, String> {
        bincode::serialize(self)
            .map(|body| Message::RaftRepairSnapshotResponse(Bytes::from(body)))
            .map_err(|error| format!("encode repair snapshot response: {error}"))
    }

    pub(crate) fn decode(message: Message) -> Result<Self, String> {
        match message {
            Message::RaftRepairSnapshotResponse(body) => deserialize_bounded(&body)
                .map_err(|error| format!("decode repair snapshot response: {error}")),
            other => Err(format!(
                "expected RaftRepairSnapshotResponse, got {:?}",
                other.msg_type()
            )),
        }
    }

    fn refused(reason: impl Into<String>) -> Self {
        Self::Refused {
            reason: reason.into(),
        }
    }
}

/// Serves `RaftRepairSnapshotRequest` on a node whose group is running.
pub(crate) struct RaftRepairSnapshotHandler {
    group: String,
    slot: SharedSnapshotSlot,
    admission: Arc<RecoveryAdmission>,
    raft: LazyRaft,
    build_wait: Duration,
}

impl RaftRepairSnapshotHandler {
    pub(crate) fn new(
        group: String,
        slot: SharedSnapshotSlot,
        admission: Arc<RecoveryAdmission>,
        raft: LazyRaft,
        build_wait: Duration,
    ) -> Self {
        Self {
            group,
            slot,
            admission,
            raft,
            build_wait,
        }
    }

    /// The current snapshot, if it reaches `minimum`.
    fn covering(&self, minimum: u64) -> Option<RepairSnapshotResponse> {
        let slot = self.slot.read();
        let (meta, bytes) = slot.as_ref()?;
        let index = meta.last_log_id?.index;
        (index >= minimum).then(|| RepairSnapshotResponse::Snapshot {
            meta: meta.clone(),
            bytes: bytes.clone(),
        })
    }

    pub(crate) async fn answer(&self, request: &RepairSnapshotRequest) -> RepairSnapshotResponse {
        if request.group != self.group {
            return RepairSnapshotResponse::refused(format!(
                "this node serves group {}, not {}",
                self.group, request.group
            ));
        }
        if let Err(refusal) = self.admission.admits() {
            return RepairSnapshotResponse::refused(format!(
                "this node cannot serve a repair while its own admission is closed: {refusal}"
            ));
        }
        let minimum = request.minimum_index;
        if let Some(snapshot) = self.covering(minimum) {
            return snapshot;
        }
        let Some(raft) = self.raft.current() else {
            return RepairSnapshotResponse::refused("Raft is not running on this node");
        };
        if let Err(error) = raft.trigger().snapshot().await {
            return RepairSnapshotResponse::refused(format!("could not start a snapshot: {error}"));
        }
        let waited = raft
            .wait(Some(self.build_wait))
            .metrics(
                |metrics| {
                    metrics
                        .snapshot
                        .is_some_and(|snapshot| snapshot.index >= minimum)
                },
                format!("a snapshot through {minimum} for a repairing peer"),
            )
            .await;
        if let Some(snapshot) = self.covering(minimum) {
            return snapshot;
        }
        RepairSnapshotResponse::refused(format!(
            "no snapshot reached index {minimum} within {:?}: {}",
            self.build_wait,
            waited
                .err()
                .map_or_else(|| "the slot was not updated".into(), |e| e.to_string())
        ))
    }
}

#[async_trait]
impl RpcHandler for RaftRepairSnapshotHandler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        self.handle_outcome(from, msg).await.into_reply()
    }

    async fn handle_outcome(&self, from: PeerId, msg: Message) -> HandlerOutcome {
        let body = match msg {
            Message::RaftRepairSnapshotRequest(body) => body,
            other => return unexpected_message("RaftRepairSnapshotHandler", &other),
        };
        let response = match RepairSnapshotRequest::decode(&body) {
            Ok(request) => {
                let response = self.answer(&request).await;
                match &response {
                    RepairSnapshotResponse::Snapshot { meta, .. } => tracing::info!(
                        peer = %from.0,
                        minimum = request.minimum_index,
                        served = ?meta.last_log_id,
                        "served a repair snapshot"
                    ),
                    RepairSnapshotResponse::Refused { reason } => tracing::warn!(
                        peer = %from.0,
                        minimum = request.minimum_index,
                        %reason,
                        "refused a repair snapshot request"
                    ),
                }
                response
            }
            Err(error) => RepairSnapshotResponse::refused(error),
        };
        match response.encode() {
            Ok(reply) => HandlerOutcome::Reply(reply),
            Err(error) => HandlerOutcome::fail(
                ferrosa_net::rpc::error_reply::RemoteFailureKind::ResponseEncodeFailed,
                error,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, LogId, StoredMembership};

    fn slot_at(index: u64) -> SharedSnapshotSlot {
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(2, 1), index)),
            last_membership: StoredMembership::default(),
            snapshot_id: format!("{index}-test"),
        };
        Arc::new(parking_lot::RwLock::new(Some((meta, vec![1, 2, 3]))))
    }

    fn handler(
        slot: SharedSnapshotSlot,
        admission: Arc<RecoveryAdmission>,
    ) -> RaftRepairSnapshotHandler {
        let (_tx, raft) = LazyRaft::channel();
        RaftRepairSnapshotHandler::new(
            "dc1".into(),
            slot,
            admission,
            raft,
            Duration::from_millis(50),
        )
    }

    fn request(minimum_index: u64) -> RepairSnapshotRequest {
        RepairSnapshotRequest {
            group: "dc1".into(),
            minimum_index,
        }
    }

    #[tokio::test]
    async fn a_healthy_node_serves_a_snapshot_that_reaches_the_minimum() {
        let served = handler(slot_at(9), Arc::new(RecoveryAdmission::new()))
            .answer(&request(9))
            .await;
        match served {
            RepairSnapshotResponse::Snapshot { meta, bytes } => {
                assert_eq!(meta.last_log_id.unwrap().index, 9);
                assert_eq!(bytes, vec![1, 2, 3]);
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_quarantined_node_never_serves_its_own_state() {
        let admission = Arc::new(RecoveryAdmission::new());
        admission.quarantine("local-raft", "interrupted apply");
        let served = handler(slot_at(9), admission).answer(&request(1)).await;
        assert!(
            matches!(&served, RepairSnapshotResponse::Refused { reason } if reason.contains("admission")),
            "{served:?}"
        );
    }

    #[tokio::test]
    async fn another_group_and_a_short_cut_are_refused() {
        let handler = handler(slot_at(4), Arc::new(RecoveryAdmission::new()));
        let other = handler
            .answer(&RepairSnapshotRequest {
                group: "dc2".into(),
                minimum_index: 1,
            })
            .await;
        assert!(matches!(other, RepairSnapshotResponse::Refused { .. }));
        let short = handler.answer(&request(9)).await;
        assert!(
            matches!(&short, RepairSnapshotResponse::Refused { reason } if reason.contains("not running")),
            "{short:?}"
        );
    }

    #[tokio::test]
    async fn the_request_and_response_survive_the_wire() {
        let wire = request(7).encode().unwrap();
        let Message::RaftRepairSnapshotRequest(body) = wire else {
            panic!("wrong message type");
        };
        assert_eq!(RepairSnapshotRequest::decode(&body).unwrap(), request(7));
        let handler = handler(slot_at(7), Arc::new(RecoveryAdmission::new()));
        let reply = handler.handle_outcome(
            (uuid::Uuid::new_v4(), "127.0.0.1:1".parse().unwrap()),
            request(7).encode().unwrap(),
        );
        let HandlerOutcome::Reply(reply) = reply.await else {
            panic!("the handler must reply");
        };
        match RepairSnapshotResponse::decode(reply).unwrap() {
            RepairSnapshotResponse::Snapshot { meta, .. } => {
                assert_eq!(meta.last_log_id.unwrap().index, 7)
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }
}
