//! Verified data movement for membership changes (P0-2, P0-4).
//!
//! A membership change that moves replica ownership commits only after the
//! data it moves has landed and been verified. This module holds the pieces
//! that decide what must move and drive it, separate from the Raft and network
//! handles so each guard can be tested on its own:
//!
//! - [`DecommissionPlan`]: for a leaving node, which nodes become replicas of
//!   each token. It covers EVERY range the leaving node replicates, not only
//!   the ranges it is primary for, and it honours each keyspace's own
//!   replication strategy.
//! - [`stream_decommission_data`]: stream the leaving node's local partitions
//!   to every new owner. Each batch must come back verified (the receiver
//!   checked count and checksum and applied it). Any read error, stream error
//!   or short apply aborts the whole transfer.
//! - [`decommission_verified`]: `Leaving` → stream → `LeaveNode`. On any
//!   failure `LeaveNode` is not proposed and the node stays `Leaving`.

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;

use ferrosa_sstable::types::Partition;
use ferrosa_storage::TableId;

use crate::error::{ClusterError, Result};
use crate::raft::{DataMovementEvidence, NodeState, RaftOp};
use crate::ring::strategy::ReplicationStrategy;
use crate::ring::TokenRing;

/// Partitions sent to one target per verified stream session.
///
/// Bounds the per-target buffer the decommission holds in memory. The
/// receiver also caps a session (50 000 mutations, 128 MiB); a batch of very
/// wide partitions that exceeds the byte cap is REJECTED by the receiver, which
/// aborts the decommission loudly rather than losing data.
pub const DECOMMISSION_BATCH_PARTITIONS: usize = 64;

/// Why a verified data movement did not complete. Every variant aborts the
/// membership change it guards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataMovementError {
    /// The leaving node replicates `token` but no other node would replicate
    /// it after the change: the data would have nowhere to go.
    NoRemainingReplica { table: String, token: i64 },
    /// Reading the leaving node's local partitions failed.
    Read { table: String, error: String },
    /// Streaming a batch to `target` failed or was rejected by the receiver.
    Stream {
        table: String,
        target: u64,
        error: String,
    },
    /// The receiver confirmed fewer partitions than were sent.
    Unverified {
        table: String,
        target: u64,
        sent: u64,
        applied: u64,
    },
}

impl std::fmt::Display for DataMovementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoRemainingReplica { table, token } => write!(
                f,
                "{table}: token {token} would have no replica after the change"
            ),
            Self::Read { table, error } => write!(f, "{table}: local read failed: {error}"),
            Self::Stream {
                table,
                target,
                error,
            } => write!(f, "{table}: stream to node {target} failed: {error}"),
            Self::Unverified {
                table,
                target,
                sent,
                applied,
            } => write!(
                f,
                "{table}: node {target} verified {applied} of {sent} partitions"
            ),
        }
    }
}

impl From<DataMovementError> for ClusterError {
    fn from(e: DataMovementError) -> Self {
        ClusterError::DataMovementUnverified(e.to_string())
    }
}

/// Sends a batch of one table's partitions to one target and returns how many
/// the target verified and applied.
///
/// Production: [`StreamSenderStreamer`]. Tests inject failures.
#[async_trait]
pub trait PartitionStreamer: Send + Sync {
    async fn stream(
        &self,
        target: u64,
        table: &TableId,
        partitions: Vec<Partition>,
    ) -> std::result::Result<u64, String>;
}

/// Proposes a membership op and waits for it to commit.
#[async_trait]
pub trait MembershipProposer: Send + Sync {
    async fn propose(&self, op: RaftOp) -> Result<()>;
}

#[async_trait]
impl MembershipProposer for crate::raft::FerrosRaft {
    async fn propose(&self, op: RaftOp) -> Result<()> {
        self.client_write(crate::raft::RaftCommand {
            op,
            schema_version: uuid::Uuid::new_v4(),
        })
        .await
        .map(|_| ())
        .map_err(|e| ClusterError::RaftError(e.to_string()))
    }
}

/// Replica sets before and after `leaving` departs.
#[derive(Debug, Clone)]
pub struct DecommissionPlan {
    leaving: u64,
    /// The ring with the leaving node counted as a replica (it is `Leaving`
    /// in the live ring, which `replicas()` skips).
    before: TokenRing,
    /// The ring with the leaving node removed.
    after: TokenRing,
}

impl DecommissionPlan {
    pub fn new(ring: &TokenRing, leaving: u64) -> Self {
        let mut before = ring.clone();
        before.set_node_state(leaving, NodeState::Normal);
        let mut after = ring.clone();
        after.remove_node(leaving);
        Self {
            leaving,
            before,
            after,
        }
    }

    /// Nodes that must receive the leaving node's copy of `token`: the
    /// replicas after the change that were not replicas before. Empty when the
    /// leaving node does not replicate `token`.
    pub fn targets(
        &self,
        table: &TableId,
        token: i64,
        strategy: &ReplicationStrategy,
    ) -> std::result::Result<Vec<u64>, DataMovementError> {
        let before = self.before.replicas_for_strategy(token, strategy);
        if !before.contains(&self.leaving) {
            return Ok(Vec::new());
        }
        let after = self.after.replicas_for_strategy(token, strategy);
        if after.is_empty() {
            return Err(DataMovementError::NoRemainingReplica {
                table: table.to_string(),
                token,
            });
        }
        Ok(after.into_iter().filter(|n| !before.contains(n)).collect())
    }

    /// Ring ranges (by their end token) the leaving node replicates under
    /// `strategy`. Reported as evidence: every one of them is covered by the
    /// transfer, whether or not it holds data.
    pub fn replicated_ranges(&self, strategy: &ReplicationStrategy) -> u64 {
        let mut ends: Vec<i64> = self
            .before
            .node_ids()
            .into_iter()
            .flat_map(|n| self.before.tokens_for_node(n))
            .collect();
        ends.sort_unstable();
        ends.dedup();
        ends.into_iter()
            .filter(|&t| {
                self.before
                    .replicas_for_strategy(t, strategy)
                    .contains(&self.leaving)
            })
            .count() as u64
    }
}

/// Stream every local partition of every table in `tables` to each node that
/// becomes its replica when the plan's node leaves.
///
/// All-or-nothing: the first read error, stream error, rejected session or
/// short apply returns an error and nothing further is sent. Batches already
/// delivered stay delivered (they are ordinary replica writes the new owner
/// will own anyway); the caller must not commit the membership change.
pub async fn stream_decommission_data<F>(
    plan: &DecommissionPlan,
    tables: &[(TableId, ReplicationStrategy)],
    scan: F,
    streamer: &dyn PartitionStreamer,
) -> std::result::Result<DataMovementEvidence, DataMovementError>
where
    F: Fn(&TableId) -> BoxStream<'static, ferrosa_common::Result<Partition>>,
{
    let mut evidence = DataMovementEvidence::default();
    for (table, strategy) in tables {
        evidence.ranges += plan.replicated_ranges(strategy);
        let mut buffers: BTreeMap<u64, Vec<Partition>> = BTreeMap::new();
        let mut partitions = scan(table);
        while let Some(item) = partitions.next().await {
            let partition = item.map_err(|e| DataMovementError::Read {
                table: table.to_string(),
                error: e.to_string(),
            })?;
            let targets = plan.targets(table, partition.key.token.0, strategy)?;
            for target in targets {
                let buffer = buffers.entry(target).or_default();
                buffer.push(partition.clone());
                if buffer.len() >= DECOMMISSION_BATCH_PARTITIONS {
                    let batch = std::mem::take(buffer);
                    send_verified(streamer, target, table, batch, &mut evidence).await?;
                }
            }
        }
        for (target, batch) in buffers {
            if !batch.is_empty() {
                send_verified(streamer, target, table, batch, &mut evidence).await?;
            }
        }
    }
    Ok(evidence)
}

async fn send_verified(
    streamer: &dyn PartitionStreamer,
    target: u64,
    table: &TableId,
    batch: Vec<Partition>,
    evidence: &mut DataMovementEvidence,
) -> std::result::Result<(), DataMovementError> {
    let sent = batch.len() as u64;
    let applied = streamer
        .stream(target, table, batch)
        .await
        .map_err(|error| DataMovementError::Stream {
            table: table.to_string(),
            target,
            error,
        })?;
    if applied != sent {
        return Err(DataMovementError::Unverified {
            table: table.to_string(),
            target,
            sent,
            applied,
        });
    }
    evidence.sessions += 1;
    evidence.partitions += sent;
    Ok(())
}

/// Decommission `node_id`: mark it `Leaving`, stream and verify its data, and
/// only then propose `LeaveNode`.
///
/// On a transfer failure `LeaveNode` is NOT proposed: the node stays `Leaving`
/// (out of `replicas()`, never promoted back by the restart pass) and keeps
/// all its data, and the error names what failed. Rerunning the decommission
/// restreams everything; the receivers apply with last-write-wins, so a
/// repeated batch is harmless.
pub async fn decommission_verified<F>(
    proposer: &dyn MembershipProposer,
    ring: &TokenRing,
    node_id: u64,
    tables: &[(TableId, ReplicationStrategy)],
    scan: F,
    streamer: &dyn PartitionStreamer,
) -> Result<DataMovementEvidence>
where
    F: Fn(&TableId) -> BoxStream<'static, ferrosa_common::Result<Partition>>,
{
    proposer
        .propose(super::membership::decommission_drain_op(node_id))
        .await?;
    let plan = DecommissionPlan::new(ring, node_id);
    let evidence = match stream_decommission_data(&plan, tables, scan, streamer).await {
        Ok(evidence) => evidence,
        Err(e) => {
            DECOMMISSION_ABORTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                node_id,
                error = %e,
                "decommission ABORTED: data movement was not verified; LeaveNode NOT \
                 proposed, the node stays Leaving and keeps its data. Fix the cause and \
                 rerun the decommission"
            );
            return Err(e.into());
        }
    };
    proposer.propose(RaftOp::LeaveNode { node_id }).await?;
    tracing::info!(
        node_id,
        sessions = evidence.sessions,
        ranges = evidence.ranges,
        partitions = evidence.partitions,
        "decommission complete: every replicated range streamed and verified, LeaveNode committed"
    );
    Ok(evidence)
}

/// The keyspace/table list a decommission must move, each with its keyspace's
/// replication strategy.
///
/// Includes `system_graph_<ks>` keyspaces (user data; the old code skipped
/// every keyspace starting with "system" and so dropped them) and excludes the
/// built-in system keyspaces, whose content is replicated through Raft. A
/// keyspace with no metadata or an unparseable strategy is an error: guessing
/// a replica set would stream to the wrong owners.
pub fn decommission_tables(
    snapshot: &ferrosa_schema::SchemaSnapshot,
) -> Result<Vec<(TableId, ReplicationStrategy)>> {
    let mut out = Vec::new();
    for (ks, tbl) in snapshot.tables.keys() {
        if !super::cluster::keyspace_needs_cluster_replay(ks) {
            continue;
        }
        let meta = snapshot.keyspaces.get(ks).ok_or_else(|| {
            ClusterError::DataMovementUnverified(format!(
                "table {ks}.{tbl} has no keyspace metadata; cannot compute its replicas"
            ))
        })?;
        let strategy = ReplicationStrategy::try_from(&meta.replication)
            .map_err(|e| ClusterError::DataMovementUnverified(format!("keyspace {ks}: {e}")))?;
        out.push((TableId::new(ks, tbl), strategy));
    }
    out.sort_by(|a, b| a.0.to_string().cmp(&b.0.to_string()));
    Ok(out)
}

/// Encode one partition as the row-stream wire mutation.
///
/// The single place a membership transfer turns a `Partition` into a
/// `StreamedMutation`, so a change to what travels (static rows and partition
/// deletions are p0-streaming-statics' work) lands here once.
pub fn partition_to_streamed_mutation(
    table: &TableId,
    partition: &Partition,
) -> std::result::Result<crate::streaming::StreamedMutation, String> {
    use crate::raft::handlers::RowWire;
    let wire_rows: Vec<RowWire> = partition.rows.iter().cloned().map(RowWire::from).collect();
    let row = bincode::serialize(&wire_rows).map_err(|e| {
        format!(
            "{table}: failed to encode partition {:?}: {e}",
            partition.key
        )
    })?;
    let timestamp = partition
        .rows
        .first()
        .and_then(|r| r.cells.first())
        .map(|(_, cv)| cv.timestamp)
        .unwrap_or(0);
    Ok(crate::streaming::StreamedMutation {
        keyspace: table.keyspace().to_string(),
        table: table.table().to_string(),
        key: partition.key.key.as_bytes().to_vec(),
        row,
        timestamp,
    })
}

/// Production [`PartitionStreamer`]: one row-stream session per batch, with
/// the receiver's `StreamEnd` verdict as the verification.
pub struct StreamSenderStreamer {
    pub peer_manager: std::sync::Arc<ferrosa_net::peer::PeerManager>,
    pub ring: std::sync::Arc<TokenRing>,
    /// openraft NodeId of the node sending (the leaving node).
    pub source_node: u64,
}

#[async_trait]
impl PartitionStreamer for StreamSenderStreamer {
    async fn stream(
        &self,
        target: u64,
        table: &TableId,
        partitions: Vec<Partition>,
    ) -> std::result::Result<u64, String> {
        let host_id = self
            .ring
            .get_node(target)
            .map(|n| n.host_id)
            .ok_or_else(|| format!("node {target} is not in the token ring"))?;
        let mutations = partitions
            .iter()
            .map(|p| partition_to_streamed_mutation(table, p))
            .collect::<std::result::Result<Vec<_>, String>>()?;
        // Random session id: every source used to number its sessions from 1,
        // and the receiver keys sessions by id alone, so two sources streaming
        // to one target overwrote each other's sessions.
        let session_id = uuid::Uuid::new_v4().as_u64_pair().0;
        crate::streaming::StreamSender::send_stream(
            mutations,
            &self.peer_manager,
            host_id,
            session_id,
            (i64::MIN, i64::MAX),
            self.source_node,
            &crate::streaming::StreamConfig::default(),
        )
        .await
        .map_err(|e| e.to_string())
    }
}

/// Decommissions aborted because data movement was not verified. Exposed for
/// metrics; a non-zero value means a node is parked in `Leaving`.
pub static DECOMMISSION_ABORTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    use crate::raft::NodeInfo;

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            host_id: uuid::Uuid::from_u128(id as u128),
            addr: format!("10.0.0.{id}:7000"),
            data_center: "dc1".into(),
            rack: "rack1".into(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    /// Nodes 1..=4 with one token each at 100, 200, 300, 400. A token `t` is
    /// owned by the first ring token >= t, so with RF=2:
    /// - token 50  -> [1, 2]  (node 1 PRIMARY)
    /// - token 350 -> [4, 1]  (node 1 a NON-primary replica)
    /// - token 250 -> [3, 4]  (node 1 not a replica)
    fn four_node_ring(leaving: u64) -> TokenRing {
        let mut ring = TokenRing::new();
        for id in 1..=4u64 {
            ring.add_node(id, node(id));
            ring.assign_tokens(id, &[id as i64 * 100]);
        }
        ring.set_node_state(leaving, NodeState::Leaving);
        ring
    }

    fn rf2() -> ReplicationStrategy {
        ReplicationStrategy::Simple {
            replication_factor: 2,
        }
    }

    fn table() -> TableId {
        TableId::new("ks", "t")
    }

    fn partition(token: i64, key: &[u8]) -> Partition {
        Partition {
            key: DecoratedKey {
                token: Token(token),
                key: PartitionKey::new(key.to_vec()),
            },
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"v".to_vec(), 1))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1),
            }],
        }
    }

    fn scan_of(
        parts: Vec<Partition>,
    ) -> impl Fn(&TableId) -> BoxStream<'static, ferrosa_common::Result<Partition>> {
        move |_| futures::stream::iter(parts.clone().into_iter().map(Ok)).boxed()
    }

    /// Records every batch; fails (or under-applies) for one target if asked.
    #[derive(Default)]
    struct RecordingStreamer {
        sent: StdMutex<Vec<(u64, Vec<Vec<u8>>)>>,
        fail_target: Option<u64>,
        short_target: Option<u64>,
    }

    #[async_trait]
    impl PartitionStreamer for RecordingStreamer {
        async fn stream(
            &self,
            target: u64,
            _table: &TableId,
            partitions: Vec<Partition>,
        ) -> std::result::Result<u64, String> {
            if self.fail_target == Some(target) {
                return Err("receiver rejected the stream: checksum mismatch".into());
            }
            let keys = partitions
                .iter()
                .map(|p| p.key.key.as_bytes().to_vec())
                .collect::<Vec<_>>();
            let n = keys.len() as u64;
            self.sent.lock().unwrap().push((target, keys));
            if self.short_target == Some(target) {
                return Ok(n - 1);
            }
            Ok(n)
        }
    }

    #[derive(Default)]
    struct RecordingProposer {
        ops: StdMutex<Vec<RaftOp>>,
    }

    #[async_trait]
    impl MembershipProposer for RecordingProposer {
        async fn propose(&self, op: RaftOp) -> Result<()> {
            self.ops.lock().unwrap().push(op);
            Ok(())
        }
    }

    impl RecordingProposer {
        fn proposed_leave(&self) -> bool {
            self.ops
                .lock()
                .unwrap()
                .iter()
                .any(|op| matches!(op, RaftOp::LeaveNode { .. }))
        }
    }

    fn sent_to(streamer: &RecordingStreamer, target: u64) -> Vec<Vec<u8>> {
        streamer
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| *t == target)
            .flat_map(|(_, keys)| keys.clone())
            .collect()
    }

    /// P0-2: a stream that fails for one range must NOT commit `LeaveNode`.
    /// The old code logged the failure and proposed `LeaveNode` anyway, so the
    /// range's only extra copy vanished with the node.
    #[tokio::test]
    async fn a_failed_stream_does_not_commit_leave_node() {
        let ring = four_node_ring(1);
        let proposer = RecordingProposer::default();
        // token 350's new owner is node 2; that stream fails.
        let streamer = RecordingStreamer {
            fail_target: Some(2),
            ..Default::default()
        };

        let result = decommission_verified(
            &proposer,
            &ring,
            1,
            &[(table(), rf2())],
            scan_of(vec![partition(50, b"a"), partition(350, b"b")]),
            &streamer,
        )
        .await;

        let err = result.expect_err("a failed stream must abort the decommission");
        assert!(
            matches!(&err, ClusterError::DataMovementUnverified(m) if m.contains("node 2")),
            "the error must name the failed target, got {err}"
        );
        assert!(
            !proposer.proposed_leave(),
            "LeaveNode must not be proposed after a failed stream: {:?}",
            proposer.ops.lock().unwrap()
        );
        assert!(
            matches!(
                proposer.ops.lock().unwrap().as_slice(),
                [RaftOp::SetNodeState {
                    node_id: 1,
                    state: NodeState::Leaving
                }]
            ),
            "the node must be left Leaving"
        );
    }

    /// P0-2: a range the leaving node replicates but is NOT primary for must be
    /// streamed to its new owner. The old code streamed only partitions whose
    /// primary owner was the leaving node, to a single target.
    #[tokio::test]
    async fn replica_ranges_are_streamed_not_only_primary_ranges() {
        let ring = four_node_ring(1);
        let proposer = RecordingProposer::default();
        let streamer = RecordingStreamer::default();

        let evidence = decommission_verified(
            &proposer,
            &ring,
            1,
            &[(table(), rf2())],
            scan_of(vec![
                partition(50, b"primary"),
                partition(350, b"replica"),
                partition(250, b"unrelated"),
            ]),
            &streamer,
        )
        .await
        .expect("a fully verified transfer must decommission");

        // token 50: [1,2] -> [2,3]: node 3 is the new owner.
        assert_eq!(sent_to(&streamer, 3), vec![b"primary".to_vec()]);
        // token 350: [4,1] -> [4,2]: node 2 is the new owner. This is the
        // non-primary replica range the old code never streamed.
        assert_eq!(sent_to(&streamer, 2), vec![b"replica".to_vec()]);
        assert!(
            sent_to(&streamer, 4).is_empty(),
            "node 4 already had its copy"
        );
        assert_eq!(evidence.partitions, 2);
        // Node 1 replicates the ranges ending at 100 (primary) and 400.
        assert_eq!(evidence.ranges, 2);
        assert!(proposer.proposed_leave(), "LeaveNode after verification");
    }

    /// A receiver that applies fewer partitions than were sent is not a
    /// verified transfer.
    #[tokio::test]
    async fn a_short_apply_does_not_commit_leave_node() {
        let ring = four_node_ring(1);
        let proposer = RecordingProposer::default();
        let streamer = RecordingStreamer {
            short_target: Some(3),
            ..Default::default()
        };

        let err = decommission_verified(
            &proposer,
            &ring,
            1,
            &[(table(), rf2())],
            scan_of(vec![partition(50, b"a"), partition(60, b"b")]),
            &streamer,
        )
        .await
        .expect_err("a short apply must abort");

        assert!(err.to_string().contains("verified 1 of 2"), "{err}");
        assert!(!proposer.proposed_leave());
    }

    /// A local read error aborts. The old code `continue`d past it, skipping
    /// the partition and still committing `LeaveNode`.
    #[tokio::test]
    async fn a_local_read_error_does_not_commit_leave_node() {
        let ring = four_node_ring(1);
        let proposer = RecordingProposer::default();
        let streamer = RecordingStreamer::default();
        let scan = |_: &TableId| {
            futures::stream::iter(vec![
                Ok(partition(50, b"a")),
                Err(ferrosa_common::Error::InvalidData("sstable corrupt".into())),
            ])
            .boxed()
        };

        let err = decommission_verified(&proposer, &ring, 1, &[(table(), rf2())], scan, &streamer)
            .await
            .expect_err("a read error must abort");

        assert!(err.to_string().contains("sstable corrupt"), "{err}");
        assert!(!proposer.proposed_leave());
    }

    /// The last replica of a range cannot leave: its data would have nowhere
    /// to go.
    #[tokio::test]
    async fn the_last_replica_cannot_leave() {
        let mut ring = TokenRing::new();
        ring.add_node(1, node(1));
        ring.assign_tokens(1, &[100]);
        let proposer = RecordingProposer::default();

        let err = decommission_verified(
            &proposer,
            &ring,
            1,
            &[(table(), rf2())],
            scan_of(vec![partition(50, b"a")]),
            &RecordingStreamer::default(),
        )
        .await
        .expect_err("leaving with no remaining replica must abort");

        assert!(err.to_string().contains("no replica"), "{err}");
        assert!(!proposer.proposed_leave());
    }
}
