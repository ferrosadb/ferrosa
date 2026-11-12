//! Structural classification of a supplied committed metadata-effect interval.
//!
//! A receipt certifies only a complete syntactically row-preserving supplied
//! interval after the checkpoint, ending at the exact retired and snapshot full
//! LogId. Entries are borrowed individually; no payloads or history are cloned,
//! collected or retained. The verifier keeps fixed scalar state and fails closed
//! on the first error. Its small allowlist covers Blank/Membership, CreateKeyspace,
//! CreateIndex/IndexStatus/DropIndex and ALTER with only regular-column additions
//! (including an empty ALTER). All other operations are conservatively refused.
//!
//! This is classification only: it does NOT establish schema-dependent semantic
//! preservation. In particular, `add_columns` can replace an existing column's
//! meaning; type removal can invalidate references and is refused here. A future
//! runtime must separately preflight the evolving trusted schema and retained
//! layouts. A receipt does NOT establish source authentication, trusted checkpoint
//! provenance, immutable history acquisition/decoding, engine maintenance,
//! user-row inventory, table incarnations, physical completeness, placement or
//! acknowledged-history coverage, metadata/index reconciliation, durable
//! activation, dormant restart or admission release. Automatic runtime repair is
//! not wired; this receipt grants no activation or fault-clearing authority.

use super::{FerrosRaftConfig, RaftOp};
use ferrosa_schema::metadata::column::ColumnKind;
use openraft::{Entry, EntryPayload, LogId};

/// A refusal to classify the supplied interval. Streaming errors are sticky.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCoverageError {
    /// No checkpoint identity was supplied (its provenance remains external).
    MissingCheckpoint,
    /// The checkpoint has no representable successor.
    IndexOverflow,
    /// The target does not follow the checkpoint.
    InvalidInterval,
    /// Snapshot and retired committed full LogIds differ.
    SnapshotIdentityMismatch,
    /// The required interval exceeds the caller's entry bound.
    ScanBoundExceeded,
    /// An entry is at/before the checkpoint or past the committed target.
    OutsideInterval,
    /// A gap, duplicate or out-of-order entry was supplied.
    NonContiguous,
    /// An intermediate leader identity conflicts with the interval's ordering.
    InvalidIdentity,
    /// The final entry does not have the exact retired committed full LogId.
    FinalIdentityMismatch,
    /// The effect is outside the structural allowlist.
    UnsupportedEffect,
    /// The stream ended before the committed target.
    Incomplete,
}

/// Privately constructed structural interval evidence; not runtime repair authority.
///
/// Deliberately non-Clone. See the module documentation for excluded guarantees.
#[derive(Debug)]
#[must_use]
pub struct MetadataIntervalReceipt {
    checkpoint: LogId<u64>,
    committed_log_id: LogId<u64>,
    entry_count: u64,
}

impl MetadataIntervalReceipt {
    /// The supplied checkpoint identity, without a provenance guarantee.
    pub fn checkpoint(&self) -> LogId<u64> {
        self.checkpoint
    }

    /// The exact retired/snapshot full LogId reached by the supplied interval.
    pub fn committed_log_id(&self) -> LogId<u64> {
        self.committed_log_id
    }

    /// Number of contiguous entries classified after the checkpoint.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }
}

/// Pure streaming classifier that never retains an entry borrow or payload.
pub struct CommittedMetadataVerifier {
    checkpoint: LogId<u64>,
    committed_log_id: LogId<u64>,
    last_log_id: LogId<u64>,
    entry_count: u64,
    first_error: Option<MetadataCoverageError>,
}

impl CommittedMetadataVerifier {
    /// Validate supplied bounds before accepting any entries.
    ///
    /// Error priority is missing checkpoint, successor overflow, invalid interval,
    /// snapshot identity, then scan bound. No checkpoint provenance is checked.
    pub fn begin(
        checkpoint: Option<LogId<u64>>,
        retired: LogId<u64>,
        snapshot: LogId<u64>,
        max_entries: u64,
    ) -> Result<Self, MetadataCoverageError> {
        use MetadataCoverageError as E;
        let checkpoint = checkpoint.ok_or(E::MissingCheckpoint)?;
        let first_index = checkpoint.index.checked_add(1).ok_or(E::IndexOverflow)?;
        if first_index > retired.index {
            return Err(E::InvalidInterval);
        }
        if snapshot != retired {
            return Err(E::SnapshotIdentityMismatch);
        }
        if retired.index - checkpoint.index > max_entries {
            return Err(E::ScanBoundExceeded);
        }
        Ok(Self {
            checkpoint,
            committed_log_id: retired,
            last_log_id: checkpoint,
            entry_count: 0,
            first_error: None,
        })
    }

    /// Classify one borrowed entry, preserving the first failure for all calls.
    ///
    /// Checks bounds before contiguity, identities before effects. A successful
    /// terminal `u64::MAX` entry does not require a representable successor.
    pub fn push(&mut self, entry: &Entry<FerrosRaftConfig>) -> Result<(), MetadataCoverageError> {
        if let Some(error) = self.first_error {
            return Err(error);
        }
        let result = self.check_entry(entry);
        if let Err(error) = result {
            self.first_error = Some(error);
        } else {
            self.last_log_id = entry.log_id;
            // Contiguity and bounds prove this cannot exceed the interval length.
            self.entry_count += 1;
        }
        result
    }

    fn check_entry(&self, entry: &Entry<FerrosRaftConfig>) -> Result<(), MetadataCoverageError> {
        use MetadataCoverageError as E;
        let log_id = entry.log_id;
        if log_id.index <= self.checkpoint.index || log_id.index > self.committed_log_id.index {
            return Err(E::OutsideInterval);
        }
        if self.last_log_id.index.checked_add(1) != Some(log_id.index) {
            return Err(E::NonContiguous);
        }
        if log_id.index == self.committed_log_id.index && log_id != self.committed_log_id {
            return Err(E::FinalIdentityMismatch);
        }
        let leader = log_id.leader_id;
        let previous = self.last_log_id.leader_id;
        let target = self.committed_log_id.leader_id;
        if leader < previous
            || leader > target
            || (leader.term == previous.term && leader != previous)
            || (leader.term == target.term && leader != target)
        {
            return Err(E::InvalidIdentity);
        }
        match &entry.payload {
            EntryPayload::Blank | EntryPayload::Membership(_) => Ok(()),
            EntryPayload::Normal(command) => match &command.op {
                RaftOp::CreateKeyspace(_)
                | RaftOp::CreateIndex(_)
                | RaftOp::IndexStatus { .. }
                | RaftOp::DropIndex { .. } => Ok(()),
                RaftOp::AlterTable { updates, .. } => {
                    if updates.params.is_none()
                        && updates.extensions.is_none()
                        && updates.drop_columns.is_empty()
                        && updates
                            .add_columns
                            .iter()
                            .all(|column| column.kind == ColumnKind::Regular)
                    {
                        Ok(())
                    } else {
                        Err(E::UnsupportedEffect)
                    }
                }
                // No wildcard: future RaftOp variants must be classified explicitly.
                RaftOp::DropKeyspace(_)
                | RaftOp::CreateTable(_)
                | RaftOp::DropTable { .. }
                | RaftOp::AlterKeyspace { .. }
                | RaftOp::CreateRole(_)
                | RaftOp::AlterRole { .. }
                | RaftOp::DropRole(_)
                | RaftOp::Grant(_)
                | RaftOp::Revoke { .. }
                | RaftOp::GrantRole { .. }
                | RaftOp::RevokeRole { .. }
                | RaftOp::CreateType(_)
                | RaftOp::DropType { .. }
                | RaftOp::CreateFunction(_)
                | RaftOp::DropFunction { .. }
                | RaftOp::CreateAggregate(_)
                | RaftOp::DropAggregate { .. }
                | RaftOp::JoinNode(_)
                | RaftOp::UpdateNodeInfo(_)
                | RaftOp::LeaveNode { .. }
                | RaftOp::AssignTokens { .. }
                | RaftOp::UpdateConfig(_)
                | RaftOp::ApproveNode { .. }
                | RaftOp::SetNodeState { .. }
                | RaftOp::AccordApply { .. }
                | RaftOp::RecordBootstrapComplete { .. } => Err(E::UnsupportedEffect),
            },
        }
    }

    /// Consume the verifier; only a complete error-free interval yields evidence.
    pub fn finish(self) -> Result<MetadataIntervalReceipt, MetadataCoverageError> {
        if let Some(error) = self.first_error {
            return Err(error);
        }
        if self.last_log_id != self.committed_log_id {
            return Err(MetadataCoverageError::Incomplete);
        }
        Ok(MetadataIntervalReceipt {
            checkpoint: self.checkpoint,
            committed_log_id: self.committed_log_id,
            entry_count: self.entry_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{CommittedMetadataVerifier, MetadataCoverageError, MetadataIntervalReceipt};
    use crate::raft::{FerrosRaftConfig, IndexNodeStatus, RaftCommand, RaftOp};
    use ferrosa_common::{AccordTimestamp, TxnId};
    use ferrosa_schema::metadata::{
        column::{ClusteringOrder, ColumnKind, ColumnMetadata},
        index::IndexMetadata,
        keyspace::{KeyspaceMetadata, ReplicationParams},
        table::{TableParams, TableUpdates},
    };
    use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership};
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use uuid::Uuid;

    // These describe test inputs, never a second classifier or a fake receipt.
    #[derive(Clone, Copy)]
    enum Effect {
        Blank,
        Membership,
        Schema,
        CreateIndex,
        IndexStatus,
        DropIndex,
        AddColumn,
        Accord(bool),
        DropTable,
        DropKeyspace,
        DropColumn,
        AddAndDrop,
        AddKeyColumn,
        TableParams,
        Extensions,
    }

    fn id(term: u64, node: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, node), index)
    }

    fn column(kind: ColumnKind) -> ColumnMetadata {
        ColumnMetadata {
            name: "new_value".into(),
            kind,
            position: 0,
            column_type: "text".into(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    fn entry(log_id: LogId<u64>, effect: Effect) -> Entry<FerrosRaftConfig> {
        let mut updates = TableUpdates {
            params: None,
            add_columns: vec![],
            drop_columns: vec![],
            extensions: None,
        };
        let op = match effect {
            Effect::Blank => {
                return Entry {
                    log_id,
                    payload: EntryPayload::Blank,
                }
            }
            Effect::Membership => {
                return Entry {
                    log_id,
                    payload: EntryPayload::Membership(Membership::new(
                        vec![BTreeSet::from([7_u64])],
                        BTreeMap::from([(7_u64, BasicNode::new("peer7"))]),
                    )),
                }
            }
            Effect::Schema => RaftOp::CreateKeyspace(KeyspaceMetadata {
                name: "ks".into(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: HashMap::from([("replication_factor".into(), "1".into())]),
                },
            }),
            Effect::CreateIndex => RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "rows".into(),
                name: "value_idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["value".into()],
                filter_predicate: None,
                options: HashMap::new(),
            }),
            Effect::IndexStatus => RaftOp::IndexStatus {
                node_id: 7,
                keyspace: "ks".into(),
                table: "rows".into(),
                index_name: "value_idx".into(),
                status: IndexNodeStatus::Ready,
            },
            Effect::DropIndex => RaftOp::DropIndex {
                keyspace: "ks".into(),
                table: "rows".into(),
                index: "value_idx".into(),
            },
            Effect::Accord(nonempty) => {
                let hlc = AccordTimestamp::synthetic(123);
                RaftOp::AccordApply {
                    txn_id: TxnId::new(7, hlc),
                    hlc,
                    mutation: if nonempty { vec![1, 2, 3] } else { vec![] },
                }
            }
            Effect::DropTable => RaftOp::DropTable {
                keyspace: "ks".into(),
                table: "rows".into(),
            },
            Effect::DropKeyspace => RaftOp::DropKeyspace("ks".into()),
            alter => {
                match alter {
                    Effect::AddColumn => updates.add_columns.push(column(ColumnKind::Regular)),
                    Effect::DropColumn => updates.drop_columns.push("value".into()),
                    Effect::AddAndDrop => {
                        updates.add_columns.push(column(ColumnKind::Regular));
                        updates.drop_columns.push("value".into());
                    }
                    Effect::AddKeyColumn => {
                        updates.add_columns.push(column(ColumnKind::PartitionKey))
                    }
                    Effect::TableParams => {
                        updates.params = Some(TableParams {
                            default_time_to_live: 1,
                            ..TableParams::default()
                        })
                    }
                    Effect::Extensions => {
                        updates.extensions = Some(HashMap::from([(
                            "opaque".into(),
                            "not proven row preserving".into(),
                        )]))
                    }
                    _ => unreachable!("non-ALTER fixture handled above"),
                }
                RaftOp::AlterTable {
                    keyspace: "ks".into(),
                    table: "rows".into(),
                    updates: Box::new(updates),
                }
            }
        };
        Entry {
            log_id,
            payload: EntryPayload::Normal(RaftCommand {
                op,
                schema_version: Uuid::from_u128(123),
            }),
        }
    }

    struct Case {
        name: &'static str,
        checkpoint: Option<LogId<u64>>,
        retired: LogId<u64>,
        snapshot: LogId<u64>,
        max_entries: u64,
        // Fixture recipes only: production sees one short-lived borrowed entry.
        steps: Vec<(LogId<u64>, Effect, Result<(), MetadataCoverageError>)>,
        begin_error: Option<MetadataCoverageError>,
        finish_error: Option<MetadataCoverageError>,
    }

    fn complete(name: &'static str) -> Case {
        Case {
            name,
            checkpoint: Some(id(2, 3, 40)),
            retired: id(5, 7, 48),
            snapshot: id(5, 7, 48),
            max_entries: 8,
            steps: [
                Effect::Blank,
                Effect::Membership,
                Effect::Schema,
                Effect::CreateIndex,
                Effect::IndexStatus,
                Effect::DropIndex,
                Effect::AddColumn,
                Effect::Blank,
            ]
            .into_iter()
            .enumerate()
            .map(|(offset, effect)| (id(5, 7, 41 + offset as u64), effect, Ok(())))
            .collect(),
            begin_error: None,
            finish_error: None,
        }
    }

    /// One bounded behavior: classify exactly a complete committed interval.
    ///
    /// Source evidence (not a second classifier): state_machine.rs apply branches
    /// for CreateKeyspace, CreateIndex/IndexStatus/DropIndex, and AlterTable update
    /// metadata/derived indexes. StorageEngine::update_table_schema preserves old
    /// row ordinals across additive ALTER. DropTable/DropKeyspace unregister storage;
    /// dropping columns loses user values. Params/extensions/key additions have no
    /// row-preservation proof here and are conservatively unsupported. AccordApply
    /// dispatches opaque mutation bytes, even when today's fixture bytes are empty.
    /// Blank/Membership update Raft bookkeeping; they prove no data placement.
    /// A future production classifier must exhaustively match the actual RaftOp.
    #[test]
    fn certifies_only_complete_row_preserving_committed_intervals() {
        use MetadataCoverageError as E;

        // Compile-time negative trait assertion without adding a dependency:
        // if the real receipt implements Clone, inference becomes ambiguous.
        trait AmbiguousIfClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfClone<()> for T {}
        impl<T: ?Sized + Clone> AmbiguousIfClone<u8> for T {}
        let _ = <MetadataIntervalReceipt as AmbiguousIfClone<_>>::check;

        let mut cases = vec![complete("complete mixed interval at exact scan bound")];
        for (name, effect) in [
            ("opaque Accord mutation", Effect::Accord(true)),
            ("empty Accord is still opaque", Effect::Accord(false)),
            ("drop user table", Effect::DropTable),
            ("drop user keyspace", Effect::DropKeyspace),
            ("drop column", Effect::DropColumn),
            ("add cannot hide a drop", Effect::AddAndDrop),
            (
                "key addition lacks row preservation proof",
                Effect::AddKeyColumn,
            ),
            (
                "TTL params lack row preservation proof",
                Effect::TableParams,
            ),
            (
                "opaque extensions lack row preservation proof",
                Effect::Extensions,
            ),
        ] {
            let mut case = complete(name);
            case.steps[6] = (id(5, 7, 47), effect, Err(E::UnsupportedEffect));
            // Neither a corrected record at the same index nor a safe tail can
            // erase the first failure or produce a classification receipt.
            case.steps.insert(
                7,
                (id(5, 7, 47), Effect::AddColumn, Err(E::UnsupportedEffect)),
            );
            case.steps.last_mut().unwrap().2 = Err(E::UnsupportedEffect);
            case.finish_error = Some(E::UnsupportedEffect);
            cases.push(case);
        }
        for (name, bad_index) in [("gap", 48), ("duplicate", 46), ("out of order", 45)] {
            let mut case = complete(name);
            case.steps[6] = (id(5, 7, bad_index), Effect::Blank, Err(E::NonContiguous));
            case.steps[7].2 = Err(E::NonContiguous);
            case.finish_error = Some(E::NonContiguous);
            cases.push(case);
        }
        for (name, index) in [
            ("before supplied interval", 40),
            ("wrong final index past target", 49),
        ] {
            let mut case = complete(name);
            case.steps[7] = (id(5, 7, index), Effect::Blank, Err(E::OutsideInterval));
            case.finish_error = Some(E::OutsideInterval);
            cases.push(case);
        }
        let mut extra = complete("extra entry after complete tail refuses receipt");
        extra
            .steps
            .push((id(5, 7, 49), Effect::Blank, Err(E::OutsideInterval)));
        extra.finish_error = Some(E::OutsideInterval);
        cases.push(extra);
        for (name, remaining) in [("missing tail", 7), ("empty stream", 0)] {
            let mut case = complete(name);
            case.steps.truncate(remaining);
            case.finish_error = Some(E::Incomplete);
            cases.push(case);
        }
        for (name, final_id) in [
            ("wrong final entry term", id(6, 7, 48)),
            ("wrong final entry node", id(5, 8, 48)),
        ] {
            let mut case = complete(name);
            case.steps[7] = (final_id, Effect::Blank, Err(E::FinalIdentityMismatch));
            case.finish_error = Some(E::FinalIdentityMismatch);
            cases.push(case);
        }
        for (name, snapshot) in [
            ("equal index wrong snapshot term", id(6, 7, 48)),
            ("equal index wrong snapshot node", id(5, 8, 48)),
            ("stale snapshot index", id(5, 7, 47)),
            ("newer unaccounted snapshot", id(5, 7, 49)),
        ] {
            let mut case = complete(name);
            case.snapshot = snapshot;
            case.begin_error = Some(E::SnapshotIdentityMismatch);
            cases.push(case);
        }
        for (name, checkpoint, error) in [
            ("missing trusted checkpoint", None, E::MissingCheckpoint),
            (
                "empty supplied interval",
                Some(id(5, 7, 48)),
                E::InvalidInterval,
            ),
            (
                "reversed supplied interval",
                Some(id(5, 7, 49)),
                E::InvalidInterval,
            ),
            (
                "checkpoint successor overflow",
                Some(id(5, 7, u64::MAX)),
                E::IndexOverflow,
            ),
        ] {
            let mut case = complete(name);
            case.checkpoint = checkpoint;
            case.begin_error = Some(error);
            cases.push(case);
        }
        for (name, bound) in [("interval exceeds scan bound", 7), ("zero scan bound", 0)] {
            let mut case = complete(name);
            case.max_entries = bound;
            case.begin_error = Some(E::ScanBoundExceeded);
            cases.push(case);
        }
        let mut max_tail = complete("terminal MAX index needs no successor");
        max_tail.checkpoint = Some(id(5, 7, u64::MAX - 2));
        max_tail.retired = id(5, 7, u64::MAX);
        max_tail.snapshot = max_tail.retired;
        max_tail.max_entries = 2;
        max_tail.steps = vec![
            (id(5, 7, u64::MAX - 1), Effect::Blank, Ok(())),
            (id(5, 7, u64::MAX), Effect::Blank, Ok(())),
        ];
        cases.push(max_tail);

        for case in cases {
            let start = CommittedMetadataVerifier::begin(
                case.checkpoint,
                case.retired,
                case.snapshot,
                case.max_entries,
            );
            if let Some(expected) = case.begin_error {
                match start {
                    Err(actual) => assert_eq!(actual, expected, "{}", case.name),
                    Ok(_) => panic!("{}: invalid interval admitted", case.name),
                }
                continue;
            }
            let mut verifier = start.unwrap_or_else(|e| panic!("{}: begin: {e:?}", case.name));
            for (log_id, effect, expected) in case.steps {
                let borrowed = entry(log_id, effect);
                assert_eq!(
                    verifier.push(&borrowed),
                    expected,
                    "{} at {log_id:?}",
                    case.name
                );
                // This explicit drop also prevents a verifier API that retains
                // entry borrows. No payload/history clones or retained entries.
                drop(borrowed);
            }
            match verifier.finish() {
                Ok(receipt) => {
                    assert!(
                        case.finish_error.is_none(),
                        "{}: unexpected receipt",
                        case.name
                    );
                    assert_eq!(
                        receipt.checkpoint(),
                        case.checkpoint.unwrap(),
                        "{}",
                        case.name
                    );
                    assert_eq!(receipt.committed_log_id(), case.retired, "{}", case.name);
                    assert_eq!(receipt.committed_log_id(), case.snapshot, "{}", case.name);
                    assert_eq!(
                        receipt.entry_count(),
                        case.retired.index - case.checkpoint.unwrap().index,
                        "{}",
                        case.name
                    );
                }
                Err(actual) => assert_eq!(Some(actual), case.finish_error, "{}", case.name),
            }
        }
    }
}
