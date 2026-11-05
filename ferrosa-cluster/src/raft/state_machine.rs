//! Raft state machine: applies [`RaftCommand`] entries to cluster state.
//!
//! [`FerrosStateMachine`] implements openraft's [`RaftStateMachine`] trait.
//! It maintains a deterministic [`RaftState`] (BTreeMap-based) and optionally
//! propagates side effects to a local [`Schema`] and [`StorageEngine`].
//!
//! Snapshots are serialized with bincode because `serde_json` does not support
//! non-string map keys (our `BTreeMap<(String, String), _>` tuple keys).

//! Last revised: 2026-11-05.
//! Last changed: Serialize durable snapshot promotion and reject stale builders on disk.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, Membership, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ferrosa_common::{AccordTimestamp, CqlType, TxnId};
use ferrosa_schema::metadata::aggregate::UserAggregateMetadata;
use ferrosa_schema::metadata::function::UserFunctionMetadata;
use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::metadata::keyspace::KeyspaceMetadata;
use ferrosa_schema::metadata::table::TableMetadata;
use ferrosa_schema::metadata::user_type::UserTypeMetadata;
use ferrosa_schema::system::persistence::SystemTableMutation;
use ferrosa_schema::{GrantEntry, RoleMetadata, Schema};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::system_table_writer::SystemTableWriter;

use crate::config::ClusterConfig;
use crate::raft::multi_dc_apply::{AppliedTxnLedger, ReorderBuffer};
use crate::raft::{
    FerrosRaftConfig, IndexNodeStatus, NodeInfo, RaftCommand, RaftOp, RaftResponse, Token,
};
use crate::ring::TokenRing;

// ---------------------------------------------------------------------------
// ApplyError — typed sub-errors that can occur inside `apply_command`.
// ---------------------------------------------------------------------------

/// A failed committed local Schema or Engine effect during `apply_command`.
///
/// These errors propagate through the retained worker as a storage failure.
/// The owner keeps its previous metadata and applied index, latches the error,
/// and refuses further apply/snapshot progress. Ordinary domain rejection is
/// a separate `RaftResponse::Error` and does not poison the machine.
/// Shared physical effects may already be visible; this is not transactional
/// rollback, a persistent failure journal, or automatic recovery.
#[derive(Debug, Clone)]
pub enum ApplyError {
    /// `engine.register_table` failed — writes to the table will not
    /// land in storage.  The most dangerous class of error.
    EngineRegisterTable {
        keyspace: String,
        table: String,
        reason: String,
    },
    /// `engine.unregister_table` failed — stale data may persist
    /// after a DropTable / DropKeyspace.
    EngineUnregisterTable {
        keyspace: String,
        table: String,
        reason: String,
    },
    /// Catch-all for sites not yet migrated to a typed variant.
    Other(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EngineRegisterTable {
                keyspace,
                table,
                reason,
            } => write!(
                f,
                "engine.register_table({keyspace}.{table}) failed: {reason}"
            ),
            Self::EngineUnregisterTable {
                keyspace,
                table,
                reason,
            } => write!(
                f,
                "engine.unregister_table({keyspace}.{table}) failed: {reason}"
            ),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

// ---------------------------------------------------------------------------
// Deferred system-table writes
// ---------------------------------------------------------------------------

/// Severity for the log line emitted if a deferred system-table write fails.
///
/// `WarnReplay` is used for the create-DDL sites where a failure is expected
/// during Raft log replay on startup (the `system_schema.*` tables are not yet
/// registered); everything else is a genuine `Error`.
#[derive(Debug, Clone, Copy)]
enum SystemWriteLogLevel {
    WarnReplay,
    Error,
}

/// A `SystemTableMutation` collected during `apply_command` to be executed
/// **after** the in-memory state mutation, on a blocking thread.
///
/// `apply_command` runs on the openraft apply task (a runtime worker). The
/// `engine.write` calls inside `SystemTableWriter::apply` are synchronous and
/// touch the memtable/commit-log, which previously parked the raft worker and
/// delayed heartbeat responses. Collecting the mutations and draining them via
/// `spawn_blocking` (awaited before the next entry, preserving openraft's
/// sequential apply ordering) keeps that blocking work off the worker.
struct PendingSystemWrite {
    mutation: SystemTableMutation,
    level: SystemWriteLogLevel,
    /// Static context string mirroring the original inline log message.
    context: &'static str,
}

// ---------------------------------------------------------------------------
// RaftState
// ---------------------------------------------------------------------------

/// All cluster metadata.
///
/// Uses [`BTreeMap`] for deterministic iteration order. The [`apply`] method
/// must be purely deterministic so that every replica converges to the same
/// state given the same log.
///
/// [`apply`]: FerrosStateMachine::apply
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RaftState {
    /// Schema version — bumped on every DDL mutation.
    pub schema_version: Uuid,
    /// All keyspaces, keyed by name.
    pub keyspaces: BTreeMap<String, KeyspaceMetadata>,
    /// All tables, keyed by (keyspace, table).
    pub tables: BTreeMap<(String, String), TableMetadata>,
    /// All roles, keyed by role name.
    pub roles: BTreeMap<String, RoleMetadata>,
    /// All grants, keyed by role name.
    pub grants: BTreeMap<String, Vec<GrantEntry>>,
    /// All secondary indexes, keyed by (keyspace, table, index_name).
    pub indexes: BTreeMap<(String, String, String), IndexMetadata>,
    /// All user-defined types, keyed by (keyspace, type_name).
    pub types: BTreeMap<(String, String), UserTypeMetadata>,
    /// All user-defined functions, keyed by (keyspace, name, arg_types).
    #[serde(default)]
    pub functions: BTreeMap<(String, String, Vec<CqlType>), UserFunctionMetadata>,
    /// All user-defined aggregates, keyed by (keyspace, name, arg_types).
    #[serde(default)]
    pub aggregates: BTreeMap<(String, String, Vec<CqlType>), UserAggregateMetadata>,
    /// Cluster members, keyed by openraft NodeId.
    pub members: BTreeMap<u64, NodeInfo>,
    /// Token ring: token → NodeId mapping.
    pub token_map: BTreeMap<Token, u64>,
    /// Per-node index build status.
    ///
    /// Keyed by (keyspace, table, index_name), maps node_id to `IndexNodeStatus`.
    /// Updated by `RaftOp::IndexStatus` proposals. Cleaned up on `DropIndex`.
    #[serde(default)]
    pub index_state_map: BTreeMap<(String, String, String), BTreeMap<u64, IndexNodeStatus>>,
    /// Cluster-wide configuration.
    pub config: ClusterConfig,
    /// Set of host IDs that have been explicitly approved to join the cluster.
    pub approved_nodes: BTreeSet<Uuid>,

    // ---- Multi-DC Accord (Sprint 7) ------------------------------------
    /// HLC watermark — the largest Accord timestamp that has been
    /// drained out of the reorder buffer and durably applied. Advances
    /// monotonically; never regresses. (W7.1 / I-27.)
    #[serde(default)]
    pub hlc_watermark: AccordTimestamp,
    /// Maximum HLC skew observed at apply time relative to the
    /// watermark, in microseconds (the HLC itself is nanoseconds).
    /// Recorded for operator visibility (W7.1 REFACTOR /
    /// RAFT_ACCORD_MAX_SKEW gauge).
    #[serde(default)]
    pub max_observed_skew_us: u64,
    /// Idempotent-apply ledger keyed by Accord transaction id. Replayed
    /// `RaftOp::AccordApply` entries with a matching `txn_id` are
    /// short-circuited to a no-op (W7.5 / I-28).
    #[serde(default)]
    pub applied_accord_txns: AppliedTxnLedger,
    /// Reorder buffer for `RaftOp::AccordApply` entries (W7.2 / I-27).
    /// Buffered entries stall until the watermark advances past their
    /// HLC; on drain they apply in ascending timestamp order across
    /// every replica.
    #[serde(default)]
    pub accord_apply_buffer: ReorderBuffer,
    /// Members whose bootstrap stream completed and was verified, keyed by
    /// openraft NodeId (P0-4). Written only by
    /// `RaftOp::RecordBootstrapComplete`; the promote pass moves a `Joining`
    /// member to `Normal` only when it has an entry here.
    ///
    /// `serde(skip)`: adding a field to this struct would change the bincode
    /// layout of every persisted snapshot, and an upgraded node could no longer
    /// load the snapshot its previous build wrote. The map travels instead in a
    /// trailing `SnapshotExtension` after the unchanged `SnapshotData`.
    #[serde(skip)]
    pub bootstrap_complete: BTreeMap<u64, super::DataMovementEvidence>,
}

// ---------------------------------------------------------------------------
// Snapshot data (persisted alongside metadata)
// ---------------------------------------------------------------------------

/// Wrapper that bundles `RaftState` together with openraft bookkeeping so
/// that a single bincode blob captures everything needed for a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotData {
    state: RaftState,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
}

/// State appended AFTER the bincode of [`SnapshotData`] (P0-4).
///
/// `bincode::deserialize` accepts trailing bytes, so a reader built before this
/// extension decodes `SnapshotData` and ignores it, and a snapshot written
/// before it simply has no trailing bytes. That keeps the format compatible in
/// both directions without touching `RaftState`'s field layout. Add new
/// snapshot state here (at the end), never to `RaftState` directly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SnapshotExtension {
    bootstrap_complete: BTreeMap<u64, super::DataMovementEvidence>,
}

/// Encode a snapshot: `SnapshotData` followed by its [`SnapshotExtension`].
fn encode_snapshot(data: &SnapshotData) -> bincode::Result<Vec<u8>> {
    let mut bytes = bincode::serialize(data)?;
    let ext = SnapshotExtension {
        bootstrap_complete: data.state.bootstrap_complete.clone(),
    };
    bytes.extend(bincode::serialize(&ext)?);
    Ok(bytes)
}

/// Decode a snapshot written by [`encode_snapshot`] or by a build that
/// predates the extension (no trailing bytes → empty extension). A trailing
/// extension that does not decode is an error, not an empty map: silently
/// dropping bootstrap records would let the promote pass refuse members that
/// did complete, or worse, hide corruption.
fn decode_snapshot(bytes: &[u8]) -> bincode::Result<SnapshotData> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut data: SnapshotData = bincode::deserialize_from(&mut cursor)?;
    let consumed = usize::try_from(cursor.position()).unwrap_or(bytes.len());
    let rest = &bytes[consumed.min(bytes.len())..];
    if !rest.is_empty() {
        let ext: SnapshotExtension = bincode::deserialize(rest)?;
        data.state.bootstrap_complete = ext.bootstrap_complete;
    }
    Ok(data)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    bytes: Vec<u8>,
}

/// The metadata prefix orders both replication and durable snapshot publication.
fn snapshot_precedes(
    incoming: &SnapshotMeta<u64, BasicNode>,
    current: &SnapshotMeta<u64, BasicNode>,
) -> bool {
    (incoming.last_log_id, incoming.last_membership.log_id())
        < (current.last_log_id, current.last_membership.log_id())
}

/// Validate the complete outer record without allocating its byte-vector payload.
/// This establishes framing, not a checksum or semantic bit-rot guarantee.
fn validate_snapshot_record(
    reader: &mut std::io::BufReader<&std::fs::File>,
    file_len: u64,
) -> std::io::Result<()> {
    let payload_len: u64 = bincode::deserialize_from(&mut *reader)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let prefix_len = std::io::Seek::stream_position(reader)?;
    if prefix_len.checked_add(payload_len) != Some(file_len) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "persisted snapshot payload length does not match the complete record",
        ));
    }
    let copied = std::io::copy(
        &mut std::io::Read::take(&mut *reader, payload_len),
        &mut std::io::sink(),
    )?;
    if copied != payload_len || !std::io::BufRead::fill_buf(reader)?.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "persisted snapshot record is incomplete or has trailing bytes",
        ));
    }
    Ok(())
}

/// Keep the coordination inode after unlocking: another process may hold it.
struct SnapshotWriterLock(std::fs::File);

impl Drop for SnapshotWriterLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Remove only this attempt's uniquely owned staging file on failure or unwind.
struct SnapshotStagingFile(PathBuf);

impl Drop for SnapshotStagingFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

// ---------------------------------------------------------------------------
// FerrosStateMachine
// ---------------------------------------------------------------------------

/// W1.21 / I-19: errors that can prevent a fail-loud recovery from
/// silently downgrading a joint or mismatched membership configuration.
///
/// Constructed by [`FerrosStateMachine::try_recover_membership_from_topology_state`]
/// when synthesizing a membership from `state.members` would lose
/// information that the actual log entry encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryError {
    /// The last committed Membership log entry was a joint config
    /// (mid-transition between two voter sets), but the snapshot was
    /// older than that log entry. Synthesizing a single-config Membership
    /// from `state.members` would silently drop the joint transition,
    /// risking a split-brain during the next election.
    ///
    /// Resolution: replay the log Membership entry verbatim instead of
    /// synthesizing.
    JointConfigLost {
        /// Voter sets from the log Membership entry (multiple sets means
        /// joint).
        log_configs: Vec<BTreeSet<u64>>,
        /// What synthesis would have produced from `state.members`.
        synthesized_voters: BTreeSet<u64>,
    },
    /// The last committed Membership log entry was a single-config that
    /// disagrees with the voter set derived from `state.members`. This
    /// indicates state.members has drifted from the consensus view —
    /// likely a sign of a separate bug (silent JoinNode/LeaveNode without
    /// the corresponding Membership change).
    SingleConfigMismatch {
        log_voters: BTreeSet<u64>,
        synthesized_voters: BTreeSet<u64>,
    },
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JointConfigLost {
                log_configs,
                synthesized_voters,
            } => write!(
                f,
                "JointConfigLost: log Membership had {} configs ({:?}); state.members synthesized {:?}",
                log_configs.len(),
                log_configs,
                synthesized_voters
            ),
            Self::SingleConfigMismatch {
                log_voters,
                synthesized_voters,
            } => write!(
                f,
                "SingleConfigMismatch: log voters {:?} != state.members voters {:?}",
                log_voters, synthesized_voters
            ),
        }
    }
}

impl std::error::Error for RecoveryError {}

/// The current-snapshot slot one live state machine shares with its builders.
type SharedSnapshotSlot = Arc<parking_lot::RwLock<Option<(SnapshotMeta<u64, BasicNode>, Vec<u8>)>>>;

struct CompletedApply {
    state: RaftState,
    responses: Vec<RaftResponse>,
}

struct PendingApply {
    log_id: LogId<u64>,
    job: tokio::task::JoinHandle<Result<CompletedApply, ferrosa_common::Error>>,
    // Both the queued worker and its owner retain these until physical work
    // and the existing ordered system-table writes have completed.
    pauses: Arc<Vec<ferrosa_storage::compaction::TableCompactionPause>>,
}

/// Openraft state machine for Ferrosa.
///
/// Applies [`RaftCommand`] entries to [`RaftState`] and optionally propagates
/// side effects to a local [`Schema`] (DDL) and [`StorageEngine`]
/// (table registration).
pub struct FerrosStateMachine {
    state: RaftState,
    physical_lifecycle: Arc<super::physical_apply::PhysicalApplyLifecycle>,
    // RAII owner token is absent from staged copies and snapshot builders.
    _physical_owner: Option<super::physical_apply::PhysicalApplyOwner>,
    pending_apply: Option<PendingApply>,
    /// First caller-owned committed index not yet transferred to a retained job.
    /// Cancellation must not allow a later apply to silently skip this suffix.
    unconsumed_input: Option<u64>,
    apply_failure: Option<String>,
    #[cfg(test)]
    projection_syncs: Arc<std::sync::atomic::AtomicU64>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// Current snapshot data + metadata, served by `get_current_snapshot`.
    ///
    /// Shared with every snapshot builder this machine hands out (outage
    /// 2026-10-10, F1/D1). openraft builds snapshots on the builder in a
    /// detached task, then serves followers from the LIVE machine's
    /// `get_current_snapshot`. When the builder cached its result on a private
    /// clone, the live machine kept offering an older snapshot while the log
    /// store purged past it, and a lagging follower could never catch up.
    /// Written only through [`Self::publish_snapshot`], which never moves the
    /// slot backwards.
    current_snapshot: SharedSnapshotSlot,
    /// Optional local schema for DDL side effects.
    schema: Option<Arc<Schema>>,
    /// Optional local storage engine for table registration side effects.
    engine: Option<Arc<StorageEngine>>,
    /// Optional live token ring — updated after topology commands
    /// (`JoinNode`, `LeaveNode`, `AssignTokens`).
    ring: Option<Arc<ArcSwap<TokenRing>>>,
    /// Optional observer used by HTTP/CLI surfaces that expose a ring snapshot.
    ring_observer: Option<Arc<ArcSwap<Option<Arc<TokenRing>>>>>,
    /// Optional system table writer for persisting DDL/auth mutations.
    system_writer: Option<SystemTableWriter>,
    /// Optional on-disk snapshot file for restart recovery.
    snapshot_path: Option<PathBuf>,
    /// Shared with the log store: the highest log index this state machine has
    /// durably persisted. The log store will not purge past it.
    ///
    /// Raised only after `persist_snapshot_to_disk` returns, which now means
    /// fsynced. Raising it earlier would restore the exact window that
    /// stranded node3 -- a purge point that outran the snapshot backing it.
    durable_applied: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl FerrosStateMachine {
    pub(crate) fn physical_lifecycle(&self) -> Arc<super::physical_apply::PhysicalApplyLifecycle> {
        Arc::clone(&self.physical_lifecycle)
    }

    fn physical_admission(
        &self,
    ) -> Result<Arc<super::physical_apply::PhysicalApplyLease>, StorageIOError<u64>> {
        self.physical_lifecycle.admit().map_err(|error| {
            StorageIOError::write_state_machine(to_any_error(std::io::Error::other(error)))
        })
    }

    /// The synchronous transition runs against a staged metadata copy. A
    /// detached waiter leaves the owned handle here, so the next trait call
    /// reaps it before exposing an applied index or constructing a snapshot.
    fn staged_apply(&self) -> Self {
        Self {
            state: self.state.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            current_snapshot: self.current_snapshot.clone(),
            schema: self.schema.clone(),
            engine: self.engine.clone(),
            ring: self.ring.clone(),
            ring_observer: self.ring_observer.clone(),
            system_writer: self.system_writer.clone(),
            snapshot_path: self.snapshot_path.clone(),
            durable_applied: self.durable_applied.clone(),
            pending_apply: None,
            unconsumed_input: None,
            physical_lifecycle: Arc::clone(&self.physical_lifecycle),
            _physical_owner: None,
            apply_failure: self.apply_failure.clone(),
            #[cfg(test)]
            projection_syncs: Arc::clone(&self.projection_syncs),
        }
    }

    async fn finish_pending_apply(
        &mut self,
    ) -> Result<Option<Vec<RaftResponse>>, Box<StorageError<u64>>> {
        if let Some(reason) = &self.apply_failure {
            return Err(Box::new(
                StorageIOError::write_state_machine(to_any_error(std::io::Error::other(
                    reason.clone(),
                )))
                .into(),
            ));
        }
        let Some(pending) = self.pending_apply.as_mut() else {
            return Ok(None);
        };
        // Poll the retained handle, not a temporary that cancellation can lose.
        let result = (&mut pending.job).await;
        let pending = self
            .pending_apply
            .take()
            .expect("pending handle was retained");
        let completed = match result {
            Ok(Ok(completed)) => completed,
            Ok(Err(error)) => {
                let reason =
                    format!("Raft local apply or system projection persistence failed: {error}");
                self.apply_failure = Some(reason.clone());
                self.physical_lifecycle.fail(&reason);
                return Err(Box::new(
                    StorageIOError::write_state_machine(to_any_error(std::io::Error::other(
                        reason,
                    )))
                    .into(),
                ));
            }
            Err(error) => {
                let reason = format!("Raft physical apply job failed: {error}");
                self.apply_failure = Some(reason.clone());
                self.physical_lifecycle.fail(&reason);
                return Err(Box::new(
                    StorageIOError::write_state_machine(to_any_error(std::io::Error::other(
                        reason,
                    )))
                    .into(),
                ));
            }
        };
        self.state = completed.state;
        self.last_applied = Some(pending.log_id);
        self.physical_lifecycle.applied(self.last_applied);
        drop(pending.pauses);
        Ok(Some(completed.responses))
    }

    /// Publish `meta`/`bytes` as the snapshot replication serves, unless the
    /// slot already holds a newer one. Returns whether the slot changed.
    ///
    /// Monotonic by `last_log_id` (D1): a builder that started before an
    /// install and finishes after it must not roll the slot back, or the log
    /// store (which purges up to `durable_applied`) could outrun the snapshot
    /// a lagging follower is offered. Equal ids replace, so a membership
    /// refresh at the same index takes effect.
    fn publish_snapshot(&self, meta: SnapshotMeta<u64, BasicNode>, bytes: Vec<u8>) -> bool {
        let mut slot = self.current_snapshot.write();
        if let Some((current, _)) = slot.as_ref() {
            if snapshot_precedes(&meta, current) {
                tracing::info!(
                    stale = ?meta.last_log_id,
                    current = ?current.last_log_id,
                    "raft snapshot not published: a newer snapshot already serves replication"
                );
                return false;
            }
        }
        *slot = Some((meta, bytes));
        true
    }

    fn current_snapshot_cloned(&self) -> Option<(SnapshotMeta<u64, BasicNode>, Vec<u8>)> {
        self.current_snapshot.read().clone()
    }

    /// Re-publish the cached snapshot with recovered membership.
    ///
    /// The snapshot is rebuilt from the LIVE state at `last_applied`, never by
    /// re-labelling the cached bytes (t_919bb710). openraft treats a snapshot at
    /// `last_log_id` as the state after every entry up to it; the old code
    /// stamped the cached (older) state with the newer `last_applied`, so a
    /// follower installing it skipped the entries in between and diverged
    /// silently. Live state, `last_applied` and membership are mutually
    /// consistent, so the rebuilt snapshot is too.
    fn refresh_current_snapshot_membership(&mut self) {
        if self.current_snapshot.read().is_none() {
            return;
        }

        let data = SnapshotData {
            state: self.state.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        };

        let refreshed_bytes = match encode_snapshot(&data) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(%e, "failed to serialize refreshed raft snapshot membership");
                return;
            }
        };

        // New content, new id: a receiver must not treat it as a resend of
        // the cached snapshot.
        let refreshed_meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                self.last_applied.map(|id| id.index).unwrap_or(0),
                uuid::Uuid::new_v4()
            ),
        };
        if !self.publish_snapshot(refreshed_meta.clone(), refreshed_bytes.clone()) {
            return;
        }

        if let Some(path) = self.snapshot_path.as_ref() {
            if let Err(e) = Self::persist_snapshot_to_disk(path, &refreshed_meta, &refreshed_bytes)
            {
                tracing::warn!(%e, "failed to persist refreshed raft snapshot membership");
            }
        }
    }

    /// Create a new state machine with empty state and no side-effect targets.
    pub fn new() -> Self {
        let (physical_lifecycle, physical_owner) =
            super::physical_apply::PhysicalApplyLifecycle::new_owner();
        // Default auto_join=true so initial cluster formation works without
        // explicit approval. Production clusters override via UpdateConfig.
        let mut state = RaftState::default();
        state.config.auto_join = true;
        Self {
            state,
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: Arc::new(parking_lot::RwLock::new(None)),
            schema: None,
            engine: None,
            ring: None,
            ring_observer: None,
            system_writer: None,
            snapshot_path: None,
            durable_applied: None,
            physical_lifecycle,
            _physical_owner: Some(physical_owner),
            pending_apply: None,
            unconsumed_input: None,
            apply_failure: None,
            #[cfg(test)]
            projection_syncs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Share the log store's durable-applied watermark.
    ///
    /// Handed over at construction because openraft owns the log store and the
    /// state machine separately once Raft is running, and the guard only works
    /// if both halves read and write the same cell.
    pub fn set_durable_applied_handle(&mut self, handle: Arc<std::sync::atomic::AtomicU64>) {
        self.durable_applied = Some(handle);
    }

    /// Record that everything up to `log_id` is now durable on disk.
    ///
    /// Monotonic: a stale snapshot completing after a newer one must never
    /// lower the watermark, or the log store would be told it may purge less
    /// than it already has.
    fn mark_durably_applied(&self, log_id: Option<LogId<u64>>) {
        let (Some(handle), Some(log_id)) = (self.durable_applied.as_ref(), log_id) else {
            return;
        };
        handle.fetch_max(log_id.index, std::sync::atomic::Ordering::AcqRel);
    }

    /// This state machine's persisted `last_applied` index, if any.
    ///
    /// Exposed so startup can compare it against the log store's purge point
    /// before openraft does; see `crate::raft::local_state`.
    pub fn last_applied_index(&self) -> Option<u64> {
        self.last_applied.map(|log_id| log_id.index)
    }

    /// Recover `last_applied` from the log store's purge point.
    ///
    /// After an OOM kill, the in-memory `last_applied` is lost (reverts to
    /// `None`), but the log store may have a `last_purged_log_id` — entries
    /// can only be purged after they've been applied and snapshotted. If
    /// `last_applied` is `None` and a purge point exists, set `last_applied`
    /// to the purge point so openraft doesn't try to replay already-purged
    /// entries (which would fail with "expected index [0, N), got [M, N)").
    pub fn recover_from_purge_point(&mut self, purge_point: Option<LogId<u64>>) {
        if self.last_applied.is_none() {
            if let Some(purged) = purge_point {
                tracing::warn!(
                    ?purged,
                    "state machine last_applied was None but log has purged entries; \
                     recovering from purge point"
                );
                self.last_applied = Some(purged);
            }
        }
    }

    /// Recover `last_membership` from the log if it was lost.
    ///
    /// After an OOM kill, `last_membership` reverts to the default (empty).
    /// Without a valid membership, no election can happen and the cluster
    /// stays stuck as Learners. This scans the log for the latest Membership
    /// entry and restores it.
    pub fn recover_membership(&mut self, membership: Option<StoredMembership<u64, BasicNode>>) {
        if self
            .last_membership
            .membership()
            .get_joint_config()
            .iter()
            .all(|c| c.is_empty())
        {
            if let Some(m) = membership {
                tracing::warn!(
                    ?m,
                    "state machine membership was empty; recovering from log"
                );
                self.last_membership = m;
                self.refresh_current_snapshot_membership();
            }
        }
    }

    /// Recover membership from committed topology state when explicit openraft
    /// membership entries are absent but `state.members` still contains the
    /// full voter set.
    pub fn recover_membership_from_topology_state(&mut self) -> bool {
        let membership_empty = self
            .last_membership
            .membership()
            .get_joint_config()
            .iter()
            .all(|c| c.is_empty());
        if !membership_empty || self.state.members.is_empty() {
            return false;
        }

        let voters: BTreeSet<u64> = self.state.members.keys().copied().collect();
        if voters.is_empty() {
            return false;
        }

        self.last_membership =
            StoredMembership::new(self.last_applied, Membership::new(vec![voters], None));
        self.refresh_current_snapshot_membership();
        true
    }

    /// W1.21 / I-19: fail-loud variant of [`Self::recover_membership_from_topology_state`]
    /// that detects the lost-joint-config corner case.
    ///
    /// When given the actual last committed `Membership` from the log
    /// (`log_membership`), this function refuses to silently downgrade a
    /// joint config (e.g. `{old_voters, new_voters}`) into a single-config
    /// synthesized from `state.members`. If the log contained a joint config
    /// at the time of the last apply, that is the only safe membership;
    /// rebuilding a single-config from `state.members` would lose the joint
    /// transition and could cause split-brain on the next election.
    ///
    /// Returns:
    /// - `Ok(true)` — synthesized a single-config from `state.members`
    ///   because there was no membership in the log (or the log_membership
    ///   matched the synthesized one).
    /// - `Ok(false)` — explicit `last_membership` was already set, or
    ///   `state.members` was empty; no recovery needed.
    /// - `Err(RecoveryError::JointConfigLost { .. })` — the snapshot is
    ///   older than the last log Membership entry AND that entry was a
    ///   joint config; recovery would silently drop it.
    pub fn try_recover_membership_from_topology_state(
        &mut self,
        log_membership: Option<&Membership<u64, BasicNode>>,
    ) -> Result<bool, RecoveryError> {
        let membership_empty = self
            .last_membership
            .membership()
            .get_joint_config()
            .iter()
            .all(|c| c.is_empty());
        if !membership_empty || self.state.members.is_empty() {
            return Ok(false);
        }

        let voters: BTreeSet<u64> = self.state.members.keys().copied().collect();
        if voters.is_empty() {
            return Ok(false);
        }

        // If we have an actual log membership, use it as the authoritative
        // truth. A joint config (configs.len() > 1) cannot be reconstructed
        // from the single voter set in state.members.
        if let Some(log_m) = log_membership {
            let log_configs = log_m.get_joint_config();
            if log_configs.len() > 1 {
                return Err(RecoveryError::JointConfigLost {
                    log_configs: log_configs.to_vec(),
                    synthesized_voters: voters,
                });
            }
            // Single-config log entry: must match the synthesized voter set.
            if log_configs.len() == 1 && log_configs[0] != voters {
                return Err(RecoveryError::SingleConfigMismatch {
                    log_voters: log_configs[0].clone(),
                    synthesized_voters: voters,
                });
            }
        }

        self.last_membership =
            StoredMembership::new(self.last_applied, Membership::new(vec![voters], None));
        self.refresh_current_snapshot_membership();
        Ok(true)
    }

    /// Create a new state machine wired to local `Schema` and `StorageEngine`
    /// for side-effect propagation.
    pub fn with_side_effects(schema: Arc<Schema>, engine: Arc<StorageEngine>) -> Self {
        let (physical_lifecycle, physical_owner) =
            super::physical_apply::PhysicalApplyLifecycle::new_owner();
        let system_writer = Some(SystemTableWriter::new(Arc::clone(&engine)));
        Self {
            state: RaftState::default(),
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: Arc::new(parking_lot::RwLock::new(None)),
            schema: Some(schema),
            engine: Some(engine),
            ring: None,
            ring_observer: None,
            system_writer,
            snapshot_path: None,
            durable_applied: None,
            physical_lifecycle,
            _physical_owner: Some(physical_owner),
            pending_apply: None,
            unconsumed_input: None,
            apply_failure: None,
            #[cfg(test)]
            projection_syncs: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Create a state machine with side effects and durable snapshot persistence.
    pub fn with_side_effects_and_snapshot_path(
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
        snapshot_path: PathBuf,
    ) -> Self {
        let mut sm = Self::with_side_effects(schema, engine);
        sm.snapshot_path = Some(snapshot_path);
        sm
    }

    #[cfg(test)]
    fn with_snapshot_path(snapshot_path: PathBuf) -> Self {
        let mut sm = Self::new();
        sm.snapshot_path = Some(snapshot_path);
        sm
    }

    /// Wire a live token ring for topology side effects.
    ///
    /// When set, topology commands (`JoinNode`, `LeaveNode`, `AssignTokens`)
    /// will rebuild the ring from `RaftState` and store it via `ArcSwap`.
    pub fn set_ring(&mut self, ring: Arc<ArcSwap<TokenRing>>) {
        self.ring = Some(ring);
    }

    /// Wire an observer holder for the current ring snapshot.
    ///
    /// The coordinator consumes the non-optional `ring` above. Web/CLI
    /// surfaces use an optional holder because standalone and pair modes have
    /// no cluster ring. Keep the observer updated from the same `sync_ring()`
    /// path so `/api/cluster/ring` reflects committed `AssignTokens` entries
    /// instead of the bootstrap snapshot.
    pub fn set_ring_observer(&mut self, ring: Arc<ArcSwap<Option<Arc<TokenRing>>>>) {
        self.ring_observer = Some(ring);
    }

    /// Seed the state machine with initial cluster topology.
    ///
    /// Called during `transition_to_cluster` so that the state machine's
    /// internal `members` and `token_map` match the initial `TokenRing`.
    /// Without this, the first `sync_ring()` call would rebuild from empty
    /// state, wiping the ring that the coordinator is using.
    pub fn seed_topology(
        &mut self,
        members: BTreeMap<u64, NodeInfo>,
        token_map: BTreeMap<Token, u64>,
    ) {
        self.state.members = members;
        self.state.token_map = token_map;
    }

    /// Read-only access to the current cluster state.
    pub fn state(&self) -> &RaftState {
        &self.state
    }

    pub fn has_topology_state(&self) -> bool {
        !self.state.members.is_empty() || !self.state.token_map.is_empty()
    }

    #[allow(clippy::result_large_err)]
    fn apply_snapshot_data(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        bytes: &[u8],
    ) -> Result<(), StorageIOError<u64>> {
        let _physical = self.physical_admission()?;
        let lifecycle_schema = self.schema.clone();
        let _lifecycle = lifecycle_schema
            .as_ref()
            .map(|schema| schema.lifecycle_write());
        let data: SnapshotData = decode_snapshot(bytes)
            .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;

        let absent = tables_absent_from_snapshot(&self.state.tables, &data.state.tables);
        self.refuse_ambiguous_snapshot_drops(&absent, meta.last_log_id)
            .map_err(|e| *e)?;
        let dropped = tables_dropped_by_snapshot(&self.state.tables, &data.state.tables);
        let previous_state = std::mem::replace(&mut self.state, data.state);
        let previous_applied = self.last_applied;
        let previous_membership = self.last_membership.clone();
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        if let Err(error) = self.sync_schema_and_engine_from_state("Raft snapshot") {
            self.state = previous_state;
            self.last_applied = previous_applied;
            self.last_membership = previous_membership;
            let reason = format!("Raft snapshot local restoration failed: {error}");
            self.apply_failure = Some(reason.clone());
            self.physical_lifecycle.fail(&reason);
            return Err(StorageIOError::read_state_machine(to_any_error(
                std::io::Error::other(reason),
            )));
        }
        self.sync_ring();
        self.unregister_tables_dropped_by_snapshot(&dropped);
        if let Some((_, applied)) = self
            .unconsumed_input
            .zip(self.last_applied)
            .filter(|(required, applied)| applied.index >= *required)
        {
            // An authoritative snapshot can supersede the abandoned prefix,
            // but it cannot authorize skipping its own next committed index.
            self.unconsumed_input = applied.index.checked_add(1);
        }
        Ok(())
    }

    /// Refuse snapshot installs that would otherwise need destructive cleanup
    /// but carry no explicit, identity-scoped drop marker proving the cleanup
    /// is safe.
    ///
    /// The current snapshot format has no such marker, so a data-bearing local
    /// table that is merely absent from an incoming snapshot is ambiguous: it
    /// might be a real compacted `DROP TABLE`, or it might be an old/incomplete
    /// snapshot. Refuse that case before `self.state` is replaced and before
    /// any call to `StorageEngine::unregister_table` can tombstone indexes or
    /// remove SSTable directories.
    fn refuse_ambiguous_snapshot_drops(
        &self,
        absent: &[(String, String)],
        snapshot_last_log_id: Option<LogId<u64>>,
    ) -> Result<(), Box<StorageIOError<u64>>> {
        let Some(engine) = &self.engine else {
            return Ok(());
        };

        for (keyspace, table) in absent {
            let tid = TableId::new(keyspace, table);
            if self
                .table_has_local_artifacts(engine, &tid)
                .map_err(|e| Box::new(StorageIOError::read_state_machine(to_any_error(e))))?
            {
                let message = format!(
                    "refusing Raft snapshot install: table {tid} is absent from snapshot at \
                     last_log_id={snapshot_last_log_id:?}, but local SSTables or persisted index \
                     registrations exist and the snapshot carries no explicit identity-matching \
                     drop marker"
                );
                tracing::error!(
                    table = %tid,
                    ?snapshot_last_log_id,
                    "{message}"
                );
                return Err(Box::new(StorageIOError::read_state_machine(to_any_error(
                    std::io::Error::other(message),
                ))));
            }
        }

        Ok(())
    }

    /// Queue the system_auth.roles row for `name` from the role the node
    /// STORES: the schema registry when one is attached (it is what
    /// authenticates, and what restart replays), else the replicated copy.
    ///
    /// The replicated `state.roles` copy is not authoritative for roles the
    /// registry seeded locally, and a cluster that applied the pre-fix
    /// CreateRole holds a client's default-password copy there (t_d0e5e6e5);
    /// persisting it would put that password back on disk.
    fn queue_stored_role_write(
        &self,
        name: &str,
        pending: &mut Vec<PendingSystemWrite>,
        context: &'static str,
    ) {
        let stored = match &self.schema {
            Some(schema) => schema.snapshot().roles.get(name).cloned(),
            None => self.state.roles.get(name).cloned(),
        };
        match stored {
            Some(role) => pending.push(PendingSystemWrite {
                mutation: SystemTableMutation::RoleCreated(role),
                level: SystemWriteLogLevel::Error,
                context,
            }),
            None => tracing::warn!(
                role = name,
                "Raft apply: role update for a role this node does not store; no system_auth \
                 row written"
            ),
        }
    }

    fn table_has_local_artifacts(
        &self,
        engine: &StorageEngine,
        table_id: &TableId,
    ) -> ferrosa_common::Result<bool> {
        table_has_local_artifacts(engine, table_id)
    }

    /// Unregister tables a snapshot explicitly dropped, exactly as
    /// `RaftOp::DropTable` would have had this node applied it: releases the
    /// table in the engine and deletes its local SSTable directory.
    ///
    /// Current snapshots carry no explicit drop markers, so
    /// `tables_dropped_by_snapshot` returns no candidates and absence alone is
    /// handled by `refuse_ambiguous_snapshot_drops` above.
    fn unregister_tables_dropped_by_snapshot(&self, dropped: &[(String, String)]) {
        let Some(engine) = &self.engine else {
            return;
        };
        for (keyspace, table) in dropped {
            let tid = TableId::new(keyspace, table);
            match engine.unregister_table(&tid) {
                Ok(()) => tracing::info!(
                    table = %tid,
                    "Raft snapshot: unregistered a table dropped while this node was behind"
                ),
                Err(e) => tracing::error!(
                    %e, table = %tid,
                    "Raft snapshot: unregister of a dropped table failed — its local SSTables remain \
                     and a recreated table of the same name would read them"
                ),
            }
        }
    }

    fn sync_schema_and_engine_from_state(&self, context: &'static str) -> Result<(), String> {
        let _lifecycle = self.schema.as_ref().map(|schema| schema.lifecycle_write());
        // A same-name table with a different UUID owns none of the local old
        // incarnation's rows. Retire physical state before publishing its identity.
        if let (Some(schema), Some(engine)) = (&self.schema, &self.engine) {
            let local = schema.snapshot();
            let mut retired = Vec::new();
            for (name, incoming) in &self.state.tables {
                if ferrosa_schema::is_system_keyspace(&name.0) {
                    continue;
                }
                let tid = TableId::new(&name.0, &name.1);
                let physical = engine.table_schema(&tid);
                let physical_id = physical
                    .as_ref()
                    .and_then(|table| table.extensions.get(ferrosa_schema::TABLE_ID_EXTENSION))
                    .map(|value| {
                        Uuid::parse_str(value).map_err(|e| {
                            format!("{context}: invalid stored table incarnation for {tid}: {e}")
                        })
                    })
                    .transpose()?;
                let known_id = physical_id.or_else(|| local.tables.get(name).map(|table| table.id));
                match known_id {
                    Some(id) if id != incoming.id => retired.push(tid),
                    None if engine.memtable_size(&tid) > 0
                        || table_has_local_artifacts(engine, &tid).map_err(|e| {
                            format!("{context}: inspect unbound table {tid}: {e}")
                        })? =>
                    {
                        return Err(format!(
                            "{context}: table {tid} has data but no authoritative local incarnation; restore schema provenance before retrying"
                        ));
                    }
                    _ => {}
                }
            }
            // Validate every table before retiring any data.
            for tid in retired {
                engine
                    .unregister_table(&tid)
                    .map_err(|e| format!("{context}: retire superseded table {tid}: {e}"))?;
            }
        }
        // Propagate full state to local Schema if present.
        if let Some(schema) = &self.schema {
            let snap = ferrosa_schema::SchemaSnapshot {
                version: self.state.schema_version,
                keyspaces: self
                    .state
                    .keyspaces
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                tables: self
                    .state
                    .tables
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                roles: self
                    .state
                    .roles
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                grants: self
                    .state
                    .grants
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                indexes: self
                    .state
                    .indexes
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                types: self
                    .state
                    .types
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                functions: self
                    .state
                    .functions
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                aggregates: self
                    .state
                    .aggregates
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            };
            schema
                .apply_snapshot(snap)
                .map_err(|e| format!("{context}: apply schema snapshot: {e}"))?;
        }

        // Land every table's schema on the engine if present.
        //
        // `register_table` early-returns for a table the engine already knows
        // and DISCARDS the incoming schema, so it can introduce a table but
        // never correct one. A node whose engine holds a stale column set —
        // from an older schema.json, or from missing an ALTER — would then keep
        // serving it forever while its own Raft state carried the right one,
        // and since regular columns are ordered alphabetically, a single
        // missing column shifts every later ordinal and makes cross-node row
        // transfers decode against the wrong types.
        //
        // The ALTER apply path already uses `update_table_schema`; use it here
        // too so both paths land a schema the same way.
        if let Some(engine) = &self.engine {
            for table in self.state.tables.values() {
                let schema = table.to_storage_schema();
                let table_id = TableId::new(&schema.keyspace, &schema.table);
                if engine.table_schema(&table_id).is_some() {
                    engine
                        .update_table_schema(&table_id, schema)
                        .map_err(|e| format!("{context}: update table {table_id}: {e}"))?;
                } else {
                    engine
                        .register_table(schema)
                        .map_err(|e| format!("{context}: register table {table_id}: {e}"))?;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn persist_snapshot_to_disk(
        path: &Path,
        meta: &SnapshotMeta<u64, BasicNode>,
        bytes: &[u8],
    ) -> Result<(), StorageIOError<u64>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".writer.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(PathBuf::from(lock_path))
            .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        lock_file
            .lock()
            .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        let _lock = SnapshotWriterLock(lock_file);

        // Compare leading metadata without allocating another snapshot payload.
        // The comparison and promotion share the same per-path lock, including
        // independent state-machine instances and cancelled async waiters.
        match std::fs::File::open(path) {
            Ok(file) => {
                let mut reader = std::io::BufReader::new(&file);
                let current: SnapshotMeta<u64, BasicNode> = bincode::deserialize_from(&mut reader)
                    .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;
                if snapshot_precedes(meta, &current) {
                    let file_len = file
                        .metadata()
                        .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?
                        .len();
                    validate_snapshot_record(&mut reader, file_len)
                        .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;
                    // A prior rename may have returned a directory-sync error.
                    // Establish durability before allowing any caller to advance
                    // its watermark, even when its older candidate is skipped.
                    file.sync_all()
                        .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
                    if let Some(parent) = path.parent() {
                        std::fs::File::open(parent)
                            .and_then(|dir| dir.sync_all())
                            .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
                    }
                    return Ok(());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageIOError::read_state_machine(to_any_error(error)));
            }
        }
        // Durable, not merely written.
        //
        // This used to be write-then-rename with no fsync of either the file
        // or its directory, so the function returned Ok while the snapshot
        // could still be entirely in the page cache. Meanwhile the log store
        // is free to purge the entries this snapshot is supposed to replace.
        // A crash in that window leaves a node whose log has been truncated
        // past a snapshot that never reached the disk -- purged to 3065,
        // applied to 2905, and the entries in between gone. That is how node3
        // was stranded on 2026-08-20.
        //
        // Returning Ok for a write that may not survive a crash is the "never
        // fake success" failure, and the caller cannot tell the difference.
        let mut staging_name = path.file_name().unwrap_or_default().to_os_string();
        staging_name.push(format!(".{}.tmp", Uuid::new_v4()));
        let tmp = path.with_file_name(staging_name);
        {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
            let _staging = SnapshotStagingFile(tmp.clone());
            {
                // Borrowed fields serialize identically to PersistedSnapshot;
                // stream directly to the file instead of copying its payload.
                #[derive(Serialize)]
                struct SnapshotOnDisk<'a> {
                    meta: &'a SnapshotMeta<u64, BasicNode>,
                    bytes: &'a [u8],
                }
                let mut writer = std::io::BufWriter::new(&file);
                bincode::serialize_into(&mut writer, &SnapshotOnDisk { meta, bytes })
                    .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
                std::io::Write::flush(&mut writer)
                    .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
            }
            file.sync_all()
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
            std::fs::rename(&tmp, path)
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        }

        // fsync the directory too, or the rename itself can be lost while the
        // file contents survive -- leaving the snapshot under its temp name
        // and the node with no snapshot at all.
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)
                .and_then(|dir| dir.sync_all())
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub fn recover_from_persisted_snapshot(&mut self) -> Result<bool, StorageIOError<u64>> {
        let result = (|| {
            let Some(path) = self.snapshot_path.clone() else {
                return Ok(false);
            };
            if !path.exists() {
                return Ok(false);
            }

            let bytes = std::fs::read(&path)
                .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;
            let persisted: PersistedSnapshot = bincode::deserialize(&bytes)
                .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;
            self.apply_snapshot_data(&persisted.meta, &persisted.bytes)?;
            self.publish_snapshot(persisted.meta, persisted.bytes);
            self.physical_lifecycle.applied(self.last_applied);
            Ok(true)
        })();
        if let Err(error) = &result {
            self.physical_lifecycle.fail(error);
        }
        result
    }

    /// Rebuild the live `TokenRing` from current `RaftState` and store it.
    ///
    /// Called after every topology-changing command so that the
    /// `ArcSwap<TokenRing>` always reflects the committed Raft state.
    fn sync_ring(&self) {
        if self.ring.is_some() || self.ring_observer.is_some() {
            let mut ring = TokenRing::new();

            // Populate nodes.
            for (&node_id, info) in &self.state.members {
                ring.add_node(node_id, info.clone());
            }

            // Populate token assignments.
            for (&token, &node_id) in &self.state.token_map {
                ring.assign_tokens(node_id, &[token]);
            }
            ring.set_bootstrap_complete(self.state.bootstrap_complete.keys().copied().collect());

            let ring = Arc::new(ring);
            if let Some(ring_swap) = &self.ring {
                ring_swap.store(Arc::clone(&ring));
            }
            if let Some(observer) = &self.ring_observer {
                observer.store(Arc::new(Some(ring)));
            }
        }
    }

    pub fn sync_live_ring_from_state(&self) {
        self.sync_ring();
    }

    // -----------------------------------------------------------------
    // W7.1–W7.5 — Multi-DC Accord apply path.
    // -----------------------------------------------------------------

    /// Apply path for [`RaftOp::AccordApply`] (W7.2). Buffers the entry
    /// by HLC timestamp, records the max-observed-skew metric, and
    /// drains everything at-or-below the current watermark.
    ///
    /// Idempotent on `txn_id` (W7.5 / I-28): replayed transactions are
    /// short-circuited at the ledger before they enter the buffer.
    fn apply_accord_marked(&mut self, txn_id: TxnId, hlc: AccordTimestamp, mutation: Vec<u8>) {
        // I-28: short-circuit replays before buffering.
        if self.state.applied_accord_txns.contains(&txn_id) {
            tracing::debug!(?txn_id, "AccordApply replay deduped at ledger");
            return;
        }

        // Track the max skew observed against the current watermark
        // (W7.1 REFACTOR / RAFT_ACCORD_MAX_SKEW gauge). `time` is HLC
        // nanoseconds; the gauge is microseconds (t_2b57a6e4).
        let skew_us = hlc.time.saturating_sub(self.state.hlc_watermark.time) / 1_000;
        if skew_us > self.state.max_observed_skew_us {
            self.state.max_observed_skew_us = skew_us;
        }

        let op = RaftOp::AccordApply {
            txn_id,
            hlc,
            mutation,
        };
        self.state.accord_apply_buffer.push(hlc, op);

        // Try to drain anything below the current watermark — entries
        // newly admitted whose HLC is below the watermark should fire
        // immediately.
        self.drain_ready_accord_entries();
    }

    /// Drain Accord-marked entries whose HLC is at-or-below the
    /// watermark, in HLC order. Records each in the idempotent ledger.
    fn drain_ready_accord_entries(&mut self) {
        let watermark = self.state.hlc_watermark;
        let ready = self.state.accord_apply_buffer.drain_ready(watermark);
        for op in ready {
            if let RaftOp::AccordApply { txn_id, hlc, .. } = op {
                // I-28: record the apply for dedupe. The mutation
                // payload is dispatched to higher layers in later
                // sprints — for now the ledger entry is the durable
                // marker that this txn has been applied.
                self.state.applied_accord_txns.record(txn_id, hlc);
            }
        }
    }

    /// Advance the HLC watermark to `new_watermark` (or hold it if the
    /// proposed value would regress) and drain any newly-eligible
    /// entries from the reorder buffer (W7.1 / W7.3).
    ///
    /// Called by the heartbeat tick in production with
    /// `now - max_skew`; tests advance it explicitly to exercise the
    /// drain semantics.
    pub fn advance_accord_watermark(&mut self, new_watermark: AccordTimestamp) {
        if new_watermark > self.state.hlc_watermark {
            self.state.hlc_watermark = new_watermark;
        }
        self.drain_ready_accord_entries();
    }

    /// Borrow the reorder buffer (test/operator inspection).
    pub fn accord_apply_buffer(&self) -> &ReorderBuffer {
        &self.state.accord_apply_buffer
    }

    /// Apply a single [`RaftCommand`] to `self.state`, updating BTreeMaps
    /// and optionally propagating side effects.
    /// Apply the in-memory state mutation for `cmd` and collect any
    /// system-table writes to be executed off the raft worker by the caller.
    ///
    /// Returns the `RaftResponse` plus the deferred `system_schema.*`/
    /// `system_auth.*` writes; the caller drains them via `spawn_blocking`.
    fn apply_command(
        &mut self,
        cmd: RaftCommand,
    ) -> ferrosa_common::Result<(RaftResponse, Vec<PendingSystemWrite>)> {
        let RaftCommand { op, schema_version } = cmd;
        let mut schema_changed = true;
        let mut apply_errors: Vec<ApplyError> = Vec::new();
        let mut command_rejection = None;
        let mut pending_system_writes: Vec<PendingSystemWrite> = Vec::new();
        match op {
            // ---- DDL: Keyspaces ----------------------------------------
            RaftOp::CreateKeyspace(ks) => {
                let inserted = if self.state.keyspaces.contains_key(&ks.name) {
                    false
                } else {
                    self.state.keyspaces.insert(ks.name.clone(), ks.clone());
                    true
                };
                schema_changed = inserted;
                // Always reconcile the externally shared `Schema` with the
                // committed Raft state — even when `self.state` already had the
                // keyspace. Fresh CQL connections read the keyspace exclusively
                // from this shared `Schema` (system_schema.keyspaces, USE,
                // keyspace validation). Gating this behind `inserted` left the
                // keyspace permanently invisible to new connections whenever
                // `self.state` and the shared `Schema` had drifted (duplicate
                // proposal replay, or recovery that repopulated `self.state`
                // from a persisted snapshot against a fresh `Schema`). See
                // forge t_86f9259d. `create_keyspace_internal` is idempotent.
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_keyspace_internal(ks.clone()) {
                        tracing::error!(%e, keyspace = %ks.name, "Raft apply: create_keyspace_internal failed — shared schema diverged from Raft state");
                        apply_errors.push(ApplyError::Other(format!(
                            "create_keyspace_internal({}) failed: {e}",
                            ks.name
                        )));
                    }
                }
                if inserted && self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::KeyspaceCreated(ks),
                        level: SystemWriteLogLevel::WarnReplay,
                        context: "Raft apply: system table write skipped for CreateKeyspace (expected during log replay)",
                    });
                }
            }
            RaftOp::DropKeyspace(name) => {
                let lifecycle_schema = self.schema.clone();
                let _lifecycle = lifecycle_schema
                    .as_ref()
                    .map(|schema| schema.lifecycle_write());
                self.state.keyspaces.remove(&name);
                // Collect tables to drop for engine unregistration.
                let dropped_tables: Vec<(String, String)> = self
                    .state
                    .tables
                    .keys()
                    .filter(|(ks, _)| ks == &name)
                    .cloned()
                    .collect();
                self.state.tables.retain(|(ks, _), _| ks != &name);
                // Also drop indexes in this keyspace.
                self.state.indexes.retain(|(ks, _, _), _| ks != &name);
                // Also drop types in this keyspace.
                self.state.types.retain(|(ks, _), _| ks != &name);
                // Also drop functions and aggregates in this keyspace.
                self.state.functions.retain(|(ks, _, _), _| ks != &name);
                self.state.aggregates.retain(|(ks, _, _), _| ks != &name);
                self.state
                    .index_state_map
                    .retain(|(ks, _, _), _| ks != &name);
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_keyspace_internal(&name) {
                        tracing::error!(%e, "Raft apply: drop_keyspace_internal failed — schema diverged from Raft state");
                        apply_errors.push(ApplyError::Other(format!(
                            "drop_keyspace_internal failed — schema diverged from Raft state: {e}"
                        )));
                    }
                }
                if let Some(engine) = &self.engine {
                    for (ks, tbl) in dropped_tables {
                        let tid = TableId::new(&ks, &tbl);
                        if let Err(e) = engine.unregister_table(&tid) {
                            tracing::error!(%e, "Raft apply: unregister_table failed");
                            apply_errors.push(ApplyError::EngineUnregisterTable {
                                keyspace: ks,
                                table: tbl,
                                reason: e.to_string(),
                            });
                        }
                    }
                }
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::KeyspaceDropped(name.clone()),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
            }
            RaftOp::AlterKeyspace { name, updates } => {
                if let Some(ks) = self.state.keyspaces.get_mut(&name) {
                    if let Some(replication) = &updates.replication {
                        ks.replication = replication.clone();
                    }
                    if let Some(durable_writes) = updates.durable_writes {
                        ks.durable_writes = durable_writes;
                    }
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.alter_keyspace_internal(&name, updates) {
                        tracing::error!(%e, "Raft apply: alter_keyspace_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "alter_keyspace_internal failed: {e}"
                        )));
                    }
                }
                if self.system_writer.is_some() {
                    if let Some(ks) = self.state.keyspaces.get(&name) {
                        pending_system_writes.push(PendingSystemWrite {
                            mutation: SystemTableMutation::KeyspaceCreated(ks.clone()),
                            level: SystemWriteLogLevel::Error,
                            context: "Raft apply: system table write failed for AlterKeyspace",
                        });
                    }
                }
            }

            // ---- DDL: Tables -------------------------------------------
            RaftOp::CreateTable(table) => {
                let lifecycle_schema = self.schema.clone();
                let _lifecycle = lifecycle_schema
                    .as_ref()
                    .map(|schema| schema.lifecycle_write());
                let key = (table.keyspace.clone(), table.name.clone());
                let inserted = if let std::collections::btree_map::Entry::Vacant(entry) =
                    self.state.tables.entry(key)
                {
                    entry.insert(*table.clone());
                    true
                } else {
                    false
                };
                schema_changed = inserted;
                // Always reconcile the shared `Schema` and the storage engine
                // with committed Raft state — same rationale as CreateKeyspace
                // (forge t_86f9259d). Fresh CQL connections resolve the table
                // from the shared `Schema`, and reads/writes need the engine
                // registration; gating these behind `inserted` made the table
                // permanently unreadable to new connections after any state /
                // schema drift. Both `create_table_internal` and
                // `register_table` are idempotent.
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_table_internal(*table.clone()) {
                        tracing::error!(%e, keyspace = %table.keyspace, table = %table.name, "Raft apply: create_table_internal failed — shared schema diverged");
                        apply_errors.push(ApplyError::Other(format!(
                            "create_table_internal({}.{}) failed: {e}",
                            table.keyspace, table.name
                        )));
                    }
                }
                if let Some(engine) = &self.engine {
                    if let Err(e) = engine.register_table(table.to_storage_schema()) {
                        tracing::error!(%e, "Raft apply: register_table failed — writes to this table will silently fail");
                        apply_errors.push(ApplyError::EngineRegisterTable {
                            keyspace: table.keyspace.clone(),
                            table: table.name.clone(),
                            reason: e.to_string(),
                        });
                    }
                }
                if inserted && self.system_writer.is_some() {
                    // Warn, not error: during Raft log replay on startup,
                    // system_schema tables may not be registered yet.  The
                    // schema bootstrap populates them once loading completes.
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::TableCreated(table),
                        level: SystemWriteLogLevel::WarnReplay,
                        context: "Raft apply: system table write skipped for CreateTable (expected during log replay)",
                    });
                }
            }
            RaftOp::DropTable { keyspace, table } => {
                let lifecycle_schema = self.schema.clone();
                let _lifecycle = lifecycle_schema
                    .as_ref()
                    .map(|schema| schema.lifecycle_write());
                self.state.tables.remove(&(keyspace.clone(), table.clone()));
                // Also drop indexes on this table.
                self.state
                    .indexes
                    .retain(|(ks, tbl, _), _| !(ks == &keyspace && tbl == &table));
                self.state
                    .index_state_map
                    .retain(|(ks, tbl, _), _| !(ks == &keyspace && tbl == &table));
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_table_internal(&keyspace, &table) {
                        tracing::error!(%e, "Raft apply: drop_table_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "drop_table_internal failed: {e}"
                        )));
                    }
                }
                if let Some(engine) = &self.engine {
                    let tid = TableId::new(&keyspace, &table);
                    if let Err(e) = engine.unregister_table(&tid) {
                        tracing::error!(%e, "Raft apply: unregister_table failed — stale table data may remain");
                        apply_errors.push(ApplyError::EngineUnregisterTable {
                            keyspace: keyspace.clone(),
                            table: table.clone(),
                            reason: e.to_string(),
                        });
                    }
                }
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::TableDropped {
                            keyspace: keyspace.clone(),
                            table: table.clone(),
                        },
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed for DropTable",
                    });
                }
            }
            RaftOp::AlterTable {
                keyspace,
                table,
                updates,
            } => {
                let new_storage_schema = {
                    let tbl = self
                        .state
                        .tables
                        .get_mut(&(keyspace.clone(), table.clone()));
                    if let Some(tbl) = tbl {
                        if let Some(params) = &updates.params {
                            tbl.params = params.clone();
                        }
                        for col in &updates.add_columns {
                            tbl.columns.insert(col.name.clone(), col.clone());
                        }
                        for col_name in &updates.drop_columns {
                            tbl.columns.shift_remove(col_name);
                        }
                        if let Some(extensions) = &updates.extensions {
                            for (k, v) in extensions {
                                tbl.extensions.insert(k.clone(), v.clone());
                            }
                        }
                        Some(tbl.to_storage_schema())
                    } else {
                        None
                    }
                };
                if let Some(schema) = &self.schema {
                    // The registry's internal ALTER is an idempotent no-op for
                    // a missing table. That is not success when committed Raft
                    // metadata still requires this table to exist locally.
                    if new_storage_schema.is_some()
                        && !schema
                            .snapshot()
                            .tables
                            .contains_key(&(keyspace.clone(), table.clone()))
                    {
                        return Err(ferrosa_common::Error::Io(std::io::Error::other(format!(
                            "committed table {keyspace}.{table} is missing from the shared schema during ALTER"
                        ))));
                    }
                    if let Err(e) = schema.alter_table_internal(&keyspace, &table, *updates) {
                        tracing::error!(%e, "Raft apply: schema.alter_table_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.alter_table_internal failed: {e}"
                        )));
                    }
                }
                // Propagate the post-ALTER column set into the storage engine
                // so subsequent flushes build the SSTable SerializationHeader
                // with the correct num_columns. Without this, writes carrying
                // newly-added column indices flush through a stale header
                // whose regular_columns.len() is too small, and the writer's
                // out-of-range-col_idx assertion would fire
                // (bug-sstable-writer-produces-zero-byte-rows-db.md).
                if let (Some(engine), Some(new_schema)) = (&self.engine, new_storage_schema) {
                    let tid = TableId::new(&keyspace, &table);
                    if let Err(e) = engine.update_table_schema(&tid, new_schema) {
                        tracing::error!(%e, "Raft apply: update_table_schema failed — future flushes may be corrupt");
                        apply_errors.push(ApplyError::Other(format!(
                            "update_table_schema failed — future flushes may be corrupt: {e}"
                        )));
                    }
                }
            }

            // ---- DDL: Indexes ------------------------------------------
            RaftOp::CreateIndex(index) => {
                let key = (
                    index.keyspace.clone(),
                    index.table.clone(),
                    index.name.clone(),
                );
                self.state
                    .indexes
                    .entry(key)
                    .or_insert_with(|| index.clone());
                // Initialize empty per-node status map for this index.
                let state_key = (
                    index.keyspace.clone(),
                    index.table.clone(),
                    index.name.clone(),
                );
                self.state
                    .index_state_map
                    .entry(state_key.clone())
                    .or_default();
                // Mark all current cluster members as Building for the new index.
                if let Some(statuses) = self.state.index_state_map.get_mut(&state_key) {
                    for &member_node_id in self.state.members.keys() {
                        statuses
                            .entry(member_node_id)
                            .or_insert(IndexNodeStatus::Building);
                    }
                }
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::IndexCreated(index.clone()),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                // Build it here too. The schema half above is what makes the
                // planner select this index on THIS node; without the engine
                // half the node holds an index it cannot answer from, and a
                // global index read fans out to every replica. Mirrors
                // `RaftOp::DropIndex` below, which has always called
                // `engine.drop_index` and pushed an `ApplyError` on failure.
                if let Some(engine) = &self.engine {
                    let partition_key = self
                        .state
                        .tables
                        .get(&(index.keyspace.clone(), index.table.clone()))
                        .map(|t| t.partition_key.clone())
                        .unwrap_or_default();
                    if let Err(e) = crate::ddl_path::build_replicated_index(
                        engine,
                        &index,
                        &partition_key,
                        "replicated DDL (raft apply)",
                    ) {
                        tracing::error!(
                            %e,
                            index = %index.name,
                            "Raft apply: building the replicated index failed — this node \
                             will refuse or under-answer reads the planner routes to it"
                        );
                        apply_errors.push(ApplyError::Other(format!(
                            "build_replicated_index({}.{}.{}) failed: {e}",
                            index.keyspace, index.table, index.name
                        )));
                    }
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_index_internal(index.clone()) {
                        tracing::error!(%e, "Raft apply: schema.create_index_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.create_index_internal failed: {e}"
                        )));
                    }
                }
            }
            RaftOp::DropIndex {
                keyspace,
                table,
                index,
            } => {
                self.state
                    .indexes
                    .remove(&(keyspace.clone(), table.clone(), index.clone()));
                // Clean up per-node build status.
                self.state.index_state_map.remove(&(
                    keyspace.clone(),
                    table.clone(),
                    index.clone(),
                ));
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::IndexDropped {
                            keyspace: keyspace.clone(),
                            table: table.clone(),
                            name: index.clone(),
                        },
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_index_internal(&keyspace, &table, &index) {
                        tracing::error!(%e, "Raft apply: schema.drop_index_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.drop_index_internal failed: {e}"
                        )));
                    }
                }
                if let Some(engine) = &self.engine {
                    if let Err(e) = engine.drop_index(&TableId::new(&keyspace, &table), &index) {
                        tracing::error!(%e, "Raft apply: engine.drop_index failed — stale index state may remain");
                        apply_errors.push(ApplyError::Other(format!(
                            "engine.drop_index({keyspace}.{table}.{index}) failed: {e}"
                        )));
                    }
                }
            }
            RaftOp::IndexStatus {
                node_id,
                keyspace,
                table,
                index_name,
                status,
            } => {
                schema_changed = false;
                let key = (keyspace, table, index_name);
                self.state
                    .index_state_map
                    .entry(key)
                    .or_default()
                    .insert(node_id, status);
            }

            // ---- DDL: User-Defined Types -------------------------------
            RaftOp::CreateType(udt) => {
                let key = (udt.keyspace.clone(), udt.name.clone());
                self.state.types.entry(key).or_insert_with(|| udt.clone());
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::TypeCreated(udt.clone()),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_type_internal(&udt) {
                        tracing::error!(%e, "Raft apply: schema.create_type_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.create_type_internal failed: {e}"
                        )));
                    }
                }
            }
            RaftOp::DropType { keyspace, name } => {
                self.state.types.remove(&(keyspace.clone(), name.clone()));
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::TypeDropped {
                            keyspace: keyspace.clone(),
                            name: name.clone(),
                        },
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_type_internal(&keyspace, &name) {
                        tracing::error!(%e, "Raft apply: schema.drop_type_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.drop_type_internal failed: {e}"
                        )));
                    }
                }
            }

            // ---- DDL: User-Defined Functions ---------------------------
            RaftOp::CreateFunction(func) => {
                let key = (
                    func.keyspace.clone(),
                    func.name.clone(),
                    func.arg_types.clone(),
                );
                self.state
                    .functions
                    .entry(key)
                    .or_insert_with(|| func.clone());
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::FunctionCreated(func.clone()),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_function_internal(&func) {
                        tracing::error!(%e, "Raft apply: schema.create_function_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.create_function_internal failed: {e}"
                        )));
                    }
                }
            }
            RaftOp::DropFunction {
                keyspace,
                name,
                arg_types,
            } => {
                let key = (keyspace.clone(), name.clone(), arg_types.clone());
                self.state.functions.remove(&key);
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::FunctionDropped {
                            keyspace: keyspace.clone(),
                            name: name.clone(),
                            arg_types: arg_types.clone(),
                        },
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_function_internal(&keyspace, &name, &arg_types) {
                        tracing::error!(%e, "Raft apply: schema.drop_function_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.drop_function_internal failed: {e}"
                        )));
                    }
                }
            }

            // ---- DDL: User-Defined Aggregates --------------------------
            RaftOp::CreateAggregate(agg) => {
                let key = (
                    agg.keyspace.clone(),
                    agg.name.clone(),
                    agg.arg_types.clone(),
                );
                self.state
                    .aggregates
                    .entry(key)
                    .or_insert_with(|| agg.clone());
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_aggregate_internal(&agg) {
                        tracing::error!(%e, "Raft apply: schema.create_aggregate_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.create_aggregate_internal failed: {e}"
                        )));
                    }
                }
            }
            RaftOp::DropAggregate {
                keyspace,
                name,
                arg_types,
            } => {
                let key = (keyspace.clone(), name.clone(), arg_types.clone());
                self.state.aggregates.remove(&key);
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_aggregate_internal(&keyspace, &name, &arg_types) {
                        tracing::error!(%e, "Raft apply: schema.drop_aggregate_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.drop_aggregate_internal failed: {e}"
                        )));
                    }
                }
            }

            // ---- DDL: Roles & Grants -----------------------------------
            RaftOp::CreateRole(role) => {
                let name = role.name.clone();
                let inserted_in_state = !self.state.roles.contains_key(&name);
                if inserted_in_state {
                    self.state.roles.insert(name.clone(), role.clone());
                }
                // The registry is the authority for whether the role is new:
                // seeded roles (ferrosa_admin, graph_engine, ferrosa_user) live
                // only there, never in the replicated `state.roles`.
                let inserted = match &self.schema {
                    Some(schema) => match schema.insert_role_if_absent(role) {
                        Ok(inserted) => inserted,
                        Err(e) => {
                            tracing::error!(%e, role = %name, "Raft apply: schema.insert_role_if_absent failed");
                            apply_errors.push(ApplyError::Other(format!(
                                "schema.insert_role_if_absent failed: {e}"
                            )));
                            false
                        }
                    },
                    None => inserted_in_state,
                };
                if !inserted {
                    // Persisting the incoming role here wrote the client's
                    // password over a seeded one on disk only; restart then
                    // replayed it (t_d0e5e6e5). The stored role is unchanged,
                    // so its row (if any) is already correct.
                    tracing::warn!(
                        role = %name,
                        "Raft apply: CreateRole for a role that already exists; kept the stored \
                         role and persisted nothing"
                    );
                } else if self.system_writer.is_some() {
                    self.queue_stored_role_write(
                        &name,
                        &mut pending_system_writes,
                        "Raft apply: system table write failed",
                    );
                }
            }
            RaftOp::AlterRole { name, updates } => {
                if let Some(role) = self.state.roles.get_mut(&name) {
                    if let Some(is_superuser) = updates.is_superuser {
                        role.is_superuser = is_superuser;
                    }
                    if let Some(can_login) = updates.can_login {
                        role.can_login = can_login;
                    }
                    if let Some(ref hash) = updates.password {
                        role.salted_hash = Some(hash.clone());
                    }
                    if let Some(ref member_of) = updates.member_of {
                        role.member_of = member_of.clone();
                    }
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.alter_role_internal(&name, updates) {
                        tracing::error!(%e, "Raft apply: schema.alter_role_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.alter_role_internal failed: {e}"
                        )));
                    }
                }
                // After the registry update, so the row is the altered stored role.
                if self.system_writer.is_some() {
                    self.queue_stored_role_write(
                        &name,
                        &mut pending_system_writes,
                        "Raft apply: system table write failed",
                    );
                }
            }
            RaftOp::DropRole(name) => {
                self.state.roles.remove(&name);
                self.state.grants.remove(&name);
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_role_internal(&name) {
                        tracing::error!(%e, "Raft apply: schema.drop_role_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.drop_role_internal failed: {e}"
                        )));
                    }
                }
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::RoleDropped(name.clone()),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
            }
            RaftOp::Grant(entry) => {
                let grants = self.state.grants.entry(entry.role.clone()).or_default();
                if let Some(existing) = grants.iter_mut().find(|g| g.resource == entry.resource) {
                    existing
                        .permissions
                        .extend(entry.permissions.iter().copied());
                } else {
                    grants.push(entry.clone());
                }
                if self.system_writer.is_some() {
                    let stored = grants
                        .iter()
                        .find(|g| g.resource == entry.resource)
                        .cloned()
                        .ok_or_else(|| {
                            ferrosa_common::Error::InvalidData(
                                "Raft grant post-state is missing its resource binding".into(),
                            )
                        })?;
                    pending_system_writes.push(PendingSystemWrite {
                        // The row contains the entire permission set, not a delta.
                        mutation: SystemTableMutation::GrantUpdated(stored),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.grant_internal(entry) {
                        tracing::error!(%e, "Raft apply: schema.grant_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.grant_internal failed: {e}"
                        )));
                    }
                }
            }
            RaftOp::Revoke {
                role,
                resource,
                permission,
            } => {
                if let Some(grants) = self.state.grants.get_mut(&role) {
                    if let Some(entry) = grants.iter_mut().find(|g| g.resource == resource) {
                        entry.permissions.remove(&permission);
                    }
                    grants.retain(|g| !g.permissions.is_empty());
                    if grants.is_empty() {
                        self.state.grants.remove(&role);
                    }
                }
                if self.system_writer.is_some() {
                    let remaining =
                        self.state.grants.get(&role).and_then(|grants| {
                            grants.iter().find(|g| g.resource == resource).cloned()
                        });
                    // A partial revoke must replace the full permission cell.
                    // Do not tombstone and rewrite: equal timestamps can mask it.
                    let mutation = match remaining {
                        Some(grant) => SystemTableMutation::GrantUpdated(grant),
                        None => SystemTableMutation::PermissionRevoked {
                            role: role.clone(),
                            resource: resource.clone(),
                            permission,
                        },
                    };
                    pending_system_writes.push(PendingSystemWrite {
                        mutation,
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed for PermissionRevoked",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.revoke_internal(&role, &resource, &permission) {
                        tracing::error!(%e, "Raft apply: schema.revoke_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.revoke_internal failed: {e}"
                        )));
                    }
                }
            }

            RaftOp::GrantRole {
                member,
                granted_role,
            } => {
                // Cycle check against the replicated state, which Raft applies
                // serially — this catches a cycle formed by two independently
                // committed grants (GRANT a TO b, GRANT b TO a). If it would
                // cycle, neither the replicated state nor the schema is mutated,
                // so they stay consistent.
                if roles_form_cycle(&self.state.roles, &granted_role, &member) {
                    tracing::warn!(
                        member,
                        granted_role,
                        "Raft apply: grant_role would create a role cycle; skipped"
                    );
                } else {
                    if let Some(role) = self.state.roles.get_mut(&member) {
                        role.member_of.insert(granted_role.clone());
                    }
                    if let Some(schema) = &self.schema {
                        if let Err(e) = schema.grant_role_internal(&member, &granted_role) {
                            tracing::error!(%e, "Raft apply: schema.grant_role_internal failed");
                            apply_errors.push(ApplyError::Other(format!(
                                "schema.grant_role_internal failed: {e}"
                            )));
                        }
                    }
                    if self.system_writer.is_some() {
                        self.queue_stored_role_write(
                            &member,
                            &mut pending_system_writes,
                            "Raft apply: system table write failed for GrantRole",
                        );
                    }
                }
            }
            RaftOp::RevokeRole {
                member,
                granted_role,
            } => {
                if let Some(role) = self.state.roles.get_mut(&member) {
                    role.member_of.remove(&granted_role);
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.revoke_role_internal(&member, &granted_role) {
                        tracing::error!(%e, "Raft apply: schema.revoke_role_internal failed");
                        apply_errors.push(ApplyError::Other(format!(
                            "schema.revoke_role_internal failed: {e}"
                        )));
                    }
                }
                if self.system_writer.is_some() {
                    self.queue_stored_role_write(
                        &member,
                        &mut pending_system_writes,
                        "Raft apply: system table write failed for RevokeRole",
                    );
                }
            }

            // ---- Topology ----------------------------------------------
            RaftOp::JoinNode(node_info) => {
                schema_changed = false;
                // Approval gate: if auto_join is disabled, verify the node was
                // pre-approved via ApproveNode. This check is inside the state
                // machine (not in the caller) to prevent TOCTOU races where
                // approval is revoked between the check and the Raft commit.
                if !self.state.config.auto_join
                    && !self.state.approved_nodes.contains(&node_info.host_id)
                {
                    tracing::warn!(
                        host_id = %node_info.host_id,
                        "rejecting JoinNode — node not approved (auto_join=false)"
                    );
                    // Skip the join — do not add to members or ring.
                    // The command is still committed to the Raft log (it's
                    // already been replicated), but the state machine ignores it.
                } else {
                    let node_id = super::uuid_to_node_id(node_info.host_id);
                    // A (re)join that starts in `Joining` must bootstrap again:
                    // an earlier record describes data that may be long gone.
                    if node_info.state == super::NodeState::Joining {
                        self.state.bootstrap_complete.remove(&node_id);
                    }
                    self.state.members.insert(node_id, node_info);
                    // Auto-assign deterministic tokens so a late-joining node
                    // (e.g. the rejoin path via `ClusterDdlForwardHandler`,
                    // which only proposes `RaftOp::JoinNode` and not
                    // `RaftOp::AssignTokens`) still appears as a token-owning
                    // peer in `system.peers`. Without this, the node lands in
                    // `state.members` but `state.token_map` has no entries for
                    // it, so `RaftClusterState::peers().tokens` is empty and
                    // CQL drivers route every key to the seed.
                    // Use `or_insert` so we never steal a token already owned
                    // by another node — deterministic generation makes
                    // collisions astronomically unlikely, but this preserves
                    // the existing owner if one occurs.
                    let num_tokens = self.state.config.num_tokens as usize;
                    if num_tokens > 0 {
                        for tok in
                            crate::controller::deterministic_tokens_for_node(node_id, num_tokens)
                        {
                            self.state.token_map.entry(tok).or_insert(node_id);
                        }
                    }
                    self.sync_ring();
                    // Mark the new node as Building for all existing indexes.
                    for statuses in self.state.index_state_map.values_mut() {
                        statuses.entry(node_id).or_insert(IndexNodeStatus::Building);
                    }
                }
            }
            RaftOp::UpdateNodeInfo(node_info) => {
                schema_changed = false;
                let node_id = super::uuid_to_node_id(node_info.host_id);
                if let Some(existing) = self.state.members.get_mut(&node_id) {
                    existing.addr = node_info.addr;
                    existing.data_center = node_info.data_center;
                    existing.rack = node_info.rack;
                    existing.state = node_info.state;
                    existing.cql_broadcast = node_info.cql_broadcast;
                    self.sync_ring();
                } else {
                    tracing::warn!(
                        host_id = %node_info.host_id,
                        "ignoring UpdateNodeInfo for unknown cluster member"
                    );
                }
            }
            RaftOp::LeaveNode { node_id } => {
                schema_changed = false;
                self.state.members.remove(&node_id);
                self.state.bootstrap_complete.remove(&node_id);
                self.state.token_map.retain(|_, n| *n != node_id);
                self.sync_ring();
                // Remove departing node from per-index build status.
                for statuses in self.state.index_state_map.values_mut() {
                    statuses.remove(&node_id);
                }
            }
            RaftOp::AssignTokens { node_id, tokens } => {
                schema_changed = false;
                for token in tokens {
                    self.state.token_map.insert(token, node_id);
                }
                self.sync_ring();
            }

            // ---- Config ------------------------------------------------
            RaftOp::UpdateConfig(config) => {
                schema_changed = false;
                self.state.config = config;
            }

            // ---- Node admission ----------------------------------------
            RaftOp::ApproveNode { host_id } => {
                schema_changed = false;
                self.state.approved_nodes.insert(host_id);
            }

            // ---- Node lifecycle ----------------------------------------
            RaftOp::SetNodeState { node_id, state } => {
                schema_changed = false;
                if let Some(node) = self.state.members.get_mut(&node_id) {
                    tracing::info!(
                        node_id,
                        old = ?node.state,
                        new = ?state,
                        "node state transition"
                    );
                    node.state = state;
                }
                self.sync_ring();
            }

            // ---- Multi-DC Accord (Sprint 7) ----------------------------
            RaftOp::AccordApply {
                txn_id,
                hlc,
                mutation,
            } => {
                schema_changed = false;
                self.apply_accord_marked(txn_id, hlc, mutation);
            }

            // ---- Verified data movement (P0-4) -------------------------
            RaftOp::RecordBootstrapComplete { node_id, evidence } => {
                schema_changed = false;
                if self.state.members.contains_key(&node_id) {
                    tracing::info!(
                        node_id,
                        sessions = evidence.sessions,
                        ranges = evidence.ranges,
                        partitions = evidence.partitions,
                        "bootstrap complete and verified; member may be promoted"
                    );
                    self.state.bootstrap_complete.insert(node_id, evidence);
                    self.sync_ring();
                } else {
                    tracing::error!(
                        node_id,
                        "refusing RecordBootstrapComplete: node is not a member"
                    );
                    command_rejection = Some(format!(
                        "RecordBootstrapComplete for node {node_id}: not a member"
                    ));
                }
            }
        }

        if schema_changed {
            // Only true schema/auth mutations should advance schema_version.
            // Duplicate replayed DDL and topology-only Raft commands must not
            // churn system.local.schema_version or drivers can sit in schema
            // agreement loops until they time out.
            self.state.schema_version = schema_version;
            if let Some(schema) = &self.schema {
                schema.set_schema_version(schema_version);
            }
        }

        // A committed local effect failure is a storage failure, not a
        // publishable command response. The retained worker propagates it to
        // the sticky apply fence before adopting staged metadata or log ID.
        if !apply_errors.is_empty() {
            let summary = apply_errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ferrosa_common::Error::Io(std::io::Error::other(summary)));
        }
        let response = command_rejection
            .map(RaftResponse::Error)
            .unwrap_or(RaftResponse::Ok);
        Ok((response, pending_system_writes))
    }

    /// Persist the collected system-table writes inside the same owned worker
    /// as physical apply. Cancellation cannot let a subsequent entry overtake
    /// this stage or release compaction pauses before it finishes.
    fn flush_pending_system_writes(
        &self,
        pending: Vec<PendingSystemWrite>,
    ) -> ferrosa_common::Result<bool> {
        let Some(writer) = &self.system_writer else {
            // Explicit metadata-only state machines have no configured projection.
            return Ok(false);
        };
        let dirty = !pending.is_empty();
        for pending in pending {
            if let Err(error) = writer.apply(pending.mutation) {
                match pending.level {
                    SystemWriteLogLevel::WarnReplay => {
                        tracing::warn!(%error, "{}", pending.context)
                    }
                    SystemWriteLogLevel::Error => tracing::error!(%error, "{}", pending.context),
                }
                // Severity is not permission to lose a canonical row. Current
                // startup registers system tables before replay; real storage
                // failures, including WarnReplay sites, must remain replayable.
                return Err(error);
            }
        }
        Ok(dirty)
    }

    /// One fence covers all canonical writes in this owned command group.
    fn sync_projection(&self, dirty: bool) -> ferrosa_common::Result<()> {
        if !dirty {
            return Ok(());
        }
        // Periodic acknowledgement alone does not permit snapshot/purge.
        if let Some(engine) = &self.engine {
            #[cfg(test)]
            self.projection_syncs
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            engine.force_commit_log_sync()?;
        }
        Ok(())
    }
}

impl Default for FerrosStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// RaftSnapshotBuilder
// ---------------------------------------------------------------------------

impl RaftSnapshotBuilder<FerrosRaftConfig> for FerrosStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<FerrosRaftConfig>, StorageError<u64>> {
        let physical = self.physical_admission()?;
        self.finish_pending_apply().await.map_err(|error| *error)?;
        let data = SnapshotData {
            state: self.state.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        };

        let bytes = encode_snapshot(&data)
            .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;

        let snapshot_id = format!(
            "{}-{}",
            self.last_applied.map(|id| id.index).unwrap_or(0),
            uuid::Uuid::new_v4()
        );

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        // Publish to the slot this builder shares with the live state machine
        // BEFORE persisting and before `mark_durably_applied` lets the log
        // store purge up to this index (D1): replication must already be able
        // to serve a snapshot covering anything that may be purged. If a newer
        // snapshot was installed meanwhile, the slot keeps it, and the
        // watermark below is monotonic, so it never exceeds the slot either.
        self.publish_snapshot(meta.clone(), bytes.clone());
        if let Some(path) = self.snapshot_path.clone() {
            let meta_for_disk = meta.clone();
            let disk_lifecycle = Arc::clone(&self.physical_lifecycle);
            let bytes_for_disk = bytes.clone();
            #[allow(clippy::result_large_err)]
            ferrosa_net::task_pool::TaskPool::current("raft-log-store")
                .spawn_blocking(move || {
                    let _physical = physical.into_job();
                    let result =
                        Self::persist_snapshot_to_disk(&path, &meta_for_disk, &bytes_for_disk);
                    if let Err(error) = &result {
                        disk_lifecycle.fail(error);
                    }
                    result
                })
                .await
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))??;
            self.mark_durably_applied(meta.last_log_id);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

// ---------------------------------------------------------------------------
// RaftStateMachine
// ---------------------------------------------------------------------------

impl RaftStateMachine<FerrosRaftConfig> for FerrosStateMachine {
    type SnapshotBuilder = FerrosStateMachine;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        self.finish_pending_apply().await.map_err(|error| *error)?;
        self.physical_lifecycle.applied(self.last_applied);
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<FerrosRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let physical = self.physical_admission()?;
        let mut entries = entries.into_iter().peekable();
        let first = entries.peek().map(|entry| entry.log_id.index);
        let required = self.unconsumed_input;
        // Capture ownership before even awaiting a preceding job. A cancelled
        // new caller may otherwise lose its input while that job is reaped.
        if required.is_none() {
            self.unconsumed_input = first;
        }
        self.finish_pending_apply().await.map_err(|error| *error)?;
        if let Some((required, first)) = required
            .zip(first)
            .filter(|(required, first)| first != required)
        {
            return Err(
                StorageIOError::write_state_machine(to_any_error(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cancelled Raft input requires committed index {required} before {first}"
                    ),
                )))
                .into(),
            );
        }
        let mut responses = Vec::new();
        while let Some(entry) = entries.next() {
            self.unconsumed_input = Some(entry.log_id.index);
            match entry.payload {
                EntryPayload::Blank => {
                    self.last_applied = Some(entry.log_id);
                    self.physical_lifecycle.applied(self.last_applied);
                    self.unconsumed_input = entries.peek().map(|next| next.log_id.index);
                    responses.push(RaftResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let mut pauses = Vec::new();
                    if let Some(engine) = &self.engine {
                        match &cmd.op {
                            RaftOp::DropTable { keyspace, table } => {
                                pauses.push(
                                    engine
                                        .pause_table_compactions(
                                            &TableId::new(keyspace, table),
                                            ferrosa_common::CancelReason::TableDropped,
                                        )
                                        .await
                                        .map_err(|error| {
                                            StorageIOError::write_state_machine(to_any_error(error))
                                        })?,
                                );
                            }
                            RaftOp::DropKeyspace(name) => {
                                for (keyspace, table) in
                                    self.state.tables.keys().filter(|(ks, _)| ks == name)
                                {
                                    pauses.push(
                                        engine
                                            .pause_table_compactions(
                                                &TableId::new(keyspace, table),
                                                ferrosa_common::CancelReason::TableDropped,
                                            )
                                            .await
                                            .map_err(|error| {
                                                StorageIOError::write_state_machine(to_any_error(
                                                    error,
                                                ))
                                            })?,
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                    // Coalesce only this existing apply input. A subsequent
                    // destructive operation gets its own group so its async
                    // compaction drain resolves against committed prior state.
                    // Accord data operations also retain their existing boundary.
                    let groupable = !matches!(
                        &cmd.op,
                        RaftOp::DropTable { .. }
                            | RaftOp::DropKeyspace(_)
                            | RaftOp::AccordApply { .. }
                    );
                    let mut commands = vec![cmd];
                    let mut last_log_id = entry.log_id;
                    while groupable
                        && matches!(
                            entries.peek().map(|next| &next.payload),
                            Some(EntryPayload::Normal(next))
                                if !matches!(&next.op, RaftOp::DropTable { .. } | RaftOp::DropKeyspace(_) | RaftOp::AccordApply { .. })
                        )
                    {
                        let Some(next) = entries.next() else {
                            break;
                        };
                        if let EntryPayload::Normal(command) = next.payload {
                            commands.push(command);
                            last_log_id = next.log_id;
                        }
                    }
                    let pauses = Arc::new(pauses);
                    let worker_pauses = Arc::clone(&pauses);
                    let mut staged = self.staged_apply();
                    let physical_job = Arc::clone(&physical).into_job();
                    let job_lifecycle = Arc::clone(&self.physical_lifecycle);
                    let job = ferrosa_net::task_pool::TaskPool::current("raft-physical-apply")
                        .spawn_blocking(move || {
                            let _physical = physical_job;
                            let _pauses = worker_pauses;
                            let result: ferrosa_common::Result<CompletedApply> = (|| {
                                let mut responses = Vec::with_capacity(commands.len());
                                let mut dirty = false;
                                for command in commands {
                                    let (response, system_writes) =
                                        staged.apply_command(command)?;
                                    dirty |= staged.flush_pending_system_writes(system_writes)?;
                                    responses.push(response);
                                }
                                staged.sync_projection(dirty)?;
                                Ok(CompletedApply {
                                    state: staged.state,
                                    responses,
                                })
                            })(
                            );
                            if let Err(error) = &result {
                                job_lifecycle.fail(error);
                            }
                            result
                        });
                    self.pending_apply = Some(PendingApply {
                        log_id: last_log_id,
                        job,
                        pauses,
                    });
                    // The worker now owns the group, but not the caller's suffix.
                    self.unconsumed_input = entries.peek().map(|next| next.log_id.index);
                    if let Some(group_responses) =
                        self.finish_pending_apply().await.map_err(|error| *error)?
                    {
                        responses.extend(group_responses);
                    }
                }
                EntryPayload::Membership(membership) => {
                    self.last_applied = Some(entry.log_id);
                    self.physical_lifecycle.applied(self.last_applied);
                    self.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    self.unconsumed_input = entries.peek().map(|next| next.log_id.index);
                    responses.push(RaftResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        if let Err(error) = self.finish_pending_apply().await {
            // This trait method cannot return an error; the returned builder
            // retains the sticky failure and build_snapshot reports it.
            tracing::error!(%error, "Raft snapshot builder refused failed apply");
        }
        // The builder is a point-in-time copy of the state (openraft runs it in
        // a detached task while apply continues), but it SHARES the
        // current-snapshot slot: what it builds is what the live machine's
        // `get_current_snapshot` serves (D1).
        FerrosStateMachine {
            state: self.state.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            current_snapshot: Arc::clone(&self.current_snapshot),
            schema: None, // snapshot builder doesn't need side effects
            engine: None,
            ring: None,          // snapshot builder doesn't need live ring
            ring_observer: None, // snapshot builder doesn't need observability ring
            system_writer: None, // snapshot builder doesn't need system writer
            snapshot_path: self.snapshot_path.clone(),
            durable_applied: self.durable_applied.clone(),
            pending_apply: None,
            unconsumed_input: None,
            physical_lifecycle: Arc::clone(&self.physical_lifecycle),
            _physical_owner: None,
            apply_failure: self.apply_failure.clone(),
            #[cfg(test)]
            projection_syncs: Arc::clone(&self.projection_syncs),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        self.finish_pending_apply().await.map_err(|error| *error)?;
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let result = async {
            let physical = self.physical_admission()?;
            self.finish_pending_apply().await.map_err(|error| *error)?;
            let bytes = snapshot.into_inner();
            self.apply_snapshot_data(meta, &bytes)?;
            self.publish_snapshot(meta.clone(), bytes.clone());
            if let Some(path) = self.snapshot_path.clone() {
                let meta_for_disk = meta.clone();
                let disk_lifecycle = Arc::clone(&self.physical_lifecycle);
                #[allow(clippy::result_large_err)]
                ferrosa_net::task_pool::TaskPool::current("raft-log-store")
                    .spawn_blocking(move || {
                        let _physical = physical.into_job();
                        let result = Self::persist_snapshot_to_disk(&path, &meta_for_disk, &bytes);
                        if let Err(error) = &result {
                            disk_lifecycle.fail(error);
                        }
                        result
                    })
                    .await
                    .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))??;
                self.mark_durably_applied(meta.last_log_id);
            }
            self.physical_lifecycle.applied(self.last_applied);

            Ok(())
        }
        .await;
        if let Err(error) = &result {
            self.physical_lifecycle.fail(error);
        }
        result
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<FerrosRaftConfig>>, StorageError<u64>> {
        self.finish_pending_apply().await.map_err(|error| *error)?;
        Ok(self
            .current_snapshot_cloned()
            .map(|(meta, bytes)| Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(bytes)),
            }))
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Whether `table_id` has anything durable on this node: SSTables (live or on
/// disk) or a persisted index registration. A table like that cannot be
/// dropped on the strength of its absence from an incoming schema (CL-22).
pub(crate) fn table_has_local_artifacts(
    engine: &StorageEngine,
    table_id: &TableId,
) -> ferrosa_common::Result<bool> {
    if engine.sstable_count(table_id) > 0 {
        return Ok(true);
    }

    let table_dir = engine.table_sstable_dir(table_id);
    if !StorageEngine::list_generations_in_dir(&table_dir).is_empty() {
        return Ok(true);
    }

    let persisted_indexes = engine.read_persisted_indexes()?;
    Ok(persisted_indexes.iter().any(|index| {
        index.keyspace_name == table_id.keyspace() && index.table_name == table_id.table()
    }))
}

/// Convert an error into an `AnyError` for openraft storage errors.
/// Non-system tables `previous` held that `next` does not: tables whose absence
/// from the incoming snapshot is ambiguous without an explicit drop marker.
fn tables_absent_from_snapshot(
    previous: &BTreeMap<(String, String), TableMetadata>,
    next: &BTreeMap<(String, String), TableMetadata>,
) -> Vec<(String, String)> {
    previous
        .keys()
        .filter(|key| !next.contains_key(*key) && !key.0.starts_with("system"))
        .cloned()
        .collect()
}

/// Snapshot-explicit table drops that are safe to apply destructively.
///
/// The current snapshot payload has no dropped-table tombstones carrying table
/// identity/generation and drop log id, so absence from `next` is not proof of
/// deletion. Keep the function shape as the single future handoff point for ADR
/// item 1; until then, no snapshot-inferred table unregisters are authorized.
fn tables_dropped_by_snapshot(
    _previous: &BTreeMap<(String, String), TableMetadata>,
    _next: &BTreeMap<(String, String), TableMetadata>,
) -> Vec<(String, String)> {
    Vec::new()
}

fn to_any_error(e: impl std::error::Error + Send + Sync + 'static) -> openraft::AnyError {
    openraft::AnyError::new(&e)
}

/// True if granting `child` membership in `parent` (adding `parent` to
/// `child.member_of`) would close a cycle in the role hierarchy — i.e. `child`
/// is already an ancestor of `parent`. Walks upward from `parent`.
fn roles_form_cycle(roles: &BTreeMap<String, RoleMetadata>, parent: &str, child: &str) -> bool {
    let mut visited: Vec<String> = Vec::new();
    let mut stack = vec![parent.to_string()];
    while let Some(cur) = stack.pop() {
        if cur == child {
            return true;
        }
        if visited.contains(&cur) {
            continue;
        }
        visited.push(cur.clone());
        if let Some(r) = roles.get(&cur) {
            for grandparent in &r.member_of {
                stack.push(grandparent.clone());
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};

    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

    use ferrosa_common::{AccordTimestamp, TxnId};
    use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::TableParams;
    use ferrosa_schema::{Permission, Resource, RoleMetadata};

    use crate::raft::{
        uuid_to_node_id, IndexNodeStatus, NodeInfo, NodeState, RaftCommand, RaftOp, Token,
    };

    // -- helpers ----------------------------------------------------------

    fn simple_keyspace(name: &str) -> KeyspaceMetadata {
        let mut opts = HashMap::new();
        opts.insert("replication_factor".to_string(), "1".to_string());
        KeyspaceMetadata {
            name: name.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: opts,
            },
        }
    }

    fn simple_table(ks: &str, name: &str) -> TableMetadata {
        use indexmap::IndexMap;

        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                kind: ColumnKind::PartitionKey,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        columns.insert(
            "value".to_string(),
            ColumnMetadata {
                name: "value".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );

        TableMetadata {
            keyspace: ks.to_string(),
            name: name.to_string(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags: HashSet::new(),
            extensions: HashMap::new(),
            is_system: false,
        }
    }

    fn simple_table_with_generation(ks: &str, name: &str, generation: u64) -> TableMetadata {
        let mut table = simple_table(ks, name);
        table.id = Uuid::from_u128(generation as u128 + 1);
        table.is_system = ks.starts_with("system");
        table
    }

    fn make_entry(term: u64, index: u64, op: RaftOp) -> Entry<FerrosRaftConfig> {
        let cmd = RaftCommand {
            op,
            schema_version: Uuid::new_v4(),
        };
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    /// Like [`make_entry`] but with an explicit `schema_version`, so a test can
    /// assert the VALUE the state machine settles on.
    fn make_entry_with_version(
        term: u64,
        index: u64,
        op: RaftOp,
        schema_version: Uuid,
    ) -> Entry<FerrosRaftConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
            payload: EntryPayload::Normal(RaftCommand { op, schema_version }),
        }
    }

    fn table_map(tables: Vec<TableMetadata>) -> BTreeMap<(String, String), TableMetadata> {
        tables
            .into_iter()
            .map(|table| ((table.keyspace.clone(), table.name.clone()), table))
            .collect()
    }

    // -- tests ------------------------------------------------------------

    /// ADR `/Users/bkearns/src/ferrosa-suite/specs/raft-snapshot-table-drop-cascade-adr.md`:
    /// snapshot table-map absence is ambiguous and must not authorize local
    /// engine unregister/delete/tombstone side effects without an explicit
    /// identity-scoped drop marker. Current production snapshots do not yet
    /// carry such markers, so absence must conservatively preserve local
    /// durable artifacts.
    #[test]
    fn snapshot_absence_without_drop_marker_does_not_unregister_table() {
        let previous = table_map(vec![simple_table("agent_memory", "entity_store")]);
        let next = BTreeMap::new();

        let inferred_drops = tables_dropped_by_snapshot(&previous, &next);

        assert!(
            inferred_drops.is_empty(),
            "absence alone is not independent proof of a drop; got inferred unregisters: {inferred_drops:?}"
        );
    }

    /// Future explicit-drop-marker invariant. The current snapshot payload has
    /// no dropped-table tombstone set and no table generation/drop log id, so
    /// the conservative behavior is to treat table-map absence as ambiguous,
    /// not as a matching explicit marker.
    #[test]
    fn snapshot_explicit_drop_marker_unregisters_matching_table_identity() {
        let previous = table_map(vec![simple_table("agent_memory", "entity_store")]);
        let next = BTreeMap::new();

        assert!(
            tables_dropped_by_snapshot(&previous, &next).is_empty(),
            "without a snapshot drop-marker API, no table-map absence may be treated as an explicit identity match"
        );
    }

    /// Same-name recreate safety from the ADR: a marker for old identity A must
    /// not delete or tombstone a recreated table with identity B. Current table
    /// metadata has UUIDs, but snapshot data does not yet expose identity-scoped
    /// dropped-table markers. Until it does, same-name absence remains
    /// ambiguous and must not authorize cleanup by name.
    #[test]
    fn snapshot_drop_marker_for_old_identity_does_not_delete_recreated_table() {
        let previous = table_map(vec![simple_table("agent_memory", "entity_store")]);
        let next = BTreeMap::new();

        assert!(
            tables_dropped_by_snapshot(&previous, &next).is_empty(),
            "without identity-scoped drop markers, a same-name recreated table must not be deleted by inferred snapshot absence"
        );
    }

    #[test]
    fn snapshot_system_keyspaces_are_not_unregistered_by_absence() {
        let previous = table_map(vec![
            simple_table("system_schema", "indexes"),
            simple_table("system_auth", "roles"),
            simple_table("system_traces", "events"),
        ]);
        let next = BTreeMap::new();

        assert!(
            tables_dropped_by_snapshot(&previous, &next).is_empty(),
            "system-prefixed keyspaces remain protected from inferred snapshot drops; this name filter is a guardrail, not sufficient proof for application tables"
        );
    }

    // ---- W7.1: HLC watermark tracking ----------------------------------

    /// W7.1 RED → GREEN. Each `RaftOp::AccordApply` step updates an HLC
    /// watermark on `RaftState`; the watermark advances monotonically as
    /// successive Accord-marked entries land. Two entries with strictly
    /// increasing HLC timestamps drive the watermark from 0 → t1 → t2.
    ///
    /// Uses HLCs realistic relative to wall-clock-microseconds and an
    /// explicit `advance_accord_watermark` step so the buffered entries
    /// are released regardless of the configured max-skew bound.
    #[tokio::test]
    async fn state_machine_tracks_hlc_watermark() {
        let mut sm = FerrosStateMachine::new();
        // Initial watermark must be the zero timestamp.
        assert_eq!(
            sm.state().hlc_watermark,
            AccordTimestamp::synthetic(0),
            "fresh state machine must start at zero watermark"
        );

        // HLCs are wall-clock microseconds: pick values comfortably above
        // any realistic skew bound so the watermark advance succeeds.
        let t1 = AccordTimestamp::synthetic(1_000_000_000);
        let t2 = AccordTimestamp::synthetic(2_000_000_000);

        let txn1 = TxnId::new(1, t1);
        let txn2 = TxnId::new(1, t2);

        let entries = vec![
            make_entry(
                1,
                1,
                RaftOp::AccordApply {
                    txn_id: txn1,
                    hlc: t1,
                    mutation: Vec::new(),
                },
            ),
            make_entry(
                1,
                2,
                RaftOp::AccordApply {
                    txn_id: txn2,
                    hlc: t2,
                    mutation: Vec::new(),
                },
            ),
        ];

        sm.apply(entries).await.unwrap();
        // Advance the watermark past t2 (simulates the heartbeat-driven
        // watermark tick once the wall-clock has progressed).
        sm.advance_accord_watermark(t2);

        // Watermark must advance to the largest observed Accord
        // timestamp once entries are released by the reorder buffer.
        assert!(
            sm.state().hlc_watermark >= t2,
            "watermark must advance past last applied Accord timestamp; got {:?}",
            sm.state().hlc_watermark
        );
        assert!(
            sm.state().hlc_watermark >= t1,
            "watermark must be monotonic w.r.t. earlier applies"
        );
    }

    /// t_2b57a6e4: `AccordTimestamp::time` is HLC nanoseconds
    /// (`HybridLogicalClock::now`), so the skew gauge must divide by 1000
    /// to report microseconds. It read 1000x high before.
    #[tokio::test]
    async fn accord_skew_gauge_reports_microseconds_from_nanosecond_hlcs() {
        let mut sm = FerrosStateMachine::new();
        let watermark_ns: u64 = 1_791_400_000_000_000_000;
        sm.advance_accord_watermark(AccordTimestamp::new(1, watermark_ns, 1));
        let ahead_ns = 2_500_000; // 2.5 ms ahead of the watermark
        let hlc = AccordTimestamp::new(1, watermark_ns + ahead_ns, 1);
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::AccordApply {
                txn_id: TxnId::new(1, hlc),
                hlc,
                mutation: Vec::new(),
            },
        )])
        .await
        .unwrap();
        assert_eq!(sm.state().max_observed_skew_us, 2_500);
    }

    /// W7.5 RED → GREEN. Applying the same `RaftOp::AccordApply` twice
    /// is a NoOp — the ledger short-circuits the replay. Final state
    /// matches the single-apply state (I-28).
    #[tokio::test]
    async fn accord_apply_idempotent() {
        let mut sm = FerrosStateMachine::new();
        let hlc = AccordTimestamp::synthetic(1_000_000_000);
        let txn = TxnId::new(7, hlc);

        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::AccordApply {
                txn_id: txn,
                hlc,
                mutation: vec![1, 2, 3],
            },
        )])
        .await
        .unwrap();
        sm.advance_accord_watermark(hlc);
        assert_eq!(sm.state().applied_accord_txns.len(), 1);
        let watermark_after_first = sm.state().hlc_watermark;

        // Replay the same txn — buffer must NOT grow; ledger size
        // unchanged; watermark unchanged.
        sm.apply(vec![make_entry(
            1,
            2,
            RaftOp::AccordApply {
                txn_id: txn,
                hlc,
                mutation: vec![1, 2, 3],
            },
        )])
        .await
        .unwrap();
        assert_eq!(sm.accord_apply_buffer().len(), 0, "replay must be deduped");
        assert_eq!(
            sm.state().applied_accord_txns.len(),
            1,
            "ledger must remain at 1 entry"
        );
        assert_eq!(
            sm.state().hlc_watermark,
            watermark_after_first,
            "watermark must not regress on replay"
        );
    }

    /// W7.4 RED → GREEN. An entry whose HLC is 500ms in the future
    /// relative to local "now" with max_skew = 200ms stalls. Cross-DC
    /// writes pause until the local clock catches up. Above
    /// REORDER_BUFFER_ALARM_DEPTH the buffer reports over-threshold so
    /// the alarm gauge fires.
    #[tokio::test]
    async fn reorder_buffer_stalls_above_max_skew() {
        use crate::raft::multi_dc_apply::{watermark_for, REORDER_BUFFER_ALARM_DEPTH};
        use std::time::Duration;

        let mut sm = FerrosStateMachine::new();
        let max_skew = Duration::from_millis(200);
        let now_us = 100_000u64;
        // Entry hlc = now + 500ms (skew far exceeds 200ms bound).
        let future_hlc = AccordTimestamp::synthetic(now_us + 500_000);
        let txn = TxnId::new(1, future_hlc);
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::AccordApply {
                txn_id: txn,
                hlc: future_hlc,
                mutation: Vec::new(),
            },
        )])
        .await
        .unwrap();

        // Watermark at now - max_skew = max(0, -100ms) = 0. Entry stalls.
        sm.advance_accord_watermark(watermark_for(now_us, max_skew));
        assert_eq!(sm.accord_apply_buffer().len(), 1, "skew > bound stalls");
        assert!(!sm.state().applied_accord_txns.contains(&txn));

        // Push enough entries past the alarm threshold (still future
        // from "now") so the gauge fires.
        for i in 0..=(REORDER_BUFFER_ALARM_DEPTH as u64) {
            let h = AccordTimestamp::synthetic(now_us + 600_000 + i);
            let id = TxnId::new(1, h);
            sm.apply(vec![make_entry(
                1,
                2 + i,
                RaftOp::AccordApply {
                    txn_id: id,
                    hlc: h,
                    mutation: Vec::new(),
                },
            )])
            .await
            .unwrap();
        }
        assert!(
            sm.accord_apply_buffer().over_alarm_threshold(),
            "buffer above {REORDER_BUFFER_ALARM_DEPTH} entries must trigger the alarm"
        );
    }

    /// W7.3 RED → GREEN. With `max_skew = 200ms`, the heartbeat-driven
    /// watermark advances when `now - 200ms > entry.hlc`. Until that
    /// crossing, the entry stalls in the reorder buffer.
    #[tokio::test]
    async fn watermark_advances_with_max_skew_200ms() {
        use crate::raft::multi_dc_apply::watermark_for;
        use std::time::Duration;

        let mut sm = FerrosStateMachine::new();
        // Feed an entry with HLC = 800ms.
        let hlc = AccordTimestamp::synthetic(800_000);
        let txn = TxnId::new(1, hlc);
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::AccordApply {
                txn_id: txn,
                hlc,
                mutation: Vec::new(),
            },
        )])
        .await
        .unwrap();

        let max_skew = Duration::from_millis(200);

        // At now = 900ms, watermark = 700_000us — below the entry's
        // HLC, so the entry must stall.
        sm.advance_accord_watermark(watermark_for(900_000, max_skew));
        assert_eq!(sm.accord_apply_buffer().len(), 1);
        assert!(!sm.state().applied_accord_txns.contains(&txn));

        // At now = 1100ms, watermark = 900_000us — above 800_000us, so
        // the entry releases.
        sm.advance_accord_watermark(watermark_for(1_100_000, max_skew));
        assert!(sm.accord_apply_buffer().is_empty());
        assert!(sm.state().applied_accord_txns.contains(&txn));
    }

    /// W7.2 RED → GREEN. Two `AccordApply` entries fed in reverse HLC
    /// order (t2 before t1) must drain in ascending HLC order — t1
    /// before t2 — so every replica sees the same apply order (I-27).
    #[tokio::test]
    async fn apply_buffers_out_of_order_accord_entries() {
        let mut sm = FerrosStateMachine::new();
        let t1 = AccordTimestamp::synthetic(1_000_000_000);
        let t2 = AccordTimestamp::synthetic(2_000_000_000);
        let txn1 = TxnId::new(1, t1);
        let txn2 = TxnId::new(1, t2);

        // Feed t2 FIRST, then t1 — exactly the reverse order.
        let entries = vec![
            make_entry(
                1,
                1,
                RaftOp::AccordApply {
                    txn_id: txn2,
                    hlc: t2,
                    mutation: Vec::new(),
                },
            ),
            make_entry(
                1,
                2,
                RaftOp::AccordApply {
                    txn_id: txn1,
                    hlc: t1,
                    mutation: Vec::new(),
                },
            ),
        ];
        sm.apply(entries).await.unwrap();

        // While the watermark is below t1, both must be buffered.
        sm.advance_accord_watermark(AccordTimestamp::synthetic(500_000_000));
        assert_eq!(sm.accord_apply_buffer().len(), 2, "below t1 — both stall");

        // Advance the watermark to release t1 first.
        sm.advance_accord_watermark(t1);
        assert_eq!(
            sm.accord_apply_buffer().len(),
            1,
            "watermark = t1 must release exactly t1"
        );
        assert!(
            sm.state().applied_accord_txns.contains(&txn1),
            "t1 ledger entry must be recorded first"
        );
        assert!(
            !sm.state().applied_accord_txns.contains(&txn2),
            "t2 must still be buffered"
        );

        // Now release t2.
        sm.advance_accord_watermark(t2);
        assert!(sm.accord_apply_buffer().is_empty());
        assert!(sm.state().applied_accord_txns.contains(&txn2));
    }

    #[tokio::test]
    async fn apply_create_keyspace() {
        let mut sm = FerrosStateMachine::new();
        let ks = simple_keyspace("test_ks");
        let entry = make_entry(1, 1, RaftOp::CreateKeyspace(ks));

        let results = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], RaftResponse::Ok));
        assert!(sm.state().keyspaces.contains_key("test_ks"));
        assert_eq!(sm.state().keyspaces.len(), 1);
    }

    #[tokio::test]
    async fn apply_create_table() {
        let mut sm = FerrosStateMachine::new();
        let ks = simple_keyspace("ks1");
        let table = simple_table("ks1", "users");

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(ks)),
            make_entry(1, 2, RaftOp::CreateTable(Box::new(table))),
        ];

        let results = sm.apply(entries).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(sm
            .state()
            .tables
            .contains_key(&("ks1".into(), "users".into())));
    }

    #[tokio::test]
    async fn apply_join_node() {
        let mut sm = FerrosStateMachine::new();
        let host_id = Uuid::new_v4();
        let node = NodeInfo {
            host_id,
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };
        let node_id = super::super::uuid_to_node_id(host_id);

        let entry = make_entry(1, 1, RaftOp::JoinNode(node));
        sm.apply(vec![entry]).await.unwrap();

        assert!(sm.state().members.contains_key(&node_id));
        assert_eq!(sm.state().members[&node_id].addr, "10.0.0.1:7000");
    }

    /// Positive control for the schema-version contract
    /// (bug-schema-version-diverges-after-restart, t_7dda9421): a genuine
    /// schema change DOES move the node's committed version to the version the
    /// command carries. Without this control, a "fix" that simply never updated
    /// the version would pass every topology guard below.
    #[tokio::test]
    async fn real_ddl_command_changes_the_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&schema), Arc::clone(&engine));

        let v1 = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        let v2 = Uuid::from_u128(0x9999_aaaa_bbbb_cccc_dddd_eeee_ffff_0001);

        sm.apply(vec![make_entry_with_version(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("tks")),
            v1,
        )])
        .await
        .unwrap();
        assert_eq!(sm.state().schema_version, v1);
        assert_eq!(schema.snapshot().version, v1);

        sm.apply(vec![make_entry_with_version(
            1,
            2,
            RaftOp::CreateTable(Box::new(simple_table("tks", "tbl"))),
            v2,
        )])
        .await
        .unwrap();

        assert_eq!(
            sm.state().schema_version,
            v2,
            "a real DDL must advance the committed schema version"
        );
        assert_eq!(schema.snapshot().version, v2);
    }

    /// bug-schema-version-diverges-after-restart (t_7dda9421), Expected:
    /// "versions should differ only while a schema change is actually being
    /// applied". A topology command changes no schema, so applying one must
    /// leave the node's committed schema version exactly where a prior real DDL
    /// put it — even when the command carries a freshly minted random version.
    ///
    /// Regression guard: the `schema_changed` gate in `apply_command` already
    /// existed on main (from the auth-bootstrap stabilisation), so this case was
    /// green before the schema-version fix. It is kept to hold the contract
    /// down against a future topology arm that forgets to clear the gate.
    #[tokio::test]
    async fn topology_command_leaves_the_schema_version_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&schema), Arc::clone(&engine));

        let ddl_version = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        sm.apply(vec![make_entry_with_version(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("tks")),
            ddl_version,
        )])
        .await
        .unwrap();
        assert_eq!(sm.state().schema_version, ddl_version);

        let host_id = Uuid::new_v4();
        let node = NodeInfo {
            host_id,
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };
        sm.apply(vec![make_entry_with_version(
            1,
            2,
            RaftOp::JoinNode(node),
            Uuid::new_v4(),
        )])
        .await
        .unwrap();

        assert_eq!(
            sm.state().schema_version,
            ddl_version,
            "a topology command must not advance the committed schema version"
        );
        assert_eq!(
            schema.snapshot().version,
            ddl_version,
            "the shared Schema must keep the committed version after a topology command"
        );
    }

    /// Regression test for the system.peers empty-tokens bug:
    /// the rejoin / late-join path goes through `ClusterDdlForwardHandler`,
    /// which proposes `RaftOp::JoinNode` but **not** a follow-up
    /// `RaftOp::AssignTokens`.  Without auto-assignment inside the
    /// state machine, the new node lands in `state.members` with no
    /// entry in `state.token_map`, so `system.peers` reports it with
    /// empty tokens and cdrs-tokio's `is_peer_row_valid` filter drops
    /// the row — making the cluster behave as single-owner.
    #[tokio::test]
    async fn apply_join_node_auto_assigns_deterministic_tokens() {
        let mut sm = FerrosStateMachine::new();
        // Seed a non-zero num_tokens so the assignment is observable.
        let cfg = ClusterConfig {
            num_tokens: 8,
            ..ClusterConfig::default()
        };
        sm.apply(vec![make_entry(1, 1, RaftOp::UpdateConfig(cfg))])
            .await
            .unwrap();

        let host_id = Uuid::new_v4();
        let node = NodeInfo {
            host_id,
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };
        let node_id = super::super::uuid_to_node_id(host_id);

        sm.apply(vec![make_entry(1, 2, RaftOp::JoinNode(node))])
            .await
            .unwrap();

        assert!(sm.state().members.contains_key(&node_id));
        let owned_tokens: Vec<i64> = sm
            .state()
            .token_map
            .iter()
            .filter_map(|(&t, &n)| (n == node_id).then_some(t))
            .collect();
        assert_eq!(
            owned_tokens.len(),
            8,
            "JoinNode must auto-assign num_tokens deterministic tokens \
             so the late-joining node is a visible owner in system.peers; \
             got {} tokens",
            owned_tokens.len()
        );
        // The auto-assigned set must match the deterministic generator.
        let expected = crate::controller::deterministic_tokens_for_node(node_id, 8);
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        let mut got_sorted = owned_tokens.clone();
        got_sorted.sort();
        assert_eq!(
            got_sorted, expected_sorted,
            "auto-assigned tokens must match deterministic_tokens_for_node"
        );
    }

    /// A follow-up explicit `AssignTokens` with the same deterministic
    /// token set must be idempotent — the seed-init and
    /// `MembershipManager::join_node` paths still issue it explicitly,
    /// so re-application must not break the auto-assigned state.
    #[tokio::test]
    async fn apply_join_node_then_explicit_assign_tokens_is_idempotent() {
        let mut sm = FerrosStateMachine::new();
        let cfg = ClusterConfig {
            num_tokens: 4,
            ..ClusterConfig::default()
        };
        sm.apply(vec![make_entry(1, 1, RaftOp::UpdateConfig(cfg))])
            .await
            .unwrap();

        let host_id = Uuid::new_v4();
        let node = NodeInfo {
            host_id,
            addr: "10.0.0.2:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };
        let node_id = super::super::uuid_to_node_id(host_id);
        let tokens = crate::controller::deterministic_tokens_for_node(node_id, 4);

        sm.apply(vec![
            make_entry(1, 2, RaftOp::JoinNode(node)),
            make_entry(
                1,
                3,
                RaftOp::AssignTokens {
                    node_id,
                    tokens: tokens.clone(),
                },
            ),
        ])
        .await
        .unwrap();

        // After both ops, the node still owns exactly the deterministic set.
        let owned: Vec<i64> = sm
            .state()
            .token_map
            .iter()
            .filter_map(|(&t, &n)| (n == node_id).then_some(t))
            .collect();
        assert_eq!(owned.len(), 4);
        for t in &tokens {
            assert_eq!(sm.state().token_map.get(t), Some(&node_id));
        }
    }

    #[tokio::test]
    async fn apply_assign_tokens() {
        let mut sm = FerrosStateMachine::new();
        let node_id = 42u64;
        let tokens: Vec<Token> = vec![-100, 0, 100];

        let entry = make_entry(
            1,
            1,
            RaftOp::AssignTokens {
                node_id,
                tokens: tokens.clone(),
            },
        );
        sm.apply(vec![entry]).await.unwrap();

        for token in &tokens {
            assert_eq!(sm.state().token_map.get(token), Some(&node_id));
        }
    }

    #[tokio::test]
    async fn apply_drop_keyspace_cascades() {
        let mut sm = FerrosStateMachine::new();
        let ks = simple_keyspace("doomed");
        let t1 = simple_table("doomed", "t1");
        let t2 = simple_table("doomed", "t2");
        let other_t = simple_table("safe_ks", "t3");

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(ks)),
            make_entry(1, 2, RaftOp::CreateKeyspace(simple_keyspace("safe_ks"))),
            make_entry(1, 3, RaftOp::CreateTable(Box::new(t1))),
            make_entry(1, 4, RaftOp::CreateTable(Box::new(t2))),
            make_entry(1, 5, RaftOp::CreateTable(Box::new(other_t))),
            make_entry(1, 6, RaftOp::DropKeyspace("doomed".to_string())),
        ];

        sm.apply(entries).await.unwrap();

        // Doomed keyspace and its tables should be gone.
        assert!(!sm.state().keyspaces.contains_key("doomed"));
        assert!(!sm
            .state()
            .tables
            .contains_key(&("doomed".into(), "t1".into())));
        assert!(!sm
            .state()
            .tables
            .contains_key(&("doomed".into(), "t2".into())));

        // Safe keyspace and its table should survive.
        assert!(sm.state().keyspaces.contains_key("safe_ks"));
        assert!(sm
            .state()
            .tables
            .contains_key(&("safe_ks".into(), "t3".into())));
    }

    #[tokio::test]
    async fn apply_is_deterministic() {
        // Apply the same sequence of commands to two independent state machines.
        let commands = [
            RaftOp::CreateKeyspace(simple_keyspace("ks1")),
            RaftOp::CreateTable(Box::new(simple_table("ks1", "t1"))),
            RaftOp::JoinNode(NodeInfo {
                host_id: Uuid::nil(),
                addr: "10.0.0.1:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            }),
            RaftOp::AssignTokens {
                node_id: super::super::uuid_to_node_id(Uuid::nil()),
                tokens: vec![-100, 0, 100],
            },
        ];

        let mut sm1 = FerrosStateMachine::new();
        let mut sm2 = FerrosStateMachine::new();

        for (i, cmd) in commands.iter().enumerate() {
            let e1 = make_entry(1, (i + 1) as u64, cmd.clone());
            let e2 = make_entry(1, (i + 1) as u64, cmd.clone());
            sm1.apply(vec![e1]).await.unwrap();
            sm2.apply(vec![e2]).await.unwrap();
        }

        // Structural equality — we can't derive PartialEq on everything,
        // and schema_version is a random UUID so we compare structural parts.
        assert_eq!(sm1.state.keyspaces.len(), sm2.state.keyspaces.len());
        assert_eq!(sm1.state.tables.len(), sm2.state.tables.len());
        assert_eq!(sm1.state.members.len(), sm2.state.members.len());
        assert_eq!(sm1.state.token_map, sm2.state.token_map);
        assert_eq!(
            sm1.state.keyspaces.keys().collect::<Vec<_>>(),
            sm2.state.keyspaces.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            sm1.state.tables.keys().collect::<Vec<_>>(),
            sm2.state.tables.keys().collect::<Vec<_>>()
        );
        // Verify last_applied is the same.
        let (la1, _) = sm1.applied_state().await.unwrap();
        let (la2, _) = sm2.applied_state().await.unwrap();
        assert_eq!(la1, la2);
    }

    #[tokio::test]
    async fn snapshot_roundtrip() {
        let mut sm = FerrosStateMachine::new();

        // Build up some state.
        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("ks1"))),
            make_entry(
                1,
                2,
                RaftOp::CreateTable(Box::new(simple_table("ks1", "users"))),
            ),
            make_entry(
                1,
                3,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::nil(),
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
        ];
        sm.apply(entries).await.unwrap();

        // Build a snapshot.
        let snapshot = sm.build_snapshot().await.unwrap();
        let snap_meta = snapshot.meta.clone();
        let snap_bytes = snapshot.snapshot.into_inner();

        // Create a new empty state machine and install the snapshot.
        let mut sm2 = FerrosStateMachine::new();
        sm2.install_snapshot(&snap_meta, Box::new(Cursor::new(snap_bytes)))
            .await
            .unwrap();

        // Verify state matches.
        assert_eq!(sm2.state().keyspaces.len(), sm.state().keyspaces.len());
        assert!(sm2.state().keyspaces.contains_key("ks1"));
        assert!(sm2
            .state()
            .tables
            .contains_key(&("ks1".into(), "users".into())));
        assert_eq!(sm2.state().members.len(), sm.state().members.len());

        // Verify applied_state matches.
        let (la1, _) = sm.applied_state().await.unwrap();
        let (la2, _) = sm2.applied_state().await.unwrap();
        assert_eq!(la1, la2);
    }

    #[tokio::test]
    async fn persisted_snapshot_roundtrip_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("state-machine.snapshot.bin");

        let mut sm = FerrosStateMachine::with_snapshot_path(snapshot_path.clone());
        // Disable auto-token-assignment so the explicit `AssignTokens` is
        // the sole source of tokens — this test exercises snapshot
        // round-trip, not the auto-assignment policy.
        let cfg = ClusterConfig {
            num_tokens: 0,
            ..ClusterConfig::default()
        };
        let entries = vec![
            make_entry(1, 0, RaftOp::UpdateConfig(cfg)),
            make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("ks1"))),
            make_entry(
                1,
                2,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::nil(),
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
            make_entry(
                1,
                3,
                RaftOp::AssignTokens {
                    node_id: uuid_to_node_id(Uuid::nil()),
                    tokens: vec![-50, 0, 50],
                },
            ),
        ];
        sm.apply(entries).await.unwrap();
        let _snapshot = sm.build_snapshot().await.unwrap();

        let mut restarted = FerrosStateMachine::with_snapshot_path(snapshot_path);
        assert!(
            restarted.recover_from_persisted_snapshot().unwrap(),
            "persisted snapshot should be loaded after restart"
        );

        assert!(restarted.state().keyspaces.contains_key("ks1"));
        assert_eq!(restarted.state().members.len(), 1);
        assert_eq!(restarted.state().token_map.len(), 3);

        let (la1, m1) = sm.applied_state().await.unwrap();
        let (la2, m2) = restarted.applied_state().await.unwrap();
        assert_eq!(la1, la2, "last_applied must survive restart");
        assert_eq!(
            m1.membership().get_joint_config(),
            m2.membership().get_joint_config(),
            "membership must survive restart"
        );
    }

    #[tokio::test]
    async fn recovered_persisted_snapshot_updates_live_schema_and_storage() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("state-machine.snapshot.bin");

        let mut source = FerrosStateMachine::with_snapshot_path(snapshot_path.clone());
        source
            .apply(vec![
                make_entry(
                    1,
                    1,
                    RaftOp::CreateKeyspace(simple_keyspace("agent_memory")),
                ),
                make_entry(
                    1,
                    2,
                    RaftOp::CreateTable(Box::new(simple_table(
                        "agent_memory",
                        "confidence_scores",
                    ))),
                ),
            ])
            .await
            .unwrap();
        source.build_snapshot().await.unwrap();

        let schema = Arc::new(test_schema_instance());
        let engine = test_engine(dir.path());
        assert!(
            !schema
                .snapshot()
                .tables
                .contains_key(&("agent_memory".to_string(), "confidence_scores".to_string())),
            "test must start with live Schema missing the Raft-snapshotted table"
        );
        assert_eq!(
            engine.table_count(),
            0,
            "test engine starts with no user tables"
        );

        let mut restarted = FerrosStateMachine::with_side_effects_and_snapshot_path(
            Arc::clone(&schema),
            Arc::clone(&engine),
            snapshot_path,
        );
        assert!(restarted.recover_from_persisted_snapshot().unwrap());

        assert!(
            schema
                .snapshot()
                .tables
                .contains_key(&("agent_memory".to_string(), "confidence_scores".to_string())),
            "recovering a durable Raft snapshot must make its tables visible to CQL prepare"
        );

        let table_id = TableId::new("agent_memory", "confidence_scores");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"score-key".to_vec(),
        ));
        let row = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
        };
        engine
            .write(&table_id, &key, row, 1)
            .expect("recovered Raft snapshot must also register the table with StorageEngine");
    }

    /// A node that crashed just after a DDL restarts holding a STALE on-disk
    /// `schema.json` version, and must CONVERGE to the committed version once it
    /// reconciles from its own replicated Raft state.
    ///
    /// It drives the two REAL production steps a restart runs — in order, rather
    /// than hand-assembling the end state — so it fails if either leg stops doing
    /// its job:
    ///
    /// 1. **Boot step 4a** (`ferrosa/src/main.rs`, "Restore schema from local
    ///    disk", `schema.apply_snapshot(local_schema_snapshot)`). The persisted
    ///    `schema.json` lands in the registry naming the version it was written
    ///    with. That version is STALE here: the maintenance loop rewrites the file
    ///    only when the version CHANGED
    ///    (`should_persist_schema(current, last) = current != last`,
    ///    `ferrosa/src/maintenance.rs`), so a crash between a DDL's commit and the
    ///    next maintenance tick leaves the file naming the pre-DDL version.
    /// 2. **Raft state-machine recovery** (`ferrosa-cluster/src/controller/cluster.rs`,
    ///    `transition_to_cluster`: `state_machine.recover_from_persisted_snapshot()`
    ///    → `apply_snapshot_data` → `sync_schema_and_engine_from_state`). That
    ///    builds a `SchemaSnapshot` whose `version` is `self.state.schema_version`
    ///    — the REPLICATED, committed version — and applies it to the same
    ///    registry.
    ///
    /// Scope: this exercises the registry call boot step 4a makes plus the state
    /// machine's recovery step, not the whole `ferrosa` binary boot, and no real
    /// crash is simulated. The stale file and the committed replicated state are
    /// fixtures with fixed, DISTINCT versions, so a pass cannot come from the two
    /// being accidentally equal.
    #[tokio::test]
    async fn a_stale_persisted_schema_version_converges_to_the_committed_version() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("state-machine.snapshot.bin");

        // Two fixed, distinct versions: the one the stale schema.json names, and
        // the one the cluster committed with the DDL.
        let stale_version = Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
        let committed_version = Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);
        assert_ne!(
            stale_version, committed_version,
            "the two fixtures must be distinct or every assertion here is vacuous"
        );

        // --- The replicated state: a DDL committed under `committed_version`. ---
        let mut committed_node = FerrosStateMachine::with_snapshot_path(snapshot_path.clone());
        committed_node
            .apply(vec![make_entry_with_version(
                1,
                1,
                RaftOp::CreateKeyspace(simple_keyspace("agent_memory")),
                committed_version,
            )])
            .await
            .unwrap();
        assert_eq!(
            committed_node.state().schema_version,
            committed_version,
            "precondition: the committed DDL must leave the replicated state on the test's \
             committed version"
        );
        committed_node.build_snapshot().await.unwrap();

        // --- The node that crashed and is now restarting. ---
        let schema = Arc::new(test_schema_instance());
        let engine = test_engine(dir.path());
        let boot_version = schema.snapshot().version;
        assert_ne!(
            boot_version, committed_version,
            "the boot-minted version must differ from the committed one or the assertion is vacuous"
        );

        // Leg 1 — boot step 4a: the STALE on-disk schema.json enters the registry.
        let mut stale_disk = ferrosa_schema::SchemaSnapshot::new();
        stale_disk.version = stale_version;
        stale_disk.keyspaces.insert(
            "stale_disk_ks".to_string(),
            simple_keyspace("stale_disk_ks"),
        );
        schema
            .apply_snapshot(stale_disk)
            .expect("boot step 4a applies the persisted local snapshot");
        let version_after_persisted_restore = schema.snapshot().version;

        // Leg 2 — the replicated-state sync.
        let mut restarted = FerrosStateMachine::with_side_effects_and_snapshot_path(
            Arc::clone(&schema),
            Arc::clone(&engine),
            snapshot_path,
        );
        assert!(
            restarted.recover_from_persisted_snapshot().unwrap(),
            "the durable Raft snapshot must load on restart"
        );

        // The convergence claim is asserted FIRST, so a control that breaks the
        // mechanism fails here rather than on the intermediate evidence.
        assert_eq!(
            schema.snapshot().version,
            committed_version,
            "a node restarted on a STALE persisted schema version must converge to the \
             COMMITTED version carried by its replicated Raft state"
        );
        assert_ne!(
            schema.snapshot().version,
            stale_version,
            "the node must not keep serving the stale on-disk version"
        );
        assert_ne!(
            schema.snapshot().version,
            boot_version,
            "the node must not keep the random version minted when the registry was constructed"
        );

        // Evidence that leg 1 really did land the stale version, so the pass
        // above is convergence and not the stale file never having been applied.
        assert_eq!(
            version_after_persisted_restore, stale_version,
            "the persisted restore must adopt the on-disk version — else nothing was stale to fix"
        );
    }

    /// REGRESSION: a Raft snapshot must be able to CORRECT the columns of a
    /// table the engine has already registered, not just introduce new tables.
    ///
    /// `sync_schema_and_engine_from_state` landed schema with
    /// `engine.register_table`, which early-returns when the table is already
    /// registered and discards the incoming schema. The ALTER apply path calls
    /// `engine.update_table_schema` instead, so the two paths disagreed about
    /// how to land a schema: a node whose engine held a stale column set could
    /// never recover it, no matter how many snapshots it installed.
    ///
    /// Observed on the native 3-node cluster: node3 served 11 columns for
    /// agent_memory.entity_store while its own Raft state machine carried the
    /// full 19. Regular columns are ordered alphabetically, so the missing
    /// columns shifted every later ordinal and repair rejected the transfer —
    /// `column "created_at", index 3: TimestampType expects 8 raw bytes but
    /// value provided 3072` (3072 = vector<float,768>, the column that actually
    /// sits at index 3 in the stale ordering). It survived restarts, a snapshot
    /// install and a full raft reset.
    #[test]
    fn snapshot_preserves_rows_with_stored_incarnation() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema_instance());
        let engine = test_engine(dir.path());
        let incoming = simple_table("incarnation_ks", "known");
        engine.register_table(incoming.to_storage_schema()).unwrap();
        let tid = TableId::new(&incoming.keyspace, &incoming.name);
        let key =
            ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"known".to_vec()));
        engine
            .write(
                &tid,
                &key,
                ferrosa_sstable::types::Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                    primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
                },
                1,
            )
            .unwrap();
        let mut sm = FerrosStateMachine::with_side_effects(schema.clone(), engine.clone());
        sm.state.tables.insert(
            (incoming.keyspace.clone(), incoming.name.clone()),
            incoming.clone(),
        );
        sm.sync_schema_and_engine_from_state("known incarnation regression")
            .unwrap();
        assert!(engine.read(&tid, &key).unwrap().is_some());
        assert_eq!(
            schema.snapshot().tables[&(incoming.keyspace, incoming.name)].id,
            incoming.id
        );
    }

    #[test]
    fn snapshot_refuses_data_without_authoritative_local_incarnation() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema_instance());
        let engine = test_engine(dir.path());
        let incoming = simple_table("incarnation_ks", "unbound");
        let mut legacy = incoming.to_storage_schema();
        legacy.extensions.remove(ferrosa_schema::TABLE_ID_EXTENSION);
        engine.register_table(legacy).unwrap();
        let tid = TableId::new(&incoming.keyspace, &incoming.name);
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"unknown".to_vec(),
        ));
        engine
            .write(
                &tid,
                &key,
                ferrosa_sstable::types::Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: ferrosa_sstable::types::DeletionTime::LIVE,
                    primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
                },
                1,
            )
            .unwrap();
        let mut sm = FerrosStateMachine::with_side_effects(schema.clone(), engine.clone());
        sm.state.tables.insert(
            (incoming.keyspace.clone(), incoming.name.clone()),
            incoming.clone(),
        );
        assert!(
            sm.sync_schema_and_engine_from_state("unbound incarnation regression")
                .is_err(),
            "data without an authoritative local UUID must not be assigned an incoming UUID"
        );
        assert!(
            engine.read(&tid, &key).unwrap().is_some(),
            "ambiguous data must remain recoverable"
        );
        assert!(!schema
            .snapshot()
            .tables
            .contains_key(&(incoming.keyspace, incoming.name)));
    }

    #[test]
    fn snapshot_recreated_table_retires_old_physical_rows() {
        use ferrosa_common::{DecoratedKey, PartitionKey};
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(test_schema_instance());
        let engine = test_engine(dir.path());
        let old = simple_table("incarnation_ks", "t");
        schema.create_table_internal(old.clone()).unwrap();
        engine.register_table(old.to_storage_schema()).unwrap();
        let tid = TableId::new(&old.keyspace, &old.name);
        let key = DecoratedKey::new(PartitionKey::new(b"old".to_vec()));
        let row = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
        };
        engine.write(&tid, &key, row, 1).unwrap();
        assert!(engine.read(&tid, &key).unwrap().is_some());
        let mut replacement = old.clone();
        replacement.id = Uuid::new_v4();
        let mut sm = FerrosStateMachine::with_side_effects(schema.clone(), engine.clone());
        sm.state.tables.insert(
            (old.keyspace.clone(), old.name.clone()),
            replacement.clone(),
        );
        sm.sync_schema_and_engine_from_state("incarnation regression")
            .unwrap();
        assert!(
            engine.read(&tid, &key).unwrap().is_none(),
            "restore reused storage from the previous table UUID"
        );
        assert_eq!(
            schema.snapshot().tables[&(old.keyspace, old.name)].id,
            replacement.id
        );
    }

    #[tokio::test]
    async fn recovering_a_snapshot_corrects_an_already_registered_stale_schema() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("sm.snapshot");

        // Source node: a table carrying an EXTRA column, snapshotted.
        let mut source = FerrosStateMachine::with_snapshot_path(snapshot_path.clone());
        let mut extended = simple_table("agent_memory", "entity_store");
        extended.columns.insert(
            "description".to_string(),
            ColumnMetadata {
                name: "description".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        let extended_columns = extended.to_storage_schema().regular_columns.len();
        source
            .apply(vec![make_entry(
                1,
                1,
                RaftOp::CreateTable(Box::new(extended)),
            )])
            .await
            .unwrap();
        source.build_snapshot().await.unwrap();

        // Recovering node: engine ALREADY holds the table, with the stale
        // (pre-ALTER) column set — the situation node3 was stuck in.
        let engine = test_engine(dir.path());
        let stale = simple_table("agent_memory", "entity_store").to_storage_schema();
        let stale_columns = stale.regular_columns.len();
        engine.register_table(stale).unwrap();
        assert!(
            stale_columns < extended_columns,
            "the test must start with the engine holding FEWER columns"
        );

        let table_id = TableId::new("agent_memory", "entity_store");
        let mut restarted = FerrosStateMachine::with_side_effects_and_snapshot_path(
            Arc::new(test_schema_instance()),
            Arc::clone(&engine),
            snapshot_path,
        );
        assert!(restarted.recover_from_persisted_snapshot().unwrap());

        assert_eq!(
            engine
                .table_schema(&table_id)
                .expect("table registered")
                .regular_columns
                .len(),
            extended_columns,
            "the snapshot's column set must reach the engine, or the node serves \
             a schema its own Raft state contradicts"
        );
    }

    #[tokio::test]
    async fn set_ring_populates_live_ring_from_recovered_topology_state() {
        let mut sm = FerrosStateMachine::new();
        let host_id = Uuid::nil();
        let node_id = uuid_to_node_id(host_id);
        sm.apply(vec![
            make_entry(
                1,
                1,
                RaftOp::JoinNode(NodeInfo {
                    host_id,
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
            make_entry(
                1,
                2,
                RaftOp::AssignTokens {
                    node_id,
                    tokens: vec![-10, 10],
                },
            ),
        ])
        .await
        .unwrap();

        let ring = Arc::new(ArcSwap::from_pointee(TokenRing::new()));
        sm.set_ring(ring.clone());
        sm.sync_live_ring_from_state();

        let live = ring.load();
        assert_eq!(live.get_node(node_id).unwrap().addr, "10.0.0.1:7000");
        assert_eq!(live.replicas(-10, 1), vec![node_id]);
    }

    #[tokio::test]
    async fn recover_membership_from_topology_state_synthesizes_voters_when_empty() {
        let mut sm = FerrosStateMachine::new();
        let node1 = uuid_to_node_id(Uuid::from_u128(1));
        let node2 = uuid_to_node_id(Uuid::from_u128(2));
        sm.apply(vec![
            make_entry(
                1,
                1,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::from_u128(1),
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
            make_entry(
                1,
                2,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::from_u128(2),
                    addr: "10.0.0.2:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
        ])
        .await
        .unwrap();

        sm.last_membership = StoredMembership::default();

        assert!(
            sm.recover_membership_from_topology_state(),
            "topology-backed membership recovery should fire when voters are empty"
        );

        let configs = sm.last_membership.membership().get_joint_config();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].len(), 2);
        assert!(configs[0].contains(&node1));
        assert!(configs[0].contains(&node2));
        assert_eq!(sm.last_membership.log_id().clone(), sm.last_applied);
    }

    /// W1.21 / I-19: when state.members exists but the last committed log
    /// Membership entry encoded a joint config, recovery must NOT
    /// silently downgrade the joint config into a synthesized single-
    /// config (which would lose the in-flight transition and could
    /// split-brain on the next election). Instead it must return
    /// RecoveryError::JointConfigLost so the operator sees the
    /// discrepancy.
    #[tokio::test]
    async fn recover_membership_fails_loud_on_lost_joint_config() {
        let mut sm = FerrosStateMachine::new();
        let node1 = uuid_to_node_id(Uuid::from_u128(1));
        let node2 = uuid_to_node_id(Uuid::from_u128(2));
        let node3 = uuid_to_node_id(Uuid::from_u128(3));

        sm.apply(vec![
            make_entry(
                1,
                1,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::from_u128(1),
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
            make_entry(
                1,
                2,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::from_u128(2),
                    addr: "10.0.0.2:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
        ])
        .await
        .unwrap();

        // last_membership is empty, simulating the snapshot being older
        // than the latest Membership log entry.
        sm.last_membership = StoredMembership::default();

        // Construct the actual log Membership entry: a joint config in the
        // middle of swapping {node1, node2} → {node1, node3}.
        let joint = Membership::new(
            vec![
                BTreeSet::from([node1, node2]),
                BTreeSet::from([node1, node3]),
            ],
            None,
        );

        let result = sm.try_recover_membership_from_topology_state(Some(&joint));
        match result {
            Err(RecoveryError::JointConfigLost {
                log_configs,
                synthesized_voters,
            }) => {
                assert_eq!(
                    log_configs.len(),
                    2,
                    "the log Membership had two configs (joint)"
                );
                // synthesized would have been {node1, node2} from
                // state.members.
                assert!(synthesized_voters.contains(&node1));
                assert!(synthesized_voters.contains(&node2));
                assert!(!synthesized_voters.contains(&node3));
            }
            other => panic!("expected RecoveryError::JointConfigLost, got {other:?}"),
        }

        // The state machine's last_membership must NOT have been mutated
        // — fail loud means refuse to proceed, not patch silently.
        assert!(sm
            .last_membership
            .membership()
            .get_joint_config()
            .iter()
            .all(|c| c.is_empty()));
    }

    /// W1.21 helper-test: when the log Membership is a single-config that
    /// matches state.members, the function succeeds (not an error). This
    /// pins the happy path so the fail-loud path doesn't over-fire.
    #[tokio::test]
    async fn recover_membership_succeeds_on_matching_single_config() {
        let mut sm = FerrosStateMachine::new();
        let node1 = uuid_to_node_id(Uuid::from_u128(1));
        let node2 = uuid_to_node_id(Uuid::from_u128(2));

        sm.apply(vec![
            make_entry(
                1,
                1,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::from_u128(1),
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
            make_entry(
                1,
                2,
                RaftOp::JoinNode(NodeInfo {
                    host_id: Uuid::from_u128(2),
                    addr: "10.0.0.2:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
        ])
        .await
        .unwrap();
        sm.last_membership = StoredMembership::default();

        let single = Membership::new(vec![BTreeSet::from([node1, node2])], None);
        assert!(matches!(
            sm.try_recover_membership_from_topology_state(Some(&single)),
            Ok(true)
        ));
    }

    #[tokio::test]
    async fn recover_membership_from_topology_state_is_noop_when_membership_exists() {
        let mut sm = FerrosStateMachine::new();
        let host_id = Uuid::from_u128(1);
        let node_id = uuid_to_node_id(host_id);
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::JoinNode(NodeInfo {
                host_id,
                addr: "10.0.0.1:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            }),
        )])
        .await
        .unwrap();
        let existing = StoredMembership::new(
            sm.last_applied,
            Membership::new(vec![BTreeSet::from([node_id])], None),
        );
        sm.last_membership = existing.clone();

        assert!(
            !sm.recover_membership_from_topology_state(),
            "explicit membership should win over synthesized topology voters"
        );
        assert_eq!(
            sm.last_membership.membership().get_joint_config(),
            existing.membership().get_joint_config()
        );
    }

    #[tokio::test]
    async fn recover_membership_from_topology_state_refreshes_cached_snapshot_membership() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("state-machine.snapshot.bin");
        let mut sm = FerrosStateMachine::with_snapshot_path(snapshot_path.clone());
        let node1 = Uuid::from_u128(1);
        let node2 = Uuid::from_u128(2);
        sm.apply(vec![
            make_entry(
                1,
                1,
                RaftOp::JoinNode(NodeInfo {
                    host_id: node1,
                    addr: "10.0.0.1:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
            make_entry(
                1,
                2,
                RaftOp::JoinNode(NodeInfo {
                    host_id: node2,
                    addr: "10.0.0.2:7000".to_string(),
                    data_center: "dc1".to_string(),
                    rack: "rack1".to_string(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
            ),
        ])
        .await
        .unwrap();
        let _snapshot = sm.build_snapshot().await.unwrap();
        sm.last_membership = StoredMembership::default();
        if let Some((meta, bytes)) = sm.current_snapshot.write().as_mut() {
            meta.last_membership = StoredMembership::default();
            let mut data: SnapshotData = bincode::deserialize(bytes).unwrap();
            data.last_membership = StoredMembership::default();
            *bytes = bincode::serialize(&data).unwrap();
        }

        assert!(sm.recover_membership_from_topology_state());

        let cached = sm.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(
            cached.meta.last_membership.membership().get_joint_config()[0],
            BTreeSet::from([uuid_to_node_id(node1), uuid_to_node_id(node2)])
        );

        let mut restarted = FerrosStateMachine::with_snapshot_path(snapshot_path);
        assert!(restarted.recover_from_persisted_snapshot().unwrap());
        assert_eq!(
            restarted
                .applied_state()
                .await
                .unwrap()
                .1
                .membership()
                .get_joint_config()[0],
            BTreeSet::from([uuid_to_node_id(node1), uuid_to_node_id(node2)])
        );
    }

    /// t_919bb710 (found during outage 2026-10-10 hardening). A membership
    /// refresh after entries were applied past the cached snapshot must not
    /// re-stamp the old snapshot's bytes with the newer index. openraft's
    /// contract is that a snapshot at `last_log_id` IS the state after applying
    /// every entry up to it; a follower that installs a mislabelled one skips
    /// the entries in between and silently diverges.
    #[tokio::test]
    async fn membership_refresh_never_labels_old_state_with_a_newer_index() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("state-machine.snapshot.bin");
        let mut leader = FerrosStateMachine::with_snapshot_path(snapshot_path.clone());
        let join = |n: u128| {
            RaftOp::JoinNode(NodeInfo {
                host_id: Uuid::from_u128(n),
                addr: format!("10.0.0.{n}:7000"),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
                cql_broadcast: None,
            })
        };
        leader
            .apply(vec![
                make_entry(1, 1, join(1)),
                make_entry(1, 2, join(2)),
                make_entry(1, 3, RaftOp::CreateKeyspace(simple_keyspace("ks_a"))),
            ])
            .await
            .unwrap();
        let mut builder = leader.get_snapshot_builder().await;
        builder.build_snapshot().await.unwrap();
        // Applied past the snapshot (index 3): ks_b and ks_c exist only in
        // entries 4 and 5.
        leader
            .apply(vec![
                make_entry(1, 4, RaftOp::CreateKeyspace(simple_keyspace("ks_b"))),
                make_entry(1, 5, RaftOp::CreateKeyspace(simple_keyspace("ks_c"))),
            ])
            .await
            .unwrap();

        leader.last_membership = StoredMembership::default();
        assert!(leader.recover_membership_from_topology_state());

        let served = leader.get_current_snapshot().await.unwrap().unwrap();
        let served_index = served.meta.last_log_id.map(|id| id.index);
        let mut follower = FerrosStateMachine::new();
        follower
            .install_snapshot(&served.meta, served.snapshot)
            .await
            .unwrap();
        let keyspaces =
            |sm: &FerrosStateMachine| sm.state().keyspaces.keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            keyspaces(&follower),
            keyspaces(&leader),
            "a follower that installed the refreshed snapshot (labelled index {served_index:?}) \
             must hold the leader's state at that index"
        );
        assert_eq!(follower.last_applied_index(), leader.last_applied_index());
        assert_eq!(
            served.meta.last_membership.membership().get_joint_config()[0],
            BTreeSet::from([
                uuid_to_node_id(Uuid::from_u128(1)),
                uuid_to_node_id(Uuid::from_u128(2))
            ]),
            "the refresh still carries the recovered membership"
        );

        // The copy persisted for restart obeys the same rule.
        let mut restarted = FerrosStateMachine::with_snapshot_path(snapshot_path);
        assert!(restarted.recover_from_persisted_snapshot().unwrap());
        assert_eq!(keyspaces(&restarted), keyspaces(&leader));
        assert_eq!(restarted.last_applied_index(), leader.last_applied_index());
    }

    #[tokio::test]
    async fn apply_leave_node_cleans_tokens() {
        let mut sm = FerrosStateMachine::new();
        let node_id = 99u64;

        let entries = vec![
            make_entry(
                1,
                1,
                RaftOp::AssignTokens {
                    node_id,
                    tokens: vec![-50, 0, 50],
                },
            ),
            make_entry(1, 2, RaftOp::LeaveNode { node_id }),
        ];
        sm.apply(entries).await.unwrap();

        assert!(!sm.state().members.contains_key(&node_id));
        assert!(sm.state().token_map.values().all(|&n| n != node_id));
    }

    #[tokio::test]
    async fn apply_create_and_drop_role() {
        let mut sm = FerrosStateMachine::new();

        let role = RoleMetadata {
            name: "analyst".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
            scram: None,
        };

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateRole(role)),
            make_entry(1, 2, RaftOp::DropRole("analyst".to_string())),
        ];
        sm.apply(entries).await.unwrap();

        assert!(!sm.state().roles.contains_key("analyst"));
    }

    #[tokio::test]
    async fn apply_grant_role_is_additive_and_cycle_safe() {
        let mut sm = FerrosStateMachine::new();
        let mk = |name: &str| {
            RaftOp::CreateRole(RoleMetadata {
                name: name.to_string(),
                is_superuser: false,
                can_login: true,
                salted_hash: None,
                member_of: HashSet::new(),
                scram: None,
            })
        };
        let grant = |member: &str, role: &str| RaftOp::GrantRole {
            member: member.to_string(),
            granted_role: role.to_string(),
        };

        sm.apply(vec![
            make_entry(1, 1, mk("a")),
            make_entry(1, 2, mk("b")),
            make_entry(1, 3, mk("c")),
            // Two independent grants to the same member — additive, neither clobbers.
            make_entry(1, 4, grant("c", "a")),
            make_entry(1, 5, grant("c", "b")),
            // a becomes a member of b.
            make_entry(1, 6, grant("a", "b")),
            // b -> a would close a cycle (a is already a member of b): must be skipped.
            make_entry(1, 7, grant("b", "a")),
        ])
        .await
        .unwrap();

        let roles = &sm.state().roles;
        let c = &roles.get("c").unwrap().member_of;
        assert!(
            c.contains("a") && c.contains("b"),
            "additive grants preserved"
        );
        assert!(roles.get("a").unwrap().member_of.contains("b"));
        assert!(
            !roles.get("b").unwrap().member_of.contains("a"),
            "cycle-forming grant must be skipped at apply"
        );

        // Revoke removes exactly one edge.
        sm.apply(vec![make_entry(
            1,
            8,
            RaftOp::RevokeRole {
                member: "c".to_string(),
                granted_role: "a".to_string(),
            },
        )])
        .await
        .unwrap();
        let c = &sm.state().roles.get("c").unwrap().member_of;
        assert!(!c.contains("a") && c.contains("b"), "revoke is subtractive");
    }

    /// Cluster (Raft) replication test: a `RaftOp::CreateRole(role)`
    /// committed via the leader and applied on a follower's state
    /// machine must persist the role's `salted_hash` byte-for-byte.
    /// Pre-fix the router was sending roles with `salted_hash: None`,
    /// so even with replication working perfectly, every follower
    /// (and the leader's own state machine) ended up with `None` —
    /// login at any node returned `Bad credentials`.
    #[tokio::test]
    async fn apply_create_role_replicates_salted_hash() {
        let mut sm = FerrosStateMachine::new();

        let role = RoleMetadata {
            name: "raft_replicated".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: Some("$2a$10$leader-side-hash".to_string()),
            member_of: HashSet::new(),
            scram: None,
        };

        sm.apply(vec![make_entry(1, 1, RaftOp::CreateRole(role))])
            .await
            .unwrap();

        let stored = sm
            .state()
            .roles
            .get("raft_replicated")
            .cloned()
            .expect("role must persist after Raft apply");
        assert_eq!(
            stored.salted_hash.as_deref(),
            Some("$2a$10$leader-side-hash"),
            "follower must replicate the salted_hash exactly — was None pre-fix"
        );
        assert!(stored.can_login);
        assert!(!stored.is_superuser);
    }

    #[tokio::test]
    async fn apply_grant_and_revoke() {
        let mut sm = FerrosStateMachine::new();

        let grant = GrantEntry {
            role: "analyst".to_string(),
            resource: Resource::Keyspace("ks1".to_string()),
            permissions: [Permission::Select].into_iter().collect(),
        };

        let entries = vec![
            make_entry(1, 1, RaftOp::Grant(grant)),
            make_entry(
                1,
                2,
                RaftOp::Revoke {
                    role: "analyst".to_string(),
                    resource: Resource::Keyspace("ks1".to_string()),
                    permission: Permission::Select,
                },
            ),
        ];
        sm.apply(entries).await.unwrap();

        // After revoking the only permission, the grant entry should be removed.
        assert!(!sm.state().grants.contains_key("analyst"));
    }

    #[tokio::test]
    async fn canonical_projection_sync_once_for_two_entry_group() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            test_engine_with_strategy(dir.path(), ferrosa_storage::SyncStrategyConfig::default());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(schema, engine);
        let responses = sm
            .apply(vec![
                make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("batch_one"))),
                make_entry(1, 2, RaftOp::CreateKeyspace(simple_keyspace("batch_two"))),
            ])
            .await
            .unwrap();
        assert_eq!(responses.len(), 2);
        assert_eq!(sm.last_applied.unwrap().index, 2);
        assert_eq!(
            sm.projection_syncs
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one canonical durability fence must cover the owned apply group"
        );
    }

    #[tokio::test]
    async fn canonical_projection_sync_skips_empty_writes() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            test_engine_with_strategy(dir.path(), ferrosa_storage::SyncStrategyConfig::default());
        engine.register_system_tables().unwrap();
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::new(test_schema_instance()), engine);
        let config = sm.state.config.clone();
        sm.apply(vec![make_entry(1, 1, RaftOp::UpdateConfig(config))])
            .await
            .unwrap();
        assert_eq!(
            sm.projection_syncs
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a metadata-only entry must not force the data commit log"
        );
    }

    async fn permission_regrant_with_clock(later_clock: i64) {
        use crate::system_table_loader::SystemTableLoader;
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let mut sm = FerrosStateMachine::with_side_effects(
            Arc::new(test_schema_instance()),
            Arc::clone(&engine),
        );
        sm.system_writer
            .as_mut()
            .unwrap()
            .set_permission_timestamp_for_test(1_000_000);
        let grant = GrantEntry {
            role: "same_clock".into(),
            resource: Resource::Keyspace("ks".into()),
            permissions: [Permission::Select].into_iter().collect(),
        };
        let entries = vec![
            make_entry(1, 1, RaftOp::Grant(grant.clone())),
            make_entry(
                1,
                2,
                RaftOp::Revoke {
                    role: grant.role.clone(),
                    resource: grant.resource.clone(),
                    permission: Permission::Select,
                },
            ),
            make_entry(
                1,
                3,
                RaftOp::Grant(GrantEntry {
                    permissions: [Permission::Modify].into_iter().collect(),
                    ..grant.clone()
                }),
            ),
        ];
        sm.apply(entries[..2].to_vec()).await.unwrap();
        let tid = ferrosa_storage::TableId::new("system_auth", "role_permissions");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            grant.role.as_bytes().to_vec(),
        ));
        let partition = engine.read(&tid, &key).unwrap().unwrap();
        let tombstone = partition
            .rows
            .iter()
            .find(|r| r.clustering == grant.resource.to_string().as_bytes())
            .unwrap()
            .deletion;
        assert!(
            !tombstone.is_live(),
            "the actual old revoke tombstone must exist"
        );
        sm.system_writer
            .as_mut()
            .unwrap()
            .set_permission_timestamp_for_test(later_clock);
        sm.apply(vec![entries[2].clone()]).await.unwrap();
        let rows = SystemTableLoader::new(Arc::clone(&engine))
            .load_role_permissions()
            .unwrap();
        let expected = [Permission::Modify].into_iter().collect();
        assert!(
            rows.iter().any(|g| g.role == grant.role
                && g.resource == grant.resource
                && g.permissions == expected),
            "later grant must supersede the actual persisted revoke tombstone"
        );
        engine.flush(&tid).unwrap();
        drop(sm);
        drop(engine);
        let (reopened, pending) = StorageEngine::open(
            test_storage_config(dir.path(), ferrosa_storage::SyncStrategyConfig::Batch, 0),
            None,
        )
        .unwrap();
        reopened.register_system_tables().unwrap();
        reopened.replay_mutations(pending).unwrap();
        let reopened = Arc::new(reopened);
        let mut replay = FerrosStateMachine::with_side_effects(
            Arc::new(test_schema_instance()),
            Arc::clone(&reopened),
        );
        replay
            .system_writer
            .as_mut()
            .unwrap()
            .set_permission_timestamp_for_test(0);
        replay.apply(entries.clone()).await.unwrap();
        replay.apply(entries).await.unwrap();
        let rows = SystemTableLoader::new(reopened)
            .load_role_permissions()
            .unwrap();
        assert!(
            rows.iter().any(|g| g.role == grant.role
                && g.resource == grant.resource
                && g.permissions == expected),
            "backward-clock cold replay and repeated replay must converge"
        );
    }

    #[tokio::test]
    async fn permission_regrant_survives_equal_clock_tombstone() {
        permission_regrant_with_clock(1_000_000).await;
    }

    #[tokio::test]
    async fn permission_regrant_survives_backward_clock_and_restart() {
        permission_regrant_with_clock(42).await;
    }

    #[tokio::test]
    async fn permission_timestamp_exhaustion_refuses_publication() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::new(test_schema_instance()), engine);
        sm.system_writer
            .as_mut()
            .unwrap()
            .set_permission_timestamp_for_test(i64::MAX);
        let grant = GrantEntry {
            role: "exhausted".into(),
            resource: Resource::Keyspace("ks".into()),
            permissions: [Permission::Select].into_iter().collect(),
        };
        sm.apply(vec![make_entry(1, 1, RaftOp::Grant(grant.clone()))])
            .await
            .unwrap();
        let result = sm
            .apply(vec![make_entry(
                1,
                2,
                RaftOp::Revoke {
                    role: grant.role,
                    resource: grant.resource,
                    permission: Permission::Select,
                },
            )])
            .await;
        assert!(result.is_err());
        assert_eq!(sm.last_applied.unwrap().index, 1);
        assert!(sm.apply_failure.is_some());
    }

    async fn grant_projection_matches_full_state(revoke: bool) {
        use crate::system_table_loader::SystemTableLoader;
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(schema, Arc::clone(&engine));
        let original = GrantEntry {
            role: "projection_analyst".into(),
            resource: Resource::Keyspace("projection_ks".into()),
            permissions: [Permission::Select, Permission::Modify]
                .into_iter()
                .collect(),
        };
        let unrelated = GrantEntry {
            role: "unrelated_analyst".into(),
            resource: Resource::Keyspace("other_ks".into()),
            permissions: [Permission::Select].into_iter().collect(),
        };
        let same_role_other_resource = GrantEntry {
            role: original.role.clone(),
            resource: Resource::Keyspace("first_other_resource".into()),
            permissions: [Permission::Select].into_iter().collect(),
        };
        let initial = if revoke {
            original.clone()
        } else {
            GrantEntry {
                permissions: [Permission::Select].into_iter().collect(),
                ..original.clone()
            }
        };
        sm.apply(vec![
            make_entry(1, 1, RaftOp::Grant(same_role_other_resource.clone())),
            make_entry(1, 2, RaftOp::Grant(initial)),
            make_entry(1, 3, RaftOp::Grant(unrelated.clone())),
        ])
        .await
        .unwrap();
        let op = if revoke {
            RaftOp::Revoke {
                role: original.role.clone(),
                resource: original.resource.clone(),
                permission: Permission::Modify,
            }
        } else {
            RaftOp::Grant(GrantEntry {
                permissions: [Permission::Modify].into_iter().collect(),
                ..original.clone()
            })
        };
        sm.apply(vec![make_entry(1, 4, op)]).await.unwrap();
        let grants = SystemTableLoader::new(Arc::clone(&engine))
            .load_role_permissions()
            .unwrap();
        let persisted = grants
            .iter()
            .find(|g| g.role == original.role && g.resource == original.resource)
            .expect("remaining grant must exist physically");
        assert_eq!(persisted.permissions, sm.state.grants[&original.role].iter().find(|g| g.resource == original.resource).unwrap().permissions,
            "canonical row must contain the complete post-state, not the delta or a whole-row tombstone");
        assert!(grants.iter().any(|g| g.role == unrelated.role
            && g.resource == unrelated.resource
            && g.permissions == unrelated.permissions));
        assert!(grants
            .iter()
            .any(|g| g.role == same_role_other_resource.role
                && g.resource == same_role_other_resource.resource
                && g.permissions == same_role_other_resource.permissions));
        let expected = persisted.permissions.clone();
        engine
            .flush(&ferrosa_storage::TableId::new(
                "system_auth",
                "role_permissions",
            ))
            .unwrap();
        drop(sm);
        drop(engine);
        let (reopened, pending) = StorageEngine::open(
            test_storage_config(dir.path(), ferrosa_storage::SyncStrategyConfig::Batch, 0),
            None,
        )
        .unwrap();
        reopened.register_system_tables().unwrap();
        reopened.replay_mutations(pending).unwrap();
        let rows = SystemTableLoader::new(Arc::new(reopened))
            .load_role_permissions()
            .unwrap();
        assert!(
            rows.iter().any(|g| g.role == original.role
                && g.resource == original.resource
                && g.permissions == expected),
            "cold engine recovery must preserve the complete permission set"
        );
        assert!(rows.iter().any(|g| g.role == unrelated.role
            && g.resource == unrelated.resource
            && g.permissions == unrelated.permissions));
        assert!(rows.iter().any(|g| g.role == same_role_other_resource.role
            && g.resource == same_role_other_resource.resource
            && g.permissions == same_role_other_resource.permissions));
    }

    #[tokio::test]
    async fn grant_projection_persists_accumulated_permissions() {
        grant_projection_matches_full_state(false).await;
    }

    #[tokio::test]
    async fn revoke_projection_preserves_remaining_permissions() {
        grant_projection_matches_full_state(true).await;
    }

    #[tokio::test]
    async fn get_current_snapshot_returns_none_initially() {
        let mut sm = FerrosStateMachine::new();
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_none());
    }

    fn blank_entry(index: u64) -> Entry<FerrosRaftConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Blank,
        }
    }

    /// A snapshot at exactly `index`, built on a scratch state machine, ready
    /// to hand to `install_snapshot` as a leader would.
    async fn snapshot_at(index: u64) -> (SnapshotMeta<u64, BasicNode>, Vec<u8>) {
        let mut source = FerrosStateMachine::new();
        source
            .apply((1..=index).map(blank_entry).collect::<Vec<_>>())
            .await
            .unwrap();
        let snapshot = source.build_snapshot().await.unwrap();
        (snapshot.meta, snapshot.snapshot.into_inner())
    }

    fn current_snapshot_index(snapshot: &Option<Snapshot<FerrosRaftConfig>>) -> Option<u64> {
        snapshot
            .as_ref()
            .and_then(|s| s.meta.last_log_id)
            .map(|id| id.index)
    }

    /// T-F1-1 (F1, D1). openraft builds snapshots on the builder that
    /// `get_snapshot_builder` returns, in a detached task, and then serves
    /// followers from the LIVE state machine's `get_current_snapshot`. The
    /// built snapshot must therefore be visible on the live machine.
    ///
    /// Replaces `get_current_snapshot_after_build`, which built on the live
    /// state machine itself and so never exercised the path openraft uses.
    #[tokio::test]
    async fn get_current_snapshot_reflects_builder_built_snapshot() {
        let mut sm = FerrosStateMachine::new();
        sm.apply(vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("ks1"))),
            make_entry(1, 2, RaftOp::CreateKeyspace(simple_keyspace("ks2"))),
            blank_entry(3),
        ])
        .await
        .unwrap();

        let mut builder = sm.get_snapshot_builder().await;
        let built = builder.build_snapshot().await.unwrap();
        assert_eq!(built.meta.last_log_id.map(|id| id.index), Some(3));

        let live = sm
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("live state machine must serve the snapshot its builder built");
        assert_eq!(live.meta.last_log_id, built.meta.last_log_id);
        assert_eq!(live.meta.snapshot_id, built.meta.snapshot_id);
        let data: SnapshotData = decode_snapshot(&live.snapshot.into_inner()).unwrap();
        assert_eq!(
            data.state.keyspaces.keys().cloned().collect::<Vec<_>>(),
            vec!["ks1".to_string(), "ks2".to_string()],
            "served snapshot must carry the state as of index 3"
        );
    }

    /// T-F1-2 (F1, D1). A builder taken at index 5 that finishes after the
    /// live machine installed a snapshot at index 10 must not roll the
    /// shared slot back to 5.
    #[tokio::test]
    async fn snapshot_builder_does_not_overwrite_newer_installed_snapshot() {
        let mut sm = FerrosStateMachine::new();
        sm.apply((1..=5).map(blank_entry).collect::<Vec<_>>())
            .await
            .unwrap();
        let mut slow_builder = sm.get_snapshot_builder().await;

        let (meta10, bytes10) = snapshot_at(10).await;
        sm.install_snapshot(&meta10, Box::new(Cursor::new(bytes10)))
            .await
            .unwrap();

        let built5 = slow_builder.build_snapshot().await.unwrap();
        assert_eq!(built5.meta.last_log_id.map(|id| id.index), Some(5));

        let current = sm.get_current_snapshot().await.unwrap();
        assert_eq!(
            current_snapshot_index(&current),
            Some(10),
            "a slow build at 5 must not replace the installed snapshot at 10"
        );
        assert_eq!(
            current.unwrap().meta.snapshot_id,
            meta10.snapshot_id,
            "the installed snapshot itself must still be the one served"
        );
    }

    #[tokio::test]
    async fn stale_builder_cannot_replace_newer_durable_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state-machine.snapshot.bin");
        let durable = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut live = FerrosStateMachine::with_snapshot_path(path.clone());
        live.set_durable_applied_handle(Arc::clone(&durable));
        live.apply(vec![make_entry(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("before")),
        )])
        .await
        .unwrap();
        let mut stale = live.get_snapshot_builder().await;
        let mut newer = FerrosStateMachine::new();
        newer
            .apply(vec![
                make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("before"))),
                make_entry(1, 2, RaftOp::CreateKeyspace(simple_keyspace("after"))),
            ])
            .await
            .unwrap();
        let installed = newer.build_snapshot().await.unwrap();
        live.install_snapshot(&installed.meta, installed.snapshot)
            .await
            .unwrap();
        stale.build_snapshot().await.unwrap();

        assert_eq!(
            current_snapshot_index(&live.get_current_snapshot().await.unwrap()),
            Some(2)
        );
        assert_eq!(durable.load(std::sync::atomic::Ordering::Acquire), 2);
        let mut restarted = FerrosStateMachine::with_snapshot_path(path);
        assert!(restarted.recover_from_persisted_snapshot().unwrap());
        assert_eq!(
            restarted.last_applied_index(),
            Some(2),
            "the durable file must cover the advertised purge watermark"
        );
        assert!(restarted.state().keyspaces.contains_key("after"));
    }

    #[tokio::test]
    async fn snapshot_file_survives_cancelled_waiter_and_concurrent_promotion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let legacy_staging = path.with_extension("tmp");
        std::fs::write(&legacy_staging, b"another attempt owns this file").unwrap();
        let lock_path = dir.path().join("snapshot.bin.writer.lock");
        let coordinator = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        coordinator.lock().unwrap();
        let (meta1, bytes1) = snapshot_at(1).await;
        let (meta2, bytes2) = snapshot_at(2).await;
        let expected = bytes2.clone();
        let first_path = path.clone();
        let (started, running) = tokio::sync::oneshot::channel();
        let (finished, completion) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            ferrosa_net::task_pool::TaskPool::current("raft-log-store")
                .spawn_blocking(move || {
                    let _ = started.send(());
                    let result =
                        FerrosStateMachine::persist_snapshot_to_disk(&first_path, &meta1, &bytes1);
                    let _ = finished.send(result);
                })
                .await
                .unwrap();
        });
        running.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let second_path = path.clone();
        let second =
            ferrosa_net::task_pool::TaskPool::current("raft-log-store").spawn_blocking(move || {
                FerrosStateMachine::persist_snapshot_to_disk(&second_path, &meta2, &bytes2)
                    .map_err(Box::new)
            });
        coordinator.unlock().unwrap();
        completion.await.unwrap().unwrap();
        second.await.unwrap().unwrap();

        let stored: PersistedSnapshot =
            bincode::deserialize(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(stored.meta.last_log_id.map(|id| id.index), Some(2));
        assert_eq!(stored.bytes, expected);
        assert_eq!(
            std::fs::read(&legacy_staging).unwrap(),
            b"another attempt owns this file"
        );
        assert!(lock_path.is_file(), "the coordination inode must persist");
        let staging: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().starts_with("snapshot.bin."))
            .filter(|name| name.to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(
            staging.is_empty(),
            "completed attempts must leave no staging files"
        );
    }

    #[tokio::test]
    async fn snapshot_file_preserves_legacy_bincode_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let (meta, bytes) = snapshot_at(3).await;
        let expected = bincode::serialize(&PersistedSnapshot {
            meta: meta.clone(),
            bytes: bytes.clone(),
        })
        .unwrap();
        FerrosStateMachine::persist_snapshot_to_disk(&path, &meta, &bytes).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), expected);
    }

    #[tokio::test]
    async fn snapshot_file_keeps_newer_membership_at_the_same_applied_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let (older, older_bytes) = snapshot_at(5).await;
        let mut source = FerrosStateMachine::new();
        source
            .apply(vec![
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 3),
                    payload: EntryPayload::Membership(openraft::Membership::new(
                        vec![BTreeSet::from([1_u64])],
                        BTreeMap::from([(1_u64, BasicNode::new("node1"))]),
                    )),
                },
                blank_entry(5),
            ])
            .await
            .unwrap();
        let newer = source.build_snapshot().await.unwrap();
        let newer_bytes = newer.snapshot.into_inner();
        FerrosStateMachine::persist_snapshot_to_disk(&path, &newer.meta, &newer_bytes).unwrap();
        FerrosStateMachine::persist_snapshot_to_disk(&path, &older, &older_bytes).unwrap();
        assert!(!source.publish_snapshot(older, older_bytes));
        let stored: PersistedSnapshot =
            bincode::deserialize(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(stored.meta.last_membership, newer.meta.last_membership);
        assert_eq!(stored.bytes, newer_bytes);
    }

    async fn reject_incomplete_newer_snapshot_record(metadata_only: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot.bin");
        let (meta, bytes) = snapshot_at(2).await;
        let malformed = if metadata_only {
            bincode::serialize(&meta).unwrap()
        } else {
            let mut encoded = bincode::serialize(&PersistedSnapshot { meta, bytes }).unwrap();
            encoded.pop();
            encoded
        };
        std::fs::write(&path, &malformed).unwrap();
        let durable = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut builder = FerrosStateMachine::with_snapshot_path(path.clone());
        builder.set_durable_applied_handle(Arc::clone(&durable));
        builder.apply(vec![blank_entry(1)]).await.unwrap();
        assert!(
            builder.build_snapshot().await.is_err(),
            "a newer metadata prefix is not proof of a complete durable snapshot"
        );
        assert_eq!(durable.load(std::sync::atomic::Ordering::Acquire), 0);
        assert_eq!(std::fs::read(path).unwrap(), malformed);
    }

    #[tokio::test]
    async fn stale_snapshot_rejects_metadata_only_file() {
        reject_incomplete_newer_snapshot_record(true).await;
    }

    #[tokio::test]
    async fn stale_snapshot_rejects_truncated_payload() {
        reject_incomplete_newer_snapshot_record(false).await;
    }

    /// Steps for T-F1-3's interleavings.
    #[derive(Clone, Debug)]
    enum SnapshotStep {
        /// Apply this many blank entries on the live state machine.
        Apply(u64),
        /// Take a builder and build at once (openraft's normal path).
        Build,
        /// Take a builder now and hold it unbuilt.
        BeginBuild,
        /// Finish the oldest held builder.
        FinishBuild,
        /// Install a leader snapshot this many entries past `last_applied`.
        Install(u64),
    }

    fn snapshot_step() -> impl proptest::strategy::Strategy<Value = SnapshotStep> {
        use proptest::prelude::*;
        prop_oneof![
            (1u64..5).prop_map(SnapshotStep::Apply),
            Just(SnapshotStep::Build),
            Just(SnapshotStep::BeginBuild),
            Just(SnapshotStep::FinishBuild),
            (1u64..10).prop_map(SnapshotStep::Install),
        ]
    }

    /// Run `steps` and return the first violation of
    /// `durable_applied <= live current snapshot index`, if any.
    async fn run_snapshot_steps(steps: Vec<SnapshotStep>) -> Result<(), String> {
        let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
        let mut sm = FerrosStateMachine::with_snapshot_path(dir.path().join("sm.snapshot.bin"));
        let durable = Arc::new(std::sync::atomic::AtomicU64::new(0));
        sm.set_durable_applied_handle(Arc::clone(&durable));
        let mut held = std::collections::VecDeque::new();

        for (i, step) in steps.iter().enumerate() {
            let next = sm.last_applied_index().unwrap_or(0) + 1;
            match step {
                SnapshotStep::Apply(n) => {
                    sm.apply((next..next + n).map(blank_entry).collect::<Vec<_>>())
                        .await
                        .map_err(|e| e.to_string())?;
                }
                SnapshotStep::Build => {
                    let mut builder = sm.get_snapshot_builder().await;
                    builder.build_snapshot().await.map_err(|e| e.to_string())?;
                }
                SnapshotStep::BeginBuild => held.push_back(sm.get_snapshot_builder().await),
                SnapshotStep::FinishBuild => {
                    if let Some(mut builder) = held.pop_front() {
                        builder.build_snapshot().await.map_err(|e| e.to_string())?;
                    }
                }
                SnapshotStep::Install(k) => {
                    let (meta, bytes) = snapshot_at(next - 1 + k).await;
                    sm.install_snapshot(&meta, Box::new(Cursor::new(bytes)))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
            let durable_now = durable.load(std::sync::atomic::Ordering::Acquire);
            let live = sm.get_current_snapshot().await.map_err(|e| e.to_string())?;
            let live_index = current_snapshot_index(&live).unwrap_or(0);
            if durable_now > live_index {
                return Err(format!(
                    "after step {i} ({step:?}): durable_applied={durable_now} exceeds the live \
                     current snapshot index {live_index}; steps={steps:?}"
                ));
            }
        }
        Ok(())
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        /// T-F1-3 (F1, D1). The log store purges up to `durable_applied`, and
        /// replication serves the live `get_current_snapshot`. If the first
        /// ever exceeds the second, a follower behind the purge point is
        /// offered a snapshot that cannot reach it -- the 2026-10-10 loop.
        #[test]
        fn durable_applied_never_exceeds_live_current_snapshot_index(
            steps in proptest::collection::vec(snapshot_step(), 1..24)
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let outcome = runtime.block_on(run_snapshot_steps(steps));
            proptest::prop_assert!(outcome.is_ok(), "{}", outcome.unwrap_err());
        }
    }

    #[tokio::test]
    async fn applied_state_tracks_log_id() {
        let mut sm = FerrosStateMachine::new();
        let (la, _) = sm.applied_state().await.unwrap();
        assert_eq!(la, None);

        let entry = make_entry(1, 5, RaftOp::CreateKeyspace(simple_keyspace("ks1")));
        sm.apply(vec![entry]).await.unwrap();

        let (la, _) = sm.applied_state().await.unwrap();
        assert_eq!(la, Some(LogId::new(CommittedLeaderId::new(1, 0), 5)));
    }

    #[tokio::test]
    async fn apply_blank_entry() {
        let mut sm = FerrosStateMachine::new();
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), 1),
            payload: EntryPayload::Blank,
        };
        let results = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], RaftResponse::Ok));
    }

    #[tokio::test]
    async fn apply_update_config() {
        let mut sm = FerrosStateMachine::new();
        let config = ClusterConfig {
            cluster_name: "my-cluster".to_string(),
            ..ClusterConfig::default()
        };

        let entry = make_entry(1, 1, RaftOp::UpdateConfig(config));
        sm.apply(vec![entry]).await.unwrap();

        assert_eq!(sm.state().config.cluster_name, "my-cluster");
    }

    #[tokio::test]
    async fn state_machine_applies_approve_node() {
        let mut sm = FerrosStateMachine::new();
        let host_id = Uuid::new_v4();

        let entry = make_entry(1, 1, RaftOp::ApproveNode { host_id });
        sm.apply(vec![entry]).await.unwrap();

        assert!(sm.state().approved_nodes.contains(&host_id));
    }

    #[tokio::test]
    async fn approved_nodes_survive_snapshot() {
        let mut sm = FerrosStateMachine::new();
        let host_id = Uuid::new_v4();

        let entry = make_entry(1, 1, RaftOp::ApproveNode { host_id });
        sm.apply(vec![entry]).await.unwrap();

        // Build a snapshot.
        let snapshot = sm.build_snapshot().await.unwrap();
        let snap_meta = snapshot.meta.clone();
        let snap_bytes = snapshot.snapshot.into_inner();

        // Install into a fresh state machine.
        let mut sm2 = FerrosStateMachine::new();
        sm2.install_snapshot(&snap_meta, Box::new(Cursor::new(snap_bytes)))
            .await
            .unwrap();

        assert!(sm2.state().approved_nodes.contains(&host_id));
    }

    #[tokio::test]
    async fn apply_create_and_drop_type() {
        use ferrosa_common::CqlType;

        let mut sm = FerrosStateMachine::new();

        // Create keyspace first
        let ks = simple_keyspace("ks");
        let udt = UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("city".to_string(), CqlType::Varchar),
            ],
        };

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(ks)),
            make_entry(1, 2, RaftOp::CreateType(udt)),
        ];
        sm.apply(entries).await.unwrap();

        assert!(sm
            .state()
            .types
            .contains_key(&("ks".into(), "address".into())));

        // Drop type
        let entry = make_entry(
            1,
            3,
            RaftOp::DropType {
                keyspace: "ks".to_string(),
                name: "address".to_string(),
            },
        );
        sm.apply(vec![entry]).await.unwrap();

        assert!(!sm
            .state()
            .types
            .contains_key(&("ks".into(), "address".into())));
    }

    #[tokio::test]
    async fn types_survive_snapshot() {
        use ferrosa_common::CqlType;

        let mut sm = FerrosStateMachine::new();

        let udt = UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![("street".to_string(), CqlType::Varchar)],
        };

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("ks"))),
            make_entry(1, 2, RaftOp::CreateType(udt)),
        ];
        sm.apply(entries).await.unwrap();

        // Build snapshot
        let snapshot = sm.build_snapshot().await.unwrap();
        let snap_meta = snapshot.meta.clone();
        let snap_bytes = snapshot.snapshot.into_inner();

        // Install into fresh state machine
        let mut sm2 = FerrosStateMachine::new();
        sm2.install_snapshot(&snap_meta, Box::new(Cursor::new(snap_bytes)))
            .await
            .unwrap();

        assert!(sm2
            .state()
            .types
            .contains_key(&("ks".into(), "address".into())));
        let udt = &sm2.state().types[&("ks".into(), "address".into())];
        assert_eq!(udt.fields.len(), 1);
        assert_eq!(udt.fields[0].0, "street");
    }

    #[tokio::test]
    async fn drop_keyspace_cascades_types() {
        use ferrosa_common::CqlType;

        let mut sm = FerrosStateMachine::new();

        let udt = UserTypeMetadata {
            keyspace: "doomed".to_string(),
            name: "address".to_string(),
            fields: vec![("street".to_string(), CqlType::Varchar)],
        };

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("doomed"))),
            make_entry(1, 2, RaftOp::CreateType(udt)),
            make_entry(1, 3, RaftOp::DropKeyspace("doomed".to_string())),
        ];
        sm.apply(entries).await.unwrap();

        assert!(
            !sm.state()
                .types
                .contains_key(&("doomed".into(), "address".into())),
            "types in dropped keyspace should be removed"
        );
    }

    // ---- BUG-011: topology changes must update live ring ----------------

    #[tokio::test]
    async fn join_node_updates_live_ring() {
        use crate::ring::TokenRing;
        use arc_swap::ArcSwap;

        let ring = Arc::new(ArcSwap::from_pointee(TokenRing::new()));
        let mut sm = FerrosStateMachine::new();
        sm.set_ring(ring.clone());

        let host_id = Uuid::new_v4();
        let node_id = super::super::uuid_to_node_id(host_id);
        let node = NodeInfo {
            host_id,
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };

        // Disable auto-token-assignment so the explicit `AssignTokens`
        // entry is the sole source of tokens — this test exercises
        // ring-sync, not the auto-assignment policy.
        let cfg = ClusterConfig {
            num_tokens: 0,
            ..ClusterConfig::default()
        };
        let entries = vec![
            make_entry(1, 0, RaftOp::UpdateConfig(cfg)),
            make_entry(1, 1, RaftOp::JoinNode(node)),
            make_entry(
                1,
                2,
                RaftOp::AssignTokens {
                    node_id,
                    tokens: vec![-100, 0, 100],
                },
            ),
        ];
        sm.apply(entries).await.unwrap();

        // The live ring must now contain the new node and its tokens.
        let live_ring = ring.load();
        assert!(
            live_ring.get_node(node_id).is_some(),
            "live ring must contain the new node after JoinNode"
        );
        assert_eq!(
            live_ring.tokens_for_node(node_id).len(),
            3,
            "live ring must have 3 tokens after AssignTokens"
        );
    }

    #[tokio::test]
    async fn topology_changes_update_ring_observer() {
        use crate::ring::TokenRing;
        use arc_swap::ArcSwap;

        let coordinator_ring = Arc::new(ArcSwap::from_pointee(TokenRing::new()));
        let observer = Arc::new(ArcSwap::from_pointee(None));
        let mut sm = FerrosStateMachine::new();
        sm.set_ring(coordinator_ring);
        sm.set_ring_observer(observer.clone());

        let host_a = Uuid::new_v4();
        let host_b = Uuid::new_v4();
        let node_a = super::super::uuid_to_node_id(host_a);
        let node_b = super::super::uuid_to_node_id(host_b);
        let info_a = NodeInfo {
            host_id: host_a,
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };
        let info_b = NodeInfo {
            host_id: host_b,
            addr: "10.0.0.2:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };

        sm.apply(vec![
            make_entry(
                1,
                0,
                RaftOp::UpdateConfig(ClusterConfig {
                    num_tokens: 0,
                    ..ClusterConfig::default()
                }),
            ),
            make_entry(1, 1, RaftOp::JoinNode(info_a)),
            make_entry(1, 2, RaftOp::JoinNode(info_b)),
            make_entry(
                1,
                3,
                RaftOp::AssignTokens {
                    node_id: node_a,
                    tokens: vec![10, 20],
                },
            ),
            make_entry(
                1,
                4,
                RaftOp::AssignTokens {
                    node_id: node_b,
                    tokens: vec![30, 40],
                },
            ),
        ])
        .await
        .unwrap();

        let observed_snapshot = observer.load_full();
        let observed = observed_snapshot
            .as_ref()
            .clone()
            .expect("ring observer must be populated");
        assert_eq!(observed.tokens_for_node(node_a).len(), 2);
        assert_eq!(observed.tokens_for_node(node_b).len(), 2);
    }

    #[tokio::test]
    async fn leave_node_updates_live_ring() {
        use crate::ring::TokenRing;
        use arc_swap::ArcSwap;

        let ring = Arc::new(ArcSwap::from_pointee(TokenRing::new()));
        let mut sm = FerrosStateMachine::new();
        sm.set_ring(ring.clone());

        let host_id = Uuid::new_v4();
        let node_id = super::super::uuid_to_node_id(host_id);
        let node = NodeInfo {
            host_id,
            addr: "10.0.0.1:7000".to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        };

        // Join, assign tokens, then leave.
        let entries = vec![
            make_entry(1, 1, RaftOp::JoinNode(node)),
            make_entry(
                1,
                2,
                RaftOp::AssignTokens {
                    node_id,
                    tokens: vec![10, 20, 30],
                },
            ),
            make_entry(1, 3, RaftOp::LeaveNode { node_id }),
        ];
        sm.apply(entries).await.unwrap();

        // After leave, the node must be gone from the live ring.
        let live_ring = ring.load();
        assert!(
            live_ring.get_node(node_id).is_none(),
            "live ring must not contain the node after LeaveNode"
        );
        assert_eq!(
            live_ring.token_count(),
            0,
            "live ring must have no tokens after LeaveNode"
        );
    }

    // -- System table writer integration tests --

    fn test_schema_instance() -> ferrosa_schema::Schema {
        use ferrosa_schema::{
            AuthMethod, LogAuditSink, PasswordHasher, PasswordPolicy, RateLimitConfig, SchemaConfig,
        };
        let config = SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: ferrosa_schema::startup::DeploymentMode::Development,
        };
        ferrosa_schema::Schema::new(config).unwrap()
    }

    fn test_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        test_engine_with_strategy(dir, Default::default())
    }

    fn test_engine_with_strategy(
        dir: &std::path::Path,
        sync_strategy: ferrosa_storage::SyncStrategyConfig,
    ) -> Arc<StorageEngine> {
        test_engine_with_reserve(dir, sync_strategy, 0)
    }

    fn test_engine_with_reserve(
        dir: &std::path::Path,
        sync_strategy: ferrosa_storage::SyncStrategyConfig,
        reserve_bytes: u64,
    ) -> Arc<StorageEngine> {
        let config = test_storage_config(dir, sync_strategy, reserve_bytes);
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn test_storage_config(
        dir: &std::path::Path,
        sync_strategy: ferrosa_storage::SyncStrategyConfig,
        reserve_bytes: u64,
    ) -> ferrosa_storage::engine::StorageEngineConfig {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig};
        ferrosa_storage::engine::StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                sync_strategy,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: reserve_bytes,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
        }
    }

    #[tokio::test]
    async fn failed_projection_group_does_not_publish_earlier_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine_with_reserve(
            dir.path(),
            ferrosa_storage::SyncStrategyConfig::Batch,
            u64::MAX,
        );
        engine.register_system_tables().unwrap();
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::new(test_schema_instance()), engine);
        let before = sm.state.config.auto_join;
        let mut config = sm.state.config.clone();
        config.auto_join = !before;
        let result = sm
            .apply(vec![
                make_entry(1, 1, RaftOp::UpdateConfig(config)),
                make_entry(
                    1,
                    2,
                    RaftOp::CreateKeyspace(simple_keyspace("failed_group")),
                ),
            ])
            .await;
        assert!(result.is_err());
        assert!(sm.last_applied.is_none());
        assert_eq!(
            sm.state.config.auto_join, before,
            "the group prefix must not publish before its durability fence"
        );
        assert!(sm.apply_failure.is_some());
    }

    async fn hard_projection_failure_must_not_publish(drop_keyspace: bool) {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine_with_reserve(
            dir.path(),
            ferrosa_storage::SyncStrategyConfig::Batch,
            u64::MAX,
        );
        engine.register_system_tables().unwrap();
        assert!(matches!(
            engine.check_write_admission(),
            Err(ferrosa_common::Error::InvalidData(_))
        ));
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(Arc::clone(&schema), engine);
        let keyspace = simple_keyspace("projection_failure");
        let op = if drop_keyspace {
            schema.create_keyspace_internal(keyspace.clone()).unwrap();
            sm.state
                .keyspaces
                .insert(keyspace.name.clone(), keyspace.clone());
            RaftOp::DropKeyspace(keyspace.name)
        } else {
            RaftOp::CreateKeyspace(keyspace)
        };
        let result = sm.apply(vec![make_entry(1, 1, op)]).await;
        assert!(
            result.is_err(),
            "real canonical projection write refusal must reach storage failure, got {result:?}"
        );
        assert!(
            sm.last_applied.is_none(),
            "failed canonical projection must remain replayable"
        );
        assert!(
            sm.apply_failure.is_some(),
            "a true persistence failure must latch"
        );
        let mut builder = sm.get_snapshot_builder().await;
        assert!(
            builder.build_snapshot().await.is_err(),
            "failed projection cannot be snapshotted and purged"
        );
    }

    #[tokio::test]
    async fn warn_replay_does_not_hide_real_projection_storage_failure() {
        hard_projection_failure_must_not_publish(false).await;
    }

    #[tokio::test]
    async fn critical_projection_storage_failure_does_not_publish_applied_index() {
        hard_projection_failure_must_not_publish(true).await;
    }

    /// A node that receives a snapshot missing a local data-bearing table must
    /// refuse the install rather than inferring a compacted DROP TABLE from
    /// absence alone. Current snapshots carry no explicit drop markers, so the
    /// safe behavior is fail-loud preservation of local artifacts.
    #[tokio::test]
    async fn snapshot_install_refuses_ambiguous_absent_data_bearing_table() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let mut lagging = FerrosStateMachine::with_side_effects(
            Arc::new(test_schema_instance()),
            Arc::clone(&engine),
        );
        lagging
            .apply(vec![
                make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("lag_ks"))),
                make_entry(
                    1,
                    2,
                    RaftOp::CreateTable(Box::new(simple_table("lag_ks", "keep"))),
                ),
                make_entry(
                    1,
                    3,
                    RaftOp::CreateTable(Box::new(simple_table("lag_ks", "gone"))),
                ),
            ])
            .await
            .unwrap();
        let gone = TableId::new("lag_ks", "gone");
        let gone_dir = dir.path().join("sstables").join(gone.to_string());
        assert!(engine.table_schema(&gone).is_some() && gone_dir.exists());
        std::fs::write(gone_dir.join("1-Data.db"), b"local durable artifact").unwrap();

        // The leader dropped `gone` while this node was away.
        let mut leader = FerrosStateMachine::new();
        leader
            .apply(vec![
                make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("lag_ks"))),
                make_entry(
                    1,
                    2,
                    RaftOp::CreateTable(Box::new(simple_table("lag_ks", "keep"))),
                ),
                make_entry(
                    1,
                    3,
                    RaftOp::CreateTable(Box::new(simple_table("lag_ks", "gone"))),
                ),
                make_entry(
                    1,
                    4,
                    RaftOp::DropTable {
                        keyspace: "lag_ks".into(),
                        table: "gone".into(),
                    },
                ),
            ])
            .await
            .unwrap();
        let system_dirs: Vec<String> = std::fs::read_dir(dir.path().join("sstables"))
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|name| name.starts_with("system"))
            .collect();
        assert!(
            !system_dirs.is_empty(),
            "the engine registered system tables"
        );
        let snapshot = leader.build_snapshot().await.unwrap();
        let err = lagging
            .install_snapshot(
                &snapshot.meta,
                Box::new(Cursor::new(snapshot.snapshot.into_inner())),
            )
            .await
            .expect_err("ambiguous data-bearing snapshot absence must be refused");
        let err = err.to_string();
        assert!(err.contains("refusing Raft snapshot install"), "{err}");
        assert!(err.contains("lag_ks.gone"), "{err}");

        assert!(
            engine.table_schema(&gone).is_some(),
            "ambiguous snapshot absence must not unregister the local table"
        );
        assert!(
            gone_dir.exists(),
            "ambiguous snapshot absence must preserve the SSTable directory"
        );
        assert!(
            engine
                .table_schema(&TableId::new("lag_ks", "keep"))
                .is_some(),
            "tables the snapshot still has are untouched"
        );
        for name in &system_dirs {
            assert!(
                dir.path().join("sstables").join(name).exists(),
                "system table {name} must never be swept"
            );
        }
    }

    /// W1.7 — physical failure is a storage error, not a successful
    /// application carrying an ordinary command-error response.
    ///
    /// The engine's `register_table` calls `std::fs::create_dir_all` on
    /// `<data_dir>/sstables/<keyspace>:<table>`.  If that path already
    /// exists as a regular file, the call fails with "Not a directory".
    /// We pre-create the file, then issue `RaftOp::CreateTable` and
    /// confirm apply refuses publication with the original registration reason,
    /// retains the preceding index, and prevents a successor from applying.
    #[tokio::test]
    async fn physical_retirement_tracks_successful_snapshot_install() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = FerrosStateMachine::new();
        source
            .apply(vec![make_entry(
                1,
                7,
                RaftOp::CreateKeyspace(simple_keyspace("installed_ks")),
            )])
            .await
            .unwrap();
        let snapshot = source.build_snapshot().await.unwrap();
        let expected = snapshot.meta.last_log_id;
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let path = dir.path().join("installed.snapshot.bin");
        let mut target = FerrosStateMachine::with_side_effects_and_snapshot_path(
            Arc::clone(&schema),
            engine,
            path.clone(),
        );
        let lifecycle = target.physical_lifecycle();
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        assert!(schema.snapshot().keyspaces.contains_key("installed_ks"));
        assert_eq!(target.last_applied, expected);
        assert!(path.is_file());
        drop(target);
        assert_eq!(
            lifecycle.drain().await.unwrap(),
            expected,
            "completed snapshot must cover the persisted committed retirement target"
        );
    }

    #[tokio::test]
    async fn physical_retirement_refuses_failed_snapshot_install() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = FerrosStateMachine::new();
        source
            .apply(vec![
                make_entry(
                    1,
                    1,
                    RaftOp::CreateKeyspace(simple_keyspace("failed_install")),
                ),
                make_entry(
                    1,
                    2,
                    RaftOp::CreateTable(Box::new(simple_table("failed_install", "victim"))),
                ),
            ])
            .await
            .unwrap();
        let snapshot = source.build_snapshot().await.unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let blocker = dir.path().join("sstables/failed_install.victim");
        std::fs::create_dir_all(blocker.parent().unwrap()).unwrap();
        std::fs::write(&blocker, b"owned physical refusal").unwrap();
        let mut target = FerrosStateMachine::with_side_effects_and_snapshot_path(
            Arc::new(test_schema_instance()),
            engine,
            dir.path().join("failed.snapshot.bin"),
        );
        let lifecycle = target.physical_lifecycle();
        assert!(target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err());
        assert!(target.last_applied.is_none());
        drop(target);
        assert!(
            lifecycle.drain().await.is_err(),
            "failed restore must not authorize Pair retirement"
        );
        assert_eq!(std::fs::read(blocker).unwrap(), b"owned physical refusal");
    }

    #[tokio::test]
    async fn apply_command_propagates_engine_register_failure() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        // Pre-create the path the engine will try to mkdir, as a file.
        let sabotage_dir = dir.path().join("sstables");
        std::fs::create_dir_all(&sabotage_dir).unwrap();
        let target = sabotage_dir.join("regress_ks.victim_tbl");
        std::fs::write(&target, b"not a directory").unwrap();

        let schema = test_schema_instance();
        let mut sm = FerrosStateMachine::with_side_effects(Arc::new(schema), Arc::clone(&engine));

        let ks = simple_keyspace("regress_ks");
        let table = simple_table("regress_ks", "victim_tbl");
        // Apply CreateKeyspace first so the schema/system table writes
        // succeed; only the engine.register_table on the table should
        // fail because the sabotage file blocks the mkdir.
        let create_ks_entry = make_entry(1, 1, RaftOp::CreateKeyspace(ks));
        let create_tbl_entry = make_entry(1, 2, RaftOp::CreateTable(Box::new(table)));

        sm.apply(vec![create_ks_entry]).await.unwrap();
        let error = sm.apply(vec![create_tbl_entry]).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("engine.register_table"), "{message}");
        assert!(
            message.contains("regress_ks") && message.contains("victim_tbl"),
            "{message}"
        );
        assert!(
            sm.apply_failure.is_some(),
            "A physical refusal must close the apply fence."
        );
        assert_eq!(sm.last_applied.unwrap().index, 1);
        assert!(!sm.state.tables.contains_key(&("regress_ks".into(), "victim_tbl".into())),
            "The unacknowledged log entry, not published metadata, retains the committed intent for replay.");
        assert!(sm
            .apply(vec![make_entry(
                1,
                3,
                RaftOp::CreateTable(Box::new(simple_table("regress_ks", "valid_tbl")))
            )])
            .await
            .is_err());
        assert!(engine
            .table_schema(&TableId::new("regress_ks", "valid_tbl"))
            .is_none());
    }

    /// Repro for forge t_86f9259d: a keyspace created through the Raft
    /// state-machine apply path must be visible in the **externally shared**
    /// `Schema` that fresh CQL connections read (`state.schema.snapshot()` and
    /// `system_schema.keyspaces`).
    ///
    /// In single-node Raft-metadata mode the CQL router and the Raft state
    /// machine share the same `Arc<Schema>` (see
    /// `ModeController::transition_to_cluster`, which passes
    /// `self.schema.clone()` to `with_side_effects`). The creating connection
    /// succeeds because it applied the DDL in-session; a *fresh* connection
    /// reads the keyspace exclusively from this shared `Schema`. If the apply
    /// path mutates only the state machine's private `self.state.keyspaces`
    /// and skips the shared `Schema`, the keyspace is permanently invisible to
    /// new connections.
    #[tokio::test]
    async fn create_keyspace_via_raft_apply_is_visible_in_shared_schema() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        // The router holds this Arc; the state machine gets a clone — exactly
        // how `transition_to_cluster` wires `self.schema.clone()`.
        let shared_schema = Arc::new(test_schema_instance());
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&shared_schema), Arc::clone(&engine));

        let ks = simple_keyspace("vis_test");
        let entry = make_entry(1, 1, RaftOp::CreateKeyspace(ks));
        sm.apply(vec![entry]).await.unwrap();

        // A fresh connection reads from the shared Schema snapshot.
        let snap = shared_schema.snapshot();
        assert!(
            snap.keyspaces.contains_key("vis_test"),
            "keyspace created via Raft apply must be visible to fresh connections \
             through the shared Schema (system_schema.keyspaces reads this snapshot)"
        );
    }

    /// Repro for forge t_86f9259d (the persistent, fresh-connection-only
    /// invisibility): when a `CreateKeyspace` is applied while the keyspace is
    /// already present in the state machine's private `self.state.keyspaces`
    /// but absent from the externally shared `Schema`, the apply path must
    /// still reconcile the shared `Schema` — not silently skip it.
    ///
    /// The divergence arises whenever `self.state` and the shared `Schema`
    /// fall out of step (e.g. a duplicate proposal replay, or a state-machine
    /// recovery that repopulated `self.state` from a persisted Raft snapshot
    /// while a fresh `Schema` was constructed at process start). The previous
    /// implementation gated the shared-`Schema` write behind the
    /// "newly inserted into `self.state`" guard, so the keyspace became
    /// permanently invisible to every new CQL connection (which reads the
    /// shared `Schema`), exactly matching the reported black-box symptom.
    #[tokio::test]
    async fn create_keyspace_reconciles_shared_schema_even_when_state_already_has_it() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let shared_schema = Arc::new(test_schema_instance());
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&shared_schema), Arc::clone(&engine));

        // Simulate the state-machine state already containing the keyspace
        // (recovered Raft state / duplicate proposal) while the shared Schema
        // — read by fresh CQL connections — does not yet know about it.
        let ks = simple_keyspace("vis_test");
        sm.state.keyspaces.insert(ks.name.clone(), ks.clone());
        assert!(
            !shared_schema.snapshot().keyspaces.contains_key("vis_test"),
            "precondition: shared Schema must start without vis_test"
        );

        let entry = make_entry(1, 1, RaftOp::CreateKeyspace(ks));
        sm.apply(vec![entry]).await.unwrap();

        assert!(
            shared_schema.snapshot().keyspaces.contains_key("vis_test"),
            "CreateKeyspace apply must reconcile the shared Schema so fresh \
             connections see the keyspace, even when self.state already had it"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn held_schema_lease_allows_async_observer_control() {
        let schema = Arc::new(test_schema_instance());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (progress_tx, progress_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _lease = schema.lifecycle_read();
            ready_tx.send(()).unwrap();
            progress_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .is_ok()
        });
        ready_rx.recv().unwrap();
        tokio::task::yield_now().await;
        progress_tx.send(()).unwrap();
        assert!(
            holder.join().unwrap(),
            "observer must run while the fixture lease is held"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn raft_create_table_keeps_async_progress_while_schema_lease_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(Arc::clone(&schema), engine);
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("async_apply")),
        )])
        .await
        .unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (progress_tx, progress_rx) = std::sync::mpsc::channel();
        let held_schema = Arc::clone(&schema);
        let holder = std::thread::spawn(move || {
            let _lease = held_schema.lifecycle_read();
            ready_tx.send(()).unwrap();
            // Cleanup bound, not a scheduling deadline: a broken apply must
            // eventually release so the test reports its assertion failure.
            progress_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .is_ok()
        });
        ready_rx.recv().unwrap();
        let apply = sm.apply(vec![make_entry(
            1,
            2,
            RaftOp::CreateTable(Box::new(simple_table("async_apply", "t"))),
        )]);
        tokio::pin!(apply);
        tokio::select! {
            biased;
            result = &mut apply => { result.unwrap(); }
            () = async { let _released_after_cleanup = progress_tx.send(()); } => { apply.await.unwrap(); }
        }
        assert!(
            holder.join().unwrap(),
            "Raft apply blocked the only async worker while waiting for the schema lease"
        );
        assert!(schema
            .snapshot()
            .tables
            .contains_key(&("async_apply".into(), "t".into())));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_raft_schema_lease_apply_is_reaped_before_snapshot_and_next_entry() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(Arc::clone(&schema), engine);
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("cancel_apply")),
        )])
        .await
        .unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held_schema = Arc::clone(&schema);
        let holder = std::thread::spawn(move || {
            let _lease = held_schema.lifecycle_read();
            ready_tx.send(()).unwrap();
            release_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .is_ok()
        });
        ready_rx.recv().unwrap();
        {
            let apply = sm.apply(vec![make_entry(
                1,
                2,
                RaftOp::CreateTable(Box::new(simple_table("cancel_apply", "first"))),
            )]);
            tokio::pin!(apply);
            std::future::poll_fn(|cx| match std::future::Future::poll(apply.as_mut(), cx) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => {
                    panic!("held lease must leave apply pending: {result:?}")
                }
            })
            .await;
            // Drop only the waiter. The admitted operation must remain owned.
        }
        let _released_after_cleanup = release_tx.send(());
        assert!(
            holder.join().unwrap(),
            "cancelling apply must not wait for the fixture cleanup deadline"
        );
        let mut builder = sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let data = decode_snapshot(snapshot.snapshot.get_ref()).unwrap();
        assert_eq!(data.last_applied.unwrap().index, 2);
        assert!(data
            .state
            .tables
            .contains_key(&("cancel_apply".into(), "first".into())));
        sm.apply(vec![make_entry(
            1,
            3,
            RaftOp::CreateTable(Box::new(simple_table("cancel_apply", "second"))),
        )])
        .await
        .unwrap();
        assert_eq!(sm.applied_state().await.unwrap().0.unwrap().index, 3);
        assert!(sm
            .state
            .tables
            .contains_key(&("cancel_apply".into(), "first".into())));
        assert!(sm
            .state
            .tables
            .contains_key(&("cancel_apply".into(), "second".into())));
    }

    async fn cancelled_mixed_input_requires_reissue(membership: bool) {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(Arc::clone(&schema), engine);
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("mixed_cancel")),
        )])
        .await
        .unwrap();
        let boundary = if membership {
            Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 3),
                payload: EntryPayload::Membership(openraft::Membership::new(
                    vec![BTreeSet::from([1_u64])],
                    BTreeMap::from([(1_u64, BasicNode::new("node1"))]),
                )),
            }
        } else {
            blank_entry(3)
        };
        let suffix = make_entry(
            1,
            4,
            RaftOp::CreateTable(Box::new(simple_table("mixed_cancel", "suffix"))),
        );
        let later = make_entry(
            1,
            5,
            RaftOp::CreateTable(Box::new(simple_table("mixed_cancel", "later"))),
        );
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held = Arc::clone(&schema);
        let holder = std::thread::spawn(move || {
            let _lease = held.lifecycle_read();
            ready_tx.send(()).unwrap();
            release_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .is_ok()
        });
        ready_rx.recv().unwrap();
        {
            let apply = sm.apply(vec![
                make_entry(
                    1,
                    2,
                    RaftOp::CreateTable(Box::new(simple_table("mixed_cancel", "first"))),
                ),
                boundary.clone(),
                suffix.clone(),
            ]);
            tokio::pin!(apply);
            std::future::poll_fn(|cx| match std::future::Future::poll(apply.as_mut(), cx) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => {
                    panic!("held first group must remain pending: {result:?}")
                }
            })
            .await;
        }
        release_tx.send(()).unwrap();
        assert!(holder.join().unwrap());
        assert_eq!(sm.applied_state().await.unwrap().0.unwrap().index, 2);
        let skipped = sm.apply(vec![later.clone()]).await;
        assert!(skipped.is_err(), "must not acknowledge index5 while the cancelled input's committed indexes3/4 are unconsumed: {skipped:?}");
        assert_eq!(sm.last_applied.unwrap().index, 2);
        assert!(!schema
            .snapshot()
            .tables
            .contains_key(&("mixed_cancel".into(), "later".into())));
        sm.apply(vec![boundary, suffix]).await.unwrap();
        assert_eq!(sm.last_applied.unwrap().index, 4);
        assert!(schema
            .snapshot()
            .tables
            .contains_key(&("mixed_cancel".into(), "suffix".into())));
        sm.apply(vec![later]).await.unwrap();
        assert_eq!(sm.last_applied.unwrap().index, 5);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_mixed_blank_input_refuses_skipping_unconsumed_suffix() {
        cancelled_mixed_input_requires_reissue(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_mixed_membership_input_refuses_skipping_unconsumed_suffix() {
        cancelled_mixed_input_requires_reissue(true).await;
    }

    async fn snapshot_supersedes_abandoned_prefix(snapshot_index: u64) {
        let mut source = FerrosStateMachine::new();
        source
            .apply(vec![blank_entry(snapshot_index)])
            .await
            .unwrap();
        let snapshot = source.build_snapshot().await.unwrap();
        let mut target = FerrosStateMachine::new();
        // The two held-worker cancellation cases establish this runtime fence.
        target.unconsumed_input = Some(3);
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        assert!(
            target
                .apply(vec![blank_entry(snapshot_index + 2)])
                .await
                .is_err(),
            "an authoritative snapshot must not permit skipping its next index"
        );
        target
            .apply(vec![blank_entry(snapshot_index + 1)])
            .await
            .unwrap();
        assert_eq!(target.last_applied.unwrap().index, snapshot_index + 1);
    }

    #[tokio::test]
    async fn snapshot_covering_part_of_cancelled_suffix_requires_its_next_index() {
        snapshot_supersedes_abandoned_prefix(3).await;
    }

    #[tokio::test]
    async fn snapshot_covering_cancelled_suffix_allows_ordered_progress() {
        snapshot_supersedes_abandoned_prefix(4).await;
    }

    async fn cancelled_projection_waiter_retains_fence(
        strategy: ferrosa_storage::SyncStrategyConfig,
        batched: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine_with_strategy(dir.path(), strategy);
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(schema, Arc::clone(&engine));
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("system_wait")),
        )])
        .await
        .unwrap();
        let recorder = sm
            .system_writer
            .as_ref()
            .unwrap()
            .last_apply_thread_handle();
        *recorder.lock().unwrap() = None;
        let syncs = Arc::clone(&sm.projection_syncs);
        let before = syncs.load(std::sync::atomic::Ordering::SeqCst);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held_engine = Arc::clone(&engine);
        let holder = std::thread::spawn(move || {
            let mut progressed = false;
            held_engine.with_commit_log_flush_held_for_test(|| {
                ready_tx.send(()).unwrap();
                progressed = release_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .is_ok();
            });
            progressed
        });
        ready_rx.recv().unwrap();
        {
            let mut entries = vec![make_entry(
                1,
                2,
                RaftOp::CreateTable(Box::new(simple_table("system_wait", "t"))),
            )];
            if batched {
                entries.push(make_entry(
                    1,
                    3,
                    RaftOp::CreateTable(Box::new(simple_table("system_wait", "t2"))),
                ));
            }
            let apply = sm.apply(entries);
            tokio::pin!(apply);
            std::future::poll_fn(|cx| match std::future::Future::poll(apply.as_mut(), cx) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => {
                    panic!("held flush must leave apply pending: {result:?}")
                }
            })
            .await;
            if batched {
                // Both physical projection writes completed and the owned job
                // reached its one real explicit sync, blocked by the held file.
                while syncs.load(std::sync::atomic::Ordering::SeqCst) == before {
                    tokio::task::yield_now().await;
                }
            } else {
                while recorder.lock().unwrap().is_none() {
                    tokio::task::yield_now().await;
                }
            }
        }
        assert_eq!(
            sm.last_applied.unwrap().index,
            1,
            "system writes must finish before publication"
        );
        let applied = sm.applied_state();
        tokio::pin!(applied);
        let waited = tokio::select! {
            biased;
            result = &mut applied => { result.unwrap(); false }
            () = async { let _after_cleanup = release_tx.send(()); } => { applied.await.unwrap(); true }
        };
        assert!(
            holder.join().unwrap(),
            "fixture must release on observer progress"
        );
        assert!(
            waited,
            "applied_state must await the retained system-table write"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_raft_system_write_is_reaped_before_applied_state() {
        cancelled_projection_waiter_retains_fence(
            ferrosa_storage::SyncStrategyConfig::Batch,
            false,
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_periodic_projection_batch_retains_durability_fence() {
        cancelled_projection_waiter_retains_fence(
            ferrosa_storage::SyncStrategyConfig::default(),
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn panicked_raft_apply_poison_refuses_every_trait_barrier() {
        let mut sm = FerrosStateMachine {
            pending_apply: Some(PendingApply {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                job: ferrosa_net::task_pool::TaskPool::current("raft-poison-test").spawn_blocking(
                    || -> ferrosa_common::Result<CompletedApply> {
                        panic!("intentional apply panic");
                    },
                ),
                pauses: Arc::new(Vec::new()),
            }),
            ..Default::default()
        };
        let error = sm.applied_state().await.unwrap_err().to_string();
        assert!(error.contains("Raft physical apply job failed"));
        assert!(sm.last_applied.is_none());
        assert!(sm.state.tables.is_empty());
        assert!(sm
            .apply(vec![make_entry(
                1,
                2,
                RaftOp::CreateKeyspace(simple_keyspace("not_applied"))
            )])
            .await
            .unwrap_err()
            .to_string()
            .contains("Raft physical apply job failed"));
        assert!(sm
            .begin_receiving_snapshot()
            .await
            .unwrap_err()
            .to_string()
            .contains("Raft physical apply job failed"));
        assert!(sm
            .get_current_snapshot()
            .await
            .unwrap_err()
            .to_string()
            .contains("Raft physical apply job failed"));
        assert!(sm
            .build_snapshot()
            .await
            .unwrap_err()
            .to_string()
            .contains("Raft physical apply job failed"));
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "poison".into(),
        };
        assert!(sm
            .install_snapshot(&meta, Box::new(Cursor::new(Vec::new())))
            .await
            .unwrap_err()
            .to_string()
            .contains("Raft physical apply job failed"));
        let mut builder = sm.get_snapshot_builder().await;
        assert!(builder
            .build_snapshot()
            .await
            .unwrap_err()
            .to_string()
            .contains("Raft physical apply job failed"));
        assert!(sm.last_applied.is_none());
    }

    /// Companion to the keyspace reconciliation repro: a `CreateTable` apply
    /// must reconcile the shared `Schema` even when `self.state.tables` already
    /// holds the table, so fresh CQL connections can resolve `ks.t`. Same
    /// `inserted`-guard divergence as CreateKeyspace (forge t_86f9259d).
    #[tokio::test]
    async fn create_table_reconciles_shared_schema_even_when_state_already_has_it() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let shared_schema = Arc::new(test_schema_instance());
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&shared_schema), Arc::clone(&engine));

        // Keyspace is present everywhere; the table exists in self.state but not
        // yet in the shared Schema (drift).
        let ks = simple_keyspace("vis_test");
        sm.apply(vec![make_entry(1, 1, RaftOp::CreateKeyspace(ks))])
            .await
            .unwrap();
        let table = simple_table("vis_test", "t");
        sm.state
            .tables
            .insert(("vis_test".into(), "t".into()), table.clone());
        assert!(
            !shared_schema
                .snapshot()
                .tables
                .contains_key(&("vis_test".to_string(), "t".to_string())),
            "precondition: shared Schema must start without vis_test.t"
        );

        sm.apply(vec![make_entry(1, 2, RaftOp::CreateTable(Box::new(table)))])
            .await
            .unwrap();

        assert!(
            shared_schema
                .snapshot()
                .tables
                .contains_key(&("vis_test".to_string(), "t".to_string())),
            "CreateTable apply must reconcile the shared Schema so fresh \
             connections can resolve the table, even when self.state already had it"
        );
    }

    #[tokio::test]
    async fn create_keyspace_emits_system_table_write() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let schema = test_schema_instance();
        let mut sm = FerrosStateMachine::with_side_effects(Arc::new(schema), Arc::clone(&engine));

        let ks = simple_keyspace("wire_ks");
        let entry = make_entry(1, 1, RaftOp::CreateKeyspace(ks));
        sm.apply(vec![entry]).await.unwrap();

        // Verify system_schema.keyspaces has a row for "wire_ks".
        let tid = TableId::new("system_schema", "keyspaces");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"wire_ks".to_vec(),
        ));
        let partition = engine.read(&tid, &key).unwrap();
        assert!(
            partition.is_some(),
            "CreateKeyspace should write to system_schema.keyspaces"
        );
    }

    #[tokio::test]
    async fn create_table_emits_system_table_write() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let schema = test_schema_instance();
        let mut sm = FerrosStateMachine::with_side_effects(Arc::new(schema), Arc::clone(&engine));

        let ks = simple_keyspace("tbl_ks");
        let table = simple_table("tbl_ks", "users");
        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(ks)),
            make_entry(1, 2, RaftOp::CreateTable(Box::new(table))),
        ];
        sm.apply(entries).await.unwrap();

        let tid = TableId::new("system_schema", "tables");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"tbl_ks".to_vec(),
        ));
        let partition = engine.read(&tid, &key).unwrap();
        assert!(
            partition.is_some(),
            "CreateTable should write to system_schema.tables"
        );
    }

    /// RED for the raft-apply offload: the synchronous `engine.write` in
    /// `SystemTableWriter::apply` must run on a tokio blocking thread, not
    /// inline on the raft apply worker (where it parks raft core and delays
    /// heartbeat responses). We drive `apply()` on a single-worker multi-thread
    /// runtime, capture that worker's thread id, and assert the system-table
    /// write recorded a *different* thread.
    #[test]
    fn system_table_apply_runs_off_the_raft_worker_thread() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = test_schema_instance();
        let mut sm = FerrosStateMachine::with_side_effects(Arc::new(schema), Arc::clone(&engine));

        let apply_thread_handle = sm
            .system_writer
            .as_ref()
            .expect("with_side_effects installs a system writer")
            .last_apply_thread_handle();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let worker_thread = rt.block_on(async move {
            let worker = std::thread::current().id();
            let ks = simple_keyspace("offload_ks");
            let entry = make_entry(1, 1, RaftOp::CreateKeyspace(ks));
            sm.apply(vec![entry]).await.unwrap();
            worker
        });

        // The CreateKeyspace must have written to system_schema.keyspaces.
        let tid = TableId::new("system_schema", "keyspaces");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"offload_ks".to_vec(),
        ));
        assert!(
            engine.read(&tid, &key).unwrap().is_some(),
            "CreateKeyspace should write to system_schema.keyspaces"
        );

        let apply_thread = apply_thread_handle
            .lock()
            .unwrap()
            .expect("SystemTableWriter::apply must have run");
        assert_ne!(
            apply_thread, worker_thread,
            "system-table apply ran inline on the raft worker thread instead of \
             being offloaded to a blocking thread"
        );
    }

    #[tokio::test]
    async fn duplicate_schema_replay_does_not_churn_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let schema = test_schema_instance();
        let mut sm = FerrosStateMachine::with_side_effects(Arc::new(schema), Arc::clone(&engine));

        let initial_version = Uuid::new_v4();
        sm.apply(vec![Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(RaftCommand {
                op: RaftOp::CreateKeyspace(simple_keyspace("dup_ks")),
                schema_version: initial_version,
            }),
        }])
        .await
        .unwrap();
        assert_eq!(sm.state().schema_version, initial_version);

        sm.apply(vec![Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
            payload: EntryPayload::Normal(RaftCommand {
                op: RaftOp::CreateKeyspace(simple_keyspace("dup_ks")),
                schema_version: Uuid::new_v4(),
            }),
        }])
        .await
        .unwrap();
        assert_eq!(
            sm.state().schema_version,
            initial_version,
            "duplicate CreateKeyspace must not advance schema_version"
        );

        let table_version = Uuid::new_v4();
        sm.apply(vec![Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 3),
            payload: EntryPayload::Normal(RaftCommand {
                op: RaftOp::CreateTable(Box::new(simple_table("dup_ks", "users"))),
                schema_version: table_version,
            }),
        }])
        .await
        .unwrap();
        assert_eq!(sm.state().schema_version, table_version);

        sm.apply(vec![Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
            payload: EntryPayload::Normal(RaftCommand {
                op: RaftOp::CreateTable(Box::new(simple_table("dup_ks", "users"))),
                schema_version: Uuid::new_v4(),
            }),
        }])
        .await
        .unwrap();
        assert_eq!(
            sm.state().schema_version,
            table_version,
            "duplicate CreateTable must not advance schema_version"
        );
    }

    #[tokio::test]
    async fn role_lifecycle_writes_to_system_auth() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let schema = test_schema_instance();
        let mut sm = FerrosStateMachine::with_side_effects(Arc::new(schema), Arc::clone(&engine));

        let role = RoleMetadata {
            name: "tester".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
            scram: None,
        };

        let entries = vec![make_entry(1, 1, RaftOp::CreateRole(role))];
        sm.apply(entries).await.unwrap();

        let tid = TableId::new("system_auth", "roles");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            b"tester".to_vec(),
        ));
        let partition = engine.read(&tid, &key).unwrap();
        assert!(
            partition.is_some(),
            "CreateRole should persist to system_auth.roles"
        );

        // Now drop the role.
        let entries = vec![make_entry(1, 2, RaftOp::DropRole("tester".to_string()))];
        sm.apply(entries).await.unwrap();

        // After drop, the write should not error (tombstone was written).
    }

    // -- t_d0e5e6e5: CREATE ROLE for an already-seeded role ------------------

    const MANAGED_ADMIN_PASSWORD: &str = "managed-admin-secret-d0e5"; // pragma: allowlist secret

    /// A schema with cheap bcrypt so the role tests stay fast.
    fn fast_role_schema() -> Arc<ferrosa_schema::Schema> {
        use ferrosa_schema::{
            AuthMethod, LogAuditSink, PasswordHasher, PasswordPolicy, RateLimitConfig, SchemaConfig,
        };
        Arc::new(
            ferrosa_schema::Schema::new(SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(LogAuditSink),
                secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
                mode: ferrosa_schema::startup::DeploymentMode::Development,
            })
            .unwrap(),
        )
    }

    /// The production seed a managed deployment runs at every start
    /// (`FERROSA_MODE=production` + `FERROSA_SEED_ADMIN_PASSWORD`).
    fn managed_seed_config() -> ferrosa_schema::auth::bootstrap::SeedConfig {
        ferrosa_schema::auth::bootstrap::SeedConfig::from_parts(
            ferrosa_schema::startup::DeploymentMode::Production,
            Some(MANAGED_ADMIN_PASSWORD.to_string()),
        )
    }

    /// The role a coordinator replicates for
    /// `CREATE ROLE IF NOT EXISTS <name> WITH PASSWORD = '<password>'`.
    fn replicated_role(name: &str, password: &str, is_superuser: bool) -> RoleMetadata {
        RoleMetadata {
            name: name.to_string(),
            is_superuser,
            can_login: true,
            salted_hash: Some(
                ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 }
                    .hash_password(password)
                    .unwrap(),
            ),
            member_of: HashSet::new(),
            scram: None,
        }
    }

    fn persisted_role(engine: &Arc<StorageEngine>, name: &str) -> Option<RoleMetadata> {
        crate::system_table_loader::SystemTableLoader::new(Arc::clone(engine))
            .load_roles()
            .unwrap()
            .into_iter()
            .find(|r| r.name == name)
    }

    fn verifies(role: &RoleMetadata, password: &str) -> bool {
        role.salted_hash.as_deref().is_some_and(|hash| {
            ferrosa_schema::PasswordHasher::verify_password_any(password, hash).unwrap()
        })
    }

    /// Finding 6 (t_d0e5e6e5). A seeded role lives only in memory. A client
    /// `CREATE ROLE IF NOT EXISTS ferrosa_admin WITH PASSWORD = '<default>'`
    /// that reaches Raft apply must not write the INCOMING default hash to
    /// system_auth.roles while memory keeps the managed one: on restart the
    /// replayed row wins and production startup exits FATAL on the default
    /// superuser password. ferrosa_user must not come back with its
    /// well-known password either.
    #[tokio::test]
    async fn finding6_if_not_exists_create_role_overwrites_persisted_seed_hash() {
        use ferrosa_schema::auth::bootstrap::{
            admin_password_is_default, seed_default_roles_with_config, SEED_ADMIN_PASSWORD,
            SEED_ADMIN_USER, SEED_APP_PASSWORD, SEED_APP_READER_USER,
        };
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let schema = fast_role_schema();
        seed_default_roles_with_config(&schema, &managed_seed_config()).unwrap();
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&schema), Arc::clone(&engine));

        sm.apply(vec![
            make_entry(
                1,
                1,
                RaftOp::CreateRole(replicated_role(SEED_ADMIN_USER, SEED_ADMIN_PASSWORD, true)),
            ),
            make_entry(
                1,
                2,
                RaftOp::CreateRole(replicated_role(
                    SEED_APP_READER_USER,
                    SEED_APP_PASSWORD,
                    false,
                )),
            ),
        ])
        .await
        .unwrap();

        assert!(
            schema
                .authenticate(SEED_ADMIN_USER, MANAGED_ADMIN_PASSWORD)
                .is_ok(),
            "memory keeps the seeded admin credential"
        );
        for (name, default) in [
            (SEED_ADMIN_USER, SEED_ADMIN_PASSWORD),
            (SEED_APP_READER_USER, SEED_APP_PASSWORD),
        ] {
            if let Some(row) = persisted_role(&engine, name) {
                assert!(
                    !verifies(&row, default),
                    "{name}: system_auth.roles must not hold the incoming default hash"
                );
            }
        }

        // Restart: a fresh registry replays system_auth.roles, then seeds.
        let restarted = fast_role_schema();
        crate::system_table_loader::SystemTableLoader::new(Arc::clone(&engine))
            .replay_roles_into_schema(&restarted)
            .unwrap();
        seed_default_roles_with_config(&restarted, &managed_seed_config()).unwrap();
        assert!(
            !admin_password_is_default(&restarted),
            "replayed admin must not verify against the default password"
        );
        assert!(restarted
            .authenticate(SEED_ADMIN_USER, MANAGED_ADMIN_PASSWORD)
            .is_ok());
        assert!(
            restarted
                .authenticate(SEED_APP_READER_USER, SEED_APP_PASSWORD)
                .is_err(),
            "a restarted node must not accept ferrosa_user's well-known password"
        );
    }

    /// The production gate exactly as `ferrosa/src/main.rs`
    /// `enforce_production_requirements` feeds it: does it refuse a default
    /// superuser password on this schema?
    fn gate_refuses_default_superuser(schema: &ferrosa_schema::Schema) -> bool {
        use ferrosa_schema::startup::{
            validate_production_requirements, DeploymentMode, ProductionCheckConfig,
            ProductionViolation,
        };
        validate_production_requirements(&ProductionCheckConfig {
            mode: DeploymentMode::Production,
            password_policy: ferrosa_schema::PasswordPolicy::permissive(),
            has_superuser_password: !ferrosa_schema::auth::bootstrap::admin_password_is_default(
                schema,
            ),
            secrets_provider_type: "env".to_string(),
            s3_allow_http: false,
            auth_enabled: true,
            listeners: Vec::new(),
            internode_require_tls: true,
        })
        .iter()
        .any(|v| matches!(v, ProductionViolation::DefaultSuperuserPassword))
    }

    /// A node's startup role sequence on the fixed binary (main.rs): replay
    /// system_auth.roles, seed, repair. Returns the node's registry.
    fn start_fixed_node(
        engine: &Arc<StorageEngine>,
        config: &ferrosa_schema::auth::bootstrap::SeedConfig,
    ) -> Arc<ferrosa_schema::Schema> {
        let schema = fast_role_schema();
        let loader = crate::system_table_loader::SystemTableLoader::new(Arc::clone(engine));
        loader.replay_roles_into_schema(&schema).unwrap();
        ferrosa_schema::auth::bootstrap::seed_default_roles_with_config(&schema, config).unwrap();
        loader
            .repair_default_credential_rows(&schema, config)
            .unwrap();
        schema
    }

    /// Rolling upgrade of an existing v2026.10.28.1513 node (t_d0e5e6e5).
    ///
    /// Its system_auth.roles holds the default-hash rows the pre-fix apply
    /// wrote for ferrosa-memory's CREATE ROLE IF NOT EXISTS. Restarted on the
    /// fixed binary with FERROSA_SEED_ADMIN_PASSWORD set, it must come up on
    /// the seeded hash and pass the production gate. While the rest of the
    /// cluster still runs the old binary, an old coordinator keeps
    /// replicating that CreateRole: the upgraded node's apply must persist
    /// nothing, so the next restart passes too. Without a configured seed the
    /// gate must still refuse.
    #[tokio::test]
    async fn rolling_upgrade_repairs_default_rows_and_gate_passes_in_mixed_cluster() {
        use ferrosa_schema::auth::bootstrap::{
            SeedConfig, SEED_ADMIN_PASSWORD, SEED_ADMIN_USER, SEED_APP_PASSWORD,
            SEED_APP_READER_USER,
        };
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let old_create_ops = || {
            vec![
                RaftOp::CreateRole(replicated_role(SEED_ADMIN_USER, SEED_ADMIN_PASSWORD, true)),
                RaftOp::CreateRole(replicated_role(
                    SEED_APP_READER_USER,
                    SEED_APP_PASSWORD,
                    false,
                )),
            ]
        };
        // Disk state left by the old binary: its apply persisted the incoming roles.
        let writer = crate::system_table_writer::SystemTableWriter::new(Arc::clone(&engine));
        for op in old_create_ops() {
            let RaftOp::CreateRole(role) = op else {
                unreachable!()
            };
            writer
                .apply(SystemTableMutation::RoleCreated(role))
                .unwrap();
        }

        // Without a configured seed there is nothing to repair to: the gate refuses.
        let unseeded =
            SeedConfig::from_parts(ferrosa_schema::startup::DeploymentMode::Production, None);
        assert!(
            gate_refuses_default_superuser(&start_fixed_node(&engine, &unseeded)),
            "the gate must still fire when no seed is configured and the row is default"
        );

        // Upgraded node start with the seed configured: repaired, gate passes.
        let schema = start_fixed_node(&engine, &managed_seed_config());
        assert!(!gate_refuses_default_superuser(&schema));
        assert!(schema
            .authenticate(SEED_ADMIN_USER, MANAGED_ADMIN_PASSWORD)
            .is_ok());
        assert!(schema
            .authenticate(SEED_APP_READER_USER, SEED_APP_PASSWORD)
            .is_err());

        // Mixed-version window: old coordinators still replicate the CreateRole.
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&schema), Arc::clone(&engine));
        let entries: Vec<_> = old_create_ops()
            .into_iter()
            .enumerate()
            .map(|(i, op)| make_entry(1, i as u64 + 1, op))
            .collect();
        sm.apply(entries).await.unwrap();
        let admin_row = persisted_role(&engine, SEED_ADMIN_USER).unwrap();
        assert!(verifies(&admin_row, MANAGED_ADMIN_PASSWORD));
        assert!(!verifies(&admin_row, SEED_ADMIN_PASSWORD));

        // Next restart of the upgraded node: still passes, nothing re-broken.
        let schema = start_fixed_node(&engine, &managed_seed_config());
        assert!(!gate_refuses_default_superuser(&schema));
    }

    /// A genuinely new role is still persisted, and the row is the stored role.
    #[tokio::test]
    async fn create_role_new_role_persists_the_stored_row() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = fast_role_schema();
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&schema), Arc::clone(&engine));

        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::CreateRole(replicated_role("fresh_role", "fresh-pw", false)),
        )])
        .await
        .unwrap();

        let row = persisted_role(&engine, "fresh_role").expect("new role must be persisted");
        assert!(verifies(&row, "fresh-pw"));
        assert!(schema.authenticate("fresh_role", "fresh-pw").is_ok());
    }

    /// ALTER/GRANT on a role must persist the role the registry STORES. A
    /// seeded role is absent from the replicated `state.roles`, and a cluster
    /// that applied the pre-fix CreateRole holds a stale default-hash copy
    /// there: persisting that copy re-writes the default password to disk.
    #[tokio::test]
    async fn role_updates_persist_the_stored_role_not_replicated_copy() {
        use ferrosa_schema::auth::bootstrap::{
            seed_default_roles_with_config, SEED_ADMIN_PASSWORD, SEED_ADMIN_USER,
            SEED_APP_READER_USER,
        };
        use ferrosa_schema::RoleUpdates;
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = fast_role_schema();
        seed_default_roles_with_config(&schema, &managed_seed_config()).unwrap();
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&schema), Arc::clone(&engine));

        // Legacy replicated state: a default-hash copy of the admin role.
        sm.state.roles.insert(
            SEED_ADMIN_USER.to_string(),
            replicated_role(SEED_ADMIN_USER, SEED_ADMIN_PASSWORD, true),
        );

        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::AlterRole {
                name: SEED_ADMIN_USER.to_string(),
                updates: RoleUpdates {
                    can_login: Some(true),
                    ..RoleUpdates::default()
                },
            },
        )])
        .await
        .unwrap();
        let row = persisted_role(&engine, SEED_ADMIN_USER).expect("ALTER must persist the role");
        assert!(
            verifies(&row, MANAGED_ADMIN_PASSWORD) && !verifies(&row, SEED_ADMIN_PASSWORD),
            "ALTER must persist the stored (managed) hash, not the replicated default copy"
        );

        // A seeded role that is NOT in state.roles: ALTER ... PASSWORD must
        // still reach disk, or the rotation is lost on restart.
        let rotated = ferrosa_schema::PasswordHasher::Bcrypt { cost: 4 }
            .hash_password("rotated-user-pw")
            .unwrap();
        sm.apply(vec![make_entry(
            1,
            2,
            RaftOp::AlterRole {
                name: SEED_APP_READER_USER.to_string(),
                updates: RoleUpdates {
                    password: Some(rotated),
                    ..RoleUpdates::default()
                },
            },
        )])
        .await
        .unwrap();
        let row = persisted_role(&engine, SEED_APP_READER_USER)
            .expect("ALTER ROLE PASSWORD on a seeded role must persist");
        assert!(verifies(&row, "rotated-user-pw"));

        // GRANT ROLE re-persists the member row: it too must be the stored role.
        sm.apply(vec![make_entry(
            1,
            3,
            RaftOp::GrantRole {
                member: SEED_ADMIN_USER.to_string(),
                granted_role: SEED_APP_READER_USER.to_string(),
            },
        )])
        .await
        .unwrap();
        let row = persisted_role(&engine, SEED_ADMIN_USER).unwrap();
        assert!(
            !verifies(&row, SEED_ADMIN_PASSWORD),
            "GRANT ROLE must not re-persist the replicated default hash"
        );
    }

    // -- IndexNodeStatus / index_state_map tests ----------------------------

    #[test]
    fn raft_state_has_index_state_map() {
        let state = RaftState::default();
        assert!(
            state.index_state_map.is_empty(),
            "index_state_map should start empty"
        );
    }

    #[test]
    fn snapshot_roundtrip_preserves_index_state_map() {
        let mut state = RaftState::default();
        let mut node_statuses = BTreeMap::new();
        node_statuses.insert(1u64, IndexNodeStatus::Ready);
        node_statuses.insert(2u64, IndexNodeStatus::Building);
        state
            .index_state_map
            .insert(("ks".into(), "tbl".into(), "idx".into()), node_statuses);

        let bytes = bincode::serialize(&state).expect("serialize");
        let decoded: RaftState = bincode::deserialize(&bytes).expect("deserialize");

        let entry = decoded
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .expect("index entry should exist");
        assert_eq!(entry.get(&1u64), Some(&IndexNodeStatus::Ready));
        assert_eq!(entry.get(&2u64), Some(&IndexNodeStatus::Building));
    }

    /// A `CREATE INDEX` replicated through Raft must BUILD the index in the
    /// receiving node's storage engine, not only record it in schema.
    ///
    /// Only the CQL router wires an index into storage, and it runs on the one
    /// node whose session received the DDL. Every other replica applies
    /// `RaftOp::CreateIndex`, which registers the index in `Schema` and in
    /// `system_schema.indexes` and marks every member `Building` — and never
    /// touches the engine. Nothing later builds it either: no node reports
    /// `Ready` because no build was ever started.
    ///
    /// The asymmetry is visible three lines down in the same `match`:
    /// `RaftOp::DropIndex` calls `engine.drop_index`, and pushes an
    /// `ApplyError` when it fails. Create unwires nothing and wires nothing.
    ///
    /// What it costs: the planner picks the index on every node, because the
    /// CQL schema lists it everywhere. A global index read fans out to all
    /// replicas (`coordinate_index_read_stream`), so the replicas that never
    /// built it answer for their whole slice of the table. Before
    /// d6645fc7 they answered it with zero rows and the coordinator unioned
    /// that into a short result reported as the complete one; since d6645fc7
    /// they refuse, naming the index, and the refusal fails the whole read.
    /// Both are wrong, and the second is what `agent_memory`'s tenant-wide
    /// entity reads have been failing on.
    #[tokio::test]
    async fn create_index_via_raft_apply_is_built_in_the_local_engine() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let shared_schema = Arc::new(test_schema_instance());
        let mut sm =
            FerrosStateMachine::with_side_effects(Arc::clone(&shared_schema), Arc::clone(&engine));

        // This node never sees the CQL statement; it only replays the log.
        let ks = simple_keyspace("idx_ks");
        let table = simple_table("idx_ks", "idx_tbl");
        let index = IndexMetadata {
            keyspace: "idx_ks".into(),
            table: "idx_tbl".into(),
            name: "idx_by_value".into(),
            index_type: ferrosa_index::IndexType::BTree,
            target_columns: vec!["value".into()],
            filter_predicate: None,
            options: std::collections::HashMap::new(),
        };
        let responses = sm
            .apply(vec![
                make_entry(1, 1, RaftOp::CreateKeyspace(ks)),
                make_entry(1, 2, RaftOp::CreateTable(Box::new(table))),
                make_entry(1, 3, RaftOp::CreateIndex(index)),
            ])
            .await
            .unwrap();
        for (i, response) in responses.iter().enumerate() {
            assert!(
                matches!(response, RaftResponse::Ok),
                "entry {i} should apply cleanly: {response:?}"
            );
        }

        // The schema half already works — that is what makes the missing half
        // invisible, and what makes the planner keep choosing the index.
        assert!(
            shared_schema.snapshot().indexes.contains_key(&(
                "idx_ks".to_string(),
                "idx_tbl".to_string(),
                "idx_by_value".to_string(),
            )),
            "the replicated index must be in the shared schema"
        );

        let table_id = TableId::new("idx_ks", "idx_tbl");
        assert!(
            engine.declares_index(&table_id, "idx_by_value"),
            "a CREATE INDEX replicated through Raft must be built in this \
             node's storage engine. The schema lists it, so the planner will \
             select it on this node; an index the engine does not have either \
             answers for its whole slice with zero rows or refuses the read \
             outright. Neither is an answer."
        );
    }

    #[test]
    fn apply_index_status_updates_state_map() {
        let mut sm = FerrosStateMachine::new();

        // First create the index so the schema entry exists.
        let create_cmd = RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        };
        sm.apply_command(create_cmd).unwrap();

        // Now report node 1 as Building.
        let status_cmd = RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Building,
            },
            schema_version: Uuid::new_v4(),
        };
        sm.apply_command(status_cmd).unwrap();

        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .expect("state map entry should exist");
        assert_eq!(entry.get(&1), Some(&IndexNodeStatus::Building));

        // Now report node 1 as Ready.
        let ready_cmd = RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        };
        sm.apply_command(ready_cmd).unwrap();

        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .expect("state map entry should exist");
        assert_eq!(entry.get(&1), Some(&IndexNodeStatus::Ready));
    }

    #[test]
    fn apply_index_status_tracks_multiple_nodes() {
        let mut sm = FerrosStateMachine::new();

        let create_cmd = RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        };
        sm.apply_command(create_cmd).unwrap();

        // Node 1: Ready, Node 2: Building, Node 3: Failed
        for (node_id, status) in [
            (1u64, IndexNodeStatus::Ready),
            (2, IndexNodeStatus::Building),
            (3, IndexNodeStatus::Failed("timeout".into())),
        ] {
            let cmd = RaftCommand {
                op: RaftOp::IndexStatus {
                    node_id,
                    keyspace: "ks".into(),
                    table: "tbl".into(),
                    index_name: "idx".into(),
                    status,
                },
                schema_version: Uuid::new_v4(),
            };
            sm.apply_command(cmd).unwrap();
        }

        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .unwrap();
        assert_eq!(entry.len(), 3);
        assert_eq!(entry.get(&1), Some(&IndexNodeStatus::Ready));
        assert_eq!(entry.get(&2), Some(&IndexNodeStatus::Building));
        assert_eq!(
            entry.get(&3),
            Some(&IndexNodeStatus::Failed("timeout".into()))
        );
    }

    #[test]
    fn drop_index_cleans_index_state_map() {
        let mut sm = FerrosStateMachine::new();

        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        assert!(!sm.state().index_state_map.is_empty());

        sm.apply_command(RaftCommand {
            op: RaftOp::DropIndex {
                keyspace: "ks".into(),
                table: "tbl".into(),
                index: "idx".into(),
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        assert!(
            !sm.state()
                .index_state_map
                .contains_key(&("ks".into(), "tbl".into(), "idx".into())),
            "index_state_map should be cleaned up after DropIndex"
        );
    }

    #[test]
    fn drop_keyspace_cleans_index_state_map() {
        let mut sm = FerrosStateMachine::new();

        sm.apply_command(RaftCommand {
            op: RaftOp::CreateKeyspace(KeyspaceMetadata {
                name: "ks".into(),
                durable_writes: true,
                replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: [("replication_factor".into(), "1".into())].into(),
                },
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();
        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        assert!(!sm.state().index_state_map.is_empty());

        sm.apply_command(RaftCommand {
            op: RaftOp::DropKeyspace("ks".into()),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        assert!(
            sm.state().index_state_map.is_empty(),
            "index_state_map should be empty after DropKeyspace"
        );
    }

    #[test]
    fn drop_table_cleans_index_state_map() {
        let mut sm = FerrosStateMachine::new();

        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        // Also create an index on a different table to ensure it survives.
        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "other_tbl".into(),
                name: "idx2".into(),
                index_type: ferrosa_index::IndexType::Hash,
                target_columns: vec!["x".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "other_tbl".into(),
                index_name: "idx2".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        assert_eq!(sm.state().index_state_map.len(), 2);

        sm.apply_command(RaftCommand {
            op: RaftOp::DropTable {
                keyspace: "ks".into(),
                table: "tbl".into(),
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        assert_eq!(sm.state().index_state_map.len(), 1);
        assert!(sm.state().index_state_map.contains_key(&(
            "ks".into(),
            "other_tbl".into(),
            "idx2".into()
        )));
    }

    /// DROP TABLE through the Raft apply route must cascade tombstones over
    /// the dropped table's dogfooded `system_schema.indexes` registrations
    /// (forge t_ae06e925). Before the fix, only DROP INDEX tombstoned its row;
    /// DROP TABLE left every registration dangling, so each node boot's
    /// `reload_indexes_from_system_schema` warned per orphan forever and
    /// `system_schema.indexes` grew unboundedly (observed live: dozens of
    /// orphaned `fts_probe_*` FullText registrations on the fmem-dev cluster).
    /// An index on a *different* table must survive the cascade.
    #[tokio::test]
    async fn drop_table_tombstones_persisted_index_rows() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        let schema = test_schema_instance();
        let mut sm = FerrosStateMachine::with_side_effects(Arc::new(schema), Arc::clone(&engine));

        let mk_index = |table: &str, name: &str, ty: ferrosa_index::IndexType| IndexMetadata {
            keyspace: "idxks".into(),
            table: table.into(),
            name: name.into(),
            index_type: ty,
            target_columns: vec!["value".into()],
            filter_predicate: None,
            options: std::collections::HashMap::new(),
        };

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("idxks"))),
            make_entry(
                1,
                2,
                RaftOp::CreateTable(Box::new(simple_table("idxks", "tbl"))),
            ),
            make_entry(
                1,
                3,
                RaftOp::CreateTable(Box::new(simple_table("idxks", "other_tbl"))),
            ),
            make_entry(
                1,
                4,
                RaftOp::CreateIndex(mk_index(
                    "tbl",
                    "fts_probe_idx",
                    ferrosa_index::IndexType::FullText,
                )),
            ),
            make_entry(
                1,
                5,
                RaftOp::CreateIndex(mk_index(
                    "tbl",
                    "value_btree_idx",
                    ferrosa_index::IndexType::BTree,
                )),
            ),
            make_entry(
                1,
                6,
                RaftOp::CreateIndex(mk_index(
                    "other_tbl",
                    "survivor_idx",
                    ferrosa_index::IndexType::BTree,
                )),
            ),
        ];
        sm.apply(entries).await.unwrap();

        let live = engine.read_persisted_indexes().unwrap();
        assert_eq!(
            live.len(),
            3,
            "all three index registrations must be dogfooded before the drop: {live:?}"
        );

        sm.apply(vec![make_entry(
            1,
            7,
            RaftOp::DropTable {
                keyspace: "idxks".into(),
                table: "tbl".into(),
            },
        )])
        .await
        .unwrap();

        let remaining = engine.read_persisted_indexes().unwrap();
        let dangling: Vec<_> = remaining
            .iter()
            .filter(|r| r.keyspace_name == "idxks" && r.table_name == "tbl")
            .collect();
        assert!(
            dangling.is_empty(),
            "DROP TABLE must tombstone every system_schema.indexes registration \
             for the dropped table; dangling rows: {dangling:?}"
        );
        assert_eq!(
            remaining.len(),
            1,
            "the index on the surviving table must not be tombstoned: {remaining:?}"
        );
        assert_eq!(remaining[0].index_name, "survivor_idx");
    }

    #[test]
    fn leave_node_cleans_node_from_index_state_map() {
        let mut sm = FerrosStateMachine::new();

        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        // Two nodes report Ready.
        for node_id in [1u64, 2] {
            sm.apply_command(RaftCommand {
                op: RaftOp::IndexStatus {
                    node_id,
                    keyspace: "ks".into(),
                    table: "tbl".into(),
                    index_name: "idx".into(),
                    status: IndexNodeStatus::Ready,
                },
                schema_version: Uuid::new_v4(),
            })
            .unwrap();
        }

        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .unwrap();
        assert_eq!(entry.len(), 2);

        // Node 1 leaves.
        sm.apply_command(RaftCommand {
            op: RaftOp::LeaveNode { node_id: 1 },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .unwrap();
        assert_eq!(entry.len(), 1);
        assert!(entry.get(&1).is_none());
        assert_eq!(entry.get(&2), Some(&IndexNodeStatus::Ready));
    }

    #[test]
    fn create_index_initializes_index_state_map_entry() {
        let mut sm = FerrosStateMachine::new();

        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        // index_state_map should have an entry (empty since no members).
        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()));
        assert!(
            entry.is_some(),
            "CreateIndex should initialize index_state_map entry"
        );
        assert!(entry.unwrap().is_empty());
    }

    #[test]
    fn join_node_marks_building_for_existing_indexes() {
        let mut sm = FerrosStateMachine::new();

        // Create an index first.
        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        // Node 1 is already Ready.
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        // New node joins.
        let host_id = Uuid::new_v4();
        let node_id = crate::raft::uuid_to_node_id(host_id);
        sm.apply_command(RaftCommand {
            op: RaftOp::JoinNode(NodeInfo {
                host_id,
                addr: "10.0.0.2:7000".into(),
                data_center: "dc1".into(),
                rack: "rack1".into(),
                state: NodeState::Joining,
                cql_broadcast: None,
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        // New node should be marked Building for the existing index.
        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .unwrap();
        assert_eq!(entry.get(&node_id), Some(&IndexNodeStatus::Building));
        // Existing node 1 should still be Ready.
        assert_eq!(entry.get(&1), Some(&IndexNodeStatus::Ready));
    }

    #[test]
    fn update_node_info_refreshes_existing_member_metadata_without_touching_tokens() {
        let mut sm = FerrosStateMachine::new();
        let host_id = Uuid::new_v4();
        let node_id = crate::raft::uuid_to_node_id(host_id);

        // Disable auto-token-assignment so the explicit `AssignTokens`
        // entry is the sole source of tokens — this test exercises that
        // UpdateNodeInfo refreshes metadata without touching tokens,
        // orthogonal to the auto-assignment policy.
        let cfg = ClusterConfig {
            num_tokens: 0,
            ..ClusterConfig::default()
        };
        sm.apply_command(RaftCommand {
            op: RaftOp::UpdateConfig(cfg),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        sm.apply_command(RaftCommand {
            op: RaftOp::JoinNode(NodeInfo {
                host_id,
                addr: "10.89.1.61:7000".into(),
                data_center: "dc1".into(),
                rack: "rack1".into(),
                state: NodeState::Normal,
                cql_broadcast: None,
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();
        sm.apply_command(RaftCommand {
            op: RaftOp::AssignTokens {
                node_id,
                tokens: vec![-10, 0, 10],
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        sm.apply_command(RaftCommand {
            op: RaftOp::UpdateNodeInfo(NodeInfo {
                host_id,
                addr: "10.89.1.7:7000".into(),
                data_center: "dc1".into(),
                rack: "rack1".into(),
                state: NodeState::Normal,
                cql_broadcast: Some("127.0.0.1:19043".into()),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        let member = sm
            .state()
            .members
            .get(&node_id)
            .expect("member must remain present after metadata refresh");
        assert_eq!(member.addr, "10.89.1.7:7000");
        assert_eq!(member.cql_broadcast.as_deref(), Some("127.0.0.1:19043"));

        let tokens: Vec<_> = sm
            .state()
            .token_map
            .iter()
            .filter_map(|(token, owner)| (*owner == node_id).then_some(*token))
            .collect();
        assert_eq!(
            tokens,
            vec![-10, 0, 10],
            "metadata refresh must not reassign tokens"
        );
    }

    #[test]
    fn create_index_marks_all_members_building() {
        let mut sm = FerrosStateMachine::new();

        // Add two nodes to the cluster.
        for (host_id_seed, addr) in [(100u64, "10.0.0.1:7000"), (200, "10.0.0.2:7000")] {
            let host_id = Uuid::from_u128(host_id_seed as u128);
            sm.apply_command(RaftCommand {
                op: RaftOp::JoinNode(NodeInfo {
                    host_id,
                    addr: addr.into(),
                    data_center: "dc1".into(),
                    rack: "rack1".into(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
                schema_version: Uuid::new_v4(),
            })
            .unwrap();
        }

        assert_eq!(sm.state().members.len(), 2);

        // Create an index.
        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .unwrap();
        // Both nodes should be marked Building.
        assert_eq!(entry.len(), 2);
        for status in entry.values() {
            assert_eq!(*status, IndexNodeStatus::Building);
        }
    }

    // -- Integration tests ------------------------------------------------

    #[test]
    fn three_node_index_lifecycle_convergence() {
        // Three state machines simulate three Raft replicas.
        let mut sm1 = FerrosStateMachine::new();
        let mut sm2 = FerrosStateMachine::new();
        let mut sm3 = FerrosStateMachine::new();

        // Helper: apply same command to all machines.
        fn apply_all(machines: &mut [&mut FerrosStateMachine], cmd: RaftCommand) {
            for sm in machines.iter_mut() {
                sm.apply_command(cmd.clone()).unwrap();
            }
        }

        // Step 1: Three nodes join.
        let mut node_ids = Vec::new();
        for i in 0..3u128 {
            let host_id = Uuid::from_u128(i + 1);
            let node_id = crate::raft::uuid_to_node_id(host_id);
            let cmd = RaftCommand {
                op: RaftOp::JoinNode(NodeInfo {
                    host_id,
                    addr: format!("10.0.0.{}:7000", i + 1),
                    data_center: "dc1".into(),
                    rack: "rack1".into(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
                schema_version: Uuid::new_v4(),
            };
            apply_all(&mut [&mut sm1, &mut sm2, &mut sm3], cmd);
            node_ids.push(node_id);
        }

        // Step 2: CREATE INDEX -- all nodes marked Building.
        let create_cmd = RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "users".into(),
                name: "idx_email".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["email".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        };
        apply_all(&mut [&mut sm1, &mut sm2, &mut sm3], create_cmd);

        // Verify all three machines agree: 3 nodes, all Building.
        for sm in [&sm1, &sm2, &sm3] {
            let entry = sm
                .state()
                .index_state_map
                .get(&("ks".into(), "users".into(), "idx_email".into()))
                .unwrap();
            assert_eq!(entry.len(), 3);
            for &nid in &node_ids {
                assert_eq!(entry.get(&nid), Some(&IndexNodeStatus::Building));
            }
        }

        // Step 3: Nodes report Ready one by one.
        for &nid in &node_ids {
            let cmd = RaftCommand {
                op: RaftOp::IndexStatus {
                    node_id: nid,
                    keyspace: "ks".into(),
                    table: "users".into(),
                    index_name: "idx_email".into(),
                    status: IndexNodeStatus::Ready,
                },
                schema_version: Uuid::new_v4(),
            };
            apply_all(&mut [&mut sm1, &mut sm2, &mut sm3], cmd);
        }

        // Verify all three machines agree: all nodes Ready.
        for sm in [&sm1, &sm2, &sm3] {
            let entry = sm
                .state()
                .index_state_map
                .get(&("ks".into(), "users".into(), "idx_email".into()))
                .unwrap();
            for &nid in &node_ids {
                assert_eq!(entry.get(&nid), Some(&IndexNodeStatus::Ready));
            }
        }

        // Step 4: DROP INDEX -- state map cleaned up.
        let drop_cmd = RaftCommand {
            op: RaftOp::DropIndex {
                keyspace: "ks".into(),
                table: "users".into(),
                index: "idx_email".into(),
            },
            schema_version: Uuid::new_v4(),
        };
        apply_all(&mut [&mut sm1, &mut sm2, &mut sm3], drop_cmd);

        for sm in [&sm1, &sm2, &sm3] {
            assert!(!sm.state().index_state_map.contains_key(&(
                "ks".into(),
                "users".into(),
                "idx_email".into()
            )));
            assert!(!sm.state().indexes.contains_key(&(
                "ks".into(),
                "users".into(),
                "idx_email".into()
            )));
        }
    }

    #[test]
    fn index_build_failure_isolated_to_one_node() {
        let mut sm = FerrosStateMachine::new();

        // Two nodes.
        for i in 1..=2u128 {
            let host_id = Uuid::from_u128(i);
            sm.apply_command(RaftCommand {
                op: RaftOp::JoinNode(NodeInfo {
                    host_id,
                    addr: format!("10.0.0.{i}:7000"),
                    data_center: "dc1".into(),
                    rack: "rack1".into(),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                }),
                schema_version: Uuid::new_v4(),
            })
            .unwrap();
        }

        // Create index.
        sm.apply_command(RaftCommand {
            op: RaftOp::CreateIndex(IndexMetadata {
                keyspace: "ks".into(),
                table: "tbl".into(),
                name: "idx".into(),
                index_type: ferrosa_index::IndexType::BTree,
                target_columns: vec!["col".into()],
                filter_predicate: None,
                options: std::collections::HashMap::new(),
            }),
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        let node1_id = crate::raft::uuid_to_node_id(Uuid::from_u128(1));
        let node2_id = crate::raft::uuid_to_node_id(Uuid::from_u128(2));

        // Node 1: Ready, Node 2: Failed.
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: node1_id,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: node2_id,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Failed("disk full".into()),
            },
            schema_version: Uuid::new_v4(),
        })
        .unwrap();

        let entry = sm
            .state()
            .index_state_map
            .get(&("ks".into(), "tbl".into(), "idx".into()))
            .unwrap();
        assert_eq!(entry.get(&node1_id), Some(&IndexNodeStatus::Ready));
        assert_eq!(
            entry.get(&node2_id),
            Some(&IndexNodeStatus::Failed("disk full".into()))
        );

        // Verify index-aware selection would prefer node 1.
        let replicas = vec![node1_id, node2_id];
        let selected = crate::coordinator::read::select_index_ready_replicas(
            &replicas,
            "ks",
            "tbl",
            "idx",
            &sm.state().index_state_map,
        );
        assert_eq!(selected[0], node1_id);
    }

    #[test]
    fn recover_from_purge_point_sets_last_applied_when_none() {
        use openraft::{CommittedLeaderId, LogId};

        let mut sm = FerrosStateMachine::new();
        assert!(sm.last_applied.is_none());

        let purge_point = LogId::new(CommittedLeaderId::new(1, 42), 6);
        sm.recover_from_purge_point(Some(purge_point));

        assert_eq!(
            sm.last_applied,
            Some(purge_point),
            "last_applied must be set to purge point when it was None"
        );
    }

    #[test]
    fn recover_from_purge_point_noop_when_already_applied() {
        use openraft::{CommittedLeaderId, LogId};

        let mut sm = FerrosStateMachine::new();
        let existing = LogId::new(CommittedLeaderId::new(5, 42), 100);
        sm.last_applied = Some(existing);

        let purge_point = LogId::new(CommittedLeaderId::new(1, 42), 6);
        sm.recover_from_purge_point(Some(purge_point));

        assert_eq!(
            sm.last_applied,
            Some(existing),
            "must not overwrite existing last_applied"
        );
    }

    #[test]
    fn recover_from_purge_point_noop_when_no_purge() {
        let mut sm = FerrosStateMachine::new();
        sm.recover_from_purge_point(None);
        assert!(
            sm.last_applied.is_none(),
            "no purge point means no recovery needed"
        );
    }

    #[test]
    fn recover_membership_restores_from_log() {
        use openraft::Membership;
        use std::collections::BTreeSet;

        let mut sm = FerrosStateMachine::new();

        // Membership is empty (the OOM-kill state).
        assert!(
            sm.last_membership
                .membership()
                .get_joint_config()
                .iter()
                .all(|c| c.is_empty()),
            "membership must start empty"
        );

        // Simulate finding a membership entry in the log.
        let voters: BTreeSet<u64> = [1, 2, 3].into_iter().collect();
        let membership = Membership::new(vec![voters], None);
        let stored = StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(1, 1), 5)),
            membership,
        );

        sm.recover_membership(Some(stored));

        let configs = sm.last_membership.membership().get_joint_config();
        assert_eq!(configs.len(), 1, "membership must be recovered from log");
        assert!(configs[0].contains(&1), "node 1 must be a voter");
        assert!(configs[0].contains(&3), "node 3 must be a voter");
    }

    #[test]
    fn recover_membership_noop_when_already_set() {
        use openraft::Membership;
        use std::collections::BTreeSet;

        let mut sm = FerrosStateMachine::new();

        // Set an existing membership.
        let voters: BTreeSet<u64> = [10, 20].into_iter().collect();
        let existing = Membership::new(vec![voters], None);
        sm.last_membership = StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(5, 10), 100)),
            existing,
        );

        // Try to recover with different membership — should be a noop.
        let new_voters: BTreeSet<u64> = [1, 2, 3].into_iter().collect();
        let new_membership = Membership::new(vec![new_voters], None);
        let stored = StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(1, 1), 5)),
            new_membership,
        );
        sm.recover_membership(Some(stored));

        assert!(
            sm.last_membership.membership().get_joint_config()[0].contains(&10),
            "existing membership must not be overwritten"
        );
    }

    mod snapshot_drop_properties {
        use super::*;

        use std::collections::BTreeSet;

        use ferrosa_common::test_generators::{
            arb_generated_drop_marker, arb_generated_index_declaration,
            arb_generated_table_identity, GeneratedDropMarker, GeneratedIndexDeclaration,
            GeneratedTableIdentity,
        };
        use proptest::prelude::*;

        fn table_key(identity: &GeneratedTableIdentity) -> (String, String) {
            (identity.name.keyspace.clone(), identity.name.table.clone())
        }

        fn table_map(
            identities: &[GeneratedTableIdentity],
        ) -> BTreeMap<(String, String), TableMetadata> {
            identities
                .iter()
                .map(|identity| {
                    let key = table_key(identity);
                    let table = simple_table_with_generation(
                        &identity.name.keyspace,
                        &identity.name.table,
                        identity.generation,
                    );
                    (key, table)
                })
                .collect()
        }

        fn has_matching_marker(
            markers: &[GeneratedDropMarker],
            identity: &GeneratedTableIdentity,
            snapshot_log_index: u64,
        ) -> bool {
            markers.iter().any(|marker| {
                marker.identity == *identity && marker.drop_log_index <= snapshot_log_index
            })
        }

        fn inferred_unregistered_identities(
            previous: &[GeneratedTableIdentity],
            incoming: &[GeneratedTableIdentity],
        ) -> Vec<GeneratedTableIdentity> {
            let dropped = tables_dropped_by_snapshot(&table_map(previous), &table_map(incoming));
            let dropped_names: BTreeSet<_> = dropped.into_iter().collect();

            previous
                .iter()
                .filter(|identity| dropped_names.contains(&table_key(identity)))
                .cloned()
                .collect()
        }

        fn incoming_subset_case() -> impl Strategy<
            Value = (
                Vec<GeneratedTableIdentity>,
                Vec<GeneratedTableIdentity>,
                Vec<GeneratedDropMarker>,
                u64,
            ),
        > {
            (
                prop::collection::btree_set(arb_generated_table_identity(), 1..5),
                prop::collection::vec(arb_generated_drop_marker(), 0..5),
                0u64..16,
            )
                .prop_flat_map(|(previous, markers, snapshot_log_index)| {
                    let previous: Vec<_> = previous.into_iter().collect();
                    let len = previous.len();
                    (
                        Just(previous),
                        Just(markers),
                        Just(snapshot_log_index),
                        0usize..(1usize << len),
                    )
                })
                .prop_map(|(previous, markers, snapshot_log_index, mask)| {
                    let incoming: Vec<_> = previous
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| (mask & (1usize << idx)) != 0)
                        .map(|(_, identity)| identity.clone())
                        .collect();
                    (previous, incoming, markers, snapshot_log_index)
                })
        }

        fn strict_subset_without_markers_case(
        ) -> impl Strategy<Value = (Vec<GeneratedTableIdentity>, Vec<GeneratedTableIdentity>)>
        {
            prop::collection::btree_set(arb_generated_table_identity(), 1..5)
                .prop_flat_map(|previous| {
                    let previous: Vec<_> = previous.into_iter().collect();
                    let len = previous.len();
                    (Just(previous), 0usize..((1usize << len) - 1))
                })
                .prop_map(|(previous, mask)| {
                    let incoming: Vec<_> = previous
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| (mask & (1usize << idx)) != 0)
                        .map(|(_, identity)| identity.clone())
                        .collect();
                    (previous, incoming)
                })
        }

        fn same_name_recreate_case(
        ) -> impl Strategy<Value = (GeneratedTableIdentity, GeneratedDropMarker, u64)> {
            arb_generated_table_identity().prop_flat_map(|new_identity| {
                let old_generation = new_identity.generation.saturating_add(1);
                let old_identity = GeneratedTableIdentity {
                    name: new_identity.name.clone(),
                    generation: old_generation,
                };
                let marker = GeneratedDropMarker {
                    identity: old_identity,
                    drop_log_index: 0,
                };
                (Just(new_identity), Just(marker), 0u64..16)
            })
        }

        fn index_convergence_case() -> impl Strategy<
            Value = (
                Vec<GeneratedTableIdentity>,
                Vec<GeneratedTableIdentity>,
                Vec<GeneratedDropMarker>,
                Vec<GeneratedIndexDeclaration>,
                u64,
            ),
        > {
            (
                incoming_subset_case(),
                prop::collection::vec(arb_generated_index_declaration(), 1..8),
            )
                .prop_map(
                    |((previous, incoming, markers, snapshot_log_index), indexes)| {
                        (previous, incoming, markers, indexes, snapshot_log_index)
                    },
                )
        }

        /// Generator reachable-state argument for ADR properties #9-#12.
        ///
        /// The shared `ferrosa-common::test_generators` strategies produce:
        /// a strict subset with no markers (`strict_subset_without_markers_case`),
        /// a matching marked drop (`incoming_subset_case` with matching marker), and
        /// a stale/mismatched marker for same-name recreate (`same_name_recreate_case`).
        /// Those cover the incident states: empty/subset snapshot, explicit drop,
        /// and stale old-identity tombstone against a new table of the same name.
        #[test]
        fn generated_snapshot_cases_cover_incident_shapes() {
            let previous = vec![GeneratedTableIdentity {
                name: ferrosa_common::test_generators::GeneratedTableName {
                    keyspace: "agent_memory".to_string(),
                    table: "entity_store".to_string(),
                },
                generation: 1,
            }];
            let incoming = Vec::new();
            let marker = GeneratedDropMarker {
                identity: previous[0].clone(),
                drop_log_index: 7,
            };
            let stale_marker = GeneratedDropMarker {
                identity: GeneratedTableIdentity {
                    generation: 0,
                    ..previous[0].clone()
                },
                drop_log_index: 6,
            };

            assert!(
                !tables_absent_from_snapshot(&table_map(&previous), &table_map(&incoming))
                    .is_empty(),
                "non-system application keyspaces are reachable as ambiguous snapshot absences"
            );
            assert!(
                inferred_unregistered_identities(&previous, &incoming).is_empty(),
                "ambiguous snapshot absence must no longer authorize destructive unregister"
            );
            assert!(has_matching_marker(&[marker], &previous[0], 7));
            assert!(!has_matching_marker(&[stale_marker], &previous[0], 7));
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(64))]

            /// ADR #9: destructive unregisters must be a subset of explicit,
            /// identity-matching, log-ordered drop markers. Ignored until snapshot
            /// payloads carry explicit markers; run with `--ignored` to see the
            /// current absence-infers-drop counterexample from the ADR.
            #[test]
            fn prop_destructive_unregisters_are_subset_of_explicit_drop_markers(
                (previous, incoming, markers, snapshot_log_index) in incoming_subset_case()
            ) {
                let inferred = inferred_unregistered_identities(&previous, &incoming);

                for identity in inferred {
                    prop_assert!(
                        has_matching_marker(&markers, &identity, snapshot_log_index),
                        "destructive unregister {:?} has no explicit matching marker at/before snapshot log {}; markers={:?}; incoming={:?}",
                        identity,
                        snapshot_log_index,
                        markers,
                        incoming
                    );
                }
            }

            /// ADR #10: absence alone is non-destructive. Ignored until
            /// `apply_snapshot_data` stops feeding previous-minus-next into
            /// `engine.unregister_table`.
            #[test]
            fn prop_absence_alone_is_non_destructive(
                (previous, incoming) in strict_subset_without_markers_case()
            ) {
                let inferred = inferred_unregistered_identities(&previous, &incoming);

                prop_assert!(
                    inferred.is_empty(),
                    "strict subset snapshot without markers inferred destructive unregisters; previous={:?}; incoming={:?}; inferred={:?}",
                    previous,
                    incoming,
                    inferred
                );
            }

            /// ADR #11: same-name recreate safety. A marker for identity A must
            /// not delete identity B's table artifacts or index registrations.
            #[test]
            fn prop_same_name_recreate_drop_marker_cannot_delete_new_identity(
                (new_identity, stale_marker, snapshot_log_index) in same_name_recreate_case()
            ) {
                let previous = vec![new_identity.clone()];
                let incoming = Vec::new();
                let inferred = inferred_unregistered_identities(&previous, &incoming);

                prop_assert!(
                    !inferred.contains(&new_identity),
                    "stale marker {:?} must not authorize deleting recreated table identity {:?}; inferred={:?}; snapshot_log_index={}",
                    stale_marker,
                    new_identity,
                    inferred,
                    snapshot_log_index
                );
            }

            /// ADR #12 for the current no-marker snapshot format: live declared
            /// indexes, persisted `system_schema.indexes` rows, and registered
            /// tracker entries must not be drained by ambiguous snapshot absence.
            #[test]
            fn prop_index_sets_converge_except_explicitly_dropped(
                (previous, incoming, markers, persisted_indexes, snapshot_log_index) in index_convergence_case()
            ) {
                let inferred_drops = inferred_unregistered_identities(&previous, &incoming);
                let inferred_drop_set: BTreeSet<_> = inferred_drops.into_iter().collect();

                let intended_live: BTreeSet<_> = persisted_indexes.iter().cloned().collect();

                let current_persisted_after_snapshot: BTreeSet<_> = persisted_indexes
                    .iter()
                    .filter(|idx| !inferred_drop_set.contains(&idx.table))
                    .cloned()
                    .collect();
                let current_registered_after_restart = current_persisted_after_snapshot.clone();

                prop_assert_eq!(
                    current_persisted_after_snapshot.clone(),
                    intended_live.clone(),
                    "persisted system_schema.indexes rows diverged from intended live declarations; previous={:?}; incoming={:?}; markers={:?}; snapshot_log_index={}",
                    previous,
                    incoming,
                    markers,
                    snapshot_log_index
                );
                prop_assert_eq!(
                    current_registered_after_restart,
                    intended_live,
                    "registered index tracker entries diverged from intended live declarations after restart"
                );
            }
        }
    }
    #[tokio::test]
    async fn committed_schema_effect_failure_does_not_publish_or_apply_later_entries() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(schema.clone(), engine.clone());
        sm.apply(vec![
            make_entry(
                1,
                1,
                RaftOp::CreateKeyspace(simple_keyspace("schema_failure")),
            ),
            make_entry(
                1,
                2,
                RaftOp::CreateTable(Box::new(simple_table("schema_failure", "victim"))),
            ),
        ])
        .await
        .unwrap();
        // A valid committed ALTER must not silently succeed when this node's
        // shared registry has drifted from the committed table it still holds.
        schema
            .drop_table_internal("schema_failure", "victim")
            .unwrap();
        assert!(sm
            .state
            .tables
            .contains_key(&("schema_failure".into(), "victim".into())));
        assert!(engine
            .table_schema(&TableId::new("schema_failure", "victim"))
            .is_some());
        let update = ferrosa_schema::metadata::table::TableUpdates {
            params: None,
            drop_columns: vec![],
            extensions: None,
            add_columns: vec![ColumnMetadata {
                name: "added".into(),
                kind: ColumnKind::Regular,
                position: 1,
                column_type: "text".into(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            }],
        };
        let outcome = sm
            .apply(vec![make_entry(
                1,
                3,
                RaftOp::AlterTable {
                    keyspace: "schema_failure".into(),
                    table: "victim".into(),
                    updates: Box::new(update),
                },
            )])
            .await;
        assert!(
            outcome.is_err(),
            "A missing local Schema effect must not publish committed success: {outcome:?}"
        );
        assert_eq!(sm.last_applied.unwrap().index, 2);
        assert!(
            !sm.state.tables[&("schema_failure".into(), "victim".into())]
                .columns
                .contains_key("added")
        );
        assert!(sm.apply_failure.is_some());
        assert!(sm
            .apply(vec![make_entry(
                1,
                4,
                RaftOp::CreateTable(Box::new(simple_table("schema_failure", "later")))
            )])
            .await
            .is_err());
        assert!(engine
            .table_schema(&TableId::new("schema_failure", "later"))
            .is_none());
    }

    #[tokio::test]
    async fn physical_table_failure_keeps_committed_index_unpublished_and_latches() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let schema = Arc::new(test_schema_instance());
        let mut sm = FerrosStateMachine::with_side_effects(schema, engine.clone());
        sm.apply(vec![make_entry(
            1,
            1,
            RaftOp::CreateKeyspace(simple_keyspace("physical_failure")),
        )])
        .await
        .unwrap();
        let previous = sm.state.clone();
        let sabotage = dir.path().join("sstables/physical_failure.victim");
        std::fs::write(&sabotage, b"owned filesystem refusal").unwrap();
        let result = sm
            .apply(vec![make_entry(
                1,
                2,
                RaftOp::CreateTable(Box::new(simple_table("physical_failure", "victim"))),
            )])
            .await;
        assert!(result.is_err(), "A failed physical effect must be a state-machine failure, not a published command response: {result:?}");
        assert_eq!(sm.last_applied.unwrap().index, 1);
        assert_eq!(sm.state.schema_version, previous.schema_version);
        assert!(!sm
            .state
            .tables
            .contains_key(&("physical_failure".into(), "victim".into())));
        assert!(sm.apply_failure.is_some());
        assert!(sm.applied_state().await.is_err());
        assert!(sm.build_snapshot().await.is_err());
        assert!(sm
            .apply(vec![make_entry(
                1,
                3,
                RaftOp::CreateTable(Box::new(simple_table("physical_failure", "later")))
            )])
            .await
            .is_err());
        assert!(engine
            .table_schema(&TableId::new("physical_failure", "later"))
            .is_none());
        assert_eq!(
            std::fs::read(sabotage).unwrap(),
            b"owned filesystem refusal"
        );
    }

    #[tokio::test]
    async fn failed_persisted_restore_keeps_prior_metadata_and_refuses_progress() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("raft.snapshot");
        let mut source = FerrosStateMachine::with_snapshot_path(snapshot_path.clone());
        source
            .apply(vec![
                make_entry(
                    1,
                    1,
                    RaftOp::CreateKeyspace(simple_keyspace("restore_failure")),
                ),
                make_entry(
                    1,
                    2,
                    RaftOp::CreateTable(Box::new(simple_table("restore_failure", "victim"))),
                ),
            ])
            .await
            .unwrap();
        source.build_snapshot().await.unwrap();
        let snapshot_bytes = std::fs::read(&snapshot_path).unwrap();
        let engine_root = dir.path().join("engine");
        let engine = test_engine(&engine_root);
        let schema = Arc::new(test_schema_instance());
        let mut restored = FerrosStateMachine::with_side_effects_and_snapshot_path(
            schema,
            engine,
            snapshot_path.clone(),
        );
        let previous = restored.state.clone();
        std::fs::create_dir_all(engine_root.join("sstables")).unwrap();
        std::fs::write(
            engine_root.join("sstables/restore_failure.victim"),
            b"owned refusal",
        )
        .unwrap();
        assert!(restored.recover_from_persisted_snapshot().is_err());
        assert!(
            restored.last_applied.is_none(),
            "Failed restoration must not adopt the snapshot's applied index."
        );
        assert_eq!(restored.state.schema_version, previous.schema_version);
        assert_eq!(restored.state.tables.len(), previous.tables.len());
        assert!(
            restored.apply_failure.is_some(),
            "Partial shared effects must prevent this instance from serving progress."
        );
        assert!(restored.applied_state().await.is_err());
        assert!(restored.build_snapshot().await.is_err());
        assert_eq!(std::fs::read(snapshot_path).unwrap(), snapshot_bytes);
    }
}

/// P0-4: the replicated "bootstrap complete" record.
#[cfg(test)]
mod bootstrap_record_tests {
    use super::*;
    use crate::raft::{DataMovementEvidence, NodeState};
    use openraft::{CommittedLeaderId, Entry, EntryPayload};

    fn entry(index: u64, op: RaftOp) -> Entry<FerrosRaftConfig> {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Normal(RaftCommand {
                op,
                schema_version: Uuid::new_v4(),
            }),
        }
    }

    fn joining(host_id: Uuid) -> RaftOp {
        RaftOp::JoinNode(NodeInfo {
            host_id,
            addr: "10.0.0.2:7000".into(),
            data_center: "dc1".into(),
            rack: "rack1".into(),
            state: NodeState::Joining,
            cql_broadcast: None,
        })
    }

    fn evidence() -> DataMovementEvidence {
        DataMovementEvidence {
            sessions: 3,
            ranges: 2,
            partitions: 17,
        }
    }

    /// A record for a `Joining` member is kept, and the live ring carries it
    /// so the promote pass (which reads the ring) can see it.
    #[tokio::test]
    async fn record_for_a_joining_member_is_kept_and_published_to_the_ring() {
        let mut sm = FerrosStateMachine::new();
        let ring = Arc::new(ArcSwap::from_pointee(TokenRing::new()));
        sm.set_ring(ring.clone());
        let host = Uuid::new_v4();
        let node_id = crate::raft::uuid_to_node_id(host);

        let responses = sm
            .apply(vec![
                entry(1, joining(host)),
                entry(
                    2,
                    RaftOp::RecordBootstrapComplete {
                        node_id,
                        evidence: evidence(),
                    },
                ),
            ])
            .await
            .unwrap();

        assert!(matches!(responses[1], RaftResponse::Ok), "{responses:?}");
        assert_eq!(
            sm.state().bootstrap_complete.get(&node_id),
            Some(&evidence())
        );
        assert!(
            ring.load().bootstrap_complete(node_id),
            "the ring the promote pass reads must carry the record"
        );
    }

    /// A record for a node that is not a member is refused loudly, not stored:
    /// a stale record must not pre-authorise a later join.
    #[tokio::test]
    async fn record_for_a_non_member_is_refused() {
        let mut sm = FerrosStateMachine::new();
        let responses = sm
            .apply(vec![entry(
                1,
                RaftOp::RecordBootstrapComplete {
                    node_id: 42,
                    evidence: evidence(),
                },
            )])
            .await
            .unwrap();

        assert!(
            matches!(&responses[0], RaftResponse::Error(msg) if msg.contains("not a member")),
            "a record for a non-member must be an error response, got {responses:?}"
        );
        assert!(sm.state().bootstrap_complete.is_empty());
        assert!(
            sm.apply_failure.is_none(),
            "A domain refusal is not a physical failure."
        );
        let next = sm
            .apply(vec![entry(2, joining(Uuid::new_v4()))])
            .await
            .unwrap();
        assert!(matches!(next[0], RaftResponse::Ok));
        assert_eq!(sm.applied_state().await.unwrap().0.unwrap().index, 2);
    }

    /// Leaving the cluster drops the record, and a fresh `Joining` JoinNode
    /// clears any old one: a node that rejoins must bootstrap again.
    #[tokio::test]
    async fn leave_and_rejoin_clear_the_record() {
        let mut sm = FerrosStateMachine::new();
        let host = Uuid::new_v4();
        let node_id = crate::raft::uuid_to_node_id(host);
        let record = RaftOp::RecordBootstrapComplete {
            node_id,
            evidence: evidence(),
        };

        sm.apply(vec![entry(1, joining(host)), entry(2, record.clone())])
            .await
            .unwrap();
        sm.apply(vec![entry(3, RaftOp::LeaveNode { node_id })])
            .await
            .unwrap();
        assert!(
            sm.state().bootstrap_complete.is_empty(),
            "LeaveNode must drop the record"
        );

        sm.apply(vec![entry(4, joining(host)), entry(5, record)])
            .await
            .unwrap();
        assert!(!sm.state().bootstrap_complete.is_empty());
        sm.apply(vec![entry(6, joining(host))]).await.unwrap();
        assert!(
            sm.state().bootstrap_complete.is_empty(),
            "a Joining JoinNode must clear an earlier record"
        );
    }

    /// The record survives a snapshot build + install.
    #[tokio::test]
    async fn record_survives_a_snapshot_round_trip() {
        let mut sm = FerrosStateMachine::new();
        let host = Uuid::new_v4();
        let node_id = crate::raft::uuid_to_node_id(host);
        sm.apply(vec![
            entry(1, joining(host)),
            entry(
                2,
                RaftOp::RecordBootstrapComplete {
                    node_id,
                    evidence: evidence(),
                },
            ),
        ])
        .await
        .unwrap();
        let snap = sm.build_snapshot().await.unwrap();

        let mut restored = FerrosStateMachine::new();
        restored
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        assert_eq!(
            restored.state().bootstrap_complete.get(&node_id),
            Some(&evidence()),
            "a snapshot must carry the bootstrap record"
        );
    }

    /// A snapshot written by the previous build (no extension) still loads,
    /// with no records: the upgrade must not strand a node behind its own
    /// snapshot. And the new format still decodes as the old struct, so a
    /// not-yet-upgraded peer can load a snapshot an upgraded leader sends.
    #[tokio::test]
    async fn snapshot_format_is_compatible_in_both_directions() {
        let mut sm = FerrosStateMachine::new();
        let host = Uuid::new_v4();
        let node_id = crate::raft::uuid_to_node_id(host);
        sm.apply(vec![
            entry(1, joining(host)),
            entry(
                2,
                RaftOp::RecordBootstrapComplete {
                    node_id,
                    evidence: evidence(),
                },
            ),
        ])
        .await
        .unwrap();
        let snap = sm.build_snapshot().await.unwrap();
        let new_bytes = snap.snapshot.into_inner();

        // Old reader on new bytes.
        let old_view: SnapshotData =
            bincode::deserialize(&new_bytes).expect("old reader must accept the new format");
        assert!(old_view.state.members.contains_key(&node_id));

        // New reader on old bytes.
        let old_bytes = bincode::serialize(&old_view).unwrap();
        let mut restored = FerrosStateMachine::new();
        restored
            .install_snapshot(&snap.meta, Box::new(Cursor::new(old_bytes)))
            .await
            .expect("new reader must accept a pre-upgrade snapshot");
        assert!(restored.state().members.contains_key(&node_id));
        assert!(
            restored.state().bootstrap_complete.is_empty(),
            "a pre-upgrade snapshot has no records"
        );
    }
}
