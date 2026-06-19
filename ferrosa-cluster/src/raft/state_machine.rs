//! Raft state machine: applies [`RaftCommand`] entries to cluster state.
//!
//! [`FerrosStateMachine`] implements openraft's [`RaftStateMachine`] trait.
//! It maintains a deterministic [`RaftState`] (BTreeMap-based) and optionally
//! propagates side effects to a local [`Schema`] and [`StorageEngine`].
//!
//! Snapshots are serialized with bincode because `serde_json` does not support
//! non-string map keys (our `BTreeMap<(String, String), _>` tuple keys).

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

/// Typed sub-error from a single side-effect during `apply_command` (W1.7).
///
/// Today the apply path logs and swallows every Schema / Engine /
/// SystemTableWriter failure (`tracing::error!(%e, ...)` with no
/// further propagation), which has produced two known
/// silent-data-loss bugs:
///
/// 1. `engine.register_table` failure — writes to the new table go
///    nowhere yet `RaftResponse::Ok` is returned to the client.
/// 2. `schema.create_table_internal` failure — schema diverges from
///    the Raft log; queries blow up with "unknown table" hours later.
///
/// `ApplyError` accumulates these so `apply_command` can return
/// `RaftResponse::Error(_)` and callers (`MembershipChanger`,
/// `client_write` users, the schema-coherence audit) can act.  The
/// migration is staged: Sprint 1 introduces the type and wires the
/// engine path; subsequent sprints migrate the remaining sites.
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
    /// Maximum HLC skew observed at apply time relative to the local
    /// wall-clock estimate. Recorded for operator visibility (W7.1
    /// REFACTOR / RAFT_ACCORD_MAX_SKEW gauge).
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    bytes: Vec<u8>,
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

/// Openraft state machine for Ferrosa.
///
/// Applies [`RaftCommand`] entries to [`RaftState`] and optionally propagates
/// side effects to a local [`Schema`] (DDL) and [`StorageEngine`]
/// (table registration).
pub struct FerrosStateMachine {
    state: RaftState,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// Current snapshot data + metadata (kept in memory for `get_current_snapshot`).
    current_snapshot: Option<(SnapshotMeta<u64, BasicNode>, Vec<u8>)>,
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
}

impl FerrosStateMachine {
    fn refresh_current_snapshot_membership(&mut self) {
        let Some((meta, bytes)) = self.current_snapshot.clone() else {
            return;
        };

        let mut data: SnapshotData = match bincode::deserialize(&bytes) {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!(%e, "failed to deserialize cached raft snapshot for membership refresh");
                return;
            }
        };
        data.last_membership = self.last_membership.clone();
        data.last_applied = self.last_applied;

        let refreshed_bytes = match bincode::serialize(&data) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(%e, "failed to serialize refreshed raft snapshot membership");
                return;
            }
        };

        let refreshed_meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id: meta.snapshot_id,
        };
        self.current_snapshot = Some((refreshed_meta.clone(), refreshed_bytes.clone()));

        if let Some(path) = self.snapshot_path.as_ref() {
            if let Err(e) = Self::persist_snapshot_to_disk(path, &refreshed_meta, &refreshed_bytes)
            {
                tracing::warn!(%e, "failed to persist refreshed raft snapshot membership");
            }
        }
    }

    /// Create a new state machine with empty state and no side-effect targets.
    pub fn new() -> Self {
        // Default auto_join=true so initial cluster formation works without
        // explicit approval. Production clusters override via UpdateConfig.
        let mut state = RaftState::default();
        state.config.auto_join = true;
        Self {
            state,
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            schema: None,
            engine: None,
            ring: None,
            ring_observer: None,
            system_writer: None,
            snapshot_path: None,
        }
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
        let system_writer = Some(SystemTableWriter::new(Arc::clone(&engine)));
        Self {
            state: RaftState::default(),
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            schema: Some(schema),
            engine: Some(engine),
            ring: None,
            ring_observer: None,
            system_writer,
            snapshot_path: None,
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
        meta: SnapshotMeta<u64, BasicNode>,
        bytes: Vec<u8>,
    ) -> Result<(), StorageIOError<u64>> {
        let data: SnapshotData = bincode::deserialize(&bytes)
            .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;

        self.state = data.state;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot = Some((meta, bytes));
        self.sync_ring();
        self.sync_schema_and_engine_from_state("Raft snapshot");
        Ok(())
    }

    fn sync_schema_and_engine_from_state(&self, context: &'static str) {
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
            if let Err(e) = schema.apply_snapshot(snap) {
                tracing::error!(%e, %context, "apply_snapshot to schema failed");
            }
        }

        // Re-register all tables with engine if present.
        if let Some(engine) = &self.engine {
            for table in self.state.tables.values() {
                if let Err(e) = engine.register_table(table.to_storage_schema()) {
                    tracing::error!(%e, %context, "register_table failed");
                }
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn persist_snapshot_to_disk(
        path: &Path,
        meta: &SnapshotMeta<u64, BasicNode>,
        bytes: &[u8],
    ) -> Result<(), StorageIOError<u64>> {
        let persisted = PersistedSnapshot {
            meta: meta.clone(),
            bytes: bytes.to_vec(),
        };
        let encoded = bincode::serialize(&persisted)
            .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, encoded)
            .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))?;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    pub fn recover_from_persisted_snapshot(&mut self) -> Result<bool, StorageIOError<u64>> {
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
        self.apply_snapshot_data(persisted.meta, persisted.bytes)?;
        Ok(true)
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
        // (W7.1 REFACTOR / RAFT_ACCORD_MAX_SKEW gauge).
        let skew_us = hlc.time.saturating_sub(self.state.hlc_watermark.time);
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
    fn apply_command(&mut self, cmd: RaftCommand) -> (RaftResponse, Vec<PendingSystemWrite>) {
        let RaftCommand { op, schema_version } = cmd;
        let mut schema_changed = true;
        let mut apply_errors: Vec<ApplyError> = Vec::new();
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
                    if let Err(e) = schema.alter_table_internal(&keyspace, &table, *updates) {
                        tracing::error!(%e, "Raft apply: schema.alter_table_internal failed");
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
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_index_internal(index) {
                        tracing::error!(%e, "Raft apply: schema.create_index_internal failed");
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
                    }
                }
            }

            // ---- DDL: Roles & Grants -----------------------------------
            RaftOp::CreateRole(role) => {
                self.state
                    .roles
                    .entry(role.name.clone())
                    .or_insert_with(|| role.clone());
                if self.system_writer.is_some() {
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::RoleCreated(role.clone()),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_role_internal(role) {
                        tracing::error!(%e, "Raft apply: schema.create_role_internal failed");
                    }
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
                if self.system_writer.is_some() {
                    if let Some(role) = self.state.roles.get(&name) {
                        pending_system_writes.push(PendingSystemWrite {
                            mutation: SystemTableMutation::RoleCreated(role.clone()),
                            level: SystemWriteLogLevel::Error,
                            context: "Raft apply: system table write failed",
                        });
                    }
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.alter_role_internal(&name, updates) {
                        tracing::error!(%e, "Raft apply: schema.alter_role_internal failed");
                    }
                }
            }
            RaftOp::DropRole(name) => {
                self.state.roles.remove(&name);
                self.state.grants.remove(&name);
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.drop_role_internal(&name) {
                        tracing::error!(%e, "Raft apply: schema.drop_role_internal failed");
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
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::GrantUpdated(entry.clone()),
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.grant_internal(entry) {
                        tracing::error!(%e, "Raft apply: schema.grant_internal failed");
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
                    pending_system_writes.push(PendingSystemWrite {
                        mutation: SystemTableMutation::PermissionRevoked {
                            role: role.clone(),
                            resource: resource.clone(),
                            permission,
                        },
                        level: SystemWriteLogLevel::Error,
                        context: "Raft apply: system table write failed for PermissionRevoked",
                    });
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.revoke_internal(&role, &resource, &permission) {
                        tracing::error!(%e, "Raft apply: schema.revoke_internal failed");
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
                    if self.system_writer.is_some() {
                        if let Some(role) = self.state.roles.get(&member) {
                            pending_system_writes.push(PendingSystemWrite {
                                mutation: SystemTableMutation::RoleCreated(role.clone()),
                                level: SystemWriteLogLevel::Error,
                                context: "Raft apply: system table write failed for GrantRole",
                            });
                        }
                    }
                    if let Some(schema) = &self.schema {
                        if let Err(e) = schema.grant_role_internal(&member, &granted_role) {
                            tracing::error!(%e, "Raft apply: schema.grant_role_internal failed");
                        }
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
                if self.system_writer.is_some() {
                    if let Some(role) = self.state.roles.get(&member) {
                        pending_system_writes.push(PendingSystemWrite {
                            mutation: SystemTableMutation::RoleCreated(role.clone()),
                            level: SystemWriteLogLevel::Error,
                            context: "Raft apply: system table write failed for RevokeRole",
                        });
                    }
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.revoke_role_internal(&member, &granted_role) {
                        tracing::error!(%e, "Raft apply: schema.revoke_role_internal failed");
                    }
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

        // W1.7 — surface accumulated sub-errors instead of silently
        // returning Ok.  Callers can detect engine/schema/system_writer
        // failures and decide whether to retry, alert, or escalate.
        let response = if apply_errors.is_empty() {
            RaftResponse::Ok
        } else {
            let summary = apply_errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            RaftResponse::Error(summary)
        };
        (response, pending_system_writes)
    }

    /// Execute the system-table writes collected by `apply_command` on a
    /// blocking thread, then emit the per-mutation log lines on the worker.
    ///
    /// The `engine.write` calls inside `SystemTableWriter::apply` are
    /// synchronous and would otherwise park the raft apply worker, delaying
    /// heartbeat responses (1s lane deadline). Running them under
    /// `spawn_blocking` isolates that work; awaiting the handle here preserves
    /// openraft's sequential apply ordering. A `JoinError` is surfaced loudly
    /// rather than swallowed.
    async fn flush_pending_system_writes(&self, pending: Vec<PendingSystemWrite>) {
        if pending.is_empty() {
            return;
        }
        let Some(writer) = self.system_writer.clone() else {
            return;
        };

        let outcomes = ferrosa_net::task_pool::TaskPool::current("raft-system-table-apply")
            .spawn_blocking(move || {
                pending
                    .into_iter()
                    .map(|p| {
                        let result = writer.apply(p.mutation);
                        (p.level, p.context, result.err())
                    })
                    .collect::<Vec<_>>()
            })
            .await;

        let outcomes = match outcomes {
            Ok(outcomes) => outcomes,
            Err(e) => {
                tracing::error!(
                    %e,
                    "Raft apply: system-table write task panicked or was cancelled"
                );
                return;
            }
        };

        for (level, context, err) in outcomes {
            if let Some(e) = err {
                match level {
                    SystemWriteLogLevel::WarnReplay => tracing::warn!(%e, "{context}"),
                    SystemWriteLogLevel::Error => tracing::error!(%e, "{context}"),
                }
            }
        }
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
        let data = SnapshotData {
            state: self.state.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        };

        let bytes = bincode::serialize(&data)
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

        // Cache the snapshot for get_current_snapshot.
        self.current_snapshot = Some((meta.clone(), bytes.clone()));
        if let Some(path) = self.snapshot_path.clone() {
            let meta_for_disk = meta.clone();
            let bytes_for_disk = bytes.clone();
            #[allow(clippy::result_large_err)]
            ferrosa_net::task_pool::TaskPool::current("raft-log-store")
                .spawn_blocking(move || {
                    Self::persist_snapshot_to_disk(&path, &meta_for_disk, &bytes_for_disk)
                })
                .await
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))??;
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
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<FerrosRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(RaftResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let (resp, pending) = self.apply_command(cmd);
                    // Drain the collected system-table writes off the raft
                    // worker. Awaiting here (before the next entry) preserves
                    // openraft's sequential apply ordering while keeping the
                    // blocking `engine.write` calls off the worker thread.
                    self.flush_pending_system_writes(pending).await;
                    responses.push(resp);
                }
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(RaftResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Return a clone of ourselves as the snapshot builder.
        // This is the simplest approach — the builder has a consistent view
        // of state at this point in time.
        FerrosStateMachine {
            state: self.state.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            current_snapshot: self.current_snapshot.clone(),
            schema: None, // snapshot builder doesn't need side effects
            engine: None,
            ring: None,          // snapshot builder doesn't need live ring
            ring_observer: None, // snapshot builder doesn't need observability ring
            system_writer: None, // snapshot builder doesn't need system writer
            snapshot_path: self.snapshot_path.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        self.apply_snapshot_data(meta.clone(), bytes.clone())?;
        if let Some(path) = self.snapshot_path.clone() {
            let meta_for_disk = meta.clone();
            #[allow(clippy::result_large_err)]
            ferrosa_net::task_pool::TaskPool::current("raft-log-store")
                .spawn_blocking(move || {
                    Self::persist_snapshot_to_disk(&path, &meta_for_disk, &bytes)
                })
                .await
                .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))??;
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<FerrosRaftConfig>>, StorageError<u64>> {
        match &self.current_snapshot {
            Some((meta, bytes)) => Ok(Some(Snapshot {
                meta: meta.clone(),
                snapshot: Box::new(Cursor::new(bytes.clone())),
            })),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Convert an error into an `AnyError` for openraft storage errors.
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

    // -- tests ------------------------------------------------------------

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
        if let Some((meta, bytes)) = sm.current_snapshot.as_mut() {
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
    async fn get_current_snapshot_returns_none_initially() {
        let mut sm = FerrosStateMachine::new();
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_none());
    }

    #[tokio::test]
    async fn get_current_snapshot_after_build() {
        let mut sm = FerrosStateMachine::new();
        let entry = make_entry(1, 1, RaftOp::CreateKeyspace(simple_keyspace("ks1")));
        sm.apply(vec![entry]).await.unwrap();

        // Build snapshot.
        sm.build_snapshot().await.unwrap();

        // Now get_current_snapshot should return Some.
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_some());
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
        use ferrosa_storage::engine::StorageEngineConfig;
        use ferrosa_storage::{CommitLogConfig, CompactionConfig};
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
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
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    /// W1.7 — `apply_command` returns `RaftResponse::Error` instead of
    /// silently swallowing engine.register_table failures.
    ///
    /// The engine's `register_table` calls `std::fs::create_dir_all` on
    /// `<data_dir>/sstables/<keyspace>:<table>`.  If that path already
    /// exists as a regular file, the call fails with "Not a directory".
    /// We pre-create the file, then issue `RaftOp::CreateTable` and
    /// confirm the apply returns `RaftResponse::Error` carrying the
    /// `engine.register_table` reason.
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

        let responses = sm
            .apply(vec![create_ks_entry, create_tbl_entry])
            .await
            .unwrap();

        // Two responses: CreateKeyspace -> Ok, CreateTable -> Error.
        assert_eq!(responses.len(), 2);
        assert!(
            matches!(&responses[0], RaftResponse::Ok),
            "CreateKeyspace should succeed: {:?}",
            responses[0]
        );
        match &responses[1] {
            RaftResponse::Error(msg) => {
                assert!(
                    msg.contains("engine.register_table"),
                    "expected register_table failure, got: {msg}"
                );
                assert!(
                    msg.contains("regress_ks") && msg.contains("victim_tbl"),
                    "error should name the failing keyspace/table: {msg}"
                );
            }
            other => panic!("expected RaftResponse::Error, got {other:?}"),
        }
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
        sm.apply_command(create_cmd);

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
        sm.apply_command(status_cmd);

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
        sm.apply_command(ready_cmd);

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
        sm.apply_command(create_cmd);

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
            sm.apply_command(cmd);
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
        });
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        });

        assert!(!sm.state().index_state_map.is_empty());

        sm.apply_command(RaftCommand {
            op: RaftOp::DropIndex {
                keyspace: "ks".into(),
                table: "tbl".into(),
                index: "idx".into(),
            },
            schema_version: Uuid::new_v4(),
        });

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
        });
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
        });
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        });

        assert!(!sm.state().index_state_map.is_empty());

        sm.apply_command(RaftCommand {
            op: RaftOp::DropKeyspace("ks".into()),
            schema_version: Uuid::new_v4(),
        });

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
        });
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        });

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
        });
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: 1,
                keyspace: "ks".into(),
                table: "other_tbl".into(),
                index_name: "idx2".into(),
                status: IndexNodeStatus::Ready,
            },
            schema_version: Uuid::new_v4(),
        });

        assert_eq!(sm.state().index_state_map.len(), 2);

        sm.apply_command(RaftCommand {
            op: RaftOp::DropTable {
                keyspace: "ks".into(),
                table: "tbl".into(),
            },
            schema_version: Uuid::new_v4(),
        });

        assert_eq!(sm.state().index_state_map.len(), 1);
        assert!(sm.state().index_state_map.contains_key(&(
            "ks".into(),
            "other_tbl".into(),
            "idx2".into()
        )));
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
        });

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
            });
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
        });

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
        });

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
        });

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
        });

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
        });

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
        });

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
        });
        sm.apply_command(RaftCommand {
            op: RaftOp::AssignTokens {
                node_id,
                tokens: vec![-10, 0, 10],
            },
            schema_version: Uuid::new_v4(),
        });

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
        });

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
            });
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
        });

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
                sm.apply_command(cmd.clone());
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
            });
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
        });

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
        });
        sm.apply_command(RaftCommand {
            op: RaftOp::IndexStatus {
                node_id: node2_id,
                keyspace: "ks".into(),
                table: "tbl".into(),
                index_name: "idx".into(),
                status: IndexNodeStatus::Failed("disk full".into()),
            },
            schema_version: Uuid::new_v4(),
        });

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
}
