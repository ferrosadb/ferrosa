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

use ferrosa_common::CqlType;
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
use crate::raft::{
    FerrosRaftConfig, IndexNodeStatus, NodeInfo, RaftCommand, RaftOp, RaftResponse, Token,
};
use crate::ring::TokenRing;

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
        Ok(())
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
        if let Some(ring_swap) = &self.ring {
            let mut ring = TokenRing::new();

            // Populate nodes.
            for (&node_id, info) in &self.state.members {
                ring.add_node(node_id, info.clone());
            }

            // Populate token assignments.
            for (&token, &node_id) in &self.state.token_map {
                ring.assign_tokens(node_id, &[token]);
            }

            ring_swap.store(Arc::new(ring));
        }
    }

    pub fn sync_live_ring_from_state(&self) {
        self.sync_ring();
    }

    /// Apply a single [`RaftCommand`] to `self.state`, updating BTreeMaps
    /// and optionally propagating side effects.
    fn apply_command(&mut self, cmd: RaftCommand) -> RaftResponse {
        let RaftCommand { op, schema_version } = cmd;
        let mut schema_changed = true;
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
                if inserted {
                    let ks_clone = ks.clone();
                    if let Some(schema) = &self.schema {
                        if let Err(e) = schema.create_keyspace_internal(ks) {
                            tracing::error!(%e, "Raft apply: create_keyspace_internal failed — schema diverged from Raft state");
                        }
                    }
                    if let Some(writer) = &self.system_writer {
                        if let Err(e) = writer.apply(SystemTableMutation::KeyspaceCreated(ks_clone))
                        {
                            tracing::warn!(%e, "Raft apply: system table write skipped for CreateKeyspace (expected during log replay)");
                        }
                    }
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
                        }
                    }
                }
                if let Some(writer) = &self.system_writer {
                    if let Err(e) = writer.apply(SystemTableMutation::KeyspaceDropped(name.clone()))
                    {
                        tracing::error!(%e, "Raft apply: system table write failed");
                    }
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
                if let Some(writer) = &self.system_writer {
                    if let Some(ks) = self.state.keyspaces.get(&name) {
                        if let Err(e) =
                            writer.apply(SystemTableMutation::KeyspaceCreated(ks.clone()))
                        {
                            tracing::error!(%e, "Raft apply: system table write failed for AlterKeyspace");
                        }
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
                if inserted {
                    if let Some(schema) = &self.schema {
                        if let Err(e) = schema.create_table_internal(*table.clone()) {
                            tracing::error!(%e, "Raft apply: create_table_internal failed — schema diverged");
                        }
                    }
                    if let Some(engine) = &self.engine {
                        if let Err(e) = engine.register_table(table.to_storage_schema()) {
                            tracing::error!(%e, "Raft apply: register_table failed — writes to this table will silently fail");
                        }
                    }
                    if let Some(writer) = &self.system_writer {
                        if let Err(e) = writer.apply(SystemTableMutation::TableCreated(table)) {
                            // Warn, not error: during Raft log replay on startup,
                            // system_schema tables may not be registered yet.  The
                            // schema bootstrap populates them once loading completes.
                            tracing::warn!(%e, "Raft apply: system table write skipped for CreateTable (expected during log replay)");
                        }
                    }
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
                    }
                }
                if let Some(writer) = &self.system_writer {
                    if let Err(e) = writer.apply(SystemTableMutation::TableDropped {
                        keyspace: keyspace.clone(),
                        table: table.clone(),
                    }) {
                        tracing::error!(%e, "Raft apply: system table write failed for DropTable");
                    }
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
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.create_type_internal(&udt) {
                        tracing::error!(%e, "Raft apply: schema.create_type_internal failed");
                    }
                }
            }
            RaftOp::DropType { keyspace, name } => {
                self.state.types.remove(&(keyspace.clone(), name.clone()));
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
                if let Some(writer) = &self.system_writer {
                    if let Err(e) = writer.apply(SystemTableMutation::RoleCreated(role.clone())) {
                        tracing::error!(%e, "Raft apply: system table write failed");
                    }
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
                if let Some(writer) = &self.system_writer {
                    if let Some(role) = self.state.roles.get(&name) {
                        if let Err(e) = writer.apply(SystemTableMutation::RoleCreated(role.clone()))
                        {
                            tracing::error!(%e, "Raft apply: system table write failed");
                        }
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
                if let Some(writer) = &self.system_writer {
                    if let Err(e) = writer.apply(SystemTableMutation::RoleDropped(name.clone())) {
                        tracing::error!(%e, "Raft apply: system table write failed");
                    }
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
                if let Some(writer) = &self.system_writer {
                    if let Err(e) = writer.apply(SystemTableMutation::GrantUpdated(entry.clone())) {
                        tracing::error!(%e, "Raft apply: system table write failed");
                    }
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
                if let Some(writer) = &self.system_writer {
                    if let Err(e) = writer.apply(SystemTableMutation::PermissionRevoked {
                        role: role.clone(),
                        resource: resource.clone(),
                        permission,
                    }) {
                        tracing::error!(%e, "Raft apply: system table write failed for PermissionRevoked");
                    }
                }
                if let Some(schema) = &self.schema {
                    if let Err(e) = schema.revoke_internal(&role, &resource, &permission) {
                        tracing::error!(%e, "Raft apply: schema.revoke_internal failed");
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
        RaftResponse::Ok
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
            tokio::task::spawn_blocking(move || {
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
                    let resp = self.apply_command(cmd);
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
            tokio::task::spawn_blocking(move || {
                Self::persist_snapshot_to_disk(&path, &meta_for_disk, &bytes)
            })
            .await
            .map_err(|e| StorageIOError::write_state_machine(to_any_error(e)))??;
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
            if let Err(e) = schema.apply_snapshot(snap) {
                tracing::error!(%e, "Raft snapshot: apply_snapshot to schema failed");
            }
        }

        // Re-register all tables with engine if present.
        if let Some(engine) = &self.engine {
            for table in self.state.tables.values() {
                if let Err(e) = engine.register_table(table.to_storage_schema()) {
                    tracing::error!(%e, "Raft snapshot: register_table failed");
                }
            }
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};

    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

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
        let entries = vec![
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
        };

        let entries = vec![
            make_entry(1, 1, RaftOp::CreateRole(role)),
            make_entry(1, 2, RaftOp::DropRole("analyst".to_string())),
        ];
        sm.apply(entries).await.unwrap();

        assert!(!sm.state().roles.contains_key("analyst"));
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
        let _ = sm.build_snapshot().await.unwrap();

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

        let entries = vec![
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
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
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
