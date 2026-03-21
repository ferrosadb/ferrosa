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
use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
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
use ferrosa_schema::{GrantEntry, RoleMetadata, Schema};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

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
}

impl FerrosStateMachine {
    /// Create a new state machine with empty state and no side-effect targets.
    pub fn new() -> Self {
        Self {
            state: RaftState::default(),
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            schema: None,
            engine: None,
            ring: None,
        }
    }

    /// Create a new state machine wired to local `Schema` and `StorageEngine`
    /// for side-effect propagation.
    pub fn with_side_effects(schema: Arc<Schema>, engine: Arc<StorageEngine>) -> Self {
        Self {
            state: RaftState::default(),
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            schema: Some(schema),
            engine: Some(engine),
            ring: None,
        }
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

    /// Apply a single [`RaftCommand`] to `self.state`, updating BTreeMaps
    /// and optionally propagating side effects.
    fn apply_command(&mut self, cmd: RaftCommand) -> RaftResponse {
        let RaftCommand { op, schema_version } = cmd;
        match op {
            // ---- DDL: Keyspaces ----------------------------------------
            RaftOp::CreateKeyspace(ks) => {
                self.state
                    .keyspaces
                    .entry(ks.name.clone())
                    .or_insert_with(|| ks.clone());
                if let Some(schema) = &self.schema {
                    let _ = schema.create_keyspace_internal(ks);
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
                    let _ = schema.drop_keyspace_internal(&name);
                }
                if let Some(engine) = &self.engine {
                    for (ks, tbl) in dropped_tables {
                        let tid = TableId::new(&ks, &tbl);
                        let _ = engine.unregister_table(&tid);
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
                    let _ = schema.alter_keyspace_internal(&name, updates);
                }
            }

            // ---- DDL: Tables -------------------------------------------
            RaftOp::CreateTable(table) => {
                let key = (table.keyspace.clone(), table.name.clone());
                self.state
                    .tables
                    .entry(key)
                    .or_insert_with(|| *table.clone());
                if let Some(schema) = &self.schema {
                    let _ = schema.create_table_internal(*table.clone());
                }
                if let Some(engine) = &self.engine {
                    let _ = engine.register_table(table.to_storage_schema());
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
                    let _ = schema.drop_table_internal(&keyspace, &table);
                }
                if let Some(engine) = &self.engine {
                    let tid = TableId::new(&keyspace, &table);
                    let _ = engine.unregister_table(&tid);
                }
            }
            RaftOp::AlterTable {
                keyspace,
                table,
                updates,
            } => {
                if let Some(tbl) = self
                    .state
                    .tables
                    .get_mut(&(keyspace.clone(), table.clone()))
                {
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
                }
                if let Some(schema) = &self.schema {
                    let _ = schema.alter_table_internal(&keyspace, &table, *updates);
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
                    let _ = schema.create_index_internal(index);
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
                    let _ = schema.drop_index_internal(&keyspace, &table, &index);
                }
            }
            RaftOp::IndexStatus {
                node_id,
                keyspace,
                table,
                index_name,
                status,
            } => {
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
                    let _ = schema.create_type_internal(&udt);
                }
            }
            RaftOp::DropType { keyspace, name } => {
                self.state.types.remove(&(keyspace.clone(), name.clone()));
                if let Some(schema) = &self.schema {
                    let _ = schema.drop_type_internal(&keyspace, &name);
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
                    let _ = schema.create_function_internal(&func);
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
                    let _ = schema.drop_function_internal(&keyspace, &name, &arg_types);
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
                    let _ = schema.create_aggregate_internal(&agg);
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
                    let _ = schema.drop_aggregate_internal(&keyspace, &name, &arg_types);
                }
            }

            // ---- DDL: Roles & Grants -----------------------------------
            RaftOp::CreateRole(role) => {
                self.state
                    .roles
                    .entry(role.name.clone())
                    .or_insert_with(|| role.clone());
                if let Some(schema) = &self.schema {
                    let _ = schema.create_role_internal(role);
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
                    let _ = schema.alter_role_internal(&name, updates);
                }
            }
            RaftOp::DropRole(name) => {
                self.state.roles.remove(&name);
                self.state.grants.remove(&name);
                if let Some(schema) = &self.schema {
                    let _ = schema.drop_role_internal(&name);
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
                if let Some(schema) = &self.schema {
                    let _ = schema.grant_internal(entry);
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
                if let Some(schema) = &self.schema {
                    let _ = schema.revoke_internal(&role, &resource, &permission);
                }
            }

            // ---- Topology ----------------------------------------------
            RaftOp::JoinNode(node_info) => {
                let node_id = super::uuid_to_node_id(node_info.host_id);
                self.state.members.insert(node_id, node_info);
                self.sync_ring();
                // Mark the new node as Building for all existing indexes.
                for statuses in self.state.index_state_map.values_mut() {
                    statuses.entry(node_id).or_insert(IndexNodeStatus::Building);
                }
            }
            RaftOp::LeaveNode { node_id } => {
                self.state.members.remove(&node_id);
                self.state.token_map.retain(|_, n| *n != node_id);
                self.sync_ring();
                // Remove departing node from per-index build status.
                for statuses in self.state.index_state_map.values_mut() {
                    statuses.remove(&node_id);
                }
            }
            RaftOp::AssignTokens { node_id, tokens } => {
                for token in tokens {
                    self.state.token_map.insert(token, node_id);
                }
                self.sync_ring();
            }

            // ---- Config ------------------------------------------------
            RaftOp::UpdateConfig(config) => {
                self.state.config = config;
            }

            // ---- Node admission ----------------------------------------
            RaftOp::ApproveNode { host_id } => {
                self.state.approved_nodes.insert(host_id);
            }
        }

        // Use the leader-generated schema version so all nodes agree.
        self.state.schema_version = schema_version;
        if let Some(schema) = &self.schema {
            schema.set_schema_version(schema_version);
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
            ring: None, // snapshot builder doesn't need live ring
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

        let data: SnapshotData = bincode::deserialize(&bytes)
            .map_err(|e| StorageIOError::read_state_machine(to_any_error(e)))?;

        self.state = data.state;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        // Cache the installed snapshot.
        self.current_snapshot = Some((meta.clone(), bytes));

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
            let _ = schema.apply_snapshot(snap);
        }

        // Re-register all tables with engine if present.
        if let Some(engine) = &self.engine {
            for table in self.state.tables.values() {
                let _ = engine.register_table(table.to_storage_schema());
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

    use crate::raft::{IndexNodeStatus, NodeInfo, NodeState, RaftCommand, RaftOp, Token};

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
        for (_, status) in entry {
            assert_eq!(*status, IndexNodeStatus::Building);
        }
    }
}
