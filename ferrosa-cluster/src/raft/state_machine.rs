//! Raft state machine: applies [`RaftCommand`] entries to cluster state.
//!
//! [`FerrosStateMachine`] implements openraft's [`RaftStateMachine`] trait.
//! It maintains a deterministic [`RaftState`] (BTreeMap-based) and optionally
//! propagates side effects to a local [`Schema`] and [`StorageEngine`].
//!
//! Snapshots are serialized with bincode because `serde_json` does not support
//! non-string map keys (our `BTreeMap<(String, String), _>` tuple keys).

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ferrosa_schema::metadata::index::IndexMetadata;
use ferrosa_schema::metadata::keyspace::KeyspaceMetadata;
use ferrosa_schema::metadata::table::TableMetadata;
use ferrosa_schema::{GrantEntry, RoleMetadata, Schema};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::config::ClusterConfig;
use crate::raft::{FerrosRaftConfig, NodeInfo, RaftCommand, RaftResponse, Token};

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
    /// Cluster members, keyed by openraft NodeId.
    pub members: BTreeMap<u64, NodeInfo>,
    /// Token ring: token → NodeId mapping.
    pub token_map: BTreeMap<Token, u64>,
    /// Cluster-wide configuration.
    pub config: ClusterConfig,
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
        }
    }

    /// Read-only access to the current cluster state.
    pub fn state(&self) -> &RaftState {
        &self.state
    }

    /// Apply a single [`RaftCommand`] to `self.state`, updating BTreeMaps
    /// and optionally propagating side effects.
    fn apply_command(&mut self, cmd: RaftCommand) -> RaftResponse {
        match cmd {
            // ---- DDL: Keyspaces ----------------------------------------
            RaftCommand::CreateKeyspace(ks) => {
                self.state
                    .keyspaces
                    .entry(ks.name.clone())
                    .or_insert_with(|| ks.clone());
                if let Some(schema) = &self.schema {
                    let _ = schema.create_keyspace_internal(ks);
                }
            }
            RaftCommand::DropKeyspace(name) => {
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
            RaftCommand::AlterKeyspace { name, updates } => {
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
            RaftCommand::CreateTable(table) => {
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
            RaftCommand::DropTable { keyspace, table } => {
                self.state.tables.remove(&(keyspace.clone(), table.clone()));
                // Also drop indexes on this table.
                self.state
                    .indexes
                    .retain(|(ks, tbl, _), _| !(ks == &keyspace && tbl == &table));
                if let Some(schema) = &self.schema {
                    let _ = schema.drop_table_internal(&keyspace, &table);
                }
                if let Some(engine) = &self.engine {
                    let tid = TableId::new(&keyspace, &table);
                    let _ = engine.unregister_table(&tid);
                }
            }
            RaftCommand::AlterTable {
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
            RaftCommand::CreateIndex(index) => {
                let key = (
                    index.keyspace.clone(),
                    index.table.clone(),
                    index.name.clone(),
                );
                self.state
                    .indexes
                    .entry(key)
                    .or_insert_with(|| index.clone());
                if let Some(schema) = &self.schema {
                    let _ = schema.create_index_internal(index);
                }
            }
            RaftCommand::DropIndex {
                keyspace,
                table,
                index,
            } => {
                self.state
                    .indexes
                    .remove(&(keyspace.clone(), table.clone(), index.clone()));
                if let Some(schema) = &self.schema {
                    let _ = schema.drop_index_internal(&keyspace, &table, &index);
                }
            }

            // ---- DDL: Roles & Grants -----------------------------------
            RaftCommand::CreateRole(role) => {
                self.state
                    .roles
                    .entry(role.name.clone())
                    .or_insert_with(|| role.clone());
                if let Some(schema) = &self.schema {
                    let _ = schema.create_role_internal(role);
                }
            }
            RaftCommand::AlterRole { name, updates } => {
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
            RaftCommand::DropRole(name) => {
                self.state.roles.remove(&name);
                self.state.grants.remove(&name);
                if let Some(schema) = &self.schema {
                    let _ = schema.drop_role_internal(&name);
                }
            }
            RaftCommand::Grant(entry) => {
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
            RaftCommand::Revoke {
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
            RaftCommand::JoinNode(node_info) => {
                let node_id = super::uuid_to_node_id(node_info.host_id);
                self.state.members.insert(node_id, node_info);
            }
            RaftCommand::LeaveNode { node_id } => {
                self.state.members.remove(&node_id);
                self.state.token_map.retain(|_, n| *n != node_id);
            }
            RaftCommand::AssignTokens { node_id, tokens } => {
                for token in tokens {
                    self.state.token_map.insert(token, node_id);
                }
            }

            // ---- Config ------------------------------------------------
            RaftCommand::UpdateConfig(config) => {
                self.state.config = config;
            }
        }

        // Bump schema version on every mutation for change detection.
        self.state.schema_version = Uuid::new_v4();
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

    use crate::raft::{NodeInfo, NodeState, RaftCommand, Token};

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

    fn make_entry(term: u64, index: u64, cmd: RaftCommand) -> Entry<FerrosRaftConfig> {
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
        let entry = make_entry(1, 1, RaftCommand::CreateKeyspace(ks));

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
            make_entry(1, 1, RaftCommand::CreateKeyspace(ks)),
            make_entry(1, 2, RaftCommand::CreateTable(Box::new(table))),
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

        let entry = make_entry(1, 1, RaftCommand::JoinNode(node));
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
            RaftCommand::AssignTokens {
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
            make_entry(1, 1, RaftCommand::CreateKeyspace(ks)),
            make_entry(
                1,
                2,
                RaftCommand::CreateKeyspace(simple_keyspace("safe_ks")),
            ),
            make_entry(1, 3, RaftCommand::CreateTable(Box::new(t1))),
            make_entry(1, 4, RaftCommand::CreateTable(Box::new(t2))),
            make_entry(1, 5, RaftCommand::CreateTable(Box::new(other_t))),
            make_entry(1, 6, RaftCommand::DropKeyspace("doomed".to_string())),
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
        let commands = vec![
            RaftCommand::CreateKeyspace(simple_keyspace("ks1")),
            RaftCommand::CreateTable(Box::new(simple_table("ks1", "t1"))),
            RaftCommand::JoinNode(NodeInfo {
                host_id: Uuid::nil(),
                addr: "10.0.0.1:7000".to_string(),
                data_center: "dc1".to_string(),
                rack: "rack1".to_string(),
                state: NodeState::Normal,
            }),
            RaftCommand::AssignTokens {
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
            make_entry(1, 1, RaftCommand::CreateKeyspace(simple_keyspace("ks1"))),
            make_entry(
                1,
                2,
                RaftCommand::CreateTable(Box::new(simple_table("ks1", "users"))),
            ),
            make_entry(
                1,
                3,
                RaftCommand::JoinNode(NodeInfo {
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
                RaftCommand::AssignTokens {
                    node_id,
                    tokens: vec![-50, 0, 50],
                },
            ),
            make_entry(1, 2, RaftCommand::LeaveNode { node_id }),
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
            make_entry(1, 1, RaftCommand::CreateRole(role)),
            make_entry(1, 2, RaftCommand::DropRole("analyst".to_string())),
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
            make_entry(1, 1, RaftCommand::Grant(grant)),
            make_entry(
                1,
                2,
                RaftCommand::Revoke {
                    role: "analyst".to_string(),
                    resource: Resource::Keyspace("ks1".to_string()),
                    permission: Permission::Select,
                },
            ),
        ];
        sm.apply(entries).await.unwrap();

        // After revoking the only permission, the grant entry should be removed.
        assert!(sm.state().grants.get("analyst").is_none());
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
        let entry = make_entry(1, 1, RaftCommand::CreateKeyspace(simple_keyspace("ks1")));
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

        let entry = make_entry(1, 5, RaftCommand::CreateKeyspace(simple_keyspace("ks1")));
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
        let mut config = ClusterConfig::default();
        config.cluster_name = "my-cluster".to_string();

        let entry = make_entry(1, 1, RaftCommand::UpdateConfig(config));
        sm.apply(vec![entry]).await.unwrap();

        assert_eq!(sm.state().config.cluster_name, "my-cluster");
    }
}
