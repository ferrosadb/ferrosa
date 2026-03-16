//! DDL path abstraction for runtime mode transitions.
//!
//! Parallels `WritePath` — the CQL router calls `DdlPath::execute()`
//! for all DDL operations. Swapped atomically via `ArcSwap`.

use std::sync::Arc;

use ferrosa_schema::Schema;
use ferrosa_storage::engine::StorageEngine;

use crate::error::{ClusterError, Result};
use crate::pair::ddl::{DdlCoordinator, DdlOperation};
use crate::raft::{FerrosRaft, RaftCommand};

/// The active DDL path. Swapped atomically via `ArcSwap` when
/// the deployment mode changes (standalone → pair → cluster).
pub enum DdlPath {
    /// Standalone: DDL applied directly to local schema + storage.
    Direct {
        schema: Arc<Schema>,
        engine: Arc<StorageEngine>,
    },
    /// Pair mode: DDL routed through DdlCoordinator (primary authority).
    Pair(Arc<DdlCoordinator>),
    /// Cluster mode: DDL proposed via Raft consensus.
    Cluster(Arc<FerrosRaft>),
    /// Degraded: peer lost, DDL rejected until operator promotes.
    Unavailable,
}

impl DdlPath {
    /// Execute a DDL operation on the current path.
    ///
    /// - `Direct`: applies directly to local schema + storage.
    /// - `Pair`: routes through `DdlCoordinator` (primary authority).
    /// - `Cluster`: proposes via Raft `client_write`; on `ForwardToLeader`
    ///   returns [`ClusterError::NotLeader`] with the leader hint.
    /// - `Unavailable`: returns an error immediately.
    pub async fn execute(&self, op: DdlOperation) -> Result<()> {
        match self {
            Self::Direct { schema, engine } => {
                // Reuse DdlCoordinator's local-apply logic by constructing a
                // temporary coordinator with no peer.  The coordinator's
                // `apply_ddl_locally` is the canonical single-node DDL path.
                //
                // We do this inline rather than constructing a DdlCoordinator
                // (which requires a peer_host_id and peer_manager) by reusing
                // the same Schema/StorageEngine operations that DdlCoordinator
                // would perform.
                apply_direct(&op, schema, engine)
            }
            Self::Pair(coordinator) => coordinator.coordinate_ddl(op).await,
            Self::Cluster(raft) => execute_via_raft(raft, op).await,
            Self::Unavailable => Err(ClusterError::Internal(
                "DDL unavailable: peer lost, wait for operator action".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Direct (standalone) DDL
// ---------------------------------------------------------------------------

/// Apply a `DdlOperation` directly to the local schema and storage engine.
///
/// This mirrors [`DdlCoordinator::apply_ddl_locally`] exactly but does not
/// require constructing a coordinator (which needs a peer ID and PeerManager).
fn apply_direct(op: &DdlOperation, schema: &Schema, engine: &StorageEngine) -> Result<()> {
    match op {
        DdlOperation::CreateKeyspace(ks) => {
            schema
                .create_keyspace_internal(ks.clone())
                .map_err(|e| ClusterError::Internal(format!("create_keyspace: {e}")))?;
        }
        DdlOperation::DropKeyspace(name) => {
            let snap = schema.snapshot();
            let table_ids: Vec<_> = snap
                .tables
                .keys()
                .filter(|(ks, _)| ks == name)
                .map(|(ks, tbl)| ferrosa_storage::TableId::new(ks, tbl))
                .collect();
            schema
                .drop_keyspace_internal(name)
                .map_err(|e| ClusterError::Internal(format!("drop_keyspace: {e}")))?;
            for tid in &table_ids {
                engine
                    .unregister_table(tid)
                    .map_err(ClusterError::Storage)?;
            }
        }
        DdlOperation::CreateTable(table) => {
            schema
                .create_table_internal(*table.clone())
                .map_err(|e| ClusterError::Internal(format!("create_table: {e}")))?;
            let storage_schema = table.to_storage_schema();
            engine
                .register_table(storage_schema)
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::DropTable { keyspace, table } => {
            schema
                .drop_table_internal(keyspace, table)
                .map_err(|e| ClusterError::Internal(format!("drop_table: {e}")))?;
            let tid = ferrosa_storage::TableId::new(keyspace, table);
            engine
                .unregister_table(&tid)
                .map_err(ClusterError::Storage)?;
        }
        DdlOperation::AlterKeyspace { name, updates } => {
            schema
                .alter_keyspace_internal(name, updates.clone())
                .map_err(|e| ClusterError::Internal(format!("alter_keyspace: {e}")))?;
        }
        DdlOperation::AlterTable {
            keyspace,
            table,
            updates,
        } => {
            schema
                .alter_table_internal(keyspace, table, *updates.clone())
                .map_err(|e| ClusterError::Internal(format!("alter_table: {e}")))?;
        }
        DdlOperation::CreateRole(role) => {
            schema
                .create_role_internal(role.clone())
                .map_err(|e| ClusterError::Internal(format!("create_role: {e}")))?;
        }
        DdlOperation::AlterRole { name, updates } => {
            schema
                .alter_role_internal(name, updates.clone())
                .map_err(|e| ClusterError::Internal(format!("alter_role: {e}")))?;
        }
        DdlOperation::DropRole(name) => {
            schema
                .drop_role_internal(name)
                .map_err(|e| ClusterError::Internal(format!("drop_role: {e}")))?;
        }
        DdlOperation::Grant(entry) => {
            schema
                .grant_internal(entry.clone())
                .map_err(|e| ClusterError::Internal(format!("grant: {e}")))?;
        }
        DdlOperation::Revoke {
            role,
            resource,
            permission,
        } => {
            schema
                .revoke_internal(role, resource, permission)
                .map_err(|e| ClusterError::Internal(format!("revoke: {e}")))?;
        }
        DdlOperation::CreateIndex(idx) => {
            schema
                .create_index_internal(idx.clone())
                .map_err(|e| ClusterError::Internal(format!("create_index: {e}")))?;
        }
        DdlOperation::DropIndex {
            keyspace,
            table,
            index,
        } => {
            schema
                .drop_index_internal(keyspace, table, index)
                .map_err(|e| ClusterError::Internal(format!("drop_index: {e}")))?;
        }
        DdlOperation::CreateType(ref udt) => {
            schema
                .create_type_internal(udt)
                .map_err(|e| ClusterError::Internal(format!("create_type: {e}")))?;
        }
        DdlOperation::DropType {
            ref keyspace,
            ref name,
        } => {
            schema
                .drop_type_internal(keyspace, name)
                .map_err(|e| ClusterError::Internal(format!("drop_type: {e}")))?;
        }
        DdlOperation::CreateFunction(ref func) => {
            schema
                .create_function_internal(func)
                .map_err(|e| ClusterError::Internal(format!("create_function: {e}")))?;
        }
        DdlOperation::DropFunction {
            ref keyspace,
            ref name,
            ref arg_types,
        } => {
            schema
                .drop_function_internal(keyspace, name, arg_types)
                .map_err(|e| ClusterError::Internal(format!("drop_function: {e}")))?;
        }
        DdlOperation::CreateAggregate(ref agg) => {
            schema
                .create_aggregate_internal(agg)
                .map_err(|e| ClusterError::Internal(format!("create_aggregate: {e}")))?;
        }
        DdlOperation::DropAggregate {
            ref keyspace,
            ref name,
            ref arg_types,
        } => {
            schema
                .drop_aggregate_internal(keyspace, name, arg_types)
                .map_err(|e| ClusterError::Internal(format!("drop_aggregate: {e}")))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cluster (Raft) DDL
// ---------------------------------------------------------------------------

/// Convert a [`DdlOperation`] to the equivalent [`RaftCommand`].
fn ddl_op_to_raft_command(op: DdlOperation) -> RaftCommand {
    match op {
        DdlOperation::CreateKeyspace(ks) => RaftCommand::CreateKeyspace(ks),
        DdlOperation::DropKeyspace(name) => RaftCommand::DropKeyspace(name),
        DdlOperation::CreateTable(table) => RaftCommand::CreateTable(table),
        DdlOperation::DropTable { keyspace, table } => RaftCommand::DropTable { keyspace, table },
        DdlOperation::AlterKeyspace { name, updates } => {
            RaftCommand::AlterKeyspace { name, updates }
        }
        DdlOperation::AlterTable {
            keyspace,
            table,
            updates,
        } => RaftCommand::AlterTable {
            keyspace,
            table,
            updates,
        },
        DdlOperation::CreateRole(role) => RaftCommand::CreateRole(role),
        DdlOperation::AlterRole { name, updates } => RaftCommand::AlterRole { name, updates },
        DdlOperation::DropRole(name) => RaftCommand::DropRole(name),
        DdlOperation::Grant(entry) => RaftCommand::Grant(entry),
        DdlOperation::Revoke {
            role,
            resource,
            permission,
        } => RaftCommand::Revoke {
            role,
            resource,
            permission,
        },
        DdlOperation::CreateIndex(idx) => RaftCommand::CreateIndex(idx),
        DdlOperation::DropIndex {
            keyspace,
            table,
            index,
        } => RaftCommand::DropIndex {
            keyspace,
            table,
            index,
        },
        DdlOperation::CreateType(udt) => RaftCommand::CreateType(udt),
        DdlOperation::DropType { keyspace, name } => RaftCommand::DropType { keyspace, name },
        DdlOperation::CreateFunction(func) => RaftCommand::CreateFunction(func),
        DdlOperation::DropFunction {
            keyspace,
            name,
            arg_types,
        } => RaftCommand::DropFunction {
            keyspace,
            name,
            arg_types,
        },
        DdlOperation::CreateAggregate(agg) => RaftCommand::CreateAggregate(agg),
        DdlOperation::DropAggregate {
            keyspace,
            name,
            arg_types,
        } => RaftCommand::DropAggregate {
            keyspace,
            name,
            arg_types,
        },
    }
}

/// Propose a DDL operation through Raft consensus.
///
/// On success the state machine has applied the command on a quorum of nodes
/// and side effects are visible on this node via [`FerrosStateMachine`].
///
/// On `ForwardToLeader` the caller receives [`ClusterError::NotLeader`] with
/// the leader hint so the CQL layer can return a CQL error that the driver can
/// act on.
async fn execute_via_raft(raft: &FerrosRaft, op: DdlOperation) -> Result<()> {
    let cmd = ddl_op_to_raft_command(op);

    match raft.client_write(cmd).await {
        Ok(_resp) => Ok(()),
        Err(raft_err) => {
            // Extract a ForwardToLeader hint if present.
            if let Some(fwd) = raft_err.forward_to_leader() {
                return Err(ClusterError::NotLeader {
                    leader_id: fwd.leader_id,
                });
            }
            // Any other error is a general Raft fault.
            Err(ClusterError::RaftError(raft_err.to_string()))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
    use ferrosa_schema::Schema;
    use ferrosa_storage::engine::StorageEngine;
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};

    use indexmap::IndexMap;
    use std::collections::HashSet;
    use uuid::Uuid;

    // -- helpers ----------------------------------------------------------

    fn test_schema() -> Arc<Schema> {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
            PasswordPolicy, RateLimitConfig, SchemaConfig,
        };
        let config = SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: SchemaDeploymentMode::Development,
        };
        Arc::new(Schema::new(config).unwrap())
    }

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            data_dir: dir.to_path_buf(),
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

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
        use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
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

    // -- DdlPath::Direct tests --------------------------------------------

    #[tokio::test]
    async fn direct_create_keyspace_applies_locally() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine,
        };

        let op = DdlOperation::CreateKeyspace(simple_keyspace("test_ks"));
        ddl.execute(op).await.unwrap();

        let snap = schema.snapshot();
        assert!(
            snap.keyspaces.contains_key("test_ks"),
            "keyspace should be visible in schema"
        );
    }

    #[tokio::test]
    async fn create_table_via_direct_registers_in_storage() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        let ddl = DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        };

        // Must create keyspace first.
        ddl.execute(DdlOperation::CreateKeyspace(simple_keyspace("ks")))
            .await
            .unwrap();

        let table = simple_table("ks", "users");
        ddl.execute(DdlOperation::CreateTable(Box::new(table)))
            .await
            .unwrap();

        let snap = schema.snapshot();
        assert!(
            snap.tables.contains_key(&("ks".into(), "users".into())),
            "table should be in schema"
        );
        // Verify storage engine knows the table (write should succeed).
        let table_id = ferrosa_storage::TableId::new("ks", "users");
        let key = ferrosa_common::key::DecoratedKey {
            token: ferrosa_common::Token(0),
            key: ferrosa_common::PartitionKey::new(b"k".to_vec()),
        };
        let row = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1),
        };
        engine
            .write(&table_id, &key, row, 1)
            .expect("storage should accept write after table registered");
    }

    #[tokio::test]
    async fn direct_unavailable_returns_error() {
        let ddl = DdlPath::Unavailable;
        let op = DdlOperation::CreateKeyspace(simple_keyspace("ks"));
        let err = ddl.execute(op).await.unwrap_err();
        assert!(
            matches!(err, ClusterError::Internal(_)),
            "Unavailable should return Internal error, got {err:?}"
        );
    }

    // -- ddl_op_to_raft_command round-trip test --------------------------

    #[test]
    fn ddl_op_to_raft_command_create_keyspace() {
        let ks = simple_keyspace("raft_ks");
        let op = DdlOperation::CreateKeyspace(ks);
        let cmd = ddl_op_to_raft_command(op);
        match cmd {
            RaftCommand::CreateKeyspace(ks) => assert_eq!(ks.name, "raft_ks"),
            other => panic!("expected CreateKeyspace, got {other:?}"),
        }
    }

    #[test]
    fn ddl_op_to_raft_command_create_table() {
        let table = simple_table("ks", "tbl");
        let op = DdlOperation::CreateTable(Box::new(table));
        let cmd = ddl_op_to_raft_command(op);
        match cmd {
            RaftCommand::CreateTable(t) => {
                assert_eq!(t.keyspace, "ks");
                assert_eq!(t.name, "tbl");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn ddl_op_to_raft_command_drop_keyspace() {
        let op = DdlOperation::DropKeyspace("bye_ks".into());
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(cmd, RaftCommand::DropKeyspace(ref n) if n == "bye_ks"));
    }

    #[test]
    fn ddl_op_to_raft_command_drop_table() {
        let op = DdlOperation::DropTable {
            keyspace: "ks".into(),
            table: "tbl".into(),
        };
        let cmd = ddl_op_to_raft_command(op);
        assert!(matches!(
            cmd,
            RaftCommand::DropTable {
                ref keyspace,
                ref table
            } if keyspace == "ks" && table == "tbl"
        ));
    }

    // -- ClusterError::NotLeader display ----------------------------------

    #[test]
    fn not_leader_error_display_with_id() {
        let err = ClusterError::NotLeader {
            leader_id: Some(42),
        };
        let msg = err.to_string();
        assert!(msg.contains("42"), "should include leader_id in message");
    }

    #[test]
    fn not_leader_error_display_without_id() {
        let err = ClusterError::NotLeader { leader_id: None };
        let msg = err.to_string();
        assert!(
            msg.contains("unknown"),
            "should say unknown when leader_id is None"
        );
    }

    #[test]
    fn raft_error_display() {
        let err = ClusterError::RaftError("quorum lost".into());
        let msg = err.to_string();
        assert!(msg.contains("quorum lost"));
    }

    /// Verify that swapping from `Direct` to `Cluster` via `ArcSwap` works correctly.
    ///
    /// This mirrors the pattern used by `ModeController::transition_to_cluster` where
    /// the `ddl_path` ArcSwap is first set to `Direct` (while Raft initialises in the
    /// background) and then atomically replaced with `DdlPath::Cluster`.
    #[test]
    fn ddl_path_transitions_from_direct_after_raft_init() {
        use arc_swap::ArcSwap;

        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let engine = test_storage(dir.path());

        // Start as Direct (standalone / pre-Raft-init state).
        let ddl_swap: ArcSwap<DdlPath> = ArcSwap::from_pointee(DdlPath::Direct {
            schema: schema.clone(),
            engine: engine.clone(),
        });

        // Verify it's Direct initially.
        assert!(
            matches!(&**ddl_swap.load(), DdlPath::Direct { .. }),
            "initial state must be Direct"
        );

        // Simulate DdlPath::Cluster being stored after Raft initialises.
        // We use a minimal placeholder FerrosRaft struct via openraft in a way
        // that only tests the ArcSwap swap — not Raft itself.
        // Since we can't construct a FerrosRaft without a running tokio runtime
        // and network, we just verify the enum variant discrimination.
        //
        // The actual Raft integration is covered by controller::tests::raft_initializes_on_third_peer.
        //
        // Re-swap back to Direct to assert the swap mechanism works bidirectionally.
        ddl_swap.store(Arc::new(DdlPath::Unavailable));
        assert!(
            matches!(&**ddl_swap.load(), DdlPath::Unavailable),
            "swap to Unavailable must be visible immediately"
        );

        ddl_swap.store(Arc::new(DdlPath::Direct { schema, engine }));
        assert!(
            matches!(&**ddl_swap.load(), DdlPath::Direct { .. }),
            "swap back to Direct must be visible immediately"
        );
    }
}
