//! Loads system table data from the storage engine at cold start.
//!
//! [`SystemTableLoader`] reads `system_schema.*` and `system_auth.*` tables
//! to reconstruct partial schema state. The caller validates this against
//! Raft state (Raft wins on conflict).

use std::sync::Arc;

use ferrosa_common::Error as FerrosaError;
use ferrosa_schema::system::persistence::PERMISSIONS_COL_PERMISSIONS;
use ferrosa_schema::{GrantEntry, Permission, Resource, Schema};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

/// Loads system table data from the storage engine at cold start.
///
/// Reads `system_schema.*` and `system_auth.*` tables to reconstruct
/// a partial schema state. The caller validates this against Raft state.
pub struct SystemTableLoader {
    engine: Arc<StorageEngine>,
}

impl SystemTableLoader {
    /// Create a new loader backed by the given storage engine.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Load keyspace names from `system_schema.keyspaces`.
    ///
    /// Returns the partition key (keyspace name) for each row.
    pub fn load_keyspace_names(&self) -> ferrosa_common::Result<Vec<String>> {
        let tid = TableId::new("system_schema", "keyspaces");
        let partitions = self.engine.read_range(&tid, None, None, 10_000)?;
        let names: Vec<String> = partitions
            .iter()
            .filter_map(|p| String::from_utf8(p.key.key.as_bytes().to_vec()).ok())
            .collect();
        Ok(names)
    }

    /// Load persisted grants from `system_auth.role_permissions`.
    pub fn load_role_permissions(&self) -> ferrosa_common::Result<Vec<GrantEntry>> {
        let tid = TableId::new("system_auth", "role_permissions");
        let partitions = self.engine.read_range(&tid, None, None, 10_000)?;
        let mut grants = Vec::new();

        for partition in partitions {
            if !partition.deletion.is_live() {
                continue;
            }
            let role = String::from_utf8(partition.key.key.as_bytes().to_vec()).map_err(|e| {
                FerrosaError::InvalidData(format!("invalid role_permissions role key utf8: {e}"))
            })?;

            for row in partition.rows {
                if !row.deletion.is_live() {
                    continue;
                }
                let resource_text = String::from_utf8(row.clustering.clone()).map_err(|e| {
                    FerrosaError::InvalidData(format!(
                        "invalid role_permissions resource utf8: {e}"
                    ))
                })?;
                let Some((_, cell)) = row
                    .cells
                    .iter()
                    .find(|(col, _)| *col == PERMISSIONS_COL_PERMISSIONS)
                else {
                    continue;
                };
                let Some(bytes) = cell.value.as_deref() else {
                    continue;
                };
                let permission_names: Vec<String> = serde_json::from_slice(bytes).map_err(|e| {
                    FerrosaError::InvalidData(format!(
                        "invalid role_permissions permissions json: {e}"
                    ))
                })?;
                let permissions = permission_names
                    .iter()
                    .map(|name| decode_permission(name))
                    .collect::<ferrosa_common::Result<std::collections::HashSet<_>>>()?;

                grants.push(GrantEntry {
                    role: role.clone(),
                    resource: decode_resource(&resource_text)?,
                    permissions,
                });
            }
        }

        Ok(grants)
    }

    /// Replay persisted `system_auth.role_permissions` rows into the live schema.
    pub fn replay_role_permissions_into_schema(
        &self,
        schema: &Schema,
    ) -> ferrosa_common::Result<usize> {
        let grants = self.load_role_permissions()?;
        let count = grants.len();
        for grant in grants {
            schema.grant_internal(grant).map_err(|e| {
                FerrosaError::InvalidData(format!("failed to replay role permission: {e}"))
            })?;
        }
        Ok(count)
    }
}

fn decode_permission(name: &str) -> ferrosa_common::Result<Permission> {
    match name.to_ascii_uppercase().as_str() {
        "CREATE" => Ok(Permission::Create),
        "ALTER" => Ok(Permission::Alter),
        "DROP" => Ok(Permission::Drop),
        "SELECT" => Ok(Permission::Select),
        "MODIFY" => Ok(Permission::Modify),
        "AUTHORIZE" => Ok(Permission::Authorize),
        "DESCRIBE" => Ok(Permission::Describe),
        "EXECUTE" => Ok(Permission::Execute),
        "BACKUP" => Ok(Permission::Backup),
        other => Err(FerrosaError::InvalidData(format!(
            "unknown role permission {other}"
        ))),
    }
}

fn decode_resource(text: &str) -> ferrosa_common::Result<Resource> {
    if text == "ALL KEYSPACES" {
        return Ok(Resource::AllKeyspaces);
    }
    if text == "ALL ROLES" {
        return Ok(Resource::AllRoles);
    }
    if let Some(name) = text.strip_prefix("keyspace ") {
        return Ok(Resource::Keyspace(name.to_string()));
    }
    if let Some(name) = text.strip_prefix("role ") {
        return Ok(Resource::Role(name.to_string()));
    }
    if let Some(rest) = text.strip_prefix("table ") {
        let (keyspace, table) = rest
            .split_once('.')
            .ok_or_else(|| FerrosaError::InvalidData(format!("invalid table resource {text:?}")))?;
        return Ok(Resource::Table(keyspace.to_string(), table.to_string()));
    }
    if let Some(keyspace) = text.strip_prefix("ALL FUNCTIONS IN KEYSPACE ") {
        return Ok(Resource::AllFunctions(keyspace.to_string()));
    }
    if let Some(rest) = text.strip_prefix("function ") {
        let (qualified_name, args_text) = rest
            .strip_suffix(')')
            .and_then(|s| s.split_once('('))
            .ok_or_else(|| {
                FerrosaError::InvalidData(format!("invalid function resource {text:?}"))
            })?;
        let (keyspace, name) = qualified_name.split_once('.').ok_or_else(|| {
            FerrosaError::InvalidData(format!("invalid function resource {text:?}"))
        })?;
        let args = if args_text.trim().is_empty() {
            Vec::new()
        } else {
            args_text
                .split(',')
                .map(|arg| arg.trim().to_string())
                .collect()
        };
        return Ok(Resource::Function(
            keyspace.to_string(),
            name.to_string(),
            args,
        ));
    }

    Err(FerrosaError::InvalidData(format!(
        "unknown role permission resource {text:?}"
    )))
}

/// Validate keyspace names from SSTables against Raft state.
///
/// Returns (validated_keyspaces, divergence_messages). Raft is authoritative:
/// - Keyspaces in Raft but not SSTables: included (will be re-written).
/// - Keyspaces in SSTables but not Raft: excluded (stale data).
pub fn validate_keyspaces_against_raft(
    sstable_keyspaces: &[String],
    raft_keyspaces: &[String],
) -> (Vec<String>, Vec<String>) {
    let raft_set: std::collections::HashSet<&String> = raft_keyspaces.iter().collect();
    let sstable_set: std::collections::HashSet<&String> = sstable_keyspaces.iter().collect();

    let mut divergences = Vec::new();

    // Keyspaces in SSTable but not in Raft (stale).
    for ks in &sstable_set {
        if !raft_set.contains(*ks) {
            divergences.push(format!(
                "keyspace '{}' found in SSTables but not in Raft state (stale, ignoring)",
                ks
            ));
        }
    }

    // Keyspaces in Raft but not in SSTable (need re-write).
    for ks in &raft_set {
        if !sstable_set.contains(*ks) {
            divergences.push(format!(
                "keyspace '{}' in Raft state but not in SSTables (will re-persist)",
                ks
            ));
        }
    }

    // Raft wins: return Raft keyspaces.
    let validated = raft_keyspaces.to_vec();
    (validated, divergences)
}

/// Report from bootstrapping system tables.
#[derive(Debug)]
pub struct BootstrapReport {
    /// Keyspaces validated against Raft.
    pub validated_keyspaces: Vec<String>,
    /// Divergence messages (logged as warnings).
    pub divergences: Vec<String>,
}

/// Bootstrap system tables: register schemas, load from SSTables, validate
/// against Raft state.
///
/// Returns a report describing what was found and any divergences. The caller
/// should log divergences as warnings and re-persist any keyspaces that exist
/// in Raft but not in SSTables.
pub fn bootstrap_system_tables(
    engine: Arc<StorageEngine>,
    raft_keyspaces: &[String],
) -> ferrosa_common::Result<BootstrapReport> {
    // Step 1: Register system table schemas (idempotent).
    engine.register_system_tables()?;

    // Step 2: Load existing keyspace names from SSTables.
    let loader = SystemTableLoader::new(Arc::clone(&engine));
    let sstable_keyspaces = loader.load_keyspace_names()?;

    // Step 3: Validate against Raft.
    let (validated, divergences) =
        validate_keyspaces_against_raft(&sstable_keyspaces, raft_keyspaces);

    // Step 4: Log divergences.
    for msg in &divergences {
        tracing::warn!("system-table-bootstrap: {msg}");
    }

    Ok(BootstrapReport {
        validated_keyspaces: validated,
        divergences,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use ferrosa_schema::{
        AuthContext, AuthMethod, EnvSecretsProvider, GrantEntry, LogAuditSink, PasswordHasher,
        PasswordPolicy, Permission, RateLimitConfig, Resource, RoleMetadata, Schema, SchemaConfig,
    };
    use ferrosa_storage::engine::StorageEngineConfig;
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine};

    use crate::system_table_writer::SystemTableWriter;

    fn test_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
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

    fn test_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(EnvSecretsProvider),
            mode: ferrosa_schema::DeploymentMode::Development,
        })
        .unwrap()
    }

    #[test]
    fn validate_raft_wins_on_conflict() {
        let sstable_keyspaces = vec!["ks1".to_string(), "ks2".to_string()];
        let raft_keyspaces = vec!["ks1".to_string(), "ks3".to_string()];

        let (validated, divergences) =
            validate_keyspaces_against_raft(&sstable_keyspaces, &raft_keyspaces);

        assert_eq!(validated.len(), 2);
        assert!(validated.contains(&"ks1".to_string()));
        assert!(validated.contains(&"ks3".to_string()));
        assert!(!validated.contains(&"ks2".to_string()));

        // Should report divergences.
        assert!(!divergences.is_empty());
    }

    #[test]
    fn load_role_permissions_decodes_persisted_grants() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let writer = SystemTableWriter::new(Arc::clone(&engine));
        writer
            .apply(
                ferrosa_schema::system::persistence::SystemTableMutation::GrantUpdated(
                    GrantEntry {
                        role: "ferrosa_user".to_string(),
                        resource: Resource::Table(
                            "agent_memory".to_string(),
                            "entity_store".to_string(),
                        ),
                        permissions: [Permission::Modify, Permission::Select]
                            .into_iter()
                            .collect(),
                    },
                ),
            )
            .unwrap();

        let grants = SystemTableLoader::new(engine)
            .load_role_permissions()
            .unwrap();

        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].role, "ferrosa_user");
        assert_eq!(
            grants[0].resource,
            Resource::Table("agent_memory".to_string(), "entity_store".to_string())
        );
        assert!(grants[0].permissions.contains(&Permission::Modify));
        assert!(grants[0].permissions.contains(&Permission::Select));
    }

    #[test]
    fn replay_role_permissions_restores_write_permission() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let writer = SystemTableWriter::new(Arc::clone(&engine));
        writer
            .apply(
                ferrosa_schema::system::persistence::SystemTableMutation::GrantUpdated(
                    GrantEntry {
                        role: "ferrosa_user".to_string(),
                        resource: Resource::Keyspace("agent_memory".to_string()),
                        permissions: [Permission::Modify].into_iter().collect(),
                    },
                ),
            )
            .unwrap();

        let schema = test_schema();
        schema
            .create_role_internal(RoleMetadata {
                name: "ferrosa_user".to_string(),
                is_superuser: false,
                can_login: true,
                salted_hash: None,
                member_of: Default::default(),
            })
            .unwrap();

        let count = SystemTableLoader::new(engine)
            .replay_role_permissions_into_schema(&schema)
            .unwrap();

        assert_eq!(count, 1);
        schema
            .check_permission(
                &AuthContext {
                    role: "ferrosa_user".to_string(),
                    is_superuser: false,
                    must_change_password: false,
                },
                Permission::Modify,
                &Resource::Table("agent_memory".to_string(), "entity_store".to_string()),
            )
            .expect("persisted MODIFY grant must be effective after replay");
    }
}
