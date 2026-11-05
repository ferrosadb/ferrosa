//! Loads system table data from the storage engine at cold start.
//!
//! [`SystemTableLoader`] reads `system_schema.*` and `system_auth.*` tables
//! to reconstruct partial schema state. The caller validates this against
//! Raft state (Raft wins on conflict).
//! Correctness: persisted system rows are decoded fail-loud and replayed before
//! callers create missing bootstrap state.
//! Last revised: 2026-09-16.
//! Last changed: restored authoritative role hashes from system_auth.roles.

use std::{collections::HashMap, sync::Arc};

use ferrosa_common::Error as FerrosaError;
use ferrosa_schema::auth::bootstrap::{
    SeedConfig, SEED_ADMIN_PASSWORD, SEED_ADMIN_USER, SEED_APP_PASSWORD, SEED_APP_READER_USER,
    SEED_GRAPH_ENGINE_USER,
};
use ferrosa_schema::system::persistence::{
    SystemTableMutation, PERMISSIONS_COL_PERMISSIONS, ROLES_COL_CAN_LOGIN, ROLES_COL_IS_SUPERUSER,
    ROLES_COL_SALTED_HASH, ROLES_COL_SCRAM,
};
use ferrosa_schema::{
    DeploymentMode, GrantEntry, PasswordHasher, Permission, Resource, RoleMetadata, Schema,
    UserFunctionMetadata, UserTypeMetadata,
};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

/// What [`SystemTableLoader::repair_default_credential_rows`] changed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CredentialRepairReport {
    /// `ferrosa_admin`'s default-password row was replaced with the
    /// configured `FERROSA_SEED_ADMIN_PASSWORD`.
    pub admin_reseeded: bool,
    /// Service roles whose default-password row had LOGIN disabled.
    pub logins_disabled: Vec<String>,
}

/// Whether `role`'s stored hash verifies against `password`.
///
/// An unverifiable hash is not a default-password match: it is logged at
/// ERROR and left for the normal authentication path to reject.
fn role_verifies(role: &RoleMetadata, password: &str) -> bool {
    let Some(hash) = role.salted_hash.as_deref() else {
        return false;
    };
    match PasswordHasher::verify_password_any(password, hash) {
        Ok(matches) => matches,
        Err(e) => {
            tracing::error!(%e, role = %role.name, "stored role hash could not be verified");
            false
        }
    }
}

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

    /// Visit every partition of `tid`, streamed one at a time by the engine's
    /// token-range walk.
    ///
    /// The walk covers the merged memtable + SSTable view, so a table of any size
    /// is read in full while at most one partition is resident. This is how the
    /// loaders read: the capped `read_range(.., N)` it replaces silently dropped
    /// the tail of a large system table, and a dropped role/grant/keyspace is
    /// data loss on cold start.
    fn walk_all_partitions(
        &self,
        tid: &TableId,
        mut visit: impl FnMut(&ferrosa_sstable::types::Partition) -> ferrosa_common::Result<()>,
    ) -> ferrosa_common::Result<()> {
        self.engine
            .walk_token_range(tid, i64::MIN, i64::MAX, |p| visit(p))
    }

    /// Load keyspace names from `system_schema.keyspaces`.
    ///
    /// Returns the partition key (keyspace name) for each row.
    pub fn load_keyspace_names(&self) -> ferrosa_common::Result<Vec<String>> {
        let tid = TableId::new("system_schema", "keyspaces");
        let mut names: Vec<String> = Vec::new();
        self.walk_all_partitions(&tid, |p| {
            if let Ok(name) = String::from_utf8(p.key.key.as_bytes().to_vec()) {
                names.push(name);
            }
            Ok(())
        })?;
        Ok(names)
    }

    /// Load authoritative role records from system_auth.roles.
    pub fn load_roles(&self) -> ferrosa_common::Result<Vec<RoleMetadata>> {
        let tid = TableId::new("system_auth", "roles");
        let mut roles = Vec::new();

        self.walk_all_partitions(&tid, |partition| {
            let name = String::from_utf8(partition.key.key.as_bytes().to_vec()).map_err(|e| {
                FerrosaError::InvalidData(format!("invalid roles role key utf8: {e}"))
            })?;
            let Some(row) = partition.rows.iter().find(|row| {
                row.cells.iter().any(|(_, cell)| {
                    cell.timestamp
                        > partition
                            .deletion
                            .marked_for_delete_at
                            .max(row.deletion.marked_for_delete_at)
                })
            }) else {
                return Ok(());
            };
            // Lookups by column id over a BORROW of the row's cells. The previous
            // shape deep-cloned every `CellValue` into a second map; because the
            // walker lends `&Partition` there is nothing to move out, and nothing
            // needs to be — the map only needs to point at the row we already hold.
            // `row.cells` is a Vec, so `collect` into a HashMap keeps the LAST
            // occurrence of a duplicated column id, which is the behaviour the
            // cloned map had; hence the reference map preserves it exactly.
            let cells: HashMap<_, _> = row.cells.iter().map(|(col, cell)| (*col, cell)).collect();
            let bool_cell = |column, field: &str| -> ferrosa_common::Result<bool> {
                let bytes = cells
                    .get(&column)
                    .and_then(|cell| cell.value.as_deref())
                    .ok_or_else(|| {
                        FerrosaError::InvalidData(format!(
                            "role {name} is missing required {field}"
                        ))
                    })?;
                match bytes {
                    [0] => Ok(false),
                    [1] => Ok(true),
                    _ => Err(FerrosaError::InvalidData(format!(
                        "role {name} has invalid {field} encoding"
                    ))),
                }
            };
            let is_superuser = bool_cell(ROLES_COL_IS_SUPERUSER, "is_superuser")?;
            let can_login = bool_cell(ROLES_COL_CAN_LOGIN, "can_login")?;
            let salted_hash = cells
                .get(&ROLES_COL_SALTED_HASH)
                .and_then(|cell| cell.value.as_deref())
                .map(|bytes| {
                    std::str::from_utf8(bytes).map(str::to_owned).map_err(|e| {
                        FerrosaError::InvalidData(format!(
                            "role {name} has invalid salted_hash utf8: {e}"
                        ))
                    })
                })
                .transpose()?;
            let scram = cells
                .get(&ROLES_COL_SCRAM)
                .and_then(|cell| cell.value.as_deref())
                .map(|bytes| {
                    serde_json::from_slice(bytes).map_err(|e| {
                        FerrosaError::InvalidData(format!(
                            "role {name} has invalid SCRAM verifier: {e}"
                        ))
                    })
                })
                .transpose()?;

            roles.push(RoleMetadata {
                name,
                is_superuser,
                can_login,
                salted_hash,
                member_of: Default::default(),
                scram,
            });
            Ok(())
        })?;

        Ok(roles)
    }

    /// Replace fresh-process bootstrap roles with persisted role records.
    pub fn replay_roles_into_schema(&self, schema: &Schema) -> ferrosa_common::Result<usize> {
        let roles = self.load_roles()?;
        let count = roles.len();
        for role in roles {
            schema.restore_role_internal(role).map_err(|e| {
                FerrosaError::InvalidData(format!("failed to replay persisted role: {e}"))
            })?;
        }
        Ok(count)
    }

    /// Repair seed-role rows that a pre-fix cluster persisted with a client's
    /// well-known password (t_d0e5e6e5). Run after role replay AND seeding.
    ///
    /// Before the fix, `CREATE ROLE IF NOT EXISTS ferrosa_admin WITH PASSWORD =
    /// '<default>'` reaching Raft apply wrote the default hash to
    /// system_auth.roles on every node while memory kept the seeded one. The
    /// replayed row then wins at restart. Two repairs, both persisted (row
    /// first, then memory) and both logged:
    ///
    /// - `ferrosa_admin`: when the operator configured
    ///   `FERROSA_SEED_ADMIN_PASSWORD` and the stored admin verifies against
    ///   the built-in default but not against that configured password, the
    ///   configured password is hashed and re-persisted. This only ever
    ///   replaces the well-known credential with the operator's own one. With
    ///   no configured password nothing is changed: the original was random
    ///   and is unrecoverable, so the production gate must still refuse the
    ///   default, and an ERROR names the remedy.
    /// - `graph_engine` / `ferrosa_user` (production mode only): a login role
    ///   whose stored hash verifies against the well-known service password
    ///   has LOGIN disabled. Production never seeds that password, so the row
    ///   came from a client literal; the random seeded password it overwrote is
    ///   unrecoverable, and a well-known credential must not authenticate. An
    ///   operator restores it with `ALTER ROLE <name> WITH PASSWORD = '...' AND
    ///   LOGIN = true`.
    pub fn repair_default_credential_rows(
        &self,
        schema: &Schema,
        config: &SeedConfig,
    ) -> ferrosa_common::Result<CredentialRepairReport> {
        let mut report = CredentialRepairReport {
            admin_reseeded: self.repair_default_admin_row(schema, config)?,
            logins_disabled: Vec::new(),
        };
        if config.mode == DeploymentMode::Production {
            for name in [SEED_GRAPH_ENGINE_USER, SEED_APP_READER_USER] {
                if self.disable_default_service_login(schema, name)? {
                    report.logins_disabled.push(name.to_string());
                }
            }
        }
        Ok(report)
    }

    fn repair_default_admin_row(
        &self,
        schema: &Schema,
        config: &SeedConfig,
    ) -> ferrosa_common::Result<bool> {
        let Some(admin) = schema.snapshot().roles.get(SEED_ADMIN_USER).cloned() else {
            return Ok(false);
        };
        if !role_verifies(&admin, SEED_ADMIN_PASSWORD) {
            return Ok(false);
        }
        let configured = config
            .admin_password
            .as_deref()
            .filter(|pw| *pw != SEED_ADMIN_PASSWORD);
        let Some(configured) = configured else {
            if config.mode == DeploymentMode::Production {
                tracing::error!(
                    role = SEED_ADMIN_USER,
                    "ferrosa_admin's stored credential is the well-known default and \
                     FERROSA_SEED_ADMIN_PASSWORD is not set, so it cannot be repaired; production \
                     startup will refuse it. Set FERROSA_SEED_ADMIN_PASSWORD to the intended admin \
                     password and restart to re-persist it (t_d0e5e6e5)"
                );
            }
            return Ok(false);
        };
        let mut repaired = admin;
        repaired.salted_hash = Some(schema.password_hasher().hash_password(configured).map_err(
            |e| FerrosaError::InvalidData(format!("hash configured admin password: {e}")),
        )?);
        repaired.scram = Some(ferrosa_schema::auth::scram::derive_with_random_salt(
            configured,
        ));
        self.persist_and_restore(schema, repaired)?;
        tracing::warn!(
            role = SEED_ADMIN_USER,
            "repaired ferrosa_admin: the persisted system_auth.roles row held the well-known \
             default password while FERROSA_SEED_ADMIN_PASSWORD is configured; re-persisted the \
             configured password (t_d0e5e6e5)"
        );
        Ok(true)
    }

    fn disable_default_service_login(
        &self,
        schema: &Schema,
        name: &str,
    ) -> ferrosa_common::Result<bool> {
        let Some(role) = schema.snapshot().roles.get(name).cloned() else {
            return Ok(false);
        };
        if !role.can_login || !role_verifies(&role, SEED_APP_PASSWORD) {
            return Ok(false);
        }
        let mut locked = role;
        locked.can_login = false;
        self.persist_and_restore(schema, locked)?;
        tracing::error!(
            role = name,
            "disabled LOGIN for a production service role whose persisted credential is the \
             well-known default password (t_d0e5e6e5). Restore it with ALTER ROLE <role> WITH \
             PASSWORD = '<new password>' AND LOGIN = true"
        );
        Ok(true)
    }

    /// Persist the row first so a failed write leaves memory unchanged and
    /// fails startup loudly instead of reporting a repair that a restart undoes.
    fn persist_and_restore(
        &self,
        schema: &Schema,
        role: RoleMetadata,
    ) -> ferrosa_common::Result<()> {
        let name = role.name.clone();
        crate::system_table_writer::SystemTableWriter::new(Arc::clone(&self.engine))
            .apply(SystemTableMutation::RoleCreated(role.clone()))
            .map_err(|e| FerrosaError::InvalidData(format!("persist repaired role {name}: {e}")))?;
        schema
            .restore_role_internal(role)
            .map_err(|e| FerrosaError::InvalidData(format!("restore repaired role {name}: {e}")))
    }

    /// Load persisted grants from `system_auth.role_permissions`.
    pub fn load_role_permissions(&self) -> ferrosa_common::Result<Vec<GrantEntry>> {
        let tid = TableId::new("system_auth", "role_permissions");
        let mut grants = Vec::new();

        self.walk_all_partitions(&tid, |partition| {
            let role = String::from_utf8(partition.key.key.as_bytes().to_vec()).map_err(|e| {
                FerrosaError::InvalidData(format!("invalid role_permissions role key utf8: {e}"))
            })?;

            for row in &partition.rows {
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
                // A newer grant can resurrect this permission cell above an
                // older row tombstone. Older/equal cells remain deleted.
                if cell.timestamp
                    <= partition
                        .deletion
                        .marked_for_delete_at
                        .max(row.deletion.marked_for_delete_at)
                {
                    continue;
                }
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
            Ok(())
        })?;

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

    /// Load persisted user-defined types from `system_schema.types`.
    ///
    /// Reconstructs each [`UserTypeMetadata`] from the storage-decoded rows
    /// (the engine drops tombstones and malformed rows). Field order is
    /// preserved exactly as persisted.
    pub fn load_user_types(&self) -> ferrosa_common::Result<Vec<UserTypeMetadata>> {
        let rows = self.engine.read_persisted_types()?;
        Ok(rows
            .into_iter()
            .map(|r| UserTypeMetadata {
                keyspace: r.keyspace_name,
                name: r.type_name,
                fields: r.fields,
            })
            .collect())
    }

    /// Replay persisted `system_schema.types` rows into the live schema Registry.
    ///
    /// Skips types whose keyspace is already gone or that already exist (the
    /// Registry's `create_type_internal` rejects duplicates) so a partially
    /// hydrated Registry does not abort the whole replay. Returns the number of
    /// types successfully (re-)registered.
    pub fn replay_types_into_schema(&self, schema: &Schema) -> ferrosa_common::Result<usize> {
        let types = self.load_user_types()?;
        let mut restored = 0usize;
        for udt in types {
            match schema.create_type_internal(&udt) {
                Ok(()) => restored += 1,
                Err(e) => {
                    tracing::warn!(
                        keyspace = %udt.keyspace,
                        name = %udt.name,
                        %e,
                        "skipping system_schema.types replay row"
                    );
                }
            }
        }
        Ok(restored)
    }

    /// Load persisted user-defined functions from `system_schema.functions`.
    ///
    /// Reconstructs each [`UserFunctionMetadata`] from the storage-decoded rows
    /// (the engine drops tombstones and malformed rows). Overloads are distinct
    /// rows, so the returned vector preserves each `(name, arg_types)` pair.
    pub fn load_user_functions(&self) -> ferrosa_common::Result<Vec<UserFunctionMetadata>> {
        let rows = self.engine.read_persisted_functions()?;
        Ok(rows
            .into_iter()
            .map(|r| UserFunctionMetadata {
                keyspace: r.keyspace_name,
                name: r.function_name,
                arg_names: r.arg_names,
                arg_types: r.arg_types,
                return_type: r.return_type,
                called_on_null: r.called_on_null,
                language: r.language,
                body: r.body,
            })
            .collect())
    }

    /// Replay persisted `system_schema.functions` rows into the live schema
    /// Registry.
    ///
    /// Skips functions whose keyspace is already gone or that already exist (the
    /// Registry's `create_function_internal` rejects duplicates) so a partially
    /// hydrated Registry does not abort the whole replay. Returns the number of
    /// functions successfully (re-)registered.
    pub fn replay_functions_into_schema(&self, schema: &Schema) -> ferrosa_common::Result<usize> {
        let functions = self.load_user_functions()?;
        let mut restored = 0usize;
        for func in functions {
            match schema.create_function_internal(&func) {
                Ok(()) => restored += 1,
                Err(e) => {
                    tracing::warn!(
                        keyspace = %func.keyspace,
                        name = %func.name,
                        %e,
                        "skipping system_schema.functions replay row"
                    );
                }
            }
        }
        Ok(restored)
    }

    /// Replay persisted `system_schema.indexes` rows into the live schema
    /// Registry's index set.
    ///
    /// `load_local_schema` (schema.json) restores table schemas with NO
    /// indexes, and the storage-side `reload_indexes_from_system_schema` only
    /// repopulates the *engine*. The CQL router resolves an `fts_match` column
    /// to its index via `SchemaSnapshot.indexes` (`resolve_fulltext_index_name`),
    /// so without this replay that lookup falls back to the bare column name
    /// after a restart and full-text search silently returns nothing. Returns
    /// the number of indexes (re-)registered.
    ///
    /// Filtered (partial) indexes are skipped here: their predicate lives in
    /// the persisted options and would need faithful decoding to register
    /// soundly; they already route via the engine, and only full-text routing
    /// depends on the schema registry.
    pub fn replay_indexes_into_schema(&self, schema: &Schema) -> ferrosa_common::Result<usize> {
        use ferrosa_schema::metadata::index::IndexMetadata;
        let rows = self.engine.read_persisted_indexes()?;
        let mut restored = 0usize;
        for row in rows {
            let Some(index_type) =
                ferrosa_schema::system::persistence::index_type_from_kind(&row.kind)
            else {
                tracing::warn!(
                    keyspace = %row.keyspace_name,
                    index = %row.index_name,
                    kind = %row.kind,
                    "skipping system_schema.indexes replay: unknown index kind"
                );
                continue;
            };
            if matches!(index_type, ferrosa_index::IndexType::Filtered) {
                continue;
            }
            let meta = IndexMetadata {
                keyspace: row.keyspace_name.clone(),
                table: row.table_name.clone(),
                name: row.index_name.clone(),
                index_type,
                target_columns: row.target.split(", ").map(|s| s.to_string()).collect(),
                filter_predicate: None,
                options: serde_json::from_str(&row.options).unwrap_or_default(),
            };
            match schema.create_index_internal(meta) {
                Ok(()) => restored += 1,
                Err(e) => tracing::warn!(
                    keyspace = %row.keyspace_name,
                    index = %row.index_name,
                    %e,
                    "skipping system_schema.indexes replay row"
                ),
            }
        }
        Ok(restored)
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
        AuthContext, AuthMethod, EnvSecretsProvider, GrantEntry, KeyspaceMetadata, LogAuditSink,
        PasswordHasher, PasswordPolicy, Permission, RateLimitConfig, ReplicationParams, Resource,
        RoleMetadata, Schema, SchemaConfig,
    };
    use ferrosa_storage::engine::StorageEngineConfig;
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngine};

    use crate::system_table_writer::SystemTableWriter;

    fn test_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        test_engine_with_flush_threshold(dir, 4096)
    }

    #[tokio::test]
    async fn canonical_role_update_survives_earlier_future_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let role = RoleMetadata {
            name: "clock_regressed_role".into(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: Default::default(),
            scram: None,
        };
        // A committed projection from before a wall-clock correction. Seed its
        // actual stored timestamps rather than changing the process clock.
        let (key, mut row, _) = ferrosa_schema::system::persistence::role_to_row(&role);
        let earlier_timestamp = i64::MAX - 1024;
        row.primary_key_liveness.timestamp = earlier_timestamp;
        for (_, cell) in &mut row.cells {
            cell.timestamp = earlier_timestamp;
        }
        engine
            .write(
                &TableId::new("system_auth", "roles"),
                &key,
                row,
                earlier_timestamp,
            )
            .unwrap();
        let loader = SystemTableLoader::new(Arc::clone(&engine));
        assert!(loader
            .load_roles()
            .unwrap()
            .iter()
            .any(|r| r.name == role.name && r.can_login));
        SystemTableWriter::new(Arc::clone(&engine))
            .apply(
                ferrosa_schema::system::persistence::SystemTableMutation::RoleCreated(
                    RoleMetadata {
                        can_login: false,
                        ..role.clone()
                    },
                ),
            )
            .unwrap();
        let restored = loader.load_roles().unwrap();
        assert!(
            restored.iter().any(|r| r.name == role.name && !r.can_login),
            "later ordered login disable must replace an earlier future-dated cell"
        );
        engine.flush(&TableId::new("system_auth", "roles")).unwrap();
        engine.force_commit_log_sync().unwrap();
        drop(loader);
        drop(engine);
        let reopened = reopen_test_engine(dir.path());
        assert!(
            SystemTableLoader::new(reopened)
                .load_roles()
                .unwrap()
                .iter()
                .any(|r| r.name == role.name && !r.can_login),
            "login disable must survive cold reopen"
        );
    }

    #[tokio::test]
    async fn canonical_role_drop_dominates_stored_future_rows_after_reopen() {
        use ferrosa_schema::system::persistence::{role_to_row, SystemTableMutation};
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let role = RoleMetadata {
            name: "drop_future_role".into(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: Default::default(),
            scram: None,
        };
        let future = i64::MAX - 2048;
        let (key, mut row, _) = role_to_row(&role);
        row.primary_key_liveness.timestamp = future;
        for (_, cell) in &mut row.cells {
            cell.timestamp = future;
        }
        let roles = TableId::new("system_auth", "roles");
        let permissions = TableId::new("system_auth", "role_permissions");
        engine.write(&roles, &key, row, future).unwrap();
        let mut writer = SystemTableWriter::new(Arc::clone(&engine));
        writer.set_permission_timestamp_for_test(future);
        writer
            .apply(SystemTableMutation::GrantUpdated(GrantEntry {
                role: role.name.clone(),
                resource: Resource::Keyspace("ks".into()),
                permissions: [Permission::Select].into_iter().collect(),
            }))
            .unwrap();
        writer
            .apply(SystemTableMutation::RoleDropped(role.name.clone()))
            .unwrap();
        let loader = SystemTableLoader::new(Arc::clone(&engine));
        let live_roles = loader.load_roles().unwrap();
        let live_grants = loader.load_role_permissions().unwrap();
        engine.flush(&roles).unwrap();
        engine.flush(&permissions).unwrap();
        engine.force_commit_log_sync().unwrap();
        drop(loader);
        drop(writer);
        drop(engine);
        let reopened = SystemTableLoader::new(reopen_test_engine(dir.path()));
        assert!(
            !live_roles.iter().any(|r| r.name == role.name)
                && !live_grants.iter().any(|g| g.role == role.name)
                && !reopened.load_roles().unwrap().iter().any(|r| r.name == role.name)
                && !reopened.load_role_permissions().unwrap().iter().any(|g| g.role == role.name),
            "committed role deletion must dominate stored future role/grants in memory and after cold reopen; roles={live_roles:?}, grants={live_grants:?}"
        );
    }

    #[tokio::test]
    async fn canonical_regrant_dominates_row_tombstone_hidden_by_partition_delete() {
        use ferrosa_schema::system::persistence::SystemTableMutation;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let mut writer = SystemTableWriter::new(Arc::clone(&engine));
        writer.set_permission_timestamp_for_test(1_000_000);
        let grant = GrantEntry {
            role: "hidden_tombstone_role".into(),
            resource: Resource::Keyspace("ks".into()),
            permissions: [Permission::Select].into_iter().collect(),
        };
        writer
            .apply(SystemTableMutation::GrantUpdated(grant.clone()))
            .unwrap();
        let tid = TableId::new("system_auth", "role_permissions");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            grant.role.as_bytes().to_vec(),
        ));
        // Legacy revocation followed by a backward-clock role deletion. Both
        // are real persisted records, not clock mocks or synthetic read results.
        engine
            .write(
                &tid,
                &key,
                Row {
                    clustering: grant.resource.to_string().into_bytes(),
                    cells: vec![],
                    deletion: DeletionTime::new(8_000_000, 8),
                    primary_key_liveness: LivenessInfo::NONE,
                },
                8_000_000,
            )
            .unwrap();
        engine
            .write(
                &tid,
                &key,
                Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: DeletionTime::new(3_000_000, 3),
                    primary_key_liveness: LivenessInfo::NONE,
                },
                3_000_000,
            )
            .unwrap();
        engine.flush(&tid).unwrap();
        engine.force_commit_log_sync().unwrap();
        assert!(
            engine
                .read_clustering_row(&tid, &key, grant.resource.to_string().as_bytes())
                .unwrap()
                .is_none(),
            "logical lookup hides the older PK even though its row tombstone is newer"
        );
        writer
            .apply(SystemTableMutation::GrantUpdated(GrantEntry {
                permissions: [Permission::Modify].into_iter().collect(),
                ..grant.clone()
            }))
            .unwrap();
        engine.flush(&tid).unwrap();
        engine.force_commit_log_sync().unwrap();
        drop(writer);
        drop(engine);
        let grants = SystemTableLoader::new(reopen_test_engine(dir.path()))
            .load_role_permissions()
            .unwrap();
        assert!(grants.iter().any(|g| g.role == grant.role
            && g.permissions == [Permission::Modify].into_iter().collect()),
            "later canonical regrant must dominate hidden stored row tombstone after reopen: {grants:?}");
    }

    #[tokio::test]
    async fn canonical_role_drop_crosses_hidden_prefix_in_bounded_metadata_pages() {
        use ferrosa_schema::system::persistence::SystemTableMutation;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine_with_flush_threshold(dir.path(), 1 << 20);
        engine.register_system_tables().unwrap();
        let tid = TableId::new("system_auth", "role_permissions");
        let role = "paged_role";
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            role.as_bytes().to_vec(),
        ));
        let mut writer = SystemTableWriter::new(Arc::clone(&engine));
        for i in 0..65 {
            writer.set_permission_timestamp_for_test(if i == 64 {
                i64::MAX - 2048
            } else {
                1_000_000
            });
            writer
                .apply(SystemTableMutation::GrantUpdated(GrantEntry {
                    role: role.into(),
                    resource: Resource::Keyspace(format!("ks{i:03}")),
                    permissions: [Permission::Select].into_iter().collect(),
                }))
                .unwrap();
        }
        // Compare against the real persisted clock: legacy nanosecond-shaped
        // inputs are normalized by StorageEngine before reaching the store.
        let late_resource = Resource::Keyspace("ks064".into()).to_string();
        let late = engine
            .read_clustering_row(&tid, &key, late_resource.as_bytes())
            .unwrap()
            .unwrap();
        let late_clock = late
            .rows
            .iter()
            .flat_map(|row| row.cells.iter().map(|(_, cell)| cell.timestamp))
            .max()
            .unwrap();
        assert!(
            late_clock
                > std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64
        );
        engine
            .write(
                &tid,
                &key,
                Row {
                    clustering: vec![],
                    cells: vec![],
                    deletion: DeletionTime::new(3_000_000, 3),
                    primary_key_liveness: LivenessInfo::NONE,
                },
                3_000_000,
            )
            .unwrap();
        assert!(
            engine
                .read_limited_rows(&tid, &key, 32)
                .unwrap()
                .unwrap()
                .rows
                .is_empty(),
            "ordinary logical page must still suppress the partition-deleted prefix"
        );
        let mut cursor = None;
        let mut rows = 0;
        let mut greatest = i64::MIN;
        loop {
            let page = engine
                .read_projection_rows(&tid, &key, cursor.as_deref(), 32)
                .unwrap()
                .unwrap();
            assert!(page.rows.len() <= 32);
            assert_eq!(page.deletion.marked_for_delete_at, 3_000_000);
            for row in &page.rows {
                rows += 1;
                for (_, cell) in &row.cells {
                    greatest = greatest.max(cell.timestamp);
                }
            }
            let Some(last) = page.rows.last() else { break };
            cursor = Some(last.clustering.clone());
        }
        assert_eq!(rows, 65, "physical cursor must cross both hidden pages");
        assert_eq!(greatest, late_clock, "late future grant must be inspected");
        assert!(engine.read_projection_rows(&tid, &key, None, 0).is_err());
        assert!(engine
            .read_projection_rows(&TableId::new("missing", "missing"), &key, None, 0)
            .is_err());
        writer
            .apply(SystemTableMutation::GrantUpdated(GrantEntry {
                role: "unrelated_role".into(),
                resource: Resource::Keyspace("safe".into()),
                permissions: [Permission::Select].into_iter().collect(),
            }))
            .unwrap();
        writer
            .apply(SystemTableMutation::RoleDropped(role.into()))
            .unwrap();
        engine.flush(&tid).unwrap();
        engine.force_commit_log_sync().unwrap();
        drop(writer);
        drop(engine);
        let grants = SystemTableLoader::new(reopen_test_engine(dir.path()))
            .load_role_permissions()
            .unwrap();
        assert!(!grants.iter().any(|g| g.role == role));
        assert!(grants.iter().any(|g| g.role == "unrelated_role"));
    }

    #[tokio::test]
    async fn canonical_regrant_survives_role_partition_tombstone() {
        use ferrosa_schema::system::persistence::SystemTableMutation;
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let mut writer = SystemTableWriter::new(Arc::clone(&engine));
        writer.set_permission_timestamp_for_test(1);
        let grant = GrantEntry {
            role: "recreated_role".into(),
            resource: Resource::Keyspace("ks".into()),
            permissions: [Permission::Select].into_iter().collect(),
        };
        writer
            .apply(SystemTableMutation::GrantUpdated(grant.clone()))
            .unwrap();
        writer
            .apply(SystemTableMutation::RoleDropped(grant.role.clone()))
            .unwrap();
        let loader = SystemTableLoader::new(Arc::clone(&engine));
        assert!(
            loader.load_role_permissions().unwrap().is_empty(),
            "drop must hide the old grant"
        );
        let tid = TableId::new("system_auth", "role_permissions");
        let key = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(
            grant.role.as_bytes().to_vec(),
        ));
        let deleted = engine.read_limited_rows(&tid, &key, 1).unwrap().unwrap();
        assert!(
            !deleted.deletion.is_live(),
            "actual partition deletion must remain"
        );
        assert!(
            engine
                .read_clustering_row(&tid, &key, grant.resource.to_string().as_bytes())
                .unwrap()
                .is_none(),
            "logical exact-row lookup omits deleted partition metadata"
        );
        writer
            .apply(SystemTableMutation::GrantUpdated(GrantEntry {
                permissions: [Permission::Modify].into_iter().collect(),
                ..grant.clone()
            }))
            .unwrap();
        assert!(
            loader
                .load_role_permissions()
                .unwrap()
                .iter()
                .any(|g| g.role == grant.role
                    && g.permissions == [Permission::Modify].into_iter().collect()),
            "a newer grant must survive an older role partition tombstone"
        );
        engine.flush(&tid).unwrap();
        engine.force_commit_log_sync().unwrap();
        drop(loader);
        drop(writer);
        drop(engine);
        let reopened = reopen_test_engine(dir.path());
        assert!(
            SystemTableLoader::new(reopened)
                .load_role_permissions()
                .unwrap()
                .iter()
                .any(|g| g.role == grant.role
                    && g.permissions == [Permission::Modify].into_iter().collect()),
            "regrant must survive cold reopen above the old partition deletion"
        );
    }

    /// A test engine whose memtable auto-flush threshold is a parameter, so a
    /// test can hold many small partitions resident without flush churn.
    fn test_engine_with_flush_threshold(
        dir: &std::path::Path,
        flush_threshold_bytes: u64,
    ) -> Arc<StorageEngine> {
        Arc::new(StorageEngine::new(test_storage_config(dir, flush_threshold_bytes), None).unwrap())
    }

    fn test_storage_config(
        dir: &std::path::Path,
        flush_threshold_bytes: u64,
    ) -> StorageEngineConfig {
        StorageEngineConfig {
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
            flush_threshold_bytes,
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

    fn reopen_test_engine(dir: &std::path::Path) -> Arc<StorageEngine> {
        let (engine, pending) = StorageEngine::open(test_storage_config(dir, 4096), None).unwrap();
        engine.register_system_tables().unwrap();
        engine.replay_mutations(pending).unwrap();
        Arc::new(engine)
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

    /// The loader must return EVERY persisted keyspace, not just the first
    /// 10_000. RED before the fix: `load_keyspace_names` read with a hard
    /// `read_range(.., 10_000)` limit, so keyspace #10_001 was silently dropped —
    /// no error, no truncated flag. That is data loss on cold start.
    #[test]
    fn load_keyspace_names_returns_every_persisted_keyspace_past_ten_thousand() {
        let dir = tempfile::tempdir().unwrap();
        // Keep every keyspace resident in the memtable: this test is about the
        // loader's cap, not flush/compaction churn.
        let engine = test_engine_with_flush_threshold(dir.path(), u64::MAX);
        engine.register_system_tables().unwrap();
        let writer = SystemTableWriter::new(Arc::clone(&engine));

        let total = 10_001usize; // one past the former 10_000 limit
        for i in 0..total {
            writer
                .apply(
                    ferrosa_schema::system::persistence::SystemTableMutation::KeyspaceCreated(
                        KeyspaceMetadata {
                            name: format!("ks_{i:05}"),
                            durable_writes: true,
                            replication: ReplicationParams {
                                strategy: "SimpleStrategy".to_string(),
                                options: Default::default(),
                            },
                        },
                    ),
                )
                .unwrap();
        }

        let names = SystemTableLoader::new(engine)
            .load_keyspace_names()
            .unwrap();

        assert_eq!(
            names.len(),
            total,
            "every persisted keyspace must be loaded; the tail must not be silently dropped"
        );
        assert!(
            names.iter().any(|n| n == "ks_10000"),
            "the keyspace past the former cap must be present"
        );
    }

    #[test]
    fn replay_roles_replaces_bootstrap_credentials_with_persisted_hash() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        let persisted_hash = PasswordHasher::Bcrypt { cost: 4 }
            .hash_password("rotated-password")
            .unwrap();
        let persisted_scram =
            ferrosa_schema::auth::scram::derive_with_random_salt("rotated-password");
        SystemTableWriter::new(Arc::clone(&engine))
            .apply(
                ferrosa_schema::system::persistence::SystemTableMutation::RoleCreated(
                    RoleMetadata {
                        name: "cassandra".to_string(),
                        is_superuser: true,
                        can_login: true,
                        salted_hash: Some(persisted_hash.clone()),
                        member_of: Default::default(),
                        scram: Some(persisted_scram.clone()),
                    },
                ),
            )
            .unwrap();

        let schema = test_schema();
        assert!(
            !schema.snapshot().roles.contains_key("cassandra"),
            "precondition: the legacy role is absent without an explicit secret"
        );

        let count = SystemTableLoader::new(engine)
            .replay_roles_into_schema(&schema)
            .unwrap();

        assert_eq!(count, 1);
        assert!(
            schema.authenticate("cassandra", "cassandra").is_err(),
            "restart replay must replace the built-in credential"
        );
        assert!(
            schema.authenticate("cassandra", "rotated-password").is_ok(),
            "the persisted rotated credential must authenticate after replay"
        );
        assert_eq!(
            schema
                .snapshot()
                .roles
                .get("cassandra")
                .and_then(|role| role.salted_hash.as_deref()),
            Some(persisted_hash.as_str())
        );
        assert_eq!(
            schema
                .snapshot()
                .roles
                .get("cassandra")
                .and_then(|role| role.scram.as_ref()),
            Some(&persisted_scram),
            "Postgres verifier must survive role replay"
        );
    }

    // -- t_d0e5e6e5: repair of default-password seed-role rows --------------

    const MANAGED_ADMIN_PASSWORD: &str = "managed-admin-secret-d0e5"; // pragma: allowlist secret

    fn fast_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(EnvSecretsProvider),
            mode: ferrosa_schema::DeploymentMode::Development,
        })
        .unwrap()
    }

    fn write_role_row(engine: &Arc<StorageEngine>, name: &str, password: &str, superuser: bool) {
        SystemTableWriter::new(Arc::clone(engine))
            .apply(SystemTableMutation::RoleCreated(RoleMetadata {
                name: name.to_string(),
                is_superuser: superuser,
                can_login: true,
                salted_hash: Some(
                    PasswordHasher::Bcrypt { cost: 4 }
                        .hash_password(password)
                        .unwrap(),
                ),
                member_of: Default::default(),
                scram: None,
            }))
            .unwrap();
    }

    fn persisted(engine: &Arc<StorageEngine>, name: &str) -> RoleMetadata {
        SystemTableLoader::new(Arc::clone(engine))
            .load_roles()
            .unwrap()
            .into_iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("{name} must be persisted"))
    }

    /// The node's startup sequence: replay system_auth.roles, seed, repair.
    fn restart(
        engine: &Arc<StorageEngine>,
        config: &SeedConfig,
    ) -> (Schema, CredentialRepairReport) {
        let schema = fast_schema();
        let loader = SystemTableLoader::new(Arc::clone(engine));
        loader.replay_roles_into_schema(&schema).unwrap();
        ferrosa_schema::auth::bootstrap::seed_default_roles_with_config(&schema, config).unwrap();
        let report = loader
            .repair_default_credential_rows(&schema, config)
            .unwrap();
        (schema, report)
    }

    /// Records every event's level and message so a test can assert that a
    /// repair is observable in the log.
    #[derive(Clone, Default)]
    struct CapturedLog(Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>);

    impl CapturedLog {
        fn contains(&self, level: tracing::Level, needle: &str) -> bool {
            self.0
                .lock()
                .unwrap()
                .iter()
                .any(|(l, m)| *l == level && m.contains(needle))
        }
    }

    impl tracing::Subscriber for CapturedLog {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Message(String);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut message = Message(String::new());
            event.record(&mut message);
            self.0
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message.0));
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// An existing cluster restarted onto the fix: system_auth.roles holds the
    /// default admin and ferrosa_user rows a pre-fix CREATE ROLE IF NOT EXISTS
    /// wrote. Startup must re-persist the configured admin password (so the
    /// production gate passes on a real credential, not a bypass), lock the
    /// well-known service login, log both, and be idempotent.
    #[test]
    fn repair_reseeds_default_admin_row_and_locks_default_service_login() {
        use ferrosa_schema::auth::bootstrap::admin_password_is_default;
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        write_role_row(&engine, SEED_ADMIN_USER, SEED_ADMIN_PASSWORD, true);
        write_role_row(&engine, SEED_APP_READER_USER, SEED_APP_PASSWORD, false);
        write_role_row(&engine, SEED_GRAPH_ENGINE_USER, "random-service-pw", false);

        let config = SeedConfig::from_parts(
            DeploymentMode::Production,
            Some(MANAGED_ADMIN_PASSWORD.to_string()),
        );
        let log = CapturedLog::default();
        let (schema, report) =
            tracing::subscriber::with_default(log.clone(), || restart(&engine, &config));

        assert_eq!(
            report,
            CredentialRepairReport {
                admin_reseeded: true,
                logins_disabled: vec![SEED_APP_READER_USER.to_string()],
            }
        );
        assert!(log.contains(tracing::Level::WARN, "repaired ferrosa_admin"));
        assert!(log.contains(tracing::Level::ERROR, "disabled LOGIN"));

        assert!(!admin_password_is_default(&schema));
        assert!(schema
            .authenticate(SEED_ADMIN_USER, MANAGED_ADMIN_PASSWORD)
            .is_ok());
        assert!(schema
            .authenticate(SEED_APP_READER_USER, SEED_APP_PASSWORD)
            .is_err());
        assert!(schema
            .authenticate(SEED_GRAPH_ENGINE_USER, "random-service-pw")
            .is_ok());

        let admin_row = persisted(&engine, SEED_ADMIN_USER);
        assert!(role_verifies(&admin_row, MANAGED_ADMIN_PASSWORD));
        assert!(!role_verifies(&admin_row, SEED_ADMIN_PASSWORD));
        assert!(!persisted(&engine, SEED_APP_READER_USER).can_login);
        assert!(persisted(&engine, SEED_GRAPH_ENGINE_USER).can_login);

        // The repair is durable: the next restart replays repaired rows and
        // has nothing left to do.
        let (schema, report) = restart(&engine, &config);
        assert_eq!(report, CredentialRepairReport::default());
        assert!(!admin_password_is_default(&schema));
    }

    /// With no configured admin password there is nothing correct to restore:
    /// the repair changes nothing and logs the remedy, and the default stays
    /// visible so the production gate still refuses it.
    #[test]
    fn repair_without_configured_admin_password_leaves_gate_to_refuse() {
        use ferrosa_schema::auth::bootstrap::admin_password_is_default;
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        write_role_row(&engine, SEED_ADMIN_USER, SEED_ADMIN_PASSWORD, true);

        let config = SeedConfig::from_parts(DeploymentMode::Production, None);
        let log = CapturedLog::default();
        let (schema, report) =
            tracing::subscriber::with_default(log.clone(), || restart(&engine, &config));

        assert!(!report.admin_reseeded);
        assert!(
            admin_password_is_default(&schema),
            "the repair must never hide the default from the production gate"
        );
        assert!(role_verifies(
            &persisted(&engine, SEED_ADMIN_USER),
            SEED_ADMIN_PASSWORD
        ));
        assert!(log.contains(
            tracing::Level::ERROR,
            "FERROSA_SEED_ADMIN_PASSWORD is not set"
        ));
    }

    /// Development mode uses the well-known service password by design.
    #[test]
    fn repair_leaves_development_service_logins_alone() {
        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();
        write_role_row(&engine, SEED_APP_READER_USER, SEED_APP_PASSWORD, false);

        let config = SeedConfig::from_parts(DeploymentMode::Development, None);
        let (schema, report) = restart(&engine, &config);

        assert!(report.logins_disabled.is_empty());
        assert!(schema
            .authenticate(SEED_APP_READER_USER, SEED_APP_PASSWORD)
            .is_ok());
    }

    #[test]
    fn replay_types_reconstructs_udt_into_schema() {
        use ferrosa_common::CqlType;
        use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
        use ferrosa_schema::UserTypeMetadata;

        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        // Persist a CREATE TYPE through the dogfooded writer.
        let udt = UserTypeMetadata {
            keyspace: "app".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
            ],
        };
        SystemTableWriter::new(Arc::clone(&engine))
            .apply(
                ferrosa_schema::system::persistence::SystemTableMutation::TypeCreated(udt.clone()),
            )
            .unwrap();

        // Fresh schema (as on a cold restart) with the owning keyspace present.
        let schema = test_schema();
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: "app".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: std::collections::HashMap::from([(
                        "replication_factor".to_string(),
                        "1".to_string(),
                    )]),
                },
            })
            .unwrap();

        let count = SystemTableLoader::new(engine)
            .replay_types_into_schema(&schema)
            .unwrap();

        assert_eq!(count, 1, "exactly one persisted UDT should be replayed");
        let restored = schema
            .get_type("app", "address")
            .expect("UDT must be live in the Registry after replay");
        assert_eq!(restored, udt, "replayed UDT must equal the persisted one");
    }

    #[test]
    fn replay_indexes_restores_fulltext_index_for_router_resolution() {
        use ferrosa_index::IndexType;
        use ferrosa_schema::metadata::index::IndexMetadata;

        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        // Persist a CREATE INDEX (FullText) through the dogfooded writer.
        let idx = IndexMetadata {
            keyspace: "app".to_string(),
            table: "docs".to_string(),
            name: "idx_body_fts".to_string(),
            index_type: IndexType::FullText,
            target_columns: vec!["body".to_string()],
            filter_predicate: None,
            options: std::collections::HashMap::new(),
        };
        SystemTableWriter::new(Arc::clone(&engine))
            .apply(
                ferrosa_schema::system::persistence::SystemTableMutation::IndexCreated(idx.clone()),
            )
            .unwrap();

        // Cold restart: a fresh registry has NO indexes (the bug — the router's
        // fts_match resolution would fall back to the bare column name).
        let schema = test_schema();
        assert!(
            schema.snapshot().indexes.is_empty(),
            "fresh schema must start with no indexes"
        );

        let count = SystemTableLoader::new(engine)
            .replay_indexes_into_schema(&schema)
            .unwrap();
        assert_eq!(count, 1, "exactly one persisted index should be replayed");

        // After replay the FullText index is resolvable, so the router maps
        // fts_match(body) -> idx_body_fts instead of the bare column.
        let snap = schema.snapshot();
        let meta = snap
            .indexes
            .get(&(
                "app".to_string(),
                "docs".to_string(),
                "idx_body_fts".to_string(),
            ))
            .expect("FullText index must be live in the registry after replay");
        assert_eq!(meta.index_type, IndexType::FullText);
        assert_eq!(meta.target_columns, vec!["body".to_string()]);
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
                scram: None,
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

    #[test]
    fn replay_functions_reconstructs_udf_into_schema() {
        use ferrosa_common::CqlType;
        use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
        use ferrosa_schema::UserFunctionMetadata;

        let dir = tempfile::tempdir().unwrap();
        let engine = test_engine(dir.path());
        engine.register_system_tables().unwrap();

        // Persist a CREATE FUNCTION through the dogfooded writer.
        let func = UserFunctionMetadata {
            keyspace: "app".to_string(),
            name: "double_it".to_string(),
            arg_names: vec!["val".to_string()],
            arg_types: vec![CqlType::Int],
            return_type: CqlType::Int,
            called_on_null: true,
            language: "wasm".to_string(),
            body: "deadbeef".to_string(),
        };
        SystemTableWriter::new(Arc::clone(&engine))
            .apply(
                ferrosa_schema::system::persistence::SystemTableMutation::FunctionCreated(
                    func.clone(),
                ),
            )
            .unwrap();

        // Fresh schema (cold restart) with the owning keyspace present.
        let schema = test_schema();
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: "app".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: std::collections::HashMap::from([(
                        "replication_factor".to_string(),
                        "1".to_string(),
                    )]),
                },
            })
            .unwrap();

        let count = SystemTableLoader::new(engine)
            .replay_functions_into_schema(&schema)
            .unwrap();

        assert_eq!(count, 1, "exactly one persisted UDF should be replayed");
        let restored = schema
            .get_function("app", "double_it", &[CqlType::Int])
            .expect("UDF must be live in the Registry after replay");
        assert_eq!(restored, func, "replayed UDF must equal the persisted one");
    }
}
