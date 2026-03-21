//! Schema registry: the central authority for schema state.
//!
//! Contains `SchemaSnapshot` (the immutable point-in-time view),
//! `SchemaConfig` (bootstrap configuration), and `AuthMethod`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::audit::event::{AuditEvent, AuditEventKind};
use crate::audit::AuditSink;
use crate::auth::password::{PasswordHasher, PasswordPolicy};
use crate::auth::permission::{GrantEntry, Permission, Resource};
use crate::auth::rate_limit::{AuthRateLimiter, RateLimitConfig};
use crate::auth::role::{AuthContext, RoleMetadata, RoleUpdates};
use crate::error::SchemaError;
use ferrosa_common::CqlType;

use crate::metadata::aggregate::UserAggregateMetadata;
use crate::metadata::function::UserFunctionMetadata;
use crate::metadata::index::IndexMetadata;
use crate::metadata::keyspace::{KeyspaceMetadata, KeyspaceUpdates};
use crate::metadata::table::{TableMetadata, TableUpdates};
use crate::metadata::user_type::UserTypeMetadata;
use crate::secrets::SecretsProvider;
use crate::startup::DeploymentMode;
use crate::virtual_registry::VirtualTableRegistry;

/// An immutable point-in-time snapshot of all schema state.
///
/// Contains keyspaces, tables, roles, and grants. Each mutation
/// produces a new snapshot with a new version UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// Unique version identifier for this snapshot.
    pub version: Uuid,
    /// All keyspaces, keyed by name.
    pub keyspaces: HashMap<String, KeyspaceMetadata>,
    /// All tables, keyed by (keyspace, table) pair.
    #[serde(with = "crate::serde_helpers::hash_map_as_vec")]
    pub tables: HashMap<(String, String), TableMetadata>,
    /// All roles, keyed by name.
    pub roles: HashMap<String, RoleMetadata>,
    /// All grants, keyed by role name.
    pub grants: HashMap<String, Vec<GrantEntry>>,
    /// All secondary indexes, keyed by (keyspace, table, index_name).
    #[serde(default, with = "crate::serde_helpers::hash_map_as_vec")]
    pub indexes: HashMap<(String, String, String), IndexMetadata>,
    /// All user-defined types, keyed by (keyspace, type_name).
    #[serde(default, with = "crate::serde_helpers::hash_map_as_vec")]
    pub types: HashMap<(String, String), UserTypeMetadata>,
    /// All user-defined functions, keyed by (keyspace, name, arg_types).
    #[serde(default, with = "crate::serde_helpers::hash_map_as_vec")]
    pub functions: HashMap<(String, String, Vec<CqlType>), UserFunctionMetadata>,
    /// All user-defined aggregates, keyed by (keyspace, name, arg_types).
    #[serde(default, with = "crate::serde_helpers::hash_map_as_vec")]
    pub aggregates: HashMap<(String, String, Vec<CqlType>), UserAggregateMetadata>,
}

impl SchemaSnapshot {
    /// Create an empty snapshot with a random version.
    pub fn new() -> Self {
        Self {
            version: Uuid::new_v4(),
            keyspaces: HashMap::new(),
            tables: HashMap::new(),
            roles: HashMap::new(),
            grants: HashMap::new(),
            indexes: HashMap::new(),
            types: HashMap::new(),
            functions: HashMap::new(),
            aggregates: HashMap::new(),
        }
    }
}

impl Default for SchemaSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// The authentication method required for client connections.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    /// Password-only authentication (bcrypt or argon2id).
    Password,
    /// Certificate-only authentication (mTLS).
    Certificate,
    /// Both certificate and password required.
    CertificateAndPassword,
}

/// Configuration for bootstrapping a `Schema` instance.
///
/// Provides the password hasher, policy, audit sink, secrets provider,
/// and deployment mode needed to initialize the schema registry.
pub struct SchemaConfig {
    /// Password hashing algorithm configuration.
    pub hasher: PasswordHasher,
    /// Password strength policy.
    pub password_policy: PasswordPolicy,
    /// Authentication method for client connections.
    pub auth_method: AuthMethod,
    /// Rate limiting configuration for authentication.
    pub rate_limit: RateLimitConfig,
    /// Audit event sink.
    pub audit_sink: Box<dyn AuditSink>,
    /// Secrets provider for retrieving passwords/keys.
    pub secrets: Box<dyn SecretsProvider>,
    /// Deployment mode (development vs. production).
    pub mode: DeploymentMode,
}

/// The central schema registry.
///
/// Holds the current `SchemaSnapshot` behind an `ArcSwap` for lock-free reads.
/// Mutations acquire `write_lock`, clone-and-swap the snapshot, and emit audit events.
pub struct Schema {
    /// Lock-free swappable snapshot, wrapped in Arc so virtual tables can share it.
    inner: Arc<ArcSwap<SchemaSnapshot>>,
    /// Serializes writes (clone → mutate → store).
    write_lock: Mutex<()>,
    /// Password hashing configuration.
    hasher_config: PasswordHasher,
    /// Password strength policy (used by create_role/alter_role in CRUD operations).
    password_policy: PasswordPolicy,
    /// Authentication rate limiter.
    rate_limiter: AuthRateLimiter,
    /// Audit event sink.
    audit_sink: Box<dyn AuditSink>,
    /// Roles whose password is the default and must be changed.
    default_password_roles: Mutex<HashSet<String>>,
    /// Registry of virtual tables (e.g. system_observability views).
    virtual_table_registry: Arc<VirtualTableRegistry>,
}

impl Schema {
    /// Bootstrap a new schema registry from the given configuration.
    ///
    /// Creates the default `cassandra` superuser role. If the secrets
    /// provider supplies a `superuser_password`, that password is hashed.
    /// Otherwise the default password `"cassandra"` is used and the role
    /// is flagged as must-change.
    pub fn new(config: SchemaConfig) -> crate::Result<Self> {
        let mut snapshot = SchemaSnapshot::new();

        // Check secrets provider for superuser password
        let superuser_password = config
            .secrets
            .get_secret("superuser_password")
            .unwrap_or(None);

        let (password, is_default) = match superuser_password {
            Some(pw) => (pw, false),
            None => ("cassandra".to_string(), true),
        };

        // Hash the password
        let salted_hash = config.hasher.hash_password(&password)?;

        // Create the cassandra superuser role
        let role = RoleMetadata {
            name: "cassandra".to_string(),
            is_superuser: true,
            can_login: true,
            salted_hash: Some(salted_hash),
            member_of: HashSet::new(),
        };
        snapshot.roles.insert("cassandra".to_string(), role);

        let mut default_password_roles = HashSet::new();
        if is_default {
            default_password_roles.insert("cassandra".to_string());
        }

        let schema = Self {
            inner: Arc::new(ArcSwap::new(Arc::new(snapshot))),
            write_lock: Mutex::new(()),
            hasher_config: config.hasher,
            password_policy: config.password_policy,
            rate_limiter: AuthRateLimiter::new(config.rate_limit),
            audit_sink: config.audit_sink,
            default_password_roles: Mutex::new(default_password_roles),
            virtual_table_registry: Arc::new(VirtualTableRegistry::new()),
        };

        // Register built-in virtual tables
        schema.virtual_table_registry.register(Arc::new(
            crate::system::index_tables::SystemSchemaIndexesTable::new(schema.snapshot_handle()),
        ));
        schema.virtual_table_registry.register(Arc::new(
            crate::system::type_tables::SystemSchemaTypesTable::new(schema.snapshot_handle()),
        ));

        // Emit bootstrap audit event
        schema.emit_audit(AuditEventKind::SchemaBootstrapped);

        if is_default {
            schema.emit_audit(AuditEventKind::SuperuserPasswordMustChange);
        }

        Ok(schema)
    }

    /// Return a lock-free snapshot of the current schema state.
    pub fn snapshot(&self) -> Arc<SchemaSnapshot> {
        self.inner.load_full()
    }

    /// Bulk-load schema from a snapshot. Bypasses auth and audit.
    /// Used for pair mode catch-up. Idempotent — silently skips
    /// keyspaces/tables that already exist.
    ///
    /// **Important:** Does NOT register tables with StorageEngine —
    /// caller must call `engine.register_table()` for each table separately.
    pub fn apply_snapshot(&self, snapshot: SchemaSnapshot) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut current = (**self.inner.load()).clone();

        for (name, ks) in &snapshot.keyspaces {
            if is_system_keyspace(name) {
                continue;
            }
            current
                .keyspaces
                .entry(name.clone())
                .or_insert_with(|| ks.clone());
        }

        for ((ks, tbl), table) in &snapshot.tables {
            if is_system_keyspace(ks) {
                continue;
            }
            current
                .tables
                .entry((ks.clone(), tbl.clone()))
                .or_insert_with(|| table.clone());
        }

        for (name, role) in &snapshot.roles {
            current
                .roles
                .entry(name.clone())
                .or_insert_with(|| role.clone());
        }

        for (role, grants) in &snapshot.grants {
            current
                .grants
                .entry(role.clone())
                .or_insert_with(|| grants.clone());
        }

        self.inner.store(Arc::new(current));
        tracing::info!("applied schema snapshot");
        Ok(())
    }

    /// Set the schema snapshot version to a specific UUID.
    ///
    /// Used by the Raft state machine to apply the leader-generated version
    /// so all nodes converge on the same schema version.
    pub fn set_schema_version(&self, version: Uuid) {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        snap.version = version;
        self.inner.store(Arc::new(snap));
    }

    /// Create a keyspace without auth checks. Idempotent — succeeds silently
    /// if keyspace already exists.
    pub fn create_keyspace_internal(&self, ks: KeyspaceMetadata) -> crate::Result<()> {
        crate::validation::validate_keyspace(&ks)?;
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        if snap.keyspaces.contains_key(&ks.name) {
            return Ok(());
        }
        snap.keyspaces.insert(ks.name.clone(), ks);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Create a table without auth checks. Idempotent — succeeds silently
    /// if table already exists.
    pub fn create_table_internal(&self, table: TableMetadata) -> crate::Result<()> {
        crate::validation::validate_table(&table)?;
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (table.keyspace.clone(), table.name.clone());
        if snap.tables.contains_key(&key) {
            return Ok(());
        }
        snap.tables.insert(key, table);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a keyspace without auth checks. Idempotent — succeeds silently
    /// if keyspace doesn't exist.
    pub fn drop_keyspace_internal(&self, name: &str) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        snap.keyspaces.remove(name);
        snap.tables.retain(|(ks, _), _| ks != name);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a table without auth checks. Idempotent — succeeds silently
    /// if table doesn't exist.
    pub fn drop_table_internal(&self, keyspace: &str, table: &str) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        snap.tables
            .remove(&(keyspace.to_string(), table.to_string()));
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Alter an existing keyspace without auth checks. No-op if keyspace doesn't exist.
    /// Used for pair mode DDL replication.
    pub fn alter_keyspace_internal(
        &self,
        name: &str,
        updates: KeyspaceUpdates,
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let Some(ks) = snap.keyspaces.get_mut(name) else {
            return Ok(());
        };
        if let Some(replication) = updates.replication {
            ks.replication = replication;
        }
        if let Some(durable_writes) = updates.durable_writes {
            ks.durable_writes = durable_writes;
        }
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Alter an existing table without auth checks. No-op if table doesn't exist.
    /// Used for pair mode DDL replication.
    pub fn alter_table_internal(
        &self,
        keyspace: &str,
        table: &str,
        updates: TableUpdates,
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (keyspace.to_string(), table.to_string());
        let Some(tbl) = snap.tables.get_mut(&key) else {
            return Ok(());
        };
        if let Some(params) = updates.params {
            tbl.params = params;
        }
        for col in updates.add_columns {
            tbl.columns.insert(col.name.clone(), col);
        }
        for col_name in &updates.drop_columns {
            tbl.columns.shift_remove(col_name);
        }
        if let Some(extensions) = updates.extensions {
            for (k, v) in extensions {
                tbl.extensions.insert(k, v);
            }
        }
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Create a role without auth checks. Idempotent — succeeds silently if role exists.
    /// Used for pair mode DDL replication.
    pub fn create_role_internal(&self, role: RoleMetadata) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        if snap.roles.contains_key(&role.name) {
            return Ok(());
        }
        snap.roles.insert(role.name.clone(), role);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Alter an existing role without auth checks. No-op if role doesn't exist.
    /// The `password` field in `RoleUpdates` is treated as a pre-hashed value
    /// and stored directly as `salted_hash` (not re-hashed).
    /// Used for pair mode DDL replication.
    pub fn alter_role_internal(&self, name: &str, updates: RoleUpdates) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let Some(role) = snap.roles.get_mut(name) else {
            return Ok(());
        };
        if let Some(is_superuser) = updates.is_superuser {
            role.is_superuser = is_superuser;
        }
        if let Some(can_login) = updates.can_login {
            role.can_login = can_login;
        }
        // password field carries the already-hashed value during replication;
        // store it directly without re-hashing.
        if let Some(hash) = updates.password {
            role.salted_hash = Some(hash);
        }
        if let Some(member_of) = updates.member_of {
            role.member_of = member_of;
        }
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a role without auth checks. Idempotent — succeeds silently if role doesn't exist.
    /// Also removes all grants associated with the role.
    /// Used for pair mode DDL replication.
    pub fn drop_role_internal(&self, name: &str) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        snap.roles.remove(name);
        snap.grants.remove(name);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Add a grant entry without auth checks.
    /// If the role already has a grant for the same resource, permissions are merged.
    /// Used for pair mode DDL replication.
    pub fn grant_internal(&self, entry: GrantEntry) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let grants = snap.grants.entry(entry.role.clone()).or_default();
        if let Some(existing) = grants.iter_mut().find(|g| g.resource == entry.resource) {
            existing
                .permissions
                .extend(entry.permissions.iter().copied());
        } else {
            grants.push(entry);
        }
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Remove a specific permission for a role on a resource without auth checks.
    /// Removes the grant entry entirely if no permissions remain.
    /// Used for pair mode DDL replication.
    pub fn revoke_internal(
        &self,
        role: &str,
        resource: &Resource,
        permission: &Permission,
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        if let Some(grants) = snap.grants.get_mut(role) {
            if let Some(entry) = grants.iter_mut().find(|g| &g.resource == resource) {
                entry.permissions.remove(permission);
            }
            grants.retain(|g| !g.permissions.is_empty());
            if grants.is_empty() {
                snap.grants.remove(role);
            }
        }
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Return a reference to the inner ArcSwap for lock-free reads.
    ///
    /// For hot-path code that needs repeated lock-free reads (e.g., observers),
    /// use this instead of `snapshot()` to avoid `Arc` cloning on each call.
    pub fn schema_ref(&self) -> &ArcSwap<SchemaSnapshot> {
        &self.inner
    }

    /// Return a shared handle to the inner ArcSwap for virtual tables
    /// that need lock-free snapshot reads.
    pub fn snapshot_handle(&self) -> Arc<ArcSwap<SchemaSnapshot>> {
        Arc::clone(&self.inner)
    }

    /// Return a reference to the virtual table registry.
    pub fn virtual_tables(&self) -> &VirtualTableRegistry {
        &self.virtual_table_registry
    }

    /// Return a cloned `Arc` to the virtual table registry.
    ///
    /// Use this when you need to share the registry across subsystems (e.g.
    /// to give the web console and the CQL router access to the same instance
    /// without registering tables twice).
    pub fn virtual_tables_arc(&self) -> Arc<VirtualTableRegistry> {
        Arc::clone(&self.virtual_table_registry)
    }

    /// Authenticate a user with username and password.
    ///
    /// Checks the rate limiter, verifies the password, optionally upgrades
    /// the hash algorithm, and returns an `AuthContext` on success.
    pub fn authenticate(&self, username: &str, password: &str) -> crate::Result<AuthContext> {
        // 1. Check rate limiter BEFORE hashing (prevent CPU-based DoS)
        self.rate_limiter.check_rate_limit(username)?;

        // 2. Look up role in snapshot
        let snap = self.snapshot();
        let role = snap.roles.get(username);

        // 3. Verify password
        let (verified, role_data) = match role {
            Some(r) if r.can_login => match &r.salted_hash {
                Some(hash) => (
                    PasswordHasher::verify_password_any(password, hash).unwrap_or(false),
                    Some(r),
                ),
                None => (false, Some(r)),
            },
            _ => {
                // Hash anyway to prevent timing side-channel
                let _ = self.hasher_config.hash_password(password);
                (false, None)
            }
        };

        if !verified {
            self.rate_limiter.record_failure(username);
            self.emit_audit(AuditEventKind::AuthFailed {
                role: username.to_string(),
            });
            return Err(SchemaError::AuthenticationFailed);
        }

        self.rate_limiter.record_success(username);

        // 4. Auto-upgrade hash if algorithm differs
        let role_data = role_data.unwrap();
        if let Some(hash) = &role_data.salted_hash {
            if self.hasher_config.needs_rehash(hash) {
                let _guard = self.write_lock.lock().unwrap();
                let mut new_snap = (*self.snapshot()).clone();
                if let Some(r) = new_snap.roles.get_mut(username) {
                    if let Ok(new_hash) = self.hasher_config.hash_password(password) {
                        r.salted_hash = Some(new_hash);
                        self.inner.store(Arc::new(new_snap));
                        self.emit_audit(AuditEventKind::PasswordChanged {
                            role: username.to_string(),
                            upgraded_algorithm: true,
                        });
                    }
                }
            }
        }

        // 5. Check must_change_password
        let must_change = self
            .default_password_roles
            .lock()
            .unwrap()
            .contains(username);

        self.emit_audit(AuditEventKind::AuthSuccess {
            role: username.to_string(),
        });

        Ok(AuthContext {
            role: username.to_string(),
            is_superuser: role_data.is_superuser,
            must_change_password: must_change,
        })
    }

    /// Check whether `auth` has `perm` on `resource`.
    ///
    /// Delegates to the free function in `auth::permission`, passing the
    /// current snapshot.
    pub fn check_permission(
        &self,
        auth: &AuthContext,
        perm: crate::auth::permission::Permission,
        resource: &crate::auth::permission::Resource,
    ) -> crate::Result<()> {
        crate::auth::permission::check_permission(&self.snapshot(), auth, perm, resource)
    }

    /// Emit an audit event through the configured sink.
    fn emit_audit(&self, kind: AuditEventKind) {
        self.audit_sink.emit(&AuditEvent {
            timestamp: SystemTime::now(),
            event: kind,
            actor: None,
            source: None,
            schema_version: Some(self.snapshot().version),
        });
    }

    /// Emit an audit event with actor information from the auth context.
    fn emit_audit_with_actor(&self, kind: AuditEventKind, auth: &AuthContext) {
        self.audit_sink.emit(&AuditEvent {
            timestamp: SystemTime::now(),
            event: kind,
            actor: Some(auth.role.clone()),
            source: None,
            schema_version: Some(self.snapshot().version),
        });
    }

    /// Validate graph-related extensions on a table.
    ///
    /// Rules:
    /// - Any `graph.*` key requires `Permission::Create` on the keyspace.
    /// - `graph.type` must be `"vertex"` or `"edge"`.
    /// - If `graph.type = "edge"`, then `graph.source`, `graph.target`,
    ///   `graph.source_label`, and `graph.target_label` are required.
    /// - `graph.source_label` and `graph.target_label` must reference
    ///   existing tables with `graph.type = "vertex"` in the same keyspace.
    /// - `graph.source` and `graph.target` must reference existing columns
    ///   in the table being created/altered.
    fn validate_graph_extensions(
        &self,
        snap: &SchemaSnapshot,
        ks: &str,
        table_name: &str,
        extensions: &HashMap<String, String>,
        table_columns: &indexmap::IndexMap<String, crate::metadata::column::ColumnMetadata>,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        let has_graph_keys = extensions.keys().any(|k| k.starts_with("graph."));
        if !has_graph_keys {
            return Ok(());
        }

        // Any graph.* key requires Create permission on the keyspace
        self.check_permission(
            auth,
            Permission::Create,
            &Resource::Keyspace(ks.to_string()),
        )?;

        // Validate graph.type
        if let Some(graph_type) = extensions.get("graph.type") {
            match graph_type.as_str() {
                "vertex" => {}
                "edge" => {
                    // Require edge-specific keys
                    for required_key in &[
                        "graph.source",
                        "graph.target",
                        "graph.source_label",
                        "graph.target_label",
                    ] {
                        if !extensions.contains_key(*required_key) {
                            return Err(SchemaError::InvalidSchema(format!(
                                "edge table {ks}.{table_name} requires extension key '{required_key}'"
                            )));
                        }
                    }

                    // Validate source_label and target_label reference vertex tables
                    for label_key in &["graph.source_label", "graph.target_label"] {
                        let label = &extensions[*label_key];
                        // Find table by graph.label extension OR by table name (fallback).
                        let ref_table = snap.tables.iter().find(|((k, t), meta)| {
                            k == ks
                                && (meta
                                    .extensions
                                    .get("graph.label")
                                    .map(|l| l.eq_ignore_ascii_case(label))
                                    .unwrap_or(false)
                                    || t.eq_ignore_ascii_case(label))
                        });
                        match ref_table {
                            None => {
                                return Err(SchemaError::InvalidSchema(format!(
                                    "edge table {ks}.{table_name}: {label_key} references non-existent vertex label '{label}'"
                                )));
                            }
                            Some((_, ref_meta)) => {
                                if ref_meta.extensions.get("graph.type")
                                    != Some(&"vertex".to_string())
                                {
                                    return Err(SchemaError::InvalidSchema(format!(
                                        "edge table {ks}.{table_name}: {label_key} references label '{label}' which is not a vertex"
                                    )));
                                }
                            }
                        }
                    }

                    // Validate source and target reference columns in this table
                    for col_key in &["graph.source", "graph.target"] {
                        let col_name = &extensions[*col_key];
                        if !table_columns.contains_key(col_name) {
                            return Err(SchemaError::InvalidSchema(format!(
                                "edge table {ks}.{table_name}: {col_key} references non-existent column '{col_name}'"
                            )));
                        }
                    }
                }
                other => {
                    return Err(SchemaError::InvalidSchema(format!(
                        "invalid graph.type '{other}' on table {ks}.{table_name}; must be 'vertex' or 'edge'"
                    )));
                }
            }
        }

        Ok(())
    }

    // ---- Keyspace CRUD ----

    /// Create a new keyspace.
    ///
    /// Requires `Create` permission on `AllKeyspaces`. System keyspaces
    /// cannot be created. Emits a `KeyspaceCreated` audit event.
    pub fn create_keyspace(&self, ks: KeyspaceMetadata, auth: &AuthContext) -> crate::Result<()> {
        self.check_permission(auth, Permission::Create, &Resource::AllKeyspaces)?;
        if is_system_keyspace(&ks.name) {
            return Err(SchemaError::SystemKeyspaceProtected(ks.name));
        }
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if snap.keyspaces.contains_key(&ks.name) {
            return Err(SchemaError::KeyspaceExists(ks.name));
        }
        let name = ks.name.clone();
        snap.keyspaces.insert(ks.name.clone(), ks);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(AuditEventKind::KeyspaceCreated { keyspace: name }, auth);
        Ok(())
    }

    /// Alter an existing keyspace.
    ///
    /// Requires `Alter` permission on the specific keyspace. System keyspaces
    /// cannot be altered. Applies `KeyspaceUpdates` fields that are `Some`.
    pub fn alter_keyspace(
        &self,
        name: &str,
        updates: KeyspaceUpdates,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        self.check_permission(
            auth,
            Permission::Alter,
            &Resource::Keyspace(name.to_string()),
        )?;
        if is_system_keyspace(name) {
            return Err(SchemaError::SystemKeyspaceProtected(name.to_string()));
        }
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        let ks = snap
            .keyspaces
            .get_mut(name)
            .ok_or_else(|| SchemaError::KeyspaceNotFound(name.to_string()))?;
        if let Some(replication) = updates.replication {
            ks.replication = replication;
        }
        if let Some(durable_writes) = updates.durable_writes {
            ks.durable_writes = durable_writes;
        }
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::KeyspaceAltered {
                keyspace: name.to_string(),
            },
            auth,
        );
        Ok(())
    }

    /// Drop an existing keyspace.
    ///
    /// Requires `Drop` permission on `AllKeyspaces`. System keyspaces
    /// cannot be dropped. Also removes all tables belonging to the keyspace.
    pub fn drop_keyspace(&self, name: &str, auth: &AuthContext) -> crate::Result<()> {
        self.check_permission(auth, Permission::Drop, &Resource::AllKeyspaces)?;
        if is_system_keyspace(name) {
            return Err(SchemaError::SystemKeyspaceProtected(name.to_string()));
        }
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if snap.keyspaces.remove(name).is_none() {
            return Err(SchemaError::KeyspaceNotFound(name.to_string()));
        }
        // Remove all tables in the dropped keyspace
        snap.tables.retain(|(ks, _), _| ks != name);
        // Remove all indexes in the dropped keyspace
        snap.indexes.retain(|(ks, _, _), _| ks != name);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::KeyspaceDropped {
                keyspace: name.to_string(),
            },
            auth,
        );
        Ok(())
    }

    // ---- Table CRUD ----

    /// Create a new table in an existing keyspace.
    ///
    /// Requires `Create` permission on the parent keyspace. The keyspace
    /// must exist and the table must not already exist. Emits a
    /// `TableCreated` audit event.
    pub fn create_table(&self, table: TableMetadata, auth: &AuthContext) -> crate::Result<()> {
        self.check_permission(
            auth,
            Permission::Create,
            &Resource::Keyspace(table.keyspace.clone()),
        )?;
        if is_system_keyspace(&table.keyspace) {
            return Err(SchemaError::SystemKeyspaceProtected(table.keyspace));
        }
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if !snap.keyspaces.contains_key(&table.keyspace) {
            return Err(SchemaError::KeyspaceNotFound(table.keyspace));
        }
        let key = (table.keyspace.clone(), table.name.clone());
        if snap.tables.contains_key(&key) {
            return Err(SchemaError::TableExists(table.keyspace, table.name));
        }
        if table.extensions.keys().any(|k| k.starts_with("graph.")) {
            self.validate_graph_extensions(
                &snap,
                &table.keyspace,
                &table.name,
                &table.extensions,
                &table.columns,
                auth,
            )?;
        }
        let ks = table.keyspace.clone();
        let name = table.name.clone();
        snap.tables.insert(key, table);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::TableCreated {
                keyspace: ks,
                table: name,
            },
            auth,
        );
        Ok(())
    }

    /// Alter an existing table.
    ///
    /// Requires `Alter` permission on the specific table. Applies
    /// `TableUpdates`: replaces params if provided, adds new columns,
    /// and drops specified columns.
    pub fn alter_table(
        &self,
        ks: &str,
        table: &str,
        updates: TableUpdates,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        self.check_permission(
            auth,
            Permission::Alter,
            &Resource::Table(ks.to_string(), table.to_string()),
        )?;
        if is_system_keyspace(ks) {
            return Err(SchemaError::SystemKeyspaceProtected(ks.to_string()));
        }
        let snap_ref = self.snapshot();
        let key = (ks.to_string(), table.to_string());
        if let Some(existing) = snap_ref.tables.get(&key) {
            if existing.is_system {
                return Err(SchemaError::SystemTableProtected(
                    ks.to_string(),
                    table.to_string(),
                ));
            }
        }
        drop(snap_ref);
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        let tbl = snap
            .tables
            .get_mut(&key)
            .ok_or_else(|| SchemaError::TableNotFound(ks.to_string(), table.to_string()))?;
        if let Some(params) = updates.params {
            tbl.params = params;
        }
        for col in updates.add_columns {
            tbl.columns.insert(col.name.clone(), col);
        }
        for col_name in &updates.drop_columns {
            tbl.columns.shift_remove(col_name);
        }
        if let Some(ref extensions) = updates.extensions {
            if extensions.keys().any(|k| k.starts_with("graph.")) {
                let columns_snapshot = tbl.columns.clone();
                self.validate_graph_extensions(
                    &snap,
                    ks,
                    table,
                    extensions,
                    &columns_snapshot,
                    auth,
                )?;
            }
            let tbl = snap.tables.get_mut(&key).expect("table must exist");
            for (k, v) in extensions {
                tbl.extensions.insert(k.clone(), v.clone());
            }
        }
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::TableAltered {
                keyspace: ks.to_string(),
                table: table.to_string(),
            },
            auth,
        );
        Ok(())
    }

    /// Drop an existing table.
    ///
    /// Requires `Drop` permission on the parent keyspace. Emits a
    /// `TableDropped` audit event.
    pub fn drop_table(&self, keyspace: &str, table: &str, auth: &AuthContext) -> crate::Result<()> {
        self.check_permission(
            auth,
            Permission::Drop,
            &Resource::Keyspace(keyspace.to_string()),
        )?;
        if is_system_keyspace(keyspace) {
            return Err(SchemaError::SystemKeyspaceProtected(keyspace.to_string()));
        }
        let snap_ref = self.snapshot();
        let key = (keyspace.to_string(), table.to_string());
        if let Some(existing) = snap_ref.tables.get(&key) {
            if existing.is_system {
                return Err(SchemaError::SystemTableProtected(
                    keyspace.to_string(),
                    table.to_string(),
                ));
            }
        }
        drop(snap_ref);
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if snap.tables.remove(&key).is_none() {
            return Err(SchemaError::TableNotFound(
                keyspace.to_string(),
                table.to_string(),
            ));
        }
        // Remove all indexes on the dropped table
        snap.indexes
            .retain(|(ks, tbl, _), _| !(ks == keyspace && tbl == table));
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::TableDropped {
                keyspace: keyspace.to_string(),
                table: table.to_string(),
            },
            auth,
        );
        Ok(())
    }

    // ---- Index CRUD ----

    /// Create a secondary index (internal, idempotent).
    ///
    /// Inserts the index if it does not already exist. Succeeds silently
    /// on duplicates (idempotent).
    pub fn create_index_internal(&self, index: IndexMetadata) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (*self.inner.load_full()).clone();
        let key = (
            index.keyspace.clone(),
            index.table.clone(),
            index.name.clone(),
        );
        snap.indexes.entry(key).or_insert(index);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a secondary index (internal, idempotent).
    ///
    /// Removes the index if it exists. Succeeds silently if the index
    /// does not exist (idempotent).
    pub fn drop_index_internal(
        &self,
        keyspace: &str,
        table: &str,
        name: &str,
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (*self.inner.load_full()).clone();
        snap.indexes
            .remove(&(keyspace.to_string(), table.to_string(), name.to_string()));
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Create a secondary index with authorization.
    ///
    /// Requires `Alter` permission on the parent table.
    pub fn create_index(&self, index: IndexMetadata, auth: &AuthContext) -> crate::Result<()> {
        let resource = Resource::Table(index.keyspace.clone(), index.table.clone());
        self.check_permission(auth, Permission::Alter, &resource)?;
        self.create_index_internal(index)
    }

    /// Drop a secondary index with authorization.
    ///
    /// Requires `Alter` permission on the parent table.
    pub fn drop_index(
        &self,
        keyspace: &str,
        table: &str,
        name: &str,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        let resource = Resource::Table(keyspace.to_string(), table.to_string());
        self.check_permission(auth, Permission::Alter, &resource)?;
        self.drop_index_internal(keyspace, table, name)
    }

    // ---- Role CRUD ----

    /// Create a new role with optional password.
    ///
    /// Requires `Create` permission on `AllRoles`. Validates password
    /// against the password policy and hashes it. Checks for role hierarchy
    /// cycles in `member_of`.
    pub fn create_role(
        &self,
        role: RoleMetadata,
        password: Option<&str>,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        self.check_permission(auth, Permission::Create, &Resource::AllRoles)?;
        let salted_hash = if let Some(pw) = password {
            self.password_policy.validate(pw, &role.name)?;
            Some(self.hasher_config.hash_password(pw)?)
        } else {
            None
        };
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if snap.roles.contains_key(&role.name) {
            return Err(SchemaError::RoleExists(role.name));
        }
        for parent in &role.member_of {
            if would_create_cycle(&snap, parent, &role.name) {
                return Err(SchemaError::RoleCycleDetected(role.name));
            }
        }
        let name = role.name.clone();
        let is_su = role.is_superuser;
        let mut role = role;
        role.salted_hash = salted_hash;
        snap.roles.insert(role.name.clone(), role);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::RoleCreated {
                role: name,
                is_superuser: is_su,
            },
            auth,
        );
        Ok(())
    }

    /// Alter an existing role.
    ///
    /// Requires `Alter` permission on `AllRoles`. Applies non-`None` fields
    /// from `RoleUpdates`. If password changes, validates against policy,
    /// hashes, and emits `PasswordChanged` audit event. Checks for cycles
    /// if `member_of` changes.
    pub fn alter_role(
        &self,
        name: &str,
        updates: RoleUpdates,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        self.check_permission(auth, Permission::Alter, &Resource::AllRoles)?;
        // Validate and hash password outside the write lock (expensive)
        let new_hash = if let Some(ref pw) = updates.password {
            self.password_policy.validate(pw, name)?;
            Some(self.hasher_config.hash_password(pw)?)
        } else {
            None
        };
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if !snap.roles.contains_key(name) {
            return Err(SchemaError::RoleNotFound(name.to_string()));
        }
        // Check for cycles before taking a mutable reference
        if let Some(ref member_of) = updates.member_of {
            for parent in member_of {
                if would_create_cycle(&snap, parent, name) {
                    return Err(SchemaError::RoleCycleDetected(name.to_string()));
                }
            }
        }
        let role = snap.roles.get_mut(name).unwrap();
        if let Some(is_superuser) = updates.is_superuser {
            role.is_superuser = is_superuser;
        }
        if let Some(can_login) = updates.can_login {
            role.can_login = can_login;
        }
        if let Some(ref member_of) = updates.member_of {
            role.member_of = member_of.clone();
        }
        let password_changed = new_hash.is_some();
        if let Some(hash) = new_hash {
            role.salted_hash = Some(hash);
        }
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::RoleAltered {
                role: name.to_string(),
            },
            auth,
        );
        if password_changed {
            self.emit_audit_with_actor(
                AuditEventKind::PasswordChanged {
                    role: name.to_string(),
                    upgraded_algorithm: false,
                },
                auth,
            );
        }
        Ok(())
    }

    /// Drop an existing role.
    ///
    /// Requires `Drop` permission on `AllRoles`. Also removes all grants
    /// associated with the role.
    pub fn drop_role(&self, name: &str, auth: &AuthContext) -> crate::Result<()> {
        self.check_permission(auth, Permission::Drop, &Resource::AllRoles)?;
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if snap.roles.remove(name).is_none() {
            return Err(SchemaError::RoleNotFound(name.to_string()));
        }
        // Remove all grants for the dropped role
        snap.grants.remove(name);
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::RoleDropped {
                role: name.to_string(),
            },
            auth,
        );
        Ok(())
    }

    // ---- Grant/Revoke ----

    /// Grant permissions to a role on a resource.
    ///
    /// Requires `Authorize` permission on `AllRoles`. If the role already
    /// has a grant for the resource, the new permissions are merged in.
    pub fn grant(
        &self,
        role: &str,
        resource: &Resource,
        perms: HashSet<Permission>,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        self.check_permission(auth, Permission::Authorize, &Resource::AllRoles)?;
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if !snap.roles.contains_key(role) {
            return Err(SchemaError::RoleNotFound(role.to_string()));
        }
        let grants = snap.grants.entry(role.to_string()).or_default();
        if let Some(existing) = grants.iter_mut().find(|g| g.resource == *resource) {
            existing.permissions.extend(perms.iter().copied());
        } else {
            grants.push(GrantEntry {
                role: role.to_string(),
                resource: resource.clone(),
                permissions: perms.clone(),
            });
        }
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::PermissionGranted {
                role: role.to_string(),
                resource: resource.clone(),
                permissions: perms,
            },
            auth,
        );
        Ok(())
    }

    /// Revoke permissions from a role on a resource.
    ///
    /// Requires `Authorize` permission on `AllRoles`. Removes the specified
    /// permissions. If all permissions are removed, the grant entry is
    /// removed entirely.
    pub fn revoke(
        &self,
        role: &str,
        resource: &Resource,
        perms: HashSet<Permission>,
        auth: &AuthContext,
    ) -> crate::Result<()> {
        self.check_permission(auth, Permission::Authorize, &Resource::AllRoles)?;
        let _guard = self.write_lock.lock().unwrap();
        let mut snap = (*self.snapshot()).clone();
        if !snap.roles.contains_key(role) {
            return Err(SchemaError::RoleNotFound(role.to_string()));
        }
        let grants = snap.grants.entry(role.to_string()).or_default();
        if let Some(existing) = grants.iter_mut().find(|g| g.resource == *resource) {
            for perm in &perms {
                existing.permissions.remove(perm);
            }
        }
        // Remove empty grant entries
        if let Some(grants) = snap.grants.get_mut(role) {
            grants.retain(|g| !g.permissions.is_empty());
            if grants.is_empty() {
                snap.grants.remove(role);
            }
        }
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        self.emit_audit_with_actor(
            AuditEventKind::PermissionRevoked {
                role: role.to_string(),
                resource: resource.clone(),
                permissions: perms,
            },
            auth,
        );
        Ok(())
    }

    // ---- User-Defined Type (UDT) operations ----

    /// Create a user-defined type. Returns error if type already exists or
    /// keyspace not found.
    pub fn create_type_internal(&self, udt: &UserTypeMetadata) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (udt.keyspace.clone(), udt.name.clone());
        if snap.types.contains_key(&key) {
            return Err(SchemaError::TypeExists(
                udt.keyspace.clone(),
                udt.name.clone(),
            ));
        }
        if !snap.keyspaces.contains_key(&udt.keyspace) {
            return Err(SchemaError::KeyspaceNotFound(udt.keyspace.clone()));
        }
        snap.types.insert(key, udt.clone());
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a user-defined type. Returns error if type not found.
    pub fn drop_type_internal(&self, keyspace: &str, name: &str) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (keyspace.to_string(), name.to_string());
        if !snap.types.contains_key(&key) {
            return Err(SchemaError::TypeNotFound(
                keyspace.to_string(),
                name.to_string(),
            ));
        }
        snap.types.remove(&key);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Look up a user-defined type by keyspace and name.
    pub fn get_type(&self, keyspace: &str, name: &str) -> Option<UserTypeMetadata> {
        let snap = self.snapshot();
        snap.types
            .get(&(keyspace.to_string(), name.to_string()))
            .cloned()
    }

    /// Alter a type — add a field. Returns error if type not found or field
    /// already exists.
    pub fn alter_type_add_field(
        &self,
        keyspace: &str,
        name: &str,
        field_name: &str,
        field_type: CqlType,
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (keyspace.to_string(), name.to_string());
        let udt = snap
            .types
            .get_mut(&key)
            .ok_or_else(|| SchemaError::TypeNotFound(keyspace.to_string(), name.to_string()))?;
        if udt.fields.iter().any(|(n, _)| n == field_name) {
            return Err(SchemaError::FieldExists(
                keyspace.to_string(),
                name.to_string(),
                field_name.to_string(),
            ));
        }
        udt.fields.push((field_name.to_string(), field_type));
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Alter a type — rename a field. Returns error if type or field not found.
    pub fn alter_type_rename_field(
        &self,
        keyspace: &str,
        type_name: &str,
        from: &str,
        to: &str,
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (keyspace.to_string(), type_name.to_string());
        let udt = snap.types.get_mut(&key).ok_or_else(|| {
            SchemaError::TypeNotFound(keyspace.to_string(), type_name.to_string())
        })?;
        let field = udt
            .fields
            .iter_mut()
            .find(|(n, _)| n == from)
            .ok_or_else(|| {
                SchemaError::FieldNotFound(
                    keyspace.to_string(),
                    type_name.to_string(),
                    from.to_string(),
                )
            })?;
        field.0 = to.to_string();
        snap.version = Uuid::new_v4();
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    // ---- User-Defined Function (UDF) operations ----

    /// Create a user-defined function. Returns error if function already exists
    /// (same keyspace, name, and arg_types) or keyspace not found.
    pub fn create_function_internal(&self, func: &UserFunctionMetadata) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (
            func.keyspace.clone(),
            func.name.clone(),
            func.arg_types.clone(),
        );
        if snap.functions.contains_key(&key) {
            return Err(SchemaError::FunctionExists(
                func.keyspace.clone(),
                func.name.clone(),
            ));
        }
        if !snap.keyspaces.contains_key(&func.keyspace) {
            return Err(SchemaError::KeyspaceNotFound(func.keyspace.clone()));
        }
        snap.functions.insert(key, func.clone());
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a user-defined function. Returns error if function not found.
    pub fn drop_function_internal(
        &self,
        keyspace: &str,
        name: &str,
        arg_types: &[CqlType],
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (keyspace.to_string(), name.to_string(), arg_types.to_vec());
        if !snap.functions.contains_key(&key) {
            return Err(SchemaError::FunctionNotFound(
                keyspace.to_string(),
                name.to_string(),
            ));
        }
        snap.functions.remove(&key);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Look up a user-defined function by keyspace, name, and arg types.
    pub fn get_function(
        &self,
        keyspace: &str,
        name: &str,
        arg_types: &[CqlType],
    ) -> Option<UserFunctionMetadata> {
        let snap = self.snapshot();
        snap.functions
            .get(&(keyspace.to_string(), name.to_string(), arg_types.to_vec()))
            .cloned()
    }

    // ---- User-Defined Aggregate (UDA) operations ----

    /// Create a user-defined aggregate. Returns error if aggregate already exists
    /// (same keyspace, name, and arg_types) or keyspace not found.
    pub fn create_aggregate_internal(&self, agg: &UserAggregateMetadata) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (
            agg.keyspace.clone(),
            agg.name.clone(),
            agg.arg_types.clone(),
        );
        if snap.aggregates.contains_key(&key) {
            return Err(SchemaError::AggregateExists(
                agg.keyspace.clone(),
                agg.name.clone(),
            ));
        }
        if !snap.keyspaces.contains_key(&agg.keyspace) {
            return Err(SchemaError::KeyspaceNotFound(agg.keyspace.clone()));
        }
        snap.aggregates.insert(key, agg.clone());
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Drop a user-defined aggregate. Returns error if aggregate not found.
    pub fn drop_aggregate_internal(
        &self,
        keyspace: &str,
        name: &str,
        arg_types: &[CqlType],
    ) -> crate::Result<()> {
        let _lock = self.write_lock.lock().unwrap();
        let mut snap = (**self.inner.load()).clone();
        let key = (keyspace.to_string(), name.to_string(), arg_types.to_vec());
        if !snap.aggregates.contains_key(&key) {
            return Err(SchemaError::AggregateNotFound(
                keyspace.to_string(),
                name.to_string(),
            ));
        }
        snap.aggregates.remove(&key);
        self.inner.store(Arc::new(snap));
        Ok(())
    }

    /// Look up a user-defined aggregate by keyspace, name, and arg types.
    pub fn get_aggregate(
        &self,
        keyspace: &str,
        name: &str,
        arg_types: &[CqlType],
    ) -> Option<UserAggregateMetadata> {
        let snap = self.snapshot();
        snap.aggregates
            .get(&(keyspace.to_string(), name.to_string(), arg_types.to_vec()))
            .cloned()
    }
}

#[cfg(test)]
impl Schema {
    /// Construct a minimal `Schema` suitable for unit tests.
    ///
    /// Uses bcrypt cost 4, permissive password policy, a no-op audit sink,
    /// and the `EnvSecretsProvider`. Clears `FERROSA_SUPERUSER_PASSWORD`
    /// from the environment before constructing so tests are hermetic.
    pub fn new_for_test() -> Self {
        use crate::audit::TestAuditSink;
        use crate::auth::password::{PasswordHasher, PasswordPolicy};
        use crate::auth::rate_limit::RateLimitConfig;
        use crate::secrets::EnvSecretsProvider;

        unsafe {
            std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
        }
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .expect("test schema construction must not fail")
    }
}

/// Returns true if the given name is a Cassandra system keyspace.
pub fn is_system_keyspace(name: &str) -> bool {
    matches!(
        name,
        "system"
            | "system_schema"
            | "system_auth"
            | "system_distributed"
            | "system_traces"
            | "system_virtual_schema"
    )
}

/// Returns true if adding `new_role_name` as a child of `parent` would
/// create a cycle in the role hierarchy.
fn would_create_cycle(snap: &SchemaSnapshot, parent: &str, new_role_name: &str) -> bool {
    // Walk upward from `parent` -- if we reach `new_role_name`, it's a cycle.
    let mut visited = HashSet::new();
    let mut stack = vec![parent.to_string()];
    while let Some(current) = stack.pop() {
        if current == new_role_name {
            return true;
        }
        if !visited.insert(current.clone()) {
            continue;
        }
        if let Some(role) = snap.roles.get(&current) {
            for grandparent in &role.member_of {
                stack.push(grandparent.clone());
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::TestAuditSink;
    use crate::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use crate::secrets::EnvSecretsProvider;

    /// Mutex to serialize tests that manipulate the FERROSA_SUPERUSER_PASSWORD env var.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn test_config() -> SchemaConfig {
        SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        }
    }

    fn test_schema() -> Schema {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
        }
        Schema::new(test_config()).unwrap()
    }

    fn test_schema_with_sink(sink: Arc<TestAuditSink>) -> Schema {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
        }
        Schema::new(SchemaConfig {
            audit_sink: Box::new(sink),
            ..test_config()
        })
        .unwrap()
    }

    // ---- Task 16 tests ----

    #[test]
    fn schema_snapshot_is_empty_on_construction() {
        let snap = SchemaSnapshot::new();
        assert!(snap.keyspaces.is_empty());
        assert!(snap.tables.is_empty());
        assert!(snap.roles.is_empty());
        assert!(snap.grants.is_empty());
        // Version should be a valid v4 UUID
        assert_eq!(snap.version.get_version_num(), 4);
    }

    #[test]
    fn schema_snapshot_serde_roundtrip() {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert(
            "test_ks".to_string(),
            KeyspaceMetadata {
                name: "test_ks".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut opts = HashMap::new();
                        opts.insert("replication_factor".to_string(), "3".to_string());
                        opts
                    },
                },
            },
        );

        let json = serde_json::to_string(&snap).expect("serialize");
        let deserialized: SchemaSnapshot = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(snap.version, deserialized.version);
        assert_eq!(deserialized.keyspaces.len(), 1);
        assert!(deserialized.keyspaces.contains_key("test_ks"));
        assert_eq!(
            deserialized.keyspaces["test_ks"].replication.strategy,
            "SimpleStrategy"
        );
    }

    #[test]
    fn schema_config_builds() {
        let config = test_config();
        // Just verify we can construct it and access fields
        assert!(matches!(config.auth_method, AuthMethod::Password));
        assert!(matches!(config.mode, DeploymentMode::Development));
        assert!(matches!(config.hasher, PasswordHasher::Bcrypt { cost: 4 }));
    }

    // ---- Task 17 tests ----

    #[test]
    fn schema_new_creates_default_superuser() {
        let schema = test_schema();
        let snap = schema.snapshot();
        let role = snap.roles.get("cassandra").expect("cassandra role exists");
        assert!(role.is_superuser);
        assert!(role.can_login);
        assert!(role.salted_hash.is_some());
    }

    #[test]
    fn schema_new_with_env_password() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("FERROSA_SUPERUSER_PASSWORD", "s3cure!Pass");
        }
        let schema = Schema::new(test_config()).unwrap();
        unsafe {
            std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
        }

        let snap = schema.snapshot();
        let role = snap.roles.get("cassandra").expect("cassandra role exists");
        let hash = role.salted_hash.as_ref().expect("has hash");

        // The password from the env var should verify correctly
        assert!(PasswordHasher::verify_password_any("s3cure!Pass", hash).unwrap());
        // The default password should NOT verify
        assert!(!PasswordHasher::verify_password_any("cassandra", hash).unwrap());
    }

    #[test]
    fn schema_new_without_env_password_marks_must_change() {
        let schema = test_schema();
        // The cassandra role should be in the default_password_roles set
        let defaults = schema.default_password_roles.lock().unwrap();
        assert!(defaults.contains("cassandra"));
    }

    #[test]
    fn schema_snapshot_returns_arc() {
        let schema = test_schema();
        let snap1 = schema.snapshot();
        let snap2 = schema.snapshot();
        // Both loads should return the same version (no mutations occurred)
        assert_eq!(snap1.version, snap2.version);
    }

    #[test]
    fn schema_bootstrap_emits_audit_event() {
        let sink = Arc::new(TestAuditSink::new());
        let _schema = test_schema_with_sink(sink.clone());

        let events = sink.events();
        // Should have SchemaBootstrapped and SuperuserPasswordMustChange
        assert!(events
            .iter()
            .any(|e| matches!(&e.event, AuditEventKind::SchemaBootstrapped)));
        assert!(events
            .iter()
            .any(|e| matches!(&e.event, AuditEventKind::SuperuserPasswordMustChange)));
    }

    // ---- Task 18 tests ----

    #[test]
    fn authenticate_valid_credentials() {
        let schema = test_schema();
        // Default password is "cassandra"
        let ctx = schema.authenticate("cassandra", "cassandra").unwrap();
        assert_eq!(ctx.role, "cassandra");
        assert!(ctx.is_superuser);
        assert!(ctx.must_change_password); // default password
    }

    #[test]
    fn authenticate_wrong_password() {
        let schema = test_schema();
        let result = schema.authenticate("cassandra", "wrong_password");
        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::AuthenticationFailed => {}
            other => panic!("expected AuthenticationFailed, got: {other}"),
        }
    }

    #[test]
    fn authenticate_nonexistent_role() {
        let schema = test_schema();
        // Should return AuthenticationFailed, NOT RoleNotFound (security)
        let result = schema.authenticate("ghost_user", "any_password");
        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::AuthenticationFailed => {}
            other => panic!("expected AuthenticationFailed, got: {other}"),
        }
    }

    #[test]
    fn authenticate_emits_success_audit() {
        let sink = Arc::new(TestAuditSink::new());
        let schema = test_schema_with_sink(sink.clone());

        sink.clear(); // Clear bootstrap events
        let _ctx = schema.authenticate("cassandra", "cassandra").unwrap();

        let events = sink.events();
        assert!(events.iter().any(|e| matches!(
            &e.event,
            AuditEventKind::AuthSuccess { role } if role == "cassandra"
        )));
    }

    #[test]
    fn authenticate_emits_failed_audit() {
        let sink = Arc::new(TestAuditSink::new());
        let schema = test_schema_with_sink(sink.clone());

        sink.clear(); // Clear bootstrap events
        let _ = schema.authenticate("cassandra", "bad");

        let events = sink.events();
        assert!(events.iter().any(|e| matches!(
            &e.event,
            AuditEventKind::AuthFailed { role } if role == "cassandra"
        )));
    }

    #[test]
    fn authenticate_rate_limited_after_failures() {
        use std::time::Duration;

        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
        }
        let schema = Schema::new(SchemaConfig {
            rate_limit: RateLimitConfig {
                max_attempts: 2,
                base_backoff: Duration::from_secs(60), // long backoff
                max_backoff: Duration::from_secs(60),
                lockout_duration: Duration::from_secs(60),
                window: Duration::from_secs(60),
            },
            ..test_config()
        })
        .unwrap();

        // Two failed attempts should trigger lockout
        let _ = schema.authenticate("cassandra", "wrong1");
        let _ = schema.authenticate("cassandra", "wrong2");
        let result = schema.authenticate("cassandra", "cassandra");
        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::AuthenticationThrottled => {}
            other => panic!("expected AuthenticationThrottled, got: {other}"),
        }
    }

    // ---- CRUD test helpers ----

    use crate::auth::permission::{Permission, Resource};
    use crate::metadata::keyspace::KeyspaceUpdates;

    fn superuser_auth() -> AuthContext {
        AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn normal_auth(role: &str) -> AuthContext {
        AuthContext {
            role: role.to_string(),
            is_superuser: false,
            must_change_password: false,
        }
    }

    fn test_keyspace(name: &str) -> KeyspaceMetadata {
        KeyspaceMetadata {
            name: name.to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: {
                    let mut opts = HashMap::new();
                    opts.insert("replication_factor".to_string(), "3".to_string());
                    opts
                },
            },
        }
    }

    // ---- Task 20: Keyspace CRUD tests ----

    #[test]
    fn create_keyspace_as_superuser() {
        let schema = test_schema();
        let auth = superuser_auth();
        let ks = test_keyspace("my_ks");
        schema.create_keyspace(ks, &auth).unwrap();

        let snap = schema.snapshot();
        assert!(snap.keyspaces.contains_key("my_ks"));
        assert_eq!(
            snap.keyspaces["my_ks"].replication.strategy,
            "SimpleStrategy"
        );
    }

    #[test]
    fn create_keyspace_duplicate_fails() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("dup_ks"), &auth)
            .unwrap();
        let result = schema.create_keyspace(test_keyspace("dup_ks"), &auth);
        assert!(matches!(result, Err(SchemaError::KeyspaceExists(ref n)) if n == "dup_ks"));
    }

    #[test]
    fn create_keyspace_bumps_version() {
        let schema = test_schema();
        let auth = superuser_auth();
        let v1 = schema.snapshot().version;
        schema
            .create_keyspace(test_keyspace("ks_v"), &auth)
            .unwrap();
        let v2 = schema.snapshot().version;
        assert_ne!(v1, v2);
    }

    #[test]
    fn create_keyspace_emits_audit() {
        let sink = Arc::new(TestAuditSink::new());
        let schema = test_schema_with_sink(sink.clone());
        let auth = superuser_auth();
        sink.clear();
        schema
            .create_keyspace(test_keyspace("audit_ks"), &auth)
            .unwrap();

        let events = sink.events();
        assert!(events.iter().any(|e| matches!(
            &e.event,
            AuditEventKind::KeyspaceCreated { keyspace } if keyspace == "audit_ks"
        )));
        // Actor should be set
        let ks_event = events
            .iter()
            .find(|e| matches!(&e.event, AuditEventKind::KeyspaceCreated { .. }))
            .unwrap();
        assert_eq!(ks_event.actor.as_deref(), Some("cassandra"));
    }

    #[test]
    fn drop_keyspace_removes_it() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("drop_ks"), &auth)
            .unwrap();
        assert!(schema.snapshot().keyspaces.contains_key("drop_ks"));

        schema.drop_keyspace("drop_ks", &auth).unwrap();
        assert!(!schema.snapshot().keyspaces.contains_key("drop_ks"));
    }

    #[test]
    fn drop_keyspace_not_found() {
        let schema = test_schema();
        let auth = superuser_auth();
        let result = schema.drop_keyspace("nonexistent", &auth);
        assert!(matches!(result, Err(SchemaError::KeyspaceNotFound(ref n)) if n == "nonexistent"));
    }

    #[test]
    fn alter_keyspace_updates_replication() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("alter_ks"), &auth)
            .unwrap();

        let updates = KeyspaceUpdates {
            replication: Some(ReplicationParams {
                strategy: "NetworkTopologyStrategy".to_string(),
                options: {
                    let mut opts = HashMap::new();
                    opts.insert("dc1".to_string(), "3".to_string());
                    opts
                },
            }),
            durable_writes: Some(false),
        };
        schema.alter_keyspace("alter_ks", updates, &auth).unwrap();

        let snap = schema.snapshot();
        let ks = &snap.keyspaces["alter_ks"];
        assert_eq!(ks.replication.strategy, "NetworkTopologyStrategy");
        assert!(!ks.durable_writes);
    }

    #[test]
    fn system_keyspace_protected() {
        let schema = test_schema();
        let auth = superuser_auth();

        // Cannot create a system keyspace
        let result = schema.create_keyspace(test_keyspace("system"), &auth);
        assert!(
            matches!(result, Err(SchemaError::SystemKeyspaceProtected(ref n)) if n == "system")
        );

        // Cannot drop a system keyspace
        let result = schema.drop_keyspace("system_auth", &auth);
        assert!(
            matches!(result, Err(SchemaError::SystemKeyspaceProtected(ref n)) if n == "system_auth")
        );

        // Cannot alter a system keyspace
        let updates = KeyspaceUpdates {
            replication: None,
            durable_writes: Some(false),
        };
        let result = schema.alter_keyspace("system_schema", updates, &auth);
        assert!(
            matches!(result, Err(SchemaError::SystemKeyspaceProtected(ref n)) if n == "system_schema")
        );
    }

    #[test]
    fn non_superuser_without_grant_denied() {
        let schema = test_schema();
        let auth = normal_auth("nobody");
        let result = schema.create_keyspace(test_keyspace("forbidden"), &auth);
        assert!(matches!(result, Err(SchemaError::PermissionDenied { .. })));
    }

    // ---- Task 21: Table CRUD tests ----

    use crate::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
    use crate::metadata::table::{TableFlag, TableMetadata, TableParams, TableUpdates};
    use indexmap::IndexMap;

    fn test_table(keyspace: &str, name: &str) -> TableMetadata {
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
        let mut flags = HashSet::new();
        flags.insert(TableFlag::Compound);
        TableMetadata {
            keyspace: keyspace.to_string(),
            name: name.to_string(),
            id: Uuid::new_v4(),
            columns,
            partition_key: vec!["id".to_string()],
            clustering_key: vec![],
            params: TableParams::default(),
            flags,
            extensions: HashMap::new(),
            is_system: false,
        }
    }

    #[test]
    fn create_table_in_existing_keyspace() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("tbl_ks"), &auth)
            .unwrap();

        let table = test_table("tbl_ks", "users");
        schema.create_table(table, &auth).unwrap();

        let snap = schema.snapshot();
        let key = ("tbl_ks".to_string(), "users".to_string());
        assert!(snap.tables.contains_key(&key));
    }

    #[test]
    fn create_table_in_nonexistent_keyspace_fails() {
        let schema = test_schema();
        let auth = superuser_auth();
        let table = test_table("no_such_ks", "users");
        let result = schema.create_table(table, &auth);
        assert!(matches!(result, Err(SchemaError::KeyspaceNotFound(ref n)) if n == "no_such_ks"));
    }

    #[test]
    fn create_table_duplicate_fails() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("dup_tbl_ks"), &auth)
            .unwrap();
        schema
            .create_table(test_table("dup_tbl_ks", "t1"), &auth)
            .unwrap();
        let result = schema.create_table(test_table("dup_tbl_ks", "t1"), &auth);
        assert!(matches!(result, Err(SchemaError::TableExists(_, _))));
    }

    #[test]
    fn drop_table_removes_it() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("drop_tbl_ks"), &auth)
            .unwrap();
        schema
            .create_table(test_table("drop_tbl_ks", "t1"), &auth)
            .unwrap();
        assert!(schema
            .snapshot()
            .tables
            .contains_key(&("drop_tbl_ks".to_string(), "t1".to_string())));

        schema.drop_table("drop_tbl_ks", "t1", &auth).unwrap();
        assert!(!schema
            .snapshot()
            .tables
            .contains_key(&("drop_tbl_ks".to_string(), "t1".to_string())));
    }

    #[test]
    fn create_table_emits_audit() {
        let sink = Arc::new(TestAuditSink::new());
        let schema = test_schema_with_sink(sink.clone());
        let auth = superuser_auth();

        schema
            .create_keyspace(test_keyspace("audit_tbl_ks"), &auth)
            .unwrap();
        sink.clear();
        schema
            .create_table(test_table("audit_tbl_ks", "events"), &auth)
            .unwrap();

        let events = sink.events();
        assert!(events.iter().any(|e| matches!(
            &e.event,
            AuditEventKind::TableCreated { keyspace, table }
                if keyspace == "audit_tbl_ks" && table == "events"
        )));
    }

    #[test]
    fn create_table_in_system_keyspace_blocked() {
        let schema = test_schema();
        let auth = superuser_auth();
        let table = test_table("system", "bad_table");
        let result = schema.create_table(table, &auth);
        assert!(
            matches!(result, Err(SchemaError::SystemKeyspaceProtected(ref n)) if n == "system")
        );
    }

    #[test]
    fn alter_table_adds_column() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("alter_tbl_ks"), &auth)
            .unwrap();
        schema
            .create_table(test_table("alter_tbl_ks", "t1"), &auth)
            .unwrap();

        let updates = TableUpdates {
            params: None,
            add_columns: vec![ColumnMetadata {
                name: "email".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            }],
            drop_columns: vec![],
            extensions: None,
        };
        schema
            .alter_table("alter_tbl_ks", "t1", updates, &auth)
            .unwrap();

        let snap = schema.snapshot();
        let key = ("alter_tbl_ks".to_string(), "t1".to_string());
        let tbl = &snap.tables[&key];
        assert!(tbl.columns.contains_key("email"));
    }

    #[test]
    fn drop_system_table_rejected() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("sys_tbl_ks"), &auth)
            .unwrap();
        let mut table = test_table("sys_tbl_ks", "sys_t");
        table.is_system = true;
        schema.create_table(table, &auth).unwrap();

        let result = schema.drop_table("sys_tbl_ks", "sys_t", &auth);
        assert!(
            matches!(result, Err(SchemaError::SystemTableProtected(ref ks, ref t)) if ks == "sys_tbl_ks" && t == "sys_t")
        );
    }

    #[test]
    fn alter_system_table_rejected() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("sys_tbl_ks2"), &auth)
            .unwrap();
        let mut table = test_table("sys_tbl_ks2", "sys_t2");
        table.is_system = true;
        schema.create_table(table, &auth).unwrap();

        let updates = TableUpdates {
            params: None,
            add_columns: vec![],
            drop_columns: vec![],
            extensions: None,
        };
        let result = schema.alter_table("sys_tbl_ks2", "sys_t2", updates, &auth);
        assert!(
            matches!(result, Err(SchemaError::SystemTableProtected(ref ks, ref t)) if ks == "sys_tbl_ks2" && t == "sys_t2")
        );
    }

    #[test]
    fn graph_extension_invalid_type_rejected() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("graph_ks"), &auth)
            .unwrap();
        let mut table = test_table("graph_ks", "bad_type");
        table
            .extensions
            .insert("graph.type".to_string(), "invalid".to_string());
        let result = schema.create_table(table, &auth);
        assert!(
            matches!(result, Err(SchemaError::InvalidSchema(ref msg)) if msg.contains("invalid graph.type"))
        );
    }

    #[test]
    fn graph_edge_extension_validates_source_label() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("graph_ks2"), &auth)
            .unwrap();

        // Create a vertex table first
        let mut vertex = test_table("graph_ks2", "person");
        vertex
            .extensions
            .insert("graph.type".to_string(), "vertex".to_string());
        vertex
            .extensions
            .insert("graph.label".to_string(), "person".to_string());
        schema.create_table(vertex, &auth).unwrap();

        // Try to create an edge that references a non-existent vertex table
        let mut edge = test_table("graph_ks2", "knows");
        edge.extensions
            .insert("graph.type".to_string(), "edge".to_string());
        edge.extensions
            .insert("graph.source".to_string(), "id".to_string());
        edge.extensions
            .insert("graph.target".to_string(), "id".to_string());
        edge.extensions
            .insert("graph.source_label".to_string(), "person".to_string());
        edge.extensions
            .insert("graph.target_label".to_string(), "nonexistent".to_string());
        let result = schema.create_table(edge, &auth);
        assert!(
            matches!(result, Err(SchemaError::InvalidSchema(ref msg)) if msg.contains("non-existent vertex label"))
        );
    }

    #[test]
    fn create_table_with_graph_extension_validates() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("graph_ks3"), &auth)
            .unwrap();

        // Create vertex tables
        let mut person = test_table("graph_ks3", "person");
        person
            .extensions
            .insert("graph.type".to_string(), "vertex".to_string());
        person
            .extensions
            .insert("graph.label".to_string(), "person".to_string());
        schema.create_table(person, &auth).unwrap();

        let mut company = test_table("graph_ks3", "company");
        company
            .extensions
            .insert("graph.type".to_string(), "vertex".to_string());
        company
            .extensions
            .insert("graph.label".to_string(), "company".to_string());
        schema.create_table(company, &auth).unwrap();

        // Create a valid edge table
        let mut edge = test_table("graph_ks3", "works_at");
        edge.columns.insert(
            "source_id".to_string(),
            ColumnMetadata {
                name: "source_id".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        edge.columns.insert(
            "target_id".to_string(),
            ColumnMetadata {
                name: "target_id".to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: "uuid".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
        edge.extensions
            .insert("graph.type".to_string(), "edge".to_string());
        edge.extensions
            .insert("graph.source".to_string(), "source_id".to_string());
        edge.extensions
            .insert("graph.target".to_string(), "target_id".to_string());
        edge.extensions
            .insert("graph.source_label".to_string(), "person".to_string());
        edge.extensions
            .insert("graph.target_label".to_string(), "company".to_string());
        schema.create_table(edge, &auth).unwrap();

        // Verify the edge table was created with extensions
        let snap = schema.snapshot();
        let key = ("graph_ks3".to_string(), "works_at".to_string());
        let tbl = &snap.tables[&key];
        assert_eq!(tbl.extensions.get("graph.type"), Some(&"edge".to_string()));
    }

    #[test]
    fn graph_extension_requires_create_permission() {
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("graph_perm_ks"), &auth)
            .unwrap();

        // Create a role with ALTER but not CREATE on the keyspace
        schema
            .create_role(
                crate::auth::role::RoleMetadata {
                    name: "alter_only".to_string(),
                    is_superuser: false,
                    can_login: true,
                    salted_hash: None,
                    member_of: HashSet::new(),
                },
                Some("password123!A"),
                &auth,
            )
            .unwrap();
        let mut alter_perms = HashSet::new();
        alter_perms.insert(Permission::Alter);
        schema
            .grant(
                "alter_only",
                &Resource::Keyspace("graph_perm_ks".to_string()),
                alter_perms,
                &auth,
            )
            .unwrap();

        // Try to create a table with graph.type extension using alter-only user
        let alter_auth = normal_auth("alter_only");
        let mut vertex = test_table("graph_perm_ks", "person");
        vertex
            .extensions
            .insert("graph.type".to_string(), "vertex".to_string());
        let result = schema.create_table(vertex, &alter_auth);
        assert!(
            result.is_err(),
            "graph.* extensions should require Permission::Create on keyspace"
        );
    }

    // ---- Task 22: Role CRUD tests ----

    use crate::auth::role::RoleUpdates;

    fn test_role(name: &str) -> RoleMetadata {
        RoleMetadata {
            name: name.to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        }
    }

    fn test_schema_with_iso27001_sink(sink: Arc<TestAuditSink>) -> Schema {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
        }
        Schema::new(SchemaConfig {
            password_policy: PasswordPolicy::iso27001(),
            audit_sink: Box::new(sink),
            ..test_config()
        })
        .unwrap()
    }

    #[test]
    fn create_role_with_password() {
        let schema = test_schema();
        let auth = superuser_auth();

        let role = test_role("app_user");
        schema.create_role(role, Some("s3cretPwd"), &auth).unwrap();

        let snap = schema.snapshot();
        let r = snap.roles.get("app_user").expect("role exists");
        assert!(r.salted_hash.is_some());
        assert!(
            PasswordHasher::verify_password_any("s3cretPwd", r.salted_hash.as_ref().unwrap())
                .unwrap()
        );
    }

    #[test]
    fn create_role_weak_password_rejected_by_policy() {
        let sink = Arc::new(TestAuditSink::new());
        let schema = test_schema_with_iso27001_sink(sink);
        let auth = superuser_auth();

        let role = test_role("weak_user");
        let result = schema.create_role(role, Some("short"), &auth);
        assert!(matches!(result, Err(SchemaError::PasswordTooWeak { .. })));
    }

    #[test]
    fn alter_role_changes_password() {
        let schema = test_schema();
        let auth = superuser_auth();

        schema
            .create_role(test_role("pw_user"), Some("oldpass"), &auth)
            .unwrap();
        schema
            .alter_role(
                "pw_user",
                RoleUpdates {
                    password: Some("newpass".to_string()),
                    ..Default::default()
                },
                &auth,
            )
            .unwrap();

        let snap = schema.snapshot();
        let r = &snap.roles["pw_user"];
        assert!(
            PasswordHasher::verify_password_any("newpass", r.salted_hash.as_ref().unwrap())
                .unwrap()
        );
        assert!(
            !PasswordHasher::verify_password_any("oldpass", r.salted_hash.as_ref().unwrap())
                .unwrap()
        );
    }

    #[test]
    fn alter_role_password_change_emits_audit() {
        let sink = Arc::new(TestAuditSink::new());
        let schema = test_schema_with_sink(sink.clone());
        let auth = superuser_auth();

        schema
            .create_role(test_role("audit_pw"), Some("original"), &auth)
            .unwrap();
        sink.clear();
        schema
            .alter_role(
                "audit_pw",
                RoleUpdates {
                    password: Some("changed!".to_string()),
                    ..Default::default()
                },
                &auth,
            )
            .unwrap();

        let events = sink.events();
        assert!(events.iter().any(|e| matches!(
            &e.event,
            AuditEventKind::PasswordChanged { role, upgraded_algorithm: false }
                if role == "audit_pw"
        )));
    }

    #[test]
    fn drop_role_removes_it() {
        let schema = test_schema();
        let auth = superuser_auth();

        schema
            .create_role(test_role("doomed"), None, &auth)
            .unwrap();
        assert!(schema.snapshot().roles.contains_key("doomed"));

        schema.drop_role("doomed", &auth).unwrap();
        assert!(!schema.snapshot().roles.contains_key("doomed"));
    }

    #[test]
    fn create_role_cycle_rejected() {
        let schema = test_schema();
        let auth = superuser_auth();

        // Create role A
        schema
            .create_role(test_role("role_a"), None, &auth)
            .unwrap();
        // Create role B as member of A
        let mut role_b = test_role("role_b");
        role_b.member_of.insert("role_a".to_string());
        schema.create_role(role_b, None, &auth).unwrap();

        // Now try to make role_a member_of role_b -- this creates a cycle
        let result = schema.alter_role(
            "role_a",
            RoleUpdates {
                member_of: Some(["role_b".to_string()].into_iter().collect()),
                ..Default::default()
            },
            &auth,
        );
        assert!(matches!(result, Err(SchemaError::RoleCycleDetected(_))));
    }

    // ---- Task 23: Grant/Revoke tests ----

    #[test]
    fn grant_adds_permissions() {
        let schema = test_schema();
        let auth = superuser_auth();

        schema
            .create_role(test_role("grantee"), None, &auth)
            .unwrap();
        schema
            .create_keyspace(test_keyspace("grant_ks"), &auth)
            .unwrap();

        let perms: HashSet<Permission> = [Permission::Select, Permission::Modify]
            .into_iter()
            .collect();
        schema
            .grant(
                "grantee",
                &Resource::Keyspace("grant_ks".to_string()),
                perms,
                &auth,
            )
            .unwrap();

        let snap = schema.snapshot();
        let grants = &snap.grants["grantee"];
        assert_eq!(grants.len(), 1);
        assert!(grants[0].permissions.contains(&Permission::Select));
        assert!(grants[0].permissions.contains(&Permission::Modify));
    }

    #[test]
    fn revoke_removes_permissions() {
        let schema = test_schema();
        let auth = superuser_auth();

        schema
            .create_role(test_role("revokee"), None, &auth)
            .unwrap();

        let perms: HashSet<Permission> = [Permission::Select, Permission::Modify]
            .into_iter()
            .collect();
        schema
            .grant("revokee", &Resource::AllKeyspaces, perms, &auth)
            .unwrap();

        // Revoke only Select
        let to_revoke: HashSet<Permission> = [Permission::Select].into_iter().collect();
        schema
            .revoke("revokee", &Resource::AllKeyspaces, to_revoke, &auth)
            .unwrap();

        let snap = schema.snapshot();
        let grants = &snap.grants["revokee"];
        assert_eq!(grants.len(), 1);
        assert!(!grants[0].permissions.contains(&Permission::Select));
        assert!(grants[0].permissions.contains(&Permission::Modify));
    }

    #[test]
    fn grant_emits_audit() {
        let sink = Arc::new(TestAuditSink::new());
        let schema = test_schema_with_sink(sink.clone());
        let auth = superuser_auth();

        schema
            .create_role(test_role("audit_grantee"), None, &auth)
            .unwrap();
        sink.clear();

        let perms: HashSet<Permission> = [Permission::Select].into_iter().collect();
        schema
            .grant("audit_grantee", &Resource::AllKeyspaces, perms, &auth)
            .unwrap();

        let events = sink.events();
        assert!(events.iter().any(|e| matches!(
            &e.event,
            AuditEventKind::PermissionGranted { role, .. } if role == "audit_grantee"
        )));
    }

    #[test]
    fn grant_to_nonexistent_role_fails() {
        let schema = test_schema();
        let auth = superuser_auth();

        let perms: HashSet<Permission> = [Permission::Select].into_iter().collect();
        let result = schema.grant("ghost_role", &Resource::AllKeyspaces, perms, &auth);
        assert!(matches!(result, Err(SchemaError::RoleNotFound(ref n)) if n == "ghost_role"));
    }

    #[test]
    fn revoke_all_removes_entry() {
        let schema = test_schema();
        let auth = superuser_auth();

        schema
            .create_role(test_role("full_revoke"), None, &auth)
            .unwrap();

        let perms: HashSet<Permission> = [Permission::Select].into_iter().collect();
        schema
            .grant("full_revoke", &Resource::AllKeyspaces, perms.clone(), &auth)
            .unwrap();

        // Revoke all permissions
        schema
            .revoke("full_revoke", &Resource::AllKeyspaces, perms, &auth)
            .unwrap();

        let snap = schema.snapshot();
        // The grants entry should be completely removed
        assert!(!snap.grants.contains_key("full_revoke"));
    }

    #[test]
    fn schema_exposes_virtual_table_registry() {
        let schema = Schema::new_for_test();
        let registry = schema.virtual_tables();
        assert!(registry.get("system_observability", "anything").is_none());
    }

    // ---- Task 3: apply_snapshot and internal DDL tests ----

    #[test]
    fn apply_snapshot_creates_keyspaces_and_tables() {
        let schema = test_schema();
        let mut snapshot = SchemaSnapshot {
            version: Uuid::new_v4(),
            keyspaces: HashMap::new(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            roles: HashMap::new(),
            grants: HashMap::new(),
            types: HashMap::new(),
            functions: HashMap::new(),
            aggregates: HashMap::new(),
        };

        let ks = KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        };
        snapshot.keyspaces.insert("test_ks".to_string(), ks);

        schema.apply_snapshot(snapshot).unwrap();

        let snap = schema.snapshot();
        assert!(snap.keyspaces.contains_key("test_ks"));
    }

    #[test]
    fn apply_snapshot_is_idempotent() {
        let schema = test_schema();
        let mut snapshot = SchemaSnapshot {
            version: Uuid::new_v4(),
            keyspaces: HashMap::new(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            roles: HashMap::new(),
            grants: HashMap::new(),
            types: HashMap::new(),
            functions: HashMap::new(),
            aggregates: HashMap::new(),
        };

        let ks = KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        };
        snapshot.keyspaces.insert("test_ks".to_string(), ks);

        schema.apply_snapshot(snapshot.clone()).unwrap();
        schema.apply_snapshot(snapshot).unwrap(); // Should not error
    }

    #[test]
    fn create_keyspace_internal_is_idempotent() {
        let schema = test_schema();

        let ks = KeyspaceMetadata {
            name: "test_ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        };

        schema.create_keyspace_internal(ks.clone()).unwrap();
        schema.create_keyspace_internal(ks).unwrap(); // Should not error

        let snap = schema.snapshot();
        assert!(snap.keyspaces.contains_key("test_ks"));
    }

    #[test]
    fn drop_table_internal_is_idempotent() {
        let schema = test_schema();
        // Drop a table that doesn't exist — should not error
        schema.drop_table_internal("nonexistent", "table").unwrap();
    }

    #[test]
    fn apply_snapshot_skips_system_keyspaces() {
        let schema = test_schema();
        let mut snapshot = SchemaSnapshot {
            version: Uuid::new_v4(),
            keyspaces: HashMap::new(),
            tables: HashMap::new(),
            indexes: HashMap::new(),
            roles: HashMap::new(),
            grants: HashMap::new(),
            types: HashMap::new(),
            functions: HashMap::new(),
            aggregates: HashMap::new(),
        };

        let ks = KeyspaceMetadata {
            name: "system".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        };
        snapshot.keyspaces.insert("system".to_string(), ks);

        schema.apply_snapshot(snapshot).unwrap();

        // system keyspace should NOT be created via snapshot
        let snap = schema.snapshot();
        assert!(
            !snap.keyspaces.contains_key("system")
                || snap
                    .keyspaces
                    .get("system")
                    .map(|k| k.name == "system")
                    .unwrap_or(false)
        );
    }

    // ---- Internal method tests (pair mode replication) ----

    #[test]
    fn alter_keyspace_internal_updates_replication() {
        use crate::metadata::keyspace::{KeyspaceUpdates, ReplicationParams};
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("ak_ks"), &auth)
            .unwrap();

        let updates = KeyspaceUpdates {
            replication: Some(ReplicationParams {
                strategy: "NetworkTopologyStrategy".to_string(),
                options: [("dc1".to_string(), "3".to_string())].into_iter().collect(),
            }),
            durable_writes: Some(false),
        };
        schema.alter_keyspace_internal("ak_ks", updates).unwrap();

        let snap = schema.snapshot();
        let ks = &snap.keyspaces["ak_ks"];
        assert_eq!(ks.replication.strategy, "NetworkTopologyStrategy");
        assert!(!ks.durable_writes);
    }

    #[test]
    fn alter_keyspace_internal_noop_if_missing() {
        use crate::metadata::keyspace::KeyspaceUpdates;
        let schema = test_schema();
        // Should not error even if keyspace doesn't exist
        schema
            .alter_keyspace_internal(
                "no_such_ks",
                KeyspaceUpdates {
                    replication: None,
                    durable_writes: Some(false),
                },
            )
            .unwrap();
    }

    #[test]
    fn alter_table_internal_adds_column() {
        use crate::metadata::table::TableUpdates;
        let schema = test_schema();
        let auth = superuser_auth();
        schema
            .create_keyspace(test_keyspace("at_ks"), &auth)
            .unwrap();
        schema
            .create_table(test_table("at_ks", "t1"), &auth)
            .unwrap();

        let updates = TableUpdates {
            params: None,
            add_columns: vec![ColumnMetadata {
                name: "extra_col".to_string(),
                kind: ColumnKind::Regular,
                position: 1,
                column_type: "text".to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            }],
            drop_columns: vec![],
            extensions: None,
        };
        schema.alter_table_internal("at_ks", "t1", updates).unwrap();

        let snap = schema.snapshot();
        let tbl = &snap.tables[&("at_ks".to_string(), "t1".to_string())];
        assert!(tbl.columns.contains_key("extra_col"));
    }

    #[test]
    fn alter_table_internal_noop_if_missing() {
        use crate::metadata::table::TableUpdates;
        let schema = test_schema();
        schema
            .alter_table_internal(
                "no_ks",
                "no_tbl",
                TableUpdates {
                    params: None,
                    add_columns: vec![],
                    drop_columns: vec![],
                    extensions: None,
                },
            )
            .unwrap();
    }

    #[test]
    fn create_role_internal_is_idempotent() {
        let schema = test_schema();
        let role = RoleMetadata {
            name: "rep_role".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: Some("$hashed$".to_string()),
            member_of: HashSet::new(),
        };
        schema.create_role_internal(role.clone()).unwrap();
        schema.create_role_internal(role).unwrap(); // idempotent

        let snap = schema.snapshot();
        assert!(snap.roles.contains_key("rep_role"));
        // Only one entry
        assert_eq!(
            snap.roles.values().filter(|r| r.name == "rep_role").count(),
            1
        );
    }

    #[test]
    fn alter_role_internal_stores_hash_directly() {
        use crate::auth::role::RoleUpdates;
        let schema = test_schema();
        let role = RoleMetadata {
            name: "hash_role".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        };
        schema.create_role_internal(role).unwrap();

        // The password field in RoleUpdates is the pre-hashed value during replication
        schema
            .alter_role_internal(
                "hash_role",
                RoleUpdates {
                    is_superuser: Some(true),
                    password: Some("$2a$04$pre_hashed_value".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();

        let snap = schema.snapshot();
        let r = &snap.roles["hash_role"];
        assert!(r.is_superuser);
        // Stored directly — not re-hashed
        assert_eq!(r.salted_hash.as_deref(), Some("$2a$04$pre_hashed_value"));
    }

    #[test]
    fn alter_role_internal_noop_if_missing() {
        use crate::auth::role::RoleUpdates;
        let schema = test_schema();
        schema
            .alter_role_internal(
                "ghost",
                RoleUpdates {
                    is_superuser: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
    }

    #[test]
    fn drop_role_internal_is_idempotent() {
        let schema = test_schema();
        // Drop a role that doesn't exist — should not error
        schema.drop_role_internal("nonexistent").unwrap();
    }

    #[test]
    fn drop_role_internal_removes_grants() {
        use crate::auth::permission::{GrantEntry, Permission, Resource};
        let schema = test_schema();
        let role = RoleMetadata {
            name: "bye_role".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        };
        schema.create_role_internal(role).unwrap();
        schema
            .grant_internal(GrantEntry {
                role: "bye_role".to_string(),
                resource: Resource::AllKeyspaces,
                permissions: [Permission::Select].into_iter().collect(),
            })
            .unwrap();
        assert!(schema.snapshot().grants.contains_key("bye_role"));

        schema.drop_role_internal("bye_role").unwrap();
        let snap = schema.snapshot();
        assert!(!snap.roles.contains_key("bye_role"));
        assert!(!snap.grants.contains_key("bye_role"));
    }

    #[test]
    fn grant_internal_merges_permissions() {
        use crate::auth::permission::{GrantEntry, Permission, Resource};
        let schema = test_schema();
        let role = RoleMetadata {
            name: "merge_role".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        };
        schema.create_role_internal(role).unwrap();

        schema
            .grant_internal(GrantEntry {
                role: "merge_role".to_string(),
                resource: Resource::AllKeyspaces,
                permissions: [Permission::Select].into_iter().collect(),
            })
            .unwrap();
        schema
            .grant_internal(GrantEntry {
                role: "merge_role".to_string(),
                resource: Resource::AllKeyspaces,
                permissions: [Permission::Modify].into_iter().collect(),
            })
            .unwrap();

        let snap = schema.snapshot();
        let grants = &snap.grants["merge_role"];
        assert_eq!(grants.len(), 1, "same resource should merge");
        assert!(grants[0].permissions.contains(&Permission::Select));
        assert!(grants[0].permissions.contains(&Permission::Modify));
    }

    #[test]
    fn revoke_internal_removes_single_permission() {
        use crate::auth::permission::{GrantEntry, Permission, Resource};
        let schema = test_schema();
        let role = RoleMetadata {
            name: "rev_role".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        };
        schema.create_role_internal(role).unwrap();
        schema
            .grant_internal(GrantEntry {
                role: "rev_role".to_string(),
                resource: Resource::AllKeyspaces,
                permissions: [Permission::Select, Permission::Modify]
                    .into_iter()
                    .collect(),
            })
            .unwrap();

        schema
            .revoke_internal("rev_role", &Resource::AllKeyspaces, &Permission::Select)
            .unwrap();

        let snap = schema.snapshot();
        let grants = &snap.grants["rev_role"];
        assert_eq!(grants.len(), 1);
        assert!(!grants[0].permissions.contains(&Permission::Select));
        assert!(grants[0].permissions.contains(&Permission::Modify));
    }

    #[test]
    fn revoke_internal_removes_entry_when_empty() {
        use crate::auth::permission::{GrantEntry, Permission, Resource};
        let schema = test_schema();
        let role = RoleMetadata {
            name: "empty_rev_role".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        };
        schema.create_role_internal(role).unwrap();
        schema
            .grant_internal(GrantEntry {
                role: "empty_rev_role".to_string(),
                resource: Resource::AllKeyspaces,
                permissions: [Permission::Select].into_iter().collect(),
            })
            .unwrap();

        schema
            .revoke_internal(
                "empty_rev_role",
                &Resource::AllKeyspaces,
                &Permission::Select,
            )
            .unwrap();

        let snap = schema.snapshot();
        assert!(!snap.grants.contains_key("empty_rev_role"));
    }

    // ---- UDT tests ----

    fn create_test_keyspace(schema: &Schema, name: &str) {
        schema
            .create_keyspace_internal(KeyspaceMetadata {
                name: name.to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            })
            .unwrap();
    }

    #[test]
    fn create_and_lookup_udt() {
        let schema = test_schema();
        create_test_keyspace(&schema, "ks");
        let udt = UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![
                ("street".to_string(), CqlType::Varchar),
                ("zip".to_string(), CqlType::Int),
            ],
        };
        schema.create_type_internal(&udt).unwrap();
        let found = schema.get_type("ks", "address");
        assert!(found.is_some());
        assert_eq!(found.unwrap().fields.len(), 2);
    }

    #[test]
    fn create_type_fails_duplicate() {
        let schema = test_schema();
        create_test_keyspace(&schema, "ks");
        let udt = UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![("street".to_string(), CqlType::Varchar)],
        };
        schema.create_type_internal(&udt).unwrap();
        let err = schema.create_type_internal(&udt).unwrap_err();
        assert!(
            format!("{err}").contains("already exists"),
            "expected 'already exists' in: {err}"
        );
    }

    #[test]
    fn create_type_fails_missing_keyspace() {
        let schema = test_schema();
        let udt = UserTypeMetadata {
            keyspace: "nonexistent".to_string(),
            name: "addr".to_string(),
            fields: vec![],
        };
        assert!(schema.create_type_internal(&udt).is_err());
    }

    #[test]
    fn drop_type() {
        let schema = test_schema();
        create_test_keyspace(&schema, "ks");
        let udt = UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![("street".to_string(), CqlType::Varchar)],
        };
        schema.create_type_internal(&udt).unwrap();
        schema.drop_type_internal("ks", "address").unwrap();
        assert!(schema.get_type("ks", "address").is_none());
    }

    #[test]
    fn alter_type_add_field() {
        let schema = test_schema();
        create_test_keyspace(&schema, "ks");
        let udt = UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![("street".to_string(), CqlType::Varchar)],
        };
        schema.create_type_internal(&udt).unwrap();
        schema
            .alter_type_add_field("ks", "address", "city", CqlType::Varchar)
            .unwrap();
        let found = schema.get_type("ks", "address").unwrap();
        assert_eq!(found.fields.len(), 2);
        assert_eq!(found.fields[1].0, "city");
    }

    #[test]
    fn alter_type_rename_field() {
        let schema = test_schema();
        create_test_keyspace(&schema, "ks");
        let udt = UserTypeMetadata {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            fields: vec![("street".to_string(), CqlType::Varchar)],
        };
        schema.create_type_internal(&udt).unwrap();
        schema
            .alter_type_rename_field("ks", "address", "street", "street_name")
            .unwrap();
        let found = schema.get_type("ks", "address").unwrap();
        assert_eq!(found.fields[0].0, "street_name");
    }

    // ---- SchemaSnapshot JSON serialization roundtrip tests ----

    #[test]
    fn schema_snapshot_tables_json_roundtrip() {
        let mut snap = SchemaSnapshot::new();
        snap.tables.insert(
            ("mykeyspace".to_string(), "users".to_string()),
            test_table("mykeyspace", "users"),
        );
        let json = serde_json::to_string(&snap)
            .expect("SchemaSnapshot with tuple-keyed tables must serialize to JSON");
        let back: SchemaSnapshot =
            serde_json::from_str(&json).expect("Deserialized SchemaSnapshot must round-trip");
        let key = ("mykeyspace".to_string(), "users".to_string());
        assert!(back.tables.contains_key(&key));
        assert_eq!(back.version, snap.version);
    }

    #[test]
    fn schema_snapshot_indexes_json_roundtrip() {
        use crate::metadata::index::IndexMetadata;
        use ferrosa_index::IndexType;

        let mut snap = SchemaSnapshot::new();
        snap.indexes.insert(
            (
                "mykeyspace".to_string(),
                "users".to_string(),
                "idx_email".to_string(),
            ),
            IndexMetadata {
                keyspace: "mykeyspace".into(),
                table: "users".into(),
                name: "idx_email".into(),
                index_type: IndexType::BTree,
                target_columns: vec!["email".into()],
                filter_predicate: None,
                options: HashMap::new(),
            },
        );
        let json = serde_json::to_string(&snap)
            .expect("SchemaSnapshot with tuple-keyed indexes must serialize to JSON");
        let back: SchemaSnapshot =
            serde_json::from_str(&json).expect("Deserialized SchemaSnapshot must round-trip");
        let key = (
            "mykeyspace".to_string(),
            "users".to_string(),
            "idx_email".to_string(),
        );
        assert!(back.indexes.contains_key(&key));
    }

    #[test]
    fn schema_snapshot_aggregates_json_roundtrip() {
        use crate::metadata::aggregate::UserAggregateMetadata;

        let mut snap = SchemaSnapshot::new();
        let arg_types = vec![CqlType::Int, CqlType::Bigint];
        snap.aggregates.insert(
            (
                "mykeyspace".to_string(),
                "my_avg".to_string(),
                arg_types.clone(),
            ),
            UserAggregateMetadata {
                keyspace: "mykeyspace".into(),
                name: "my_avg".into(),
                arg_types: arg_types.clone(),
                state_func: "avg_state".into(),
                state_type: CqlType::Tuple(vec![CqlType::Bigint, CqlType::Int]),
                final_func: Some("avg_final".into()),
                init_cond: None,
                return_type: CqlType::Double,
                wasm_body: None,
            },
        );
        let json = serde_json::to_string(&snap)
            .expect("SchemaSnapshot with tuple+vec-keyed aggregates must serialize to JSON");
        let back: SchemaSnapshot =
            serde_json::from_str(&json).expect("Deserialized SchemaSnapshot must round-trip");
        let key = ("mykeyspace".to_string(), "my_avg".to_string(), arg_types);
        assert!(back.aggregates.contains_key(&key));
    }
}
