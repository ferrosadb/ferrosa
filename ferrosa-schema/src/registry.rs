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
use crate::auth::permission::GrantEntry;
use crate::auth::rate_limit::{AuthRateLimiter, RateLimitConfig};
use crate::auth::role::{AuthContext, RoleMetadata};
use crate::error::SchemaError;
use crate::metadata::keyspace::KeyspaceMetadata;
use crate::metadata::table::TableMetadata;
use crate::secrets::SecretsProvider;
use crate::startup::DeploymentMode;

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
    pub tables: HashMap<(String, String), TableMetadata>,
    /// All roles, keyed by name.
    pub roles: HashMap<String, RoleMetadata>,
    /// All grants, keyed by role name.
    pub grants: HashMap<String, Vec<GrantEntry>>,
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
    /// Lock-free swappable snapshot.
    inner: ArcSwap<SchemaSnapshot>,
    /// Serializes writes (clone → mutate → store).
    write_lock: Mutex<()>,
    /// Password hashing configuration.
    hasher_config: PasswordHasher,
    /// Password strength policy (used by create_role/alter_role in CRUD operations).
    #[allow(dead_code)]
    password_policy: PasswordPolicy,
    /// Authentication rate limiter.
    rate_limiter: AuthRateLimiter,
    /// Audit event sink.
    audit_sink: Box<dyn AuditSink>,
    /// Roles whose password is the default and must be changed.
    default_password_roles: Mutex<HashSet<String>>,
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
            inner: ArcSwap::new(Arc::new(snapshot)),
            write_lock: Mutex::new(()),
            hasher_config: config.hasher,
            password_policy: config.password_policy,
            rate_limiter: AuthRateLimiter::new(config.rate_limit),
            audit_sink: config.audit_sink,
            default_password_roles: Mutex::new(default_password_roles),
        };

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
                        new_snap.version = Uuid::new_v4();
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
}
