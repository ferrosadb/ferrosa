//! Schema registry: the central authority for schema state.
//!
//! Contains `SchemaSnapshot` (the immutable point-in-time view),
//! `SchemaConfig` (bootstrap configuration), and `AuthMethod`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::audit::AuditSink;
use crate::auth::password::{PasswordHasher, PasswordPolicy};
use crate::auth::permission::GrantEntry;
use crate::auth::rate_limit::RateLimitConfig;
use crate::auth::role::RoleMetadata;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::TestAuditSink;
    use crate::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    use crate::secrets::EnvSecretsProvider;

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
}
