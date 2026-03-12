//! Schema management for the Ferrosa distributed database.
//!
//! This crate is the authority for keyspaces, tables, columns, and roles.
//! Every mutating operation requires an `AuthContext` (ADR-006).
//! Every mutation emits an audit event (ADR-008).

pub mod audit;
pub mod auth;
pub mod convert;
pub mod error;
pub mod metadata;
pub mod registry;
pub mod secrets;
pub mod startup;
pub mod system;

pub use audit::{
    AuditContext, AuditEvent, AuditEventKind, AuditLogEntry, AuditSink, CompositeSink,
    LogAuditSink, SystemTableAuditSink, TestAuditSink,
};
pub use auth::{
    check_permission, AuthContext, AuthRateLimiter, GrantEntry, PasswordHasher, PasswordPolicy,
    Permission, RateLimitConfig, Resource, RoleMetadata, RoleUpdates,
};
pub use convert::cql_to_marshal_type;
pub use error::{Result, SchemaError};
pub use metadata::{
    CachingParams, ClusteringOrder, ColumnKind, ColumnMask, ColumnMetadata, KeyspaceMetadata,
    KeyspaceUpdates, ReplicationParams, TableFlag, TableMetadata, TableParams, TableUpdates,
};
pub use registry::{AuthMethod, Schema, SchemaConfig, SchemaSnapshot};
pub use secrets::{EnvSecretsProvider, SecretsError, SecretsProvider};
pub use startup::{
    validate_production_requirements, DeploymentMode, ProductionCheckConfig, ProductionViolation,
};
pub use system::auth_tables::{
    query_audit_log, query_role_members, query_role_permissions, query_roles, RoleMemberRow,
    RolePermissionRow, RoleRow,
};
pub use system::local::{query_local, LocalInfo, NodeConfig};
pub use system::peers::{query_peers, ClusterState, PeerInfo};
pub use system::schema_tables::{
    query_columns, query_keyspaces, query_tables, ColumnRow, KeyspaceRow, TableRow,
};
