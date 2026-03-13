//! Audit event types for tracking schema and auth changes.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::SystemTime;

use serde::Serialize;
use uuid::Uuid;

use crate::auth::permission::{Permission, Resource};

/// A complete audit event capturing who did what, when, and from where.
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    /// When the event occurred.
    pub timestamp: SystemTime,
    /// What happened.
    pub event: AuditEventKind,
    /// The authenticated role that performed the action, if any.
    pub actor: Option<String>,
    /// The network address the request came from, if any.
    pub source: Option<SocketAddr>,
    /// The schema version at the time of the event, if applicable.
    pub schema_version: Option<Uuid>,
}

/// The specific kind of audit event.
///
/// `#[non_exhaustive]` allows adding new event kinds without semver breakage.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize)]
pub enum AuditEventKind {
    /// Successful authentication.
    AuthSuccess { role: String },
    /// Failed authentication attempt.
    AuthFailed { role: String },
    /// Authentication throttled due to rate limiting.
    AuthThrottled { role: String },
    /// Account locked out after too many failures.
    AuthLockedOut { role: String, failure_count: u32 },
    /// Password was changed (possibly with algorithm upgrade).
    PasswordChanged {
        role: String,
        upgraded_algorithm: bool,
    },
    /// A keyspace was created.
    KeyspaceCreated { keyspace: String },
    /// A keyspace was altered.
    KeyspaceAltered { keyspace: String },
    /// A keyspace was dropped.
    KeyspaceDropped { keyspace: String },
    /// A table was created.
    TableCreated { keyspace: String, table: String },
    /// A table was altered.
    TableAltered { keyspace: String, table: String },
    /// A table was dropped.
    TableDropped { keyspace: String, table: String },
    /// A role was created.
    RoleCreated { role: String, is_superuser: bool },
    /// A role was altered.
    RoleAltered { role: String },
    /// A role was dropped.
    RoleDropped { role: String },
    /// Permissions were granted.
    PermissionGranted {
        role: String,
        resource: Resource,
        permissions: HashSet<Permission>,
    },
    /// Permissions were revoked.
    PermissionRevoked {
        role: String,
        resource: Resource,
        permissions: HashSet<Permission>,
    },
    /// Schema was bootstrapped (initial setup).
    SchemaBootstrapped,
    /// Superuser password must be changed from the default.
    SuperuserPasswordMustChange,
    /// A graph query (read) was executed.
    GraphQueryExecuted {
        query: String,
        keyspace: String,
        rows_returned: usize,
        execution_ms: u64,
        status: GraphAuditStatus,
    },
    /// A graph mutation (write) was executed.
    GraphMutationExecuted {
        query: String,
        keyspace: String,
        vertices_affected: usize,
        edges_affected: usize,
        status: GraphAuditStatus,
    },
}

/// Status of a graph query or mutation for audit purposes.
#[derive(Debug, Clone, Serialize)]
pub enum GraphAuditStatus {
    /// The operation completed successfully.
    Ok,
    /// The operation timed out.
    Timeout,
    /// The operation was denied (authorization failure).
    Denied,
    /// The operation failed with an error.
    Error,
}

/// Context for audit event creation, carrying request metadata.
#[derive(Debug, Clone, Default)]
pub struct AuditContext {
    /// The source address of the request, if known.
    pub source: Option<SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::permission::{Permission, Resource};
    use std::collections::HashSet;

    use std::time::SystemTime;
    use uuid::Uuid;

    #[test]
    fn audit_event_construction() {
        let event = AuditEvent {
            timestamp: SystemTime::now(),
            event: AuditEventKind::AuthSuccess {
                role: "admin".to_string(),
            },
            actor: Some("admin".to_string()),
            source: Some("127.0.0.1:9042".parse().unwrap()),
            schema_version: Some(Uuid::new_v4()),
        };

        assert!(event.actor.is_some());
        assert_eq!(event.actor.as_deref(), Some("admin"));
        assert!(event.source.is_some());
        assert!(event.schema_version.is_some());
    }

    #[test]
    fn audit_event_kind_all_variants_constructible() {
        let mut perms = HashSet::new();
        perms.insert(Permission::Select);
        perms.insert(Permission::Modify);

        let variants: Vec<AuditEventKind> = vec![
            AuditEventKind::AuthSuccess { role: "r".into() },
            AuditEventKind::AuthFailed { role: "r".into() },
            AuditEventKind::AuthThrottled { role: "r".into() },
            AuditEventKind::AuthLockedOut {
                role: "r".into(),
                failure_count: 5,
            },
            AuditEventKind::PasswordChanged {
                role: "r".into(),
                upgraded_algorithm: true,
            },
            AuditEventKind::KeyspaceCreated {
                keyspace: "ks".into(),
            },
            AuditEventKind::KeyspaceAltered {
                keyspace: "ks".into(),
            },
            AuditEventKind::KeyspaceDropped {
                keyspace: "ks".into(),
            },
            AuditEventKind::TableCreated {
                keyspace: "ks".into(),
                table: "tbl".into(),
            },
            AuditEventKind::TableAltered {
                keyspace: "ks".into(),
                table: "tbl".into(),
            },
            AuditEventKind::TableDropped {
                keyspace: "ks".into(),
                table: "tbl".into(),
            },
            AuditEventKind::RoleCreated {
                role: "r".into(),
                is_superuser: true,
            },
            AuditEventKind::RoleAltered { role: "r".into() },
            AuditEventKind::RoleDropped { role: "r".into() },
            AuditEventKind::PermissionGranted {
                role: "r".into(),
                resource: Resource::AllKeyspaces,
                permissions: perms.clone(),
            },
            AuditEventKind::PermissionRevoked {
                role: "r".into(),
                resource: Resource::Table("ks".into(), "tbl".into()),
                permissions: perms,
            },
            AuditEventKind::SchemaBootstrapped,
            AuditEventKind::SuperuserPasswordMustChange,
            AuditEventKind::GraphQueryExecuted {
                query: "MATCH (n) RETURN n".into(),
                keyspace: "ks".into(),
                rows_returned: 42,
                execution_ms: 100,
                status: GraphAuditStatus::Ok,
            },
            AuditEventKind::GraphMutationExecuted {
                query: "CREATE (n:Person)".into(),
                keyspace: "ks".into(),
                vertices_affected: 1,
                edges_affected: 0,
                status: GraphAuditStatus::Ok,
            },
        ];

        assert_eq!(variants.len(), 20);
    }

    #[test]
    fn graph_audit_event_variants_constructible() {
        let query_event = AuditEventKind::GraphQueryExecuted {
            query: "MATCH (n:Person) RETURN n".into(),
            keyspace: "social".into(),
            rows_returned: 10,
            execution_ms: 50,
            status: GraphAuditStatus::Ok,
        };
        // Verify it can be debug-printed (ensures Debug is derived)
        let _ = format!("{:?}", query_event);

        let mutation_event = AuditEventKind::GraphMutationExecuted {
            query: "CREATE (n:Person {name: 'Alice'})".into(),
            keyspace: "social".into(),
            vertices_affected: 1,
            edges_affected: 0,
            status: GraphAuditStatus::Error,
        };
        let _ = format!("{:?}", mutation_event);

        // Verify all GraphAuditStatus variants
        for status in &[
            GraphAuditStatus::Ok,
            GraphAuditStatus::Timeout,
            GraphAuditStatus::Denied,
            GraphAuditStatus::Error,
        ] {
            let _ = format!("{:?}", status);
        }
    }

    #[test]
    fn audit_event_serializes_to_json() {
        let event = AuditEvent {
            timestamp: SystemTime::UNIX_EPOCH,
            event: AuditEventKind::KeyspaceCreated {
                keyspace: "test_ks".to_string(),
            },
            actor: Some("admin".to_string()),
            source: None,
            schema_version: None,
        };

        let json = serde_json::to_string(&event).expect("should serialize to JSON");
        assert!(json.contains("test_ks"));
        assert!(json.contains("admin"));
    }

    #[test]
    fn audit_context_default_is_empty() {
        let ctx = AuditContext::default();
        assert!(ctx.source.is_none());
    }
}
