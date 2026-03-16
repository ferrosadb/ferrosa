//! Error types for ferrosa-schema.

use std::fmt;

use crate::auth::permission::{Permission, Resource};

/// Errors that can occur in schema operations.
///
/// `#[non_exhaustive]` allows adding variants without semver breakage.
#[derive(Debug)]
#[non_exhaustive]
pub enum SchemaError {
    /// Keyspace already exists.
    KeyspaceExists(String),
    /// Keyspace not found.
    KeyspaceNotFound(String),
    /// Table already exists (keyspace, table).
    TableExists(String, String),
    /// Table not found (keyspace, table).
    TableNotFound(String, String),
    /// Role already exists.
    RoleExists(String),
    /// Role not found.
    RoleNotFound(String),
    /// Intentionally vague — does not reveal whether role exists,
    /// can't login, or has a bad password.
    AuthenticationFailed,
    /// Rate limited — too many failed attempts.
    AuthenticationThrottled,
    /// Insufficient permissions for the requested operation.
    PermissionDenied {
        role: String,
        permission: Permission,
        resource: Resource,
    },
    /// Cannot modify a system keyspace.
    SystemKeyspaceProtected(String),
    /// Granting a role would create a cycle.
    RoleCycleDetected(String),
    /// Password does not meet policy requirements.
    PasswordTooWeak { violations: Vec<String> },
    /// Cannot modify a system-managed table (keyspace, table).
    SystemTableProtected(String, String),
    /// User-defined type already exists (keyspace, type).
    TypeExists(String, String),
    /// User-defined type not found (keyspace, type).
    TypeNotFound(String, String),
    /// Field already exists in a user-defined type (keyspace, type, field).
    FieldExists(String, String, String),
    /// Field not found in a user-defined type (keyspace, type, field).
    FieldNotFound(String, String, String),
    /// User-defined function already exists (keyspace, function).
    FunctionExists(String, String),
    /// User-defined function not found (keyspace, function).
    FunctionNotFound(String, String),
    /// User-defined aggregate already exists (keyspace, aggregate).
    AggregateExists(String, String),
    /// User-defined aggregate not found (keyspace, aggregate).
    AggregateNotFound(String, String),
    /// Generic schema validation error.
    InvalidSchema(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyspaceExists(name) => write!(f, "keyspace already exists: {name}"),
            Self::KeyspaceNotFound(name) => write!(f, "keyspace not found: {name}"),
            Self::TableExists(ks, t) => write!(f, "table already exists: {ks}.{t}"),
            Self::TableNotFound(ks, t) => write!(f, "table not found: {ks}.{t}"),
            Self::RoleExists(name) => write!(f, "role already exists: {name}"),
            Self::RoleNotFound(name) => write!(f, "role not found: {name}"),
            Self::AuthenticationFailed => write!(f, "authentication failed"),
            Self::AuthenticationThrottled => {
                write!(f, "too many failed attempts, try again later")
            }
            Self::PermissionDenied {
                role,
                permission,
                resource,
            } => {
                write!(
                    f,
                    "permission denied: {role} lacks {permission} on {resource}"
                )
            }
            Self::SystemKeyspaceProtected(name) => {
                write!(f, "cannot modify system keyspace: {name}")
            }
            Self::RoleCycleDetected(name) => {
                write!(f, "role cycle detected involving: {name}")
            }
            Self::PasswordTooWeak { violations } => {
                write!(f, "password too weak: {}", violations.join(", "))
            }
            Self::SystemTableProtected(ks, t) => {
                write!(f, "cannot modify system table: {ks}.{t}")
            }
            Self::TypeExists(ks, t) => write!(f, "type already exists: {ks}.{t}"),
            Self::TypeNotFound(ks, t) => write!(f, "type not found: {ks}.{t}"),
            Self::FieldExists(ks, t, field) => {
                write!(f, "field already exists: {field} in type {ks}.{t}")
            }
            Self::FieldNotFound(ks, t, field) => {
                write!(f, "field not found: {field} in type {ks}.{t}")
            }
            Self::FunctionExists(ks, func) => {
                write!(f, "function already exists: {ks}.{func}")
            }
            Self::FunctionNotFound(ks, func) => {
                write!(f, "function not found: {ks}.{func}")
            }
            Self::AggregateExists(ks, agg) => {
                write!(f, "aggregate already exists: {ks}.{agg}")
            }
            Self::AggregateNotFound(ks, agg) => {
                write!(f, "aggregate not found: {ks}.{agg}")
            }
            Self::InvalidSchema(msg) => write!(f, "invalid schema: {msg}"),
        }
    }
}

impl std::error::Error for SchemaError {}

/// Result type alias for schema operations.
pub type Result<T> = std::result::Result<T, SchemaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyspace_exists_display() {
        let err = SchemaError::KeyspaceExists("ks1".to_string());
        assert!(err.to_string().contains("ks1"));
    }

    #[test]
    fn authentication_failed_is_vague() {
        let err = SchemaError::AuthenticationFailed;
        let msg = err.to_string();
        assert!(!msg.contains("not found"));
        assert!(!msg.contains("login"));
        assert!(!msg.contains("password"));
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<SchemaError>();
        assert_sync::<SchemaError>();
    }

    #[test]
    fn system_table_protected_display() {
        let err = SchemaError::SystemTableProtected("ks".into(), "t".into());
        assert_eq!(err.to_string(), "cannot modify system table: ks.t");
    }

    #[test]
    fn all_variants_display() {
        let errors = vec![
            SchemaError::KeyspaceExists("ks".into()),
            SchemaError::KeyspaceNotFound("ks".into()),
            SchemaError::TableExists("ks".into(), "t".into()),
            SchemaError::TableNotFound("ks".into(), "t".into()),
            SchemaError::RoleExists("r".into()),
            SchemaError::RoleNotFound("r".into()),
            SchemaError::AuthenticationFailed,
            SchemaError::AuthenticationThrottled,
            SchemaError::PermissionDenied {
                role: "r".into(),
                permission: Permission::Select,
                resource: Resource::Table("ks".into(), "t".into()),
            },
            SchemaError::SystemKeyspaceProtected("system".into()),
            SchemaError::RoleCycleDetected("r".into()),
            SchemaError::PasswordTooWeak {
                violations: vec!["too short".into()],
            },
            SchemaError::SystemTableProtected("ks".into(), "t".into()),
            SchemaError::TypeExists("ks".into(), "t".into()),
            SchemaError::TypeNotFound("ks".into(), "t".into()),
            SchemaError::FieldExists("ks".into(), "t".into(), "f".into()),
            SchemaError::FieldNotFound("ks".into(), "t".into(), "f".into()),
            SchemaError::FunctionExists("ks".into(), "f".into()),
            SchemaError::FunctionNotFound("ks".into(), "f".into()),
            SchemaError::AggregateExists("ks".into(), "a".into()),
            SchemaError::AggregateNotFound("ks".into(), "a".into()),
            SchemaError::InvalidSchema("bad".into()),
        ];
        for err in &errors {
            let _ = err.to_string(); // Should not panic
        }
    }
}
