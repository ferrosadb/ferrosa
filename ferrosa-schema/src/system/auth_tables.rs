//! `system_auth` query functions.
//!
//! Provides row types and query functions for `system_auth.roles`,
//! `system_auth.role_members`, `system_auth.role_permissions`, and
//! `system_auth.audit_log`.

use std::collections::HashSet;

use crate::audit::table_sink::{AuditLogEntry, SystemTableAuditSink};
use crate::auth::permission::{Permission, Resource};
use crate::auth::role::AuthContext;
use crate::error::SchemaError;
use crate::registry::SchemaSnapshot;

/// A row from `system_auth.roles`.
#[derive(Debug, Clone)]
pub struct RoleRow {
    /// Role name.
    pub role: String,
    /// Whether this role is a superuser.
    pub is_superuser: bool,
    /// Whether this role can login.
    pub can_login: bool,
    /// Salted password hash — filtered to `None` for non-superuser callers.
    pub salted_hash: Option<String>,
}

/// A row from `system_auth.role_members`.
#[derive(Debug, Clone)]
pub struct RoleMemberRow {
    /// The role that is a member of another role.
    pub role: String,
    /// The parent role that `role` is a member of.
    pub member: String,
}

/// A row from `system_auth.role_permissions`.
#[derive(Debug, Clone)]
pub struct RolePermissionRow {
    /// The role that has the permissions.
    pub role: String,
    /// The resource the permissions apply to.
    pub resource: String,
    /// The set of permission names.
    pub permissions: HashSet<String>,
}

/// Query `system_auth.roles` from a snapshot.
///
/// Non-superuser callers have `salted_hash` filtered to `None`
/// for all roles, to prevent password hash exposure.
pub fn query_roles(snap: &SchemaSnapshot, auth: &AuthContext) -> Vec<RoleRow> {
    snap.roles
        .values()
        .map(|r| RoleRow {
            role: r.name.clone(),
            is_superuser: r.is_superuser,
            can_login: r.can_login,
            salted_hash: if auth.is_superuser {
                r.salted_hash.clone()
            } else {
                None
            },
        })
        .collect()
}

/// Query `system_auth.role_members` from a snapshot.
///
/// Derives membership rows from each role's `member_of` set.
pub fn query_role_members(snap: &SchemaSnapshot) -> Vec<RoleMemberRow> {
    snap.roles
        .values()
        .flat_map(|r| {
            r.member_of.iter().map(move |parent| RoleMemberRow {
                role: r.name.clone(),
                member: parent.clone(),
            })
        })
        .collect()
}

/// Query `system_auth.role_permissions` from a snapshot.
///
/// Derives permission rows from the grants map.
pub fn query_role_permissions(snap: &SchemaSnapshot) -> Vec<RolePermissionRow> {
    snap.grants
        .values()
        .flatten()
        .map(|grant| RolePermissionRow {
            role: grant.role.clone(),
            resource: grant.resource.to_string(),
            permissions: grant.permissions.iter().map(|p| p.to_string()).collect(),
        })
        .collect()
}

/// Query `system_auth.audit_log`.
///
/// Requires superuser privileges. Returns an error if the caller
/// is not a superuser.
pub fn query_audit_log(
    sink: &SystemTableAuditSink,
    auth: &AuthContext,
) -> crate::Result<Vec<AuditLogEntry>> {
    if !auth.is_superuser {
        return Err(SchemaError::PermissionDenied {
            role: auth.role.clone(),
            permission: Permission::Select,
            resource: Resource::Table("system_auth".to_string(), "audit_log".to_string()),
        });
    }
    Ok(sink.query())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::permission::GrantEntry;
    use crate::auth::role::RoleMetadata;
    use crate::registry::SchemaSnapshot;

    fn snapshot_with_hashed_role(
        name: &str,
        is_superuser: bool,
        salted_hash: Option<String>,
    ) -> SchemaSnapshot {
        let mut snap = SchemaSnapshot::new();
        snap.roles.insert(
            name.to_string(),
            RoleMetadata {
                name: name.to_string(),
                is_superuser,
                can_login: true,
                salted_hash,
                member_of: HashSet::new(),
                scram: None,
            },
        );
        snap
    }

    #[test]
    fn query_roles_superuser_sees_hash() {
        let snap = snapshot_with_hashed_role("admin", true, Some("$2b$hash".into()));
        let auth = AuthContext {
            role: "admin".into(),
            is_superuser: true,
            must_change_password: false,
        };
        let rows = query_roles(&snap, &auth);
        let admin_row = rows.iter().find(|r| r.role == "admin").unwrap();
        assert!(admin_row.salted_hash.is_some());
    }

    #[test]
    fn query_roles_non_superuser_hash_filtered() {
        let snap = snapshot_with_hashed_role("user1", false, Some("$2b$hash".into()));
        let auth = AuthContext {
            role: "user1".into(),
            is_superuser: false,
            must_change_password: false,
        };
        let rows = query_roles(&snap, &auth);
        for row in &rows {
            assert!(
                row.salted_hash.is_none(),
                "non-superuser should not see hashes"
            );
        }
    }

    #[test]
    fn query_role_members_derives_from_member_of() {
        let mut snap = SchemaSnapshot::new();
        snap.roles.insert(
            "child".to_string(),
            RoleMetadata {
                name: "child".to_string(),
                is_superuser: false,
                can_login: true,
                salted_hash: None,
                member_of: ["parent".to_string()].into_iter().collect(),
                scram: None,
            },
        );
        let rows = query_role_members(&snap);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].role, "child");
        assert_eq!(rows[0].member, "parent");
    }

    #[test]
    fn query_role_permissions_from_grants() {
        let mut snap = SchemaSnapshot::new();
        snap.grants.insert(
            "reader".to_string(),
            vec![GrantEntry {
                role: "reader".to_string(),
                resource: Resource::Table("ks".to_string(), "t".to_string()),
                permissions: [Permission::Select].into_iter().collect(),
            }],
        );
        let rows = query_role_permissions(&snap);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].role, "reader");
        assert!(rows[0].permissions.contains("SELECT"));
    }

    #[test]
    fn query_audit_log_requires_superuser() {
        let sink = SystemTableAuditSink::new(100);
        let non_super = AuthContext {
            role: "user".into(),
            is_superuser: false,
            must_change_password: false,
        };
        let result = query_audit_log(&sink, &non_super);
        assert!(matches!(result, Err(SchemaError::PermissionDenied { .. })));
    }

    #[test]
    fn query_audit_log_superuser_succeeds() {
        let sink = SystemTableAuditSink::new(100);
        let super_auth = AuthContext {
            role: "admin".into(),
            is_superuser: true,
            must_change_password: false,
        };
        let result = query_audit_log(&sink, &super_auth);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
