//! Permission, resource, and grant types for Cassandra-style authorization.

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Serialize};

/// A database permission that can be granted or revoked.
///
/// Mirrors Cassandra's permission model. `#[non_exhaustive]` allows
/// adding new permissions (e.g., `Unmask`) without semver breakage.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Permission {
    /// Permission to create keyspaces, tables, roles, functions.
    Create,
    /// Permission to alter schema definitions.
    Alter,
    /// Permission to drop keyspaces, tables, roles, functions.
    Drop,
    /// Permission to read data from tables.
    Select,
    /// Permission to write data (INSERT, UPDATE, DELETE).
    Modify,
    /// Permission to grant/revoke permissions.
    Authorize,
    /// Permission to describe schema metadata.
    Describe,
    /// Permission to execute functions/aggregates.
    Execute,
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create => write!(f, "CREATE"),
            Self::Alter => write!(f, "ALTER"),
            Self::Drop => write!(f, "DROP"),
            Self::Select => write!(f, "SELECT"),
            Self::Modify => write!(f, "MODIFY"),
            Self::Authorize => write!(f, "AUTHORIZE"),
            Self::Describe => write!(f, "DESCRIBE"),
            Self::Execute => write!(f, "EXECUTE"),
        }
    }
}

/// A database resource that permissions apply to.
///
/// Resources form a hierarchy: `AllKeyspaces` > `Keyspace` > `Table`.
/// `#[non_exhaustive]` allows adding new resource types without semver breakage.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Resource {
    /// All keyspaces (global scope).
    AllKeyspaces,
    /// A specific keyspace.
    Keyspace(String),
    /// A specific table (keyspace, table).
    Table(String, String),
    /// All roles (global scope).
    AllRoles,
    /// A specific role.
    Role(String),
    /// All functions in a keyspace.
    AllFunctions(String),
    /// A specific function (keyspace, function name, argument types).
    Function(String, String, Vec<String>),
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllKeyspaces => write!(f, "ALL KEYSPACES"),
            Self::Keyspace(ks) => write!(f, "keyspace {ks}"),
            Self::Table(ks, tbl) => write!(f, "table {ks}.{tbl}"),
            Self::AllRoles => write!(f, "ALL ROLES"),
            Self::Role(name) => write!(f, "role {name}"),
            Self::AllFunctions(ks) => write!(f, "ALL FUNCTIONS IN KEYSPACE {ks}"),
            Self::Function(ks, name, args) => {
                write!(f, "function {ks}.{name}({})", args.join(", "))
            }
        }
    }
}

/// A grant entry binding a role to permissions on a resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantEntry {
    /// The role being granted permissions.
    pub role: String,
    /// The resource the permissions apply to.
    pub resource: Resource,
    /// The set of permissions granted.
    pub permissions: HashSet<Permission>,
}

use crate::auth::role::AuthContext;
use crate::error::SchemaError;
use crate::registry::SchemaSnapshot;

/// Check whether `auth` has `perm` on `resource` according to the snapshot.
///
/// Superusers bypass all checks. For non-superusers, walks the grant
/// hierarchy (direct grants, resource hierarchy, role hierarchy via `member_of`).
pub fn check_permission(
    snap: &SchemaSnapshot,
    auth: &AuthContext,
    perm: Permission,
    resource: &Resource,
) -> crate::Result<()> {
    // Superuser bypass
    if auth.is_superuser {
        return Ok(());
    }

    let mut visited = HashSet::new();
    if has_permission_recursive(snap, &auth.role, perm, resource, &mut visited) {
        Ok(())
    } else {
        Err(SchemaError::PermissionDenied {
            role: auth.role.clone(),
            permission: perm,
            resource: resource.clone(),
        })
    }
}

/// Recursively check grants for `role_name`, walking up the role hierarchy.
fn has_permission_recursive(
    snap: &SchemaSnapshot,
    role_name: &str,
    perm: Permission,
    resource: &Resource,
    visited: &mut HashSet<String>,
) -> bool {
    if !visited.insert(role_name.to_string()) {
        return false; // Cycle detected
    }

    // Check direct grants
    if let Some(grants) = snap.grants.get(role_name) {
        for grant in grants {
            if grant.permissions.contains(&perm) && resource_matches(&grant.resource, resource) {
                return true;
            }
        }
    }

    // Check inherited grants via member_of
    if let Some(role) = snap.roles.get(role_name) {
        for parent in &role.member_of {
            if has_permission_recursive(snap, parent, perm, resource, visited) {
                return true;
            }
        }
    }

    false
}

/// Returns true if `grant_resource` covers `target_resource`.
///
/// Resource hierarchy: `AllKeyspaces` > `Keyspace` > `Table`,
/// `AllRoles` > `Role`.
fn resource_matches(grant_resource: &Resource, target_resource: &Resource) -> bool {
    match (grant_resource, target_resource) {
        (Resource::AllKeyspaces, Resource::AllKeyspaces) => true,
        (Resource::AllKeyspaces, Resource::Keyspace(_)) => true,
        (Resource::AllKeyspaces, Resource::Table(_, _)) => true,
        (Resource::Keyspace(g), Resource::Keyspace(t)) => g == t,
        (Resource::Keyspace(g), Resource::Table(ks, _)) => g == ks,
        (Resource::Table(gks, gt), Resource::Table(tks, tt)) => gks == tks && gt == tt,
        (Resource::AllRoles, Resource::AllRoles) => true,
        (Resource::AllRoles, Resource::Role(_)) => true,
        (Resource::Role(g), Resource::Role(t)) => g == t,
        _ => grant_resource == target_resource,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_variants_are_distinct() {
        let perms = [
            Permission::Create,
            Permission::Alter,
            Permission::Drop,
            Permission::Select,
            Permission::Modify,
            Permission::Authorize,
            Permission::Describe,
            Permission::Execute,
        ];

        let set: HashSet<Permission> = perms.iter().copied().collect();
        assert_eq!(set.len(), 8, "all permission variants must be distinct");
    }

    #[test]
    fn resource_table_includes_keyspace() {
        let resource = Resource::Table("my_ks".to_string(), "my_table".to_string());
        match &resource {
            Resource::Table(ks, tbl) => {
                assert_eq!(ks, "my_ks");
                assert_eq!(tbl, "my_table");
            }
            _ => panic!("expected Resource::Table"),
        }
    }

    #[test]
    fn grant_entry_serde_roundtrip() {
        let entry = GrantEntry {
            role: "analyst".to_string(),
            resource: Resource::Table("analytics".to_string(), "events".to_string()),
            permissions: [Permission::Select, Permission::Describe]
                .into_iter()
                .collect(),
        };

        let json = serde_json::to_string(&entry).expect("serialize");
        let deserialized: GrantEntry = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.role, "analyst");
        assert_eq!(
            deserialized.resource,
            Resource::Table("analytics".to_string(), "events".to_string())
        );
        assert!(deserialized.permissions.contains(&Permission::Select));
        assert!(deserialized.permissions.contains(&Permission::Describe));
        assert_eq!(deserialized.permissions.len(), 2);
    }

    #[test]
    fn resource_display_for_error_messages() {
        assert_eq!(Resource::AllKeyspaces.to_string(), "ALL KEYSPACES");
        assert_eq!(
            Resource::Keyspace("my_ks".to_string()).to_string(),
            "keyspace my_ks"
        );
        assert_eq!(
            Resource::Table("ks".to_string(), "tbl".to_string()).to_string(),
            "table ks.tbl"
        );
        assert_eq!(Resource::AllRoles.to_string(), "ALL ROLES");
        assert_eq!(
            Resource::Role("admin".to_string()).to_string(),
            "role admin"
        );
        assert_eq!(
            Resource::AllFunctions("ks".to_string()).to_string(),
            "ALL FUNCTIONS IN KEYSPACE ks"
        );
        assert_eq!(
            Resource::Function(
                "ks".to_string(),
                "my_fn".to_string(),
                vec!["int".to_string(), "text".to_string()]
            )
            .to_string(),
            "function ks.my_fn(int, text)"
        );
    }

    #[test]
    fn permission_display() {
        assert_eq!(Permission::Create.to_string(), "CREATE");
        assert_eq!(Permission::Alter.to_string(), "ALTER");
        assert_eq!(Permission::Drop.to_string(), "DROP");
        assert_eq!(Permission::Select.to_string(), "SELECT");
        assert_eq!(Permission::Modify.to_string(), "MODIFY");
        assert_eq!(Permission::Authorize.to_string(), "AUTHORIZE");
        assert_eq!(Permission::Describe.to_string(), "DESCRIBE");
        assert_eq!(Permission::Execute.to_string(), "EXECUTE");
    }

    // ---- Task 19: Permission checking tests ----

    use crate::auth::role::{AuthContext, RoleMetadata};
    use crate::registry::SchemaSnapshot;

    fn superuser_ctx() -> AuthContext {
        AuthContext {
            role: "admin".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn normal_ctx(role: &str) -> AuthContext {
        AuthContext {
            role: role.to_string(),
            is_superuser: false,
            must_change_password: false,
        }
    }

    fn snap_with_grant(
        role_name: &str,
        resource: Resource,
        perms: Vec<Permission>,
    ) -> SchemaSnapshot {
        let mut snap = SchemaSnapshot::new();
        snap.roles.insert(
            role_name.to_string(),
            RoleMetadata {
                name: role_name.to_string(),
                is_superuser: false,
                can_login: true,
                salted_hash: None,
                member_of: HashSet::new(),
            },
        );
        snap.grants.insert(
            role_name.to_string(),
            vec![GrantEntry {
                role: role_name.to_string(),
                resource,
                permissions: perms.into_iter().collect(),
            }],
        );
        snap
    }

    #[test]
    fn superuser_bypasses_all_checks() {
        let snap = SchemaSnapshot::new(); // no grants at all
        let auth = superuser_ctx();
        let result = check_permission(
            &snap,
            &auth,
            Permission::Drop,
            &Resource::Table("ks".into(), "tbl".into()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn no_grant_means_denied() {
        let snap = SchemaSnapshot::new();
        let auth = normal_ctx("app_user");
        let result = check_permission(
            &snap,
            &auth,
            Permission::Select,
            &Resource::Table("ks".into(), "tbl".into()),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::PermissionDenied { .. }
        ));
    }

    #[test]
    fn direct_grant_allows_access() {
        let snap = snap_with_grant(
            "reader",
            Resource::Table("analytics".into(), "events".into()),
            vec![Permission::Select],
        );
        let auth = normal_ctx("reader");
        let result = check_permission(
            &snap,
            &auth,
            Permission::Select,
            &Resource::Table("analytics".into(), "events".into()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn table_inherits_keyspace_grant() {
        let snap = snap_with_grant(
            "ks_reader",
            Resource::Keyspace("my_ks".into()),
            vec![Permission::Select],
        );
        let auth = normal_ctx("ks_reader");
        // Check permission on a specific table in that keyspace
        let result = check_permission(
            &snap,
            &auth,
            Permission::Select,
            &Resource::Table("my_ks".into(), "users".into()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn role_hierarchy_grants_inherited() {
        let mut snap = SchemaSnapshot::new();

        // Parent role "data_team" has SELECT on a keyspace
        snap.roles.insert(
            "data_team".to_string(),
            RoleMetadata {
                name: "data_team".to_string(),
                is_superuser: false,
                can_login: false,
                salted_hash: None,
                member_of: HashSet::new(),
            },
        );
        snap.grants.insert(
            "data_team".to_string(),
            vec![GrantEntry {
                role: "data_team".to_string(),
                resource: Resource::Keyspace("analytics".into()),
                permissions: [Permission::Select].into_iter().collect(),
            }],
        );

        // Child role "alice" is a member of "data_team"
        snap.roles.insert(
            "alice".to_string(),
            RoleMetadata {
                name: "alice".to_string(),
                is_superuser: false,
                can_login: true,
                salted_hash: None,
                member_of: ["data_team".to_string()].into_iter().collect(),
            },
        );

        let auth = normal_ctx("alice");
        let result = check_permission(
            &snap,
            &auth,
            Permission::Select,
            &Resource::Table("analytics".into(), "events".into()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn cycle_detection_prevents_infinite_loop() {
        let mut snap = SchemaSnapshot::new();

        // role_a is member_of role_b, role_b is member_of role_a
        snap.roles.insert(
            "role_a".to_string(),
            RoleMetadata {
                name: "role_a".to_string(),
                is_superuser: false,
                can_login: true,
                salted_hash: None,
                member_of: ["role_b".to_string()].into_iter().collect(),
            },
        );
        snap.roles.insert(
            "role_b".to_string(),
            RoleMetadata {
                name: "role_b".to_string(),
                is_superuser: false,
                can_login: false,
                salted_hash: None,
                member_of: ["role_a".to_string()].into_iter().collect(),
            },
        );

        let auth = normal_ctx("role_a");
        // Neither role has any grants, so this should be denied (not loop forever)
        let result = check_permission(
            &snap,
            &auth,
            Permission::Select,
            &Resource::Table("ks".into(), "tbl".into()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn all_keyspaces_grant_covers_table() {
        let snap = snap_with_grant(
            "global_reader",
            Resource::AllKeyspaces,
            vec![Permission::Select],
        );
        let auth = normal_ctx("global_reader");
        let result = check_permission(
            &snap,
            &auth,
            Permission::Select,
            &Resource::Table("any_ks".into(), "any_table".into()),
        );
        assert!(result.is_ok());
    }
}
