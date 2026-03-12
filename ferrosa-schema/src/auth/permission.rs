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
}
