//! Role and authentication metadata types.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Metadata for a database role.
///
/// Roles in Cassandra are the principals for both authentication and
/// authorization. A role may be a login account, a group, or both.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleMetadata {
    /// Role name (unique identifier).
    pub name: String,
    /// Whether this role has unrestricted access.
    pub is_superuser: bool,
    /// Whether this role can authenticate (log in).
    pub can_login: bool,
    /// Bcrypt/Argon2 salted hash of the password, if set.
    pub salted_hash: Option<String>,
    /// Set of role names this role is a member of (inherited permissions).
    pub member_of: HashSet<String>,
}

/// Authentication context for the current session.
///
/// Created after successful authentication and threaded through
/// every schema operation for authorization checks.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The authenticated role name.
    pub role: String,
    /// Whether the role is a superuser (cached for fast checks).
    pub is_superuser: bool,
    /// Whether the role must change its password before other operations.
    pub must_change_password: bool,
}

/// Optional updates for a role (builder-style partial update).
///
/// `None` fields are left unchanged; `Some` fields are applied.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoleUpdates {
    /// New superuser status, if changing.
    pub is_superuser: Option<bool>,
    /// New login status, if changing.
    pub can_login: Option<bool>,
    /// New password (plaintext — will be hashed before storage).
    pub password: Option<String>,
    /// New set of parent roles, if changing.
    pub member_of: Option<HashSet<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_metadata_construction() {
        let role = RoleMetadata {
            name: "admin".to_string(),
            is_superuser: true,
            can_login: true,
            salted_hash: Some("$2a$10$abcdef".to_string()),
            member_of: HashSet::new(),
        };

        assert_eq!(role.name, "admin");
        assert!(role.is_superuser);
        assert!(role.can_login);
        assert!(role.salted_hash.is_some());
        assert!(role.member_of.is_empty());
    }

    #[test]
    fn auth_context_superuser() {
        let ctx = AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };

        assert_eq!(ctx.role, "cassandra");
        assert!(ctx.is_superuser);
        assert!(!ctx.must_change_password);

        let non_super = AuthContext {
            role: "app_user".to_string(),
            is_superuser: false,
            must_change_password: true,
        };

        assert!(!non_super.is_superuser);
        assert!(non_super.must_change_password);
    }

    #[test]
    fn role_updates_optional_fields() {
        let updates = RoleUpdates {
            is_superuser: Some(true),
            ..Default::default()
        };

        assert_eq!(updates.is_superuser, Some(true));
        assert!(updates.can_login.is_none());
        assert!(updates.password.is_none());
        assert!(updates.member_of.is_none());
    }
}
