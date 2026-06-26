//! Seed-role bootstrap for first-start auth enablement.
//!
//! When a fresh cluster comes up with `auth_enabled=true`, no roles
//! exist in `system_auth.roles` yet, so no client can authenticate —
//! including the administrator who needs to provision additional
//! roles. This module creates three seed roles idempotently:
//!
//! - `ferrosa_admin` (password `ferrosa_admin`) — SUPERUSER, used for
//!   bootstrap / recovery. The password is INTENTIONALLY well-known so a
//!   half-rolled-out cluster can always be accessed by the operator; a
//!   `WARN` fires after 5 minutes of uptime if the password is still
//!   the default, prompting rotation.
//! - `graph_engine` (password `ferrosa_user`) — LOGIN only. Receives
//!   MODIFY+SELECT on all graph tables in `agent_memory` and DESCRIBE on
//!   system tables. Used by the graph subsystem for internal CQL writes.
//! - `app_reader` (password `ferrosa_user`) — LOGIN only. Receives
//!   SELECT on graph tables, MODIFY+SELECT on app tables, and DESCRIBE on
//!   system tables. Used by application-layer clients for read/write
//!   access to non-graph data.
//!
//! **Idempotency:** If any role already exists, this function is a
//! no-op for that role. It is safe to call on every startup.
//!
//! **Grant list authority:** `AGENT_MEMORY_GRAPH_TABLES` is the single
//! canonical list of graph table names. All bootstrap logic and tests
//! derive from this constant — never copy it.
//!
//! The default credentials match the ones documented in
//! `specs/decisions/design-cql-role-auth-rollout.md` and the ddl file
//! `ferrosa-memory/ddl/100_roles.cql`.

use std::collections::HashSet;

use tracing::warn;
use uuid::Uuid;

use crate::auth::permission::{GrantEntry, Permission, Resource};
use crate::auth::role::RoleMetadata;
use crate::registry::Schema;
use crate::startup::DeploymentMode;

/// Canonical list of graph table names in keyspace `agent_memory`.
///
/// This is the **single source of truth** for the graph-table set used
/// by both the bootstrap grant matrix and any tests that verify it.
/// Add or remove names here; do not copy this list elsewhere.
pub const AGENT_MEMORY_GRAPH_TABLES: &[&str] = &[
    "typed_edges",
    "folded_into",
    "mentioned_in",
    "co_occurs_with",
    "supersedes",
    "derived_edges_by_pred",
    "derived_edges_by_src",
];

/// Seed role name for the superuser created on first startup under auth.
pub const SEED_ADMIN_USER: &str = "ferrosa_admin";

/// Default password for `SEED_ADMIN_USER`. MUST be rotated; see the
/// `password_still_default_warning_task` for the 5-minute reminder.
pub const SEED_ADMIN_PASSWORD: &str = "ferrosa_admin";

/// Seed role name for the graph-engine internal service account.
pub const SEED_GRAPH_ENGINE_USER: &str = "graph_engine";

/// Seed role name for the application-layer normal-user account.
///
/// Historically named `app_reader`, renamed to `ferrosa_user` to match
/// the documented public credential contract (see
/// `specs/in-process/bug-seeded-ferrosa-user-cannot-authenticate-to-graph-http.md`
/// and `specs/decisions/design-cql-role-auth-rollout.md`).
pub const SEED_APP_READER_USER: &str = "ferrosa_user";

/// Preferred name alias — matches the role-name string. New callers
/// should prefer this. `SEED_APP_READER_USER` remains for backwards
/// compatibility with existing call sites.
pub const SEED_APP_USER: &str = SEED_APP_READER_USER;

/// Default password shared by `SEED_GRAPH_ENGINE_USER` and
/// `SEED_APP_USER`. Same rotation guidance as `SEED_ADMIN_PASSWORD`.
pub const SEED_APP_PASSWORD: &str = "ferrosa_user";

/// Generate a cryptographically random password.
///
/// Draws 48 bytes from the OS CSPRNG (via three UUIDv4 calls, each providing
/// 16 bytes of entropy) and hex-encodes them to 96 printable ASCII characters.
/// All characters are from the hex alphabet `[0-9a-f]` so the result is
/// always valid in a CQL `WITH PASSWORD = '...'` string.
///
/// `uuid::Uuid::new_v4()` uses the OS CSPRNG (`getrandom`) internally and is
/// already a dependency of this crate.
pub fn generate_random_password() -> String {
    [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()]
        .iter()
        .flat_map(|u| *u.as_bytes())
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Configuration for `seed_default_roles_with_config`.
pub struct SeedConfig {
    /// Explicit admin password. `None` = generate random (production) or use
    /// `SEED_ADMIN_PASSWORD` (development).
    pub admin_password: Option<String>,
    /// Service account password shared by `graph_engine` and `ferrosa_user`.
    /// `None` = generate random (production) or use `SEED_APP_PASSWORD` (development).
    pub service_password: Option<String>,
    /// Deployment mode controls whether well-known defaults are used.
    pub mode: DeploymentMode,
}

impl SeedConfig {
    /// Build a `SeedConfig` from the environment.
    ///
    /// `FERROSA_SEED_ADMIN_PASSWORD` supplies an explicit admin password at
    /// first bootstrap (documented in `config/ferrosa.example.toml`); when
    /// unset, production mode generates a random password and development
    /// mode uses the well-known seed credential.
    pub fn from_env() -> Self {
        Self::from_parts(
            DeploymentMode::from_env(),
            std::env::var("FERROSA_SEED_ADMIN_PASSWORD").ok(),
        )
    }

    /// Pure constructor backing `from_env`, separated so tests stay free of
    /// process-global environment races.
    ///
    /// An empty password is treated as unset so `FERROSA_SEED_ADMIN_PASSWORD=""`
    /// in a compose file can never seed an empty admin password.
    pub fn from_parts(mode: DeploymentMode, admin_password: Option<String>) -> Self {
        Self {
            admin_password: admin_password.filter(|p| !p.is_empty()),
            service_password: None,
            mode,
        }
    }
}

/// Outcome returned by `seed_default_roles_with_config`.
///
/// Each field is `Some(password)` only when the corresponding role was **newly
/// created** in this call. When a role already existed the field is `None`
/// (idempotency: the existing password is never revealed or overwritten).
pub struct SeedOutcome {
    /// The admin password used to seed `ferrosa_admin`, if newly created.
    pub admin_password: Option<String>,
    /// The service password used to seed `graph_engine` and `ferrosa_user`,
    /// if at least one of those roles was newly created.
    pub service_password: Option<String>,
}

/// Bootstrap the three default seed roles using a caller-supplied `SeedConfig`.
///
/// - Development mode: well-known passwords are used unless overridden.
///   A `warn!` is emitted to prompt operators to use production mode.
/// - Production mode: passwords are randomly generated unless an explicit
///   password is given. When random passwords are generated the plain-text
///   passwords are printed to stderr (via `eprintln!`) so operators can
///   record them; they are also logged at `warn!` level.
///
/// Returns a `SeedOutcome` whose fields are `Some(password)` only for roles
/// that were actually created in this call.
///
/// # Errors
///
/// Returns `Err` if password hashing or role creation fails. Grant failures
/// from the underlying `seed_grants_if_absent` are propagated.
pub fn seed_default_roles_with_config(
    schema: &Schema,
    config: &SeedConfig,
) -> crate::Result<SeedOutcome> {
    // Resolve passwords for this call.
    let admin_pw = match config.admin_password.as_deref() {
        Some(explicit) => explicit.to_string(),
        None => match config.mode {
            DeploymentMode::Development => {
                warn!(
                    "FERROSA_MODE=development — using well-known seed credentials. \
                     Set FERROSA_MODE=production for random credentials"
                );
                SEED_ADMIN_PASSWORD.to_string()
            }
            DeploymentMode::Production => generate_random_password(),
        },
    };
    let service_pw = match config.service_password.as_deref() {
        Some(explicit) => explicit.to_string(),
        None => match config.mode {
            DeploymentMode::Development => SEED_APP_PASSWORD.to_string(),
            DeploymentMode::Production => generate_random_password(),
        },
    };

    // Seed each role and track whether it was newly created.
    let admin_created = seed_role_if_absent(schema, SEED_ADMIN_USER, &admin_pw, true)?;
    let graph_created = seed_role_if_absent(schema, SEED_GRAPH_ENGINE_USER, &service_pw, false)?;
    let app_created = seed_role_if_absent(schema, SEED_APP_READER_USER, &service_pw, false)?;

    // In production mode, print generated credentials to stderr so operators
    // can record them. Only emit when the role was actually created.
    if config.mode == DeploymentMode::Production {
        if admin_created && config.admin_password.is_none() {
            eprintln!(
                "==========================================================================\n\
                 ferrosa: GENERATED ADMIN PASSWORD (write this down — shown only once):\n\
                   role:     {SEED_ADMIN_USER}\n\
                   password: {admin_pw}\n\
                 =========================================================================="
            );
            warn!(
                role = SEED_ADMIN_USER,
                "production seed: admin password generated — written to stderr"
            );
        }
        if (graph_created || app_created) && config.service_password.is_none() {
            eprintln!(
                "==========================================================================\n\
                 ferrosa: GENERATED SERVICE PASSWORD (write this down — shown only once):\n\
                   roles:    {SEED_GRAPH_ENGINE_USER}, {SEED_APP_READER_USER}\n\
                   password: {service_pw}\n\
                 =========================================================================="
            );
            warn!("production seed: service account password generated — written to stderr");
        }
    }

    seed_grants_if_absent(schema, "agent_memory")?;

    Ok(SeedOutcome {
        admin_password: admin_created.then_some(admin_pw),
        service_password: (graph_created || app_created).then_some(service_pw),
    })
}

/// Bootstrap the three default seed roles on a `Schema`.
///
/// - Creates `ferrosa_admin` with SUPERUSER=true and LOGIN=true, if absent.
/// - Creates `graph_engine` with SUPERUSER=false and LOGIN=true, if absent.
/// - Creates `app_reader` with SUPERUSER=false and LOGIN=true, if absent.
///
/// After role creation, applies the per-resource grant matrix defined in
/// `specs/decisions/design-cql-role-auth-rollout.md`:
/// - `graph_engine`: MODIFY+SELECT on each graph table; DESCRIBE on AllKeyspaces.
/// - `app_reader`: SELECT on each graph table; DESCRIBE on AllKeyspaces.
///   (MODIFY on app tables is deferred until app tables are registered at startup.)
///
/// Password selection and mode-sensitive behavior are delegated to
/// `seed_default_roles_with_config`. In development mode (the default),
/// the well-known credentials `SEED_ADMIN_PASSWORD` / `SEED_APP_PASSWORD`
/// are used. In production mode (`FERROSA_MODE=production`), random
/// passwords are generated and printed to stderr. An explicit admin
/// password may be supplied via `FERROSA_SEED_ADMIN_PASSWORD` (empty
/// value = unset); see `SeedConfig::from_env`.
///
/// If a role already exists the corresponding branch is skipped — no
/// grants are re-applied, no password is overwritten.
///
/// Returns `Err` only if the password hasher itself fails or a grant
/// insert fails. Grant failures are not swallowed.
pub fn seed_default_roles(schema: &Schema) -> crate::Result<()> {
    let config = SeedConfig::from_env();
    seed_default_roles_with_config(schema, &config)?;
    Ok(())
}

/// Insert the canonical per-resource grant matrix for `graph_engine` and
/// `app_reader` on the given keyspace.
///
/// The graph table list is driven entirely by `AGENT_MEMORY_GRAPH_TABLES`.
/// Grants are merged idempotently by `grant_internal` — calling this
/// multiple times is safe.
///
/// # Errors
///
/// Returns `Err` immediately if any `grant_internal` call fails. Failures
/// are never swallowed.
pub fn seed_grants_if_absent(schema: &Schema, keyspace: &str) -> crate::Result<()> {
    // graph_engine: MODIFY + SELECT on each graph table; DESCRIBE on all keyspaces.
    for table in AGENT_MEMORY_GRAPH_TABLES {
        let entry = GrantEntry {
            role: SEED_GRAPH_ENGINE_USER.to_string(),
            resource: Resource::Table(keyspace.to_string(), table.to_string()),
            permissions: [Permission::Modify, Permission::Select]
                .into_iter()
                .collect(),
        };
        schema.grant_internal(entry)?;
    }
    schema.grant_internal(GrantEntry {
        role: SEED_GRAPH_ENGINE_USER.to_string(),
        resource: Resource::AllKeyspaces,
        permissions: [Permission::Describe].into_iter().collect(),
    })?;

    // app_reader: SELECT on each graph table; DESCRIBE on all keyspaces.
    // (MODIFY on app tables is provisioned separately at application startup.)
    for table in AGENT_MEMORY_GRAPH_TABLES {
        let entry = GrantEntry {
            role: SEED_APP_READER_USER.to_string(),
            resource: Resource::Table(keyspace.to_string(), table.to_string()),
            permissions: [Permission::Select].into_iter().collect(),
        };
        schema.grant_internal(entry)?;
    }
    schema.grant_internal(GrantEntry {
        role: SEED_APP_READER_USER.to_string(),
        resource: Resource::AllKeyspaces,
        permissions: [Permission::Describe].into_iter().collect(),
    })?;

    Ok(())
}

/// Create a role with the given name/password/superuser flag, if
/// that role doesn't already exist in the schema snapshot.
///
/// Returns `true` if the role was newly created, `false` if it already
/// existed (idempotency: the existing role is not modified).
fn seed_role_if_absent(
    schema: &Schema,
    username: &str,
    password: &str,
    is_superuser: bool,
) -> crate::Result<bool> {
    {
        let snap = schema.snapshot();
        if snap.roles.contains_key(username) {
            return Ok(false);
        }
    }

    let hashed = schema.password_hasher().hash_password(password)?;
    let role = RoleMetadata {
        name: username.to_string(),
        is_superuser,
        can_login: true,
        salted_hash: Some(hashed),
        member_of: HashSet::new(),
        scram: None,
    };
    schema.create_role_internal(role)?;
    warn!(
        role = username,
        "seed role created — rotate credentials before exposing to production traffic"
    );
    Ok(true)
}

/// Return `true` if `ferrosa_admin` still has the built-in default
/// password. Used by the 5-minute startup warning task.
///
/// Hashes `SEED_ADMIN_PASSWORD` against the stored hash for
/// `ferrosa_admin`; if the verification succeeds, the operator has
/// not rotated the seed password yet.
pub fn admin_password_is_default(schema: &Schema) -> bool {
    let snap = schema.snapshot();
    let Some(admin) = snap.roles.get(SEED_ADMIN_USER) else {
        return false;
    };
    let Some(hash) = admin.salted_hash.as_deref() else {
        return false;
    };
    crate::auth::password::PasswordHasher::verify_password_any(SEED_ADMIN_PASSWORD, hash)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::TestAuditSink;
    use crate::auth::password::{PasswordHasher, PasswordPolicy};
    use crate::auth::permission::{check_permission, Permission, Resource};
    use crate::auth::rate_limit::RateLimitConfig;
    use crate::auth::role::AuthContext;
    use crate::registry::{AuthMethod, SchemaConfig};
    use crate::secrets::EnvSecretsProvider;
    use crate::startup::DeploymentMode;

    fn test_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .expect("schema init")
    }

    fn production_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Production,
        })
        .expect("schema init")
    }

    fn graph_engine_ctx() -> AuthContext {
        AuthContext {
            role: SEED_GRAPH_ENGINE_USER.to_string(),
            is_superuser: false,
            must_change_password: false,
        }
    }

    fn app_reader_ctx() -> AuthContext {
        AuthContext {
            role: SEED_APP_READER_USER.to_string(),
            is_superuser: false,
            must_change_password: false,
        }
    }

    #[test]
    fn seeding_creates_all_three_roles() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        assert!(snap.roles.get(SEED_ADMIN_USER).unwrap().is_superuser);
        assert!(!snap.roles.get(SEED_GRAPH_ENGINE_USER).unwrap().is_superuser);
        assert!(!snap.roles.get(SEED_APP_READER_USER).unwrap().is_superuser);
    }

    #[test]
    fn seeding_is_idempotent() {
        let schema = test_schema();
        // Schema::new always creates the built-in "cassandra" superuser role,
        // so after seeding we expect exactly 4 roles total
        // (cassandra + ferrosa_admin + graph_engine + app_reader).
        let roles_before = schema.snapshot().roles.len();
        seed_default_roles(&schema).unwrap();
        seed_default_roles(&schema).unwrap(); // idempotent: second call must not add duplicates
        let snap = schema.snapshot();
        assert_eq!(
            snap.roles.len(),
            roles_before + 3,
            "seeding must add exactly 3 new roles regardless of how many times it is called"
        );
        assert!(
            snap.roles.contains_key(SEED_ADMIN_USER),
            "ferrosa_admin must be present"
        );
        assert!(
            snap.roles.contains_key(SEED_GRAPH_ENGINE_USER),
            "graph_engine must be present"
        );
        assert!(
            snap.roles.contains_key(SEED_APP_READER_USER),
            "app_reader must be present"
        );
    }

    #[test]
    fn admin_default_password_detected() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        assert!(admin_password_is_default(&schema));
    }

    /// Bug: `ferrosa_user` / `ferrosa_user` is documented as the
    /// public normal-user credential but was never seeded — prior
    /// bootstrap seeded `app_reader` under the same password instead,
    /// so clients using the documented `ferrosa_user` login got
    /// "authentication failed" on the graph HTTP endpoint. See
    /// `specs/in-process/bug-seeded-ferrosa-user-cannot-authenticate-to-graph-http.md`.
    #[test]
    fn seeded_ferrosa_user_role_exists_after_bootstrap() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        assert!(
            snap.roles.contains_key(SEED_APP_USER),
            "ferrosa_user role must be seeded — see bug-seeded-ferrosa-user-cannot-authenticate-to-graph-http.md"
        );
        let role = snap.roles.get(SEED_APP_USER).unwrap();
        assert!(role.can_login, "ferrosa_user must be a LOGIN role");
        assert!(!role.is_superuser, "ferrosa_user must NOT be a superuser");
    }

    #[test]
    fn seeded_ferrosa_user_authenticates_with_default_password() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        // The same authentication path the graph HTTP endpoint takes.
        let auth_ctx = schema
            .authenticate(SEED_APP_USER, SEED_APP_PASSWORD)
            .expect("ferrosa_user must authenticate with its default password");
        assert_eq!(auth_ctx.role, SEED_APP_USER);
    }

    #[test]
    fn seeded_ferrosa_user_has_select_on_graph_tables() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        let auth = AuthContext {
            role: SEED_APP_USER.to_string(),
            is_superuser: false,
            must_change_password: false,
        };

        for table in AGENT_MEMORY_GRAPH_TABLES {
            let resource = Resource::Table("agent_memory".to_string(), table.to_string());
            assert!(
                check_permission(&snap, &auth, Permission::Select, &resource).is_ok(),
                "ferrosa_user must have SELECT on agent_memory.{table}"
            );
        }
    }

    #[test]
    fn seeded_ferrosa_user_does_not_have_modify_on_graph_tables() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        let auth = AuthContext {
            role: SEED_APP_USER.to_string(),
            is_superuser: false,
            must_change_password: false,
        };
        let resource = Resource::Table("agent_memory".to_string(), "typed_edges".to_string());
        assert!(
            check_permission(&snap, &auth, Permission::Modify, &resource).is_err(),
            "ferrosa_user must NOT have MODIFY on graph tables"
        );
    }

    // ── Grant matrix tests ────────────────────────────────────────────────────

    /// After bootstrap, `graph_engine` must have MODIFY on every graph table
    /// in `agent_memory`.
    #[test]
    fn graph_engine_has_modify_on_graph_tables_after_bootstrap() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        let auth = graph_engine_ctx();

        for table in AGENT_MEMORY_GRAPH_TABLES {
            let resource = Resource::Table("agent_memory".to_string(), table.to_string());
            assert!(
                check_permission(&snap, &auth, Permission::Modify, &resource).is_ok(),
                "graph_engine must have MODIFY on agent_memory.{table}"
            );
        }
    }

    /// `graph_engine` must NOT have MODIFY on a non-graph table in `agent_memory`.
    #[test]
    fn graph_engine_has_no_modify_on_non_graph_table() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        let auth = graph_engine_ctx();

        let resource = Resource::Table("agent_memory".to_string(), "entity_store".to_string());
        assert!(
            check_permission(&snap, &auth, Permission::Modify, &resource).is_err(),
            "graph_engine must NOT have MODIFY on a non-graph table (entity_store)"
        );
    }

    /// After bootstrap, `graph_engine` must also have SELECT on each graph table.
    #[test]
    fn graph_engine_has_select_on_graph_tables_after_bootstrap() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        let auth = graph_engine_ctx();

        for table in AGENT_MEMORY_GRAPH_TABLES {
            let resource = Resource::Table("agent_memory".to_string(), table.to_string());
            assert!(
                check_permission(&snap, &auth, Permission::Select, &resource).is_ok(),
                "graph_engine must have SELECT on agent_memory.{table}"
            );
        }
    }

    /// After bootstrap, `app_reader` must have SELECT (not MODIFY) on graph tables.
    #[test]
    fn app_reader_has_select_not_modify_on_graph_tables_after_bootstrap() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();
        let auth = app_reader_ctx();

        for table in AGENT_MEMORY_GRAPH_TABLES {
            let resource = Resource::Table("agent_memory".to_string(), table.to_string());
            assert!(
                check_permission(&snap, &auth, Permission::Select, &resource).is_ok(),
                "app_reader must have SELECT on agent_memory.{table}"
            );
            assert!(
                check_permission(&snap, &auth, Permission::Modify, &resource).is_err(),
                "app_reader must NOT have MODIFY on graph table agent_memory.{table}"
            );
        }
    }

    /// After bootstrap, both `graph_engine` and `app_reader` must have DESCRIBE
    /// on all keyspaces (covers system table introspection).
    #[test]
    fn seed_roles_have_describe_on_all_keyspaces() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        let snap = schema.snapshot();

        for (role_name, auth) in [
            (SEED_GRAPH_ENGINE_USER, graph_engine_ctx()),
            (SEED_APP_READER_USER, app_reader_ctx()),
        ] {
            assert!(
                check_permission(&snap, &auth, Permission::Describe, &Resource::AllKeyspaces)
                    .is_ok(),
                "{role_name} must have DESCRIBE on AllKeyspaces"
            );
        }
    }

    /// `seed_default_roles` is idempotent — calling it twice must not duplicate grants
    /// or return an error.
    #[test]
    fn seed_grants_idempotent() {
        let schema = test_schema();
        seed_default_roles(&schema).unwrap();
        seed_default_roles(&schema).unwrap(); // must not panic or double-count

        // Verify permissions still hold after second call.
        let snap = schema.snapshot();
        let auth = graph_engine_ctx();
        let resource = Resource::Table("agent_memory".to_string(), "typed_edges".to_string());
        assert!(
            check_permission(&snap, &auth, Permission::Modify, &resource).is_ok(),
            "MODIFY on typed_edges must still be granted after idempotent second call"
        );
    }

    // ── New tests for generate_random_password / SeedConfig / SeedOutcome ──

    // A1: random password is not equal to any well-known default credential
    #[test]
    fn random_password_is_not_well_known_default() {
        let pw = generate_random_password();
        assert_ne!(
            pw, SEED_ADMIN_PASSWORD,
            "generated password must not equal SEED_ADMIN_PASSWORD"
        );
        assert_ne!(
            pw, SEED_APP_PASSWORD,
            "generated password must not equal SEED_APP_PASSWORD"
        );
    }

    // A2: random password has sufficient length (>= 20 chars)
    #[test]
    fn random_password_has_sufficient_length() {
        let pw = generate_random_password();
        assert!(
            pw.len() >= 20,
            "generated password length {} must be >= 20",
            pw.len()
        );
    }

    // A3: two calls to generate_random_password produce different values
    #[test]
    fn random_password_is_unique() {
        let pw1 = generate_random_password();
        let pw2 = generate_random_password();
        assert_ne!(
            pw1, pw2,
            "two generated passwords must not be equal (CSPRNG should produce unique output)"
        );
    }

    // A4: production mode generates random admin password (not "ferrosa_admin")
    #[test]
    fn production_mode_generates_random_admin_password() {
        let schema = production_schema();
        let config = SeedConfig {
            admin_password: None,
            service_password: None,
            mode: DeploymentMode::Production,
        };
        let outcome = seed_default_roles_with_config(&schema, &config).unwrap();
        let actual_admin_pw = outcome
            .admin_password
            .expect("production mode must return the generated admin password in SeedOutcome");
        assert_ne!(
            actual_admin_pw, SEED_ADMIN_PASSWORD,
            "production-mode seed must NOT use the well-known admin password"
        );
    }

    // A5: production mode seeded admin does NOT authenticate with the well-known password
    #[test]
    fn production_mode_admin_cannot_authenticate_with_default_password() {
        let schema = production_schema();
        let config = SeedConfig {
            admin_password: None,
            service_password: None,
            mode: DeploymentMode::Production,
        };
        seed_default_roles_with_config(&schema, &config).unwrap();
        let result = schema.authenticate(SEED_ADMIN_USER, SEED_ADMIN_PASSWORD);
        assert!(
            result.is_err(),
            "production-seeded admin must NOT authenticate with the well-known password '{SEED_ADMIN_PASSWORD}'"
        );
    }

    // A6: dev mode uses well-known passwords
    #[test]
    fn dev_mode_uses_well_known_admin_password() {
        let schema = test_schema();
        let config = SeedConfig {
            admin_password: None,
            service_password: None,
            mode: DeploymentMode::Development,
        };
        seed_default_roles_with_config(&schema, &config).unwrap();
        let result = schema.authenticate(SEED_ADMIN_USER, SEED_ADMIN_PASSWORD);
        assert!(
            result.is_ok(),
            "dev-mode seed must allow admin to authenticate with the well-known password"
        );
    }

    // A7: idempotency — existing roles untouched on second call; outcome returns None for already-present
    #[test]
    fn seed_config_idempotent_existing_roles_not_overwritten() {
        let schema = test_schema();
        let config = SeedConfig {
            admin_password: None,
            service_password: None,
            mode: DeploymentMode::Development,
        };
        let first = seed_default_roles_with_config(&schema, &config).unwrap();
        assert!(
            first.admin_password.is_some(),
            "first call must produce an admin_password in SeedOutcome"
        );
        let second = seed_default_roles_with_config(&schema, &config).unwrap();
        assert!(
            second.admin_password.is_none(),
            "second call must not produce an admin_password — role already exists"
        );
        // Roles count must not increase on second call.
        let snap = schema.snapshot();
        assert!(snap.roles.contains_key(SEED_ADMIN_USER));
        assert!(snap.roles.contains_key(SEED_GRAPH_ENGINE_USER));
        assert!(snap.roles.contains_key(SEED_APP_READER_USER));
    }

    // A8: production mode service accounts do not have the well-known "ferrosa_user" password
    #[test]
    fn production_mode_service_accounts_not_seeded_with_well_known_password() {
        let schema = production_schema();
        let config = SeedConfig {
            admin_password: None,
            service_password: None,
            mode: DeploymentMode::Production,
        };
        seed_default_roles_with_config(&schema, &config).unwrap();
        // graph_engine must not authenticate with SEED_APP_PASSWORD
        let result = schema.authenticate(SEED_GRAPH_ENGINE_USER, SEED_APP_PASSWORD);
        assert!(
            result.is_err(),
            "production graph_engine must NOT authenticate with well-known SEED_APP_PASSWORD"
        );
        // ferrosa_user must not authenticate with SEED_APP_PASSWORD
        let result = schema.authenticate(SEED_APP_READER_USER, SEED_APP_PASSWORD);
        assert!(
            result.is_err(),
            "production ferrosa_user must NOT authenticate with well-known SEED_APP_PASSWORD"
        );
    }

    // A9: explicit password override works in production mode
    #[test]
    fn explicit_password_overrides_random_generation() {
        let schema = production_schema();
        let explicit_pw = "my-explicit-admin-password-42!";
        let config = SeedConfig {
            admin_password: Some(explicit_pw.to_string()),
            service_password: None,
            mode: DeploymentMode::Production,
        };
        let outcome = seed_default_roles_with_config(&schema, &config).unwrap();
        assert_eq!(
            outcome.admin_password.as_deref(),
            Some(explicit_pw),
            "SeedOutcome must reflect the explicitly provided admin password"
        );
        let result = schema.authenticate(SEED_ADMIN_USER, explicit_pw);
        assert!(
            result.is_ok(),
            "admin must authenticate with the explicit override password"
        );
    }

    // A10: FERROSA_SEED_ADMIN_PASSWORD (documented in ferrosa.example.toml)
    // must flow into SeedConfig. Pure constructor so the test is env-race-free.
    #[test]
    fn from_parts_with_password_sets_admin_password() {
        let config = SeedConfig::from_parts(
            DeploymentMode::Production,
            Some("operator-supplied-pw".to_string()),
        );
        assert_eq!(
            config.admin_password.as_deref(),
            Some("operator-supplied-pw"),
            "FERROSA_SEED_ADMIN_PASSWORD value must become the explicit admin password"
        );
        assert_eq!(config.mode, DeploymentMode::Production);
    }

    // A11: empty env value must be treated as unset, never seeded as a password
    #[test]
    fn from_parts_empty_string_treated_as_unset() {
        let config = SeedConfig::from_parts(DeploymentMode::Production, Some(String::new()));
        assert!(
            config.admin_password.is_none(),
            "FERROSA_SEED_ADMIN_PASSWORD=\"\" must not seed an empty admin password"
        );
    }
}
