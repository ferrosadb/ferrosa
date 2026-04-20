//! Integration test — Sprint E of the CQL role-auth rollout.
//!
//! Verifies that once seed roles are bootstrapped, the
//! `app_reader` unprivileged role can SELECT from graph tables but
//! cannot INSERT/UPDATE/DELETE, while `ferrosa_admin` (superuser) can.
//!
//! The auth check is the same one the CQL router calls:
//! `Schema::authenticate(user, pw) -> AuthContext`, then
//! `Schema::check_permission(&auth, perm, resource)`. If those two
//! primitives behave correctly, the CQL router is covered too because
//! every handler (INSERT / UPDATE / DELETE / SELECT / DDL) goes through
//! `check_permission` before touching storage — see
//! `ferrosa-cql/src/router.rs` (search for `check_permission(`).
//!
//! The test also exercises `StorageEngineConfig.auth_enabled` so its
//! presence and default are pinned by the type system — a future
//! edit that removes the field will break this test at compile time.

use std::sync::Arc;

use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy, Permission,
    RateLimitConfig, Resource, Schema, SchemaConfig, TestAuditSink,
};

const ADMIN_USER: &str = "ferrosa_admin";
const ADMIN_PASS: &str = "ferrosa_admin";
const APP_USER: &str = "app_reader";
const APP_PASS: &str = "ferrosa_user";

/// Build a Schema configured for auth tests (permissive policy, Bcrypt cost 4
/// so hashing is fast).
fn test_schema() -> Arc<Schema> {
    Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .expect("schema init"),
    )
}

#[test]
fn seed_bootstrap_creates_seed_roles() {
    let schema = test_schema();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();

    let snap = schema.snapshot();
    let admin = snap
        .roles
        .get(ADMIN_USER)
        .expect("ferrosa_admin must be created by bootstrap");
    assert!(
        admin.is_superuser,
        "ferrosa_admin must be a superuser (has SUPERUSER=true)"
    );
    assert!(admin.can_login, "ferrosa_admin must have LOGIN=true");
    assert!(
        admin.salted_hash.is_some(),
        "ferrosa_admin must have a stored password hash"
    );

    let app = snap
        .roles
        .get(APP_USER)
        .expect("app_reader must be created by bootstrap");
    assert!(
        !app.is_superuser,
        "app_reader must NOT be a superuser (principle of least privilege)"
    );
    assert!(app.can_login, "app_reader must have LOGIN=true");
    assert!(
        app.salted_hash.is_some(),
        "app_reader must have a stored password hash"
    );
}

#[test]
fn seed_bootstrap_is_idempotent() {
    let schema = test_schema();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();
    // Second call must not fail and must not duplicate roles.
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();
    let snap = schema.snapshot();
    assert!(snap.roles.contains_key(ADMIN_USER));
    assert!(snap.roles.contains_key(APP_USER));
}

#[test]
fn unprivileged_user_can_authenticate_with_password() {
    let schema = test_schema();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();

    let auth = schema
        .authenticate(APP_USER, APP_PASS)
        .expect("app_reader must authenticate with the seeded password");
    assert_eq!(auth.role, APP_USER);
    assert!(!auth.is_superuser);
}

#[test]
fn superuser_can_authenticate_with_password() {
    let schema = test_schema();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();

    let auth = schema
        .authenticate(ADMIN_USER, ADMIN_PASS)
        .expect("ferrosa_admin must authenticate with the seeded password");
    assert_eq!(auth.role, ADMIN_USER);
    assert!(auth.is_superuser);
}

#[test]
fn unprivileged_user_cannot_modify_graph_table() {
    // End-to-end equivalent of the CQL router's INSERT/DELETE/UPDATE path:
    // the router calls `check_permission(Modify, Table(agent_memory, typed_edges))`
    // before executing any write. This test asserts that call denies
    // the unprivileged role.
    let schema = test_schema();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();

    let auth = schema.authenticate(APP_USER, APP_PASS).unwrap();
    let resource = Resource::Table("agent_memory".into(), "typed_edges".into());
    let result = schema.check_permission(&auth, Permission::Modify, &resource);
    assert!(
        result.is_err(),
        "app_reader must be denied MODIFY on agent_memory.typed_edges \
         (the graph-owned tables are the whole point of the isolation)"
    );
}

#[test]
fn unprivileged_user_can_select_graph_table() {
    // Per the design doc, app_reader has SELECT on the whole keyspace
    // so it can read graph state — only writes are denied. In this test
    // we seed a keyspace-level SELECT grant to match what ddl/100_roles.cql
    // will apply on a real cluster.
    let schema = test_schema();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();

    // Grant SELECT on the whole agent_memory keyspace to app_reader.
    // This is the same grant the DDL file applies at cluster bootstrap.
    let mut perms = std::collections::HashSet::new();
    perms.insert(Permission::Select);
    schema
        .grant_internal(ferrosa_schema::GrantEntry {
            role: APP_USER.into(),
            resource: Resource::Keyspace("agent_memory".into()),
            permissions: perms,
        })
        .unwrap();

    let auth = schema.authenticate(APP_USER, APP_PASS).unwrap();
    let resource = Resource::Table("agent_memory".into(), "typed_edges".into());
    let result = schema.check_permission(&auth, Permission::Select, &resource);
    assert!(
        result.is_ok(),
        "app_reader must be allowed SELECT on agent_memory.typed_edges: {:?}",
        result.err()
    );
}

#[test]
fn superuser_can_modify_graph_table() {
    let schema = test_schema();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();

    let auth = schema.authenticate(ADMIN_USER, ADMIN_PASS).unwrap();
    let resource = Resource::Table("agent_memory".into(), "typed_edges".into());
    let result = schema.check_permission(&auth, Permission::Modify, &resource);
    assert!(
        result.is_ok(),
        "ferrosa_admin (superuser) must bypass all permission checks: {:?}",
        result.err()
    );
}

#[test]
fn storage_engine_config_has_auth_enabled_field_with_false_default() {
    // Pins the new StorageEngineConfig.auth_enabled field at compile time.
    // `test_config()` and `from_env()` must both default to false so that
    // no existing test or deployment sees a behavior change.
    let dir = tempfile::tempdir().unwrap();
    let cfg = ferrosa_storage::StorageEngineConfig::test_config(dir.path());
    assert!(
        !cfg.auth_enabled,
        "auth_enabled must default to false for backward compat"
    );
}
