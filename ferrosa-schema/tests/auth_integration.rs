//! Integration tests for bootstrap, production mode, and hash filtering.
//!
//! Tests that manipulate environment variables are serialized with `ENV_LOCK`
//! to prevent data races between parallel test threads.

use std::collections::HashSet;
use std::sync::Mutex;

use ferrosa_schema::*;

/// Mutex to serialize tests that manipulate the FERROSA_SUPERUSER_PASSWORD env var.
static ENV_LOCK: Mutex<()> = Mutex::new(());

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

fn test_schema() -> Schema {
    // Safety: env var mutation is not thread-safe; caller must hold ENV_LOCK.
    unsafe {
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }
    Schema::new(test_config()).unwrap()
}

fn superuser_auth() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn test_role(name: &str) -> RoleMetadata {
    RoleMetadata {
        name: name.to_string(),
        is_superuser: false,
        can_login: true,
        salted_hash: None,
        member_of: HashSet::new(),
    }
}

#[test]
fn bootstrap_with_env_password_no_must_change() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Safety: env var mutation is not thread-safe; protected by ENV_LOCK.
    unsafe {
        std::env::set_var("FERROSA_SUPERUSER_PASSWORD", "Str0ng!P@sswd");
    }
    let schema = Schema::new(test_config()).unwrap();
    let ctx = schema.authenticate("cassandra", "Str0ng!P@sswd").unwrap();
    assert!(!ctx.must_change_password);
    unsafe {
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }
}

#[test]
fn bootstrap_without_env_password_must_change() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Safety: env var mutation is not thread-safe; protected by ENV_LOCK.
    unsafe {
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }
    let schema = Schema::new(test_config()).unwrap();
    let ctx = schema.authenticate("cassandra", "cassandra").unwrap();
    assert!(ctx.must_change_password);
}

#[test]
fn production_mode_rejects_weak_password_policy() {
    let config = ProductionCheckConfig {
        mode: DeploymentMode::Production,
        password_policy: PasswordPolicy::permissive(),
        has_superuser_password: true,
        secrets_provider_type: "aws-sm".into(), // pragma: allowlist secret
        s3_allow_http: false,
    };
    let violations = validate_production_requirements(&config);
    assert!(violations
        .iter()
        .any(|v| matches!(v, ProductionViolation::PasswordPolicyBelowMinimum)));
}

#[test]
fn development_mode_allows_permissive_policy() {
    let config = ProductionCheckConfig {
        mode: DeploymentMode::Development,
        password_policy: PasswordPolicy::permissive(),
        has_superuser_password: false,
        secrets_provider_type: "env".into(), // pragma: allowlist secret
        s3_allow_http: true,
    };
    let violations = validate_production_requirements(&config);
    assert!(
        violations.is_empty(),
        "development mode should return no violations"
    );
}

#[test]
fn hash_filtering_integration() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let schema = test_schema();
    let su = superuser_auth();
    schema
        .create_role(test_role("viewer"), Some("ViewP@ss1!"), &su)
        .unwrap();
    let snap = schema.snapshot();

    // Superuser sees hashes
    let su_rows = query_roles(&snap, &su);
    assert!(su_rows
        .iter()
        .any(|r| r.role == "viewer" && r.salted_hash.is_some()));

    // Non-superuser does not
    let viewer_auth = schema.authenticate("viewer", "ViewP@ss1!").unwrap();
    let viewer_rows = query_roles(&snap, &viewer_auth);
    for row in &viewer_rows {
        assert!(
            row.salted_hash.is_none(),
            "non-superuser should not see hashes"
        );
    }
}
