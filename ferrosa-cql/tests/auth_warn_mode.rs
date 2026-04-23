//! Tests for CQL auth warn-mode (Sprint D of
//! `specs/decisions/design-cql-role-auth-rollout.md`).
//!
//! Warn mode (`FERROSA_AUTH_WARN=true`) converts would-be denials into
//! `WARN` log lines + an atomic counter bump while still permitting the
//! request. This is the soak-before-enforce step of the CQL role-auth
//! rollout.
//!
//! These tests exercise the helper `ferrosa_cql::auth::enforce_permission`
//! directly — they do NOT spin up a full CQL server. The helper is
//! the single choke point every router site will funnel through, so
//! covering it in isolation is the fastest way to lock the behaviour in.
//!
//! The `contradictory_config_logs_error_at_startup` test pokes the
//! `log_auth_warn_state` helper in `ferrosa-storage` (also exported) so
//! the ERROR-level log for the nonsensical
//! `auth_enabled=false, auth_warn=true` combination is covered.

use std::collections::HashSet;
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};

use ferrosa_cql::auth::{
    __clear_warn_denial_counters_for_tests, enforce_permission, warn_denial_stats,
};
use ferrosa_cql::error::CqlError;
use ferrosa_schema::auth::permission::{GrantEntry, Permission, Resource};
use ferrosa_schema::auth::role::{AuthContext, RoleMetadata};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};

use tracing::subscriber::with_default;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

// ── Log capture helper ──────────────────────────────────────────────────

/// Thread-safe buffer that satisfies `MakeWriter` so we can scrape emitted
/// log lines in-process.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

struct LogWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(Arc::clone(&self.0))
    }
}

fn with_log_capture<R>(f: impl FnOnce(&CapturedLogs) -> R) -> (R, String) {
    let captured = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_max_level(Level::TRACE)
        .with_ansi(false)
        .finish();
    let r = with_default(subscriber, || f(&captured));
    let logs = captured.contents();
    (r, logs)
}

fn auth_warn_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

// ── Schema fixture ─────────────────────────────────────────────────────

fn make_schema() -> Schema {
    // SAFETY: tests set env vars inside a single-threaded cargo test
    // worker; the `unsafe` is the workspace's standard pattern (see
    // `registry.rs::new_for_test`).
    unsafe {
        std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
    }
    Schema::new(SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    })
    .expect("schema construction")
}

fn seed_role(schema: &Schema, role: &str, grants: Vec<GrantEntry>) {
    schema
        .create_role_internal(RoleMetadata {
            name: role.to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        })
        .expect("seed role");
    for grant in grants {
        schema.grant_internal(grant).expect("seed grant");
    }
}

fn normal_ctx(role: &str) -> AuthContext {
    AuthContext {
        role: role.to_string(),
        is_superuser: false,
        must_change_password: false,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[test]
fn auth_warn_permits_but_logs_on_denial() {
    let _guard = auth_warn_test_guard();
    __clear_warn_denial_counters_for_tests();

    let schema = make_schema();
    // Role exists with NO grants on the target table.
    seed_role(&schema, "ferrosa_user", vec![]);
    let ctx = normal_ctx("ferrosa_user");
    let resource = Resource::Table("agent_memory".into(), "typed_edges".into());

    let (result, logs) = with_log_capture(|_| {
        enforce_permission(
            &schema,
            &ctx,
            Permission::Modify,
            &resource,
            /*auth_warn=*/ true,
        )
    });

    assert!(
        result.is_ok(),
        "warn mode must permit the request; got {result:?}"
    );

    assert!(
        logs.contains("WARN MODE"),
        "log line must be tagged 'WARN MODE' so operators can grep for soak-mode denials. got:\n{logs}"
    );
    assert!(
        logs.contains("ferrosa_user"),
        "log must mention the role. got:\n{logs}"
    );
    assert!(
        logs.contains("MODIFY"),
        "log must mention the permission. got:\n{logs}"
    );
    assert!(
        logs.contains("typed_edges"),
        "log must mention the resource. got:\n{logs}"
    );

    let snap = warn_denial_stats();
    assert_eq!(snap.total, 1, "counter must have ticked once, got {snap:?}");
    assert_eq!(snap.by_role.get("ferrosa_user").copied(), Some(1));
    assert_eq!(
        snap.by_resource
            .get("table agent_memory.typed_edges")
            .copied(),
        Some(1),
        "resource key uses Display form: {snap:?}"
    );
}

#[test]
fn auth_warn_false_returns_err_on_denial() {
    let _guard = auth_warn_test_guard();
    __clear_warn_denial_counters_for_tests();

    let schema = make_schema();
    seed_role(&schema, "stranger", vec![]);
    let ctx = normal_ctx("stranger");
    let resource = Resource::Table("agent_memory".into(), "typed_edges".into());

    let result = enforce_permission(
        &schema,
        &ctx,
        Permission::Modify,
        &resource,
        /*auth_warn=*/ false,
    );

    match result {
        Err(CqlError::Unauthorized(msg)) => {
            assert!(msg.contains("stranger"), "err must mention role: {msg}");
            assert!(msg.contains("MODIFY"), "err must mention perm: {msg}");
        }
        other => panic!("expected Err(Unauthorized), got {other:?}"),
    }

    let snap = warn_denial_stats();
    assert_eq!(
        snap.total, 0,
        "counter must NOT be bumped in enforcement mode; got {snap:?}"
    );
}

#[test]
fn auth_warn_does_not_fire_when_permitted() {
    let _guard = auth_warn_test_guard();
    __clear_warn_denial_counters_for_tests();

    let schema = make_schema();
    let resource = Resource::Table("agent_memory".into(), "entity_store".into());
    // Seed a role that actually has MODIFY on the target table.
    seed_role(
        &schema,
        "writer",
        vec![GrantEntry {
            role: "writer".into(),
            resource: resource.clone(),
            permissions: [Permission::Modify].into_iter().collect(),
        }],
    );
    let ctx = normal_ctx("writer");

    // First: auth_warn = true — should be a pure allow, no counter bump.
    let r1 = enforce_permission(&schema, &ctx, Permission::Modify, &resource, true);
    // Second: auth_warn = false — also a pure allow, no counter bump.
    let r2 = enforce_permission(&schema, &ctx, Permission::Modify, &resource, false);

    assert!(r1.is_ok(), "permitted request must return Ok in warn mode");
    assert!(
        r2.is_ok(),
        "permitted request must return Ok in enforcement mode"
    );

    let snap = warn_denial_stats();
    assert_eq!(
        snap.total, 0,
        "permitted requests must not increment the would-be-denial counter; got {snap:?}"
    );
}

#[test]
fn contradictory_config_logs_error_at_startup() {
    let _guard = auth_warn_test_guard();
    // `auth_enabled=false, auth_warn=true` is nonsensical: auth isn't
    // checked, so there is nothing to warn about. The helper must log
    // at ERROR level so an operator paging through startup logs sees it.
    let (_, logs) = with_log_capture(|_| {
        ferrosa_storage::engine::log_auth_warn_state(
            /*auth_enabled=*/ false, /*auth_warn=*/ true,
        );
    });

    assert!(
        logs.contains("ERROR"),
        "contradictory config must log at ERROR level. got:\n{logs}"
    );
    assert!(
        logs.contains("FERROSA_AUTH_WARN"),
        "error must name the offending env var. got:\n{logs}"
    );
    assert!(
        logs.contains("FERROSA_AUTH_ENABLED"),
        "error must name the dependency that's missing. got:\n{logs}"
    );
    assert!(
        logs.to_lowercase().contains("ignor"),
        "error must say auth_warn is being IGNORED so operators know the effective behavior. got:\n{logs}"
    );
}
