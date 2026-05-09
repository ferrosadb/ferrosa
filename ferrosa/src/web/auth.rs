//! Basic authentication middleware for the web observability console.
//!
//! Extracts `Authorization: Basic <b64>` headers, authenticates against the
//! schema role registry, and checks for admin/operator membership via the
//! `member_of` chain with cycle detection.

use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine as _;
use serde::Serialize;

use ferrosa_schema::{Schema, SchemaSnapshot};

use super::WebAppState;

/// JSON error body returned on authentication/authorization failure.
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// Axum middleware for Basic authentication.
///
/// Designed for use with `axum::middleware::from_fn_with_state`.
///
/// - If `state.auth_disabled` is true, the request passes through.
/// - Extracts and decodes the `Authorization: Basic` header.
/// - Authenticates via `Schema::authenticate`.
/// - Superusers pass immediately.
/// - Non-superusers must belong to "admin" or "operator" (directly or
///   transitively via `member_of`).
/// - On success, injects `AuthContext` into request extensions.
pub async fn auth_middleware(
    State(state): State<WebAppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    // Bypass auth when disabled (e.g. development mode).
    if state.auth_disabled {
        return next.run(req).await;
    }

    // Extract the Authorization header.
    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let Some(auth_value) = auth_header else {
        return unauthorized("missing Authorization header");
    };

    let Some(encoded) = auth_value.strip_prefix("Basic ") else {
        return unauthorized("unsupported auth scheme (expected Basic)");
    };

    let decoded = match base64::engine::general_purpose::STANDARD.decode(encoded.trim()) {
        Ok(bytes) => bytes,
        Err(_) => {
            return unauthorized("invalid base64 in Authorization header");
        }
    };

    let decoded_str = match String::from_utf8(decoded) {
        Ok(s) => s,
        Err(_) => {
            return unauthorized("invalid UTF-8 in credentials");
        }
    };

    let Some((username, password)) = decoded_str.split_once(':') else {
        return unauthorized("invalid credentials format");
    };

    // Authenticate against the schema.
    let auth_ctx = match state.schema.authenticate(username, password) {
        Ok(ctx) => ctx,
        Err(_) => {
            return unauthorized("authentication failed");
        }
    };

    // Superusers always pass.
    if auth_ctx.is_superuser {
        req.extensions_mut().insert(auth_ctx);
        return next.run(req).await;
    }

    // Check role chain for admin or operator membership.
    if has_admin_or_operator_role(&state.schema, &auth_ctx.role) {
        req.extensions_mut().insert(auth_ctx);
        return next.run(req).await;
    }

    forbidden(&auth_ctx.role)
}

/// Check whether a role has "admin" or "operator" privileges, either
/// directly (the role name itself) or transitively via `member_of`.
fn has_admin_or_operator_role(schema: &Arc<Schema>, role_name: &str) -> bool {
    // Direct name match.
    if role_name == "admin" || role_name == "operator" {
        return true;
    }

    let snap = schema.snapshot();
    let mut visited = HashSet::new();
    check_role_chain(&snap, role_name, &mut visited)
}

/// Recursively walk `member_of` to find "admin" or "operator".
///
/// Uses `visited` for cycle detection — if a role has already been visited,
/// we skip it to prevent infinite loops.
fn check_role_chain(snap: &SchemaSnapshot, role_name: &str, visited: &mut HashSet<String>) -> bool {
    if !visited.insert(role_name.to_string()) {
        // Already visited — cycle detected.
        return false;
    }

    let Some(role_meta) = snap.roles.get(role_name) else {
        return false;
    };

    for parent in &role_meta.member_of {
        if parent == "admin" || parent == "operator" {
            return true;
        }
        if check_role_chain(snap, parent, visited) {
            return true;
        }
    }

    false
}

/// Return a 401 Unauthorized response with the `WWW-Authenticate` header.
fn unauthorized(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"ferrosa\"")],
        Json(ErrorBody {
            error: message.to_string(),
        }),
    )
        .into_response()
}

/// Return a 403 Forbidden response indicating insufficient privileges.
fn forbidden(role: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(ErrorBody {
            error: format!("role '{role}' lacks admin or operator privileges"),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::Router;
    use ferrosa_cluster::ModeController;
    use ferrosa_net::rpc::HandlerRegistry;
    use ferrosa_schema::DeploymentMode;
    use ferrosa_schema::{
        AuthContext, AuthMethod, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, RoleMetadata, SchemaConfig, TestAuditSink, VirtualTableRegistry,
    };
    use ferrosa_storage::commitlog::CommitLogConfig;
    use ferrosa_storage::compaction::CompactionConfig;
    use ferrosa_storage::{StorageEngine, StorageEngineConfig};
    use tower::ServiceExt;

    /// Simple handler that returns 200 OK.
    async fn ok_handler() -> &'static str {
        "ok"
    }

    /// Build a Schema suitable for cross-crate tests.
    fn test_schema() -> Schema {
        // Safety: test-only — clearing env var for hermetic tests.
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
        .expect("test schema construction must not fail")
    }

    /// Build a test router wrapped with auth middleware.
    fn test_router(auth_disabled: bool) -> (Router, Arc<Schema>) {
        let schema = Arc::new(test_schema());

        let dir = tempfile::tempdir().expect("tempdir");
        let storage_config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.path().join("commitlog"),
                checkpoint_dir: dir.path().join("commitlog"),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
        };
        let storage = Arc::new(StorageEngine::new(storage_config, None).expect("storage engine"));

        let registry = Arc::new(HandlerRegistry::new());
        let host_id = uuid::Uuid::new_v4();
        let (mode_controller, _handles) = ModeController::new(
            Arc::new(ferrosa_cluster::ClusterConfig::default()),
            Arc::new(ferrosa_net::config::NetConfig::default()),
            host_id,
            storage.clone(),
            schema.clone(),
            registry,
        );

        let state = WebAppState {
            registry: Arc::new(VirtualTableRegistry::new()),
            mode_controller,
            schema: schema.clone(),
            storage,
            host_id,
            auth_disabled,
            debug: None,
        };

        let router = Router::new()
            .route("/test", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);

        (router, schema)
    }

    /// Encode credentials as a Basic auth header value.
    fn basic_auth_header(user: &str, pass: &str) -> String {
        use base64::engine::general_purpose::STANDARD;
        let encoded = STANDARD.encode(format!("{user}:{pass}"));
        format!("Basic {encoded}")
    }

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn unauthenticated_returns_401() {
        let (router, _) = test_router(false);
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(resp.headers().contains_key(header::WWW_AUTHENTICATE));
    }

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn superuser_cassandra_returns_200() {
        let (router, _) = test_router(false);
        let req = Request::builder()
            .uri("/test")
            .header(
                header::AUTHORIZATION,
                basic_auth_header("cassandra", "cassandra"),
            )
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn bad_credentials_returns_401() {
        let (router, _) = test_router(false);
        let req = Request::builder()
            .uri("/test")
            .header(
                header::AUTHORIZATION,
                basic_auth_header("cassandra", "wrongpass"),
            )
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn auth_disabled_passes_through() {
        let (router, _) = test_router(true);
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn non_privileged_role_returns_403() {
        let (router, schema) = test_router(false);

        // Create a login-capable role with no admin/operator membership.
        let superuser_ctx = AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        let role = RoleMetadata {
            name: "viewer".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: HashSet::new(),
        };
        schema
            .create_role(role, Some("viewerpass"), &superuser_ctx)
            .expect("create viewer role");

        let req = Request::builder()
            .uri("/test")
            .header(
                header::AUTHORIZATION,
                basic_auth_header("viewer", "viewerpass"),
            )
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    #[serial_test::serial(env)]
    async fn operator_role_via_member_of_returns_200() {
        let (router, schema) = test_router(false);

        let superuser_ctx = AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };

        // Create the "operator" group role (no login needed).
        let operator_role = RoleMetadata {
            name: "operator".to_string(),
            is_superuser: false,
            can_login: false,
            salted_hash: None,
            member_of: HashSet::new(),
        };
        schema
            .create_role(operator_role, None, &superuser_ctx)
            .expect("create operator role");

        // Create "ops_user" as member of "operator".
        let mut member_of = HashSet::new();
        member_of.insert("operator".to_string());
        let ops_user = RoleMetadata {
            name: "ops_user".to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of,
        };
        schema
            .create_role(ops_user, Some("opspass123"), &superuser_ctx)
            .expect("create ops_user role");

        let req = Request::builder()
            .uri("/test")
            .header(
                header::AUTHORIZATION,
                basic_auth_header("ops_user", "opspass123"),
            )
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
