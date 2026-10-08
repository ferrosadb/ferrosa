//! Basic authentication middleware for the web observability console.
//!
//! Extracts `Authorization: Basic <b64>` headers, authenticates against the
//! schema role registry, and checks for admin/operator membership via the
//! `member_of` chain with cycle detection.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

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

    // Authenticate against the schema — but reuse a recent successful
    // verification when the schema has not moved on since it was made.
    //
    // The credential check is a bcrypt `cost=12` comparison (~0.18 s). It runs
    // on the blocking pool, never on a tokio worker: `Schema::authenticate` is
    // synchronous, and the CQL login path offloads it the same way
    // (`authenticate_off_runtime`, `ferrosa-cql/src/connection.rs`).
    //
    // Only a *successful* verification is cached, so a wrong password always
    // pays a full bcrypt and guessing costs are unchanged. A hit additionally
    // requires the schema snapshot the credential was verified against to still
    // be the live one (`Arc::ptr_eq` inside `AuthCache::get`), so any password,
    // role or grant change invalidates the entry immediately — authorization is
    // never cached. Capture the snapshot *before* verifying and bind the entry
    // to it: if the schema mutates during the bcrypt, the entry is simply not
    // served again rather than trusted against the newer snapshot.
    let live = state.schema.snapshot();
    let auth_ctx = match state
        .auth_cache
        .get(username, password, &live, Instant::now())
    {
        Some(ctx) => ctx,
        None => {
            let schema = state.schema.clone();
            let user = username.to_string();
            let pass = password.to_string();
            let verified = match tokio::task::spawn_blocking(move || {
                schema.authenticate(&user, &pass)
            })
            .await
            {
                Ok(Ok(ctx)) => ctx,
                _ => return unauthorized("authentication failed"),
            };
            state
                .auth_cache
                .insert(username, password, live, verified.clone(), Instant::now());
            verified
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
        let schema = Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .expect("test schema construction must not fail");
        ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();
        schema
    }

    /// Build a `WebAppState` with the auth middleware's cache injected, so a
    /// test can observe how many credentials were actually re-verified.
    fn test_state_with_cache(
        auth_disabled: bool,
        cache: Arc<crate::web::auth_cache::AuthCache>,
    ) -> (WebAppState, Arc<Schema>) {
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
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
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
            auth_cache: cache,
            debug: None,
            listeners: std::sync::Arc::new(crate::listener_status::ListenerStatus::default()),
            supervision: std::sync::Arc::new(crate::supervisor::SupervisionStatus::default()),
        };

        (state, schema)
    }

    /// Build a test router wrapped with auth middleware (default cache).
    fn test_router(auth_disabled: bool) -> (Router, Arc<Schema>) {
        let cache = Arc::new(crate::web::auth_cache::AuthCache::default());
        let (state, schema) = test_state_with_cache(auth_disabled, cache);
        (router_with_auth(state), schema)
    }

    /// Build a test router with a caller-supplied cache, so a test can assert on
    /// `AuthCache::misses()` after driving requests through the middleware.
    fn test_router_with_cache(
        auth_disabled: bool,
        cache: Arc<crate::web::auth_cache::AuthCache>,
    ) -> (Router, Arc<Schema>) {
        let (state, schema) = test_state_with_cache(auth_disabled, cache);
        (router_with_auth(state), schema)
    }

    /// Wrap `state` in a minimal router whose only route sits behind
    /// `auth_middleware`.
    fn router_with_auth(state: WebAppState) -> Router {
        Router::new()
            .route("/test", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
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
    async fn seeded_ferrosa_admin_returns_200() {
        let (router, _) = test_router(false);
        let req = Request::builder()
            .uri("/test")
            .header(
                header::AUTHORIZATION,
                basic_auth_header("ferrosa_admin", "ferrosa_admin"),
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
            scram: None,
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

    /// Build a minimal `WebAppState` similar to the one in `web/api.rs` tests,
    /// for use in /admin auth tests that require the full router.
    fn make_state_for_admin_tests(auth_disabled: bool) -> crate::web::WebAppState {
        use ferrosa_cluster::ModeController;
        use ferrosa_net::rpc::HandlerRegistry;
        use ferrosa_storage::commitlog::CommitLogConfig;
        use ferrosa_storage::compaction::CompactionConfig;
        use ferrosa_storage::{StorageEngine, StorageEngineConfig};

        // Safety: test-only — clearing env var for hermetic tests.
        unsafe {
            std::env::remove_var("FERROSA_SUPERUSER_PASSWORD");
        }

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
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
        };
        let storage =
            std::sync::Arc::new(StorageEngine::new(storage_config, None).expect("storage engine"));
        let schema = std::sync::Arc::new(
            ferrosa_schema::Schema::new(ferrosa_schema::SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: ferrosa_schema::PasswordPolicy::permissive(),
                auth_method: ferrosa_schema::AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .expect("test schema"),
        );
        ferrosa_schema::auth::bootstrap::seed_default_roles(&schema).unwrap();
        let host_id = uuid::Uuid::new_v4();
        let registry = std::sync::Arc::new(HandlerRegistry::new());
        let (mode_controller, _handles) = ModeController::new(
            std::sync::Arc::new(ferrosa_cluster::ClusterConfig::default()),
            std::sync::Arc::new(ferrosa_net::config::NetConfig::default()),
            host_id,
            storage.clone(),
            schema.clone(),
            registry,
        );
        crate::web::WebAppState {
            registry: std::sync::Arc::new(ferrosa_schema::VirtualTableRegistry::new()),
            mode_controller,
            schema,
            storage,
            host_id,
            auth_disabled,
            auth_cache: std::sync::Arc::new(crate::web::auth_cache::AuthCache::default()),
            debug: None,
            listeners: std::sync::Arc::new(crate::listener_status::ListenerStatus::default()),
            supervision: std::sync::Arc::new(crate::supervisor::SupervisionStatus::default()),
        }
    }

    // B1: /admin/* returns 401 without credentials when auth is enabled
    #[tokio::test]
    #[serial_test::serial(env)]
    async fn admin_route_returns_401_without_credentials() {
        let state = make_state_for_admin_tests(false);
        let router = crate::web::build_router(state);
        let req = axum::http::Request::builder()
            .uri("/admin/membership-snapshot")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = tower::ServiceExt::oneshot(router, req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::UNAUTHORIZED,
            "GET /admin/membership-snapshot must return 401 when no credentials are provided"
        );
    }

    // B2: /admin/* returns 200 with valid superuser credentials
    #[tokio::test]
    #[serial_test::serial(env)]
    async fn admin_route_returns_200_with_superuser_credentials() {
        let state = make_state_for_admin_tests(false);
        let router = crate::web::build_router(state);
        let req = axum::http::Request::builder()
            .uri("/admin/membership-snapshot")
            .header(
                axum::http::header::AUTHORIZATION,
                basic_auth_header("ferrosa_admin", "ferrosa_admin"),
            )
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = tower::ServiceExt::oneshot(router, req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::OK,
            "GET /admin/membership-snapshot must return 200 with valid superuser credentials"
        );
    }

    // B3: /admin/* returns 200 when auth_disabled=true (dev mode bypass)
    #[tokio::test]
    #[serial_test::serial(env)]
    async fn admin_route_open_when_auth_disabled() {
        let state = make_state_for_admin_tests(true);
        let router = crate::web::build_router(state);
        let req = axum::http::Request::builder()
            .uri("/admin/membership-snapshot")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = tower::ServiceExt::oneshot(router, req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::OK,
            "GET /admin/membership-snapshot must return 200 when auth_disabled=true (dev mode)"
        );
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
            scram: None,
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
            scram: None,
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

    // -----------------------------------------------------------------------
    // Auth-cache integration: the middleware must reuse a successful
    // verification and offload the bcrypt, without changing observable
    // behaviour (same 200/401/403, authorization still live).
    // -----------------------------------------------------------------------

    /// A successful verification is reused: two authenticated requests with the
    /// same credentials cause exactly one `Schema::authenticate` (one miss).
    #[tokio::test]
    #[serial_test::serial(env)]
    async fn successful_verification_is_reused_across_requests() {
        let cache = Arc::new(crate::web::auth_cache::AuthCache::new(
            std::time::Duration::from_secs(60),
            16,
        ));
        let (router, _) = test_router_with_cache(false, cache.clone());

        let request = || {
            Request::builder()
                .uri("/test")
                .header(
                    header::AUTHORIZATION,
                    basic_auth_header("ferrosa_admin", "ferrosa_admin"),
                )
                .body(Body::empty())
                .unwrap()
        };

        let first = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            cache.misses(),
            1,
            "the first request must verify the credential (one miss)"
        );

        let second = router.oneshot(request()).await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(
            cache.misses(),
            1,
            "the second request must be served from the cache — no second bcrypt"
        );
    }

    /// A failed verification is never cached: a wrong password pays a full
    /// bcrypt every time and still returns 401.
    #[tokio::test]
    #[serial_test::serial(env)]
    async fn failed_verification_is_never_cached() {
        let cache = Arc::new(crate::web::auth_cache::AuthCache::new(
            std::time::Duration::from_secs(60),
            16,
        ));
        let (router, _) = test_router_with_cache(false, cache.clone());

        let request = || {
            Request::builder()
                .uri("/test")
                .header(
                    header::AUTHORIZATION,
                    basic_auth_header("cassandra", "wrongpass"),
                )
                .body(Body::empty())
                .unwrap()
        };

        let first = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::UNAUTHORIZED);
        let second = router.oneshot(request()).await.unwrap();
        assert_eq!(second.status(), StatusCode::UNAUTHORIZED);

        assert_eq!(
            cache.misses(),
            2,
            "both wrong-password requests must re-verify; failures are never cached"
        );
        assert_eq!(cache.len(), 0, "a failed verification stores nothing");
    }

    /// A schema change invalidates: once a new snapshot is installed, the same
    /// credentials miss and are re-verified against the live schema.
    #[tokio::test]
    #[serial_test::serial(env)]
    async fn schema_change_invalidates_cached_verification() {
        let cache = Arc::new(crate::web::auth_cache::AuthCache::new(
            std::time::Duration::from_secs(3600),
            16,
        ));
        let (router, schema) = test_router_with_cache(false, cache.clone());

        let request = || {
            Request::builder()
                .uri("/test")
                .header(
                    header::AUTHORIZATION,
                    basic_auth_header("ferrosa_admin", "ferrosa_admin"),
                )
                .body(Body::empty())
                .unwrap()
        };

        let first = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let second = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(cache.misses(), 1, "the second request must have been a hit");

        // Any mutation installs a fresh snapshot (`Arc`). The TTL is an hour, so
        // only the snapshot change can explain the miss that follows.
        let superuser_ctx = AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        schema
            .create_role(
                RoleMetadata {
                    name: "viewer".to_string(),
                    is_superuser: false,
                    can_login: true,
                    salted_hash: None,
                    member_of: HashSet::new(),
                    scram: None,
                },
                Some("viewerpass"),
                &superuser_ctx,
            )
            .expect("create viewer role");

        let third = router.oneshot(request()).await.unwrap();
        assert_eq!(third.status(), StatusCode::OK);
        assert_eq!(
            cache.misses(),
            2,
            "after the schema moved on, the credential must be re-verified"
        );
    }

    /// TTL `0` disables the cache: two requests verify twice.
    #[tokio::test]
    #[serial_test::serial(env)]
    async fn zero_ttl_disables_the_cache() {
        let cache = Arc::new(crate::web::auth_cache::AuthCache::new(
            std::time::Duration::ZERO,
            16,
        ));
        let (router, _) = test_router_with_cache(false, cache.clone());

        let request = || {
            Request::builder()
                .uri("/test")
                .header(
                    header::AUTHORIZATION,
                    basic_auth_header("ferrosa_admin", "ferrosa_admin"),
                )
                .body(Body::empty())
                .unwrap()
        };

        let first = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let second = router.oneshot(request()).await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);

        assert_eq!(
            cache.misses(),
            2,
            "a disabled cache must verify on every request"
        );
        assert_eq!(cache.len(), 0, "a disabled cache stores nothing");
    }
}
