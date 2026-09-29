//! SPARQL HTTP endpoint (W3C SPARQL Protocol).
//!
//! Implements the SPARQL 1.1 Protocol over HTTP:
//! - `POST /sparql` with `application/sparql-query` content type
//! - `GET /sparql?query=...` for URL-encoded queries
//! - `GET /sparql/health` for health checks

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Query as AxumQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use base64::Engine as _;
use ferrosa_schema::auth::permission::{Permission, Resource};
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;

use crate::engine::SparqlEngine;

/// HTTP server configuration.
#[derive(Debug, Clone)]
pub struct SparqlHttpConfig {
    pub bind_addr: SocketAddr,
    /// PEM certificate chain (`[sparql] tls_cert`). With `tls_key_path`,
    /// the endpoint serves HTTPS only (t_d5d122ba).
    pub tls_cert_path: Option<String>,
    /// PEM private key (`[sparql] tls_key`).
    pub tls_key_path: Option<String>,
    /// Refuse to start without a certificate (`[sparql] require_tls`).
    pub require_tls: bool,
}

impl Default for SparqlHttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 8080)),
            tls_cert_path: None,
            tls_key_path: None,
            require_tls: false,
        }
    }
}

/// Shared state for the HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<SparqlEngine>,
    pub schema: Arc<Schema>,
    pub auth_disabled: bool,
}

/// Start the SPARQL HTTP server.
pub async fn start_sparql_http(config: &SparqlHttpConfig, state: AppState) -> std::io::Result<()> {
    serve_app(config, build_router(state)).await
}

/// Bind `config.bind_addr` and serve `app` — over TLS when a certificate is
/// configured, built by the shared `ferrosa_net::tls` builder (one crypto
/// provider for every listener). `require_tls` without a certificate, or only
/// one of cert/key, is an error rather than a plaintext fallback.
async fn serve_app(config: &SparqlHttpConfig, app: Router) -> std::io::Result<()> {
    let tls = ferrosa_net::tls::optional_server_config(
        "SPARQL",
        config.tls_cert_path.as_deref(),
        config.tls_key_path.as_deref(),
        config.require_tls,
        ferrosa_net::tls::HTTP_ALPN,
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    match tls {
        Some(tls) => {
            tracing::info!(addr = %config.bind_addr, "SPARQL HTTPS server listening");
            axum_server::bind_rustls(
                config.bind_addr,
                axum_server::tls_rustls::RustlsConfig::from_config(tls),
            )
            .serve(app.into_make_service())
            .await
        }
        None => {
            let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
            tracing::info!(addr = %config.bind_addr, "SPARQL HTTP server listening (plain)");
            axum::serve(listener, app).await
        }
    }
}

/// Maximum query body size: 1 MiB. Prevents DoS via oversized requests (BUG-S17).
const MAX_QUERY_BODY: usize = 1024 * 1024;

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/sparql", post(handle_sparql_post))
        .route("/sparql", get(handle_sparql_get))
        .route("/sparql/update", post(handle_sparql_update))
        .route("/sparql/health", get(handle_health))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(
            MAX_QUERY_BODY,
        ))
        .with_state(state)
}

/// POST /sparql — execute a SPARQL query.
///
/// Accepts `application/sparql-query` (raw SPARQL text) or
/// `application/x-www-form-urlencoded` (query=... parameter).
/// Authenticate a SPARQL request via HTTP Basic auth, mirroring the graph/CQL
/// path (t_e2bf1e62, FMEA SP-1). Honors the `auth_disabled` kill-switch by
/// returning a superuser context (same as the CQL/graph behaviour). Returns a
/// 401 `Response` on any failure.
// The Err is an axum `Response` (the ready-to-return 401) — large by nature for
// an HTTP handler helper; boxing it would only complicate the call sites.
#[allow(clippy::result_large_err)]
fn authenticate_sparql(
    auth_disabled: bool,
    schema: &Schema,
    headers: &axum::http::HeaderMap,
) -> Result<AuthContext, Response> {
    if auth_disabled {
        return Ok(AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        });
    }
    let auth_value = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error_response(StatusCode::UNAUTHORIZED, "missing Authorization header"))?;
    let encoded = auth_value.strip_prefix("Basic ").ok_or_else(|| {
        error_response(
            StatusCode::UNAUTHORIZED,
            "unsupported auth scheme (expected Basic)",
        )
    })?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .map_err(|_| {
            error_response(
                StatusCode::UNAUTHORIZED,
                "invalid base64 in Authorization header",
            )
        })?;
    let decoded_str = String::from_utf8(decoded)
        .map_err(|_| error_response(StatusCode::UNAUTHORIZED, "invalid UTF-8 in credentials"))?;
    let (username, password) = decoded_str
        .split_once(':')
        .ok_or_else(|| error_response(StatusCode::UNAUTHORIZED, "invalid credentials format"))?;
    schema
        .authenticate(username, password)
        .map_err(|_| error_response(StatusCode::UNAUTHORIZED, "authentication failed"))
}

/// Authorize the authenticated role for `perm` on the target keyspace, so the
/// attacker-controllable `X-Keyspace` header is no longer the only tenancy
/// boundary. Returns a 403 `Response` on denial.
#[allow(clippy::result_large_err)] // Err is the ready-to-return 403 Response.
fn authorize_keyspace(
    schema: &Schema,
    auth: &AuthContext,
    keyspace: &str,
    perm: Permission,
) -> Result<(), Response> {
    schema
        .check_permission(auth, perm, &Resource::Keyspace(keyspace.to_string()))
        .map_err(|_| {
            error_response(
                StatusCode::FORBIDDEN,
                &format!("permission denied: {perm} on keyspace {keyspace}"),
            )
        })
}

async fn handle_sparql_post(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let auth = match authenticate_sparql(state.auth_disabled, &state.schema, &headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let query_str = match extract_query_from_post(&headers, &body) {
        Ok(q) => q,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };

    let keyspace = headers
        .get("X-Keyspace")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("rdf");

    if let Err(resp) = authorize_keyspace(&state.schema, &auth, keyspace, Permission::Select) {
        return resp;
    }

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    execute_and_respond(&state, &query_str, keyspace, accept).await
}

/// GET /sparql?query=... — execute a SPARQL query via URL parameter.
#[derive(Deserialize)]
struct SparqlGetParams {
    query: String,
    #[serde(default = "default_keyspace")]
    keyspace: String,
}

fn default_keyspace() -> String {
    "rdf".into()
}

async fn handle_sparql_get(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    AxumQuery(params): AxumQuery<SparqlGetParams>,
) -> Response {
    let auth = match authenticate_sparql(state.auth_disabled, &state.schema, &headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) =
        authorize_keyspace(&state.schema, &auth, &params.keyspace, Permission::Select)
    {
        return resp;
    }

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    execute_and_respond(&state, &params.query, &params.keyspace, accept).await
}

/// POST /sparql/update — execute a SPARQL UPDATE (INSERT DATA, DELETE DATA).
async fn handle_sparql_update(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let auth = match authenticate_sparql(state.auth_disabled, &state.schema, &headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let update_str = match extract_query_from_post(&headers, &body) {
        Ok(q) => q,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };

    let keyspace = headers
        .get("X-Keyspace")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("rdf");

    // SPARQL UPDATE is a write — require MODIFY on the keyspace.
    if let Err(resp) = authorize_keyspace(&state.schema, &auth, keyspace, Permission::Modify) {
        return resp;
    }

    match state.engine.execute_update(&update_str, keyspace).await {
        Ok(result) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "triples_inserted": result.triples_inserted,
                "triples_deleted": result.triples_deleted,
            })),
        )
            .into_response(),
        Err(crate::error::SparqlError::Parse(msg)) => error_response(StatusCode::BAD_REQUEST, &msg),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// GET /sparql/health — basic health check.
///
/// Returns service status. When auth is enabled, returns a minimal
/// response that does not reveal internal details.
async fn handle_health(State(state): State<AppState>) -> Response {
    if state.auth_disabled {
        (
            StatusCode::OK,
            Json(serde_json::json!({"status": "ok", "service": "sparql"})),
        )
            .into_response()
    } else {
        // With auth enabled, return just the status without service details.
        (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
    }
}

/// Execute a SPARQL query and build the HTTP response.
///
/// Parses the `Accept` header to determine the response format. Supports:
/// - `text/turtle` -> Turtle serialization
/// - `application/n-triples` -> N-Triples serialization
/// - Default -> `application/sparql-results+json`
async fn execute_and_respond(
    state: &AppState,
    query: &str,
    keyspace: &str,
    accept: &str,
) -> Response {
    let format = crate::results::ResultFormat::from_accept(accept);

    match state.engine.execute(query, keyspace).await {
        Ok(result) => match result.serialize(format) {
            Ok(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, format.content_type())],
                bytes,
            )
                .into_response(),
            Err(e) => error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("serialization error: {e}"),
            ),
        },
        Err(crate::error::SparqlError::Parse(msg)) => error_response(StatusCode::BAD_REQUEST, &msg),
        Err(crate::error::SparqlError::Plan(msg)) => error_response(StatusCode::BAD_REQUEST, &msg),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn extract_query_from_post(headers: &axum::http::HeaderMap, body: &[u8]) -> Result<String, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/sparql-query") {
        String::from_utf8(body.to_vec()).map_err(|e| format!("invalid UTF-8: {e}"))
    } else if content_type.contains("application/x-www-form-urlencoded") {
        // Parse query=... from form body.
        let params: Vec<(String, String)> =
            serde_urlencoded::from_bytes(body).map_err(|e| format!("form parse error: {e}"))?;
        params
            .into_iter()
            .find(|(k, _)| k == "query")
            .map(|(_, v)| v)
            .ok_or_else(|| "missing 'query' parameter".into())
    } else if content_type.contains("application/json") {
        // Accept JSON body with {"query": "..."} for convenience.
        let parsed: serde_json::Value =
            serde_json::from_slice(body).map_err(|e| format!("JSON parse error: {e}"))?;
        parsed["query"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "JSON body missing 'query' field".into())
    } else {
        // Default: treat body as raw SPARQL text.
        String::from_utf8(body.to_vec()).map_err(|e| format!("invalid UTF-8: {e}"))
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({"error": message}))).into_response()
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, SchemaConfig, TestAuditSink,
    };

    fn test_schema() -> Schema {
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap()
    }

    #[test]
    fn default_listener_is_loopback_only() {
        assert_eq!(
            SparqlHttpConfig::default().bind_addr,
            SocketAddr::from(([127, 0, 0, 1], 8080))
        );
    }

    /// The kill-switch: when auth is disabled, requests are served as superuser
    /// (same as the CQL/graph behaviour).
    #[test]
    fn auth_disabled_kill_switch_allows() {
        let res = authenticate_sparql(true, &test_schema(), &axum::http::HeaderMap::new());
        assert!(res.is_ok(), "auth_disabled must allow (kill-switch)");
        assert!(res.unwrap().is_superuser);
    }

    /// t_e2bf1e62 (FMEA SP-1): an unauthenticated SPARQL request must be REJECTED,
    /// not served. Previously no handler checked credentials at all.
    #[test]
    fn missing_authorization_header_rejected_when_auth_on() {
        let res = authenticate_sparql(false, &test_schema(), &axum::http::HeaderMap::new());
        assert!(
            res.is_err(),
            "with auth enabled and no Authorization header, the request must be rejected"
        );
    }

    /// Per-keyspace authorization: a superuser passes; an unprivileged role with
    /// no grant is denied — so the X-Keyspace header is no longer the only boundary.
    #[test]
    fn keyspace_authorization_enforced() {
        let schema = test_schema();
        let superuser = AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        assert!(
            authorize_keyspace(&schema, &superuser, "rdf", Permission::Modify).is_ok(),
            "superuser must be authorized"
        );
        let unprivileged = AuthContext {
            role: "nobody".to_string(),
            is_superuser: false,
            must_change_password: false,
        };
        assert!(
            authorize_keyspace(&schema, &unprivileged, "rdf", Permission::Select).is_err(),
            "an unprivileged role with no grant must be denied"
        );
    }

    fn free_addr() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    }

    async fn connect_retry(addr: SocketAddr) -> tokio::net::TcpStream {
        for _ in 0..100 {
            if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
                return stream;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("listener at {addr} never came up");
    }

    /// t_d5d122ba: with a certificate configured the endpoint speaks HTTPS
    /// only; a plaintext request gets no HTTP response.
    #[tokio::test]
    async fn serves_https_when_a_certificate_is_configured() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, certified.cert.pem()).unwrap();
        std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
        let config = SparqlHttpConfig {
            bind_addr: free_addr(),
            tls_cert_path: Some(cert.to_str().unwrap().into()),
            tls_key_path: Some(key.to_str().unwrap().into()),
            require_tls: true,
        };
        let addr = config.bind_addr;
        let app = Router::new().route("/sparql/health", get(|| async { "ok" }));
        tokio::spawn(async move { serve_app(&config, app).await });

        let mut roots = rustls::RootCertStore::empty();
        roots.add(certified.cert.der().clone()).unwrap();
        let client =
            rustls::ClientConfig::builder_with_provider(ferrosa_net::tls::crypto_provider())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        let mut tls = tokio_rustls::TlsConnector::from(Arc::new(client))
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                connect_retry(addr).await,
            )
            .await
            .expect("TLS handshake with SPARQL");
        tls.write_all(
            b"GET /sparql/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
        let mut response = Vec::new();
        tls.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));

        let mut plain = connect_retry(addr).await;
        plain
            .write_all(b"GET /sparql/health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            plain.read_to_end(&mut buf),
        )
        .await;
        assert!(
            !String::from_utf8_lossy(&buf).starts_with("HTTP/"),
            "plaintext must not be served on a TLS port (read {read:?})"
        );
    }

    #[tokio::test]
    async fn require_tls_without_a_certificate_refuses_to_start() {
        let config = SparqlHttpConfig {
            bind_addr: free_addr(),
            require_tls: true,
            ..SparqlHttpConfig::default()
        };
        let err = serve_app(&config, Router::new())
            .await
            .expect_err("require_tls with no certificate must not serve plaintext");
        assert!(err.to_string().contains("SPARQL"), "{err}");
    }
}
