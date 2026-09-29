//! Web observability console — HTTP server on a dedicated port (default 9090).
//!
//! Routes:
//!   `GET /`                         → embedded `index.html` (rust-embed)
//!   `GET /metrics`                  → Prometheus text exposition (no auth)
//!   `GET /api/tables`               → list of registered virtual tables
//!   `GET /api/connections`          → CQL connection rows
//!   `GET /api/storage_stats`        → per-table storage metrics
//!   `GET /api/storage`              → alias for `/api/storage_stats`
//!   `GET /api/active_queries`       → active query rows
//!   `GET /api/queries`              → alias for `/api/active_queries`
//!   `GET /api/cluster/status`       → cluster mode, role, host_id
//!   `POST /api/cluster/promote`     → force-promote to standalone primary
//!   `POST /api/cluster/switchover`  → swap primary/secondary roles
//!   `POST /api/cluster/add-node`    → pre-approve a node for cluster admission
//!   `POST /api/cluster/decommission`→ initiate graceful removal of a node
//!   `GET /api/cluster/ring`         → token ring topology
//!   `POST /api/cluster/rebalance`   → rebalance token distribution
//!   `GET /api/snapshots`            → list PITR snapshots
//!   `POST /api/snapshots`           → create a PITR snapshot
//!   `DELETE /api/snapshots/:name`   → delete a PITR snapshot
//!   `GET /api/archive_status`       → commit-log archive health
//!   `POST /api/restore/preflight`   → validate a restore without applying it
//!   `POST /api/restore`             → trigger a PITR restore

pub mod api;
pub mod auth;
mod compaction;
pub mod debug;
pub mod observability;
pub mod readiness;
pub mod snapshots;
pub mod static_files;
pub mod ws;

use std::{net::SocketAddr, sync::Arc};

use axum::extract::FromRef;
use axum::routing::get;
use axum::Router;
use ferrosa_cluster::ModeController;
use ferrosa_schema::{Schema, VirtualTableRegistry};
use ferrosa_storage::StorageEngine;

/// Configuration for the web observability server.
#[derive(Debug, Clone)]
pub struct WebConfig {
    /// Address to bind the HTTP server on. Default: `127.0.0.1:9090`.
    pub bind_addr: SocketAddr,
    /// PEM certificate chain (`[web] tls_cert`). With `tls_key_path` the
    /// console, `/api`, `/admin`, `/metrics` and `/readyz` are HTTPS only
    /// (t_d5d122ba).
    pub tls_cert_path: Option<String>,
    /// PEM private key (`[web] tls_key`).
    pub tls_key_path: Option<String>,
    /// Refuse to start without a certificate (`[web] require_tls`).
    pub require_tls: bool,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9090".parse().expect("hardcoded addr is valid"),
            tls_cert_path: None,
            tls_key_path: None,
            require_tls: false,
        }
    }
}

// The bind address is resolved by the binary via `config_val`
// (`[web] bind` in the config file wins over `FERROSA_WEB_BIND`, then the
// default), so `WebConfig` is constructed directly with the resolved address.

#[derive(Clone)]
pub struct WebAppState {
    pub registry: Arc<VirtualTableRegistry>,
    pub mode_controller: Arc<ModeController>,
    pub schema: Arc<Schema>,
    /// Storage engine — used by snapshot and restore endpoints.
    pub storage: Arc<StorageEngine>,
    /// Host UUID — used as the `node_id` when creating snapshots.
    pub host_id: uuid::Uuid,
    pub auth_disabled: bool,
    /// Debug profiler state (shared mutex for single-session profiling).
    pub debug: Option<debug::DebugState>,
    /// Health of the background client listeners (Postgres, SPARQL, graph, Bolt).
    /// A failed listener keeps `/readyz` from reporting ready.
    pub listeners: Arc<crate::listener_status::ListenerStatus>,
}

impl FromRef<WebAppState> for Arc<crate::listener_status::ListenerStatus> {
    fn from_ref(state: &WebAppState) -> Self {
        Arc::clone(&state.listeners)
    }
}

impl FromRef<WebAppState> for Arc<VirtualTableRegistry> {
    fn from_ref(state: &WebAppState) -> Self {
        state.registry.clone()
    }
}

impl FromRef<WebAppState> for Arc<ModeController> {
    fn from_ref(state: &WebAppState) -> Self {
        state.mode_controller.clone()
    }
}

/// Build the axum router for the web console.
///
/// Auth middleware is applied to both `/api/*` and `/admin/*` routes.
/// Static assets (the embedded web UI at `/`) and `/metrics` (Prometheus
/// scrape) remain publicly accessible without credentials.
///
/// When `state.auth_disabled` is `true` (development mode), the
/// `auth_middleware` bypasses credential checks for all protected routes.
pub fn build_router(state: WebAppState) -> Router {
    let protected = Router::new()
        .nest("/api", api::routes())
        .nest("/api", snapshots::snapshot_routes())
        .nest("/api", observability::routes())
        .nest("/api/cluster", api::cluster_routes())
        .nest("/api/index", api::index_routes())
        .nest("/api/compaction", compaction::routes())
        .nest("/api/debug", debug::debug_routes())
        .route("/api/ws", get(ws::ws_handler))
        .nest("/admin", api::admin_routes())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::auth_middleware,
        ));

    // /readyz is not behind auth so orchestrators (docker-compose, k8s,
    // smoke scripts) can probe it without credentials.
    protected
        .merge(readiness::readiness_route())
        .route("/metrics", get(api::get_metrics))
        .fallback(static_files::static_handler)
        .with_state(state)
}

/// Start the web server in a background task, returning the bound address.
pub async fn start_web_server(
    config: &WebConfig,
    state: WebAppState,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    serve_router(config, build_router(state)).await
}

/// Bind and serve `router` in a background task, over TLS when a certificate
/// is configured. The TLS config comes from the shared `ferrosa_net::tls`
/// builder (one crypto provider for every listener); `require_tls` without a
/// certificate, or only one of cert/key, fails here, before anything binds.
async fn serve_router(
    config: &WebConfig,
    router: Router,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let tls = ferrosa_net::tls::optional_server_config(
        "web console",
        config.tls_cert_path.as_deref(),
        config.tls_key_path.as_deref(),
        config.require_tls,
        ferrosa_net::tls::HTTP_ALPN,
    )?;
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let addr = listener.local_addr()?;
    let pool = ferrosa_common::task_pool::TaskPool::current("web-server");
    match tls {
        Some(tls) => {
            let server = axum_server::from_tcp_rustls(
                listener.into_std()?,
                axum_server::tls_rustls::RustlsConfig::from_config(tls),
            )?;
            tracing::info!(%addr, "web console serving HTTPS");
            pool.spawn(async move {
                if let Err(e) = server.serve(router.into_make_service()).await {
                    tracing::error!(%e, "web server error");
                }
            });
        }
        None => {
            pool.spawn(async move {
                if let Err(e) = axum::serve(listener, router).await {
                    tracing::error!(%e, "web server error");
                }
            });
        }
    }
    Ok(addr)
}

#[cfg(test)]
mod tls_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn self_signed() -> (
        tempfile::TempDir,
        WebConfig,
        rustls::pki_types::CertificateDer<'static>,
    ) {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, certified.cert.pem()).unwrap();
        std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
        let config = WebConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            tls_cert_path: Some(cert.to_str().unwrap().into()),
            tls_key_path: Some(key.to_str().unwrap().into()),
            require_tls: true,
        };
        (dir, config, certified.cert.der().clone())
    }

    /// t_d5d122ba: with a certificate the console (incl. `/readyz`) is HTTPS
    /// only; plaintext gets no HTTP response.
    #[tokio::test]
    async fn web_console_serves_https_when_a_certificate_is_configured() {
        let (_dir, config, der) = self_signed();
        let app = Router::new().route("/readyz", get(|| async { "ready" }));
        let addr = serve_router(&config, app)
            .await
            .expect("web console starts");

        let mut roots = rustls::RootCertStore::empty();
        roots.add(der).unwrap();
        let client =
            rustls::ClientConfig::builder_with_provider(ferrosa_net::tls::crypto_provider())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut tls = tokio_rustls::TlsConnector::from(Arc::new(client))
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
            .expect("TLS handshake with the web console");
        tls.write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        tls.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));

        let mut plain = tokio::net::TcpStream::connect(addr).await.unwrap();
        plain
            .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n")
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
        let config = WebConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            require_tls: true,
            ..WebConfig::default()
        };
        let err = serve_router(&config, Router::new())
            .await
            .expect_err("require_tls with no certificate must not serve plaintext");
        assert!(err.to_string().contains("web console"), "{err}");
    }
}
