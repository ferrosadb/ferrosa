//! Web observability console — HTTP server on a dedicated port (default 9090).
//!
//! Routes:
//!   `GET /`                       → embedded `index.html` (rust-embed)
//!   `GET /api/tables`             → list of registered virtual tables
//!   `GET /api/connections`        → CQL connection rows
//!   `GET /api/storage_stats`      → per-table storage metrics
//!   `GET /api/active_queries`     → active query rows
//!   `GET /api/cluster/status`     → cluster mode, role, host_id
//!   `POST /api/cluster/promote`   → force-promote to standalone primary
//!   `POST /api/cluster/switchover`→ swap primary/secondary roles

pub mod api;
pub mod static_files;

use std::{net::SocketAddr, sync::Arc};

use axum::Router;
use ferrosa_cluster::ModeController;
use ferrosa_schema::VirtualTableRegistry;

/// Configuration for the web observability server.
#[derive(Debug, Clone)]
pub struct WebConfig {
    /// Address to bind the HTTP server on. Default: `0.0.0.0:9090`.
    pub bind_addr: SocketAddr,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9090".parse().expect("hardcoded addr is valid"),
        }
    }
}

impl WebConfig {
    /// Build from environment variables, falling back to defaults.
    ///
    /// `FERROSA_WEB_BIND` — bind address (e.g. `127.0.0.1:9090`)
    pub fn from_env() -> Self {
        let bind_addr = std::env::var("FERROSA_WEB_BIND")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| "0.0.0.0:9090".parse().expect("hardcoded addr is valid"));
        Self { bind_addr }
    }
}

/// Build the axum router for the web console.
pub fn build_router(
    registry: Arc<VirtualTableRegistry>,
    mode_controller: Arc<ModeController>,
) -> Router {
    Router::new()
        .nest("/api", api::routes(registry))
        .nest("/api/cluster", api::cluster_routes(mode_controller))
        .fallback(static_files::static_handler)
}

/// Start the web server in a background task, returning the bound address.
pub async fn start_web_server(
    config: &WebConfig,
    registry: Arc<VirtualTableRegistry>,
    mode_controller: Arc<ModeController>,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let router = build_router(registry, mode_controller);
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!(%e, "web server error");
        }
    });
    Ok(addr)
}
