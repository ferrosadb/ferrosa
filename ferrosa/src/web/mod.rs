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
pub mod debug;
pub mod observability;
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
/// Auth middleware is scoped to `/api/*` routes only — static assets
/// (the embedded web UI at `/`) and `/metrics` (Prometheus scrape)
/// remain publicly accessible.
pub fn build_router(state: WebAppState) -> Router {
    let api = Router::new()
        .nest("/api", api::routes())
        .nest("/api", snapshots::snapshot_routes())
        .nest("/api", observability::routes())
        .nest("/api/cluster", api::cluster_routes())
        .nest("/api/debug", debug::debug_routes())
        .route("/api/ws", get(ws::ws_handler))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::auth_middleware,
        ));

    // /admin/* is not behind auth — it exposes read-only diagnostics used by
    // the Jepsen verification harness (Sprint 2 W2.3).
    api.nest("/admin", api::admin_routes())
        .route("/metrics", get(api::get_metrics))
        .fallback(static_files::static_handler)
        .with_state(state)
}

/// Start the web server in a background task, returning the bound address.
pub async fn start_web_server(
    config: &WebConfig,
    state: WebAppState,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let router = build_router(state);
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!(%e, "web server error");
        }
    });
    Ok(addr)
}
