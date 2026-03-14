//! Ferrosa binary — composes all crates into the running database.
//!
//! Startup sequence:
//! 1. Initialize tracing
//! 2. Load/generate host_id
//! 3. Create StorageEngine (with S3 if configured)
//! 4. Create Schema
//! 5. Create ModeController (standalone WritePath + ClusterState)
//! 6. Create PeerManager + RPC handlers
//! 7. Start internode RPC server (port 7000)
//! 8. Start CQL server (port 9042)
//! 9. Start web observability console (port 9090)
//! 10. Create GraphEngine + HTTP server (if enabled)
//! 11. Background: connect to seeds → triggers mode transition
//! 12. Wait for shutdown signal
//! 13. Graceful shutdown

mod web;

use std::path::Path;
use std::sync::Arc;

use uuid::Uuid;

/// Load host_id from disk, env var, or generate a new one.
fn load_or_generate_host_id(data_dir: &Path) -> Uuid {
    let path = data_dir.join("host_id");

    // Try reading existing host_id from disk.
    if let Ok(contents) = std::fs::read_to_string(&path) {
        if let Ok(id) = Uuid::parse_str(contents.trim()) {
            tracing::info!(%id, "loaded host_id from disk");
            return id;
        }
    }

    // Check env var override.
    if let Ok(id_str) = std::env::var("FERROSA_HOST_ID") {
        if let Ok(id) = Uuid::parse_str(&id_str) {
            let _ = std::fs::write(&path, id.to_string());
            tracing::info!(%id, "using host_id from FERROSA_HOST_ID");
            return id;
        }
    }

    // Generate new host_id and persist.
    let id = Uuid::new_v4();
    let _ = std::fs::write(&path, id.to_string());
    tracing::info!(%id, "generated new host_id");
    id
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    tracing::info!("ferrosa starting");

    // 2. Load/generate host_id
    let data_dir = std::env::var("FERROSA_DATA_DIR").unwrap_or_else(|_| "/var/lib/ferrosa".into());
    std::fs::create_dir_all(&data_dir)?;
    let host_id = load_or_generate_host_id(Path::new(&data_dir));

    // 3. Create StorageEngine
    let storage_config = ferrosa_storage::StorageEngineConfig::from_env()?;
    let rt = tokio::runtime::Handle::current();
    let storage = Arc::new(ferrosa_storage::StorageEngine::new(
        storage_config,
        Some(&rt),
    )?);

    // 4. Create Schema
    let schema_config = ferrosa_schema::SchemaConfig {
        hasher: ferrosa_schema::PasswordHasher::default(),
        password_policy: ferrosa_schema::PasswordPolicy::permissive(),
        auth_method: ferrosa_schema::AuthMethod::Password,
        rate_limit: ferrosa_schema::RateLimitConfig::default(),
        audit_sink: Box::new(ferrosa_schema::LogAuditSink),
        secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
        mode: ferrosa_schema::DeploymentMode::Development,
    };
    let schema = Arc::new(ferrosa_schema::Schema::new(schema_config)?);

    // 5. Create ModeController — starts in standalone mode
    let cluster_config = Arc::new(ferrosa_cluster::ClusterConfig::from_env());
    let net_config = Arc::new(ferrosa_net::config::NetConfig::from_env());

    let (mode_controller, handles) = ferrosa_cluster::ModeController::new(
        cluster_config,
        net_config.clone(),
        host_id,
        storage.clone(),
    );

    // 6. Create PeerManager + RPC handlers
    let peer_manager = Arc::new(ferrosa_net::peer::PeerManager::new(
        net_config.clone(),
        host_id,
        mode_controller.clone(),
    ));
    mode_controller.set_peer_manager(peer_manager.clone());

    // Build handler registry for pair mode RPC
    let mut registry = ferrosa_net::rpc::HandlerRegistry::new();
    // Pair mode handlers will be registered by ModeController when transitioning
    // For now, register catch-up and role-swap handlers that are always available
    let catchup_handler = Arc::new(ferrosa_cluster::pair::catchup::PairCatchUpHandler::new(
        storage.clone(),
    ));
    registry.register(ferrosa_net::codec::MsgType::PairCatchUp, catchup_handler);

    // 7. Start internode RPC server
    let rpc_server = Arc::new(ferrosa_net::rpc::server::RpcServer::new(
        (*net_config).clone(),
        host_id,
        registry,
    ));
    let internode_addr = rpc_server.start_and_get_addr().await?;
    tracing::info!(%internode_addr, %host_id, "internode server listening");

    // 8. Start CQL server
    let cql_bind: std::net::SocketAddr = std::env::var("FERROSA_CQL_BIND")
        .unwrap_or_else(|_| "0.0.0.0:9042".to_string())
        .parse()?;
    let cql_config = ferrosa_cql::server::ServerConfig {
        bind_addr: cql_bind,
        auth_disabled: std::env::var("FERROSA_AUTH_DISABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false),
        ..ferrosa_cql::server::ServerConfig::default()
    };
    let node_config = Arc::new(ferrosa_schema::NodeConfig {
        rpc_address: cql_bind.ip(),
        rpc_port: cql_bind.port(),
        host_id,
        listen_port: internode_addr.port(),
        ..ferrosa_schema::NodeConfig::default()
    });
    let connection_tracker =
        Arc::new(ferrosa_cql::virtual_tables::connections::ConnectionTracker::new());
    let query_tracker = Arc::new(ferrosa_cql::virtual_tables::active_queries::QueryTracker::new());
    let shared_state = Arc::new(ferrosa_cql::router::SharedState {
        engine: storage.clone(),
        schema: schema.clone(),
        node_config,
        cluster_state: handles.cluster_state,
        write_path: handles.write_path,
        prepared_cache: Arc::new(ferrosa_cql::prepared::PreparedCache::new(64 * 1024 * 1024)),
        connection_tracker,
        query_tracker,
    });
    let cql_server = ferrosa_cql::server::CqlServer::new(cql_config, shared_state);
    let cql_addr = cql_server.start_background().await?;
    tracing::info!(%cql_addr, "CQL server listening");

    // 9. Web observability console
    let vt_registry = Arc::new(ferrosa_schema::VirtualTableRegistry::new());
    let web_config = web::WebConfig::from_env();
    let web_addr = web::start_web_server(&web_config, vt_registry).await?;
    tracing::info!(%web_addr, "web console listening");

    // 10. Graph engine (check FERROSA_GRAPH_ENABLED)
    let graph_enabled = std::env::var("FERROSA_GRAPH_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if graph_enabled {
        let graph_config = ferrosa_graph::engine::GraphConfig {
            enabled: true,
            http: ferrosa_graph::http::GraphHttpConfig {
                require_tls: false,
                ..ferrosa_graph::http::GraphHttpConfig::default()
            },
            ..ferrosa_graph::engine::GraphConfig::default()
        };

        let http_config = graph_config.http.clone();
        let graph_engine = Arc::new(ferrosa_graph::engine::GraphEngine::new(
            schema.clone(),
            storage.clone(),
            graph_config.engine,
            graph_config.reconciliation_interval,
        ));

        let schema_for_http = schema.clone();
        let state = ferrosa_graph::http::AppState {
            engine: graph_engine,
            schema: schema_for_http,
        };
        tokio::spawn(async move {
            if let Err(e) = ferrosa_graph::http::start_graph_http(&http_config, state).await {
                tracing::error!(%e, "graph HTTP server failed");
            }
        });
    } else {
        tracing::info!("graph engine disabled (set FERROSA_GRAPH_ENABLED=true to enable)");
    }

    // 11. Background: connect to seeds
    if !net_config.seeds.is_empty() {
        let seeds = net_config.seeds.clone();
        let net_cfg = net_config.clone();
        let pm = peer_manager.clone();
        tokio::spawn(async move {
            for seed_addr in &seeds {
                tracing::info!(%seed_addr, "connecting to seed");
                match ferrosa_net::pool::PriorityPool::connect(net_cfg.clone(), host_id, *seed_addr)
                    .await
                {
                    Ok(pool) => {
                        // The handshake exchanged host_ids. For now, use a placeholder
                        // peer_id — the PeerEventListener will handle the mode transition.
                        // TODO: extract peer host_id from handshake response.
                        let peer_id = Uuid::new_v4();
                        pm.add_peer((peer_id, *seed_addr), pool).await;
                        tracing::info!(%seed_addr, "seed connected");
                    }
                    Err(e) => {
                        tracing::warn!(%seed_addr, %e, "seed connection failed (will retry)");
                    }
                }
            }
        });
    }

    // 12. Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");

    // 13. Graceful shutdown
    storage.shutdown()?;
    tracing::info!("ferrosa stopped");

    Ok(())
}
