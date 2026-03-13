//! Ferrosa binary — composes all crates into the running database.
//!
//! Startup sequence:
//! 1. Initialize tracing
//! 2. Create StorageEngine
//! 3. Create Schema
//! 4. Start CQL server (port 9042)
//! 5. Start web observability console (port 9090)
//! 6. Create GraphEngine + HTTP server (if enabled)
//! 7. Wait for shutdown signal
//! 8. Graceful shutdown

mod web;

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    tracing::info!("ferrosa starting");

    // 2. Create StorageEngine
    let storage_config = ferrosa_storage::StorageEngineConfig::from_env()?;
    let rt = tokio::runtime::Handle::current();
    let storage = Arc::new(ferrosa_storage::StorageEngine::new(
        storage_config,
        Some(&rt),
    )?);

    // 3. Create Schema
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

    // 4. Start CQL server
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
        ..ferrosa_schema::NodeConfig::default()
    });
    let connection_tracker =
        Arc::new(ferrosa_cql::virtual_tables::connections::ConnectionTracker::new());
    let query_tracker = Arc::new(ferrosa_cql::virtual_tables::active_queries::QueryTracker::new());
    let shared_state = Arc::new(ferrosa_cql::router::SharedState {
        engine: storage.clone(),
        schema: schema.clone(),
        node_config,
        cluster_state: Arc::new(ferrosa_cql::router::SingleNodeClusterState),
        prepared_cache: Arc::new(ferrosa_cql::prepared::PreparedCache::new(64 * 1024 * 1024)),
        connection_tracker,
        query_tracker,
    });
    let cql_server = ferrosa_cql::server::CqlServer::new(cql_config, shared_state);
    let cql_addr = cql_server.start_background().await?;
    tracing::info!(%cql_addr, "CQL server listening");

    // 5. Web observability console
    let vt_registry = Arc::new(ferrosa_schema::VirtualTableRegistry::new());
    let web_config = web::WebConfig::from_env();
    let web_addr = web::start_web_server(&web_config, vt_registry).await?;
    tracing::info!(%web_addr, "web console listening");

    // 6. Graph engine (check FERROSA_GRAPH_ENABLED)
    let graph_enabled = std::env::var("FERROSA_GRAPH_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if graph_enabled {
        let graph_config = ferrosa_graph::engine::GraphConfig {
            enabled: true,
            http: ferrosa_graph::http::GraphHttpConfig {
                require_tls: false, // TODO: read from env
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

    // 7. Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");

    // 8. Graceful shutdown
    storage.shutdown()?;
    tracing::info!("ferrosa stopped");

    Ok(())
}
