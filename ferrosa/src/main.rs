//! Ferrosa binary — composes all crates into the running database.
//!
//! Startup sequence:
//! 1. Initialize tracing
//! 2. Load/generate host_id
//! 3. Create StorageEngine (with S3 if configured)
//! 4. Create Schema
//! 5. Create ModeController (standalone WritePath + ClusterState)
//! 6. Create PeerManager + RPC handlers + heartbeat loop
//! 7. Start internode RPC server (port 7000)
//! 8. Start CQL server (port 9042)
//! 9. Start web observability console (port 9090)
//! 10. Create GraphEngine + HTTP server (if enabled)
//! 11. Background: connect to seeds with exponential backoff
//! 12. Background: maintenance loop (flush, compaction, commit log GC)
//! 13. Wait for shutdown signal
//! 14. Graceful shutdown with timeout

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

    // Build handler registry — shared between RPC server and ModeController.
    // Catch-up handler is always available; pair write/role-swap handlers are
    // registered dynamically by ModeController on mode transition.
    let registry = Arc::new(ferrosa_net::rpc::HandlerRegistry::new());
    let catchup_handler = Arc::new(ferrosa_cluster::pair::catchup::PairCatchUpHandler::new(
        storage.clone(),
    ));
    registry.register(ferrosa_net::codec::MsgType::PairCatchUp, catchup_handler);

    let (mode_controller, handles) = ferrosa_cluster::ModeController::new(
        cluster_config,
        net_config.clone(),
        host_id,
        storage.clone(),
        schema.clone(),
        registry.clone(),
    );

    // 6. Create PeerManager — ModeController is the PeerEventListener
    let peer_manager = Arc::new(ferrosa_net::peer::PeerManager::new(
        net_config.clone(),
        host_id,
        mode_controller.clone(),
    ));
    mode_controller.set_peer_manager(peer_manager.clone());

    // 6b. Start heartbeat loop for peer failure detection
    let heartbeat_pm = peer_manager.clone();
    tokio::spawn(async move {
        heartbeat_pm.run_heartbeat_loop().await;
    });

    // 7. Start internode RPC server with inbound peer callback
    let rpc_server = Arc::new(
        ferrosa_net::rpc::server::RpcServer::new((*net_config).clone(), host_id, registry)
            .with_inbound_callback(mode_controller.clone()),
    );
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
        ddl_path: handles.ddl_path,
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
    let web_addr = web::start_web_server(&web_config, vt_registry, mode_controller.clone()).await?;
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

    // 11. Background: connect to seeds with exponential backoff
    // Seeds can be hostnames (e.g., "node2:7000") which SocketAddr can't parse.
    // Resolve via DNS in the background task.
    let seed_strs: Vec<String> = std::env::var("FERROSA_SEED")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if !seed_strs.is_empty() {
        let net_cfg = net_config.clone();
        let pm = peer_manager.clone();
        tokio::spawn(async move {
            let mut delay = std::time::Duration::from_millis(500);
            let max_delay = std::time::Duration::from_secs(10);

            'outer: loop {
                tokio::time::sleep(delay).await;

                let mut all_connected = true;
                for seed in &seed_strs {
                    match tokio::net::lookup_host(seed.as_str()).await {
                        Ok(mut addrs) => {
                            if let Some(seed_addr) = addrs.next() {
                                match ferrosa_net::pool::PriorityPool::connect(
                                    net_cfg.clone(),
                                    host_id,
                                    seed_addr,
                                )
                                .await
                                {
                                    Ok(pool) => {
                                        let peer_host_id = pool.peer_host_id();
                                        pm.add_peer((peer_host_id, seed_addr), pool).await;
                                        tracing::info!(%seed, %peer_host_id, "seed connected");
                                        continue; // This seed is done
                                    }
                                    Err(e) => {
                                        tracing::debug!(%seed, %e, "seed connection attempt failed, will retry");
                                        all_connected = false;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(%seed, %e, "seed DNS resolution failed, will retry");
                            all_connected = false;
                        }
                    }
                }

                if all_connected {
                    tracing::info!("all seeds connected");
                    break 'outer;
                }

                delay = std::cmp::min(delay * 2, max_delay);
            }
        });
    }

    // 12. Background maintenance loop: periodic flush, compaction polling, commit log GC
    let maintenance_engine = storage.clone();
    let has_s3 = maintenance_engine.has_s3();
    tokio::spawn(async move {
        let mut flush_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut compact_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        let mut s3_warned = false;

        loop {
            tokio::select! {
                _ = flush_interval.tick() => {
                    if let Err(e) = maintenance_engine.flush_if_needed() {
                        tracing::warn!(%e, "periodic flush failed");
                    }

                    // Commit log GC: discard segments with no remaining dirty tables.
                    match maintenance_engine.discard_completed_commit_log_segments() {
                        Ok(n) if n > 0 => {
                            tracing::debug!(segments = n, "commit log GC cleaned up segments");
                        }
                        Err(e) => {
                            tracing::warn!(%e, "commit log GC failed");
                        }
                        _ => {}
                    }
                }
                _ = compact_interval.tick() => {
                    maintenance_engine.poll_compactions();

                    // TODO: S3 upload should be triggered from flush/compaction results.
                    // The UploadManager infrastructure exists but wiring it requires
                    // knowing which SSTable files were produced by each flush — that's
                    // a larger refactor to make flush() and poll_compactions() return
                    // SSTable handles.
                    if has_s3 && !s3_warned {
                        tracing::warn!(
                            "S3 object storage configured but automatic upload not yet wired; \
                             SSTables will remain on local disk only"
                        );
                        s3_warned = true;
                    }
                }
            }
        }
    });

    // 13. Wait for shutdown signal
    tokio::signal::ctrl_c().await?;

    // 14. Graceful shutdown with timeout
    tracing::info!("shutdown signal received, draining...");

    // CqlServer and RpcServer do not yet expose shutdown methods to stop
    // accepting new connections. For now, proceed directly to storage shutdown
    // which flushes memtables and stops compaction.
    // TODO: Add CqlServer::shutdown() and RpcServer::shutdown() to drain
    // in-flight requests before stopping the storage layer.

    let shutdown_timeout = std::time::Duration::from_secs(30);
    match tokio::time::timeout(shutdown_timeout, async { storage.shutdown() }).await {
        Ok(Ok(())) => tracing::info!("clean shutdown"),
        Ok(Err(e)) => tracing::error!(%e, "shutdown error"),
        Err(_) => tracing::error!("shutdown timed out after 30s"),
    }

    tracing::info!("ferrosa stopped");

    Ok(())
}
