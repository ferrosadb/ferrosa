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

/// Read a config value: env var takes precedence, then config file, then default.
fn config_val(
    env_key: &str,
    config: &toml::Value,
    section: &str,
    key: &str,
    default: &str,
) -> String {
    std::env::var(env_key).unwrap_or_else(|_| {
        config
            .get(section)
            .and_then(|s| s.get(key))
            .and_then(|v| v.as_str().map(String::from).or_else(|| Some(v.to_string())))
            .unwrap_or_else(|| default.to_string())
    })
}

/// Load TOML configuration from disk. Returns an empty table if the file does not exist.
fn load_config(path: &str) -> Result<toml::Value, Box<dyn std::error::Error>> {
    if std::path::Path::new(path).exists() {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    } else {
        Ok(toml::Value::Table(toml::map::Map::new()))
    }
}

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

/// Bootstrap schema and table registrations from S3.
///
/// On a cold restart (local data wiped, S3 has data), this function:
/// 1. Loads the schema snapshot from S3 (`schema.json`)
/// 2. Applies it to the in-memory schema (creates keyspaces + tables)
/// 3. Registers each table with the StorageEngine so reads can proceed
async fn bootstrap_from_s3(
    storage: &ferrosa_storage::StorageEngine,
    schema: &ferrosa_schema::Schema,
) -> Result<(), Box<dyn std::error::Error>> {
    let (os_config, store) = storage.object_store_and_config()?;
    let prefix = &os_config.prefix;

    // Load schema snapshot (retry up to 5 times — the S3-compatible store may
    // not be ready immediately after a container restart).
    let snapshot_data = {
        let mut loaded = None;
        for attempt in 1..=5u32 {
            match ferrosa_storage::load_schema_snapshot(store.as_ref(), prefix).await {
                Ok(Some(data)) => {
                    loaded = Some(data);
                    break;
                }
                Ok(None) if attempt < 5 => {
                    tracing::info!(attempt, "no schema snapshot in S3 yet, retrying in 2s…");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Ok(None) => {}
                Err(e) if attempt < 5 => {
                    tracing::warn!(attempt, "S3 schema load failed: {e}, retrying in 2s…");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Err(e) => {
                    tracing::warn!("S3 schema load failed after 5 attempts: {e}");
                }
            }
        }
        loaded
    };
    let snapshot_data = match snapshot_data {
        Some(data) => data,
        None => {
            tracing::info!("no schema snapshot in S3 after retries — starting fresh");
            return Ok(());
        }
    };

    let snapshot: ferrosa_schema::SchemaSnapshot =
        serde_json::from_slice(&snapshot_data).map_err(|e| format!("bad schema.json: {e}"))?;

    let table_count = snapshot.tables.len();
    let ks_count = snapshot.keyspaces.len();

    // Apply schema (creates keyspaces, tables, roles, grants)
    schema.apply_snapshot(snapshot)?;

    // Load manifest to know what SSTables exist in S3.
    let (manifest, _version) = ferrosa_storage::Manifest::load(store.as_ref(), prefix).await?;
    let sstable_count: usize = manifest.sstables.values().map(|v| v.len()).sum();

    // Download SSTables from S3 to local disk BEFORE registering tables,
    // so register_table() finds them and opens readers.
    let snap = schema.snapshot();
    let mut downloaded_total = 0usize;
    for ((_ks, _tbl), table_meta) in &snap.tables {
        if ferrosa_schema::is_system_keyspace(&table_meta.keyspace) {
            continue;
        }
        let table_id = ferrosa_storage::TableId::new(&table_meta.keyspace, &table_meta.name);
        match storage
            .download_sstables_from_s3(&table_id, &manifest)
            .await
        {
            Ok(n) => downloaded_total += n,
            Err(e) => {
                tracing::warn!(
                    table = %table_meta.name,
                    ks = %table_meta.keyspace,
                    "failed to download SSTables from S3: {e}"
                );
            }
        }
    }

    // Register each table with the StorageEngine — will find downloaded SSTables on disk.
    for ((_ks, _tbl), table_meta) in &snap.tables {
        if ferrosa_schema::is_system_keyspace(&table_meta.keyspace) {
            continue;
        }
        let storage_schema = table_meta.to_storage_schema();
        if let Err(e) = storage.register_table(storage_schema) {
            tracing::warn!(
                table = %table_meta.name,
                ks = %table_meta.keyspace,
                "failed to register table from S3 bootstrap: {e}"
            );
        }
    }

    tracing::info!(
        ks_count,
        table_count,
        sstable_count,
        downloaded_total,
        "S3 bootstrap complete: schema restored, SSTables downloaded"
    );

    Ok(())
}

/// Persist the current schema snapshot to S3 for cold restart recovery.
async fn persist_schema_to_s3(
    storage: &ferrosa_storage::StorageEngine,
    schema: &ferrosa_schema::Schema,
) {
    let Ok((os_config, store)) = storage.object_store_and_config() else {
        return;
    };
    let snap = schema.snapshot();
    match serde_json::to_vec_pretty(&*snap) {
        Ok(json) => {
            if let Err(e) =
                ferrosa_storage::save_schema_snapshot(store.as_ref(), &os_config.prefix, &json)
                    .await
            {
                tracing::warn!("failed to persist schema to S3: {e}");
            }
        }
        Err(e) => tracing::warn!("failed to serialize schema snapshot: {e}"),
    }
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

    // 1b. Load TOML config file (env vars override file values)
    let config_path =
        std::env::var("FERROSA_CONFIG").unwrap_or_else(|_| "/etc/ferrosa/ferrosa.toml".to_string());
    let file_config = load_config(&config_path)?;
    if std::path::Path::new(&config_path).exists() {
        tracing::info!(path = %config_path, "loaded config file");
    }

    // 2. Load/generate host_id
    let data_dir = config_val(
        "FERROSA_DATA_DIR",
        &file_config,
        "storage",
        "data_dir",
        "/var/lib/ferrosa",
    );
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

    // 4b. Schema and table recovery on restart.
    //
    // Two cases:
    //   - Warm restart (local SSTables exist): load schema from local schema.json,
    //     register tables so existing SSTables become readable.
    //   - Cold restart (local data empty, S3 configured): download schema + SSTables
    //     from S3.
    {
        let sstables_dir = Path::new(&data_dir).join("sstables");
        let local_empty = !sstables_dir.exists()
            || std::fs::read_dir(&sstables_dir)
                .map(|mut d| d.next().is_none())
                .unwrap_or(true);

        if !local_empty {
            // Warm restart: load schema from local disk and register tables.
            let schema_path = Path::new(&data_dir).join("schema.json");
            if schema_path.exists() {
                match std::fs::read(&schema_path) {
                    Ok(data) => {
                        match serde_json::from_slice::<ferrosa_schema::SchemaSnapshot>(&data) {
                            Ok(snapshot) => {
                                let table_count = snapshot.tables.len();
                                let ks_count = snapshot.keyspaces.len();
                                if let Err(e) = schema.apply_snapshot(snapshot) {
                                    tracing::warn!("failed to apply local schema snapshot: {e}");
                                } else {
                                    // Register each table with the StorageEngine so existing
                                    // SSTables on disk become readable.
                                    let snap = schema.snapshot();
                                    for ((_ks, _tbl), table_meta) in &snap.tables {
                                        if ferrosa_schema::is_system_keyspace(&table_meta.keyspace)
                                        {
                                            continue;
                                        }
                                        let storage_schema = table_meta.to_storage_schema();
                                        if let Err(e) = storage.register_table(storage_schema) {
                                            tracing::warn!(
                                                table = %table_meta.name,
                                                ks = %table_meta.keyspace,
                                                "failed to register table from local schema: {e}"
                                            );
                                        }
                                    }
                                    tracing::info!(
                                        ks_count,
                                        table_count,
                                        "warm restart: loaded schema from local disk"
                                    );
                                }
                            }
                            Err(e) => tracing::warn!("failed to parse local schema.json: {e}"),
                        }
                    }
                    Err(e) => tracing::warn!("failed to read local schema.json: {e}"),
                }
            }
        } else if storage.has_s3() {
            tracing::info!("S3 configured, local data empty — attempting S3 bootstrap");
            if let Err(e) = bootstrap_from_s3(&storage, &schema).await {
                tracing::warn!("S3 bootstrap failed (non-fatal, starting fresh): {e}");
            }
        }
    }

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

    // Register MutationForwardHandler for cluster mode write forwarding
    let mutation_fwd_handler = Arc::new(ferrosa_cluster::MutationForwardHandler::new(
        storage.clone(),
    ));
    registry.register(
        ferrosa_net::codec::MsgType::MutationForward,
        mutation_fwd_handler,
    );

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
    let cql_bind: std::net::SocketAddr = config_val(
        "FERROSA_CQL_BIND",
        &file_config,
        "cql",
        "bind",
        "0.0.0.0:9042",
    )
    .parse()?;
    let auth_disabled_str = config_val(
        "FERROSA_AUTH_DISABLED",
        &file_config,
        "cql",
        "auth_disabled",
        "false",
    );
    let cql_tls_cert = config_val("FERROSA_CQL_TLS_CERT", &file_config, "cql", "tls_cert", "");
    let cql_tls_key = config_val("FERROSA_CQL_TLS_KEY", &file_config, "cql", "tls_key", "");
    let cql_require_tls = config_val(
        "FERROSA_CQL_REQUIRE_TLS",
        &file_config,
        "cql",
        "require_tls",
        "false",
    );
    let cql_config = ferrosa_cql::server::ServerConfig {
        bind_addr: cql_bind,
        auth_disabled: auth_disabled_str == "true" || auth_disabled_str == "1",
        tls_cert_path: if cql_tls_cert.is_empty() {
            None
        } else {
            Some(cql_tls_cert)
        },
        tls_key_path: if cql_tls_key.is_empty() {
            None
        } else {
            Some(cql_tls_key)
        },
        require_tls: cql_require_tls == "true" || cql_require_tls == "1",
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
    let udf_executor = Arc::new(
        ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default())
            .expect("failed to initialize UDF executor"),
    );
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
        udf_executor,
        event_sender: tokio::sync::broadcast::channel(64).0,
        mode_controller: Arc::clone(&mode_controller),
    });
    let auth_disabled = cql_config.auth_disabled;

    // 9a. Register observability virtual tables into the schema's shared registry
    //     so they are visible to both the CQL query router and the web console.
    //     Previously a separate VirtualTableRegistry was created for the web
    //     console only, which caused `SELECT * FROM system_observability.*`
    //     queries to fail with "table not found".
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::connections::ConnectionsTable::new(
            shared_state.connection_tracker.clone(),
        ),
    ));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::active_queries::ActiveQueriesTable::new(
            shared_state.query_tracker.clone(),
        ),
    ));
    // TODO: Register StorageStatsTable once StorageEngine implements StorageStatsProvider.
    // Until then, GET /api/storage_stats will return [].

    let cql_server = ferrosa_cql::server::CqlServer::new(cql_config, shared_state);
    let cql_addr = cql_server.start_background().await?;
    tracing::info!(%cql_addr, "CQL server listening");

    // 9b. Web observability console — reuse the same registry as the CQL router.
    let web_state = web::WebAppState {
        registry: schema.virtual_tables_arc(),
        mode_controller: mode_controller.clone(),
        schema: schema.clone(),
        storage: storage.clone(),
        host_id,
        auth_disabled,
    };
    let web_config = web::WebConfig::from_env();
    let web_addr = web::start_web_server(&web_config, web_state).await?;
    tracing::info!(%web_addr, "web console listening");

    // 10. Graph engine (check FERROSA_GRAPH_ENABLED)
    let graph_enabled = std::env::var("FERROSA_GRAPH_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // Create a shutdown watch channel for services that need graceful shutdown
    // notification (e.g. the Bolt server).
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

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

        // 10a. Graph HTTP server (port 7474)
        let schema_for_http = schema.clone();
        let state = ferrosa_graph::http::AppState {
            engine: graph_engine.clone(),
            schema: schema_for_http,
            auth_disabled,
        };
        tokio::spawn(async move {
            if let Err(e) = ferrosa_graph::http::start_graph_http(&http_config, state).await {
                tracing::error!(%e, "graph HTTP server failed");
            }
        });

        // 10b. Bolt server (port 7687)
        let bolt_port: u16 = config_val(
            "FERROSA_BOLT_PORT",
            &file_config,
            "graph",
            "bolt_port",
            "7687",
        )
        .parse()
        .unwrap_or(7687);
        let bolt_bind: std::net::SocketAddr = std::net::SocketAddr::from(([0, 0, 0, 0], bolt_port));
        let bolt_config = ferrosa_graph::bolt::server::BoltConfig {
            bind_addr: bolt_bind,
            ..ferrosa_graph::bolt::server::BoltConfig::default()
        };
        let bolt_engine = graph_engine;
        let bolt_schema = schema.clone();
        let bolt_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = ferrosa_graph::bolt::server::start_bolt_server(
                bolt_engine,
                bolt_schema,
                bolt_config,
                bolt_shutdown,
            )
            .await
            {
                tracing::error!(%e, "Bolt server failed");
            }
        });
        tracing::info!(%bolt_bind, "Bolt server starting");
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

    // 12. Background maintenance loop: periodic flush, compaction polling, commit log GC,
    //     and S3 schema persistence.
    let flush_interval_secs: u64 = config_val(
        "FERROSA_FLUSH_INTERVAL_SECS",
        &file_config,
        "storage",
        "flush_interval_secs",
        "30",
    )
    .parse()
    .unwrap_or(30);
    let maintenance_engine = storage.clone();
    let maintenance_schema = schema.clone();
    let has_s3 = maintenance_engine.has_s3();
    tokio::spawn(async move {
        let mut flush_interval =
            tokio::time::interval(std::time::Duration::from_secs(flush_interval_secs));
        let mut compact_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        // Persist schema snapshot + flush all memtables to S3 every 30s.
        let mut schema_sync_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut last_schema_version = uuid::Uuid::nil();

        loop {
            tokio::select! {
                _ = flush_interval.tick() => {
                    if let Err(e) = maintenance_engine.flush_if_needed() {
                        tracing::warn!(%e, "periodic flush failed");
                    }

                    // After flush, sync new SSTables to S3.
                    if has_s3 {
                        match maintenance_engine.sync_sstables_to_s3().await {
                            Ok(n) if n > 0 => {
                                tracing::info!(count = n, "synced SSTables to S3");
                            }
                            Err(e) => {
                                tracing::warn!(%e, "S3 SSTable sync failed");
                            }
                            _ => {}
                        }
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
                }
                _ = schema_sync_interval.tick() => {
                    if !has_s3 { continue; }
                    let snap = maintenance_schema.snapshot();
                    if snap.version != last_schema_version {
                        // Flush all memtables before persisting schema so SSTables
                        // in S3 match the schema snapshot (Cassandra SNAPSHOT reason).
                        if let Err(e) = maintenance_engine.flush_all() {
                            tracing::warn!(%e, "pre-schema-persist flush failed, skipping schema persist");
                            continue; // Don't persist schema without data — retry next tick
                        }
                        // Sync flushed SSTables to S3 before saving schema.
                        match maintenance_engine.sync_sstables_to_s3().await {
                            Ok(n) if n > 0 => {
                                tracing::info!(count = n, "pre-schema-persist S3 sync");
                            }
                            Err(e) => {
                                tracing::warn!(%e, "pre-schema-persist S3 sync failed, skipping schema persist");
                                continue; // Don't persist schema without data — retry next tick
                            }
                            _ => {}
                        }
                        persist_schema_to_s3(&maintenance_engine, &maintenance_schema).await;
                        last_schema_version = snap.version;
                    }
                }
            }
        }
    });

    // 13. Wait for shutdown signal (SIGINT or SIGTERM)
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    // 14. Graceful shutdown: flush memtables, sync to S3, stop compaction
    tracing::info!("shutdown signal received, draining...");

    // Signal Bolt server (and any other watch-based services) to stop.
    let _ = shutdown_tx.send(true);

    let shutdown_timeout = std::time::Duration::from_secs(30);
    match tokio::time::timeout(shutdown_timeout, async {
        // Drain internode connections before flushing memtables so peers stop
        // sending mutations while we're in the middle of a flush.
        tracing::info!("draining internode connections...");
        rpc_server
            .shutdown(std::time::Duration::from_secs(10))
            .await;
        tracing::info!("internode drained");

        // Flush all memtables to SSTables on disk.
        storage.shutdown()?;
        tracing::info!("memtables flushed");

        // Sync any new SSTables to S3 and persist schema.
        if storage.has_s3() {
            match storage.sync_sstables_to_s3().await {
                Ok(n) => tracing::info!(count = n, "shutdown: synced SSTables to S3"),
                Err(e) => tracing::warn!(%e, "shutdown: S3 SSTable sync failed"),
            }
            persist_schema_to_s3(&storage, &schema).await;
            tracing::info!("shutdown: schema snapshot persisted to S3");
        }

        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    {
        Ok(Ok(())) => tracing::info!("clean shutdown"),
        Ok(Err(e)) => tracing::error!(%e, "shutdown error"),
        Err(_) => tracing::error!("shutdown timed out after 30s"),
    }

    tracing::info!("ferrosa stopped");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_config() -> toml::Value {
        toml::Value::Table(toml::map::Map::new())
    }

    fn sample_config() -> toml::Value {
        toml::from_str(
            r#"
            [storage]
            data_dir = "/data/ferrosa"

            [cql]
            bind = "127.0.0.1:19042"
            auth_disabled = true

            [internode]
            cluster_name = "test-cluster"
            "#,
        )
        .unwrap()
    }

    #[test]
    fn config_val_returns_default_when_empty_config_and_no_env() {
        // Use a unique env key to avoid collisions with real env vars.
        let key = "FERROSA_TEST_CFG_EMPTY_5a3b";
        std::env::remove_var(key);

        let result = config_val(
            key,
            &empty_config(),
            "storage",
            "data_dir",
            "/var/lib/ferrosa",
        );
        assert_eq!(result, "/var/lib/ferrosa");
    }

    #[test]
    fn config_val_reads_from_toml_when_no_env() {
        let key = "FERROSA_TEST_CFG_TOML_7d2e";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "storage", "data_dir", "/var/lib/ferrosa");
        assert_eq!(result, "/data/ferrosa");
    }

    #[test]
    fn config_val_env_overrides_toml() {
        let key = "FERROSA_TEST_CFG_OVERRIDE_9f1c";
        std::env::set_var(key, "/env/override");
        let config = sample_config();

        let result = config_val(key, &config, "storage", "data_dir", "/var/lib/ferrosa");
        assert_eq!(result, "/env/override");

        std::env::remove_var(key);
    }

    #[test]
    fn config_val_handles_boolean_values() {
        let key = "FERROSA_TEST_CFG_BOOL_4c8a";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "cql", "auth_disabled", "false");
        assert_eq!(result, "true");
    }

    #[test]
    fn config_val_missing_section_returns_default() {
        let key = "FERROSA_TEST_CFG_NOSEC_1b7f";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "nonexistent", "key", "fallback");
        assert_eq!(result, "fallback");
    }

    #[test]
    fn config_val_missing_key_returns_default() {
        let key = "FERROSA_TEST_CFG_NOKEY_8e3d";
        std::env::remove_var(key);
        let config = sample_config();

        let result = config_val(key, &config, "storage", "nonexistent", "default_val");
        assert_eq!(result, "default_val");
    }

    #[test]
    fn load_config_returns_empty_table_for_missing_file() {
        let result = load_config("/tmp/ferrosa_nonexistent_config_test.toml").unwrap();
        assert!(result.is_table());
        assert!(result.as_table().unwrap().is_empty());
    }

    #[test]
    fn load_config_parses_valid_toml_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ferrosa.toml");
        std::fs::write(
            &path,
            r#"
            [storage]
            data_dir = "/test/data"

            [cql]
            bind = "0.0.0.0:9042"
            "#,
        )
        .unwrap();

        let config = load_config(path.to_str().unwrap()).unwrap();
        assert_eq!(
            config
                .get("storage")
                .unwrap()
                .get("data_dir")
                .unwrap()
                .as_str()
                .unwrap(),
            "/test/data"
        );
        assert_eq!(
            config
                .get("cql")
                .unwrap()
                .get("bind")
                .unwrap()
                .as_str()
                .unwrap(),
            "0.0.0.0:9042"
        );
    }

    #[test]
    fn load_config_rejects_invalid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "this is not [valid toml =").unwrap();

        assert!(load_config(path.to_str().unwrap()).is_err());
    }

    #[test]
    fn example_config_file_is_valid_toml() {
        let example = include_str!("../ferrosa.example.toml");
        let parsed: Result<toml::Value, _> = toml::from_str(example);
        assert!(parsed.is_ok(), "ferrosa.example.toml must be valid TOML");

        let config = parsed.unwrap();
        // Verify key sections exist
        assert!(config.get("cql").is_some());
        assert!(config.get("internode").is_some());
        assert!(config.get("storage").is_some());
        assert!(config.get("s3").is_some());
        assert!(config.get("graph").is_some());
        assert!(config.get("web").is_some());
    }
}
