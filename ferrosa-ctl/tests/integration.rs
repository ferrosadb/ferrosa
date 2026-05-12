//! Integration tests for `ferrosa-ctl` subcommand queries.
//!
//! These tests boot a CQL server, register the `system_observability.*`
//! virtual tables, connect via `CqlClient`, and verify the queries that
//! `ferrosa-ctl` commands issue against the running server.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_cluster::WritePath;
use ferrosa_cql::client::{CqlClient, QueryResult, ResultRow};
use ferrosa_cql::prepared::PreparedCache;
use ferrosa_cql::router::SharedState;
use ferrosa_cql::server::{CqlServer, ServerConfig};
use ferrosa_cql::topology::ClientTopologyPolicy;
use ferrosa_cql::virtual_tables::active_queries::{ActiveQueriesTable, QueryTracker};
use ferrosa_cql::virtual_tables::connections::{ConnectionTracker, ConnectionsTable};

use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, NodeConfig, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a UTF-8 string from a column cell, returning None for null cells.
fn cell_as_str(row: &ResultRow, idx: usize) -> Option<String> {
    row.columns
        .get(idx)
        .and_then(|c| c.as_ref())
        .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
}

/// Find the index of a column by name in a QueryResult.
fn column_index(result: &QueryResult, name: &str) -> Option<usize> {
    result.column_names.iter().position(|n| n == name)
}

/// Create storage engine, schema, and shared state for a test.
/// Registers `system_observability.connections` and `system_observability.active_queries`
/// virtual tables so the queries ferrosa-ctl issues work end-to-end.
fn setup_state() -> (Arc<SharedState>, TempDir) {
    let dir = TempDir::new().unwrap();
    let commit_log = CommitLogConfig {
        segment_size: 4096,
        max_segment_age: std::time::Duration::from_secs(60),
        sync_strategy: SyncStrategyConfig::Batch,
        log_dir: dir.path().join("commitlog"),
        checkpoint_dir: dir.path().join("commitlog"),
        archive: None,
    };
    let compaction = CompactionConfig::from_env(dir.path().join("compaction"));
    let engine_config = StorageEngineConfig {
        commit_log,
        compaction,
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
        memtable_num_shards: 64,
    };
    let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());
    let schema = Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap(),
    );
    let node_config = Arc::new(NodeConfig {
        cluster_name: "ctl-integration-test".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        rpc_port: 9042,
        host_id: uuid::Uuid::new_v4(),
        listen_address: "127.0.0.1".parse().unwrap(),
        listen_port: 7000,
        broadcast_address: "127.0.0.1".parse().unwrap(),
        broadcast_port: 7000,
        rpc_address: "127.0.0.1".parse().unwrap(),
        internal_rpc_address: "127.0.0.1".parse().unwrap(),
        internal_rpc_port: 9042,
        tokens: vec![],
    });
    let connection_tracker = Arc::new(ConnectionTracker::new());
    let query_tracker = Arc::new(QueryTracker::new());

    // Register virtual tables so CQL SELECT against system_observability works.
    schema
        .virtual_tables()
        .register(Arc::new(ConnectionsTable::new(connection_tracker.clone())));
    schema
        .virtual_tables()
        .register(Arc::new(ActiveQueriesTable::new(query_tracker.clone())));
    schema.virtual_tables().register(Arc::new(
        ferrosa_cql::virtual_tables::ConsolidationStatusTable::new(schema.clone()),
    ));

    let udf_executor =
        Arc::new(ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).unwrap());
    let mode_controller =
        ferrosa_cluster::ModeController::standalone_for_test(schema.clone(), engine.clone());
    let state = Arc::new(SharedState {
        engine: engine.clone(),
        schema: schema.clone(),
        node_config,
        cluster_state: Arc::new(ArcSwap::from_pointee(
            ferrosa_cluster::ClusterStateHolder::Standalone,
        )),
        write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone()))),
        ddl_path: Arc::new(ArcSwap::from_pointee(ferrosa_cluster::DdlPath::Direct {
            schema,
            engine,
        })),
        prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
        connection_tracker,
        query_tracker,
        udf_executor,
        event_sender: tokio::sync::broadcast::channel(64).0,
        mode_controller,
        topology_policy: ClientTopologyPolicy::default(),
        cql_metrics: Arc::new(ferrosa_cql::observability::CqlMetrics::new()),
        auth_warn: false,
        peer_manager: None,
        accord_clock: None,
    });
    (state, dir)
}

/// Server config with auth disabled, binding to a random port.
fn test_config() -> ServerConfig {
    ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        auth_disabled: true,
        ..ServerConfig::default()
    }
}

/// Boot a CQL server and return a connected, ready client.
async fn boot_and_connect() -> (CqlClient, Arc<SharedState>, TempDir) {
    let (state, dir) = setup_state();
    let server = CqlServer::new(test_config(), state.clone());
    let addr = server.start_background().await.unwrap();
    let client = CqlClient::connect(addr).await.unwrap();
    assert!(client.is_ready(), "client should be ready after connect");
    (client, state, dir)
}

// ---------------------------------------------------------------------------
// Tests — system_observability.connections
// ---------------------------------------------------------------------------

/// Verify that `SELECT * FROM system_observability.connections` returns at
/// least one row representing our own connection. This is the same query
/// that `ferrosa-ctl connections` and `ferrosa-ctl status` issue.
#[tokio::test]
async fn connections_returns_at_least_one_row() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("SELECT system_observability.connections failed");

    assert!(
        !result.rows.is_empty(),
        "expected at least 1 connection row (our own); got 0"
    );

    // Verify expected columns are present.
    assert!(
        column_index(&result, "peer_address").is_some(),
        "connections should have 'peer_address'; columns: {:?}",
        result.column_names
    );
    assert!(
        column_index(&result, "peer_port").is_some(),
        "connections should have 'peer_port'"
    );
    assert!(
        column_index(&result, "state").is_some(),
        "connections should have 'state'"
    );
    assert!(
        column_index(&result, "protocol_version").is_some(),
        "connections should have 'protocol_version'"
    );
}

/// Verify the `state` column for our connection reports "ready".
#[tokio::test]
async fn connection_state_is_ready() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("SELECT connections failed");

    let state_idx = column_index(&result, "state").expect("'state' column not found");
    let states: Vec<Option<String>> = result
        .rows
        .iter()
        .map(|row| cell_as_str(row, state_idx))
        .collect();

    assert!(
        states.iter().any(|s| s.as_deref() == Some("ready")),
        "expected at least one connection in 'ready' state; states: {states:?}"
    );
}

/// Verify that opening a second connection increases the connection count.
#[tokio::test]
async fn multiple_connections_tracked() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(), state.clone());
    let addr = server.start_background().await.unwrap();

    let mut client1 = CqlClient::connect(addr).await.unwrap();
    assert!(client1.is_ready());

    // First client should see at least 1 connection.
    let r1 = client1
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("query failed");
    let count1 = r1.rows.len();
    assert!(count1 >= 1, "expected >= 1 connection; got {count1}");

    // Open a second client.
    let _client2 = CqlClient::connect(addr).await.unwrap();

    // Now we should see more connections.
    let r2 = client1
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("query failed");
    let count2 = r2.rows.len();
    assert!(
        count2 > count1,
        "expected connection count to increase from {count1} after second connect; got {count2}"
    );
}

// ---------------------------------------------------------------------------
// Tests — system_observability.active_queries
// ---------------------------------------------------------------------------

/// Verify that `SELECT * FROM system_observability.active_queries` succeeds.
/// Active queries are transient, so the table may be empty when we read it
/// (our own SELECT has already finished by the time results are returned).
/// The important thing is that the query succeeds (no "table not found").
#[tokio::test]
async fn active_queries_is_queryable() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system_observability.active_queries")
        .await
        .expect("SELECT system_observability.active_queries failed");

    // Verify expected columns are present.
    assert!(
        column_index(&result, "query_id").is_some(),
        "active_queries should have 'query_id'; columns: {:?}",
        result.column_names
    );
    assert!(
        column_index(&result, "query_text").is_some(),
        "active_queries should have 'query_text'"
    );
    assert!(
        column_index(&result, "elapsed_ms").is_some(),
        "active_queries should have 'elapsed_ms'"
    );
    assert!(
        column_index(&result, "state").is_some(),
        "active_queries should have 'state'"
    );
}

// ---------------------------------------------------------------------------
// Tests — status query (combines connections + summary)
// ---------------------------------------------------------------------------

/// Simulate the query that `ferrosa-ctl status` issues and verify it works.
#[tokio::test]
async fn status_query_succeeds() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    // ferrosa-ctl status issues: SELECT * FROM system_observability.connections
    let result = client
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("status query failed");

    let conn_count = result.rows.len();
    assert!(conn_count >= 1, "status should report >= 1 connection");
}

// ---------------------------------------------------------------------------
// Tests — system.local (used by ferrosa-ctl topology/peers indirectly)
// ---------------------------------------------------------------------------

/// Verify `SELECT * FROM system.local` returns data (used for cluster context).
#[tokio::test]
async fn system_local_accessible() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system.local")
        .await
        .expect("SELECT system.local failed");

    assert!(
        !result.rows.is_empty(),
        "system.local should return at least one row"
    );

    assert!(
        column_index(&result, "cluster_name").is_some(),
        "system.local should have 'cluster_name'"
    );
    assert!(
        column_index(&result, "data_center").is_some(),
        "system.local should have 'data_center'"
    );
}

/// Verify `SELECT * FROM system.peers` succeeds (used by `ferrosa-ctl peers`
/// and `ferrosa-ctl topology`). In a single-node test the result may be empty
/// but the query must not fail.
#[tokio::test]
async fn system_peers_accessible() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT peer, data_center, rack, release_version FROM system.peers")
        .await
        .expect("SELECT system.peers failed");

    // Single-node: zero rows is expected but the query must succeed.
    let _ = result.rows.len();
}

// ---------------------------------------------------------------------------
// Tests — connections column structure matches ferrosa-ctl expectations
// ---------------------------------------------------------------------------

/// Verify the full column set that ferrosa-ctl relies on for table rendering.
#[tokio::test]
async fn connections_has_all_expected_columns() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("SELECT connections failed");

    let expected = [
        "peer_address",
        "peer_port",
        "state",
        "username",
        "idle_seconds",
        "requests_served",
        "protocol_version",
    ];

    for col in &expected {
        assert!(
            column_index(&result, col).is_some(),
            "connections is missing expected column '{}'; have: {:?}",
            col,
            result.column_names
        );
    }
}

/// Verify the full column set for active_queries that ferrosa-ctl expects.
#[tokio::test]
async fn active_queries_has_all_expected_columns() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system_observability.active_queries")
        .await
        .expect("SELECT active_queries failed");

    let expected = [
        "query_id",
        "client_address",
        "username",
        "query_text",
        "keyspace",
        "start_time",
        "elapsed_ms",
        "state",
    ];

    for col in &expected {
        assert!(
            column_index(&result, col).is_some(),
            "active_queries is missing expected column '{}'; have: {:?}",
            col,
            result.column_names
        );
    }
}

// ---------------------------------------------------------------------------
// Tests — peer_address is populated correctly
// ---------------------------------------------------------------------------

/// Verify that the peer_address in the connections table contains a valid
/// loopback address (since we connect from localhost).
#[tokio::test]
async fn connection_peer_address_is_loopback() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("SELECT connections failed");

    let addr_idx = column_index(&result, "peer_address").expect("peer_address column missing");
    let addrs: Vec<Option<String>> = result
        .rows
        .iter()
        .map(|row| cell_as_str(row, addr_idx))
        .collect();

    assert!(
        addrs.iter().any(|a| a.as_deref() == Some("127.0.0.1")),
        "expected at least one connection from 127.0.0.1; addrs: {addrs:?}"
    );
}

// ---------------------------------------------------------------------------
// Tests — requests_served increments
// ---------------------------------------------------------------------------

/// Verify that issuing queries increments `requests_served` for the connection.
#[tokio::test]
async fn requests_served_increments() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    // Issue a first query to see the baseline.
    let r1 = client
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("first query failed");

    let req_idx = column_index(&r1, "requests_served").expect("requests_served column missing");

    // Issue a few more queries to increment the counter.
    client
        .query("SELECT * FROM system.local")
        .await
        .expect("query failed");
    client
        .query("SELECT * FROM system.local")
        .await
        .expect("query failed");

    // Check the counter again.
    let r2 = client
        .query("SELECT * FROM system_observability.connections")
        .await
        .expect("second query failed");

    // The requests_served values are encoded as big-endian i64 bytes.
    // CqlClient returns them as raw bytes; cell_as_str won't decode
    // int columns as meaningful strings, so we check that the raw
    // bytes differ (the counter increased).
    //
    // For each row, grab the raw bytes of requests_served.
    let get_req_bytes = |result: &QueryResult| -> Vec<Vec<u8>> {
        result
            .rows
            .iter()
            .filter_map(|row| row.columns.get(req_idx).and_then(|c| c.as_ref()).cloned())
            .collect()
    };

    let before = get_req_bytes(&r1);
    let after = get_req_bytes(&r2);

    // We should have at least one connection, and its counter should differ.
    assert!(!before.is_empty(), "no connections in first query");
    assert!(!after.is_empty(), "no connections in second query");

    // At least one row should show a higher requests_served.
    let max_before: i64 = before
        .iter()
        .filter_map(|b| b.as_slice().try_into().ok().map(i64::from_be_bytes))
        .max()
        .unwrap_or(0);
    let max_after: i64 = after
        .iter()
        .filter_map(|b| b.as_slice().try_into().ok().map(i64::from_be_bytes))
        .max()
        .unwrap_or(0);

    assert!(
        max_after > max_before,
        "requests_served should have increased; before={max_before}, after={max_after}"
    );
}
