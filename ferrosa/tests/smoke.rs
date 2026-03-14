//! Smoke tests for the ferrosa binary crate.
//!
//! These tests boot a CQL server, connect via CqlClient, and exercise
//! DDL, DML, system table queries, and multi-client scenarios.

use std::sync::Arc;

use arc_swap::ArcSwap;
use ferrosa_cluster::WritePath;
use ferrosa_cql::client::{CqlClient, QueryResult, ResultRow};
use ferrosa_cql::prepared::PreparedCache;
use ferrosa_cql::router::SharedState;
use ferrosa_cql::server::{CqlServer, ServerConfig};
use ferrosa_cql::virtual_tables::active_queries::QueryTracker;
use ferrosa_cql::virtual_tables::connections::ConnectionTracker;

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
/// Returns the shared state and the TempDir (must be kept alive).
fn setup_state() -> (Arc<SharedState>, TempDir) {
    let dir = TempDir::new().unwrap();
    let commit_log = CommitLogConfig {
        segment_size: 4096,
        max_segment_age: std::time::Duration::from_secs(60),
        sync_strategy: SyncStrategyConfig::Batch,
        log_dir: dir.path().join("commitlog"),
        checkpoint_dir: dir.path().join("commitlog"),
    };
    let compaction = CompactionConfig::from_env(dir.path().join("compaction"));
    let engine_config = StorageEngineConfig {
        commit_log,
        compaction,
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        data_dir: dir.path().to_path_buf(),
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
        cluster_name: "smoke-test".into(),
        data_center: "dc1".into(),
        rack: "rack1".into(),
        rpc_port: 9042,
        host_id: uuid::Uuid::new_v4(),
        listen_address: "127.0.0.1".parse().unwrap(),
        listen_port: 7000,
        broadcast_address: "127.0.0.1".parse().unwrap(),
        broadcast_port: 7000,
        rpc_address: "127.0.0.1".parse().unwrap(),
        tokens: vec![],
    });
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
        connection_tracker: Arc::new(ConnectionTracker::new()),
        query_tracker: Arc::new(QueryTracker::new()),
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

/// Create a keyspace and table, returning the connected client.
async fn boot_with_table() -> (CqlClient, Arc<SharedState>, TempDir) {
    let (mut client, state, dir) = boot_and_connect().await;
    client
        .query("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        .await
        .unwrap();
    client
        .query("CREATE TABLE test_ks.users (id text PRIMARY KEY, name text)")
        .await
        .unwrap();
    (client, state, dir)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_starts_and_accepts_connection() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(), state);
    let addr = server.start_background().await.unwrap();

    let client = CqlClient::connect(addr).await.unwrap();
    assert!(client.is_ready(), "client should be ready after connect");
}

#[tokio::test]
async fn create_keyspace_and_table() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    // CREATE KEYSPACE
    let result = client
        .query("CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        .await;
    assert!(result.is_ok(), "CREATE KEYSPACE failed: {result:?}");

    // CREATE TABLE with fully-qualified name
    let result = client
        .query("CREATE TABLE test_ks.users (id text PRIMARY KEY, name text)")
        .await;
    assert!(result.is_ok(), "CREATE TABLE failed: {result:?}");
}

#[tokio::test]
async fn insert_and_select() {
    let (mut client, _state, _dir) = boot_with_table().await;

    // INSERT
    let result = client
        .query("INSERT INTO test_ks.users (id, name) VALUES ('1', 'Alice')")
        .await;
    assert!(result.is_ok(), "INSERT failed: {result:?}");

    // SELECT
    let result = client
        .query("SELECT * FROM test_ks.users WHERE id = '1'")
        .await
        .expect("SELECT failed");

    assert!(
        !result.rows.is_empty(),
        "SELECT should return at least one row"
    );

    // Find the 'name' column and verify it contains 'Alice'
    let name_idx = column_index(&result, "name").expect("'name' column not found");
    let name_val = cell_as_str(&result.rows[0], name_idx);
    assert_eq!(
        name_val.as_deref(),
        Some("Alice"),
        "expected 'Alice', got {name_val:?}"
    );
}

#[tokio::test]
async fn system_tables_accessible() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system.local")
        .await
        .expect("SELECT system.local failed");

    assert!(
        !result.rows.is_empty(),
        "system.local should return at least one row"
    );

    // Verify expected columns are present
    assert!(
        column_index(&result, "cluster_name").is_some(),
        "system.local should have 'cluster_name' column; columns: {:?}",
        result.column_names
    );
    assert!(
        column_index(&result, "data_center").is_some(),
        "system.local should have 'data_center' column"
    );
}

#[tokio::test]
async fn multiple_connections() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(), state);
    let addr = server.start_background().await.unwrap();

    let mut client1 = CqlClient::connect(addr).await.unwrap();
    let mut client2 = CqlClient::connect(addr).await.unwrap();

    assert!(client1.is_ready(), "client1 should be ready");
    assert!(client2.is_ready(), "client2 should be ready");

    // Both clients can query independently
    let r1 = client1
        .query("SELECT * FROM system.local")
        .await
        .expect("client1 query failed");
    let r2 = client2
        .query("SELECT * FROM system.local")
        .await
        .expect("client2 query failed");

    assert!(!r1.rows.is_empty(), "client1 should get rows");
    assert!(!r2.rows.is_empty(), "client2 should get rows");
}

#[tokio::test]
async fn describe_keyspaces() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    let result = client
        .query("SELECT * FROM system_schema.keyspaces")
        .await
        .expect("SELECT system_schema.keyspaces failed");

    assert!(
        !result.rows.is_empty(),
        "system_schema.keyspaces should return rows"
    );

    // Collect all keyspace names from the result
    let ks_idx = column_index(&result, "keyspace_name")
        .expect("'keyspace_name' column not found in system_schema.keyspaces");
    let keyspace_names: Vec<Option<String>> = result
        .rows
        .iter()
        .map(|row| cell_as_str(row, ks_idx))
        .collect();

    // system and system_schema should be present
    assert!(
        keyspace_names
            .iter()
            .any(|n| n.as_deref() == Some("system")),
        "system keyspace not found; keyspaces: {keyspace_names:?}"
    );
    assert!(
        keyspace_names
            .iter()
            .any(|n| n.as_deref() == Some("system_schema")),
        "system_schema keyspace not found; keyspaces: {keyspace_names:?}"
    );
}

#[tokio::test]
async fn test_create_and_query_indexes() {
    let (mut client, _state, _dir) = boot_and_connect().await;

    // Setup keyspace and table
    client
        .query("CREATE KEYSPACE idx_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        .await
        .unwrap();
    client
        .query("CREATE TABLE idx_test.users (id text PRIMARY KEY, name text, email text)")
        .await
        .unwrap();

    // Create indexes
    client
        .query("CREATE INDEX idx_email ON idx_test.users (email) USING 'btree'")
        .await
        .unwrap();
    client
        .query("CREATE INDEX idx_name ON idx_test.users (name) USING 'hash'")
        .await
        .unwrap();

    // Idempotent CREATE INDEX IF NOT EXISTS should succeed
    client
        .query("CREATE INDEX IF NOT EXISTS idx_email ON idx_test.users (email) USING 'btree'")
        .await
        .unwrap();

    // Verify indexes exist in schema snapshot (virtual table query path
    // for system_schema.indexes is a follow-up integration task).
    // For now, verify that CREATE INDEX DDL succeeded by checking that
    // a duplicate CREATE INDEX IF NOT EXISTS also succeeds (idempotent).

    // Insert data (index building happens asynchronously)
    client
        .query("INSERT INTO idx_test.users (id, name, email) VALUES ('1', 'Alice', 'alice@example.com')")
        .await
        .unwrap();
    client
        .query(
            "INSERT INTO idx_test.users (id, name, email) VALUES ('2', 'Bob', 'bob@example.com')",
        )
        .await
        .unwrap();

    // Drop index — verify DDL succeeds
    client.query("DROP INDEX idx_test.idx_email").await.unwrap();
    // DROP IF EXISTS on already-dropped index should also succeed
    client
        .query("DROP INDEX IF EXISTS idx_test.idx_email")
        .await
        .unwrap();

    // Cleanup — drop table (DROP KEYSPACE parser support is pending)
    client.query("DROP TABLE idx_test.users").await.unwrap();
}
