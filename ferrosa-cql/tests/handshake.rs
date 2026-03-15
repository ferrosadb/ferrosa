//! Integration test: CQL v5 auth handshake.
//!
//! These tests validate the CQL protocol handshake sequence including
//! the ConnectionPhase state machine (M7).

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use arc_swap::ArcSwap;
use ferrosa_cluster::WritePath;
use ferrosa_cql::frame::*;
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

/// Timeout for handshake operations — fail fast if the server doesn't respond.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

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
        cluster_name: "test".into(),
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
        event_sender: tokio::sync::broadcast::channel(64).0,
    });
    (state, dir)
}

fn encode_startup_frame() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u16(1);
    let key = b"CQL_VERSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    let val = b"3.0.0";
    body.put_u16(val.len() as u16);
    body.put_slice(val);

    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Startup,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

fn encode_auth_response(username: &str, password: &str) -> BytesMut {
    let sasl = format!("\0{username}\0{password}");
    let sasl_bytes = sasl.as_bytes();

    let mut body = BytesMut::new();
    body.put_i32(sasl_bytes.len() as i32);
    body.put_slice(sasl_bytes);

    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::AuthResponse,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

async fn send_startup(stream: &mut TcpStream) {
    let buf = encode_startup_frame();
    stream.write_all(&buf).await.unwrap();
}

struct RawFrame {
    header: FrameHeader,
    opcode: Opcode,
    body: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream) -> RawFrame {
    let mut hdr_buf = vec![0u8; HEADER_SIZE];
    timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut hdr_buf))
        .await
        .expect("timed out waiting for frame header — connection handler not implemented?")
        .unwrap();
    let header = FrameHeader::decode(&hdr_buf).unwrap();
    let mut body = vec![0u8; header.length as usize];
    if !body.is_empty() {
        timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut body))
            .await
            .expect("timed out waiting for frame body")
            .unwrap();
    }
    let opcode = header.opcode;
    RawFrame {
        header,
        opcode,
        body,
    }
}

async fn send_raw_frame(stream: &mut TcpStream, opcode: Opcode, body: &[u8]) {
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(body);
    stream.write_all(&buf).await.unwrap();
}

#[allow(dead_code)]
async fn send_raw_frame_with_stream(
    stream: &mut TcpStream,
    opcode: Opcode,
    body: &[u8],
    stream_id: i16,
) {
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id,
        opcode,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(body);
    stream.write_all(&buf).await.unwrap();
}

fn test_config(auth_disabled: bool) -> ServerConfig {
    ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        auth_disabled,
        ..ServerConfig::default()
    }
}

/// Helper: complete startup+auth handshake and return a ready connection.
#[allow(dead_code)]
async fn connect_and_authenticate(addr: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let auth = encode_auth_response("cassandra", "cassandra");
    stream.write_all(&auth).await.unwrap();
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::AuthSuccess);
    stream
}

/// Helper: complete startup for auth_disabled server.
async fn connect_auth_disabled(addr: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Ready);
    stream
}

fn encode_query_body(query: &str) -> Vec<u8> {
    let query_bytes = query.as_bytes();
    let mut body = Vec::new();
    body.extend_from_slice(&(query_bytes.len() as i32).to_be_bytes());
    body.extend_from_slice(query_bytes);
    // consistency (short) + flags (byte)
    body.extend_from_slice(&0u16.to_be_bytes()); // ONE
    body.push(0); // no flags
    body
}

fn encode_prepare_body(query: &str) -> Vec<u8> {
    let query_bytes = query.as_bytes();
    let mut body = Vec::new();
    body.extend_from_slice(&(query_bytes.len() as i32).to_be_bytes());
    body.extend_from_slice(query_bytes);
    body
}

/// Assert that the frame is a RESULT; if it's an ERROR, decode and panic with the error message.
fn assert_result(resp: &RawFrame) {
    if resp.opcode == Opcode::Error && resp.body.len() >= 6 {
        let error_code = i32::from_be_bytes(resp.body[..4].try_into().unwrap());
        let msg_len = u16::from_be_bytes(resp.body[4..6].try_into().unwrap()) as usize;
        let msg = if resp.body.len() >= 6 + msg_len {
            std::str::from_utf8(&resp.body[6..6 + msg_len]).unwrap_or("<invalid utf8>")
        } else {
            "<truncated>"
        };
        panic!("expected RESULT but got ERROR(0x{error_code:04X}): {msg}");
    }
    assert_eq!(resp.opcode, Opcode::Result);
}

// ── Original handshake tests (un-ignored) ────────────────────────────────

#[tokio::test]
async fn startup_then_authenticate_then_auth_success() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();

    // Read AUTHENTICATE response
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    // Send AUTH_RESPONSE with valid credentials
    let auth = encode_auth_response("cassandra", "cassandra");
    stream.write_all(&auth).await.unwrap();

    // Read AUTH_SUCCESS
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::AuthSuccess);
}

#[tokio::test]
async fn malformed_sasl_payload_returns_bad_credentials() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let bad_payload = b"not-valid-sasl";
    let mut body = Vec::new();
    body.extend_from_slice(&(bad_payload.len() as i32).to_be_bytes());
    body.extend_from_slice(bad_payload);
    send_raw_frame(&mut stream, Opcode::AuthResponse, &body).await;

    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Error);
    let error_code = i32::from_be_bytes(resp.body[..4].try_into().unwrap());
    assert_eq!(error_code, 0x0100);
}

#[tokio::test]
async fn three_failed_auth_attempts_closes_connection() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let bad_payload = b"not-valid-sasl";
    for _ in 0..3 {
        let mut body = Vec::new();
        body.extend_from_slice(&(bad_payload.len() as i32).to_be_bytes());
        body.extend_from_slice(bad_payload);
        send_raw_frame(&mut stream, Opcode::AuthResponse, &body).await;

        let resp = read_frame(&mut stream).await;
        assert_eq!(resp.opcode, Opcode::Error);
    }

    let mut buf = vec![0u8; 64];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "connection should be closed after 3 auth failures");
}

#[tokio::test]
async fn auth_disabled_startup_returns_ready() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Ready);
}

// ── New integration tests ─────────────────────────────────────────────────

#[tokio::test]
async fn query_creates_keyspace_and_table_and_inserts_and_selects() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace
    let query = encode_query_body(
        "CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // USE keyspace
    let query = encode_query_body("USE test_ks");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Create table
    let query = encode_query_body("CREATE TABLE users (id int PRIMARY KEY, name text)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Insert
    let query = encode_query_body("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Select
    let query = encode_query_body("SELECT * FROM users WHERE id = 1");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
}

#[tokio::test]
async fn prepare_and_execute() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace and table
    let query = encode_query_body(
        "CREATE KEYSPACE prep_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("USE prep_ks");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("CREATE TABLE items (id int PRIMARY KEY, val text)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // PREPARE an INSERT
    let prep_body = encode_prepare_body("INSERT INTO items (id, val) VALUES (1, 'test')");
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Extract the prepared ID from the response body.
    // Body format: [i32 kind=0x0004][u16 id_len=16][bytes id]
    assert!(resp.body.len() >= 22, "PREPARED response too short");
    let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0004, "expected PREPARED result kind");
    let id_len = u16::from_be_bytes(resp.body[4..6].try_into().unwrap()) as usize;
    assert_eq!(id_len, 16);
    let mut prepared_id = [0u8; 16];
    prepared_id.copy_from_slice(&resp.body[6..22]);

    // EXECUTE the prepared statement.
    let mut exec_body = Vec::new();
    exec_body.extend_from_slice(&16u16.to_be_bytes()); // id_len
    exec_body.extend_from_slice(&prepared_id);
    exec_body.extend_from_slice(&0u16.to_be_bytes()); // consistency ONE
    exec_body.push(0); // flags: no bound values
    send_raw_frame(&mut stream, Opcode::Execute, &exec_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
}

/// Exercises the full set of system table queries that cqlsh sends during
/// startup introspection. This is the sequence that caused "'local' not found
/// in keyspace 'system'" and similar errors.
#[tokio::test]
async fn cqlsh_introspection_queries_all_succeed() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // cqlsh startup introspection queries — these must all return RESULT (Rows).
    let introspection_queries = [
        "SELECT * FROM system.local",
        "SELECT * FROM system.peers",
        "SELECT * FROM system.peers_v2",
        "SELECT * FROM system_schema.keyspaces",
        "SELECT * FROM system_schema.tables",
        "SELECT * FROM system_schema.columns",
        "SELECT * FROM system_schema.types",
        "SELECT * FROM system_schema.functions",
        "SELECT * FROM system_schema.aggregates",
        "SELECT * FROM system_schema.triggers",
        "SELECT * FROM system_schema.views",
        "SELECT * FROM system_schema.indexes",
    ];

    for cql in &introspection_queries {
        let query = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &query).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);

        // Verify it's a Rows result (kind = 0x0002)
        let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
        assert_eq!(kind, 0x0002, "{cql} should return Rows result");
    }
}

/// Verifies system.local returns the `tokens` column (set<varchar>).
/// Regression: cqlsh prints "'local' not found in keyspace 'system'" when
/// the tokens column is missing from system_schema.columns metadata.
#[tokio::test]
async fn cqlsh_system_local_has_tokens_column() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Query system_schema.columns for system.local columns
    let query = encode_query_body("SELECT * FROM system_schema.columns");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // The response body should contain "tokens" somewhere in the row data.
    // Simple check: the bytes for "tokens" must appear in the result.
    let body_str = String::from_utf8_lossy(&resp.body);
    assert!(
        body_str.contains("tokens"),
        "system_schema.columns must include a 'tokens' entry for system.local"
    );
}

/// Full cqlsh-like workflow: connect, introspect, create schema, write, read.
#[tokio::test]
async fn cqlsh_full_workflow() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Phase 1: Introspection (what cqlsh does on startup)
    for cql in &[
        "SELECT * FROM system.local",
        "SELECT * FROM system_schema.keyspaces",
        "SELECT * FROM system_schema.tables",
        "SELECT * FROM system_schema.columns",
    ] {
        let query = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &query).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // Phase 2: DDL
    let ddl_queries = [
        "CREATE KEYSPACE smoke_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE smoke_ks",
        "CREATE TABLE users (id int PRIMARY KEY, name text, email text)",
        "CREATE TABLE events (user_id int, ts timestamp, data text, PRIMARY KEY (user_id, ts))",
    ];
    for cql in &ddl_queries {
        let query = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &query).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // Phase 3: DML writes
    let insert_queries = [
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
        "INSERT INTO events (user_id, ts, data) VALUES (1, 1000, 'login')",
        "INSERT INTO events (user_id, ts, data) VALUES (1, 2000, 'logout')",
    ];
    for cql in &insert_queries {
        let query = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &query).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // Phase 4: DML reads
    let select_queries = [
        "SELECT * FROM users WHERE id = 1",
        "SELECT * FROM users WHERE id = 2",
        "SELECT * FROM events WHERE user_id = 1",
    ];
    for cql in &select_queries {
        let query = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &query).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
        let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
        assert_eq!(kind, 0x0002, "{cql} should return Rows");
        // Verify at least 1 row returned (row count at varying offset, but body should be non-trivial)
        assert!(resp.body.len() > 20, "{cql} should return non-empty rows");
    }

    // Phase 5: UPDATE and DELETE
    let mutate_queries = [
        "UPDATE users SET name = 'Alice Updated' WHERE id = 1",
        "DELETE FROM events WHERE user_id = 1 AND ts = 1000",
    ];
    for cql in &mutate_queries {
        let query = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &query).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // Phase 6: Verify update took effect
    let query = encode_query_body("SELECT * FROM users WHERE id = 1");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    // Should contain "Alice Updated" in the response
    let body_str = String::from_utf8_lossy(&resp.body);
    assert!(
        body_str.contains("Alice Updated"),
        "updated name should appear in SELECT result"
    );
}

/// M7 / T5: QUERY before STARTUP must be rejected with ERROR(Protocol).
/// Regression test for Critical-rated auth bypass threat (T5, risk 9).
#[tokio::test]
async fn query_before_startup_returns_protocol_error() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send QUERY directly without STARTUP — should be rejected
    let query = b"SELECT * FROM system.local";
    let mut body = Vec::new();
    body.extend_from_slice(&(query.len() as i32).to_be_bytes());
    body.extend_from_slice(query);
    body.extend_from_slice(&1u16.to_be_bytes()); // consistency ONE
    body.push(0); // flags
    send_raw_frame(&mut stream, Opcode::Query, &body).await;

    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::Error,
        "must get ERROR for query before STARTUP"
    );
    let error_code = i32::from_be_bytes(resp.body[..4].try_into().unwrap());
    assert_eq!(error_code, 0x000A, "must be Protocol error (0x000A)");
}

#[tokio::test]
async fn stream_id_preserved() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP with stream_id = 42
    let mut body = BytesMut::new();
    body.put_u16(1);
    let key = b"CQL_VERSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    let val = b"3.0.0";
    body.put_u16(val.len() as u16);
    body.put_slice(val);

    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 42,
        opcode: Opcode::Startup,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    stream.write_all(&buf).await.unwrap();

    // Read response and verify stream_id is preserved
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.header.stream_id, 42,
        "response stream_id must match request"
    );
}
