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
use ferrosa_cql::topology::ClientTopologyPolicy;
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
        archive: None,
    };
    let compaction = CompactionConfig::from_env(dir.path().join("compaction"));
    let engine_config = StorageEngineConfig {
        commit_log,
        compaction,
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
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
        internal_rpc_address: "127.0.0.1".parse().unwrap(),
        internal_rpc_port: 9042,
        tokens: vec![],
    });
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
        connection_tracker: Arc::new(ConnectionTracker::new()),
        query_tracker: Arc::new(QueryTracker::new()),
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
        version: 0x04, // v4 — raw TCP tests don't implement v5 framing
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
        version: 0x04, // v4 — raw TCP tests don't implement v5 framing
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
        version: 0x04,
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

// ── v5 framing helpers ──────────────────────────────────────────────────

fn encode_startup_frame_v5() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u16(1);
    let key = b"CQL_VERSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    let val = b"3.0.0";
    body.put_u16(val.len() as u16);
    body.put_slice(val);

    let header = FrameHeader {
        version: 0x05, // v5
        flags: 0x10,   // USE_BETA: opt into v5 framing
        stream_id: 0,
        opcode: Opcode::Startup,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

/// Read a v5 framed response: 6-byte LE header + CRC24 + payload + CRC32.
/// Extracts the 9-byte envelope from inside the frame.
///
/// Retained for the future v5 modern-framing work; ferrosa currently caps at
/// v4 so no test exchanges v5 frames.
#[allow(dead_code)]
async fn read_v5_frame(stream: &mut TcpStream) -> RawFrame {
    // Read 6-byte frame header.
    let mut frame_hdr = [0u8; 6];
    timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut frame_hdr))
        .await
        .expect("timed out waiting for v5 frame header")
        .unwrap();

    let h = u32::from_le_bytes([frame_hdr[0], frame_hdr[1], frame_hdr[2], 0]);
    let payload_len = (h & 0x1FFFF) as usize;

    // Read payload + 4-byte CRC32.
    let mut payload_and_crc = vec![0u8; payload_len + 4];
    timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut payload_and_crc))
        .await
        .expect("timed out waiting for v5 frame payload")
        .unwrap();

    // Extract the 9-byte envelope from the payload.
    let envelope = &payload_and_crc[..payload_len];
    assert!(
        envelope.len() >= HEADER_SIZE,
        "v5 frame payload too short for envelope"
    );
    let header = FrameHeader::decode(&envelope[..HEADER_SIZE]).unwrap();
    let body = envelope[HEADER_SIZE..].to_vec();
    let opcode = header.opcode;
    RawFrame {
        header,
        opcode,
        body,
    }
}

/// Send a v5 framed message.
///
/// Retained for the future v5 modern-framing work; ferrosa currently caps at
/// v4 so no test exchanges v5 frames.
#[allow(dead_code)]
async fn send_v5_frame(stream: &mut TcpStream, opcode: Opcode, body: &[u8]) {
    let header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 0,
        opcode,
        length: body.len() as u32,
    };
    let mut envelope = BytesMut::new();
    header.encode(&mut envelope);
    envelope.extend_from_slice(body);

    let payload_len = envelope.len();

    // Build 3-byte LE header: payload_length(17 bits) | isSelfContained(1 bit)
    let header_bits: u32 = (payload_len as u32) | (1 << 17);
    let h_bytes = header_bits.to_le_bytes();

    // CRC24 of header bytes.
    let crc24 = ferrosa_cql::frame::crc24_public(&h_bytes[..3]);
    let crc24_bytes = crc24.to_le_bytes();

    // CRC32 of payload.
    let crc32 = ferrosa_cql::frame::crc32_public(&envelope);
    let crc32_bytes = crc32.to_le_bytes();

    let mut buf = BytesMut::new();
    buf.put_slice(&h_bytes[..3]);
    buf.put_slice(&crc24_bytes[..3]);
    buf.put_slice(&envelope);
    buf.put_slice(&crc32_bytes[..4]);

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
        version: 0x04, // v4 — raw TCP tests don't implement v5 framing
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

/// Same shape as `setup_state` but calls `seed_default_roles` so the
/// public documented credentials (`ferrosa_admin` / `ferrosa_user`) are
/// available. Mirrors how a real auth-enabled cluster boots.
fn setup_state_with_seeded_roles() -> (Arc<SharedState>, TempDir) {
    let (state, dir) = setup_state();
    ferrosa_schema::auth::bootstrap::seed_default_roles(&state.schema).unwrap();
    (state, dir)
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

/// Bug: auth-enabled cluster times out for cdrs-tokio with
/// `ferrosa_admin / ferrosa_admin` credentials. This is the documented
/// public credential pair (see seed_default_roles) that `ferrosa-memory`
/// uses. A full v4 STARTUP → AUTHENTICATE → AUTH_RESPONSE → AUTH_SUCCESS
/// must complete; failure here indicates a wire-level server bug.
///
/// See `specs/in-process/bug-cql-auth-enabled-cluster-times-out-for-cdrs-clients.md`.
#[tokio::test]
async fn seeded_ferrosa_admin_can_authenticate_over_v4_tcp() {
    let (state, _dir) = setup_state_with_seeded_roles();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // STARTUP
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();

    // AUTHENTICATE
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    // AUTH_RESPONSE with the seeded admin credentials
    let auth = encode_auth_response("ferrosa_admin", "ferrosa_admin");
    stream.write_all(&auth).await.unwrap();

    // AUTH_SUCCESS — if this times out or returns ERROR, the bug is real.
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::AuthSuccess,
        "ferrosa_admin must authenticate — got opcode {:?} body {:?}",
        resp.opcode,
        resp.body
    );
}

/// Reproduce the exact post-AUTH_SUCCESS sequence that cdrs-tokio's
/// session builder runs: introspect system.local + system.peers +
/// system_schema.* under authenticated context. If any of these hangs
/// or returns a malformed frame, cdrs-tokio's transport logs
/// "failed to fill whole buffer" and the 10s session-build timer
/// fires. See specs/in-process/bug-cql-auth-enabled-cluster-times-
/// out-for-cdrs-clients.md.
#[tokio::test]
async fn auth_enabled_post_auth_introspection_does_not_hang() {
    let (state, _dir) = setup_state_with_seeded_roles();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // STARTUP → AUTHENTICATE → AUTH_RESPONSE → AUTH_SUCCESS
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let auth = encode_auth_response("ferrosa_admin", "ferrosa_admin");
    stream.write_all(&auth).await.unwrap();
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::AuthSuccess);

    // The same introspection sequence cqlsh + cdrs-tokio session build run.
    let introspection = [
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

    for cql in &introspection {
        let body = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &body).await;
        let resp = read_frame(&mut stream).await;
        assert_eq!(
            resp.opcode,
            Opcode::Result,
            "{cql} under authenticated context must return RESULT — got opcode={:?} body_len={}",
            resp.opcode,
            resp.body.len()
        );
        // Result kind: Rows = 0x0002. If we got Error or partial bytes the body
        // would be too short or kind would be different.
        assert!(
            resp.body.len() >= 4,
            "{cql} result body must be at least 4 bytes (kind tag) — got {}",
            resp.body.len()
        );
        let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
        assert_eq!(
            kind, 0x0002,
            "{cql} result kind must be Rows (0x0002) — got {kind:#x}"
        );
    }
}

/// cdrs-tokio's `cluster_metadata_manager` runs the EXACT queries below
/// during session bootstrap (see cluster_metadata_manager.rs:497, 665, 729,
/// 757). If any of these returns a malformed RESULT body, cdrs-tokio's
/// row decoder fails and the session-build retries forever (manifesting
/// as `IO error: failed to fill whole buffer` on the wire side).
///
/// See specs/in-process/bug-cql-auth-enabled-cluster-times-out-for-cdrs-clients.md.
#[tokio::test]
async fn cdrs_tokio_session_bootstrap_queries_return_well_formed_results() {
    let (state, _dir) = setup_state_with_seeded_roles();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // STARTUP → AUTHENTICATE → AUTH_RESPONSE → AUTH_SUCCESS
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);
    let auth = encode_auth_response("ferrosa_admin", "ferrosa_admin");
    stream.write_all(&auth).await.unwrap();
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::AuthSuccess);

    // The four EXACT queries cdrs-tokio's cluster_metadata_manager fires.
    let session_bootstrap_queries = [
        "SELECT * FROM system.local",
        "SELECT * FROM system.peers",
        "SELECT * FROM system.peers_v2",
        // toJson() on the system_schema.keyspaces.replication map column.
        // cdrs-tokio expects a single Varchar column called "replication"
        // holding a JSON object; any other shape and its row decoder fails.
        "SELECT keyspace_name, toJson(replication) AS replication FROM system_schema.keyspaces",
    ];

    for cql in &session_bootstrap_queries {
        let body = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &body).await;
        let resp = read_frame(&mut stream).await;
        assert_eq!(
            resp.opcode,
            Opcode::Result,
            "{cql} must return RESULT — got opcode={:?}",
            resp.opcode
        );
        assert!(
            resp.body.len() >= 4,
            "{cql} body too short ({} bytes)",
            resp.body.len()
        );
        let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
        assert_eq!(
            kind, 0x0002,
            "{cql} kind must be Rows (0x0002), got {kind:#x}"
        );
    }
}

/// Reproduce cdrs-tokio's actual handshake: OPTIONS first, THEN
/// STARTUP. If SUPPORTED or AUTHENTICATE is mis-encoded, cdrs-tokio's
/// transport logs `IO error: failed to fill whole buffer` and the
/// session never becomes ready.
#[tokio::test]
async fn cdrs_tokio_shaped_handshake_options_then_startup_then_auth() {
    let (state, _dir) = setup_state_with_seeded_roles();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 1. OPTIONS — cdrs-tokio opens a connection by asking what the
    //    server supports before STARTUP. If the SUPPORTED response is
    //    malformed, cdrs-tokio hangs here.
    let options_header = FrameHeader {
        version: 0x04,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Options,
        length: 0,
    };
    let mut options_buf = BytesMut::new();
    options_header.encode(&mut options_buf);
    stream.write_all(&options_buf).await.unwrap();

    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::Supported,
        "OPTIONS must elicit SUPPORTED — got {:?}",
        resp.opcode
    );
    // SUPPORTED body must be parseable as a string-multimap: [short n_keys]...
    assert!(
        resp.body.len() >= 2,
        "SUPPORTED body too short: {} bytes",
        resp.body.len()
    );
    let n_keys = u16::from_be_bytes([resp.body[0], resp.body[1]]);
    assert!(
        n_keys >= 1,
        "SUPPORTED must advertise at least CQL_VERSION — got {n_keys} keys"
    );

    // 2. STARTUP — still using the same stream, cdrs-tokio follows up.
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    // 3. AUTH_RESPONSE with seeded admin creds.
    let auth = encode_auth_response("ferrosa_admin", "ferrosa_admin");
    stream.write_all(&auth).await.unwrap();
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::AuthSuccess,
        "AUTH_SUCCESS expected — got {:?} body={:?}",
        resp.opcode,
        resp.body
    );
}

/// cdrs-tokio requests LZ4 compression in STARTUP by default. After
/// AUTH_SUCCESS the server flips the codec to compressed mode. Any
/// mismatch in when compression is enabled will manifest as "failed to
/// fill whole buffer" on the client side.
#[tokio::test]
async fn cdrs_tokio_startup_with_lz4_compression_completes_handshake() {
    let (state, _dir) = setup_state_with_seeded_roles();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // STARTUP with COMPRESSION=lz4 in the string-map body.
    let mut body = BytesMut::new();
    body.put_u16(2); // n_keys
    let k1 = b"CQL_VERSION";
    body.put_u16(k1.len() as u16);
    body.put_slice(k1);
    let v1 = b"3.0.0";
    body.put_u16(v1.len() as u16);
    body.put_slice(v1);
    let k2 = b"COMPRESSION";
    body.put_u16(k2.len() as u16);
    body.put_slice(k2);
    let v2 = b"lz4";
    body.put_u16(v2.len() as u16);
    body.put_slice(v2);

    let header = FrameHeader {
        version: 0x04,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Startup,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    stream.write_all(&buf).await.unwrap();

    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let auth = encode_auth_response("ferrosa_admin", "ferrosa_admin");
    stream.write_all(&auth).await.unwrap();

    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::AuthSuccess,
        "AUTH_SUCCESS expected after compressed STARTUP — got {:?}",
        resp.opcode
    );
    // AUTH_SUCCESS must be sent UNCOMPRESSED (compression is enabled
    // on the server *after* sending AUTH_SUCCESS per CQL spec). If the
    // server compressed it, cdrs-tokio would fail to decompress.
    assert_eq!(
        resp.header.flags & 0x01,
        0,
        "AUTH_SUCCESS must have COMPRESSION flag clear — got flags={:#x}",
        resp.header.flags
    );
}

/// Parity check: the other seeded role (`ferrosa_user`) must also
/// complete the handshake, not just the admin. This pins the fix for
/// bug-seeded-ferrosa-user-cannot-authenticate-to-graph-http.md at the
/// CQL layer as well.
#[tokio::test]
async fn seeded_ferrosa_user_can_authenticate_over_v4_tcp() {
    let (state, _dir) = setup_state_with_seeded_roles();
    let server = CqlServer::new(test_config(false), state);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();

    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let auth = encode_auth_response("ferrosa_user", "ferrosa_user");
    stream.write_all(&auth).await.unwrap();

    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::AuthSuccess,
        "ferrosa_user must authenticate — got opcode {:?} body {:?}",
        resp.opcode,
        resp.body
    );
}

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

/// Regression: partition-level DELETE on a clustered table must succeed
/// end-to-end (memtable accept → SELECT after).
///
/// Reproduces examples/cql-comprehensive/queries.cql:90 — `DELETE FROM
/// delete_test WHERE pk = 2;` followed by `SELECT * FROM delete_test
/// WHERE pk = 2;`. Pre-fix, the strict clustering-shape guard added in
/// the timeuuid flush-wedge fix rejected the row that the bridge builds
/// for the partition delete (empty clustering on a clustered table) at
/// memtable boundary, surfacing as `NoHostAvailable: server error:
/// storage error: invalid data: ks.delete_test (clustering): clustering
/// bytes are empty but schema declares 1 clustering column(s)` on the
/// next SELECT.
#[tokio::test]
async fn partition_delete_on_clustered_table_does_not_wedge_validator() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    let setup = [
        "CREATE KEYSPACE part_del_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE part_del_ks",
        "CREATE TABLE delete_test (pk int, ck int, v text, PRIMARY KEY (pk, ck))",
        "INSERT INTO delete_test (pk, ck, v) VALUES (1, 1, 'a')",
        "INSERT INTO delete_test (pk, ck, v) VALUES (1, 2, 'b')",
        "INSERT INTO delete_test (pk, ck, v) VALUES (2, 1, 'c')",
        "INSERT INTO delete_test (pk, ck, v) VALUES (2, 2, 'd')",
        // Row-level delete (control case — already worked pre-fix).
        "DELETE FROM delete_test WHERE pk = 1 AND ck = 2",
        // Partition-level delete — the regression case.
        "DELETE FROM delete_test WHERE pk = 2",
    ];
    for cql in &setup {
        let query = encode_query_body(cql);
        send_raw_frame(&mut stream, Opcode::Query, &query).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // The follow-up SELECT must succeed — pre-fix this returned a server
    // error because the partition-tombstone Row got rejected on flush.
    let query = encode_query_body("SELECT * FROM delete_test WHERE pk = 2");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(
        kind, 0x0002,
        "SELECT after partition delete should return Rows"
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
        version: 0x04, // v4 — raw TCP tests don't implement v5 framing
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

// ── SUBSCRIBE tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn subscribe_every_receives_streaming_frames() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace and table
    let body = encode_query_body(
        "CREATE KEYSPACE sub_test WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 1).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Result);

    let body = encode_query_body("CREATE TABLE sub_test.data (id text PRIMARY KEY, value text)");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 2).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Result);

    // Insert some data
    let body = encode_query_body("INSERT INTO sub_test.data (id, value) VALUES ('k1', 'v1')");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 3).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Result);

    // Subscribe with 500ms polling interval (minimum allowed)
    let body = encode_query_body("SUBSCRIBE SELECT * FROM sub_test.data EVERY 500ms");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 10).await;

    // First response: void ACK
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Result, "should get void ACK");
    assert_eq!(resp.header.stream_id, 10);

    // Second frame: first polling result (streaming flag set)
    let resp = timeout(Duration::from_secs(3), read_frame(&mut stream))
        .await
        .expect("should receive streaming frame within 3s");
    assert_eq!(resp.opcode, Opcode::Result);
    assert_eq!(resp.header.stream_id, 10);
    assert_ne!(
        resp.header.flags & STREAMING_FLAG,
        0,
        "streaming flag must be set"
    );
    // Body should contain rows (non-empty)
    assert!(
        !resp.body.is_empty(),
        "streaming frame should have row data"
    );

    // Send UNSUBSCRIBE to cancel
    let body = encode_query_body("UNSUBSCRIBE");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 11).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Result);
}

#[tokio::test]
async fn subscribe_without_every_returns_error() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace and table
    let body = encode_query_body(
        "CREATE KEYSPACE sub_err WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 1).await;
    read_frame(&mut stream).await;

    let body = encode_query_body("CREATE TABLE sub_err.t (id text PRIMARY KEY)");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 2).await;
    read_frame(&mut stream).await;

    // SUBSCRIBE without EVERY — should return error
    let body = encode_query_body("SUBSCRIBE SELECT * FROM sub_err.t");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 3).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::Error,
        "subscribe without EVERY should fail"
    );
}

#[tokio::test]
async fn unsubscribe_returns_void_result() {
    // UNSUBSCRIBE without active subscriptions should still succeed.
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    let body = encode_query_body("UNSUBSCRIBE");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 1).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Result);
}

#[tokio::test]
async fn subscribe_max_subscriptions_enforced() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create table
    let body = encode_query_body(
        "CREATE KEYSPACE sub_max WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 1).await;
    read_frame(&mut stream).await;

    let body = encode_query_body("CREATE TABLE sub_max.t (id text PRIMARY KEY)");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 2).await;
    read_frame(&mut stream).await;

    // Create 8 subscriptions (the max) — use long polling interval to avoid
    // interleaved streaming frames confusing the ACK reads.
    for i in 0..8u16 {
        let body = encode_query_body("SUBSCRIBE SELECT * FROM sub_max.t EVERY 60s");
        send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 100 + i as i16).await;
        let resp = read_frame(&mut stream).await;
        assert_eq!(
            resp.opcode,
            Opcode::Result,
            "subscription {i} should succeed"
        );
    }

    // 9th subscription should fail
    let body = encode_query_body("SUBSCRIBE SELECT * FROM sub_max.t EVERY 60s");
    send_raw_frame_with_stream(&mut stream, Opcode::Query, &body, 200).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::Error,
        "9th subscription should be rejected"
    );
}

// ── UDT integration tests ────────────────────────────────────────────────

#[tokio::test]
async fn create_type_and_use_in_table() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace
    let query = encode_query_body(
        "CREATE KEYSPACE udt_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // USE keyspace
    let query = encode_query_body("USE udt_ks");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // CREATE TYPE
    let query = encode_query_body("CREATE TYPE address (street text, city text, zip int)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    // Verify it's a SchemaChange result (kind = 0x0005)
    let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0005, "CREATE TYPE should return SchemaChange");

    // CREATE TABLE with frozen<address> column
    let query = encode_query_body("CREATE TABLE users (id int PRIMARY KEY, home frozen<address>)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // INSERT with UDT literal (field names as quoted strings)
    let query = encode_query_body(
        "INSERT INTO users (id, home) VALUES (1, {'street': '123 Main', 'city': 'Springfield', 'zip': 62701})",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // SELECT and verify the result
    let query = encode_query_body("SELECT * FROM users WHERE id = 1");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0002, "SELECT should return Rows");
    // Verify row data contains our UDT values
    let body_str = String::from_utf8_lossy(&resp.body);
    assert!(
        body_str.contains("123 Main"),
        "SELECT result should contain UDT street value"
    );
    assert!(
        body_str.contains("Springfield"),
        "SELECT result should contain UDT city value"
    );
}

#[tokio::test]
async fn create_type_if_not_exists() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace
    let query = encode_query_body(
        "CREATE KEYSPACE udt_ine_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("USE udt_ine_ks");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Create type first time
    let query = encode_query_body("CREATE TYPE address (street text, city text)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Create same type again with IF NOT EXISTS — should succeed without error
    let query = encode_query_body("CREATE TYPE IF NOT EXISTS address (street text, city text)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
}

#[tokio::test]
async fn drop_type_if_exists() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace
    let query = encode_query_body(
        "CREATE KEYSPACE udt_die_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("USE udt_die_ks");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // DROP TYPE IF EXISTS on nonexistent type — should succeed without error
    let query = encode_query_body("DROP TYPE IF EXISTS nonexistent_type");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Create type, then drop it, then drop again with IF EXISTS
    let query = encode_query_body("CREATE TYPE temp_type (x text)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("DROP TYPE temp_type");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Drop again with IF EXISTS — should succeed
    let query = encode_query_body("DROP TYPE IF EXISTS temp_type");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
}

#[tokio::test]
async fn alter_type_add_field() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace
    let query = encode_query_body(
        "CREATE KEYSPACE udt_alter_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("USE udt_alter_ks");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Create type with one field
    let query = encode_query_body("CREATE TYPE address (street text)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // ALTER TYPE ADD city text
    let query = encode_query_body("ALTER TYPE address ADD city text");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Verify via system_schema.types that the new field appears
    let query = encode_query_body("SELECT * FROM system_schema.types");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let body_str = String::from_utf8_lossy(&resp.body);
    assert!(
        body_str.contains("address"),
        "system_schema.types should contain the altered type"
    );
    assert!(
        body_str.contains("city"),
        "system_schema.types should show the added field 'city'"
    );
    assert!(
        body_str.contains("street"),
        "system_schema.types should still show the original field 'street'"
    );
}

#[tokio::test]
async fn system_schema_types_queryable() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Query system_schema.types before any types exist — should return empty Rows
    let query = encode_query_body("SELECT * FROM system_schema.types");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0002, "should return Rows");

    // Create keyspace and type
    let query = encode_query_body(
        "CREATE KEYSPACE udt_sys_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("USE udt_sys_ks");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let query = encode_query_body("CREATE TYPE contact (name text, phone text, email text)");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Query system_schema.types — should now contain our type
    let query = encode_query_body("SELECT * FROM system_schema.types");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0002, "should return Rows");

    let body_str = String::from_utf8_lossy(&resp.body);
    assert!(
        body_str.contains("udt_sys_ks"),
        "system_schema.types should contain our keyspace"
    );
    assert!(
        body_str.contains("contact"),
        "system_schema.types should contain our type name"
    );
    assert!(
        body_str.contains("name"),
        "system_schema.types should list field 'name'"
    );
    assert!(
        body_str.contains("phone"),
        "system_schema.types should list field 'phone'"
    );
    assert!(
        body_str.contains("email"),
        "system_schema.types should list field 'email'"
    );
}

// ── QUERY frame with bind values (filtering bug) ────────────────────────
//
// Regression test for ferrosa-memory bug: QUERY frames with bind values
// (query_with_values) must correctly substitute bind markers and return
// matching rows. This tests the exact code path that debug_dynamic_query_
// with_bind_values and debug_query_bind_values_vs_inline exercise.

/// Encode a CQL v4 QUERY frame body WITH bind values.
///
/// Frame format:
///   [int query_len][bytes query][short consistency][byte flags][short n][value*]
/// Each value: [int len][bytes data]
fn encode_query_body_with_values(query: &str, values: &[&[u8]]) -> Vec<u8> {
    let query_bytes = query.as_bytes();
    let mut body = Vec::new();
    // [int] query string length + bytes
    body.extend_from_slice(&(query_bytes.len() as i32).to_be_bytes());
    body.extend_from_slice(query_bytes);
    // [short] consistency = ONE
    body.extend_from_slice(&1u16.to_be_bytes());
    // [byte] flags: bit 0 = values present
    body.push(0x01);
    // [short] number of values
    body.extend_from_slice(&(values.len() as u16).to_be_bytes());
    // Each value: [int len][bytes]
    for val in values {
        body.extend_from_slice(&(val.len() as i32).to_be_bytes());
        body.extend_from_slice(val);
    }
    body
}

/// Encode a CQL v4 EXECUTE frame body with positional bind values.
fn encode_execute_body_with_values(prepared_id: &[u8; 16], values: &[&[u8]]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&16u16.to_be_bytes()); // prepared id len
    body.extend_from_slice(prepared_id);
    body.extend_from_slice(&1u16.to_be_bytes()); // consistency = ONE
    body.push(0x01); // values present
    body.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for val in values {
        body.extend_from_slice(&(val.len() as i32).to_be_bytes());
        body.extend_from_slice(val);
    }
    body
}

/// Extract row count from a CQL RESULT Rows frame body.
fn extract_row_count_from_result(body: &[u8]) -> i32 {
    assert!(body.len() >= 4, "result body too short");
    let kind = i32::from_be_bytes(body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0002, "expected Rows result kind, got {kind:#06x}");
    // flags at offset 4..8
    let _flags = i32::from_be_bytes(body[4..8].try_into().unwrap());
    let col_count = i32::from_be_bytes(body[8..12].try_into().unwrap()) as usize;
    let mut off = 12;
    // Skip keyspace string
    let ks_len = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
    off += 2 + ks_len;
    // Skip table string
    let tbl_len = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
    off += 2 + tbl_len;
    // Skip column specs
    for _ in 0..col_count {
        let name_len = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + name_len;
        let type_id = u16::from_be_bytes(body[off..off + 2].try_into().unwrap());
        off += 2;
        match type_id {
            0x0020 | 0x0022 => off += 2, // List/Set: element type_id
            0x0021 => off += 4,          // Map: key + val type_ids
            0x0031 => {
                let n = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                off += 2 + n * 2;
            }
            _ => {}
        }
    }
    // row_count
    i32::from_be_bytes(body[off..off + 4].try_into().unwrap())
}

/// Return the raw column bytes for the first row in a Rows result.
fn extract_first_row_values_from_result(body: &[u8]) -> Vec<Option<Vec<u8>>> {
    assert!(body.len() >= 4, "result body too short");
    let kind = i32::from_be_bytes(body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0002, "expected Rows result kind, got {kind:#06x}");

    let col_count = i32::from_be_bytes(body[8..12].try_into().unwrap()) as usize;
    let mut off = 12;

    let ks_len = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
    off += 2 + ks_len;
    let tbl_len = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
    off += 2 + tbl_len;

    for _ in 0..col_count {
        let name_len = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + name_len;
        let type_id = u16::from_be_bytes(body[off..off + 2].try_into().unwrap());
        off += 2;
        match type_id {
            0x0000 => {
                let custom_len =
                    u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                off += 2 + custom_len;
            }
            0x0020 | 0x0022 => off += 2, // List/Set element type
            0x0021 => off += 4,          // Map key/value type
            0x0030 => {
                let ks_len = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                off += 2 + ks_len;
                let type_name_len =
                    u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                off += 2 + type_name_len;
                let field_count =
                    u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                off += 2;
                for _ in 0..field_count {
                    let field_name_len =
                        u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                    off += 2 + field_name_len;
                    let nested_type_id = u16::from_be_bytes(body[off..off + 2].try_into().unwrap());
                    off += 2;
                    match nested_type_id {
                        0x0000 => {
                            let nested_len =
                                u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                            off += 2 + nested_len;
                        }
                        0x0020 | 0x0022 => off += 2,
                        0x0021 => off += 4,
                        _ => {}
                    }
                }
            }
            0x0031 => {
                let n = u16::from_be_bytes(body[off..off + 2].try_into().unwrap()) as usize;
                off += 2 + n * 2;
            }
            _ => {}
        }
    }

    let row_count = i32::from_be_bytes(body[off..off + 4].try_into().unwrap()) as usize;
    off += 4;
    assert!(row_count >= 1, "expected at least one row");

    let mut values = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let len = i32::from_be_bytes(body[off..off + 4].try_into().unwrap());
        off += 4;
        if len < 0 {
            values.push(None);
        } else {
            let len = len as usize;
            values.push(Some(body[off..off + len].to_vec()));
            off += len;
        }
    }
    values
}

#[tokio::test]
async fn query_with_bind_values_returns_matching_rows() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace + table
    let body = encode_query_body(
        "CREATE KEYSPACE bv WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let body = encode_query_body(
        "CREATE TABLE bv.entities (\
         tenant_id uuid, session_id uuid, entity_id uuid, \
         entity_name text, entity_type text, \
         PRIMARY KEY ((tenant_id, session_id), entity_id))",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let tid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let sid = "11111111-2222-3333-4444-555555555555";

    // Insert 2 rows with inline values
    for (eid, name, etype) in [
        ("00000001-0000-0000-0000-000000000001", "Alice", "person"),
        ("00000002-0000-0000-0000-000000000002", "Rust", "concept"),
    ] {
        let body = encode_query_body(&format!(
            "INSERT INTO bv.entities \
             (tenant_id, session_id, entity_id, entity_name, entity_type) \
             VALUES ({tid}, {sid}, {eid}, '{name}', '{etype}')"
        ));
        send_raw_frame(&mut stream, Opcode::Query, &body).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // Query 1: inline values (baseline) — should return 2 rows
    let body = encode_query_body(&format!(
        "SELECT * FROM bv.entities \
         WHERE tenant_id = {tid} AND session_id = {sid}"
    ));
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let inline_count = extract_row_count_from_result(&resp.body);
    assert_eq!(inline_count, 2, "inline query should return 2 rows");

    // Query 2: bind values via QUERY frame — must also return 2 rows
    let tid_uuid = uuid::Uuid::parse_str(tid).unwrap();
    let sid_uuid = uuid::Uuid::parse_str(sid).unwrap();
    let body = encode_query_body_with_values(
        "SELECT * FROM bv.entities WHERE tenant_id = ? AND session_id = ?",
        &[tid_uuid.as_bytes(), sid_uuid.as_bytes()],
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let bind_count = extract_row_count_from_result(&resp.body);
    assert_eq!(
        bind_count, 2,
        "QUERY with bind values should return 2 rows (got {bind_count}); \
         bind values in QUERY frame may not be substituted correctly"
    );
}

/// Encode a CQL v4 QUERY frame body with bind values AND page_size
/// (flags = 0x05 = VALUES | PAGE_SIZE), mimicking cdrs-tokio's default behavior.
fn encode_query_body_with_values_and_page_size(
    query: &str,
    values: &[&[u8]],
    page_size: i32,
) -> Vec<u8> {
    let query_bytes = query.as_bytes();
    let mut body = Vec::new();
    body.extend_from_slice(&(query_bytes.len() as i32).to_be_bytes());
    body.extend_from_slice(query_bytes);
    // [short] consistency = ONE
    body.extend_from_slice(&1u16.to_be_bytes());
    // [byte] flags: bit 0x01 = values, bit 0x04 = page_size
    body.push(0x05);
    // [short] number of values
    body.extend_from_slice(&(values.len() as u16).to_be_bytes());
    // Each value: [int len][bytes]
    for val in values {
        body.extend_from_slice(&(val.len() as i32).to_be_bytes());
        body.extend_from_slice(val);
    }
    // [int] page_size
    body.extend_from_slice(&page_size.to_be_bytes());
    body
}

#[tokio::test]
async fn query_with_bind_values_and_page_size_flag() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace + table
    let body = encode_query_body(
        "CREATE KEYSPACE bvp WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let body = encode_query_body(
        "CREATE TABLE bvp.items (\
         tenant_id uuid, session_id uuid, entity_id uuid, \
         entity_name text, \
         PRIMARY KEY ((tenant_id, session_id), entity_id))",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let tid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let sid = "11111111-2222-3333-4444-555555555555";

    // Insert 3 rows
    for (eid, name) in [
        ("00000001-0000-0000-0000-000000000001", "Alice"),
        ("00000002-0000-0000-0000-000000000002", "Bob"),
        ("00000003-0000-0000-0000-000000000003", "Carol"),
    ] {
        let body = encode_query_body(&format!(
            "INSERT INTO bvp.items \
             (tenant_id, session_id, entity_id, entity_name) \
             VALUES ({tid}, {sid}, {eid}, '{name}')"
        ));
        send_raw_frame(&mut stream, Opcode::Query, &body).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // Send QUERY with bind values AND page_size flag (0x05), like cdrs-tokio does
    let tid_uuid = uuid::Uuid::parse_str(tid).unwrap();
    let sid_uuid = uuid::Uuid::parse_str(sid).unwrap();
    let body = encode_query_body_with_values_and_page_size(
        "SELECT * FROM bvp.items WHERE tenant_id = ? AND session_id = ?",
        &[tid_uuid.as_bytes(), sid_uuid.as_bytes()],
        5000, // page_size
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let count = extract_row_count_from_result(&resp.body);
    assert_eq!(
        count, 3,
        "QUERY with bind values + page_size flag should return 3 rows, \
         got {count} — page_size flag may be corrupting bind value parsing"
    );
}

/// Exact ferrosa-memory entity_store scenario: composite UUID PK,
/// bind values in QUERY frame, specific column selection.
#[tokio::test]
async fn query_with_bind_values_entity_store_scenario() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace
    let body = encode_query_body(
        "CREATE KEYSPACE agent_memory WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Create entity_store with same schema as ferrosa-memory DDL
    let body = encode_query_body(
        "CREATE TABLE agent_memory.entity_store (\
         tenant_id uuid, entity_id uuid, session_id uuid, \
         entity_name text, entity_type text, source_fold_id uuid, \
         context_snippet text, confidence float, created_at timestamp, \
         PRIMARY KEY ((tenant_id, session_id), entity_id))",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let tid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let sid = "11111111-2222-3333-4444-555555555555";
    let eid = "66666666-7777-8888-9999-aaaaaaaaaaaa";

    // Insert via inline (known working path)
    let body = encode_query_body(&format!(
        "INSERT INTO agent_memory.entity_store \
         (tenant_id, session_id, entity_id, entity_name, entity_type, \
          context_snippet, confidence, created_at) \
         VALUES ({tid}, {sid}, {eid}, 'test-entity', 'concept', \
                 'test context', 1.0, 1711036800000)"
    ));
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Query 1: inline values (baseline)
    let body = encode_query_body(&format!(
        "SELECT entity_id, entity_name, entity_type, source_fold_id, \
         context_snippet, confidence, created_at \
         FROM agent_memory.entity_store \
         WHERE tenant_id = {tid} AND session_id = {sid}"
    ));
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let inline_count = extract_row_count_from_result(&resp.body);
    assert_eq!(inline_count, 1, "inline query should return 1 row");

    // Query 2: bind values (the path ferrosa-memory uses)
    let tid_uuid = uuid::Uuid::parse_str(tid).unwrap();
    let sid_uuid = uuid::Uuid::parse_str(sid).unwrap();
    let body = encode_query_body_with_values(
        "SELECT entity_id, entity_name, entity_type, source_fold_id, \
         context_snippet, confidence, created_at \
         FROM agent_memory.entity_store \
         WHERE tenant_id = ? AND session_id = ?",
        &[tid_uuid.as_bytes(), sid_uuid.as_bytes()],
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let bind_count = extract_row_count_from_result(&resp.body);
    assert_eq!(
        bind_count, 1,
        "bind value QUERY should return 1 row, got {bind_count}"
    );

    // Query 3: bind values + ALLOW FILTERING (exact query from ferrosa_bugs.rs)
    let body = encode_query_body_with_values(
        "SELECT entity_id, entity_name, entity_type, source_fold_id, \
         context_snippet, confidence, created_at \
         FROM agent_memory.entity_store \
         WHERE tenant_id = ? AND session_id = ? ALLOW FILTERING",
        &[tid_uuid.as_bytes(), sid_uuid.as_bytes()],
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let af_count = extract_row_count_from_result(&resp.body);
    assert_eq!(
        af_count, 1,
        "bind value QUERY with ALLOW FILTERING should return 1 row, got {af_count}"
    );

    // Query 4: bind values + page_size flag (what cdrs-tokio actually sends)
    let body = encode_query_body_with_values_and_page_size(
        "SELECT entity_id, entity_name, entity_type, source_fold_id, \
         context_snippet, confidence, created_at \
         FROM agent_memory.entity_store \
         WHERE tenant_id = ? AND session_id = ? ALLOW FILTERING",
        &[tid_uuid.as_bytes(), sid_uuid.as_bytes()],
        5000,
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let paged_count = extract_row_count_from_result(&resp.body);
    assert_eq!(
        paged_count, 1,
        "bind value QUERY with ALLOW FILTERING + page_size should return 1 row, \
         got {paged_count}"
    );
}

#[tokio::test]
async fn prepared_update_existing_entity_store_row_is_visible_to_readback() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    let body = encode_query_body(
        "CREATE KEYSPACE agent_memory WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let body = encode_query_body(
        "CREATE TABLE agent_memory.entity_store (\
         tenant_id uuid, session_id uuid, entity_id uuid, \
         entity_name text, updated_at timestamp, entity_embedding vector<float, 3>, \
         PRIMARY KEY ((tenant_id, session_id), entity_id))",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let tid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let sid = "11111111-2222-3333-4444-555555555555";
    let eid = "66666666-7777-8888-9999-aaaaaaaaaaaa";

    let body = encode_query_body(&format!(
        "INSERT INTO agent_memory.entity_store \
         (tenant_id, session_id, entity_id, entity_name, updated_at) \
         VALUES ({tid}, {sid}, {eid}, 'test-entity', 1711036800000)"
    ));
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let prep_body = encode_prepare_body(
        "UPDATE agent_memory.entity_store \
         SET updated_at = ?, entity_embedding = ? \
         WHERE tenant_id = ? AND session_id = ? AND entity_id = ?",
    );
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    assert!(resp.body.len() >= 22, "PREPARED response too short");
    let kind = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(kind, 0x0004, "expected PREPARED result kind");
    let mut prepared_id = [0u8; 16];
    prepared_id.copy_from_slice(&resp.body[6..22]);

    let updated_at = 1712036800000i64.to_be_bytes();
    let embedding = ferrosa_index::vec_f32_to_bytes(&[1.0, 2.0, 3.0]);
    let tid_uuid = uuid::Uuid::parse_str(tid).unwrap();
    let sid_uuid = uuid::Uuid::parse_str(sid).unwrap();
    let eid_uuid = uuid::Uuid::parse_str(eid).unwrap();
    let exec_body = encode_execute_body_with_values(
        &prepared_id,
        &[
            &updated_at,
            &embedding,
            tid_uuid.as_bytes(),
            sid_uuid.as_bytes(),
            eid_uuid.as_bytes(),
        ],
    );
    send_raw_frame(&mut stream, Opcode::Execute, &exec_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let body = encode_query_body(&format!(
        "SELECT updated_at, entity_embedding \
         FROM agent_memory.entity_store \
         WHERE tenant_id = {tid} AND session_id = {sid} AND entity_id = {eid}"
    ));
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let values = extract_first_row_values_from_result(&resp.body);
    assert_eq!(values.len(), 2, "expected updated_at and entity_embedding");
    assert_eq!(
        values[0].as_deref(),
        Some(updated_at.as_slice()),
        "updated_at should reflect the prepared UPDATE value"
    );
    assert_eq!(
        values[1].as_deref(),
        Some(embedding.as_slice()),
        "entity_embedding should reflect the prepared UPDATE value"
    );
}

#[tokio::test]
async fn query_with_bind_values_allow_filtering() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Create keyspace + table with PK = id, non-PK = category
    let body = encode_query_body(
        "CREATE KEYSPACE bvf WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let body =
        encode_query_body("CREATE TABLE bvf.items (id int PRIMARY KEY, category text, score int)");
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    // Insert 4 rows: 2 tech, 2 art
    for (id, cat, score) in [
        (1, "tech", 10),
        (2, "art", 20),
        (3, "tech", 30),
        (4, "art", 40),
    ] {
        let body = encode_query_body(&format!(
            "INSERT INTO bvf.items (id, category, score) VALUES ({id}, '{cat}', {score})"
        ));
        send_raw_frame(&mut stream, Opcode::Query, &body).await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
    }

    // Query with bind values on non-PK column + ALLOW FILTERING
    // category = 'tech' as a CQL varchar bind value
    let category_bytes = b"tech";
    let body = encode_query_body_with_values(
        "SELECT * FROM bvf.items WHERE category = ? ALLOW FILTERING",
        &[category_bytes],
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    let filtered_count = extract_row_count_from_result(&resp.body);
    assert_eq!(
        filtered_count, 2,
        "ALLOW FILTERING with bind value category='tech' should return 2 rows, \
         got {filtered_count}"
    );
}

// ── CQL v4-cap negotiation tests ─────────────────────────────────────────
//
// ferrosa caps the native protocol at v4. v5 added a modern framing layer that
// drivers implement inconsistently (gocql sends plain legacy envelopes at v5;
// the DataStax Java driver sends CRC-checksummed modern frames), so no single
// server framing mode serves both. The server therefore rejects any v5 STARTUP
// with a protocol-version ERROR advertising `supported = 4`, and every
// well-behaved driver falls back to the one well-tested v4 transport. Proper v5
// modern framing is tracked as future work.

/// Parse the `[string]` message out of a CQL ERROR frame body
/// (`[i32 code][u16 len][utf8 bytes]`).
fn parse_error_message(body: &[u8]) -> String {
    assert!(
        body.len() >= 6,
        "ERROR body too short to hold a message: {} bytes",
        body.len()
    );
    let len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
    assert!(
        body.len() >= 6 + len,
        "ERROR body truncated: need {} bytes, have {}",
        6 + len,
        body.len()
    );
    String::from_utf8_lossy(&body[6..6 + len]).into_owned()
}

/// A v5 STARTUP must be rejected with a protocol-version ERROR (code 0x000A),
/// itself framed as v4 (0x84) so the driver can read it, advertising v4 as the
/// greatest supported version. The server must NOT reply READY at v5 — that
/// would leave a v5 driver expecting modern framing the server can't speak.
#[tokio::test]
async fn v5_startup_is_rejected_to_force_v4_fallback() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send a v5 STARTUP (version byte 0x05 + USE_BETA flag).
    stream.write_all(&encode_startup_frame_v5()).await.unwrap();

    // The server rejects v5 at decode and replies with a v4-framed ERROR.
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.opcode,
        Opcode::Error,
        "v5 STARTUP must be rejected with an ERROR, not accepted with READY"
    );
    assert_eq!(
        resp.header.version, 0x84,
        "the rejection ERROR must be framed as v4 (0x84) so the driver can read it"
    );
    let error_code = i32::from_be_bytes(resp.body[0..4].try_into().unwrap());
    assert_eq!(
        error_code, 0x000A,
        "protocol-version rejection must use error code 0x000A (protocol error)"
    );
    let msg = parse_error_message(&resp.body);
    assert!(
        msg.contains("greatest is 4"),
        "rejection must advertise v4 as the greatest supported version; got: {msg}"
    );
}

// ── P0-22 regression: PREPARE response must carry bind-marker column metadata ──
//
// The CQL native protocol PREPARE response (RESULT/Prepared body) includes a
// "bind-variable metadata" section that reports how many `?` placeholders the
// statement has and what type each one maps to.  This metadata is:
//
//   [i32 flags] [i32 col_count] [i32 pk_count] [pk_indexes...] [ks] [tbl]
//   [for each bind col: col_name_str + type_id_u16]
//
// Strict CQL drivers (scylla Rust driver, DataStax Java/C#, gocql, Python
// cassandra-driver) read `col_count` and reject execute_unpaged when the
// caller supplies a different number of values (WrongColumnCount).  The
// cdrs-tokio fork used by fmem pre-p1-22 was lenient and ignored this.
//
// These tests assert the col_count from the raw PREPARE response body so that
// any future regression is caught before it reaches a strict external driver.

/// Parse the bind-variable `col_count` from a RESULT/Prepared body.
///
/// Body layout starting at byte 0:
///   [0..4]   i32 kind = 0x0004
///   [4..6]   u16 id_len = 16
///   [6..22]  16 bytes id
///   [22..26] i32 bind_flags
///   [26..30] i32 bind_col_count  ← returned by this function
fn extract_bind_col_count_from_prepared_response(body: &[u8]) -> i32 {
    assert!(
        body.len() >= 30,
        "PREPARED response body too short to contain bind_col_count; \
         body len = {}, expected >= 30",
        body.len()
    );
    let kind = i32::from_be_bytes(body[0..4].try_into().unwrap());
    assert_eq!(
        kind, 0x0004,
        "expected Prepared kind (0x0004), got 0x{kind:04X}"
    );
    let id_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
    assert_eq!(id_len, 16, "prepared ID length must be 16");
    // bind_flags at offset 22, bind_col_count at offset 26
    i32::from_be_bytes(body[26..30].try_into().unwrap())
}

/// P0-22 regression: INSERT with 3 `?` bind markers must report col_count = 3.
///
/// This is the canonical external-driver compatibility test.  A strict driver
/// (scylla, gocql, DataStax) reads col_count from the PREPARE response and
/// rejects execute_unpaged when the Rust tuple length != col_count.  Returning
/// col_count = 0 causes:
///   WrongColumnCount { rust_cols: 3, cql_cols: 0 }
#[tokio::test]
async fn prepare_insert_bind_col_count_matches_placeholder_count() {
    let (state, _dir) = setup_state();
    let server = ferrosa_cql::server::CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // DDL: keyspace + table
    let body = encode_query_body(
        "CREATE KEYSPACE p0_22_ins WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let body = encode_query_body("CREATE TABLE p0_22_ins.t (a uuid PRIMARY KEY, b text, c int)");
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    // PREPARE an INSERT with 3 bind markers
    let prep_body = encode_prepare_body("INSERT INTO p0_22_ins.t (a, b, c) VALUES (?, ?, ?)");
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let col_count = extract_bind_col_count_from_prepared_response(&resp.body);
    assert_eq!(
        col_count, 3,
        "PREPARE INSERT with 3 bind markers must report bind col_count = 3; \
         got {col_count}. A strict CQL driver (scylla, gocql, DataStax) would \
         reject execute_unpaged with WrongColumnCount {{ rust_cols: 3, cql_cols: {col_count} }}"
    );
}

/// P0-22 regression: INSERT with 8 `?` bind markers (the fmem entity_store case).
///
/// This exercises the exact statement that failed in ferrosa-memory PR #10 CI:
///   WrongColumnCount { rust_cols: 8, cql_cols: 0 }
#[tokio::test]
async fn prepare_insert_eight_bind_markers_reports_col_count_eight() {
    let (state, _dir) = setup_state();
    let server = ferrosa_cql::server::CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    let body = encode_query_body(
        "CREATE KEYSPACE agent_memory_p0_22 WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    // Mirrors the fmem entity_store schema (minus vector column, which is not
    // part of the failing INSERT).
    let body = encode_query_body(
        "CREATE TABLE agent_memory_p0_22.entity_store (\
         tenant_id uuid, session_id uuid, entity_id uuid, \
         entity_name text, entity_type text, context_snippet text, \
         confidence float, created_at timestamp, \
         PRIMARY KEY ((tenant_id, session_id), entity_id))",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    // This is the exact PREPARE statement from ferrosa_bugs.rs:100-104
    let prep_body = encode_prepare_body(
        "INSERT INTO agent_memory_p0_22.entity_store \
         (tenant_id, session_id, entity_id, entity_name, entity_type, \
          context_snippet, confidence, created_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    );
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let col_count = extract_bind_col_count_from_prepared_response(&resp.body);
    assert_eq!(
        col_count, 8,
        "PREPARE INSERT (entity_store, 8 bind markers) must report bind col_count = 8; \
         got {col_count}. This is the fmem p0-22 failure: \
         WrongColumnCount {{ rust_cols: 8, cql_cols: {col_count} }}"
    );
}

/// P0-22 regression: SELECT with 1 WHERE bind marker must report col_count = 1.
#[tokio::test]
async fn prepare_select_with_one_where_bind_marker_reports_col_count_one() {
    let (state, _dir) = setup_state();
    let server = ferrosa_cql::server::CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    let body = encode_query_body(
        "CREATE KEYSPACE p0_22_sel WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let body = encode_query_body("CREATE TABLE p0_22_sel.t (a uuid PRIMARY KEY, b text)");
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let prep_body = encode_prepare_body("SELECT * FROM p0_22_sel.t WHERE a = ?");
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let col_count = extract_bind_col_count_from_prepared_response(&resp.body);
    assert_eq!(
        col_count, 1,
        "PREPARE SELECT with 1 WHERE bind marker must report bind col_count = 1; \
         got {col_count}"
    );
}

/// FRSA-BUG-025 regression: SELECT with WHERE bind markers plus `ANN OF ?`
/// must report all bind marker columns in PREPARE metadata. The ANN query
/// vector is bound against the vector column (`fold_embedding`) and strict
/// drivers reject the prepared statement if this third col_spec is missing.
#[tokio::test]
async fn prepare_select_ann_of_bind_marker_reports_col_count_three() {
    let (state, _dir) = setup_state();
    let server = ferrosa_cql::server::CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    let body = encode_query_body(
        "CREATE KEYSPACE ann_prepare WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let body = encode_query_body(
        "CREATE TABLE ann_prepare.trajectory_folds (\
         session_id uuid, tenant_id uuid, fold_id uuid, \
         fold_embedding vector<float, 3>, fold_summary text, \
         PRIMARY KEY ((session_id, tenant_id), fold_id))",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let prep_body = encode_prepare_body(
        "SELECT fold_id, fold_summary \
         FROM ann_prepare.trajectory_folds \
         WHERE session_id = ? AND tenant_id = ? \
         ORDER BY fold_embedding ANN OF ? LIMIT 5",
    );
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let col_count = extract_bind_col_count_from_prepared_response(&resp.body);
    assert_eq!(
        col_count, 3,
        "PREPARE SELECT with 2 WHERE bind markers plus ANN OF ? must report \
         bind col_count = 3; got {col_count}. Missing the ANN vector col_spec \
         reproduces FRSA-BUG-025 / strict-driver WrongColumnCount."
    );
}

/// P0-22 regression: PREPARE for a table that is not yet in the local schema
/// (simulates Raft schema-replication lag) must return an error, NOT a
/// PREPARED response with col_count = 0.
///
/// Background: In a multi-node ferrosa cluster the CREATE TABLE DDL goes
/// through Raft.  After the leader commits the entry, a follower node may
/// receive a PREPARE on the same connection before its state machine has
/// applied the log entry.  Before this fix, handle_prepare() returned a
/// valid-looking PREPARED result with bind col_count = 0, which caused
/// strict CQL drivers to reject every subsequent execute_unpaged call:
///   WrongColumnCount { rust_cols: N, cql_cols: 0 }
///
/// After the fix, handle_prepare() returns an ERROR (Invalid) when the
/// bound-column count from schema lookup does not match the bind-marker count
/// from the AST, so the driver retries and hits a node with current schema.
#[tokio::test]
async fn prepare_against_missing_table_returns_error_not_col_count_zero() {
    let (state, _dir) = setup_state();
    let server = ferrosa_cql::server::CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    // Intentionally do NOT create the keyspace or table — the table does not
    // exist in the schema on this node (simulates Raft schema lag).
    let prep_body =
        encode_prepare_body("INSERT INTO ghost_ks.ghost_table (a, b, c) VALUES (?, ?, ?)");
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;

    // Must be ERROR, NOT RESULT
    assert_eq!(
        resp.opcode,
        Opcode::Error,
        "PREPARE against a table not in local schema must return ERROR (not PREPARED \
         with col_count=0). Before p0-22 fix this returned PREPARED with col_count=0, \
         causing WrongColumnCount in strict CQL drivers."
    );
}

/// P0-22 control case: SELECT with no `?` markers must report col_count = 0.
#[tokio::test]
async fn prepare_no_bind_markers_reports_col_count_zero() {
    let (state, _dir) = setup_state();
    let server = ferrosa_cql::server::CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();
    let mut stream = connect_auth_disabled(addr).await;

    let body = encode_query_body(
        "CREATE KEYSPACE p0_22_ctrl WITH replication = \
         {'class': 'SimpleStrategy', 'replication_factor': '1'}",
    );
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let body = encode_query_body("CREATE TABLE p0_22_ctrl.t (a uuid PRIMARY KEY, b text)");
    send_raw_frame(&mut stream, Opcode::Query, &body).await;
    assert_result(&read_frame(&mut stream).await);

    let prep_body = encode_prepare_body("SELECT * FROM p0_22_ctrl.t");
    send_raw_frame(&mut stream, Opcode::Prepare, &prep_body).await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);

    let col_count = extract_bind_col_count_from_prepared_response(&resp.body);
    assert_eq!(
        col_count, 0,
        "PREPARE SELECT with no bind markers must report col_count = 0; \
         got {col_count} (control case)"
    );
}

/// On the same server, a v4 connection succeeds while a v5 connection is
/// rejected — the two do NOT coexist under the v4 cap.
#[tokio::test]
async fn v4_works_and_v5_is_rejected_on_same_server() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(test_config(true), state);
    let addr = server.start_background().await.unwrap();

    // v4 connection: STARTUP → READY at 0x84.
    let mut v4 = TcpStream::connect(addr).await.unwrap();
    send_startup(&mut v4).await;
    let ready4 = read_frame(&mut v4).await;
    assert_eq!(ready4.opcode, Opcode::Ready);
    assert_eq!(ready4.header.version, 0x84, "v4 response should be 0x84");

    // v5 connection: STARTUP → protocol-version ERROR, forcing the driver to
    // retry at v4.
    let mut v5 = TcpStream::connect(addr).await.unwrap();
    v5.write_all(&encode_startup_frame_v5()).await.unwrap();
    let resp5 = read_frame(&mut v5).await;
    assert_eq!(
        resp5.opcode,
        Opcode::Error,
        "v5 must be rejected, not accepted alongside v4"
    );
    let error_code = i32::from_be_bytes(resp5.body[0..4].try_into().unwrap());
    assert_eq!(
        error_code, 0x000A,
        "v5 rejection must use error code 0x000A (protocol error)"
    );
}
