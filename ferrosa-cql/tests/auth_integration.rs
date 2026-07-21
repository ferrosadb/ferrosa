//! Integration tests — Sprint A of the CQL role-auth rollout.
//!
//! Covers:
//! - Sprint A: STARTUP with `auth_disabled=false` returns AUTHENTICATE; then
//!   sending a QUERY before completing auth returns ERROR 0x2100 Unauthorized
//!   (not a Protocol error).
//! - Sprint A: `auth_disabled=true` logs `WARN auth: DISABLED (env override)`.
//! - Sprint B: Fresh engine bootstrap creates three seed roles with correct
//!   permissions: `ferrosa_admin` (superuser), `graph_engine`, `app_reader`.

use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use bytes::BytesMut;
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

/// Timeout for handshake operations.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

fn setup_state() -> (Arc<SharedState>, TempDir) {
    let dir = TempDir::new().unwrap();
    let commit_log = CommitLogConfig {
        segment_size: 4096,
        max_segment_age: Duration::from_secs(60),
        sync_strategy: SyncStrategyConfig::Batch,
        batch: Default::default(),
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
        core: Arc::new(ferrosa_session::SessionCore {
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
            udf_executor,
            mode_controller,
            auth_warn: false,
            peer_manager: None,
            accord_clock: None,
            accord_state: ferrosa_cluster::accord::empty_accord_state_slot(),
        }),
        prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
        connection_tracker: Arc::new(ConnectionTracker::new()),
        query_tracker: Arc::new(QueryTracker::new()),
        full_scan_tracker: Arc::new(ferrosa_cql::virtual_tables::FullScanTracker::new()),
        index_usage_tracker: Arc::new(ferrosa_cql::virtual_tables::IndexUsageTracker::new()),
        event_sender: tokio::sync::broadcast::channel(64).0,
        last_schema_event: tokio::sync::watch::channel(None).0,
        topology_policy: ClientTopologyPolicy::default(),
        txn_registry: ferrosa_cql::txn_registry::TransactionRegistry::shared_default(),
        cql_metrics: Arc::new(ferrosa_cql::observability::CqlMetrics::new()),
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
        version: 0x04,
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

fn encode_query_body(query: &str) -> Vec<u8> {
    let query_bytes = query.as_bytes();
    let mut body = Vec::new();
    body.extend_from_slice(&(query_bytes.len() as i32).to_be_bytes());
    body.extend_from_slice(query_bytes);
    body.extend_from_slice(&0u16.to_be_bytes()); // consistency ONE
    body.push(0); // no flags
    body
}

async fn send_raw_frame(stream: &mut TcpStream, opcode: Opcode, body: &[u8]) {
    let header = FrameHeader {
        version: 0x04,
        flags: 0,
        stream_id: 1,
        opcode,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(body);
    stream.write_all(&buf).await.unwrap();
}

async fn read_frame(stream: &mut TcpStream) -> (Opcode, Vec<u8>) {
    let mut hdr_buf = vec![0u8; HEADER_SIZE];
    timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut hdr_buf))
        .await
        .expect("timed out waiting for frame header")
        .unwrap();
    let header = FrameHeader::decode(&hdr_buf).unwrap();
    let mut body = vec![0u8; header.length as usize];
    if !body.is_empty() {
        timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut body))
            .await
            .expect("timed out waiting for frame body")
            .unwrap();
    }
    (header.opcode, body)
}

fn server_config(auth_disabled: bool) -> ServerConfig {
    ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        auth_disabled,
        ..ServerConfig::default()
    }
}

// ── Sprint A tests ─────────────────────────────────────────────────────────

/// Sprint A: STARTUP with auth_disabled=false must return AUTHENTICATE, then
/// sending a QUERY before completing auth must return ERROR 0x2100 (Unauthorized),
/// not ERROR 0x000A (Protocol). This pins the behavior that "no credentials
/// means you are unauthorised" — not "you broke the protocol".
#[tokio::test]
async fn startup_then_query_without_auth_returns_unauthorized() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(server_config(false), state);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP — server should challenge with AUTHENTICATE.
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();
    let (opcode, _body) = read_frame(&mut stream).await;
    assert_eq!(
        opcode,
        Opcode::Authenticate,
        "server must challenge with AUTHENTICATE when auth is enabled"
    );

    // Skip auth and immediately send a QUERY — should get 0x2100 Unauthorized,
    // not 0x000A Protocol error.
    let query = encode_query_body("SELECT * FROM system.local");
    send_raw_frame(&mut stream, Opcode::Query, &query).await;
    let (opcode, body) = read_frame(&mut stream).await;

    assert_eq!(
        opcode,
        Opcode::Error,
        "server must return ERROR when query arrives before auth completes"
    );
    assert!(
        body.len() >= 4,
        "ERROR frame body must contain at least the error code"
    );
    let error_code = i32::from_be_bytes(body[..4].try_into().unwrap());
    assert_eq!(
        error_code, 0x2100,
        "error code must be 0x2100 (Unauthorized), got 0x{error_code:04X}"
    );
}

/// Sprint A: When auth_disabled=false, STARTUP followed by a non-AUTH_RESPONSE
/// opcode (here: OPTIONS) must also return 0x2100 Unauthorized.
#[tokio::test]
async fn startup_then_options_without_auth_returns_unauthorized() {
    let (state, _dir) = setup_state();
    let server = CqlServer::new(server_config(false), state);
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();
    let (opcode, _body) = read_frame(&mut stream).await;
    assert_eq!(opcode, Opcode::Authenticate);

    // OPTIONS during auth phase — must be Unauthorized, not Protocol error.
    send_raw_frame(&mut stream, Opcode::Options, &[]).await;
    let (opcode, body) = read_frame(&mut stream).await;
    assert_eq!(opcode, Opcode::Error);
    let error_code = i32::from_be_bytes(body[..4].try_into().unwrap());
    assert_eq!(
        error_code, 0x2100,
        "non-AUTH_RESPONSE during authentication phase must return 0x2100 (Unauthorized), \
         got 0x{error_code:04X}"
    );
}

/// Sprint A: Logging test — when `auth_disabled=true` is set on the ServerConfig,
/// starting the server must emit a WARN-level log containing "DISABLED".
/// We verify this via the `FERROSA_AUTH_DISABLED` env-var startup path using a
/// tracing subscriber that captures events.
#[tokio::test]
async fn auth_disabled_server_logs_disabled_warning_at_startup() {
    use std::sync::Mutex;
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;

    // Capture WARN log lines emitted during server start.
    #[derive(Default)]
    struct Capture {
        lines: Mutex<Vec<String>>,
    }

    // Newtype so we can impl a foreign trait for a local type.
    struct CaptureLayer(Arc<Capture>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() <= Level::WARN {
                struct Visitor(String);
                impl tracing::field::Visit for Visitor {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        if field.name() == "message" {
                            self.0 = format!("{value:?}");
                        }
                    }
                    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                        if field.name() == "message" {
                            self.0 = value.to_string();
                        }
                    }
                }
                let mut v = Visitor(String::new());
                event.record(&mut v);
                if !v.0.is_empty() {
                    self.0.lines.lock().unwrap().push(v.0);
                }
            }
        }
    }

    let capture = Arc::new(Capture::default());
    let subscriber = tracing_subscriber::Registry::default().with(CaptureLayer(capture.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let (state, _dir) = setup_state();
    // Starting a server with auth_disabled=true should log a WARN.
    let server = CqlServer::new(server_config(true), state);
    let _addr = server.start_background().await.unwrap();

    // Give the background task a moment to emit the startup log.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let lines = capture.lines.lock().unwrap();
    let found = lines.iter().any(|l| {
        l.to_ascii_uppercase().contains("DISABLED") || l.contains("auth") || l.contains("AUTH")
    });
    assert!(
        found,
        "expected a WARN log containing 'DISABLED' when auth_disabled=true; \
         captured lines: {lines:?}"
    );
}

// ── Sprint B tests ─────────────────────────────────────────────────────────

/// Sprint B: Fresh-cluster bootstrap creates exactly three seed roles.
/// After `seed_default_roles`, the schema must contain:
///   - `ferrosa_admin` (superuser=true, can_login=true)
///   - `graph_engine`  (superuser=false, can_login=true)
///   - `app_reader`    (superuser=false, can_login=true)
#[test]
fn seed_bootstrap_creates_three_roles() {
    use ferrosa_schema::auth::bootstrap;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };

    let schema = Schema::new(SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    })
    .unwrap();

    bootstrap::seed_default_roles(&schema).unwrap();

    let snap = schema.snapshot();

    // ferrosa_admin — superuser, can login.
    let admin = snap
        .roles
        .get("ferrosa_admin")
        .expect("ferrosa_admin role must be created by seed_default_roles");
    assert!(admin.is_superuser, "ferrosa_admin must be a superuser");
    assert!(admin.can_login, "ferrosa_admin must have LOGIN=true");
    assert!(
        admin.salted_hash.is_some(),
        "ferrosa_admin must have a password hash"
    );

    // graph_engine — not superuser, can login.
    let ge = snap
        .roles
        .get("graph_engine")
        .expect("graph_engine role must be created by seed_default_roles");
    assert!(!ge.is_superuser, "graph_engine must NOT be a superuser");
    assert!(ge.can_login, "graph_engine must have LOGIN=true");
    assert!(
        ge.salted_hash.is_some(),
        "graph_engine must have a password hash"
    );

    // app_reader — not superuser, can login.
    let ar = snap
        .roles
        .get("ferrosa_user")
        .expect("app_reader role must be created by seed_default_roles");
    assert!(!ar.is_superuser, "app_reader must NOT be a superuser");
    assert!(ar.can_login, "app_reader must have LOGIN=true");
    assert!(
        ar.salted_hash.is_some(),
        "app_reader must have a password hash"
    );
}

/// Sprint B: Bootstrap adds exactly three new roles (ferrosa_admin, graph_engine,
/// app_reader) on top of whatever roles Schema::new pre-creates (e.g. `cassandra`).
#[test]
fn seed_bootstrap_role_count_is_exactly_three() {
    use ferrosa_schema::auth::bootstrap;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };

    let schema = Schema::new(SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    })
    .unwrap();

    let roles_before = schema.snapshot().roles.len();
    bootstrap::seed_default_roles(&schema).unwrap();

    let snap = schema.snapshot();
    let new_roles = snap.roles.len() - roles_before;
    assert_eq!(
        new_roles,
        3,
        "seed_default_roles must add exactly 3 new roles \
         (ferrosa_admin, graph_engine, app_reader); \
         added {} (total {} roles: {:?})",
        new_roles,
        snap.roles.len(),
        snap.roles.keys().collect::<Vec<_>>()
    );
}

/// Sprint B: Bootstrap is idempotent — calling twice doesn't create duplicates.
#[test]
fn seed_bootstrap_is_idempotent() {
    use ferrosa_schema::auth::bootstrap;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };

    let schema = Schema::new(SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    })
    .unwrap();

    let roles_before = schema.snapshot().roles.len();
    bootstrap::seed_default_roles(&schema).unwrap();
    bootstrap::seed_default_roles(&schema).unwrap();

    let snap = schema.snapshot();
    let new_roles = snap.roles.len() - roles_before;
    assert_eq!(
        new_roles,
        3,
        "second call to seed_default_roles must be a no-op; \
         expected 3 new roles, got {} new roles ({} total)",
        new_roles,
        snap.roles.len()
    );
}

/// Sprint B: Each seeded role can authenticate with its documented default password.
#[test]
fn seed_bootstrap_roles_authenticate_with_default_passwords() {
    use ferrosa_schema::auth::bootstrap;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };

    let schema = Schema::new(SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    })
    .unwrap();

    bootstrap::seed_default_roles(&schema).unwrap();

    let admin = schema
        .authenticate("ferrosa_admin", "ferrosa_admin")
        .expect("ferrosa_admin must authenticate with password 'ferrosa_admin'");
    assert!(admin.is_superuser);

    // graph_engine and app_reader use "ferrosa_user" as the documented default.
    let ge = schema
        .authenticate("graph_engine", "ferrosa_user")
        .expect("graph_engine must authenticate with password 'ferrosa_user'");
    assert!(!ge.is_superuser);

    let ar = schema
        .authenticate("ferrosa_user", "ferrosa_user")
        .expect("app_reader must authenticate with password 'ferrosa_user'");
    assert!(!ar.is_superuser);
}
