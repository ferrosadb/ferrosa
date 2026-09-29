//! t_e1c819ad: PostgreSQL front-end security, driven by a real driver
//! (`tokio-postgres`) over loopback against the schema-backed role store.
//!
//! * Role checks — every statement is authorized with the same
//!   `Schema::check_permission` model the CQL router uses; a denial is
//!   SQLSTATE 42501 `insufficient_privilege`.
//! * Failed-login limiter — PostgreSQL logins go through the schema's shared
//!   per-user rate limiter, so a lockout earned over PostgreSQL also refuses
//!   CQL (`Schema::authenticate`) for that user.
//! * TLS — `SSLRequest` is upgraded with the shared rustls machinery, and a
//!   plaintext client is refused (`28000`) when `require_tls` is set.
//!
//! No external infrastructure: a temp `StorageEngine` with no object store.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_postgres::{server, QueryContext, SchemaVerifierStore};
use ferrosa_schema::auth::permission::{Permission, Resource};
use ferrosa_schema::auth::role::RoleMetadata;
use ferrosa_schema::{
    AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
    EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
    ReplicationParams, Schema, SchemaConfig, SchemaError, TableMetadata, TableParams,
    TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use indexmap::IndexMap;
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

const PASSWORD: &str = "Correct-h0rse-battery!";

fn superuser() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn schema_config(rate_limit: RateLimitConfig) -> SchemaConfig {
    SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit,
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    }
}

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        write_verify: false,
    }
}

fn column(name: &str, kind: ColumnKind) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position: 0,
        column_type: "text".to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

fn login_role(schema: &Schema, name: &str) {
    schema
        .create_role(
            RoleMetadata {
                name: name.to_string(),
                is_superuser: false,
                can_login: true,
                salted_hash: None,
                member_of: Default::default(),
                scram: None,
            },
            Some(PASSWORD),
            &superuser(),
        )
        .expect("create login role");
}

fn grant(schema: &Schema, role: &str, resource: Resource, perm: Permission) {
    schema
        .grant(role, &resource, HashSet::from([perm]), &superuser())
        .expect("grant");
}

/// `public.kv(k text PK, v text)` plus three roles:
/// * `reader` — SELECT on `public.kv`
/// * `writer` — MODIFY on keyspace `public`
/// * `nobody` — no grants
fn build_schema(rate_limit: RateLimitConfig) -> Schema {
    let schema = Schema::new(schema_config(rate_limit)).expect("schema bootstraps");
    let su = superuser();
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            },
            &su,
        )
        .expect("create keyspace public");
    let mut cols = IndexMap::new();
    cols.insert("k".to_string(), column("k", ColumnKind::PartitionKey));
    cols.insert("v".to_string(), column("v", ColumnKind::Regular));
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "kv".to_string(),
                id: Uuid::new_v4(),
                columns: cols,
                partition_key: vec!["k".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &su,
        )
        .expect("create table kv");

    login_role(&schema, "reader");
    grant(
        &schema,
        "reader",
        Resource::Table("public".into(), "kv".into()),
        Permission::Select,
    );
    login_role(&schema, "writer");
    grant(
        &schema,
        "writer",
        Resource::Keyspace("public".into()),
        Permission::Modify,
    );
    login_role(&schema, "nobody");
    schema
}

fn kv_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    TableSchema {
        keyspace: "public".to_string(),
        table: "kv".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

struct Fixture {
    port: u16,
    schema: Arc<Schema>,
    _dir: tempfile::TempDir,
}

async fn start(rate_limit: RateLimitConfig) -> Fixture {
    start_with_tls(rate_limit, server::PgTls::plaintext()).await
}

async fn start_with_tls(rate_limit: RateLimitConfig, tls: server::PgTls) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
    engine.register_table(kv_storage_schema()).unwrap();
    let schema = Arc::new(build_schema(rate_limit));
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: schema.clone(),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord_committer: None,
        ddl: None,
    });
    let store = Arc::new(SchemaVerifierStore::new(schema.clone()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(listener, store, ctx, tls));
    Fixture {
        port,
        schema,
        _dir: dir,
    }
}

async fn try_connect(
    port: u16,
    user: &str,
    password: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user(user)
        .password(password)
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("test connection task ended: {error}");
        }
    });
    Ok(client)
}

async fn connect(port: u16, user: &str) -> tokio_postgres::Client {
    try_connect(port, user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("{user} should authenticate: {e}"))
}

fn sqlstate(err: &tokio_postgres::Error) -> Option<&str> {
    err.code().map(|c| c.code())
}

#[tokio::test]
async fn role_without_select_is_refused_42501_on_simple_and_extended() {
    let fx = start(RateLimitConfig::default()).await;
    let client = connect(fx.port, "nobody").await;

    let err = client
        .simple_query("SELECT k, v FROM kv")
        .await
        .expect_err("a role without SELECT must be refused");
    assert_eq!(sqlstate(&err), Some("42501"), "simple query: {err}");

    let err = client
        .query("SELECT k, v FROM kv WHERE k = $1", &[&"a"])
        .await
        .expect_err("a role without SELECT must be refused over the extended protocol");
    assert_eq!(sqlstate(&err), Some("42501"), "extended query: {err}");

    // Statements that touch no table are not gated.
    client
        .simple_query("SELECT 1")
        .await
        .expect("a table-free SELECT needs no grant");
}

#[tokio::test]
async fn role_with_select_reads_but_cannot_write() {
    let fx = start(RateLimitConfig::default()).await;
    let client = connect(fx.port, "reader").await;

    client
        .simple_query("SELECT k, v FROM kv")
        .await
        .expect("SELECT on public.kv is granted");
    client
        .query("SELECT k, v FROM kv WHERE k = $1", &[&"a"])
        .await
        .expect("SELECT on public.kv is granted over the extended protocol");

    let err = client
        .simple_query("INSERT INTO kv (k, v) VALUES ('a', 'b')")
        .await
        .expect_err("SELECT does not imply MODIFY");
    assert_eq!(sqlstate(&err), Some("42501"), "{err}");
}

#[tokio::test]
async fn role_with_modify_writes_and_returning_also_needs_select() {
    let fx = start(RateLimitConfig::default()).await;
    let client = connect(fx.port, "writer").await;

    client
        .simple_query("INSERT INTO kv (k, v) VALUES ('a', 'b')")
        .await
        .expect("MODIFY on keyspace public covers public.kv");
    client
        .execute("DELETE FROM kv WHERE k = $1", &[&"a"])
        .await
        .expect("MODIFY covers extended-protocol DELETE");

    // RETURNING reads the row back, so it additionally requires SELECT.
    let err = client
        .simple_query("INSERT INTO kv (k, v) VALUES ('c', 'd') RETURNING k")
        .await
        .expect_err("RETURNING without SELECT must be refused");
    assert_eq!(sqlstate(&err), Some("42501"), "{err}");

    let err = client
        .simple_query("SELECT k FROM kv")
        .await
        .expect_err("MODIFY does not imply SELECT");
    assert_eq!(sqlstate(&err), Some("42501"), "{err}");
}

#[tokio::test]
async fn repeated_bad_passwords_lock_out_postgres_and_cql() {
    let fx = start(RateLimitConfig {
        max_attempts: 3,
        base_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        lockout_duration: Duration::from_secs(600),
        window: Duration::from_secs(600),
    })
    .await;

    for attempt in 1..=3 {
        let err = try_connect(fx.port, "reader", "wrong-password")
            .await
            .err()
            .unwrap_or_else(|| panic!("attempt {attempt}: a wrong password must not log in"));
        assert_eq!(sqlstate(&err), Some("28P01"), "attempt {attempt}: {err}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let err = try_connect(fx.port, "reader", PASSWORD)
        .await
        .expect_err("a locked-out user must be refused even with the right password");
    assert_eq!(sqlstate(&err), Some("28000"), "{err}");
    let message = err
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_default();
    assert!(
        message.contains("throttled"),
        "the refusal must say why: {message:?}"
    );

    // The lockout lives in the schema's shared per-user limiter, so the CQL
    // login path refuses the same user.
    assert!(
        matches!(
            fx.schema.authenticate("reader", PASSWORD),
            Err(SchemaError::AuthenticationThrottled)
        ),
        "a lockout earned over PostgreSQL must also refuse CQL"
    );

    // Other users are unaffected.
    connect(fx.port, "writer").await;
}

// ── TLS ─────────────────────────────────────────────────────────────────────

/// A self-signed certificate for `localhost` written to a temp dir; returns
/// the dir guard, the cert/key paths, and the DER cert for the client trust
/// store.
fn self_signed() -> (
    tempfile::TempDir,
    String,
    String,
    rustls::pki_types::CertificateDer<'static>,
) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).unwrap();
    std::fs::write(&key_path, certified.signing_key.serialize_pem()).unwrap();
    (
        dir,
        cert_path.to_str().unwrap().to_string(),
        key_path.to_str().unwrap().to_string(),
        certified.cert.der().clone(),
    )
}

/// Negotiate TLS the way libpq does (SSLRequest → `S` → TLS handshake), then
/// run the PostgreSQL protocol over the encrypted stream.
async fn connect_over_tls(
    port: u16,
    user: &str,
    trust: rustls::pki_types::CertificateDer<'static>,
) -> tokio_postgres::Client {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut ssl_request = Vec::new();
    ssl_request.extend_from_slice(&8i32.to_be_bytes());
    ssl_request.extend_from_slice(&80877103i32.to_be_bytes());
    tcp.write_all(&ssl_request).await.unwrap();
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await.unwrap();
    assert_eq!(
        answer[0], b'S',
        "a TLS-configured server must accept SSLRequest"
    );

    let mut roots = rustls::RootCertStore::empty();
    roots.add(trust).unwrap();
    let client_config =
        rustls::ClientConfig::builder_with_provider(ferrosa_net::tls::crypto_provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tls = connector
        .connect(name, tcp)
        .await
        .expect("TLS handshake with the PostgreSQL listener");

    let (client, connection) = Config::new()
        .user(user)
        .password(PASSWORD)
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable) // TLS is already established underneath
        .connect_raw(tls, NoTls)
        .await
        .expect("SCRAM over TLS");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("TLS test connection task ended: {error}");
        }
    });
    client
}

#[tokio::test]
async fn tls_handshake_succeeds_and_plaintext_is_refused_when_required() {
    let (_certs, cert, key, der) = self_signed();
    let tls = server::PgTls::from_pem(Some(&cert), Some(&key), true).expect("PgTls");
    let fx = start_with_tls(RateLimitConfig::default(), tls).await;

    let client = connect_over_tls(fx.port, "reader", der).await;
    client
        .simple_query("SELECT k, v FROM kv")
        .await
        .expect("an authorized query runs over TLS");

    let err = try_connect(fx.port, "reader", PASSWORD)
        .await
        .expect_err("a plaintext client must be refused when TLS is required");
    assert_eq!(sqlstate(&err), Some("28000"), "{err}");
    let message = err
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_default();
    assert!(message.contains("TLS is required"), "{message:?}");
}

#[tokio::test]
async fn tls_offered_but_not_required_still_accepts_plaintext() {
    let (_certs, cert, key, der) = self_signed();
    let tls = server::PgTls::from_pem(Some(&cert), Some(&key), false).expect("PgTls");
    let fx = start_with_tls(RateLimitConfig::default(), tls).await;
    connect_over_tls(fx.port, "reader", der).await;
    connect(fx.port, "reader").await;
}

#[test]
fn require_tls_without_a_certificate_is_a_config_error() {
    let err = server::PgTls::from_pem(None, None, true)
        .err()
        .expect("require_tls with no certificate must not start a plaintext listener");
    assert!(err.contains("postgres"), "{err}");
}
