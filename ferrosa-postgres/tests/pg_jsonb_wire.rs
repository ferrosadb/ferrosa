//! jsonb (OID 3802) on the PG wire, end to end with a real driver (T-161a, D11,
//! D24, D26, D6b, D14b).
//!
//! `tokio-postgres` talks to the real server. The schema comes from PG DDL, so
//! the table is created the way a client creates it. Parameters go through a raw
//! wrapper type so a test controls the format code and the exact bytes (version
//! byte included) rather than trusting a driver codec.

use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::BytesMut;
use ferrosa_cluster::ddl_path::DdlPath;
use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_jsonb::{Limits, LimitsConfig};
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, AccordAccess, ClusterDdl, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, DeploymentMode, EnvSecretsProvider, KeyspaceMetadata, PasswordHasher,
    PasswordPolicy, RateLimitConfig, ReplicationParams, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row as StorageRow};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{to_sql_checked, Format, FromSql, IsNull, ToSql, Type};
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

struct OneRole(ScramVerifier);

/// A single superuser login with no limiter (PR #465 added the limiter
/// methods; `security_live.rs` covers the schema-backed store).
impl VerifierStore for OneRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == "ferrosa_user").then(|| self.0.clone())
    }
    fn admit(&self, _user: &str) -> Result<(), String> {
        Ok(())
    }
    fn record_failure(&self, _user: &str) {}
    fn record_success(&self, user: &str) -> Result<ferrosa_schema::AuthContext, String> {
        Ok(ferrosa_schema::AuthContext {
            role: user.to_string(),
            is_superuser: true,
            must_change_password: false,
        })
    }
}

/// A parameter whose bytes and format code the test chooses.
#[derive(Debug)]
struct Raw {
    bytes: Vec<u8>,
    text_format: bool,
}

impl Raw {
    fn text(s: &str) -> Raw {
        Raw {
            bytes: s.as_bytes().to_vec(),
            text_format: true,
        }
    }

    /// Binary jsonb: `version` then the JSON text (`jsonb_send` layout).
    fn binary(version: u8, s: &str) -> Raw {
        let mut bytes = vec![version];
        bytes.extend_from_slice(s.as_bytes());
        Raw {
            bytes,
            text_format: false,
        }
    }
}

impl ToSql for Raw {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.bytes);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB || *ty == Type::JSON || *ty == Type::TEXT
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        if self.text_format {
            Format::Text
        } else {
            Format::Binary
        }
    }

    to_sql_checked!();
}

/// A binary-format jsonb result column, bytes untouched.
struct RawOut(Vec<u8>);

impl<'a> FromSql<'a> for RawOut {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(RawOut(raw.to_vec()))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }
}

fn schema_config() -> SchemaConfig {
    SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
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
        cache_hot_window_secs: 900,
        write_verify: false,
    }
}

struct Fixture {
    client: tokio_postgres::Client,
    engine: Arc<StorageEngine>,
    _dir: tempfile::TempDir,
}

fn limits(cfg: LimitsConfig) -> Limits {
    Limits::from_config(&cfg, 32 * 1024 * 1024).expect("limits resolve")
}

async fn start() -> Fixture {
    start_with(limits(LimitsConfig::default())).await
}

async fn start_with(jsonb_limits: Limits) -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).expect("engine"));
    let schema = Arc::new(Schema::new(schema_config()).expect("schema bootstraps"));
    let auth = AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    };
    let mut options = HashMap::new();
    options.insert("replication_factor".to_string(), "1".to_string());
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options,
                },
            },
            &auth,
        )
        .expect("create keyspace");
    let path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
        schema: schema.clone(),
        engine: engine.clone(),
    }));
    let ctx = Arc::new(QueryContext {
        engine: engine.clone(),
        schema,
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: Some(Arc::new(ClusterDdl::new(path))),
        jsonb_limits,
        portals: Default::default(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let store = Arc::new(OneRole(ScramVerifier::from_password(
        "devpass",
        b"ferrosa-dev-salt",
        4096,
    )));
    tokio::spawn(server::serve(
        listener,
        store,
        ctx,
        server::PgTls::plaintext(),
    ));
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("SCRAM handshake succeeds");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("driver connection ended: {error}");
        }
    });
    let fx = Fixture {
        client,
        engine,
        _dir: dir,
    };
    fx.client
        .batch_execute("CREATE TABLE j (id int PRIMARY KEY, doc jsonb)")
        .await
        .expect("CREATE TABLE with a jsonb column");
    fx
}

fn code_of(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|db| db.code().code().to_string())
        .unwrap_or_else(|| format!("not a database error: {error}"))
}

/// The text of `doc` for the row with `id`, through the simple protocol (text
/// format), or `None` when the row is absent. A NULL cell is a test failure.
async fn simple_doc(fx: &Fixture, id: i32) -> Option<String> {
    let messages = fx
        .client
        .simple_query(&format!("SELECT doc FROM j WHERE id = {id}"))
        .await
        .expect("simple SELECT");
    let mut found = None;
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            found = Some(
                row.get(0)
                    .expect("a stored jsonb cell is never NULL")
                    .to_string(),
            );
        }
    }
    found
}

async fn row_count(fx: &Fixture) -> usize {
    fx.client
        .simple_query("SELECT id FROM j")
        .await
        .expect("count rows")
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

const INSERT: &str = "INSERT INTO j (id, doc) VALUES ($1, $2)";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_jsonb_literal_and_text_param_round_trip() {
    let fx = start().await;
    // Untyped string literal into a jsonb column: parsed and validated.
    fx.client
        .batch_execute(r#"INSERT INTO j (id, doc) VALUES (1, '{"aa":2,"b":1}')"#)
        .await
        .expect("literal coerces to jsonb");
    // PostgreSQL text form: keys shortest first, `": "` and `", "` (D26).
    assert_eq!(
        simple_doc(&fx, 1).await.as_deref(),
        Some(r#"{"b": 1, "aa": 2}"#)
    );

    // Scale is preserved (D2a); duplicate keys resolve last-wins (D6b).
    fx.client
        .batch_execute(r#"INSERT INTO j (id, doc) VALUES (2, '{"x":1.10,"k":1,"k":2}')"#)
        .await
        .expect("literal with scale and a duplicate key");
    assert_eq!(
        simple_doc(&fx, 2).await.as_deref(),
        Some(r#"{"k": 2, "x": 1.10}"#)
    );

    // A text-FORMAT parameter (format code 0) on the prepared jsonb parameter.
    let stmt = fx.client.prepare(INSERT).await.expect("prepare");
    fx.client
        .execute(&stmt, &[&3i32, &Raw::text(r#"{"aa":2,"b":1}"#)])
        .await
        .expect("text-format jsonb parameter");
    assert_eq!(
        simple_doc(&fx, 3).await.as_deref(),
        Some(r#"{"b": 1, "aa": 2}"#)
    );

    // A parameter DECLARED text (OID 25) bound to the jsonb column is parsed too.
    let typed = fx
        .client
        .prepare_typed(INSERT, &[Type::INT4, Type::TEXT])
        .await
        .expect("prepare with a declared text parameter");
    fx.client
        .execute(&typed, &[&4i32, &Raw::text(r#"[1, 2.50, null]"#)])
        .await
        .expect("declared-text parameter coerces to jsonb");
    assert_eq!(simple_doc(&fx, 4).await.as_deref(), Some("[1, 2.50, null]"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_jsonb_wire_version_byte_rules() {
    let fx = start().await;
    let stmt = fx.client.prepare(INSERT).await.expect("prepare");
    // A binary parameter: version 1 then the text.
    fx.client
        .execute(&stmt, &[&1i32, &Raw::binary(1, r#"{"aa":2,"b":1}"#)])
        .await
        .expect("binary jsonb parameter with version 1");

    // A binary result is 0x01 followed by the PostgreSQL text form.
    let rows = fx
        .client
        .query("SELECT doc FROM j WHERE id = 1", &[])
        .await
        .expect("binary-format SELECT");
    let out: RawOut = rows[0].get(0);
    let mut want = vec![1u8];
    want.extend_from_slice(br#"{"b": 1, "aa": 2}"#);
    assert_eq!(out.0, want);

    // Any other version is XX000, as in PostgreSQL 16, and writes nothing (FM-41).
    for version in [0u8, 2, 255] {
        let error = fx
            .client
            .execute(&stmt, &[&9i32, &Raw::binary(version, "{}")])
            .await
            .expect_err("a bad version byte is refused");
        assert_eq!(code_of(&error), "XX000", "version {version}");
    }
    // An empty binary value has no version byte at all.
    let empty = Raw {
        bytes: Vec::new(),
        text_format: false,
    };
    let error = fx
        .client
        .execute(&stmt, &[&9i32, &empty])
        .await
        .expect_err("an empty binary jsonb is refused");
    assert_eq!(code_of(&error), "08P01");
    assert_eq!(row_count(&fx).await, 1, "the refused writes left no row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_invalid_jsonb_param_is_22p02_not_null() {
    let fx = start().await;
    let stmt = fx.client.prepare(INSERT).await.expect("prepare");
    let secret = "SENTINEL-VALUE-MUST-NOT-ECHO";
    let bad_text = format!(r#"{{"a": {secret}}}"#);

    let text = fx
        .client
        .execute(&stmt, &[&1i32, &Raw::text(&bad_text)])
        .await
        .expect_err("invalid JSON text is refused");
    assert_eq!(code_of(&text), "22P02");
    let binary = fx
        .client
        .execute(&stmt, &[&2i32, &Raw::binary(1, &bad_text)])
        .await
        .expect_err("invalid JSON binary is refused");
    assert_eq!(code_of(&binary), "22P02");
    let literal = fx
        .client
        .batch_execute(&format!("INSERT INTO j (id, doc) VALUES (3, '{bad_text}')"))
        .await
        .expect_err("an invalid literal is refused");
    assert_eq!(code_of(&literal), "22P02");

    for error in [&text, &binary, &literal] {
        let message = error.as_db_error().expect("db error").message();
        assert!(
            message.contains("byte offset"),
            "carries an offset: {message}"
        );
        assert!(!message.contains(secret), "never echoes input: {message}");
    }
    assert_eq!(row_count(&fx).await, 0, "no row was written, and no NULL");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_jsonb_over_limit_input_is_54000_and_writes_nothing() {
    let fx = start_with(limits(LimitsConfig {
        max_input_bytes: Some(64),
        max_nesting_depth: Some(4),
        ..LimitsConfig::default()
    }))
    .await;
    let stmt = fx.client.prepare(INSERT).await.expect("prepare");
    let big = format!(r#"{{"k": "{}"}}"#, "x".repeat(100));
    let deep = "[[[[[[1]]]]]]";

    let by_size = fx
        .client
        .execute(&stmt, &[&1i32, &Raw::text(&big)])
        .await
        .expect_err("over max_input_bytes");
    assert_eq!(code_of(&by_size), "54000");
    let by_depth = fx
        .client
        .execute(&stmt, &[&2i32, &Raw::binary(1, deep)])
        .await
        .expect_err("over max_nesting_depth");
    assert_eq!(code_of(&by_depth), "54000");
    let by_literal = fx
        .client
        .batch_execute(&format!("INSERT INTO j (id, doc) VALUES (3, '{big}')"))
        .await
        .expect_err("literal over max_input_bytes");
    assert_eq!(code_of(&by_literal), "54000");
    assert_eq!(row_count(&fx).await, 0);

    // Within the limits it still works: the limits are configured, not blanket.
    fx.client
        .batch_execute(r#"INSERT INTO j (id, doc) VALUES (4, '{"a": [1]}')"#)
        .await
        .expect("an in-limit document is accepted");
    assert_eq!(simple_doc(&fx, 4).await.as_deref(), Some(r#"{"a": [1]}"#));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_jsonb_describe_reports_3802() {
    let fx = start().await;
    // ParameterDescription: the jsonb column's parameter is 3802.
    let stmt = fx.client.prepare(INSERT).await.expect("prepare INSERT");
    assert_eq!(stmt.params(), [Type::INT4, Type::JSONB]);
    // RowDescription: OID 3802 with typlen -1 (asserted on the raw message by
    // the server unit tests; the driver reports the resolved type here).
    let select = fx
        .client
        .prepare("SELECT doc FROM j")
        .await
        .expect("prepare SELECT");
    assert_eq!(select.columns()[0].type_(), &Type::JSONB);

    // A `json` column is stored as jsonb, and the catalog reports jsonb (D11).
    fx.client
        .batch_execute("CREATE TABLE jj (id int PRIMARY KEY, doc json)")
        .await
        .expect("CREATE TABLE with a json column");
    let json_select = fx
        .client
        .prepare("SELECT doc FROM jj")
        .await
        .expect("prepare");
    assert_eq!(json_select.columns()[0].type_(), &Type::JSONB);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_jsonb_corrupt_stored_cell_is_an_error_not_null() {
    let fx = start().await;
    fx.client
        .batch_execute(r#"INSERT INTO j (id, doc) VALUES (1, '{"a": 1}')"#)
        .await
        .expect("insert a good row");
    // Overwrite the cell with bytes no jsonb validator accepts, straight into
    // the engine (a torn write or bit rot looks like this).
    let key = DecoratedKey::new(PartitionKey::new(1i32.to_be_bytes().to_vec()));
    let row = StorageRow {
        clustering: Vec::new(),
        cells: vec![(
            0,
            CellValue::live(vec![0xde, 0xad, 0xbe, 0xef], i64::MAX / 2),
        )],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(i64::MAX / 2),
    };
    fx.engine
        .write(&TableId::new("public", "j"), &key, row, i64::MAX / 2)
        .expect("raw engine write");

    let text = fx.client.simple_query("SELECT doc FROM j").await;
    let rows_seen = match &text {
        Ok(messages) => messages
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
            .count(),
        Err(_) => 0,
    };
    assert!(
        text.is_err(),
        "a corrupt cell must fail the query, not read as NULL"
    );
    assert_eq!(rows_seen, 0);
    let binary = fx.client.query("SELECT doc FROM j", &[]).await;
    assert!(binary.is_err(), "and in the binary format");
}
