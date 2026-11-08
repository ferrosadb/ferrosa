//! The DDL and catalog statements Oban's stock migrations send (W3 of the Oban
//! programme; `specs/todo/postgres-frontend/oban-compat.md`), driven through a
//! real driver (`tokio-postgres`) against the real server.
//!
//! `client.execute(sql, &[])` runs a statement over the EXTENDED protocol
//! (Parse/Bind/Describe/Execute/Sync), which is what Ecto uses for every
//! migration statement; `batch_execute` runs it over the simple protocol. The
//! server's DDL executor is `ClusterDdl` over `DdlPath::Direct`, the same
//! schema-change path CQL DDL takes on a standalone node.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use ferrosa_cluster::ddl_path::DdlPath;
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, AccordAccess, ClusterDdl, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, DeploymentMode, EnvSecretsProvider, KeyspaceMetadata, PasswordHasher,
    PasswordPolicy, RateLimitConfig, ReplicationParams, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

struct OneRole(ScramVerifier);

impl VerifierStore for OneRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == "ferrosa_user").then(|| self.0.clone())
    }
    fn admit(&self, _user: &str) -> Result<(), String> {
        Ok(())
    }
    fn record_failure(&self, _user: &str) {}
    fn record_success(&self, user: &str) -> Result<AuthContext, String> {
        Ok(AuthContext {
            role: user.to_string(),
            is_superuser: true,
            must_change_password: false,
        })
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
    client: Client,
    schema: Arc<Schema>,
    _dir: tempfile::TempDir,
}

async fn connect(port: u16) -> Client {
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
    client
}

fn public_keyspace() -> KeyspaceMetadata {
    KeyspaceMetadata {
        name: "public".to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
        },
    }
}

/// A server whose only user keyspace is `public` (the default schema), with no tables.
async fn start() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).expect("engine"));
    engine
        .register_system_tables()
        .expect("system tables register");
    let schema = Arc::new(Schema::new(schema_config()).expect("schema bootstraps"));
    let auth = AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    };
    schema
        .create_keyspace(public_keyspace(), &auth)
        .expect("create keyspace");
    let path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
        schema: schema.clone(),
        engine: engine.clone(),
    }));
    let ctx = Arc::new(QueryContext {
        cluster_read: Default::default(),
        engine: engine.clone(),
        schema: schema.clone(),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: Some(Arc::new(ClusterDdl::new(path))),
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
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
    let client = connect(port).await;
    Fixture {
        client,
        schema,
        _dir: dir,
    }
}

/// The SQLSTATE of a failed statement.
fn code<T: std::fmt::Debug>(result: Result<T, tokio_postgres::Error>) -> String {
    let error = result.expect_err("the statement must fail");
    error
        .code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| panic!("error carries no SQLSTATE: {error:?}"))
}

/// The text cells of every row a simple query returns.
async fn rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed: {sql}: {error:?}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn table_exists(fx: &Fixture, table: &str) -> bool {
    fx.schema
        .snapshot()
        .tables
        .contains_key(&("public".to_string(), table.to_string()))
}

const SCHEMA_MIGRATIONS: &str = "CREATE TABLE IF NOT EXISTS \"schema_migrations\" \
     (\"version\" bigint, \"inserted_at\" timestamp(0), PRIMARY KEY (\"version\"))";

// ---------------------------------------------------------------------------
// 1. DDL over the extended protocol (census statement 3)
// ---------------------------------------------------------------------------

/// Ecto creates its migrations table with an unnamed prepared statement. Parse of DDL
/// used to be refused (`0A000 only SELECT and INSERT/UPDATE/DELETE …`).
#[tokio::test]
async fn create_table_runs_over_the_extended_protocol() {
    let fx = start().await;
    let affected = fx
        .client
        .execute(SCHEMA_MIGRATIONS, &[])
        .await
        .expect("CREATE TABLE over Parse/Bind/Execute");
    assert_eq!(affected, 0, "DDL reports no row count");
    assert!(table_exists(&fx, "schema_migrations"));
    // IF NOT EXISTS over an existing table succeeds again, over the same path.
    fx.client
        .execute(SCHEMA_MIGRATIONS, &[])
        .await
        .expect("IF NOT EXISTS is idempotent");
    // And the table is usable.
    fx.client
        .execute(
            "INSERT INTO schema_migrations (version, inserted_at) VALUES ($1, $2)",
            &[&20_240_101_i64, &std::time::SystemTime::now()],
        )
        .await
        .expect("insert into the new table");
    assert_eq!(
        rows(&fx.client, "SELECT version FROM schema_migrations").await,
        vec![vec![Some("20240101".to_string())]]
    );
}

/// A DDL statement cannot carry `$n` parameters (PostgreSQL refuses them too); the refusal
/// is a parse-time error, not a silent drop of the parameter.
#[tokio::test]
async fn extended_ddl_refuses_a_parameter() {
    let fx = start().await;
    let error = code(fx.client.prepare("DROP TABLE $1").await);
    assert_eq!(error, "42601");
    assert!(!table_exists(&fx, "schema_migrations"));
}
