//! PG DDL `CREATE TABLE [IF NOT EXISTS]` end to end (T-132a, D10, D24).
//!
//! A real driver (`tokio-postgres`) runs the DDL against the real server. The
//! server's `ddl` executor is `ClusterDdl` over `DdlPath::Direct`: the same
//! schema-change path CQL `CREATE TABLE` takes in standalone mode. The tests
//! then read the table back through PG and through the schema registry (what
//! CQL `DESCRIBE` reads).
//!
//! NOTICE is not supported by this front-end (there is no `NoticeResponse`
//! message), so `IF NOT EXISTS` on an existing table succeeds silently.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use ferrosa_cluster::ddl_path::DdlPath;
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, ClusterDdl, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, ColumnKind, DeploymentMode, EnvSecretsProvider, KeyspaceMetadata,
    PasswordHasher, PasswordPolicy, RateLimitConfig, ReplicationParams, Schema, SchemaConfig,
    TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};

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
        write_verify: false,
    }
}

struct Fixture {
    client: tokio_postgres::Client,
    schema: Arc<Schema>,
    _dir: tempfile::TempDir,
}

/// A server whose only keyspace is `public` (the session's default schema) and
/// which has no tables: every table in a test comes from PG DDL.
async fn start() -> Fixture {
    start_with(true).await
}

/// As [`start`]; `with_public = false` leaves the default keyspace uncreated.
async fn start_with(with_public: bool) -> Fixture {
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
    if with_public {
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
    }
    let path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
        schema: schema.clone(),
        engine: engine.clone(),
    }));
    let ctx = Arc::new(QueryContext {
        engine,
        schema: schema.clone(),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord_committer: None,
        ddl: Some(Arc::new(ClusterDdl::new(path))),
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
    Fixture {
        client,
        schema,
        _dir: dir,
    }
}

/// The SQLSTATE of a failed statement.
fn sqlstate(result: Result<(), tokio_postgres::Error>) -> String {
    let error = result.expect_err("the statement must fail");
    error
        .as_db_error()
        .map(|db| db.code().code().to_string())
        .unwrap_or_else(|| format!("not a database error: {error}"))
}

fn message(result: Result<(), tokio_postgres::Error>) -> String {
    let error = result.expect_err("the statement must fail");
    error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| format!("not a database error: {error}"))
}

/// (name, kind, position, cql type) of every column, in declared order.
fn columns_of(schema: &Schema, table: &str) -> Vec<(String, ColumnKind, i32, String)> {
    let snapshot = schema.snapshot();
    let meta = snapshot
        .tables
        .get(&("public".to_string(), table.to_string()))
        .unwrap_or_else(|| panic!("public.{table} is not in the schema registry"));
    meta.columns
        .values()
        .map(|c| (c.name.clone(), c.kind, c.position, c.column_type.clone()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_round_trip_catalog() {
    let fx = start().await;
    fx.client
        .batch_execute(
            "CREATE TABLE users (id bigint NOT NULL, name text, age integer, \
             active boolean, score double precision, PRIMARY KEY (id))",
        )
        .await
        .expect("CREATE TABLE executes");

    // The schema registry (what CQL DESCRIBE reads) agrees on every column type.
    let cols = columns_of(&fx.schema, "users");
    let want: Vec<(&str, ColumnKind, &str)> = vec![
        ("id", ColumnKind::PartitionKey, "bigint"),
        ("name", ColumnKind::Regular, "text"),
        ("age", ColumnKind::Regular, "int"),
        ("active", ColumnKind::Regular, "boolean"),
        ("score", ColumnKind::Regular, "double"),
    ];
    let got: Vec<(&str, ColumnKind, &str)> = cols
        .iter()
        .map(|(n, k, _, t)| (n.as_str(), *k, t.as_str()))
        .collect();
    assert_eq!(got, want);

    // INSERT and SELECT through PG return the row.
    fx.client
        .batch_execute(
            "INSERT INTO users (id, name, age, active, score) VALUES (7, 'ada', 36, true, 1.5)",
        )
        .await
        .expect("INSERT into the table PG created");
    let rows = fx
        .client
        .query("SELECT id, name, age, active, score FROM users", &[])
        .await
        .expect("SELECT from the table PG created");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 7);
    assert_eq!(rows[0].get::<_, &str>(1), "ada");
    assert_eq!(rows[0].get::<_, i32>(2), 36);
    assert!(rows[0].get::<_, bool>(3));
    assert_eq!(rows[0].get::<_, f64>(4), 1.5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_key_mapping_and_if_not_exists() {
    let fx = start().await;

    // Single-column key: the partition key.
    fx.client
        .batch_execute("CREATE TABLE one (id uuid PRIMARY KEY, v text)")
        .await
        .expect("single-column key");
    let one = columns_of(&fx.schema, "one");
    assert_eq!(one[0].1, ColumnKind::PartitionKey);
    assert_eq!(one[1].1, ColumnKind::Regular);

    // Composite key: first column partition, the rest clustering, in order.
    fx.client
        .batch_execute("CREATE TABLE comp (a int, b int, c int, v text, PRIMARY KEY (a, b, c))")
        .await
        .expect("composite key");
    let comp = columns_of(&fx.schema, "comp");
    assert_eq!(
        comp.iter().map(|c| (c.1, c.2)).collect::<Vec<_>>(),
        vec![
            (ColumnKind::PartitionKey, 0),
            (ColumnKind::Clustering, 0),
            (ColumnKind::Clustering, 1),
            (ColumnKind::Regular, 0),
        ]
    );

    // IF NOT EXISTS twice succeeds; the second changes nothing.
    fx.client
        .batch_execute("CREATE TABLE IF NOT EXISTS twice (id int PRIMARY KEY)")
        .await
        .expect("first IF NOT EXISTS creates");
    fx.client
        .batch_execute("CREATE TABLE IF NOT EXISTS twice (id int PRIMARY KEY, extra text)")
        .await
        .expect("second IF NOT EXISTS is a no-op success");
    assert_eq!(columns_of(&fx.schema, "twice").len(), 1);

    // A duplicate without IF NOT EXISTS is 42P07.
    let dup = fx
        .client
        .batch_execute("CREATE TABLE twice (id int PRIMARY KEY)")
        .await;
    assert_eq!(sqlstate(dup), "42P07");

    // An unknown type is 42704 and names the type.
    let unknown = fx
        .client
        .batch_execute("CREATE TABLE bad (id int PRIMARY KEY, v timestamptz)")
        .await;
    let msg = message(unknown);
    assert!(msg.contains("timestamptz"), "message names the type: {msg}");
    let unknown = fx
        .client
        .batch_execute("CREATE TABLE bad (id int PRIMARY KEY, v timestamptz)")
        .await;
    assert_eq!(sqlstate(unknown), "42704");
    assert!(
        !fx.schema
            .snapshot()
            .tables
            .contains_key(&("public".to_string(), "bad".to_string())),
        "a refused CREATE TABLE creates nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_jsonb_is_refused_until_the_engine_type_exists() {
    let fx = start().await;
    for ty in ["jsonb", "json"] {
        let result = fx
            .client
            .batch_execute(&format!("CREATE TABLE j (id int PRIMARY KEY, doc {ty})"))
            .await;
        let error = result.expect_err("jsonb columns are refused");
        let db = error.as_db_error().expect("db error");
        assert_eq!(db.code().code(), "0A000");
        assert!(
            db.message().contains("jsonb"),
            "names jsonb: {}",
            db.message()
        );
    }
    assert!(!fx
        .schema
        .snapshot()
        .tables
        .contains_key(&("public".to_string(), "j".to_string())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_named_refusals_survive_end_to_end() {
    let fx = start().await;
    let cases = [
        // (sql, expected sqlstate)
        ("CREATE TABLE t (id int PRIMARY KEY, v int UNIQUE)", "0A000"),
        ("CREATE TABLE t (id int, v int)", "0A000"),
        (
            "CREATE TABLE t (id int PRIMARY KEY, v int DEFAULT 1)",
            "0A000",
        ),
        (
            "CREATE TABLE t (id int PRIMARY KEY, v int CHECK (v > 0))",
            "0A000",
        ),
        ("CREATE TABLE nosuch.t (id int PRIMARY KEY)", "0A000"),
        ("CREATE TABLE t (id int PRIMARY KEY, v money)", "42704"),
        ("CREATE TABLE t (id int PRIMARY KEY, id text)", "42701"),
    ];
    for (sql, code) in cases {
        let result = fx.client.batch_execute(sql).await;
        assert_eq!(sqlstate(result), code, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_without_the_keyspace_is_3f000() {
    let fx = start_with(false).await;
    let result = fx
        .client
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY)")
        .await;
    assert_eq!(sqlstate(result), "3F000");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_inside_a_transaction_block_is_refused() {
    let fx = start().await;
    fx.client.batch_execute("BEGIN").await.expect("BEGIN");
    let result = fx
        .client
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY)")
        .await;
    assert_eq!(sqlstate(result), "25001");
    fx.client.batch_execute("ROLLBACK").await.expect("ROLLBACK");
    assert!(!fx
        .schema
        .snapshot()
        .tables
        .contains_key(&("public".to_string(), "t".to_string())));
}
