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
use ferrosa_postgres::{server, AccordAccess, ClusterDdl, QueryContext};
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
use tokio_postgres::types::Type;
use tokio_postgres::{Config, NoTls};

// t_6b946676: stock pgbench timestamp compatibility, through the real wire path.
mod timestamp_regression {
    use super::{start, Duration, Fixture, Type};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_postgres::{Client, SimpleQueryMessage};

    async fn history() -> Fixture {
        let fixture = start().await;
        fixture
            .client
            .batch_execute(
                "CREATE TABLE pgbench_history \
                 (tid int, bid int, aid int, delta int, mtime timestamp, filler char(22))",
            )
            .await
            .expect("create stock pgbench history table");
        fixture
    }

    fn epoch_micros() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    }

    async fn timestamps(client: &Client, sql: &str) -> Vec<i64> {
        client
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("timestamp query failed: {sql}: {error:?}"))
            .into_iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => Some(
                    chrono::NaiveDateTime::parse_from_str(
                        row.get(0).expect("timestamp must not be NULL"),
                        "%Y-%m-%d %H:%M:%S%.f",
                    )
                    .expect("valid PostgreSQL timestamp text")
                    .and_utc()
                    .timestamp_micros(),
                ),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn prepared_select_uses_execute_time_and_autocommit_refreshes() {
        let fixture = start().await;
        let client = &fixture.client;
        let prepared = client
            .prepare("SELECT CURRENT_TIMESTAMP, now()")
            .await
            .unwrap();
        assert!(prepared
            .columns()
            .iter()
            .all(|c| c.type_() == &Type::TIMESTAMPTZ));
        tokio::time::sleep(Duration::from_millis(30)).await;
        let before = SystemTime::now();
        let first = client.query_one(&prepared, &[]).await.unwrap();
        let started: SystemTime = first.get(0);
        assert!(started >= before && started <= SystemTime::now());
        assert_eq!(started, first.get::<_, SystemTime>(1));
        tokio::time::sleep(Duration::from_millis(30)).await;
        let second: SystemTime = client.query_one(&prepared, &[]).await.unwrap().get(0);
        assert!(second > started);
        let messages = client
            .simple_query("SELECT CURRENT_TIMESTAMP, now()")
            .await
            .unwrap();
        let row = messages
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .unwrap();
        assert_eq!(row.get(0), row.get(1));
        assert!(row.get(0).unwrap().ends_with("+00"));
    }

    #[tokio::test]
    async fn timestamp_update_uses_transaction_clock_and_unknown_functions_fail() {
        let fixture = start().await;
        let client = &fixture.client;
        client
            .batch_execute("CREATE TABLE clock_updates (id int PRIMARY KEY, ts timestamp)")
            .await
            .unwrap();
        client.batch_execute("BEGIN").await.unwrap();
        let first = timestamps(
            client,
            "INSERT INTO clock_updates (id, ts) VALUES (1, now()) RETURNING ts",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        client
            .batch_execute("UPDATE clock_updates SET ts = CURRENT_TIMESTAMP WHERE id = 1")
            .await
            .unwrap();
        client.batch_execute("COMMIT").await.unwrap();
        assert_eq!(
            timestamps(client, "SELECT ts FROM clock_updates WHERE id = 1").await,
            vec![first[0] / 1000 * 1000]
        );
        for sql in [
            "SELECT clock_timestamp()",
            "INSERT INTO clock_updates (id, ts) VALUES (2, clock_timestamp())",
        ] {
            let error = client.simple_query(sql).await.unwrap_err();
            assert_eq!(error.code().unwrap().code(), "0A000");
        }
    }

    #[tokio::test]
    async fn stock_pgbench_insert_current_timestamp_persists_a_current_timestamp() {
        let fixture = history().await;
        let before = epoch_micros();
        let messages = fixture
            .client
            .simple_query(
                "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) \
                 VALUES (1, 1, 1, 100, CURRENT_TIMESTAMP)",
            )
            .await
            .expect("stock pgbench CURRENT_TIMESTAMP INSERT must succeed");
        let after = epoch_micros();
        assert!(messages
            .iter()
            .any(|message| matches!(message, SimpleQueryMessage::CommandComplete(1))));
        let stored = timestamps(&fixture.client, "SELECT mtime FROM pgbench_history").await;
        assert_eq!(stored.len(), 1);
        // The current storage bridge persists milliseconds. This bound detects
        // NULL/epoch/fabricated values without claiming microsecond storage fidelity.
        assert!((before / 1000 * 1000..=after).contains(&stored[0]));
    }

    #[tokio::test]
    async fn current_timestamp_is_begin_time_across_rows_and_statements() {
        let fixture = history().await;
        let client = &fixture.client;
        let before_begin = epoch_micros();
        client.batch_execute("BEGIN").await.unwrap();
        let after_begin = epoch_micros();
        // A delay before the FIRST use rejects lazy initialization at INSERT.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let first = timestamps(
            client,
            "INSERT INTO pgbench_history (tid, mtime) \
             VALUES (1, CURRENT_TIMESTAMP), (2, CURRENT_TIMESTAMP) RETURNING mtime",
        )
        .await;
        assert_eq!(first.len(), 2);
        assert_eq!(first[0], first[1], "one transaction time for every row");
        assert!((before_begin..=after_begin).contains(&first[0]));
        tokio::time::sleep(Duration::from_millis(30)).await;
        let second = timestamps(
            client,
            "INSERT INTO pgbench_history (tid, mtime) \
             VALUES (3, CURRENT_TIMESTAMP) RETURNING mtime",
        )
        .await;
        assert_eq!(
            second,
            vec![first[0]],
            "stable across separate wire queries"
        );
        client.batch_execute("COMMIT").await.unwrap();
        let stored = timestamps(client, "SELECT mtime FROM pgbench_history").await;
        assert_eq!(stored.len(), 3);
        assert!(stored.iter().all(|value| *value == first[0] / 1000 * 1000));
    }

    #[tokio::test]
    async fn prepared_now_alias_uses_execution_transaction_and_resets_after_end() {
        let fixture = history().await;
        let client = &fixture.client;
        // Parse/Describe happen before BEGIN: now() must not freeze at prepare time.
        let insert = client
            .prepare("INSERT INTO pgbench_history (tid, mtime) VALUES ($1, now()) RETURNING mtime")
            .await
            .expect("prepare now() INSERT");
        tokio::time::sleep(Duration::from_millis(30)).await;
        let before_begin = epoch_micros();
        client.batch_execute("BEGIN").await.unwrap();
        let after_begin = epoch_micros();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let row = client
            .query_one(&insert, &[&1_i32])
            .await
            .expect("execute now() INSERT");
        assert_eq!(row.columns()[0].type_(), &Type::TIMESTAMP);
        let first: SystemTime = row.get(0);
        let first_micros = first.duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        assert!((before_begin..=after_begin).contains(&first_micros));
        tokio::time::sleep(Duration::from_millis(30)).await;
        let current = timestamps(
            client,
            "INSERT INTO pgbench_history (tid, mtime) \
             VALUES (2, CURRENT_TIMESTAMP) RETURNING mtime",
        )
        .await;
        assert_eq!(
            current,
            vec![first_micros],
            "now() aliases CURRENT_TIMESTAMP"
        );
        client.batch_execute("COMMIT").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let second: SystemTime = client.query_one(&insert, &[&3_i32]).await.unwrap().get(0);
        assert!(second > first, "COMMIT must clear the previous timestamp");

        client.batch_execute("BEGIN").await.unwrap();
        let rolled_back: SystemTime = client.query_one(&insert, &[&4_i32]).await.unwrap().get(0);
        client.batch_execute("ROLLBACK").await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let after_rollback: SystemTime = client.query_one(&insert, &[&5_i32]).await.unwrap().get(0);
        assert!(
            after_rollback > rolled_back,
            "ROLLBACK must clear the timestamp"
        );
        let stored = timestamps(client, "SELECT mtime FROM pgbench_history").await;
        assert_eq!(stored.len(), 4, "rolled-back INSERT must not persist");
    }

    #[tokio::test]
    async fn select_timestamp_aliases_have_postgres_type_and_transaction_stability() {
        let fixture = start().await;
        let client = &fixture.client;
        client.batch_execute("BEGIN").await.unwrap();
        let first = client
            .query_one("SELECT CURRENT_TIMESTAMP, now()", &[])
            .await
            .expect("SELECT both transaction timestamp spellings");
        for column in first.columns() {
            assert_eq!(column.type_(), &Type::TIMESTAMPTZ);
        }
        let current: SystemTime = first.get(0);
        assert_eq!(current, first.get::<_, SystemTime>(1));
        tokio::time::sleep(Duration::from_millis(30)).await;
        let second = client
            .query_one("SELECT CURRENT_TIMESTAMP, now()", &[])
            .await
            .unwrap();
        assert_eq!(current, second.get::<_, SystemTime>(0));
        assert_eq!(current, second.get::<_, SystemTime>(1));
        client.batch_execute("ROLLBACK").await.unwrap();
    }
}

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
        cache_hot_window_secs: 900,
        write_verify: false,
    }
}

struct Fixture {
    client: tokio_postgres::Client,
    schema: Arc<Schema>,
    /// The live storage engine, so a test can wait on engine-side state (e.g. that an
    /// `ALTER`'s asynchronous index backfill has completed before issuing the workload).
    engine: Arc<StorageEngine>,
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
    // Production registers the catalog tables at boot (`ferrosa/src/main.rs:2242`). Without
    // them an `ALTER ... ADD PRIMARY KEY` cannot record its index and fails loud with
    // E58000 "table not registered: system_schema.indexes", which is a fixture gap rather
    // than a product failure.
    engine
        .register_system_tables()
        .expect("system tables register");
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
        engine,
        _dir: dir,
    }
}

/// Wait until the `<table>_pkey` secondary index is CURRENT — its backfill has completed.
///
/// `ALTER ... ADD PRIMARY KEY` records the key and builds the index over the rows it
/// flushes; that backfill runs on the engine's index-build scheduler and is therefore
/// asynchronous, so the index is briefly NOT current after the `ALTER` returns. A
/// production `pgbench` issues its workload after that backfill has finished; this makes
/// the test wait for the same condition instead of racing it (deterministically, with a
/// bounded budget rather than a fixed sleep).
async fn await_index_current(fx: &Fixture, table: &str, index: &str) {
    let table_id = ferrosa_storage::TableId::new("public", table);
    for _ in 0..500 {
        if fx.engine.index_is_current(&table_id, index) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the {table}.{index} index never became current within the wait budget");
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

/// T-161a lifted the jsonb refusal: `jsonb` and `json` (stored as jsonb, D11)
/// columns create a CQL `jsonb` column. The T-300 mode gate is REMOVED, so such
/// DDL is permitted off a standalone node too — see
/// `pg_ddl_create_table_jsonb_is_permitted_off_standalone`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_jsonb_and_json_create_jsonb_columns() {
    let fx = start().await;
    fx.client
        .batch_execute("CREATE TABLE j (id int PRIMARY KEY, a jsonb, b json)")
        .await
        .expect("jsonb and json columns are creatable");
    let cols = columns_of(&fx.schema, "j");
    let types: Vec<(&str, &str)> = cols
        .iter()
        .map(|(n, _, _, t)| (n.as_str(), t.as_str()))
        .collect();
    assert_eq!(types, vec![("id", "int"), ("a", "jsonb"), ("b", "jsonb")]);
}

/// T-300 gate REMOVED (t_57fa8a9e): off a standalone node a jsonb or json
/// column is still created as a CQL `jsonb` column — there is no mode gate.
/// The surviving constraint is the T-154a placement rule (see the PRIMARY KEY
/// case below).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_jsonb_is_permitted_off_standalone() {
    let fx = start().await;
    fx.schema
        .set_deployment_mode(ferrosa_common::deployment_mode::DeploymentMode::Cluster);
    fx.client
        .batch_execute("CREATE TABLE g (id int PRIMARY KEY, doc jsonb, j json)")
        .await
        .expect("jsonb DDL is permitted off a standalone node");
    let cols = columns_of(&fx.schema, "g");
    let types: Vec<(&str, &str)> = cols
        .iter()
        .map(|(n, _, _, t)| (n.as_str(), t.as_str()))
        .collect();
    assert_eq!(types, vec![("id", "int"), ("doc", "jsonb"), ("j", "jsonb")]);
}

/// T-154a (D3): jsonb in a PRIMARY KEY column is refused with `42P16`
/// (invalid_table_definition) naming the column. Postgres accepts it; the
/// divergence is documented (D3). Nothing is created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_jsonb_primary_key_is_invalid_table_definition() {
    let fx = start().await;
    let statements = [
        "CREATE TABLE k (doc jsonb PRIMARY KEY)",
        "CREATE TABLE k (id int, doc jsonb, PRIMARY KEY (id, doc))",
        "CREATE TABLE k (doc jsonb, v int, PRIMARY KEY (doc))",
    ];
    for sql in statements {
        let error = fx
            .client
            .batch_execute(sql)
            .await
            .expect_err("jsonb key is refused");
        let db = error.as_db_error().expect("db error");
        assert_eq!(db.code().code(), "42P16", "{sql}: {}", db.message());
        assert!(
            db.message().contains("\"doc\""),
            "names the column: {}",
            db.message()
        );
    }
    assert!(!fx
        .schema
        .snapshot()
        .tables
        .contains_key(&("public".to_string(), "k".to_string())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_named_refusals_survive_end_to_end() {
    let fx = start().await;
    // A PK-less `CREATE TABLE` is *accepted*, not refused: `ddl::plan_create_table`
    // gives it a synthetic `_sys_ck_` partition key (the "PK-less CREATE TABLE end to
    // end" item in the roadmap). It is exercised here as an accepted statement so a
    // future regression to "refused" is caught, and it is deliberately NOT in the
    // named-refusal list below.
    fx.client
        .batch_execute("CREATE TABLE pk_less (id int, v int)")
        .await
        .expect("a PK-less CREATE TABLE is accepted with a synthetic key");
    let cases = [
        // (sql, expected sqlstate)
        ("CREATE TABLE t (id int PRIMARY KEY, v int UNIQUE)", "0A000"),
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

    // A PK-less `CREATE TABLE` is NO LONGER a named refusal: it is accepted with a
    // synthesized `_sys_ck_` key (f7c03c1d), which is what lets `pgbench -i` create
    // `pgbench_accounts (aid int, bid int, abalance int, filler char(84))`. Pinned
    // positively here so the one shape that was removed from the list above stays covered.
    fx.client
        .batch_execute("CREATE TABLE pkless (id int, v int)")
        .await
        .expect("a PK-less CREATE TABLE is accepted with a synthesized key");
    let cols = columns_of(&fx.schema, "pkless");
    assert!(
        cols.iter()
            .any(|(name, kind, _, _)| name == "_sys_ck_" && *kind == ColumnKind::PartitionKey),
        "the table carries a synthesized `_sys_ck_` partition key: {cols:?}"
    );
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

// ---------------------------------------------------------------------------
// Test list: rows -> index -> the index must be USED by the queries.
//
// A PG table with no PRIMARY KEY gets the synthetic `_sys_ck_` storage key, so a later
// `ALTER TABLE ... ADD PRIMARY KEY (aid)` cannot become the storage key: it records the
// DECLARED key and, because the storage key is shadowed, must build a SECONDARY index over
// `aid`. From that point the row the client names is reachable only through that index —
// `aid` cannot address a partition by itself.
//
// - [x] rows land in a PK-less table
// - [x] ALTER ADD PRIMARY KEY indexes the rows already written (not just future ones)
// - [x] SELECT by the declared key finds the row (asserted by VALUE)
// - [x] UPDATE by the declared key hits exactly that row (VALUE + untouched sibling)
// - [ ] DELETE by the declared key removes exactly that row
// - [ ] rows inserted AFTER the index exists are findable too (index maintained on write)
// - [x] a WHERE on a column with NO index still WORKS: fall back to a scan, never refuse
//       (user ruling 2026-10-11). The scan must still APPLY the predicate — a scan that
//       skipped it would rewrite every row — so the sibling row is asserted unchanged.
//       This REVERSES the earlier "refuse a non-key column rather than scan" decision
//       recorded at ddl.rs:449-452 for the FK check and in this feature item's own
//       decision section; both need updating in the same change.
// - [x] a scan fallback is LOGGED LOUDLY (user ruling 2026-10-11) — never silent. Nothing in
//       ferrosa-postgres/src logs a scan today (zero matches), so the SELECT scan and the DML
//       fallback are both silent right now. Copy the precedent already in the tree:
//       ferrosa-sparql/src/executor.rs:641-646 warns on its scan fallback, naming the index it
//       could not use and that it is streaming a full scan with filtering.
// ---------------------------------------------------------------------------

/// `pgbench -i`'s shape, end to end: rows first, the primary key afterwards.
///
/// RED as written: `ALTER` reports success and the index machinery exists, but nothing in
/// the PG front-end consults it — `query.rs` has no `_pkey` and no `pg_key::of` — so
/// tpcb's row statements cannot address the row the client names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_primary_key_becomes_a_secondary_index_the_queries_then_use() {
    let fx = start().await;

    // No PRIMARY KEY at CREATE — exactly what `pgbench -i` issues, so the storage key is
    // the synthetic uuid the client never sees.
    fx.client
        .batch_execute("CREATE TABLE pbalter (aid integer, abalance integer)")
        .await
        .expect("a PK-less CREATE TABLE executes");

    // Rows land BEFORE the key exists, so the index has to cover them.
    fx.client
        .batch_execute("INSERT INTO pbalter (aid, abalance) VALUES (1, 100), (2, 200), (7, 700)")
        .await
        .expect("rows land in a PK-less table");

    // `pgbench -i` then adds the key: for us a secondary index over `aid`.
    fx.client
        .batch_execute("ALTER TABLE pbalter ADD PRIMARY KEY (aid)")
        .await
        .expect("ALTER ADD PRIMARY KEY executes");

    // The ALTER returns before its backfill lands: the index is briefly NOT current, and a
    // read through it fails LOUD ("58000 ... not current; refusing to return incomplete
    // results while index backfill is pending") rather than answering incompletely. pgbench
    // issues its workload after that backfill; wait for the same condition, deterministically.
    //
    // OPEN ITEM, not papered over: a client that queries by the declared key without waiting
    // gets that 58000 straight after a successful ALTER. Either ALTER should await its own
    // backfill, or the front-end should retry — worth deciding before this ships.
    await_index_current(&fx, "pbalter", "pbalter_pkey").await;

    // tpcb command 1: SELECT by the declared key.
    let selected = fx
        .client
        .query("SELECT abalance FROM pbalter WHERE aid = 2", &[])
        .await
        .unwrap_or_else(|e| panic!("SELECT by the declared key must be served: {e}"));
    assert_eq!(selected.len(), 1, "exactly one row is named by aid = 2");
    let balance: i32 = selected[0].get(0);
    assert_eq!(balance, 200, "the index over aid must find the row");

    // tpcb command 5: UPDATE by the declared key.
    fx.client
        .execute(
            "UPDATE pbalter SET abalance = abalance + 5 WHERE aid = 2",
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("UPDATE by the declared key must not be refused: {e}"));

    let after = fx
        .client
        .query("SELECT abalance FROM pbalter WHERE aid = 2", &[])
        .await
        .expect("read back after the update");
    assert_eq!(
        after[0].get::<_, i32>(0),
        205,
        "the UPDATE must hit the row the index names"
    );

    // The rows the predicate did NOT name are untouched — values, not counts.
    let sibling = fx
        .client
        .query("SELECT abalance FROM pbalter WHERE aid = 7", &[])
        .await
        .expect("read the sibling row");
    assert_eq!(
        sibling[0].get::<_, i32>(0),
        700,
        "the row not named by the predicate must not change"
    );
}

/// User ruling (2026-10-11): a WHERE on a column with NO index still WORKS — it falls back
/// to a scan rather than being refused. The scan must also still APPLY the predicate: one
/// that skipped it would rewrite every row, so the sibling row is asserted here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_where_on_a_column_with_no_index_falls_back_to_a_filtering_scan() {
    let fx = start().await;
    fx.client
        .batch_execute("CREATE TABLE pbscan (aid integer, abalance integer)")
        .await
        .expect("a PK-less CREATE TABLE executes");
    fx.client
        .batch_execute("INSERT INTO pbscan (aid, abalance) VALUES (1, 100), (2, 200)")
        .await
        .expect("rows land");

    // No ALTER: `aid` has no index at all, so nothing can address the row but a scan.
    let selected = fx
        .client
        .query("SELECT abalance FROM pbscan WHERE aid = 2", &[])
        .await
        .unwrap_or_else(|e| panic!("SELECT with no index must be served by a scan: {e}"));
    assert_eq!(
        selected.len(),
        1,
        "the scan must apply the predicate, not return every row"
    );
    assert_eq!(
        selected[0].get::<_, i32>(0),
        200,
        "the scanned row is the one named"
    );

    // The same fallback must serve DML — a refusal is not an option.
    fx.client
        .execute(
            "UPDATE pbscan SET abalance = abalance + 5 WHERE aid = 2",
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("UPDATE with no index must not be refused: {e}"));

    let sibling = fx
        .client
        .query("SELECT abalance FROM pbscan WHERE aid = 1", &[])
        .await
        .expect("read the sibling");
    assert_eq!(
        sibling[0].get::<_, i32>(0),
        100,
        "a non-indexed UPDATE must not touch rows the predicate did not name"
    );
}

/// The DELETE twin of the declared-key UPDATE.
///
/// LABEL, honestly: this was written AFTER the implementation landed, so it is a regression
/// guard and not a red. The behaviour it pins was `0A000` before the change — the UPDATE twin
/// failed on exactly the same gate (`query.rs:3653` for DELETE, `:3226` for UPDATE), so the
/// pre-fix outcome is evidenced rather than assumed. To make it a true red, revert the
/// `execute_delete` half alone and watch this fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_by_the_declared_key_removes_exactly_that_row() {
    let fx = start().await;
    fx.client
        .batch_execute("CREATE TABLE pbdel (aid integer, abalance integer)")
        .await
        .expect("a PK-less CREATE TABLE executes");
    fx.client
        .batch_execute("INSERT INTO pbdel (aid, abalance) VALUES (1, 100), (2, 200), (7, 700)")
        .await
        .expect("rows land");
    fx.client
        .batch_execute("ALTER TABLE pbdel ADD PRIMARY KEY (aid)")
        .await
        .expect("ALTER ADD PRIMARY KEY executes");
    await_index_current(&fx, "pbdel", "pbdel_pkey").await;

    fx.client
        .execute("DELETE FROM pbdel WHERE aid = 2", &[])
        .await
        .unwrap_or_else(|e| panic!("DELETE by the declared key must not be refused: {e}"));

    // Values, not a count: the SURVIVING identities are what a wrong-row delete cannot fake.
    let survivors = fx
        .client
        .query("SELECT aid FROM pbdel", &[])
        .await
        .expect("read the survivors");
    let mut ids: Vec<i32> = survivors.iter().map(|r| r.get::<_, i32>(0)).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 7],
        "exactly the row the predicate named is gone"
    );
}

/// Invariants for tpcb's statements, stated BEFORE the assertions. Each must hold AFTER the
/// statement runs — not merely "the statement returned no error", which is the check that
/// let a live UPDATE pass: it returned in 0 s, logged no error, and changed nothing.
///
///  I1  a load that reports success lands EVERY row: `count(*)` is exactly N. (Live at
///      scale 10 the census was exactly 1000000|100|10, so the invariant is measured.)
///  I2  a point lookup by the DECLARED key returns the named row, compared by VALUE — the
///      key must address the row, not a prefix of the table.
///  I3  that lookup does NOT fall back to a scan. The observable is the warning
///      `ferrosa_postgres::query` emits when it scans — the loud log added for exactly this
///      purpose — so its ABSENCE is evidence the index was used. Paired with a POSITIVE
///      CONTROL below, without which the absence assertion would be vacuous.
///  I4  after `UPDATE ... WHERE <declared key> = k`, reading k back returns the UPDATED
///      value and every other row is unchanged (identity, never a count).
#[test]
fn the_tpcb_statement_invariants_hold_after_each_statement() {
    let test_name = "the_tpcb_statement_invariants_hold_after_each_statement";
    with_captured_logs(test_name, |logs| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        runtime.block_on(async {
            let fx = start().await;

            // I1 — the load must land every row, at test scale.
            const N: i64 = 64;
            fx.client
                .batch_execute("CREATE TABLE inv (aid integer, abalance integer)")
                .await
                .expect("a PK-less CREATE TABLE executes");
            for aid in 1..=N {
                fx.client
                    .execute(
                        "INSERT INTO inv (aid, abalance) VALUES ($1, $2)",
                        &[&(aid as i32), &100i32],
                    )
                    .await
                    .expect("row lands");
            }
            let counted: i64 = fx
                .client
                .query_one("SELECT count(*) FROM inv", &[])
                .await
                .expect("count(*) is queryable")
                .get(0);
            assert_eq!(counted, N, "I1: every inserted row must be countable");

            // I6 (strengthened here): the `count` column is advertised as int8,
            // so the `i64` read above deserializes — asserted directly on the wire
            // OID, not inferred from the cast succeeding.
            let typed = fx
                .client
                .query_one("SELECT count(*) FROM inv", &[])
                .await
                .expect("count(*) is queryable for its type");
            assert_eq!(
                typed.columns()[0].type_(),
                &Type::INT8,
                "I6: count(*) must be advertised as int8 (a client reads i64)"
            );

            // The declared key arrives by ALTER: `aid` becomes a SECONDARY index and the
            // storage key stays the synthetic `_sys_ck_`.
            fx.client
                .batch_execute("ALTER TABLE inv ADD PRIMARY KEY (aid)")
                .await
                .expect("ALTER ADD PRIMARY KEY executes");
            await_index_current(&fx, "inv", "inv_pkey").await;

            // I2 + I3 — the row comes back, and it comes back through the index.
            let read: i32 = fx
                .client
                .query_one("SELECT abalance FROM inv WHERE aid = 42", &[])
                .await
                .expect("I2: the named row must come back")
                .get(0);
            assert_eq!(
                read, 100,
                "I2: the lookup must return the row the key names"
            );
            assert!(
                !logs.text().contains("names no indexed column"),
                "I3: a lookup by the declared key must not scan. captured:\n{}",
                logs.text()
            );

            // I4 — the update takes effect, and only on the named row.
            fx.client
                .execute("UPDATE inv SET abalance = abalance + 5 WHERE aid = 42", &[])
                .await
                .expect("I4: UPDATE by the declared key must not be refused");
            let after: i32 = fx
                .client
                .query_one("SELECT abalance FROM inv WHERE aid = 42", &[])
                .await
                .expect("read back after the update")
                .get(0);
            assert_eq!(
                after, 105,
                "I4: the UPDATE must be visible when the row is read back"
            );
            let sibling: i32 = fx
                .client
                .query_one("SELECT abalance FROM inv WHERE aid = 43", &[])
                .await
                .expect("read the sibling")
                .get(0);
            assert_eq!(
                sibling, 100,
                "I4: rows the predicate did not name must be unchanged"
            );

            // POSITIVE CONTROL for I3: the same capture, on a column with NO index, MUST see
            // the warning. Without this, "the warning was absent" proves nothing about the
            // index — it would also be absent if the capture were broken.
            fx.client
                .batch_execute("CREATE TABLE inv2 (aid integer, abalance integer)")
                .await
                .expect("a PK-less CREATE TABLE executes");
            fx.client
                .execute("INSERT INTO inv2 (aid, abalance) VALUES (1, 100)", &[])
                .await
                .expect("row lands");
            fx.client
                .execute("UPDATE inv2 SET abalance = abalance + 5 WHERE aid = 1", &[])
                .await
                .expect("a non-indexed UPDATE must not be refused");
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while std::time::Instant::now() < deadline
                && !logs.text().contains("names no indexed column")
            {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                logs.text().contains("names no indexed column"),
                "positive control: a non-indexed WHERE MUST log the scan warning, otherwise \
                 I3's absence assertion is vacuous. captured:\n{}",
                logs.text()
            );
        });
    });
}
/// A `MakeWriter` that collects everything logged into a shared buffer, so a test can
/// assert a warning was actually emitted. The same pattern ferrosa-cql's router tests and
/// ferrosa-storage's engine tests use for their scan/index warnings.
#[derive(Clone, Default)]
struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Isolate each capture from the test harness's other subscribers and callsites.
/// In tracing-core 0.1.36, the single-dispatch registration fast path consults the
/// emitting thread's default. A first use on an unsubscribed thread can cache
/// `Interest::never()` even while another thread has a scoped capture installed.
/// A dedicated child with a global default covers every server task and first use.
fn with_captured_logs(test_name: &str, test: impl FnOnce(CapturedLogs)) {
    const CHILD: &str = "FERROSA_PG_DDL_LOG_CAPTURE_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(test_name) {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("one global subscriber per capture child");
        test(logs);
    } else {
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
            .env(CHILD, test_name)
            .output()
            .expect("run isolated log capture");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && stdout.contains("running 1 test"),
            "isolated capture {test_name} failed ({}):\n{stdout}\n{stderr}",
            output.status
        );
    }
}

/// Force the first-use interleaving that broke the SQL warning capture, then
/// verify that an overlapping thread-local subscriber neither steals nor leaks events.
#[test]
fn log_capture_survives_unsubscribed_first_use_and_concurrent_subscriber() {
    fn emit(label: &str) {
        tracing::warn!(label, "capture isolation control");
    }
    with_captured_logs(
        "log_capture_survives_unsubscribed_first_use_and_concurrent_subscriber",
        |logs| {
            std::thread::spawn(|| emit("first-use-without-local-subscriber"))
                .join()
                .expect("first-use thread");
            emit("capture-after-first-use");
            assert!(
                logs.text().contains("capture-after-first-use"),
                "an unsubscribed first use must not disable later capture: {}",
                logs.text()
            );
            assert!(logs.text().contains("first-use-without-local-subscriber"));

            let competing_logs = CapturedLogs::default();
            let ready = std::sync::Barrier::new(2);
            let done = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let subscriber = tracing_subscriber::fmt()
                        .with_writer(competing_logs.clone())
                        .with_max_level(tracing::Level::WARN)
                        .with_ansi(false)
                        .finish();
                    tracing::subscriber::with_default(subscriber, || {
                        emit("competing-subscriber-only");
                        ready.wait();
                        done.wait();
                    });
                });
                ready.wait();
                emit("capture-during-competing-subscriber");
                done.wait();
            });
            assert!(logs.text().contains("capture-during-competing-subscriber"));
            assert!(!logs.text().contains("competing-subscriber-only"));
            assert!(competing_logs.text().contains("competing-subscriber-only"));
            assert!(!competing_logs
                .text()
                .contains("capture-during-competing-subscriber"));
        },
    );
}

/// User ruling (2026-10-11): the scan fallback that serves the previous test must be
/// LOGGED LOUDLY, never silent. A silent fallback hides a missing or unusable index
/// behind a result that looks the same, so this pins the warning on the real statement
/// path: it boots a server under a capturing subscriber and asserts that an UPDATE whose
/// WHERE names no indexed column emits a warning that names the table.
///
/// Runs in its own process with a global subscriber: a thread-local subscriber cannot
/// isolate tracing's process-wide callsite interest cache from concurrent tests.
#[test]
fn a_scan_fallback_is_logged_loudly() {
    with_captured_logs("a_scan_fallback_is_logged_loudly", |logs| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        runtime.block_on(async {
            let fx = start().await;
            fx.client
                .batch_execute("CREATE TABLE pbscanlog (aid integer, abalance integer)")
                .await
                .expect("a PK-less CREATE TABLE executes");
            fx.client
                .batch_execute("INSERT INTO pbscanlog (aid, abalance) VALUES (1, 100), (2, 200)")
                .await
                .expect("rows land");
            // No ALTER: `aid` has no index, so this must scan — and say so.
            fx.client
                .execute(
                    "UPDATE pbscanlog SET abalance = abalance + 5 WHERE aid = 2",
                    &[],
                )
                .await
                .expect("UPDATE with no index must not be refused");

            // CandidateContext::rows emits the warning synchronously before the
            // UPDATE response. Awaiting execute is the synchronization boundary.
        });

        let captured = logs.text();
        assert!(
            captured.contains("pbscanlog"),
            "the scan fallback must be logged LOUDLY and name the table it scanned. \
         --- captured warnings ---\n{captured}"
        );
        assert!(
            captured.contains("full scan"),
            "the warning must say the statement is streaming a full scan with filtering. \
         --- captured warnings ---\n{captured}"
        );
    });
}

/// I6 — `count(*)` MUST be reported and encoded as `int8` (PostgreSQL's own type
/// for `count()`), so a client reading `i64` deserializes it.
///
/// RED before the fix: the front end derived `count` as `int4` (OID 23), so
/// tokio-postgres refused the value with `WrongType { postgres: Int4, rust:
/// "i64" }` — a client could not read a `count(*)` at all. The advertised OID is
/// the whole contract a client deserializes against, so it is asserted directly
/// rather than inferred from the cast succeeding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn count_star_is_reported_and_encoded_as_int8() {
    let fx = start().await;
    fx.client
        .batch_execute("CREATE TABLE cnt (aid integer)")
        .await
        .expect("a PK-less CREATE TABLE executes");
    fx.client
        .batch_execute("INSERT INTO cnt (aid) VALUES (1), (2), (3)")
        .await
        .expect("rows land");

    let row = fx
        .client
        .query_one("SELECT count(*) FROM cnt", &[])
        .await
        .expect("count(*) is queryable");
    let column = &row.columns()[0];
    assert_eq!(column.name(), "count");
    assert_eq!(
        column.type_(),
        &Type::INT8,
        "I6: count(*) must be advertised as int8, not {}",
        column.type_()
    );
    assert_eq!(column.type_().oid(), 20, "I6: int8 is OID 20");
    // The client reads in the type the advertised OID names.
    let n: i64 = row.get(0);
    assert_eq!(n, 3, "I6: a client reading i64 must deserialize the count");
}

/// I7 — a computed/aggregate column is the GENERAL case. The invariant is not
/// "count(*) is int8"; it is that every DERIVED column's advertised OID is
/// PostgreSQL's RESULT type for that aggregate, and that OID is the type its
/// bytes are encoded as, so a client reading the matching Rust type
/// deserializes. This SELECT derives several distinct aggregates through the
/// SAME derivation path (`ferrosa_sql::plan::aggregate_column`), so a
/// special-case fix for `count` alone would leave the rest unchecked.
///
/// Derived here, matching PostgreSQL exactly: `count(*)` -> int8;
/// `min`/`max` over an `integer` argument -> int4 (the argument's engine type);
/// `sum(integer)` -> int8 (PostgreSQL widens an int4 sum to int8). Each is read
/// back with the Rust type its advertised OID names — the deserialization only
/// succeeds if the OID and the bytes agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn derived_columns_advertise_the_oid_their_bytes_carry() {
    let fx = start().await;
    fx.client
        .batch_execute("CREATE TABLE aggd (aid integer, abalance integer)")
        .await
        .expect("a PK-less CREATE TABLE executes");
    fx.client
        .batch_execute("INSERT INTO aggd (aid, abalance) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("rows land");

    let row = fx
        .client
        .query_one(
            "SELECT count(*), min(aid), max(aid), sum(abalance) FROM aggd",
            &[],
        )
        .await
        .expect("derived columns are queryable");
    let columns = row.columns();
    let expected = [
        ("count", Type::INT8),
        ("min", Type::INT4),
        ("max", Type::INT4),
        ("sum", Type::INT8),
    ];
    assert_eq!(columns.len(), expected.len());
    for (i, (name, want)) in expected.iter().enumerate() {
        assert_eq!(columns[i].name(), *name, "column {i}");
        assert_eq!(
            columns[i].type_(),
            want,
            "I7: derived column {i} ({name}) must be advertised as {want}, not {}",
            columns[i].type_()
        );
    }
    // A client deserializes each column on the strength of its advertised OID.
    assert_eq!(row.get::<_, i64>(0), 3, "count");
    assert_eq!(row.get::<_, i32>(1), 1, "min");
    assert_eq!(row.get::<_, i32>(2), 3, "max");
    // sum(integer) is int8 in PostgreSQL: read back as i64.
    assert_eq!(row.get::<_, i64>(3), 60, "sum");
}

/// I6b — where PostgreSQL's result type is `numeric` (`sum(int8)`,
/// `sum(numeric)`, and every `avg` over an integer/numeric argument) the engine
/// has no numeric-valued result and the PG wire layer has no binary numeric
/// codec (`pg_types::for_column_type(Numeric).binary == false`). The front end
/// MUST fail loud (`0A000`, feature_not_supported) on the DECLARED numeric type
/// rather than hand the driver `int`/`float` bytes under OID 1700, which it would
/// decode as garbage. A silent wrong type is the one outcome that is not
/// acceptable, so the refusal is asserted through a real client, which requests
/// binary results — the format where the mislabelling would otherwise happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn numeric_typed_aggregates_fail_loud_instead_of_mislabelling_bytes() {
    let fx = start().await;
    fx.client
        .batch_execute("CREATE TABLE numagg (aid bigint, aval integer)")
        .await
        .expect("a PK-less CREATE TABLE executes");
    fx.client
        .batch_execute("INSERT INTO numagg (aid, aval) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("rows land");

    // `sum(bigint)` -> numeric in PostgreSQL, never int8 bytes under OID 1700.
    let err = fx
        .client
        .query("SELECT sum(aid) FROM numagg", &[])
        .await
        .expect_err("sum(bigint) types as numeric, which this front end cannot encode");
    assert_numeric_refused(&err, "sum(bigint)");

    // `avg(integer)` -> numeric in PostgreSQL: also refused.
    let err = fx
        .client
        .query("SELECT avg(aval) FROM numagg", &[])
        .await
        .expect_err("avg(integer) types as numeric, which this front end cannot encode");
    assert_numeric_refused(&err, "avg(integer)");
}

/// Assert the client surfaced the front end's fail-loud refusal: a server-side
/// `ErrorResponse` with SQLSTATE `0A000` whose message names `numeric`.
fn assert_numeric_refused(err: &tokio_postgres::Error, what: &str) {
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("{what}: expected a server ErrorResponse, got {err:?}"));
    assert_eq!(
        db.code(),
        &tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED,
        "{what}: the refusal must be 0A000 (feature_not_supported), not {:?}: {err}",
        db.code()
    );
    assert!(
        db.message().contains("numeric"),
        "{what}: the refusal must name the type it cannot encode: {}",
        db.message()
    );
}
