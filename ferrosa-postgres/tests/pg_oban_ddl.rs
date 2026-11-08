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
    port: u16,
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

/// A second connection to the fixture's server.
async fn connect_to(fx: &Fixture) -> Client {
    connect(fx.port).await
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
        port,
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

// ---------------------------------------------------------------------------
// 2. Transactional DDL (census statements 4–7 and every migration)
// ---------------------------------------------------------------------------

fn columns_of(fx: &Fixture, table: &str) -> Vec<String> {
    fx.schema
        .snapshot()
        .tables
        .get(&("public".to_string(), table.to_string()))
        .map(|meta| meta.columns.keys().cloned().collect())
        .unwrap_or_default()
}

/// DDL inside BEGIN is buffered: later statements in the block see it, the committed
/// schema does not change until COMMIT, and COMMIT applies the batch in order.
#[tokio::test]
async fn ddl_inside_a_transaction_is_buffered_until_commit() {
    let fx = start().await;
    let c = &fx.client;
    c.batch_execute("BEGIN").await.expect("BEGIN");
    c.batch_execute("CREATE TABLE jobs (id bigint PRIMARY KEY, queue text)")
        .await
        .expect("CREATE TABLE inside a transaction");
    // The next statement sees the buffered table: IF NOT EXISTS is satisfied by it ...
    c.batch_execute("CREATE TABLE IF NOT EXISTS jobs (id bigint PRIMARY KEY)")
        .await
        .expect("IF NOT EXISTS sees the buffered table");
    // ... and an ALTER of it plans against the buffered definition (extended protocol).
    c.execute("ALTER TABLE jobs ADD COLUMN worker text", &[])
        .await
        .expect("ALTER of a table created earlier in the block");
    assert!(
        !table_exists(&fx, "jobs"),
        "nothing reaches the committed schema before COMMIT"
    );
    c.batch_execute("COMMIT").await.expect("COMMIT");
    assert_eq!(columns_of(&fx, "jobs"), vec!["id", "queue", "worker"]);
    // The applied table is usable after COMMIT.
    c.batch_execute("INSERT INTO jobs (id, queue, worker) VALUES (1, 'default', 'W')")
        .await
        .expect("insert into the committed table");
}

/// A statement planned against the buffered schema is refused when the buffered schema
/// refuses it: a second CREATE of the same table in one block is 42P07.
#[tokio::test]
async fn buffered_ddl_is_checked_against_the_buffered_schema() {
    let fx = start().await;
    let c = &fx.client;
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("CREATE TABLE twice (id int PRIMARY KEY)")
        .await
        .unwrap();
    assert_eq!(
        code(
            c.batch_execute("CREATE TABLE twice (id int PRIMARY KEY)")
                .await
        ),
        "42P07"
    );
    // The failure aborted the block: COMMIT is a ROLLBACK and applies nothing.
    c.batch_execute("COMMIT").await.unwrap();
    assert!(!table_exists(&fx, "twice"));
}

/// ROLLBACK applies none of the buffered DDL.
#[tokio::test]
async fn rollback_discards_buffered_ddl() {
    let fx = start().await;
    let c = &fx.client;
    c.batch_execute("CREATE TABLE kept (id int PRIMARY KEY)")
        .await
        .unwrap();
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("CREATE TABLE dropped_by_rollback (id int PRIMARY KEY)")
        .await
        .unwrap();
    c.execute("ALTER TABLE kept ADD COLUMN extra text", &[])
        .await
        .unwrap();
    c.batch_execute("ROLLBACK").await.unwrap();
    assert!(!table_exists(&fx, "dropped_by_rollback"));
    assert_eq!(columns_of(&fx, "kept"), vec!["id"]);
}

/// The batch was planned against the schema as it stood when each statement ran. If a
/// concurrent session changed that schema before COMMIT, the plan no longer holds: COMMIT
/// is refused with 40001 and applies NONE of the batch.
#[tokio::test]
async fn commit_refuses_a_batch_a_concurrent_change_invalidated() {
    let fx = start().await;
    let other = connect_to(&fx).await;
    let c = &fx.client;
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("CREATE TABLE first_in_batch (id int PRIMARY KEY)")
        .await
        .unwrap();
    c.batch_execute("CREATE TABLE contested (id int PRIMARY KEY)")
        .await
        .unwrap();
    other
        .batch_execute("CREATE TABLE contested (id int PRIMARY KEY, other text)")
        .await
        .expect("the concurrent session creates the same table first");
    assert_eq!(code(c.batch_execute("COMMIT").await), "40001");
    assert!(
        !table_exists(&fx, "first_in_batch"),
        "a refused batch applies nothing, not even the steps before the conflict"
    );
    assert_eq!(columns_of(&fx, "contested"), vec!["id", "other"]);
}

/// An extended-protocol pipeline is an implicit transaction: DDL in it is applied at Sync,
/// together with the pipeline's other statements.
#[tokio::test]
async fn extended_pipeline_ddl_is_applied_at_sync() {
    let fx = start().await;
    fx.client
        .execute("CREATE TABLE piped (id int PRIMARY KEY)", &[])
        .await
        .expect("pipeline DDL");
    assert!(table_exists(&fx, "piped"));
}

/// DML inside the block runs against the COMMITTED schema: a table created earlier in the
/// same block does not exist for it yet. That is refused loudly, never written somewhere.
#[tokio::test]
async fn dml_against_a_table_created_in_the_same_block_is_refused() {
    let fx = start().await;
    let c = &fx.client;
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute("CREATE TABLE not_yet (id int PRIMARY KEY)")
        .await
        .unwrap();
    let error = code(c.batch_execute("INSERT INTO not_yet (id) VALUES (1)").await);
    assert_eq!(error, "42P01");
    c.batch_execute("ROLLBACK").await.unwrap();
}

// ---------------------------------------------------------------------------
// 3. LOCK TABLE (census statement 5, Ecto's migration lock)
// ---------------------------------------------------------------------------

const MIGRATION_LOCK: &str = "LOCK TABLE \"schema_migrations\" IN SHARE UPDATE EXCLUSIVE MODE";

/// Two migrators each take Ecto's lock. On a standalone node the lock is real: the second
/// waits until the first transaction ends, then proceeds.
#[tokio::test]
async fn migration_lock_serializes_two_migrators() {
    let fx = start().await;
    fx.client.batch_execute(SCHEMA_MIGRATIONS).await.unwrap();
    let second = connect_to(&fx).await;
    fx.client.batch_execute("BEGIN").await.unwrap();
    fx.client
        .execute(MIGRATION_LOCK, &[])
        .await
        .expect("LOCK TABLE over the extended protocol");
    second.batch_execute("BEGIN").await.unwrap();
    let waiter =
        tokio::spawn(async move { second.batch_execute(MIGRATION_LOCK).await.map(|()| second) });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !waiter.is_finished(),
        "the second migrator must wait while the first holds the lock"
    );
    fx.client.batch_execute("COMMIT").await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the lock is granted once the holder commits")
        .expect("task")
        .expect("LOCK succeeds");
    second.batch_execute("COMMIT").await.unwrap();
}

/// While the lock is held, a writer of the locked table waits too — stronger than
/// PostgreSQL's SHARE UPDATE EXCLUSIVE, which admits plain writers (documented).
#[tokio::test]
async fn migration_lock_holds_off_writers_of_the_table() {
    let fx = start().await;
    fx.client.batch_execute(SCHEMA_MIGRATIONS).await.unwrap();
    let writer = connect_to(&fx).await;
    fx.client.batch_execute("BEGIN").await.unwrap();
    fx.client.batch_execute(MIGRATION_LOCK).await.unwrap();
    let insert = tokio::spawn(async move {
        writer
            .batch_execute("INSERT INTO schema_migrations (version) VALUES (7)")
            .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!insert.is_finished(), "a writer waits for the lock holder");
    fx.client.batch_execute("ROLLBACK").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), insert)
        .await
        .expect("the writer proceeds after ROLLBACK")
        .expect("task")
        .expect("insert succeeds");
}

#[tokio::test]
async fn lock_outside_a_transaction_block_is_25p01() {
    let fx = start().await;
    fx.client.batch_execute(SCHEMA_MIGRATIONS).await.unwrap();
    assert_eq!(code(fx.client.batch_execute(MIGRATION_LOCK).await), "25P01");
}

#[tokio::test]
async fn lock_of_a_missing_table_is_42p01_and_nowait_is_0a000() {
    let fx = start().await;
    fx.client.batch_execute("BEGIN").await.unwrap();
    assert_eq!(code(fx.client.batch_execute(MIGRATION_LOCK).await), "42P01");
    fx.client.batch_execute("ROLLBACK").await.unwrap();
    fx.client.batch_execute(SCHEMA_MIGRATIONS).await.unwrap();
    fx.client.batch_execute("BEGIN").await.unwrap();
    assert_eq!(
        code(
            fx.client
                .batch_execute("LOCK TABLE schema_migrations NOWAIT")
                .await
        ),
        "0A000"
    );
}

// ---------------------------------------------------------------------------
// 4. COMMENT ON TABLE and pg_description (census statements 51 and 8)
// ---------------------------------------------------------------------------

const TABLE_COMMENT: &str = "SELECT d.description FROM pg_catalog.pg_description d \
     JOIN pg_catalog.pg_class c ON d.objoid = c.oid WHERE c.relname = 'oban_jobs'";

async fn create_jobs(client: &Client) {
    client
        .batch_execute("CREATE TABLE oban_jobs (id bigint PRIMARY KEY, queue text)")
        .await
        .expect("create oban_jobs");
}

/// Oban records its migration version as the table comment and reads it back through
/// `pg_description`.
#[tokio::test]
async fn comment_on_table_is_stored_and_projected_in_pg_description() {
    let fx = start().await;
    create_jobs(&fx.client).await;
    assert!(
        rows(&fx.client, TABLE_COMMENT).await.is_empty(),
        "no comment yet"
    );
    fx.client
        .execute("COMMENT ON TABLE \"public\".oban_jobs IS '12'", &[])
        .await
        .expect("COMMENT ON TABLE over the extended protocol");
    assert_eq!(
        rows(&fx.client, TABLE_COMMENT).await,
        vec![vec![Some("12".to_string())]]
    );
    // A later comment replaces it; IS NULL removes it (as does '', in PostgreSQL).
    fx.client
        .batch_execute("COMMENT ON TABLE oban_jobs IS '13'")
        .await
        .unwrap();
    assert_eq!(
        rows(&fx.client, TABLE_COMMENT).await,
        vec![vec![Some("13".to_string())]]
    );
    fx.client
        .batch_execute("COMMENT ON TABLE oban_jobs IS NULL")
        .await
        .unwrap();
    assert!(rows(&fx.client, TABLE_COMMENT).await.is_empty());
}

/// pg_description's shape: the comment of a table is the row with `classoid` =
/// `pg_class` (1259) and `objsubid` = 0.
#[tokio::test]
async fn pg_description_row_shape() {
    let fx = start().await;
    create_jobs(&fx.client).await;
    fx.client
        .batch_execute("COMMENT ON TABLE oban_jobs IS 'v12'")
        .await
        .unwrap();
    assert_eq!(
        rows(
            &fx.client,
            "SELECT classoid, objsubid, description FROM pg_catalog.pg_description"
        )
        .await,
        vec![vec![
            Some("1259".to_string()),
            Some("0".to_string()),
            Some("v12".to_string())
        ]]
    );
}

/// The comment is transactional DDL like any other: ROLLBACK discards it.
#[tokio::test]
async fn comment_inside_a_rolled_back_block_is_discarded() {
    let fx = start().await;
    create_jobs(&fx.client).await;
    fx.client.batch_execute("BEGIN").await.unwrap();
    fx.client
        .batch_execute("COMMENT ON TABLE oban_jobs IS '12'")
        .await
        .unwrap();
    fx.client.batch_execute("ROLLBACK").await.unwrap();
    assert!(rows(&fx.client, TABLE_COMMENT).await.is_empty());
}

#[tokio::test]
async fn comment_refusals_are_named() {
    let fx = start().await;
    assert_eq!(
        code(
            fx.client
                .batch_execute("COMMENT ON TABLE missing IS 'x'")
                .await
        ),
        "42P01"
    );
    create_jobs(&fx.client).await;
    assert_eq!(
        code(
            fx.client
                .batch_execute("COMMENT ON COLUMN oban_jobs.queue IS 'x'")
                .await
        ),
        "0A000"
    );
}

/// Census statement 8 reads the comment with `obj_description(oid, 'pg_class')`, a function
/// call in a projection plus a LEFT JOIN: that needs W1's expression engine. Until W1
/// merges it is refused loudly, never answered with an empty or wrong row. This test pins
/// that refusal; W1's merge replaces it with the real answer ('12').
#[tokio::test]
async fn obj_description_awaits_the_expression_engine() {
    let fx = start().await;
    create_jobs(&fx.client).await;
    fx.client
        .batch_execute("COMMENT ON TABLE oban_jobs IS '12'")
        .await
        .unwrap();
    let census_8 = "SELECT pg_catalog.obj_description(pg_class.oid, 'pg_class') FROM pg_class \
         LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace \
         WHERE pg_class.relname = 'oban_jobs' AND pg_namespace.nspname = 'public'";
    let error = fx
        .client
        .simple_query(census_8)
        .await
        .expect_err("refused until W1 lands function calls in projections");
    assert!(error.code().is_some(), "a typed SQLSTATE: {error:?}");
}

// ---------------------------------------------------------------------------
// 10. pg_range (Postgrex's type bootstrap joins it; census statements 1, 53, 56)
// ---------------------------------------------------------------------------

/// Ferrosa has no range types, so `pg_range` exists and is empty — the correct answer for
/// Postgrex's `LEFT JOIN pg_range`, not a missing relation.
#[tokio::test]
async fn pg_range_exists_and_is_empty() {
    let fx = start().await;
    assert!(rows(
        &fx.client,
        "SELECT rngtypid, rngsubtype, rngmultitypid FROM pg_catalog.pg_range"
    )
    .await
    .is_empty());
    let prepared = fx
        .client
        .prepare("SELECT * FROM pg_catalog.pg_range")
        .await
        .expect("pg_range describes");
    let names: Vec<&str> = prepared.columns().iter().map(|c| c.name()).collect();
    assert_eq!(
        names,
        vec![
            "rngtypid",
            "rngsubtype",
            "rngmultitypid",
            "rngcollation",
            "rngsubopc",
            "rngcanonical",
            "rngsubdiff"
        ]
    );
}

// ---------------------------------------------------------------------------
// 5. Enum types (census statements 9, 10, 36, 53, 56)
// ---------------------------------------------------------------------------

const CREATE_STATE: &str = "CREATE TYPE \"public\".oban_job_state AS ENUM \
     ('available', 'scheduled', 'executing')";
const STATE_TABLE: &str = "CREATE TABLE jobs (id bigint PRIMARY KEY, \
     \"state\" \"public\".oban_job_state NOT NULL)";

async fn enum_fixture() -> Fixture {
    let fx = start().await;
    fx.client
        .execute(CREATE_STATE, &[])
        .await
        .expect("CREATE TYPE ... AS ENUM over the extended protocol");
    fx.client
        .execute(STATE_TABLE, &[])
        .await
        .expect("a column typed by the enum");
    fx
}

/// A value outside the enum is refused (22P02), over both protocols; a label is stored
/// and read back as its text.
#[tokio::test]
async fn enum_column_accepts_labels_and_refuses_other_values() {
    let fx = enum_fixture().await;
    fx.client
        .batch_execute("INSERT INTO jobs (id, state) VALUES (1, 'available')")
        .await
        .expect("a declared label");
    assert_eq!(
        code(
            fx.client
                .batch_execute("INSERT INTO jobs (id, state) VALUES (2, 'bogus')")
                .await
        ),
        "22P02"
    );
    assert_eq!(
        code(
            fx.client
                .execute(
                    "INSERT INTO jobs (id, state) VALUES ($1, $2)",
                    &[&3_i64, &"nope"]
                )
                .await
        ),
        "22P02"
    );
    assert_eq!(
        code(
            fx.client
                .batch_execute("UPDATE jobs SET state = 'bogus' WHERE id = 1")
                .await
        ),
        "22P02"
    );
    assert_eq!(
        rows(&fx.client, "SELECT state FROM jobs").await,
        vec![vec![Some("available".to_string())]]
    );
}

/// `ADD VALUE IF NOT EXISTS` is idempotent; the new label is then accepted; an existing
/// label without `IF NOT EXISTS` is 42710; a missing type is 42704.
#[tokio::test]
async fn alter_type_add_value() {
    let fx = enum_fixture().await;
    let add = "ALTER TYPE \"public\".oban_job_state ADD VALUE IF NOT EXISTS 'cancelled'";
    fx.client.execute(add, &[]).await.expect("ADD VALUE");
    fx.client.batch_execute(add).await.expect("ADD VALUE again");
    fx.client
        .batch_execute("INSERT INTO jobs (id, state) VALUES (1, 'cancelled')")
        .await
        .expect("the added label is accepted");
    assert_eq!(
        code(
            fx.client
                .batch_execute("ALTER TYPE oban_job_state ADD VALUE 'available'")
                .await
        ),
        "42710"
    );
    assert_eq!(
        code(
            fx.client
                .batch_execute("ALTER TYPE no_such_type ADD VALUE 'x'")
                .await
        ),
        "42704"
    );
}

#[tokio::test]
async fn enum_definition_refusals() {
    let fx = enum_fixture().await;
    assert_eq!(code(fx.client.batch_execute(CREATE_STATE).await), "42710");
    assert_eq!(
        code(
            fx.client
                .batch_execute("CREATE TYPE dup AS ENUM ('a', 'a')")
                .await
        ),
        "42710"
    );
    assert_eq!(
        code(
            fx.client
                .batch_execute("CREATE TABLE t2 (id int PRIMARY KEY, s no_such_type)")
                .await
        ),
        "42704"
    );
}

/// The enum is visible in pg_type (with its array type), and pg_attribute types the column
/// by it. The type's backing storage is not visible as a relation.
#[tokio::test]
async fn enum_is_projected_in_the_catalog() {
    let fx = enum_fixture().await;
    let types = rows(
        &fx.client,
        "SELECT typname FROM pg_catalog.pg_type WHERE typname = 'oban_job_state' \
         OR typname = '_oban_job_state'",
    )
    .await;
    let mut names: Vec<String> = types.into_iter().filter_map(|r| r[0].clone()).collect();
    names.sort();
    assert_eq!(names, vec!["_oban_job_state", "oban_job_state"]);
    assert_eq!(
        rows(
            &fx.client,
            "SELECT t.typname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t \
             ON a.atttypid = t.oid WHERE a.attname = 'state'"
        )
        .await,
        vec![vec![Some("oban_job_state".to_string())]]
    );
    assert!(rows(
        &fx.client,
        "SELECT relname FROM pg_catalog.pg_class WHERE relname = '_sys_pg_types'"
    )
    .await
    .is_empty());
}

/// Enum DDL is transactional: a type created in a block is usable by later statements in
/// it, and ROLLBACK discards both.
#[tokio::test]
async fn enum_ddl_is_transactional() {
    let fx = start().await;
    let c = &fx.client;
    c.batch_execute("BEGIN").await.unwrap();
    c.batch_execute(CREATE_STATE).await.unwrap();
    c.batch_execute(STATE_TABLE)
        .await
        .expect("the buffered type is visible to the next statement");
    c.batch_execute("ROLLBACK").await.unwrap();
    assert!(!table_exists(&fx, "jobs"));
    assert_eq!(code(c.batch_execute(STATE_TABLE).await), "42704");
}

// ---------------------------------------------------------------------------
// 7. bigserial per D15 (census statement 10's "id" bigserial)
// ---------------------------------------------------------------------------

fn extension(fx: &Fixture, table: &str, key: &str) -> Option<String> {
    fx.schema
        .snapshot()
        .tables
        .get(&("public".to_string(), table.to_string()))
        .and_then(|meta| meta.extensions.get(key).cloned())
}

/// A `bigserial` column is a `bigint` marked for server-generated D15 ids (the INSERT
/// side, which mints them, is W2's).
#[tokio::test]
async fn bigserial_column_is_a_marked_bigint() {
    let fx = start().await;
    fx.client
        .execute(
            "CREATE TABLE IF NOT EXISTS \"public\".\"serials\" (\"id\" bigserial, \
             \"v\" text, PRIMARY KEY (\"id\"))",
            &[],
        )
        .await
        .expect("bigserial is accepted");
    let snapshot = fx.schema.snapshot();
    let meta = snapshot
        .tables
        .get(&("public".to_string(), "serials".to_string()))
        .expect("table");
    assert_eq!(meta.columns["id"].column_type, "bigint");
    assert_eq!(
        extension(&fx, "serials", "pg.column_serial.id").as_deref(),
        Some("bigserial")
    );
    let prepared = fx.client.prepare("SELECT id FROM serials").await.unwrap();
    assert_eq!(
        prepared.columns()[0].type_(),
        &tokio_postgres::types::Type::INT8
    );
}

/// `serial` and `smallserial` cannot hold a D15 id, and there are no sequences.
#[tokio::test]
async fn narrow_serials_and_sequences_are_refused() {
    let fx = start().await;
    for sql in [
        "CREATE TABLE s4 (id serial PRIMARY KEY)",
        "CREATE TABLE s2 (id smallserial PRIMARY KEY)",
        "CREATE SEQUENCE jobs_id_seq",
    ] {
        assert_eq!(code(fx.client.batch_execute(sql).await), "0A000", "{sql}");
    }
    // Through the general planner (a FROM clause). The no-FROM form still goes through the
    // narrow scalar grammar and fails at parse (42601), a W1 open item.
    fx.client
        .batch_execute("CREATE TABLE one (id int PRIMARY KEY)")
        .await
        .unwrap();
    for sql in [
        "SELECT nextval('jobs_id_seq') FROM one",
        "SELECT setval('jobs_id_seq', 5) FROM one",
        "SELECT currval('jobs_id_seq') FROM one",
    ] {
        assert_eq!(code(fx.client.simple_query(sql).await), "0A000", "{sql}");
    }
}

// ---------------------------------------------------------------------------
// Found while building: a non-key NOT NULL was parsed and dropped, never enforced.
// ---------------------------------------------------------------------------

/// `worker text NOT NULL` must refuse a NULL — explicit, omitted, bound, or by UPDATE —
/// with 23502. It used to store the NULL silently.
#[tokio::test]
async fn not_null_on_a_regular_column_is_enforced() {
    let fx = start().await;
    let c = &fx.client;
    c.batch_execute("CREATE TABLE nn (id int PRIMARY KEY, worker text NOT NULL, note text)")
        .await
        .unwrap();
    for sql in [
        "INSERT INTO nn (id, worker) VALUES (1, NULL)",
        "INSERT INTO nn (id, note) VALUES (2, 'no worker')",
    ] {
        assert_eq!(code(c.batch_execute(sql).await), "23502", "{sql}");
    }
    let none: Option<&str> = None;
    assert_eq!(
        code(
            c.execute(
                "INSERT INTO nn (id, worker) VALUES ($1, $2)",
                &[&3_i32, &none]
            )
            .await
        ),
        "23502"
    );
    c.batch_execute("INSERT INTO nn (id, worker) VALUES (4, 'W')")
        .await
        .expect("a non-NULL value is admitted");
    assert_eq!(
        code(
            c.batch_execute("UPDATE nn SET worker = NULL WHERE id = 4")
                .await
        ),
        "23502"
    );
    // A nullable column still takes NULL.
    c.batch_execute("UPDATE nn SET note = NULL WHERE id = 4")
        .await
        .unwrap();
    assert_eq!(
        rows(c, "SELECT id, worker FROM nn").await,
        vec![vec![Some("4".to_string()), Some("W".to_string())]]
    );
}

// ---------------------------------------------------------------------------
// 8. ALTER TABLE variants, DEFAULTs and CHECK constraints (census 10, 19-20, 22, 28-29,
//    35, 37-40, 47-49)
// ---------------------------------------------------------------------------

const OBAN_JOBS_V1: &str =
    "CREATE TABLE IF NOT EXISTS \"public\".\"oban_jobs\" (\"id\" bigserial, \
     \"state\" \"public\".oban_job_state DEFAULT 'available' NOT NULL, \
     \"queue\" text DEFAULT 'default' NOT NULL, \"worker\" text NOT NULL, \"args\" jsonb NOT NULL, \
     \"attempt\" integer DEFAULT 0 NOT NULL, \"max_attempts\" integer DEFAULT 20 NOT NULL, \
     \"inserted_at\" timestamp DEFAULT timezone('UTC', now()) NOT NULL, \
     \"attempted_at\" timestamp, PRIMARY KEY (\"id\"))";

async fn oban_jobs() -> Fixture {
    let fx = start().await;
    fx.client.batch_execute(CREATE_STATE).await.unwrap();
    fx.client
        .execute(OBAN_JOBS_V1, &[])
        .await
        .expect("Oban's v1 CREATE TABLE with DEFAULTs");
    fx
}

/// DEFAULT expressions are stored as canonical SQL text for the INSERT path (W2).
#[tokio::test]
async fn column_defaults_are_recorded() {
    let fx = oban_jobs().await;
    let get = |c: &str| extension(&fx, "oban_jobs", &format!("pg.column_default.{c}"));
    assert_eq!(get("state").as_deref(), Some("'available'"));
    assert_eq!(get("queue").as_deref(), Some("'default'"));
    assert_eq!(get("attempt").as_deref(), Some("0"));
    assert_eq!(
        get("inserted_at").as_deref(),
        Some("timezone('UTC', now())")
    );
    assert_eq!(get("worker"), None);
    assert_eq!(
        extension(&fx, "oban_jobs", "pg.not_null.state").as_deref(),
        Some("true")
    );
}

/// A literal DEFAULT outside the enum is refused at DDL time, as PostgreSQL does.
#[tokio::test]
async fn an_enum_default_must_be_a_label() {
    let fx = start().await;
    fx.client.batch_execute(CREATE_STATE).await.unwrap();
    assert_eq!(
        code(
            fx.client
                .batch_execute(
                    "CREATE TABLE bad (id int PRIMARY KEY, s oban_job_state DEFAULT 'nope')"
                )
                .await
        ),
        "22P02"
    );
}

/// Several actions in one ALTER: ADD COLUMN IF NOT EXISTS (census 28), ALTER COLUMN TYPE
/// to the same type and SET DEFAULT (29), SET NOT NULL (37).
#[tokio::test]
async fn multi_action_alter_table() {
    let fx = oban_jobs().await;
    let c = &fx.client;
    let add = "ALTER TABLE \"public\".\"oban_jobs\" ADD COLUMN IF NOT EXISTS \"discarded_at\" \
               timestamp, ADD COLUMN IF NOT EXISTS \"priority\" integer, \
               ADD COLUMN IF NOT EXISTS \"meta\" jsonb DEFAULT '{}'";
    c.execute(add, &[])
        .await
        .expect("ADD COLUMN IF NOT EXISTS x3");
    c.execute(add, &[]).await.expect("idempotent");
    c.execute(
        "ALTER TABLE \"public\".\"oban_jobs\" ALTER COLUMN \"priority\" TYPE integer, \
         ALTER COLUMN \"priority\" SET DEFAULT 0, ALTER COLUMN \"args\" TYPE jsonb, \
         ALTER COLUMN \"args\" SET DEFAULT '{}', ALTER COLUMN \"priority\" SET NOT NULL",
        &[],
    )
    .await
    .expect("same-type TYPE, SET DEFAULT, SET NOT NULL");
    let cols = columns_of(&fx, "oban_jobs");
    assert!(cols.contains(&"priority".to_string()) && cols.contains(&"meta".to_string()));
    assert_eq!(
        extension(&fx, "oban_jobs", "pg.column_default.priority").as_deref(),
        Some("0")
    );
    assert_eq!(
        extension(&fx, "oban_jobs", "pg.column_default.meta").as_deref(),
        Some("'{}'")
    );
    assert_eq!(
        extension(&fx, "oban_jobs", "pg.not_null.priority").as_deref(),
        Some("true")
    );
    // A real type change cannot be done safely: refused by name, nothing applied.
    assert_eq!(
        code(
            c.batch_execute(
                "ALTER TABLE oban_jobs ALTER COLUMN priority TYPE text, \
                             ALTER COLUMN worker SET DEFAULT 'x'"
            )
            .await
        ),
        "0A000"
    );
    assert_eq!(
        extension(&fx, "oban_jobs", "pg.column_default.worker"),
        None,
        "a refused statement applies none of its actions"
    );
}

/// SET NOT NULL validates existing rows.
#[tokio::test]
async fn set_not_null_refuses_existing_nulls() {
    let fx = start().await;
    let c = &fx.client;
    c.batch_execute("CREATE TABLE sn (id int PRIMARY KEY, p int)")
        .await
        .unwrap();
    c.batch_execute("INSERT INTO sn (id) VALUES (1)")
        .await
        .unwrap();
    assert_eq!(
        code(
            c.batch_execute("ALTER TABLE sn ALTER COLUMN p SET NOT NULL")
                .await
        ),
        "23502"
    );
    c.batch_execute("UPDATE sn SET p = 0 WHERE id = 1")
        .await
        .unwrap();
    c.batch_execute("ALTER TABLE sn ALTER COLUMN p SET NOT NULL")
        .await
        .expect("no NULL left");
}

/// CHECK constraints (census 19, 38-40, 48-49): enforced on writes, validated against
/// existing rows unless NOT VALID, and DROP CONSTRAINT removes one.
#[tokio::test]
async fn check_constraints_are_enforced() {
    let fx = oban_jobs().await;
    let c = &fx.client;
    for sql in [
        "ALTER TABLE \"public\".\"oban_jobs\" ADD CONSTRAINT \"worker_length\" CHECK \
         (char_length(worker) > 0 AND char_length(worker) < 128)",
        "ALTER TABLE \"public\".\"oban_jobs\" ADD CONSTRAINT \"positive_max_attempts\" CHECK \
         (max_attempts > 0)",
        "ALTER TABLE \"public\".\"oban_jobs\" ADD CONSTRAINT \"attempt_range\" CHECK \
         (attempt between 0 and max_attempts)",
    ] {
        c.execute(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let insert = |worker: &str, attempt: i32, max: i32| {
        format!(
            "INSERT INTO oban_jobs (id, state, queue, worker, args, attempt, max_attempts, \
             inserted_at) VALUES ({attempt}{max}, 'available', 'q', '{worker}', '{{}}', \
             {attempt}, {max}, now())"
        )
    };
    c.batch_execute(&insert("W", 0, 20))
        .await
        .expect("a valid row");
    // UPDATE is checked on the complete new row: the stored max_attempts (20) bounds attempt.
    assert_eq!(
        code(
            c.batch_execute("UPDATE oban_jobs SET attempt = 25 WHERE id = 20")
                .await
        ),
        "23514"
    );
    c.batch_execute("UPDATE oban_jobs SET attempt = 3 WHERE id = 20")
        .await
        .expect("within the range");
    for (worker, attempt, max) in [("", 0, 20), ("W", 0, 0), ("W", 21, 20)] {
        assert_eq!(
            code(c.batch_execute(&insert(worker, attempt, max)).await),
            "23514",
            "{worker:?} {attempt} {max}"
        );
    }
    // A validated ADD fails on an existing violating row; NOT VALID is accepted and still
    // checks new writes.
    assert_eq!(
        code(
            c.batch_execute("ALTER TABLE oban_jobs ADD CONSTRAINT never CHECK (attempt > 100)")
                .await
        ),
        "23514"
    );
    c.execute(
        "ALTER TABLE \"public\".\"oban_jobs\" ADD CONSTRAINT \"non_negative_priority\" \
         CHECK (attempt > 5) NOT VALID",
        &[],
    )
    .await
    .expect("NOT VALID skips existing rows");
    assert_eq!(code(c.batch_execute(&insert("W", 1, 20)).await), "23514");
    c.execute(
        "ALTER TABLE \"public\".\"oban_jobs\" DROP CONSTRAINT \"non_negative_priority\"",
        &[],
    )
    .await
    .expect("DROP CONSTRAINT");
    c.batch_execute(&insert("W", 1, 20))
        .await
        .expect("the dropped constraint no longer applies");
    assert_eq!(
        code(
            c.batch_execute("ALTER TABLE oban_jobs DROP CONSTRAINT missing")
                .await
        ),
        "42704"
    );
}

/// SET UNLOGGED (census 47) is recorded, not applied: ferrosa keeps the table durable,
/// which is stronger than what was asked.
#[tokio::test]
async fn set_unlogged_is_recorded() {
    let fx = start().await;
    fx.client
        .batch_execute("CREATE TABLE peers (name text PRIMARY KEY)")
        .await
        .unwrap();
    fx.client
        .execute("ALTER TABLE \"public\".peers SET UNLOGGED", &[])
        .await
        .expect("SET UNLOGGED");
    assert_eq!(
        extension(&fx, "peers", "pg.unlogged").as_deref(),
        Some("true")
    );
}

// ---------------------------------------------------------------------------
// 9. CREATE / DROP INDEX (census 11-13, 17-18, 24-27, 30-31, 41-45)
// ---------------------------------------------------------------------------

fn physical_index(fx: &Fixture, table: &str, name: &str) -> bool {
    fx.schema.snapshot().indexes.contains_key(&(
        "public".to_string(),
        table.to_string(),
        name.to_string(),
    ))
}

/// A single-column index is a real secondary index; IF NOT EXISTS is idempotent;
/// DROP INDEX IF EXISTS removes it and tolerates a missing name.
#[tokio::test]
async fn single_column_index_is_built_and_dropped() {
    let fx = oban_jobs().await;
    let c = &fx.client;
    let create = "CREATE INDEX IF NOT EXISTS \"oban_jobs_queue_index\" ON \"public\".\"oban_jobs\" (\"queue\")";
    c.execute(create, &[]).await.expect("CREATE INDEX");
    c.execute(create, &[]).await.expect("IF NOT EXISTS");
    assert!(physical_index(&fx, "oban_jobs", "oban_jobs_queue_index"));
    assert_eq!(
        code(
            c.batch_execute("CREATE INDEX \"oban_jobs_queue_index\" ON oban_jobs (state)")
                .await
        ),
        "42P07"
    );
    c.execute(
        "DROP INDEX IF EXISTS \"public\".\"oban_jobs_queue_index\"",
        &[],
    )
    .await
    .expect("DROP INDEX");
    assert!(!physical_index(&fx, "oban_jobs", "oban_jobs_queue_index"));
    c.execute(
        "DROP INDEX IF EXISTS \"public\".\"oban_jobs_args_vector\"",
        &[],
    )
    .await
    .expect("IF EXISTS tolerates a missing index");
    assert_eq!(code(c.batch_execute("DROP INDEX nope").await), "42704");
}

/// Composite, partial, DESC and GIN indexes are accepted and recorded, but build no
/// physical index (ferrosa's secondary indexes are single-column): queries scan. The
/// partial predicate must parse.
#[tokio::test]
async fn composite_partial_and_gin_indexes_are_recorded_not_built() {
    let fx = oban_jobs().await;
    let c = &fx.client;
    for (name, sql) in [
        ("oban_jobs_state_queue_priority_scheduled_at_id_index",
         "CREATE INDEX IF NOT EXISTS \"oban_jobs_state_queue_priority_scheduled_at_id_index\" ON \"public\".\"oban_jobs\" (\"state\", \"queue\", \"attempt\", \"id\")"),
        ("oban_jobs_attempted_at_id_index",
         "CREATE INDEX IF NOT EXISTS \"oban_jobs_attempted_at_id_index\" ON \"public\".\"oban_jobs\" (attempted_at desc, \"id\") WHERE state in ('completed', 'discarded')"),
        ("oban_jobs_args_index",
         "CREATE INDEX IF NOT EXISTS \"oban_jobs_args_index\" ON \"public\".\"oban_jobs\" USING gin (\"args\")"),
        ("oban_jobs_scheduled_at_index",
         "CREATE INDEX \"oban_jobs_scheduled_at_index\" ON \"public\".\"oban_jobs\" (\"attempted_at\") WHERE state in ('available', 'scheduled')"),
    ] {
        c.execute(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert!(
            extension(&fx, "oban_jobs", &format!("pg.index.{name}")).is_some(),
            "{name} is recorded"
        );
        assert!(!physical_index(&fx, "oban_jobs", name), "{name} builds nothing");
    }
    c.batch_execute("DROP INDEX oban_jobs_args_index")
        .await
        .expect("drop a recorded index");
    assert_eq!(
        extension(&fx, "oban_jobs", "pg.index.oban_jobs_args_index").as_deref(),
        Some("")
    );
    for sql in [
        "CREATE UNIQUE INDEX u ON oban_jobs (queue)",
        "CREATE INDEX e ON oban_jobs ((lower(queue)))",
        "CREATE INDEX b ON oban_jobs USING brin (queue)",
        "CREATE INDEX m ON oban_jobs (no_such_column)",
    ] {
        let got = code(c.batch_execute(sql).await);
        assert!(got == "0A000" || got == "42703", "{sql}: {got}");
    }
}
