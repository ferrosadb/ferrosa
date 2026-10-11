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
/// columns create a CQL `jsonb` column. Standalone-vs-cluster gating (T-300) is
/// `pg_ddl_create_table_jsonb_refused_off_standalone`.
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

/// T-300 (D24) on the PG path (t_57fa8a9e): off a standalone node, a jsonb or
/// json column is refused with `0A000` naming the mode and the D15a ledger, and
/// nothing is created. A table with no jsonb column is unaffected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ddl_create_table_jsonb_refused_off_standalone() {
    let fx = start().await;
    fx.schema
        .set_deployment_mode(ferrosa_common::deployment_mode::DeploymentMode::Cluster);
    for sql in [
        "CREATE TABLE g (id int PRIMARY KEY, doc jsonb)",
        "CREATE TABLE g (id int PRIMARY KEY, doc json)",
    ] {
        let error = fx
            .client
            .batch_execute(sql)
            .await
            .expect_err("jsonb DDL is refused in cluster mode");
        let db = error.as_db_error().expect("db error");
        assert_eq!(db.code().code(), "0A000", "{sql}: {}", db.message());
        assert!(
            db.message().contains("D15a"),
            "names the ledger requirement: {}",
            db.message()
        );
    }
    assert!(!fx
        .schema
        .snapshot()
        .tables
        .contains_key(&("public".to_string(), "g".to_string())));
    fx.client
        .batch_execute("CREATE TABLE plain (id int PRIMARY KEY, v text)")
        .await
        .expect("non-jsonb DDL is unaffected by the jsonb gate");
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

/// User ruling (2026-10-11): the scan fallback that serves the previous test must be
/// LOGGED LOUDLY, never silent. A silent fallback hides a missing or unusable index
/// behind a result that looks the same, so this pins the warning on the real statement
/// path: it boots a server under a capturing subscriber and asserts that an UPDATE whose
/// WHERE names no indexed column emits a warning that names the table.
///
/// Deliberately NOT `tokio::test`: the runtime is built INSIDE the subscriber scope so
/// every task the server spawns — including the connection handler that logs — polls on
/// the same thread as the capturing subscriber.
#[test]
fn a_scan_fallback_is_logged_loudly() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
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
        });
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
}
