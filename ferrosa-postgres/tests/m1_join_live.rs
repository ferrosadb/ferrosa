//! M1 end-to-end milestone: a real Postgres driver (`tokio-postgres`) runs a
//! two-table inner JOIN against ferrosa storage through the wire front-end and
//! gets the correct rows back.
//!
//! This is the first time the full stack is exercised in one path: SCRAM auth →
//! `ReadyForQuery` → simple `Query` → SQL parse/bind/execute over real
//! `StorageEngine` rows → `RowDescription`/`DataRow`/`CommandComplete`. No
//! external infrastructure (no S3 / Docker / cluster) — a temp engine with
//! `object_store: None`, fully local.
//!
//! Tables (mirroring `ferrosa-sql`'s in-memory M1 fixture):
//! - `public.users(id int PK, name text)`
//! - `public.orders(oid int PK, uid int)`
//!
//! Rows: alice(id=1), bob(id=2); orders 10→1, 11→1, 12→2. The query
//! `SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 1`
//! must return alice's two orders (oid 10 and 11).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, AccordAccess, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
    EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
    ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row as StorageRow};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};
use indexmap::IndexMap;
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

// ── Auth ──────────────────────────────────────────────────────────────────────

struct OneRole {
    user: String,
    verifier: ScramVerifier,
}

/// A single superuser login with no limiter: these tests exercise the query
/// engine, not authorization (see `security_live.rs` for that).
impl VerifierStore for OneRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == self.user).then(|| self.verifier.clone())
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

fn dev_store() -> Arc<OneRole> {
    let salt = b"ferrosa-dev-salt";
    Arc::new(OneRole {
        user: "ferrosa_user".into(),
        verifier: ScramVerifier::from_password("devpass", salt, 4096),
    })
}

// ── Schema / engine config ──────────────────────────────────────────────────

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

fn superuser() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
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

fn column(name: &str, kind: ColumnKind, ty: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position: 0,
        column_type: ty.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

/// Create keyspace `public` plus the two M1 tables through the public DDL API.
fn create_schema() -> Schema {
    let schema = Schema::new(schema_config()).expect("schema bootstraps");
    let auth = superuser();

    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut o = HashMap::new();
                        o.insert("replication_factor".to_string(), "1".to_string());
                        o
                    },
                },
            },
            &auth,
        )
        .expect("create keyspace public");

    // users(id int PK, name text)
    let mut users_cols = IndexMap::new();
    users_cols.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, "int"),
    );
    users_cols.insert(
        "name".to_string(),
        column("name", ColumnKind::Regular, "text"),
    );
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "users".to_string(),
                id: Uuid::new_v4(),
                columns: users_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table users");

    // orders(oid int PK, uid int)
    let mut orders_cols = IndexMap::new();
    orders_cols.insert(
        "oid".to_string(),
        column("oid", ColumnKind::PartitionKey, "int"),
    );
    orders_cols.insert("uid".to_string(), column("uid", ColumnKind::Regular, "int"));
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "orders".to_string(),
                id: Uuid::new_v4(),
                columns: orders_cols,
                partition_key: vec!["oid".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table orders");

    schema
}

/// Storage-layer cell schema for `users`: PK `id` (Int32), regular `name` (UTF8).
fn users_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    TableSchema {
        keyspace: "public".to_string(),
        table: "users".to_string(),
        key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "name".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

/// Storage-layer cell schema for `orders`: PK `oid` (Int32), regular `uid` (Int32).
fn orders_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    TableSchema {
        keyspace: "public".to_string(),
        table: "orders".to_string(),
        key_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "uid".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

/// A no-clustering storage `Row` carrying one regular cell (ordinal 0).
fn single_cell_row(cell_bytes: Vec<u8>, ts: i64) -> StorageRow {
    StorageRow {
        clustering: vec![],
        cells: vec![(0, CellValue::live(cell_bytes, ts))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Build the engine, register both tables, and write the M1 fixture rows.
fn seed_engine(dir: &Path) -> StorageEngine {
    let engine = StorageEngine::new(engine_config(dir), None).unwrap();
    engine.register_table(users_storage_schema()).unwrap();
    engine.register_table(orders_storage_schema()).unwrap();

    let users = TableId::new("public", "users");
    let orders = TableId::new("public", "orders");

    // users: id=1 -> alice, id=2 -> bob. Partition key is the Int32 id.
    let pk = |i: i32| DecoratedKey::new(PartitionKey::new(i.to_be_bytes().to_vec()));
    engine
        .write(
            &users,
            &pk(1),
            single_cell_row(b"alice".to_vec(), 1000),
            1000,
        )
        .unwrap();
    engine
        .write(&users, &pk(2), single_cell_row(b"bob".to_vec(), 1001), 1001)
        .unwrap();

    // orders: oid=10 -> uid 1, oid=11 -> uid 1, oid=12 -> uid 2.
    let int_cell = |v: i32, ts: i64| single_cell_row(v.to_be_bytes().to_vec(), ts);
    engine
        .write(&orders, &pk(10), int_cell(1, 1002), 1002)
        .unwrap();
    engine
        .write(&orders, &pk(11), int_cell(1, 1003), 1003)
        .unwrap();
    engine
        .write(&orders, &pk(12), int_cell(2, 1004), 1004)
        .unwrap();

    engine
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("SCRAM handshake should succeed");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn m1_join_returns_rows_to_a_real_driver() {
    let dir = tempfile::tempdir().unwrap();
    let engine = seed_engine(dir.path());
    let schema = create_schema();
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));

    let client = connect(port).await;

    // ── The M1 JOIN ──────────────────────────────────────────────────────────
    let rows = client
        .simple_query(
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 1",
        )
        .await
        .expect("M1 join query should return rows");

    // simple_query yields RowDescription + data rows + CommandComplete; filter to
    // the data rows.
    let data: Vec<&tokio_postgres::SimpleQueryRow> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();

    assert_eq!(data.len(), 2, "alice has exactly two orders");
    let mut oids: Vec<&str> = data
        .iter()
        .map(|r| {
            assert_eq!(r.get(0), Some("alice"), "name column must be alice");
            r.get(1).expect("oid column present")
        })
        .collect();
    oids.sort();
    assert_eq!(oids, vec!["10", "11"], "alice's orders are oid 10 and 11");

    // ── Fail loud: a syntax error surfaces as a driver error ──────────────────
    let syntax_err = client
        .simple_query("SELCT bogus")
        .await
        .expect_err("a syntax error must surface as a driver error");
    assert_eq!(
        syntax_err.code().map(|c| c.code()),
        Some("42601"),
        "unexpected error for syntax: {syntax_err}"
    );

    // ── Fail loud: an unknown table surfaces as undefined_table ───────────────
    let no_table_err = client
        .simple_query("SELECT * FROM ghosts")
        .await
        .expect_err("an unknown table must surface as a driver error");
    assert_eq!(
        no_table_err.code().map(|c| c.code()),
        Some("42P01"),
        "unexpected error for missing table: {no_table_err}"
    );

    // The connection is still usable after the failures (each got its own
    // ReadyForQuery): re-run the join to confirm.
    let again = client
        .simple_query("SELECT u.name FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = 2")
        .await
        .expect("session remains usable after fail-loud errors");
    let again_rows = again
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(again_rows, 1, "bob has exactly one order");
}

#[tokio::test]
async fn extended_query_error_recovers_after_sync() {
    // A parameterized query against a missing table must surface as a fail-loud
    // driver error (extended protocol: ErrorResponse, then ignore until Sync),
    // and the connection must remain usable for the next query.
    let dir = tempfile::tempdir().unwrap();
    let engine = seed_engine(dir.path());
    let schema = create_schema();
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));
    let client = connect(port).await;

    // Unknown table via the extended (parameterized) path ⇒ undefined_table.
    let err = client
        .query("SELECT name FROM ghosts WHERE id = $1", &[&1i32])
        .await
        .expect_err("missing table must surface as a driver error");
    assert_eq!(
        err.code().map(|c| c.code()),
        Some("42P01"),
        "unexpected error for missing table: {err}"
    );

    // Session still usable: a valid parameterized query returns rows.
    let rows = client
        .query(
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = $1",
            &[&1i32],
        )
        .await
        .expect("session remains usable after a fail-loud extended error");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn extended_parameterized_join_over_a_real_driver() {
    // tokio-postgres `query()` uses the EXTENDED protocol with BINARY parameter
    // AND BINARY result encoding: Parse → Describe(S) → Bind → Execute → Sync.
    // This exercises the full prepared-statement / portal path end-to-end.
    let dir = tempfile::tempdir().unwrap();
    let engine = seed_engine(dir.path());
    let schema = create_schema();
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));
    let client = connect(port).await;

    // alice (id=1) has two orders: oid 10 and 11. `$1` is a binary int4 param,
    // and the typed `row.get::<_, i32>` forces binary result decoding.
    let rows = client
        .query(
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = $1",
            &[&1i32],
        )
        .await
        .expect("parameterized join should return rows");
    assert_eq!(rows.len(), 2, "alice has exactly two orders");
    let mut oids: Vec<i32> = rows
        .iter()
        .map(|r| {
            assert_eq!(r.get::<_, &str>(0), "alice", "name column must be alice");
            r.get::<_, i32>(1)
        })
        .collect();
    oids.sort_unstable();
    assert_eq!(oids, vec![10, 11], "alice's orders are oid 10 and 11");

    // bob (id=2) has exactly one order (oid 12).
    let bob = client
        .query(
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = $1",
            &[&2i32],
        )
        .await
        .expect("parameterized join for bob should return rows");
    assert_eq!(bob.len(), 1, "bob has exactly one order");
    assert_eq!(bob[0].get::<_, &str>(0), "bob");
    assert_eq!(bob[0].get::<_, i32>(1), 12);
}

#[tokio::test]
async fn group_by_order_by_limit_over_a_real_driver() {
    let dir = tempfile::tempdir().unwrap();
    let engine = seed_engine(dir.path());
    let schema = create_schema();
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));
    let client = connect(port).await;

    let collect = |msgs: Vec<tokio_postgres::SimpleQueryMessage>| {
        msgs.into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect::<Vec<_>>()
    };

    // Orders per user, ascending: uid=1 → 2 orders, uid=2 → 1 order.
    let grouped = collect(
        client
            .simple_query("SELECT o.uid, COUNT(*) FROM orders o GROUP BY o.uid ORDER BY o.uid")
            .await
            .expect("GROUP BY query should return aggregated rows"),
    );
    assert_eq!(grouped.len(), 2, "two groups");
    assert_eq!(
        (grouped[0].get(0), grouped[0].get(1)),
        (Some("1"), Some("2"))
    );
    assert_eq!(
        (grouped[1].get(0), grouped[1].get(1)),
        (Some("2"), Some("1"))
    );

    // ORDER BY DESC + LIMIT trims to the highest uid.
    let limited = collect(
        client
            .simple_query(
                "SELECT o.uid, COUNT(*) FROM orders o GROUP BY o.uid ORDER BY o.uid DESC LIMIT 1",
            )
            .await
            .expect("LIMIT query should succeed"),
    );
    assert_eq!(limited.len(), 1, "LIMIT 1");
    assert_eq!(
        limited[0].get(0),
        Some("2"),
        "DESC orders the highest uid first"
    );
}

#[tokio::test]
async fn where_having_distinct_over_a_real_driver() {
    let dir = tempfile::tempdir().unwrap();
    let engine = seed_engine(dir.path());
    let schema = create_schema();
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));
    let client = connect(port).await;

    let collect = |msgs: Vec<tokio_postgres::SimpleQueryMessage>| {
        msgs.into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect::<Vec<_>>()
    };

    // Boolean WHERE (AND): exactly order 10 belongs to uid 1.
    let anded = collect(
        client
            .simple_query("SELECT o.oid FROM orders o WHERE o.uid = 1 AND o.oid = 10")
            .await
            .expect("WHERE AND should work"),
    );
    assert_eq!(anded.len(), 1);
    assert_eq!(anded[0].get(0), Some("10"));

    // Boolean WHERE (OR + parens): all three orders.
    let ored = collect(
        client
            .simple_query("SELECT o.oid FROM orders o WHERE (o.uid = 1 OR o.uid = 2)")
            .await
            .expect("WHERE OR should work"),
    );
    assert_eq!(ored.len(), 3);

    // HAVING filters groups by aggregate: only uid 1 has more than one order.
    let having = collect(
        client
            .simple_query("SELECT o.uid, COUNT(*) FROM orders o GROUP BY o.uid HAVING COUNT(*) > 1")
            .await
            .expect("HAVING should work"),
    );
    assert_eq!(having.len(), 1);
    assert_eq!((having[0].get(0), having[0].get(1)), (Some("1"), Some("2")));

    // DISTINCT collapses the two uid=1 orders to one distinct uid.
    let distinct = collect(
        client
            .simple_query("SELECT DISTINCT o.uid FROM orders o ORDER BY o.uid")
            .await
            .expect("DISTINCT should work"),
    );
    assert_eq!(distinct.len(), 2, "two distinct uids");
    assert_eq!(distinct[0].get(0), Some("1"));
    assert_eq!(distinct[1].get(0), Some("2"));
}

// ── Extended-protocol DML (parameterized INSERT/UPDATE/DELETE + RETURNING) ──────
//
// These exercise the Parse → Bind($N) → Execute path that `Ecto.Repo`'s
// insert/update/delete/all calls drive: tokio-postgres `execute`/`query` with
// bound parameters, over the same in-memory engine + `users(id int PK, name
// text)` fixture. They are the end-to-end evidence for `feat/pg-extended-crud`.

/// Spin up a fresh server over the seeded engine and return a connected driver
/// client (+ the tempdir to keep storage alive for the test's lifetime). No
/// Accord committer — autocommit DML applies immediately; a COMMIT carrying
/// buffered DML would fail loud (cluster mode required).
async fn dml_client() -> (tokio_postgres::Client, tempfile::TempDir) {
    let (client, _, dir, _) = dml_client_with_committer(false).await;
    (client, dir)
}

/// As [`dml_client`], but `with_committer` installs a
/// [`MockTransactionCommitter`] in the `QueryContext` so a `BEGIN`/`COMMIT`
/// block commits its buffered write-set (the mock records it and reports
/// `Committed`) instead of failing loud for missing cluster mode.
async fn dml_client_with_committer(
    with_committer: bool,
) -> (
    tokio_postgres::Client,
    tokio_postgres::Client,
    tempfile::TempDir,
    Option<Arc<ferrosa_storage::accord::MockTransactionCommitter>>,
) {
    use ferrosa_storage::accord::MockTransactionCommitter;
    let dir = tempfile::tempdir().unwrap();
    let engine = seed_engine(dir.path());
    let schema = create_schema();
    let accord_committer = if with_committer {
        Some(Arc::new(MockTransactionCommitter::new()))
    } else {
        None
    };
    // The fixture owns the Accord plumbing (a mock here, a real state machine
    // elsewhere), so it bypasses the live-mode gate with `fixed`.
    let accord = match accord_committer.as_ref() {
        Some(committer) => {
            let committer: Arc<dyn ferrosa_storage::accord::TransactionCommitter> =
                committer.clone();
            AccordAccess::fixed(committer)
        }
        None => AccordAccess::disabled(),
    };
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord,
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));
    (
        connect(port).await,
        connect(port).await,
        dir,
        accord_committer,
    )
}

/// Native PostgreSQL client wired to a real single-replica Accord committer.
/// The in-process replica runs the production Accord state machine and storage
/// applier; only peer transport is unused for this RF=1 fixture.
async fn dml_client_with_local_accord() -> (
    tokio_postgres::Client,
    tokio_postgres::Client,
    tempfile::TempDir,
) {
    use ferrosa_cluster::accord::{
        AccordStateMachine, AccordTransactionCommitter, EngineStorageApplier, ReplicaResolver,
    };
    use ferrosa_common::accord::HybridLogicalClock;
    use ferrosa_net::{
        codec::Lane,
        error::{NetError, Result as NetResult},
        message::Message,
    };
    use ferrosa_storage::accord::{sync_writer::MockSyncWriter, TransactionCommitter};

    struct NoPeerTransport;
    #[async_trait::async_trait]
    impl ferrosa_cluster::accord::transport::AccordTransport for NoPeerTransport {
        async fn send(&self, _host: Uuid, _msg: Message, _lane: Lane) -> NetResult<Message> {
            Err(NetError::Timeout(
                "RF=1 fixture must not send to peers".into(),
            ))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(seed_engine(dir.path()));
    let schema = Arc::new(create_schema());
    let host = Uuid::from_u128(0xA11CE);
    let node_id = u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap());
    let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
    let applier = Arc::new(EngineStorageApplier::new(engine.clone()));
    let state = Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
        node_id,
        Arc::new(MockSyncWriter::new()),
        applier.clone(),
    )));
    let resolve: ReplicaResolver = Arc::new(move |_keyspace: &str, _key: &[u8]| Some(vec![host]));
    let committer = Arc::new(
        AccordTransactionCommitter::new(
            node_id,
            clock,
            Arc::new(NoPeerTransport),
            applier,
            resolve,
        )
        .with_local_accord_state(state),
    );
    let query_committer: Arc<dyn TransactionCommitter> = committer;
    let ctx = Arc::new(QueryContext {
        engine,
        schema,
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        // The real in-process Accord state machine below IS this fixture's
        // cluster, so offer it unconditionally.
        accord: AccordAccess::fixed(query_committer),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(
        listener,
        dev_store(),
        ctx,
        server::PgTls::plaintext(),
    ));
    (connect(port).await, connect(port).await, dir)
}

/// In-process network used by the two-node PostgreSQL/Accord test. Messages go
/// through each node's real AccordHandler and state machine.
struct AccordTestTransport {
    handlers: HashMap<Uuid, Arc<ferrosa_cluster::accord::AccordHandler>>,
}

#[async_trait::async_trait]
impl ferrosa_cluster::accord::transport::AccordTransport for AccordTestTransport {
    async fn send(
        &self,
        host: Uuid,
        msg: ferrosa_net::message::Message,
        _lane: ferrosa_net::codec::Lane,
    ) -> ferrosa_net::error::Result<ferrosa_net::message::Message> {
        use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
        let handler = self
            .handlers
            .get(&host)
            .ok_or_else(|| ferrosa_net::error::NetError::Timeout("unknown test peer".into()))?;
        let peer: PeerId = (host, "127.0.0.1:0".parse().unwrap());
        handler
            .handle(peer, msg)
            .await
            .ok_or_else(|| ferrosa_net::error::NetError::Timeout("no test-peer response".into()))
    }
}

async fn dml_clients_on_two_accord_nodes() -> (
    tokio_postgres::Client,
    tokio_postgres::Client,
    [tempfile::TempDir; 2],
) {
    use ferrosa_cluster::accord::{AccordHandler, AccordStateMachine, EngineStorageApplier};
    use ferrosa_storage::accord::sync_writer::MockSyncWriter;

    fn new_node(
        host: Uuid,
        engine: Arc<StorageEngine>,
    ) -> (
        Arc<ferrosa_cluster::accord::AccordHandler>,
        Arc<EngineStorageApplier>,
        Arc<parking_lot::Mutex<AccordStateMachine>>,
    ) {
        let node_id = u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap());
        let applier = Arc::new(EngineStorageApplier::new(engine));
        let state = Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
            node_id,
            Arc::new(MockSyncWriter::new()),
            applier.clone(),
        )));
        (
            Arc::new(AccordHandler::new(state.clone(), node_id)),
            applier,
            state,
        )
    }

    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let engines = [
        Arc::new(seed_engine(dirs[0].path())),
        Arc::new(seed_engine(dirs[1].path())),
    ];
    let schema = Arc::new(create_schema());
    // Accord derives the numeric node id from each UUID's first eight bytes.
    // Put the distinguishing value there; low-only UUIDs would both derive id 0
    // and make the two real replicas impersonate the same host in the driver.
    let hosts = [
        Uuid::from_u128(0xA000_0000_0000_0000_0000_0000_0000_0001),
        Uuid::from_u128(0xB000_0000_0000_0000_0000_0000_0000_0002),
    ];
    let nodes = [
        new_node(hosts[0], engines[0].clone()),
        new_node(hosts[1], engines[1].clone()),
    ];
    let transport: Arc<dyn ferrosa_cluster::accord::transport::AccordTransport> =
        Arc::new(AccordTestTransport {
            handlers: HashMap::from([
                (hosts[0], nodes[0].0.clone()),
                (hosts[1], nodes[1].0.clone()),
            ]),
        });

    async fn start_server(
        host: Uuid,
        engine: Arc<StorageEngine>,
        schema: Arc<Schema>,
        applier: Arc<ferrosa_cluster::accord::EngineStorageApplier>,
        transport: Arc<dyn ferrosa_cluster::accord::transport::AccordTransport>,
        hosts: [Uuid; 2],
        local_state: Arc<parking_lot::Mutex<ferrosa_cluster::accord::AccordStateMachine>>,
    ) -> tokio_postgres::Client {
        use ferrosa_cluster::accord::{AccordTransactionCommitter, ReplicaResolver};
        use ferrosa_common::accord::HybridLogicalClock;
        use ferrosa_storage::accord::TransactionCommitter;

        let node_id = u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap());
        let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
        let replicas = hosts.to_vec();
        let resolve: ReplicaResolver =
            Arc::new(move |_keyspace: &str, _key: &[u8]| Some(replicas.clone()));
        let committer = Arc::new(
            AccordTransactionCommitter::new(node_id, clock, transport, applier, resolve)
                .with_local_accord_state(local_state),
        );
        let query_committer: Arc<dyn TransactionCommitter> = committer;
        let ctx = Arc::new(QueryContext {
            engine,
            schema,
            default_schema: "public".into(),
            mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
            // Two real Accord nodes are the cluster here.
            accord: AccordAccess::fixed(query_committer),
            ddl: None,
            jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(server::serve(
            listener,
            dev_store(),
            ctx,
            server::PgTls::plaintext(),
        ));
        connect(port).await
    }

    let client_a = start_server(
        hosts[0],
        engines[0].clone(),
        schema.clone(),
        nodes[0].1.clone(),
        transport.clone(),
        hosts,
        nodes[0].2.clone(),
    )
    .await;
    let client_b = start_server(
        hosts[1],
        engines[1].clone(),
        schema,
        nodes[1].1.clone(),
        transport,
        hosts,
        nodes[1].2.clone(),
    )
    .await;
    (client_a, client_b, dirs)
}

#[tokio::test]
async fn extended_parameterized_insert_writes_row() {
    let (client, _dir) = dml_client().await;

    // Parse → Bind($1=42, $2='carol') → Execute. tokio-postgres `execute`
    // returns the affected-row count parsed from the CommandComplete tag.
    let n = client
        .execute(
            "INSERT INTO users (id, name) VALUES ($1, $2)",
            &[&42i32, &"carol"],
        )
        .await
        .expect("parameterized INSERT should apply");
    assert_eq!(n, 1, "INSERT 0 1 ⇒ one affected row");

    // The row is now readable back over the same connection.
    let rows = client
        .query("SELECT name FROM users WHERE id = $1", &[&42i32])
        .await
        .expect("the inserted row should be readable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "carol");
}

#[tokio::test]
async fn extended_insert_returning_id_yields_a_data_row() {
    let (client, _dir) = dml_client().await;

    // INSERT ... RETURNING id: the row Ecto reads back to recover a key. The
    // returned value is the value just written (no storage read-back).
    let rows = client
        .query(
            "INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id",
            &[&7i32, &"dave"],
        )
        .await
        .expect("INSERT RETURNING should return the new row");
    assert_eq!(rows.len(), 1, "RETURNING yields exactly the inserted row");
    assert_eq!(rows[0].get::<_, i32>(0), 7, "RETURNING id echoes the key");
}

#[tokio::test]
async fn extended_insert_returning_star_yields_all_columns() {
    let (client, _dir) = dml_client().await;
    let rows = client
        .query(
            "INSERT INTO users (id, name) VALUES ($1, $2) RETURNING *",
            &[&9i32, &"erin"],
        )
        .await
        .expect("INSERT RETURNING * should return all columns");
    assert_eq!(rows.len(), 1);
    // Column order follows the table schema: id (PK) then name.
    assert_eq!(rows[0].get::<_, i32>("id"), 9);
    assert_eq!(rows[0].get::<_, &str>("name"), "erin");
}

#[tokio::test]
async fn extended_parameterized_update_applies() {
    let (client, _dir) = dml_client().await;

    // alice (id=1) starts as "alice"; UPDATE her name via bound params.
    let n = client
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"alice2", &1i32],
        )
        .await
        .expect("parameterized UPDATE should apply");
    assert_eq!(n, 1, "UPDATE 1 ⇒ one affected row");

    let rows = client
        .query("SELECT name FROM users WHERE id = $1", &[&1i32])
        .await
        .expect("the updated row should be readable");
    assert_eq!(rows[0].get::<_, &str>(0), "alice2");
}

#[tokio::test]
async fn extended_parameterized_delete_applies() {
    let (client, _dir) = dml_client().await;

    // bob (id=2) exists in the fixture; DELETE him via a bound param.
    let n = client
        .execute("DELETE FROM users WHERE id = $1", &[&2i32])
        .await
        .expect("parameterized DELETE should apply");
    assert_eq!(n, 1, "DELETE 1 ⇒ one affected row");

    let rows = client
        .query("SELECT name FROM users WHERE id = $1", &[&2i32])
        .await
        .expect("query after delete should succeed");
    assert!(rows.is_empty(), "the deleted row is gone");
}

#[tokio::test]
async fn extended_update_returning_is_unsupported_fail_loud() {
    let (client, _dir) = dml_client().await;
    // UPDATE ... RETURNING is out of scope this PR: a clear 0A000, not a silent
    // drop of the RETURNING clause.
    let err = client
        .query(
            "UPDATE users SET name = $1 WHERE id = $2 RETURNING id",
            &[&"x", &1i32],
        )
        .await
        .expect_err("UPDATE RETURNING must fail loud");
    assert_eq!(
        err.code().map(|c| c.code()),
        Some("0A000"),
        "UPDATE RETURNING should be feature_not_supported, got: {err}"
    );
    // The connection survives (its own Sync/ReadyForQuery): a plain read works.
    let ok = client
        .query("SELECT name FROM users WHERE id = $1", &[&1i32])
        .await
        .expect("session usable after the fail-loud error");
    assert_eq!(ok.len(), 1);
}

#[tokio::test]
async fn extended_delete_returning_is_unsupported_fail_loud() {
    let (client, _dir) = dml_client().await;
    let err = client
        .query("DELETE FROM users WHERE id = $1 RETURNING id", &[&1i32])
        .await
        .expect_err("DELETE RETURNING must fail loud");
    assert_eq!(
        err.code().map(|c| c.code()),
        Some("0A000"),
        "DELETE RETURNING should be feature_not_supported, got: {err}"
    );
}

#[tokio::test]
async fn extended_dml_in_transaction_rollback_discards_buffered_write() {
    // Extended-protocol DML inside a BEGIN block is BUFFERED (not applied), and
    // ROLLBACK discards the buffer — the write is never applied. The INSERT
    // RETURNING still returns its row (built from the in-memory values) while
    // the write would only commit at COMMIT. A committer is installed so the
    // write buffers cleanly rather than tripping the standalone fail-loud path.
    let (client, _, _dir, _) = dml_client_with_committer(true).await;

    client.batch_execute("BEGIN").await.expect("BEGIN");

    // INSERT ... RETURNING inside the txn returns its row now (buffered write).
    let rows = client
        .query(
            "INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id",
            &[&100i32, &"frank"],
        )
        .await
        .expect("buffered INSERT RETURNING returns its row");
    assert_eq!(rows.len(), 1, "RETURNING yields the buffered row");
    assert_eq!(rows[0].get::<_, i32>(0), 100);

    // ROLLBACK discards the buffer — the write was never applied.
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");

    // Nothing was applied: the targeted row never appeared in storage.
    let after = client
        .query("SELECT name FROM users WHERE id = $1", &[&100i32])
        .await
        .expect("read after rollback should succeed");
    assert!(
        after.is_empty(),
        "the buffered in-txn INSERT was discarded by ROLLBACK, not applied"
    );
}

#[tokio::test]
async fn extended_dml_in_transaction_commit_drives_the_committer() {
    // Extended-protocol DML inside a BEGIN block buffers; COMMIT drives the
    // buffered write-set through the Accord committer. With a committer wired,
    // COMMIT succeeds cleanly (not the standalone `0A000`). The atomic-apply
    // semantics themselves are unit-tested in `server::txn_atomicity_tests`
    // against a real engine; here we prove the extended path reaches COMMIT.
    let (client, _, _dir, _) = dml_client_with_committer(true).await;

    client.batch_execute("BEGIN").await.expect("BEGIN");
    let n = client
        .execute(
            "INSERT INTO users (id, name) VALUES ($1, $2)",
            &[&101i32, &"grace"],
        )
        .await
        .expect("buffered INSERT acks inside the txn");
    assert_eq!(n, 1, "INSERT 0 1 ⇒ one buffered row");

    // COMMIT drives the write-set through the committer and succeeds.
    client
        .batch_execute("COMMIT")
        .await
        .expect("COMMIT of a buffered write-set should succeed with a committer");
}

#[tokio::test]
async fn native_drivers_commit_serializable_transactions_through_accord() {
    let (client_a, client_b, _dir) = dml_client_with_local_accord().await;

    client_a
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin first serializable transaction");
    client_b
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin second serializable transaction");
    client_a
        .query("SELECT name FROM users WHERE id = $1", &[&1i32])
        .await
        .expect("first transaction reads");
    client_b
        .query("SELECT name FROM users WHERE id = $1", &[&1i32])
        .await
        .expect("second transaction reads");
    client_a
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"ivan", &1i32],
        )
        .await
        .expect("first transaction buffers an update");
    client_b
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"jane", &2i32],
        )
        .await
        .expect("second transaction buffers a disjoint update");
    client_a
        .batch_execute("COMMIT")
        .await
        .expect("first serializable transaction commits");
    let conflict = client_b
        .batch_execute("COMMIT")
        .await
        .expect_err("stale serializable transaction must abort");
    assert_eq!(conflict.code().map(|code| code.code()), Some("40001"));

    let final_row = client_b
        .query_one("SELECT name FROM users WHERE id = $1", &[&1i32])
        .await
        .expect("read the committed row through the second native client");
    assert_eq!(final_row.get::<_, &str>(0), "ivan");
}

#[tokio::test]
async fn native_driver_transaction_applies_through_real_accord_state_machine() {
    let (writer, reader, _dir) = dml_client_with_local_accord().await;

    writer
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin serializable transaction");
    let changed = writer
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"accord-applied", &1i32],
        )
        .await
        .expect("buffer update before commit");
    assert_eq!(changed, 1);
    writer
        .batch_execute("COMMIT")
        .await
        .expect("real Accord transaction commits and applies the mutation");

    let rows = reader
        .query("SELECT name FROM users WHERE id = $1", &[&1i32])
        .await
        .expect("committed row is visible through another native client");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "accord-applied");
}

#[tokio::test]
async fn cross_node_serializable_predicate_conflict_aborts_one_native_transaction() {
    let (client_a, client_b, _dirs) = dml_clients_on_two_accord_nodes().await;
    client_a
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin node A transaction");
    client_b
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin node B transaction");

    let rows_a = client_a
        .query("SELECT id, name FROM users", &[])
        .await
        .expect("node A reads predicate");
    let rows_b = client_b
        .query("SELECT id, name FROM users", &[])
        .await
        .expect("node B reads predicate");
    assert_eq!(rows_a.len(), 2);
    assert_eq!(rows_b.len(), 2);

    client_a
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"alice-a", &1i32],
        )
        .await
        .expect("node A buffers update");
    client_b
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"bob-b", &2i32],
        )
        .await
        .expect("node B buffers disjoint update");

    client_a
        .batch_execute("COMMIT")
        .await
        .expect("first transaction commits");
    let conflict = client_b
        .batch_execute("COMMIT")
        .await
        .expect_err("cross-node stale predicate must cause a serialization failure");
    assert_eq!(conflict.code().map(|code| code.code()), Some("40001"));

    let final_a = client_a
        .query("SELECT id, name FROM users ORDER BY id", &[])
        .await
        .expect("read final rows through native PostgreSQL driver");
    assert_eq!(final_a[0].get::<_, &str>(1), "alice-a");
    assert_eq!(final_a[1].get::<_, &str>(1), "bob");
}

#[tokio::test]
async fn cross_node_serializable_transaction_keeps_its_snapshot_after_peer_commit() {
    let (client_a, client_b, _dirs) = dml_clients_on_two_accord_nodes().await;
    client_b
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin reader transaction on node B");

    let initial = client_b
        .query("SELECT id, name FROM users ORDER BY id", &[])
        .await
        .expect("read initial snapshot on node B");
    assert_eq!(initial.len(), 2);
    assert_eq!(initial[0].get::<_, &str>(1), "alice");
    assert_eq!(initial[1].get::<_, &str>(1), "bob");

    client_a
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin multi-row writer transaction on node A");
    client_a
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"alice-updated", &1i32],
        )
        .await
        .expect("buffer first row update on node A");
    client_a
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"bob-updated", &2i32],
        )
        .await
        .expect("buffer second row update on node A");
    client_a
        .batch_execute("COMMIT")
        .await
        .expect("commit both row updates on node A");

    let retained_snapshot = client_b
        .query("SELECT id, name FROM users ORDER BY id", &[])
        .await
        .expect("repeat scan must use the transaction's retained snapshot");
    assert_eq!(retained_snapshot.len(), 2);
    assert_eq!(retained_snapshot[0].get::<_, &str>(1), "alice");
    assert_eq!(retained_snapshot[1].get::<_, &str>(1), "bob");

    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("discard the read-only transaction after observing its stable snapshot");

    client_a
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin a fresh reader after the writer committed");
    let committed_snapshot = client_a
        .query("SELECT id, name FROM users ORDER BY id", &[])
        .await
        .expect("read the complete committed write-set");
    assert_eq!(committed_snapshot.len(), 2);
    assert_eq!(committed_snapshot[0].get::<_, &str>(1), "alice-updated");
    assert_eq!(committed_snapshot[1].get::<_, &str>(1), "bob-updated");
    client_a
        .batch_execute("COMMIT")
        .await
        .expect("commit fresh read-only transaction");
}

#[tokio::test]
async fn unsupported_explicit_isolation_levels_fail_loud() {
    let (client, _dir) = dml_client().await;

    for statement in [
        "BEGIN ISOLATION LEVEL READ COMMITTED",
        "BEGIN ISOLATION LEVEL REPEATABLE READ",
    ] {
        let error = client
            .batch_execute(statement)
            .await
            .expect_err("unsupported explicit isolation level must be rejected");
        assert_eq!(
            error.code().map(|code| code.code()),
            Some("0A000"),
            "unsupported isolation level must use feature_not_supported: {statement}"
        );
    }
}

#[tokio::test]
async fn serializable_begin_after_peer_commit_reads_the_committed_value() {
    let (client_a, client_b, _dirs) = dml_clients_on_two_accord_nodes().await;

    client_a
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin writer transaction");
    client_a
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"alice-committed", &1i32],
        )
        .await
        .expect("buffer writer update");
    client_a
        .batch_execute("COMMIT")
        .await
        .expect("writer commit completes before the second transaction begins");

    client_b
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin reader after the writer's successful COMMIT response");
    let row = client_b
        .query_one("SELECT name FROM users WHERE id = 1", &[])
        .await
        .expect("read after a real-time predecessor transaction");
    assert_eq!(row.get::<_, &str>(0), "alice-committed");
    client_b
        .batch_execute("COMMIT")
        .await
        .expect("read-only transaction commits in real-time order");
}

#[tokio::test]
async fn cross_node_lost_update_aborts_without_overwriting_the_winner() {
    let (client_a, client_b, _dirs) = dml_clients_on_two_accord_nodes().await;

    client_a
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin transaction on node A");
    client_b
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin transaction on node B");
    assert_eq!(
        client_a
            .query_one("SELECT name FROM users WHERE id = 1", &[])
            .await
            .expect("node A reads before either write")
            .get::<_, &str>(0),
        "alice"
    );
    assert_eq!(
        client_b
            .query_one("SELECT name FROM users WHERE id = 1", &[])
            .await
            .expect("node B reads the same initial value")
            .get::<_, &str>(0),
        "alice"
    );

    client_a
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"winner", &1i32],
        )
        .await
        .expect("node A buffers the winning update");
    client_b
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"stale-loser", &1i32],
        )
        .await
        .expect("node B buffers a conflicting update");

    client_a
        .batch_execute("COMMIT")
        .await
        .expect("first update commits");
    let error = client_b
        .batch_execute("COMMIT")
        .await
        .expect_err("second update based on a stale snapshot must abort");
    assert_eq!(error.code().map(|code| code.code()), Some("40001"));

    let winner = client_b
        .query_one("SELECT name FROM users WHERE id = 1", &[])
        .await
        .expect("read committed winner after rejected transaction");
    assert_eq!(winner.get::<_, &str>(0), "winner");
}

#[tokio::test]
async fn cluster_autocommit_reads_do_not_advance_the_postgres_commit_marker() {
    let (client_a, client_b, _dirs) = dml_clients_on_two_accord_nodes().await;
    client_b
        .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("begin snapshot transaction on node B");

    client_a
        .batch_execute("SELECT id FROM users")
        .await
        .expect("run read-only autocommit query on node A");

    let row = client_b
        .query_one("SELECT name FROM users WHERE id = 1", &[])
        .await
        .expect("read-only autocommit must not stale node B snapshot");
    assert_eq!(row.get::<_, &str>(0), "alice");
    client_b
        .batch_execute("ROLLBACK")
        .await
        .expect("end snapshot transaction");
}

#[tokio::test]
async fn cluster_autocommit_simple_and_extended_dml_commit_through_accord() {
    let (client_a, client_b, _dirs) = dml_clients_on_two_accord_nodes().await;

    client_a
        .execute(
            "UPDATE users SET name = $1 WHERE id = $2",
            &[&"alice-extended", &1i32],
        )
        .await
        .expect("commit extended-protocol autocommit through Accord");
    let row = client_b
        .query_one("SELECT name FROM users WHERE id = 1", &[])
        .await
        .expect("observe extended-protocol autocommit on peer");
    assert_eq!(row.get::<_, &str>(0), "alice-extended");

    client_b
        .batch_execute("UPDATE users SET name = 'bob-simple' WHERE id = 2")
        .await
        .expect("commit simple-protocol autocommit through Accord");
    let row = client_a
        .query_one("SELECT name FROM users WHERE id = 2", &[])
        .await
        .expect("observe simple-protocol autocommit on peer");
    assert_eq!(row.get::<_, &str>(0), "bob-simple");
}

#[tokio::test]
async fn standalone_transaction_commit_uses_local_mvcc() {
    // Standalone has no Accord peers, so its transaction is ordered and applied
    // by the process-local MVCC manager. Cluster mode takes the Accord path.
    let (client, _dir) = dml_client().await; // no committer

    client.batch_execute("BEGIN").await.expect("BEGIN");
    let n = client
        .execute(
            "INSERT INTO users (id, name) VALUES ($1, $2)",
            &[&102i32, &"heidi"],
        )
        .await
        .expect("buffered INSERT acks inside the txn");
    assert_eq!(n, 1, "the write buffers (acks) inside the txn");

    client
        .batch_execute("COMMIT")
        .await
        .expect("standalone MVCC transaction commit");

    // The local MVCC commit is visible to subsequent statements.
    let after = client
        .query("SELECT name FROM users WHERE id = $1", &[&102i32])
        .await
        .expect("read after failed commit should succeed");
    assert!(
        after.len() == 1 && after[0].get::<_, &str>(0) == "heidi",
        "the committed standalone MVCC write is visible"
    );
}
