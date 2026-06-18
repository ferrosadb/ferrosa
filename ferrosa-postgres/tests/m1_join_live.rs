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
use ferrosa_postgres::{server, QueryContext};
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

impl VerifierStore for OneRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == self.user).then(|| self.verifier.clone())
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
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(listener, dev_store(), ctx));

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
