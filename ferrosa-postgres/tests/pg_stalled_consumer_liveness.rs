//! A client that stops reading must not stall other clients' queries.
//!
//! `result_stream` (t_f348ba0b) made the executor's output stream to the
//! socket with backpressure. The backpressure reaches all the way down: a
//! portal suspended by `max_rows`, or a client that stops draining its socket,
//! parks the PG executor, which parks the PG scan producer, which parks the
//! storage range-scan producer in `blocking_send` — and that producer runs
//! inside a slot of the process-global bounded scan pool (`ferrosa-sched`,
//! `cores - reserved` slots). Before streaming, the result was collected first,
//! so the slot was released before the first byte was written.
//!
//! The invariant pinned here: a stalled consumer can cost its own query's
//! resources, but it cannot hold a node-wide scan slot hostage. With every
//! slot held by a suspended portal, another session's scan must still finish.

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

/// Scan-pool slots for this test binary. Each integration test file is its own
/// process, so this reservation is the only one the global pool ever sees.
const POOL_SLOTS: usize = 2;

/// Partitions in the table: far more than every buffer between the storage
/// producer and the socket (storage channel, PG scan channel, result batches),
/// so a suspended portal's producers are genuinely parked mid-scan.
const ROWS: usize = 4_000;

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

/// `ks.t(id text PK, ck int CK, name text, score int)`.
fn schema_with_table() -> Schema {
    let schema = Schema::new(schema_config()).expect("schema bootstraps");
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
                name: "ks".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options,
                },
            },
            &auth,
        )
        .expect("create keyspace");
    let mut columns = IndexMap::new();
    for (name, kind, ty) in [
        ("id", ColumnKind::PartitionKey, "text"),
        ("ck", ColumnKind::Clustering, "int"),
        ("name", ColumnKind::Regular, "text"),
        ("score", ColumnKind::Regular, "int"),
    ] {
        columns.insert(name.to_string(), column(name, kind, ty));
    }
    schema
        .create_table(
            TableMetadata {
                keyspace: "ks".to_string(),
                name: "t".to_string(),
                id: Uuid::new_v4(),
                columns,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![("ck".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table");
    schema
}

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            // Durability is not under test; an fsync per seeded row made
            // startup take ~40 s.
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: Duration::from_secs(1),
            },
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 64 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
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

fn storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    let col = |name: &str, ty: &str| ColumnDefinition {
        name: name.to_string(),
        type_name: ty.to_string(),
    };
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![col("ck", "org.apache.cassandra.db.marshal.Int32Type")],
        static_columns: vec![],
        regular_columns: vec![
            col("name", "org.apache.cassandra.db.marshal.UTF8Type"),
            col("score", "org.apache.cassandra.db.marshal.Int32Type"),
        ],
        extensions: Default::default(),
    }
}

fn seed(engine: &StorageEngine) {
    let tid = TableId::new("ks", "t");
    for i in 0..ROWS {
        let key = DecoratedKey::new(PartitionKey::new(format!("row{i:08}").into_bytes()));
        let ts = 1000 + i as i64;
        let row = StorageRow {
            clustering: 1i32.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live(b"x".to_vec(), ts)),
                (1, CellValue::live((i as i32).to_be_bytes().to_vec(), ts)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        engine.write(&tid, &key, row, ts).expect("write row");
    }
}

async fn start_server() -> (u16, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = StorageEngine::new(engine_config(dir.path()), None).expect("engine");
    engine.register_table(storage_schema()).expect("register");
    seed(&engine);
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema_with_table()),
        default_schema: "ks".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
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
    (port, dir)
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
        .expect("SCRAM handshake succeeds");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("driver connection ended: {error}");
        }
    });
    client
}

/// Every scan-pool slot held by a suspended portal must not stop another
/// session's full scan from completing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_portals_do_not_starve_other_sessions_scans() {
    let pool = ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(POOL_SLOTS + 1, 1));
    assert_eq!(pool.capacity(), POOL_SLOTS, "this binary owns the pool");
    let (port, _dir) = start_server().await;

    // One idle client per slot, each with a portal suspended after one row.
    // The driver keeps the transaction (and so the portal) open while `tx`
    // lives; the client then simply stops asking for rows.
    let mut idle = Vec::new();
    for _ in 0..POOL_SLOTS {
        idle.push(connect(port).await);
    }
    let mut parked = Vec::new();
    for client in &mut idle {
        let tx = client.transaction().await.expect("begin");
        let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
        let portal = tx.bind(&statement, &[]).await.expect("bind");
        let first = tx.query_portal(&portal, 1).await.expect("execute");
        assert_eq!(first.len(), 1, "the portal suspends after max_rows");
        parked.push((portal, tx));
    }
    // Every slot is held for as long as a parked producer waits for room.
    tokio::time::sleep(Duration::from_millis(500)).await;
    // The premise: each suspended portal's storage producer really is blocked
    // on its consumer. Without this, a buffer large enough to hold the whole
    // table would make the test pass without exercising anything.
    assert!(
        ferrosa_sched::scan_parks_total() >= POOL_SLOTS as u64,
        "only {} of {POOL_SLOTS} suspended scans blocked on their consumer",
        ferrosa_sched::scan_parks_total()
    );

    let other = connect(port).await;
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        other.query("SELECT id FROM t", &[]),
    )
    .await;
    let rows = match outcome {
        Ok(result) => result.expect("the other session's scan succeeds"),
        Err(_) => panic!(
            "another session's scan made no progress in 20 s while {POOL_SLOTS} \
             suspended portals held every scan-pool slot (active = {})",
            pool.active()
        ),
    };
    assert_eq!(rows.len(), ROWS, "every row is returned");
    drop(parked);
}
