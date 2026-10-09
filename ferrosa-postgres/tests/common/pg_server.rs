//! A real PG listener over a real storage engine, seeded with `ks.t`, for the
//! liveness tests that need the whole stack between socket and storage
//! producer.
//!
//! Each test binary includes this with `#[path]`, and not every binary uses
//! every helper.
#![allow(dead_code)]

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

/// The partition key of seeded row `i`.
pub fn row_id(i: usize) -> String {
    format!("row{i:08}")
}

/// Write row `i` (`score = i`) through the storage engine.
pub fn write_row(engine: &StorageEngine, i: usize) {
    let tid = TableId::new("ks", "t");
    let key = DecoratedKey::new(PartitionKey::new(row_id(i).into_bytes()));
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

/// A running server: its port, its engine (for writes behind the PG layer's
/// back) and the data directory that must outlive it.
pub struct TestServer {
    pub port: u16,
    pub engine: Arc<StorageEngine>,
    /// The listener's suspended-portal accounting.
    pub portals: Arc<ferrosa_postgres::SuspendedPortals>,
    pub dir: tempfile::TempDir,
}

/// Start a PG listener over a fresh engine seeded with `rows` partitions.
pub async fn start_server(rows: usize) -> TestServer {
    start_server_with(rows, ferrosa_postgres::PortalLimits::default()).await
}

/// [`start_server`] with explicit suspended-portal limits.
pub async fn start_server_with(rows: usize, limits: ferrosa_postgres::PortalLimits) -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = StorageEngine::new(engine_config(dir.path()), None).expect("engine");
    engine.register_table(storage_schema()).expect("register");
    for i in 0..rows {
        write_row(&engine, i);
    }
    let engine = Arc::new(engine);
    let portals = Arc::new(ferrosa_postgres::SuspendedPortals::new(limits));
    let ctx = Arc::new(QueryContext {
        engine: Arc::clone(&engine),
        schema: Arc::new(schema_with_table()),
        default_schema: "ks".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        truncate: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
        portals: Arc::clone(&portals),
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
    TestServer {
        port,
        engine,
        portals,
        dir,
    }
}

/// A SCRAM-authenticated `tokio-postgres` client.
pub async fn connect(port: u16) -> tokio_postgres::Client {
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
