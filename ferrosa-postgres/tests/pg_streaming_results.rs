//! Streaming result delivery over the Postgres wire (forge t_f348ba0b,
//! FMEA PG-Tf348ba0b).
//!
//! The executor's operators stream and spill, and the storage scan streams
//! (`pg_full_scan_memory_bound.rs`). What remained was the boundary: the front
//! end collected the executor's output into `QueryResult.rows`, then rendered a
//! `Vec<BackendMessage>` holding every `DataRow`, before writing the first byte.
//! A `SELECT` over a table larger than the heap therefore OOMed the server, and
//! the upcoming jsonb set-returning functions can produce unbounded row counts.
//!
//! These tests drive a real driver (`tokio-postgres`) against the real server and
//! pin the behavior a materializing boundary cannot have:
//!
//! 1. A result larger than the memory budget completes, every row arrives, and
//!    the peak of live heap bytes stays under the budget.
//! 2. `Execute` with `max_rows = N` returns N rows and suspends; the next
//!    `Execute` continues exactly where it stopped — no gap, no duplicate, in
//!    the same order as an unlimited run.
//!
//! The mid-stream error and `Close` cases need the stream itself and are pinned
//! next to it (`result_stream.rs`, `extended.rs`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
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
use futures::TryStreamExt;
use indexmap::IndexMap;
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
//
// The same tracker `pg_full_scan_memory_bound.rs` uses. Each integration test
// file is its own binary, so this `#[global_allocator]` affects nothing else.
// Server and client share this process, so the window covers both; the client
// side below folds rows into an O(1) digest, so what it counts is the server's.

struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: frees of memory allocated before the window must
            // not drive the counter negative and hide later allocations.
            LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            })
            .ok();
        }
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static GLOBAL: TrackingAlloc = TrackingAlloc;

// --- fixture -----------------------------------------------------------------

/// Payload per row: large enough that holding the result is unmistakable.
const VALUE_BYTES: usize = 16 * 1024;

/// Rows in the table: 12 MiB of payload, far past the streaming window.
const ROWS: usize = 768;

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

fn column(name: &str, kind: ColumnKind, ty: &str, position: i32) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position,
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
        columns.insert(name.to_string(), column(name, kind, ty, 0));
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

/// One partition per row, each carrying a `VALUE_BYTES` payload in `name`.
fn seed(engine: &StorageEngine) {
    let tid = TableId::new("ks", "t");
    for i in 0..ROWS {
        let id = format!("row{i:08}");
        let key = DecoratedKey::new(PartitionKey::new(id.into_bytes()));
        let ts = 1000 + i as i64;
        let row = StorageRow {
            clustering: 1i32.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live("x".repeat(VALUE_BYTES).into_bytes(), ts)),
                (1, CellValue::live((i as i32).to_be_bytes().to_vec(), ts)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        engine.write(&tid, &key, row, ts).expect("write row");
    }
}

/// Start a server over a seeded engine and return a connected driver client.
async fn start() -> (tokio_postgres::Client, tempfile::TempDir) {
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
    (client, dir)
}

// --- the guards --------------------------------------------------------------

/// A `SELECT` larger than the memory budget completes with every row, and the
/// live heap never approaches the size of the result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_larger_than_the_budget_streams_within_it() {
    let (client, _dir) = start().await;

    // Fold rows into an O(1) digest as they arrive: the driver's `query` would
    // collect them and put the whole result back on the heap being measured.
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let outcome = async {
        let rows = client
            .query_raw("SELECT id, name, score FROM t", Vec::<String>::new())
            .await?;
        futures::pin_mut!(rows);
        let (mut count, mut payload, mut score_sum) = (0usize, 0usize, 0i64);
        while let Some(row) = rows.try_next().await? {
            let name: &str = row.get(1);
            let score: i32 = row.get(2);
            count += 1;
            payload += name.len();
            score_sum += i64::from(score);
        }
        Ok::<_, tokio_postgres::Error>((count, payload, score_sum))
    }
    .await;
    ARMED.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);

    let (count, payload, score_sum) = outcome.expect("the query completes");
    // 1. Every row arrives exactly once. A result bound is never the fix.
    assert_eq!(count, ROWS);
    assert_eq!(payload, ROWS * VALUE_BYTES);
    assert_eq!(score_sum, (0..ROWS as i64).sum::<i64>());

    // 2. The window between executor and socket is a few batches, not the table.
    // Materialized, this result is ROWS * VALUE_BYTES = 12 MiB of payload, twice
    // over (the collected rows and the rendered messages).
    let materialized = (ROWS * VALUE_BYTES) as i64;
    let budget = 4 * 1024 * 1024;
    assert!(budget < materialized / 2, "test is miscalibrated");
    assert!(
        peak < budget,
        "the query peaked at {peak} bytes over a {budget}-byte budget (a \
         materialized result costs {materialized}); rows are accumulating \
         between the executor and the socket"
    );
}

/// `Execute` with `max_rows` returns that many rows and suspends; each further
/// `Execute` continues exactly where the last stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_max_rows_suspends_and_resumes_without_gap_or_duplicate() {
    let (mut client, _dir) = start().await;

    // The reference: one unlimited run, in the order the scan yields.
    let all: Vec<String> = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("unlimited run")
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    assert_eq!(all.len(), ROWS);

    // The portal lives in a transaction, as the driver requires for `bind`.
    let tx = client.transaction().await.expect("begin");
    let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
    let portal = tx.bind(&statement, &[]).await.expect("bind");
    let mut sizes = Vec::new();
    let mut resumed: Vec<String> = Vec::new();
    loop {
        let batch = tx.query_portal(&portal, 100).await.expect("execute");
        sizes.push(batch.len());
        resumed.extend(batch.iter().map(|row| row.get::<_, String>(0)));
        if batch.len() < 100 {
            break;
        }
    }
    drop(portal);
    tx.commit().await.expect("commit");

    assert_eq!(
        sizes,
        [100, 100, 100, 100, 100, 100, 100, ROWS - 700],
        "every Execute but the last returns exactly max_rows"
    );
    assert_eq!(resumed, all, "no gap, no duplicate, same order");
}
