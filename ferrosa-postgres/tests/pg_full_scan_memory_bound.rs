//! P0 regression guard: a Postgres-wire full-table scan must STREAM.
//!
//! `storage_provider::load_table` used to drain the whole `range_iter` stream
//! into a `Vec<Row>` and hand back a `ferrosa_sql::InMemoryTable`
//! (`stream-push-accumulation`, tracked as t_f348ba0b). Any Postgres-wire
//! `SELECT` without a key bound therefore held **every row of the table** in
//! memory at once, before a single byte reached the client — a client-facing
//! OOM on a table larger than the node's heap.
//!
//! The fix makes the provider stream: an async producer drains `range_iter` and
//! hands rows to the synchronous executor through a **bounded** channel
//! (`SCAN_BUFFER_ROWS`), so the source-side peak is one partition plus the
//! channel, not the table.
//!
//! This test locks that in. It seeds a table far larger than the channel, drains
//! the scan with an O(1) consumer, and asserts BOTH halves of the contract:
//!
//! 1. **Every row is returned.** A bound that drops rows is not a fix — the
//!    checksum below is over every `id` written, so a truncated scan fails.
//! 2. **Peak memory is bounded by the channel, not by the table.** If the
//!    provider ever materializes again, peak scales with row count and this
//!    fails loudly — long before it can OOM a real node.
//!
//! Note what this test does NOT claim. It bounds the *source* side only. The
//! relational executor still collects its base row set into a `Vec<Row>`
//! (`ferrosa-sql/src/plan.rs`, `seq_scan(...).collect()`) and `QueryResult.rows`
//! is a `Vec`, so an end-to-end `SELECT *` peak is still O(result) until
//! t_50d99192 lands. That materialization is flagged by the audit under its own
//! allowlist entries and is deliberately not hidden by this fix.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_postgres::storage_provider::{load_table, ScanFailure, SCAN_BUFFER_ROWS};
use ferrosa_schema::{
    AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
    EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
    ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
};
use ferrosa_sql::{TableProvider, Value};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row as StorageRow};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};
use indexmap::IndexMap;
use uuid::Uuid;

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
//
// Mirrors `ferrosa-storage/tests/recovery_oom_memory_bound.rs`. Each
// integration test file is its own binary, so this `#[global_allocator]`
// affects nothing else in the workspace. `alloc`/`dealloc` touch only atomics
// and `System`, never the heap, so there is no reentrancy. Tracking is gated by
// `ARMED`; only the one measuring test arms it, and it does so after seeding.

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
            // Clamp at zero: `measure_peak` zeroes LIVE at arm time, so a free
            // of memory allocated BEFORE the window would drive the counter
            // negative and, because PEAK is a running maximum of LIVE, suppress
            // every later allocation.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static GLOBAL: TrackingAlloc = TrackingAlloc;

/// Run `f` with peak-allocation tracking armed; return its result and the peak
/// number of additional live bytes observed during the call.
fn measure_peak<T>(f: impl FnOnce() -> T) -> (T, i64) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let out = f();
    ARMED.store(false, Ordering::Relaxed);
    (out, PEAK.load(Ordering::Relaxed))
}

// --- fixture -----------------------------------------------------------------

/// Payload per row. Large enough that materializing `ROWS` of them is
/// unmistakable against the channel's own footprint.
const VALUE_BYTES: usize = 16 * 1024;

/// Rows in the fixture table. Far more than `SCAN_BUFFER_ROWS` so a streaming
/// provider is forced to apply backpressure many times over.
const ROWS: usize = 768;

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

/// `ks.t(id text PK, ck int CK, name text, score int)`, declared in that order.
fn schema_with_table() -> Schema {
    let schema = Schema::new(schema_config()).expect("schema bootstraps");
    let auth = superuser();

    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "ks".to_string(),
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
        .expect("create keyspace");

    let mut columns = IndexMap::new();
    columns.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, "text", 0),
    );
    columns.insert(
        "ck".to_string(),
        column("ck", ColumnKind::Clustering, "int", 0),
    );
    columns.insert(
        "name".to_string(),
        column("name", ColumnKind::Regular, "text", 0),
    );
    columns.insert(
        "score".to_string(),
        column("score", ColumnKind::Regular, "int", 0),
    );

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
        write_verify: false,
    }
}

fn storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![
            ColumnDefinition {
                name: "name".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            ColumnDefinition {
                name: "score".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            },
        ],
        extensions: Default::default(),
    }
}

/// One partition per row, each carrying a `VALUE_BYTES` payload in `name`.
fn seed(engine: &StorageEngine) {
    let tid = TableId::new("ks", "t");
    for i in 0..ROWS {
        // Zero-padded so the partition key is a fixed width; the id encodes the
        // row number, which is what the checksum below verifies.
        let id = format!("row{i:08}");
        let payload = "x".repeat(VALUE_BYTES);
        let key = DecoratedKey::new(PartitionKey::new(id.clone().into_bytes()));
        let ts = 1000 + i as i64;
        let row = StorageRow {
            clustering: 1i32.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live(payload.into_bytes(), ts)),
                (1, CellValue::live((i as i32).to_be_bytes().to_vec(), ts)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        };
        engine.write(&tid, &key, row, ts).expect("write row");
    }
}

/// Fold one scanned row into an O(1) accumulator: the count, and a sum over the
/// `score` column (which holds the row's index). Together these prove every row
/// arrived exactly once without the consumer ever holding more than one row.
#[derive(Default, PartialEq, Eq, Debug)]
struct Digest {
    count: usize,
    score_sum: i64,
    payload_bytes: usize,
}

fn digest_of(rows: impl Iterator<Item = ferrosa_sql::Row>) -> Digest {
    let mut d = Digest::default();
    for row in rows {
        d.count += 1;
        match row.get(3) {
            Value::Int(score) => d.score_sum += score,
            other => panic!("score must be an int, got {other:?}"),
        }
        match row.get(2) {
            Value::Text(name) => d.payload_bytes += name.len(),
            other => panic!("name must be text, got {other:?}"),
        }
    }
    d
}

/// The expected digest if — and only if — every seeded row is returned.
fn expected_digest() -> Digest {
    Digest {
        count: ROWS,
        score_sum: (0..ROWS as i64).sum(),
        payload_bytes: ROWS * VALUE_BYTES,
    }
}

// --- the guard ---------------------------------------------------------------

/// A full-table scan must return every row while holding only a bounded window
/// of them at once.
///
/// The two assertions are deliberately paired. Bounding memory by dropping rows
/// would satisfy the second and fail the first; materializing the table would
/// satisfy the first and fail the second. Only streaming satisfies both.
#[test]
fn full_table_scan_returns_every_row_with_bounded_peak() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime builds");

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).expect("engine"));
    engine.register_table(storage_schema()).expect("register");
    let schema = schema_with_table();

    seed(&engine);

    let failures = ScanFailure::default();

    // Arm AFTER seeding: we measure the read path, not the fixture. The window
    // spans BOTH `load_table` and the drain, because those are the two places a
    // materialization can hide — loading every row up front and then handing
    // them out one at a time is exactly the shape being removed here, and it
    // would look bounded if only the drain were measured. The consumer is O(1)
    // (a fold into `Digest`), so every byte counted belongs to the provider.
    let (digest, peak) = measure_peak(|| {
        rt.block_on(async {
            let table = Arc::new(
                load_table(&engine, &schema, "ks", "t", failures.clone())
                    .await
                    .expect("load succeeds"),
            );
            // The synchronous executor runs on a blocking thread (see
            // `offload.rs`); the scan must be driven from the same place, which
            // is what makes a blocking receive on the bounded channel legal.
            tokio::task::spawn_blocking(move || digest_of(table.scan()))
                .await
                .expect("scan task")
        })
    });

    // 1. Every row is returned. A RESULT bound is never the fix.
    assert_eq!(
        digest,
        expected_digest(),
        "the scan must return every seeded row exactly once"
    );

    // No storage error may have been swallowed on the way.
    assert_eq!(
        failures.take(),
        None,
        "the scan must not have recorded a storage failure"
    );

    // 2. Peak stays bounded by the channel, not the table.
    //
    // Materialized, this scan holds ROWS * VALUE_BYTES = 8 MiB of payload at
    // once (plus the `Vec<Row>` spine). Streamed, the source-side peak is one
    // partition plus SCAN_BUFFER_ROWS rows in flight. The threshold sits well
    // above the streaming bound and well below the materialized one, so it
    // tracks the shape of the code rather than allocator noise.
    let materialized = (ROWS * VALUE_BYTES) as i64;
    let streaming_bound = (SCAN_BUFFER_ROWS * VALUE_BYTES) as i64;
    let threshold = streaming_bound * 3;
    assert!(
        threshold < materialized / 2,
        "test is miscalibrated: threshold {threshold} must be far below the \
         materialized cost {materialized}"
    );
    assert!(
        peak < threshold,
        "full-table scan peaked at {peak} bytes, over the {threshold}-byte \
         streaming budget (materializing the whole table would cost \
         {materialized}); the provider is accumulating instead of streaming"
    );

    engine.shutdown().expect("shutdown");
}
