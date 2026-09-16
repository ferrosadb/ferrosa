//! P0 OOM regression guard: the graph read path must never materialize a whole
//! table (forge t_bc5f0e6f, t_250fa355).
//!
//! `WritePath::range_read` is a materializing convenience wrapper: it drains
//! `range_read_stream_all_with` into a `Vec<Partition>`, so every caller reads
//! an ENTIRE table into RAM through an API that looks like an ordinary read.
//! Eleven `ferrosa-graph` call sites used it — the anchor scan, the per-hop
//! edge scan, the var-length path seed and its expansion fallback, the
//! find-edge / find-vertex / find-row fallbacks, and both adjacency-reconcile
//! scans. On a multi-tenant node a single unanchored `MATCH` could therefore
//! pull a user-sized table into the heap and OOM the process.
//!
//! These tests lock the fix in the same shape as
//! `ferrosa-storage/tests/recovery_oom_memory_bound.rs`: drive a table far
//! larger than any internal buffer, and assert the peak heap held during the
//! operation does NOT scale with table size. The materializing `range_read` is
//! still called directly as the BASELINE so each test proves its own fixture is
//! big enough to be meaningful — if the graph path ever materializes again its
//! peak returns to the baseline and these fail loudly.
//!
//! IMPORTANT — this is a MEMORY bound, not a result bound. Nothing here caps
//! how many rows a query may return; a cap on the result would turn an OOM into
//! a silently wrong answer, which is worse. The invariant is that the bytes
//! RESIDENT AT ONCE are bounded by the query's own working set, not by how much
//! data the tenant happens to store.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use indexmap::IndexMap;
use tempfile::TempDir;
use uuid::Uuid;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_graph::engine::GraphEngine;
use ferrosa_graph::executor::expand::GraphEngineConfig;
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy, Permission,
    RateLimitConfig, Resource, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
//
// Each integration test file is its own binary, so this `#[global_allocator]`
// affects nothing else in the workspace. `alloc`/`dealloc` touch only atomics
// and `System`, never the heap, so there is no reentrancy. Tracking is gated by
// `ARMED`; every test in this file holds `MEASURE_LOCK` for its whole body, so
// no second test runs — let alone arms the flag — while a measurement window is
// open. Queries are driven on a CURRENT-THREAD tokio runtime so no worker
// thread allocates outside the measuring thread either.

struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);
static MEASURE_LOCK: Mutex<()> = Mutex::new(());

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
            // every later allocation. Seeding runs outside the window by design.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static GLOBAL: TrackingAlloc = TrackingAlloc;

/// Serialize measurement: only one armed window may exist at a time, and no
/// other test body may run allocating work inside someone else's window.
fn measure_guard() -> MutexGuard<'static, ()> {
    MEASURE_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

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

// --- fixture -------------------------------------------------------------

const PAYLOAD_BYTES: usize = 16 * 1024;
const SMALL_VERTICES: usize = 32;
const LARGE_VERTICES: usize = 512;
const KEYSPACE: &str = "memguard";
const NEEDLE: &str = "needle";

fn storage_config(dir: &TempDir) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 8 * 1024 * 1024,
            max_segment_age: Duration::from_secs(3600),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.path().join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        // No auto-flush mid-seed: one manual flush at the end keeps the fixture
        // a small number of generations regardless of table size.
        flush_threshold_bytes: 512 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.path().to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        write_verify: false,
    }
}

fn superuser() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    }
}

fn column(name: &str, kind: ColumnKind, position: i32, ty: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position,
        column_type: ty.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

/// `memguard.person_v` (vertex) + `memguard.knows_e` (edge), both carrying a
/// fat `payload` column so a materialized scan is unmistakably expensive.
fn create_graph_schema(schema: &Schema) {
    let auth = superuser();
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: KEYSPACE.to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
                },
            },
            &auth,
        )
        .unwrap();
    schema
        .grant(
            "cassandra",
            &Resource::Keyspace(KEYSPACE.to_string()),
            HashSet::from([
                Permission::Select,
                Permission::Modify,
                Permission::Create,
                Permission::Drop,
                Permission::Alter,
                Permission::Authorize,
            ]),
            &auth,
        )
        .unwrap();

    let mut person_cols = IndexMap::new();
    person_cols.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, 0, "text"),
    );
    person_cols.insert("name".to_string(), column("name", ColumnKind::Regular, -1, "text"));
    person_cols.insert(
        "payload".to_string(),
        column("payload", ColumnKind::Regular, -1, "text"),
    );
    schema
        .create_table(
            TableMetadata {
                keyspace: KEYSPACE.to_string(),
                name: "person_v".to_string(),
                id: Uuid::new_v4(),
                columns: person_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::from([
                    ("graph.type".to_string(), "vertex".to_string()),
                    ("graph.label".to_string(), "Person".to_string()),
                ]),
                is_system: false,
            },
            &auth,
        )
        .unwrap();

    let mut knows_cols = IndexMap::new();
    knows_cols.insert(
        "src_id".to_string(),
        column("src_id", ColumnKind::PartitionKey, 0, "text"),
    );
    knows_cols.insert(
        "dst_id".to_string(),
        column("dst_id", ColumnKind::Clustering, 0, "text"),
    );
    knows_cols.insert(
        "payload".to_string(),
        column("payload", ColumnKind::Regular, -1, "text"),
    );
    knows_cols.insert("tag".to_string(), column("tag", ColumnKind::Regular, -1, "text"));
    schema
        .create_table(
            TableMetadata {
                keyspace: KEYSPACE.to_string(),
                name: "knows_e".to_string(),
                id: Uuid::new_v4(),
                columns: knows_cols,
                partition_key: vec!["src_id".to_string()],
                clustering_key: vec![("dst_id".to_string(), ClusteringOrder::Asc)],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::from([
                    ("graph.type".to_string(), "edge".to_string()),
                    ("graph.label".to_string(), "KNOWS".to_string()),
                    ("graph.source".to_string(), "src_id".to_string()),
                    ("graph.target".to_string(), "dst_id".to_string()),
                    ("graph.source_label".to_string(), "Person".to_string()),
                    ("graph.target_label".to_string(), "Person".to_string()),
                ]),
                is_system: false,
            },
            &auth,
        )
        .unwrap();
}

fn register_storage_tables(storage: &StorageEngine) {
    storage
        .register_table(TableSchema {
            keyspace: KEYSPACE.to_string(),
            table: "person_v".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            // Cassandra column order: regular columns sorted by name.
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "payload".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: HashMap::new(),
        })
        .unwrap();
    storage
        .register_table(TableSchema {
            keyspace: KEYSPACE.to_string(),
            table: "knows_e".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "dst_id".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "payload".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "tag".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
            extensions: HashMap::new(),
        })
        .unwrap();
}

fn payload_bytes(seed: usize) -> Vec<u8> {
    // Vary the bytes per partition so nothing is trivially deduplicated, and
    // keep them printable so `text` decoding produces a same-sized JSON string.
    (0..PAYLOAD_BYTES)
        .map(|j| b'a' + ((seed.wrapping_add(j)) % 26) as u8)
        .collect()
}

struct Fixture {
    _dir: TempDir,
    schema: Arc<Schema>,
    storage: Arc<StorageEngine>,
    write_path: Arc<arc_swap::ArcSwap<WritePath>>,
    engine: Arc<GraphEngine>,
}

impl Fixture {
    fn table(&self, name: &str) -> TableId {
        TableId::new(KEYSPACE, name)
    }
}

/// Seed `n` Person vertices (one of which is the `needle`) and, when
/// `with_edges`, one KNOWS edge per person. Every row carries a 16 KiB payload.
fn fixture(n: usize, with_edges: bool) -> Fixture {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(StorageEngine::new(storage_config(&dir), None).unwrap());
    let schema = Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap(),
    );
    create_graph_schema(&schema);
    register_storage_tables(&storage);

    let snap = schema.snapshot();
    let person_meta = snap
        .tables
        .get(&(KEYSPACE.to_string(), "person_v".to_string()))
        .expect("person_v");
    let edge_meta = snap
        .tables
        .get(&(KEYSPACE.to_string(), "knows_e".to_string()))
        .expect("knows_e");
    let name_idx = person_meta.storage_column_index("name").expect("name cell");
    let person_payload_idx = person_meta
        .storage_column_index("payload")
        .expect("payload cell");
    let edge_payload_idx = edge_meta
        .storage_column_index("payload")
        .expect("edge payload cell");
    let tag_idx = edge_meta.storage_column_index("tag").expect("tag cell");

    let person_tid = TableId::new(KEYSPACE, "person_v");
    let edge_tid = TableId::new(KEYSPACE, "knows_e");
    for i in 0..n {
        let ts = 1_000 + i as i64;
        let id = format!("p{i:06}");
        // Exactly ONE vertex matches the needle predicate, so a correct
        // streaming scan holds one row's worth of state no matter how many
        // partitions it walks past.
        let name = if i == n / 2 {
            NEEDLE.to_string()
        } else {
            format!("person-{i:06}")
        };
        let key = DecoratedKey::new(PartitionKey::new(id.clone().into_bytes()));
        storage
            .write(
                &person_tid,
                &key,
                Row {
                    clustering: vec![],
                    cells: vec![
                        (name_idx, CellValue::live(name.into_bytes(), ts)),
                        (
                            person_payload_idx,
                            CellValue::live(payload_bytes(i), ts),
                        ),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                },
                ts,
            )
            .unwrap();

        if with_edges {
            let dst = format!("p{:06}", (i + 1) % n.max(1));
            let tag = if i == n / 2 {
                NEEDLE.to_string()
            } else {
                format!("edge-{i:06}")
            };
            storage
                .write(
                    &edge_tid,
                    &key,
                    Row {
                        clustering: dst.into_bytes(),
                        cells: vec![
                            (edge_payload_idx, CellValue::live(payload_bytes(i + 7), ts)),
                            (tag_idx, CellValue::live(tag.into_bytes(), ts)),
                        ],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(ts),
                    },
                    ts,
                )
                .unwrap();
        }
    }
    storage.flush(&person_tid).unwrap();
    if with_edges {
        storage.flush(&edge_tid).unwrap();
    }

    let write_path = Arc::new(arc_swap::ArcSwap::from_pointee(WritePath::direct(
        Arc::clone(&storage),
    )));
    let engine = Arc::new(GraphEngine::new(
        Arc::clone(&schema),
        Arc::clone(&storage),
        Arc::clone(&write_path),
        GraphEngineConfig::default(),
        Duration::from_secs(3600),
    ));
    Fixture {
        _dir: dir,
        schema,
        storage,
        write_path,
        engine,
    }
}

fn current_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Peak bytes held by the materializing `WritePath::range_read` over `table` —
/// the pre-fix behaviour of every site under test, kept as the yardstick that
/// proves the fixture is large enough for the comparison to mean anything.
///
/// This also WARMS the storage read path (SSTable index, bloom filter,
/// decompression chunk cache) before any operation is measured. Those are
/// per-SSTable structures whose cost the graph executor does not control and
/// which a real node pays once, not per query; leaving them inside the measured
/// window would charge the first query for them and blur the thing under test.
fn materialize_baseline_peak(fx: &Fixture, rt: &tokio::runtime::Runtime, table: &str) -> i64 {
    let wp = fx.write_path.load_full();
    let tid = fx.table(table);
    let warm = rt.block_on(wp.range_read(&tid)).expect("warm-up range_read");
    assert!(!warm.is_empty(), "warm-up must read the seeded partitions");
    drop(warm);
    let (partitions, peak) = measure_peak(|| rt.block_on(wp.range_read(&tid)).expect("range_read"));
    assert!(
        !partitions.is_empty(),
        "baseline must actually read the seeded partitions"
    );
    drop(partitions);
    peak
}

/// Peaks for one fixture size: materializing the whole table vs. the real
/// graph operation under test.
struct Peaks {
    materialize: i64,
    operation: i64,
}

fn assert_bounded(name: &str, small: &Peaks, large: &Peaks, growth_factor: i64) {
    // Sanity: the baseline must actually scale with the 16x larger table,
    // otherwise the comparison proves nothing.
    assert!(
        large.materialize > small.materialize * 4,
        "{name}: sanity — materializing {LARGE_VERTICES} partitions ({} B) should cost far more \
         than {SMALL_VERTICES} ({} B); the fixture sizes are too close to be meaningful",
        large.materialize,
        small.materialize,
    );

    // Primary guard: on the large table the operation must hold only a small
    // fraction of what materializing it costs. A materializing implementation
    // peaks at ~the baseline and fails here.
    assert!(
        large.operation * 4 < large.materialize,
        "{name}: peak {} B is not far below the whole-table materialization peak {} B on a \
         {LARGE_VERTICES}-vertex table — REGRESSION: the graph read path is materializing whole \
         tables again (forge t_bc5f0e6f), the OOM vector an unanchored MATCH can trigger on any \
         tenant table",
        large.operation,
        large.materialize,
    );

    // Shape guard: cost stays ~flat as the table grows 16x, while
    // materialization grew with it.
    assert!(
        large.operation < small.operation * growth_factor,
        "{name}: peak scaled with table size ({} B for {SMALL_VERTICES} vertices -> {} B for \
         {LARGE_VERTICES}) while materialization went {} B -> {} B — REGRESSION: graph memory now \
         tracks table size instead of the query's own working set",
        small.operation,
        large.operation,
        small.materialize,
        large.materialize,
    );
}

// --- 1. anchor scan (executor/expand.rs read_anchor_partitions) -----------

fn anchor_scan_peaks(n: usize) -> Peaks {
    let fx = fixture(n, false);
    let rt = current_thread_rt();
    let materialize = materialize_baseline_peak(&fx, &rt, "person_v");

    let engine = Arc::clone(&fx.engine);
    let auth = superuser();
    let query = format!("MATCH (p:Person {{name: '{NEEDLE}'}}) RETURN p.name");
    // One unmeasured run so lazily-built, query-independent state (schema
    // snapshots, adjacency-keyspace registration, the storage read path) is not
    // charged to the measured run.
    rt.block_on(engine.execute(&query, KEYSPACE, &auth))
        .expect("warm-up query");
    let (result, operation) = measure_peak(|| {
        rt.block_on(engine.execute(
            &format!("MATCH (p:Person {{name: '{NEEDLE}'}}) RETURN p.name"),
            KEYSPACE,
            &auth,
        ))
    });
    let result = result.expect("anchor query must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "exactly one vertex carries the needle name; a memory fix must not change the ANSWER"
    );
    Peaks {
        materialize,
        operation,
    }
}

/// The anchor scan is the widest-blast-radius site: any `MATCH` whose anchor
/// properties do not pin the full primary key used to read the entire anchor
/// table into a `Vec<Partition>` before examining the first candidate.
#[test]
fn anchor_scan_memory_is_independent_of_vertex_table_size() {
    let _guard = measure_guard();
    let small = anchor_scan_peaks(SMALL_VERTICES);
    let large = anchor_scan_peaks(LARGE_VERTICES);
    assert_bounded("anchor scan", &small, &large, 3);
}

// --- 2. edge-anchored hop scan + find-vertex fallback (expand.rs) ---------

fn edge_anchored_peaks(n: usize) -> Peaks {
    let fx = fixture(n, true);
    let rt = current_thread_rt();
    let materialize = materialize_baseline_peak(&fx, &rt, "knows_e");

    let engine = Arc::clone(&fx.engine);
    let auth = superuser();
    let query = format!("MATCH (a:Person)-[r:KNOWS {{tag: '{NEEDLE}'}}]->(b:Person) RETURN r.tag");
    rt.block_on(engine.execute(&query, KEYSPACE, &auth))
        .expect("warm-up query");
    let (result, operation) = measure_peak(|| {
        rt.block_on(engine.execute(
            &format!("MATCH (a:Person)-[r:KNOWS {{tag: '{NEEDLE}'}}]->(b:Person) RETURN r.tag"),
            KEYSPACE,
            &auth,
        ))
    });
    let result = result.expect("edge-anchored query must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "exactly one edge carries the needle tag; a memory fix must not change the ANSWER"
    );
    Peaks {
        materialize,
        operation,
    }
}

/// `MATCH (a:L)-[r:LABEL {prop}]->(b:L)` takes the edge-anchored fast path, which
/// scanned the whole relationship table into a `Vec<Partition>` and then, per
/// surviving edge, fell back to a whole-vertex-table scan in
/// `find_vertex_match`.
#[test]
fn edge_anchored_scan_memory_is_independent_of_edge_table_size() {
    let _guard = measure_guard();
    let small = edge_anchored_peaks(SMALL_VERTICES);
    let large = edge_anchored_peaks(LARGE_VERTICES);
    assert_bounded("edge-anchored scan", &small, &large, 3);
}

// --- 3. variable-length path seed (executor/varpath.rs) -------------------

fn varpath_peaks(n: usize) -> Peaks {
    let fx = fixture(n, true);
    let rt = current_thread_rt();
    let materialize = materialize_baseline_peak(&fx, &rt, "person_v");

    let engine = Arc::clone(&fx.engine);
    let auth = superuser();
    let query = format!("MATCH (a:Person {{name: '{NEEDLE}'}})-[:KNOWS*1..2]->(b) RETURN b");
    rt.block_on(engine.execute(&query, KEYSPACE, &auth))
        .expect("warm-up query");
    let (result, operation) = measure_peak(|| {
        rt.block_on(engine.execute(
            &format!("MATCH (a:Person {{name: '{NEEDLE}'}})-[:KNOWS*1..2]->(b) RETURN b"),
            KEYSPACE,
            &auth,
        ))
    });
    result.expect("var-length path query must succeed");
    Peaks {
        materialize,
        operation,
    }
}

/// The var-length path executor seeded its BFS frontier by reading the entire
/// anchor table, then fell back to a whole-edge-table scan per frontier vertex
/// when adjacency was not materialized.
#[test]
fn varpath_seed_memory_is_independent_of_vertex_table_size() {
    let _guard = measure_guard();
    let small = varpath_peaks(SMALL_VERTICES);
    let large = varpath_peaks(LARGE_VERTICES);
    assert_bounded("var-length path seed", &small, &large, 3);
}

// --- 4. adjacency reconciliation (adjacency/reconcile.rs) -----------------

fn reconcile_peaks(n: usize) -> Peaks {
    let fx = fixture(n, true);
    let rt = current_thread_rt();
    let materialize = materialize_baseline_peak(&fx, &rt, "knows_e");

    // The reconcile pass writes adjacency repairs, so its keyspace must exist.
    rt.block_on(
        fx.engine
            .ensure_adjacency_storage_for_keyspace_for_test(KEYSPACE),
    )
    .expect("adjacency keyspace registration");

    let schema = Arc::clone(&fx.schema);
    let wp = fx.write_path.load_full();
    // One unmeasured pass performs every repair, so the measured pass is a
    // steady-state scan: what it still holds is the scan's own working set.
    let warm = rt.block_on(ferrosa_graph::adjacency::reconcile::reconcile_once(
        &schema, &wp, KEYSPACE,
    ));
    assert!(
        warm.entries_checked > 0,
        "warm-up reconcile must walk the seeded edges, got {warm:?}"
    );
    let (metrics, operation) = measure_peak(|| {
        rt.block_on(ferrosa_graph::adjacency::reconcile::reconcile_once(
            &schema, &wp, KEYSPACE,
        ))
    });
    assert!(
        metrics.entries_checked > 0,
        "reconcile must actually walk the seeded edges, got {metrics:?}"
    );
    Peaks {
        materialize,
        operation,
    }
}

/// Background reconciliation already yielded every N partitions inside its
/// loop — but it pre-collected the WHOLE edge table (and then the whole
/// adjacency index) before entering that loop, so the yielding bought latency
/// without bounding memory.
#[test]
fn reconcile_memory_is_independent_of_edge_table_size() {
    let _guard = measure_guard();
    let small = reconcile_peaks(SMALL_VERTICES);
    let large = reconcile_peaks(LARGE_VERTICES);
    // growth_factor 6, not 3: reconcile REPAIRS what it finds, and every repair
    // is an adjacency mutation that stays live in the memtable. That write
    // volume is proportional to the edge count by definition — it is the work
    // the pass exists to do, not a scan buffer. The scan itself is what must
    // stay flat, and a materializing scan would still blow past 6x (its peak
    // tracked the 16x baseline).
    assert_bounded("adjacency reconcile", &small, &large, 6);
}

// --- 5. the operations still return complete answers ---------------------

/// Guard against "fixing" an OOM by capping the RESULT: a bound on how much
/// data is resident is legitimate, a bound on how many rows the caller gets is
/// a silent wrong answer. Every Person must come back from an unfiltered
/// anchor scan of a table far larger than any internal buffer.
#[test]
fn streaming_anchor_scan_still_returns_every_row() {
    let _guard = measure_guard();
    let fx = fixture(LARGE_VERTICES, false);
    let rt = current_thread_rt();
    let result = rt
        .block_on(
            fx.engine
                .execute("MATCH (p:Person) RETURN p.name", KEYSPACE, &superuser()),
        )
        .expect("unfiltered anchor scan must succeed");
    assert_eq!(
        result.rows.len(),
        LARGE_VERTICES,
        "streaming must bound MEMORY, never the result set: an unfiltered MATCH over \
         {LARGE_VERTICES} vertices must return all {LARGE_VERTICES} rows"
    );
    // Keep the storage engine alive until after the assertion.
    drop(fx.storage);
}
