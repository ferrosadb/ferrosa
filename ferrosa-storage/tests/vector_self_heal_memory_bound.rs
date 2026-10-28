//! The self-heal controller's vector work must not hold a sidecar's worth of
//! heap: checking a generation's sidecars, and rebuilding them from its rows.
//!
//! Live failure this pins (2026-10-27, memory cluster node2, jemalloc heap
//! profile): one self-heal tick took the node from ~250 MiB to over 1 GiB of
//! live heap. `SelfHealController::run_one_tick` -> `check_vector_indexes` ->
//! `TableStore::verify_vector_index` read a 640 MiB scoped HNSW sidecar whole
//! (`std::fs::read`) and then decoded every vector and every neighbour list
//! into an `HnswGraphData` (+168 MiB) only to count them. The verified mark
//! lives in memory, so every restart paid that once per generation.
//!
//! The rebuild (`repair_vector_sidecars`) held the partition, a copy of its
//! vectors, the graph, a CLONE of the graph (`to_data`) and the whole JSON
//! encoding at once before a byte reached disk.
//!
//! Both are measured at the engine's own entry points, on the calling thread
//! (the self-heal tick and `repair_vector_index` both run synchronously on
//! it), with a per-thread tracker so the engine's background threads do not
//! land in the window. Seeding and flushing run outside it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};

// --- peak-additional-heap tracker, per thread (see sidecar_memory_bound.rs) ---
struct TrackingAlloc;

thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
    static TOTAL: Cell<i64> = const { Cell::new(0) };
}

fn if_armed(f: impl FnOnce()) {
    if ARMED.try_with(Cell::get).unwrap_or(false) {
        f();
    }
}

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            if_armed(|| {
                let live = LIVE.with(|live| {
                    let updated = live.get() + layout.size() as i64;
                    live.set(updated);
                    updated
                });
                PEAK.with(|peak| peak.set(peak.get().max(live)));
                TOTAL.with(|total| total.set(total.get() + layout.size() as i64));
            });
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if_armed(|| {
            // Clamp at zero: memory allocated before the window may be freed in it.
            LIVE.with(|live| live.set((live.get() - layout.size() as i64).max(0)));
        });
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    let (out, peak, _) = measure(f);
    (out, peak)
}

/// Every byte allocated in the window, freed or not: the churn, which a
/// peak hides when something larger peaks in the same window.
fn measure_allocated<R>(f: impl FnOnce() -> R) -> (R, i64) {
    let (out, _, total) = measure(f);
    (out, total)
}

fn measure<R>(f: impl FnOnce() -> R) -> (R, i64, i64) {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    TOTAL.with(|total| total.set(0));
    ARMED.with(|armed| armed.set(true));
    let out = f();
    ARMED.with(|armed| armed.set(false));
    (out, PEAK.with(Cell::get), TOTAL.with(Cell::get))
}

const KS: &str = "agent_memory";
const TBL: &str = "embeddings";
const IDX: &str = "vec_idx";
const DIM: usize = 256;
/// Rows in the one partition: one scope, the shape of a tenant-wide
/// embedding partition, so its scoped sidecar is the big one.
const ROWS: usize = 1_500;

fn table_id() -> TableId {
    TableId::new(KS, TBL)
}

/// An engine with an empty table and its vector index, so registration finds
/// nothing to rebuild and starts no background repair.
fn engine(dir: &std::path::Path) -> StorageEngine {
    let engine = engine_without_vector_index(dir);
    engine.add_vector_index(&table_id(), IDX, 0, DIM).unwrap();
    engine
}

/// The same table with no vector index: the cost of everything a flush does
/// that is not vector work.
fn engine_without_vector_index(dir: &std::path::Path) -> StorageEngine {
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir), None).unwrap();
    engine
        .register_table(TableSchema {
            keyspace: KS.to_string(),
            table: TBL.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "embedding".to_string(),
                type_name: format!("vector<float, {DIM}>"),
            }],
            extensions: Default::default(),
        })
        .unwrap();
    engine
}

/// A deterministic, well-spread vector for row `i`.
fn vector(i: usize) -> Vec<f32> {
    (0..DIM)
        .map(|d| (((i * 31 + d * 17) % 997) as f32 / 997.0) - 0.5)
        .collect()
}

/// Write `ROWS` vectors into one partition and flush them into one SSTable
/// with its scoped sidecar and manifest. Returns the sidecar's size on disk.
fn seed_and_flush(engine: &StorageEngine, dir: &std::path::Path) -> u64 {
    write_rows(engine);
    engine.flush(&table_id()).unwrap();
    let scoped = vector_sidecars(dir, &format!("-VEC-{IDX}__scope_"));
    assert_eq!(
        scoped.len(),
        1,
        "one partition writes one scoped sidecar: {scoped:?}"
    );
    std::fs::metadata(&scoped[0]).unwrap().len()
}

/// Write `ROWS` vectors into the one partition, unflushed.
fn write_rows(engine: &StorageEngine) {
    // The partitioner's own token: ANN reads the partition back by its key.
    let key = DecoratedKey::new(PartitionKey::new(b"tenant-hot".to_vec()));
    for i in 0..ROWS {
        let row = Row {
            clustering: (i as i32).to_be_bytes().to_vec(),
            cells: vec![(
                0,
                CellValue::live(ferrosa_index::vec_f32_to_bytes(&vector(i)), 1000),
            )],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        };
        engine.write(&table_id(), &key, row, 1000).unwrap();
    }
}

/// Every file under `dir` whose name contains `fragment`.
fn vector_sidecars(dir: &std::path::Path, fragment: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(fragment))
            {
                found.push(path);
            }
        }
    }
    found
}

/// The partition keys ANN returns nearest to row 42's vector.
fn nearest_partitions(engine: &StorageEngine) -> Vec<Vec<u8>> {
    engine
        .ann_search_partitions(&table_id(), IDX, &vector(42), 1, 64)
        .unwrap()
        .into_iter()
        .map(|p| p.key.key.as_bytes().to_vec())
        .collect()
}

/// The raw bytes of the vectors themselves: the floor any in-memory graph of
/// them costs.
const RAW_VECTOR_BYTES: i64 = (ROWS * DIM * 4) as i64;

/// Checking a generation reads its sidecars to count vectors and check their
/// dimension and neighbour ranges. That is a scan of a mapped file: counters,
/// nothing proportional to it.
///
/// Measured (debug, 1,500 x 256-d, 4.6 MB sidecar): 6.78 MB before the fix
/// (read whole + decoded), 918 bytes after. The budget sits between with
/// two orders of magnitude either side.
const VERIFY_BUDGET_BYTES: i64 = 64 * 1024;

/// The rebuild's ceiling as a multiple of [`RAW_VECTOR_BYTES`].
///
/// Measured (same fixture): 12.6x before the fix (rows + a copy of the
/// vectors + the graph + a clone of it + the whole JSON), 2.44x with the
/// sidecar streamed but the rows still held while the vectors were copied
/// out, 1.73x with the rows consumed as they are decoded. 2x rejects both
/// earlier shapes and leaves ~15% for allocator jitter.
const REBUILD_BUDGET_RAW_MULTIPLE: f64 = 2.0;

/// One ANN query searches each generation's sidecars for the nearest
/// vectors. That is a walk of a few hundred graph nodes; it must not read
/// the sidecar into memory and decode the whole graph to take it.
///
/// Measured (debug, same fixture, k = 10, ef 64): 8.87 MB before the fix
/// (the 4.6 MB JSON sidecar read whole and decoded), 5.0 KB after (the
/// binary sidecar mapped and searched in place). 64 KiB sits between with
/// better than an order of magnitude either side.
const ANN_QUERY_BUDGET_BYTES: i64 = 64 * 1024;

/// Every byte a flush allocates for its vector work, freed or not, as a
/// multiple of [`RAW_VECTOR_BYTES`]: the flush with the index less the same
/// flush without it. A peak cannot see this, because the SSTable encode
/// peaks higher in the same window (8.7 MB here, index or not).
///
/// The flush builds two graphs over the same vectors (the global sidecar
/// and the partition's scoped one). Their neighbour lists are new; the
/// vectors are already in memory, and copying them is not needed.
///
/// Measured (debug, same fixture): 143x before the fix (every vector copied
/// into each build, the whole JSON encoding held, fresh search heaps per
/// layer of every insert), 41.7x with the vectors borrowed but the search
/// and prune scratch still allocated per insert, 1.3x after. 2x rejects one
/// more copy of the vectors (2.3x) and every earlier shape.
const FLUSH_CHURN_RAW_MULTIPLE: f64 = 2.0;

#[test]
fn an_ann_query_searches_the_sidecar_in_place_instead_of_decoding_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let sidecar_bytes = seed_and_flush(&engine, dir.path());

    // The index step alone: `ann_search_partitions` goes on to read the
    // matched partition back, which is its answer, not the search's cost.
    let (nearest, peak) = measure_peak(|| {
        engine
            .ann_search_in_partition_scope(&table_id(), IDX, b"tenant-hot", &vector(42), 10, 64)
            .unwrap()
    });

    assert_eq!(nearest.len(), 10, "positive control: {nearest:?}");
    assert_eq!(
        nearest[0].score, 0.0,
        "positive control: row 42's own vector is nearest to itself: {nearest:?}"
    );
    assert!(
        peak <= ANN_QUERY_BUDGET_BYTES,
        "one ANN query peaked at {peak} bytes of heap over a {sidecar_bytes}-byte sidecar \
         (budget {ANN_QUERY_BUDGET_BYTES}); the query must search the sidecar where it lies, \
         not read and decode it"
    );
    engine.shutdown().unwrap();
}

#[test]
fn a_flush_indexes_its_vectors_without_copying_them_or_churning_the_heap() {
    let plain_dir = tempfile::tempdir().unwrap();
    let plain = engine_without_vector_index(plain_dir.path());
    write_rows(&plain);
    let ((), without_index) = measure_allocated(|| plain.flush(&table_id()).unwrap());
    plain.shutdown().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    write_rows(&engine);
    let ((), with_index) = measure_allocated(|| engine.flush(&table_id()).unwrap());

    assert_eq!(
        nearest_partitions(&engine),
        vec![b"tenant-hot".to_vec()],
        "positive control: ANN answers from the flushed sidecars"
    );
    let churn = with_index - without_index;
    let budget = (RAW_VECTOR_BYTES as f64 * FLUSH_CHURN_RAW_MULTIPLE) as i64;
    assert!(
        churn <= budget,
        "indexing {ROWS} vectors in a flush allocated {churn} bytes ({:.1}x the \
         {RAW_VECTOR_BYTES} raw vector bytes, budget {FLUSH_CHURN_RAW_MULTIPLE}x); the flush \
         must build its sidecars from the memtable's vectors, not copies, and reuse its \
         search scratch across inserts",
        churn as f64 / RAW_VECTOR_BYTES as f64
    );
    engine.shutdown().unwrap();
}

/// A generation checked once stays checked across a restart: the check
/// decodes every vector of every sidecar, and paying it again on every
/// start is the startup cost a 640 MiB sidecar made visible.
///
/// RED as written: the verified mark lived in memory only, so the reopened
/// engine decoded the generation again (`verified == 1`).
#[test]
fn a_restart_does_not_decode_a_generation_it_already_checked() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine(dir.path());
        seed_and_flush(&engine, dir.path());
        let checks = engine.check_vector_indexes(&table_id(), 8).unwrap();
        assert_eq!(
            checks[0].verify.verified, 1,
            "first check decodes: {checks:?}"
        );
        let again = engine.check_vector_indexes(&table_id(), 8).unwrap();
        assert_eq!(
            again[0].verify.verified, 0,
            "a checked generation is not re-decoded"
        );
        engine.shutdown().unwrap();
    }

    // Reopened the way a restart registers the table and its index.
    let engine = engine(dir.path());
    assert_eq!(
        nearest_partitions(&engine),
        vec![b"tenant-hot".to_vec()],
        "positive control: the reopened engine serves the generation"
    );
    let checks = engine.check_vector_indexes(&table_id(), 8).unwrap();
    assert_eq!(checks.len(), 1, "{checks:?}");
    let verify = &checks[0].verify;
    assert!(verify.invalidated.is_empty(), "{verify:?}");
    assert_eq!(verify.pending, 0, "{verify:?}");
    assert_eq!(
        verify.verified, 0,
        "the reopened engine decoded a generation checked before the restart: {verify:?}"
    );
    engine.shutdown().unwrap();
}

#[test]
fn checking_a_generation_streams_its_sidecar_instead_of_loading_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let sidecar_bytes = seed_and_flush(&engine, dir.path());

    let (checks, peak) = measure_peak(|| engine.check_vector_indexes(&table_id(), 8).unwrap());

    assert_eq!(checks.len(), 1, "{checks:?}");
    let verify = &checks[0].verify;
    assert_eq!(
        verify.verified, 1,
        "the flushed generation was decoded: {verify:?}"
    );
    assert!(
        verify.invalidated.is_empty(),
        "a sound sidecar passes: {verify:?}"
    );
    assert!(
        peak <= VERIFY_BUDGET_BYTES,
        "checking one generation peaked at {peak} bytes of heap for a {sidecar_bytes}-byte \
         sidecar (budget {VERIFY_BUDGET_BYTES}); the check must stream the sidecar, not load \
         and decode it"
    );
    engine.shutdown().unwrap();
}

#[test]
fn a_rebuild_never_holds_the_encoded_sidecar_or_a_copy_of_the_graph() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let sidecar_bytes = seed_and_flush(&engine, dir.path());
    assert_eq!(
        nearest_partitions(&engine),
        vec![b"tenant-hot".to_vec()],
        "positive control: ANN answers from the flushed sidecar"
    );

    // Lose the manifest: the check invalidates the generation and the repair
    // must rebuild it from its rows — the self-heal path end to end.
    let manifests = vector_sidecars(dir.path(), &format!("-VEC-{IDX}__manifest"));
    assert_eq!(
        manifests.len(),
        1,
        "one generation, one manifest: {manifests:?}"
    );
    std::fs::remove_file(&manifests[0]).unwrap();
    let checks = engine.check_vector_indexes(&table_id(), 8).unwrap();
    assert_eq!(checks[0].verify.pending, 1, "{:?}", checks[0]);

    let (outcome, peak) = measure_peak(|| engine.repair_vector_index(&table_id(), IDX).unwrap());

    let outcome = outcome.expect("no other repair of the index is running");
    assert_eq!((outcome.repaired, outcome.failed), (1, 0), "{outcome:?}");
    assert_eq!(outcome.vectors, ROWS as u64, "{outcome:?}");
    // The rebuilt sidecar must be the same index: ANN answers from it.
    assert_eq!(
        nearest_partitions(&engine),
        vec![b"tenant-hot".to_vec()],
        "ANN answers from the rebuilt sidecar"
    );

    // The graph itself (vectors + neighbour lists) is unavoidable; the rows
    // held beside it, the encoded sidecar (~3x the raw vectors as JSON) and a
    // second copy of the graph are not.
    let budget = (RAW_VECTOR_BYTES as f64 * REBUILD_BUDGET_RAW_MULTIPLE) as i64;
    assert!(
        peak <= budget,
        "rebuilding a {ROWS}-vector generation peaked at {peak} bytes of heap \
         ({:.2}x the {RAW_VECTOR_BYTES} raw vector bytes, budget \
         {REBUILD_BUDGET_RAW_MULTIPLE}x; sidecar is {sidecar_bytes} bytes); the rebuild \
         must consume the rows and stream the sidecar to disk",
        peak as f64 / RAW_VECTOR_BYTES as f64
    );
    engine.shutdown().unwrap();
}
