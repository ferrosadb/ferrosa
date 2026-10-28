//! A limited range read must hold O(limit + sources) partitions, not
//! O(limit x sources).
//!
//! `TableStore::read_range_limited_rows` answers "the `limit` smallest keys in
//! `[start, end]`" across the memtables and every SSTable. Collecting each
//! source's own `limit` smallest keys and merging afterwards gives the right
//! answer but holds `limit` decoded partitions per SSTable at once, so the peak
//! grows with the SSTable count. The read is a k-way merge instead: it decodes
//! only the current key's group and stops after `limit` partitions, so adding
//! SSTables adds one partition in flight each, not `limit`.
//!
//! The signal is peak additional live heap bytes, the idiom of
//! `memtable_scan_memory_bound.rs` and `fulltext_streaming_each_memory_bound.rs`.
//! The tracker is process-wide, so all measurements run in ONE `#[test]`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId,
};

struct TrackingAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && ARMED.load(Ordering::Relaxed) {
            let live =
                LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed) + layout.size() as i64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: freeing memory allocated before the window would
            // drive LIVE negative and suppress every later peak.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

const KS: &str = "range_ks";
const TBL: &str = "wide_values";
/// Each partition carries one value this large, so decoded partitions dominate
/// the peak rather than reader bookkeeping.
const PAYLOAD_BYTES: usize = 4096;
/// Partitions written to each SSTable.
const PER_SSTABLE: usize = 64;
/// Partitions the read asks for.
const LIMIT: usize = 32;

fn engine(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 64 * 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        // Flushed explicitly, once per SSTable.
        flush_threshold_bytes: u64::MAX,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 4,
        cache_hot_window_secs: 900,
    };
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine
        .register_table(TableSchema {
            keyspace: KS.to_string(),
            table: TBL.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            }],
            extensions: Default::default(),
        })
        .unwrap();
    engine
}

/// Write `sstables` SSTables of distinct keys with their real Murmur3
/// tokens, so the `LIMIT` smallest keys are spread over every SSTable. Returns
/// every key written.
fn seed(engine: &StorageEngine, sstables: usize) -> Vec<DecoratedKey> {
    let tid = TableId::new(KS, TBL);
    let mut written = Vec::with_capacity(sstables * PER_SSTABLE);
    for s in 0..sstables {
        for i in 0..PER_SSTABLE {
            let dk = DecoratedKey::new(PartitionKey::new(format!("k-{s:03}-{i:05}").into_bytes()));
            written.push(dk.clone());
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(vec![b'x'; PAYLOAD_BYTES], 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            };
            engine.write(&tid, &dk, row, 1000).unwrap();
        }
        engine.flush(&tid).unwrap();
    }
    assert_eq!(
        engine.sstable_count(&tid),
        sstables,
        "the fixture needs one SSTable per flush; a compaction merged them"
    );
    written
}

/// Peak heap of one limited read over `sstables` SSTables, after checking it
/// returned the `LIMIT` smallest keys in order.
fn read_peak(sstables: usize) -> i64 {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let mut written = seed(&engine, sstables);
    written.sort();
    let tid = TableId::new(KS, TBL);
    // Warm the reader pool so opening readers is not part of the window.
    engine
        .read_range_limited_rows(&tid, None, None, 1, 0)
        .unwrap();

    let (parts, peak) = measure_peak(|| {
        engine
            .read_range_limited_rows(&tid, None, None, LIMIT, 0)
            .unwrap()
    });
    let keys: Vec<&DecoratedKey> = parts.iter().map(|p| &p.key).collect();
    let expected: Vec<&DecoratedKey> = written.iter().take(LIMIT).collect();
    assert_eq!(
        keys, expected,
        "the read must return the {LIMIT} smallest keys"
    );
    drop(parts);
    peak
}

#[test]
fn limited_range_read_peak_does_not_scale_with_sstable_count() {
    let output_bytes = (LIMIT * PAYLOAD_BYTES) as i64;

    let few = read_peak(2);
    // Negative control: the result itself is live in the window, so a reading
    // below it means the tracker is not armed.
    assert!(
        few >= output_bytes,
        "peak {few} B is below the {output_bytes} B result: the tracker is not armed"
    );

    let many = read_peak(16);
    eprintln!(
        "limited range read, limit {LIMIT} x {PAYLOAD_BYTES} B: 2 SSTables {few} B, \
         16 SSTables {many} B"
    );
    // Collecting `limit` per source holds 16 x 32 x 4 KiB = 2 MiB here, 8x the
    // 2-SSTable reading. A merge adds one partition per SSTable in flight.
    assert!(
        many < few * 2,
        "16 SSTables peaked at {many} B against {few} B for 2: the read holds \
         partitions per source instead of merging them"
    );
    assert!(
        many < output_bytes * 3,
        "16 SSTables peaked at {many} B for a {output_bytes} B result"
    );
}
