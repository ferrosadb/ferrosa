//! Real executor before/after probe: assert every emitted cell before reporting cost.
//! Correctness: merged identities, ordering, values, tombstones and BTI point reads.
//! Last revised: 2026-10-23
//! Last changed: Added a baseline-compatible allocation, time and live-byte probe.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_sstable::compression::Compression;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};
use ferrosa_storage::compaction::executor::CompactionExecutor;
use ferrosa_storage::compaction::metadata::{CompactionTask, SSTableMetadata};
use ferrosa_storage::compaction::purge::PurgePolicy;
use ferrosa_storage::flush::{
    build_serialization_header, open_file_sstable, FileFlushTarget, FlushTarget,
};
use ferrosa_storage::TableId;

struct CountAlloc;
static ARMED: AtomicBool = AtomicBool::new(false);
static EVENTS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn allocated(n: usize) {
    let live = LIVE.fetch_add(n, Ordering::Relaxed) + n;
    if ARMED.load(Ordering::Relaxed) {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(n, Ordering::Relaxed);
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

// Safety: every operation delegates to System with the original pointer/layout;
// counters only perform lock-free integer atomics and cannot allocate/reenter.
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            allocated(l.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            allocated(l.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe {
            System.dealloc(p, l);
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let out = unsafe { System.realloc(p, l, n) };
        if !out.is_null() {
            LIVE.fetch_sub(l.size(), Ordering::Relaxed);
            allocated(n);
        }
        out
    }
}

#[global_allocator]
static GLOBAL: CountAlloc = CountAlloc;

const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const INT32: &str = "org.apache.cassandra.db.marshal.Int32Type";
const MAP: &str = "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";

fn schema(complex: bool) -> TableSchema {
    let col = |name: &str, ty: &str| ColumnDefinition {
        name: name.into(),
        type_name: ty.into(),
    };
    let mut regular_columns = vec![col("c0", UTF8), col("c1", UTF8)];
    if complex {
        regular_columns.push(col("c2", MAP));
    }
    TableSchema {
        keyspace: "probe".into(),
        table: "compaction".into(),
        key_type: UTF8.into(),
        clustering_columns: vec![col("ck", INT32)],
        static_columns: Vec::new(),
        regular_columns,
        extensions: Default::default(),
    }
}

fn value(source: usize, i: usize, bytes: usize) -> Vec<u8> {
    let mut out = vec![b'a' + source as u8; bytes.max(16)];
    out[..8].copy_from_slice(&(i as u64).to_be_bytes());
    out[8..16].copy_from_slice(&(source as u64).to_be_bytes());
    out
}

fn row(source: usize, i: usize, bytes: usize, complex: bool, last: bool) -> Row {
    let ts = 1000 + source as i64;
    let mut cells = vec![(0, CellValue::live(value(source, i, bytes), ts))];
    cells.push((
        1,
        if last && i.is_multiple_of(7) {
            CellValue::tombstone(ts, 1)
        } else {
            CellValue::live(value(source, i + 1, bytes), ts)
        },
    ));
    if complex {
        cells.push((
            2,
            CellValue::live(value(source, i + 2, 16), ts).with_path(b"p0".to_vec()),
        ));
        cells.push((
            2,
            CellValue::live(value(source, i + 3, 16), ts).with_path(b"p1".to_vec()),
        ));
    }
    Row {
        clustering: (i as i32).to_be_bytes().to_vec(),
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

fn inputs(
    base: &Path,
    dimensions: [usize; 2],
    fanin: usize,
    bytes: usize,
    complex: bool,
    compressed: bool,
    schema: &TableSchema,
) -> Vec<SSTableMetadata> {
    let [partitions, rows] = dimensions;
    (0..fanin)
        .map(|s| {
            let dir = base.join(format!("input-{s}"));
            std::fs::create_dir_all(&dir).unwrap();
            let mut parts: Vec<_> = (0..partitions)
                .map(|p| Partition {
                    key: DecoratedKey::new(PartitionKey::new(format!("key-{p:08}").into_bytes())),
                    deletion: DeletionTime::LIVE,
                    static_row: None,
                    rows: (0..rows)
                        .map(|i| row(s, i, bytes, complex, s + 1 == fanin))
                        .collect(),
                })
                .collect();
            parts.sort_by(|a, b| a.key.cmp(&b.key));
            let header = build_serialization_header(schema, &parts);
            let options = WriteOptions {
                compression: compressed.then_some(Compression::Lz4),
                verify_output: false,
                ..WriteOptions::default()
            };
            let mut writer = SSTableWriter::new(options, header);
            for p in &parts {
                writer.add_partition(p).unwrap();
            }
            let target = FileFlushTarget::new(dir.clone()).unwrap();
            target.flush(writer.finish().unwrap()).unwrap();
            SSTableMetadata {
                id: target.generation().to_string(),
                path: dir,
                size_bytes: 0,
                min_token: parts.first().unwrap().key.token.0,
                max_token: parts.last().unwrap().key.token.0,
                min_timestamp: 1000,
                max_timestamp: 1000 + fanin as i64,
                partition_count: partitions as u64,
                legacy_format: false,
            }
        })
        .collect()
}

fn validate(
    meta: &SSTableMetadata,
    partitions: usize,
    rows: usize,
    fanin: usize,
    bytes: usize,
    complex: bool,
    purge: bool,
) {
    let reader = open_file_sstable(&meta.path, &meta.id).unwrap();
    let mut iter = reader.partitions_iter().unwrap();
    let mut keys: Vec<_> = (0..partitions)
        .map(|p| DecoratedKey::new(PartitionKey::new(format!("key-{p:08}").into_bytes())))
        .collect();
    keys.sort();
    for key in keys {
        let p = iter
            .next_partition()
            .unwrap()
            .expect("missing output partition");
        assert_eq!(p.key, key, "partition identity/order changed");
        assert_eq!(p.rows.len(), rows, "missing or extra rows");
        for (i, actual) in p.rows.iter().enumerate() {
            let mut expected = row(fanin - 1, i, bytes, complex, true);
            if purge && i.is_multiple_of(7) {
                expected.cells.remove(1);
            }
            assert!(
                actual == &expected,
                "wrong merged value at row {i}: actual_cells={}, expected_cells={} (payload diagnostics omitted)",
                actual.cells.len(),
                expected.cells.len()
            );
        }
        // Exercise BTI partition lookup as well as sequential decoding.
        assert!(
            reader.get_partition(&key).unwrap().as_ref() == Some(&p),
            "BTI partition lookup disagrees with sequential output (payload diagnostics omitted)"
        );
        for i in [0, rows / 2, rows - 1] {
            let point = reader
                .get_clustering_row(&key, &(i as i32).to_be_bytes())
                .unwrap()
                .expect("missing BTI row lookup");
            assert_eq!(point.key, key);
            assert!(
                point.rows.as_slice() == std::slice::from_ref(&p.rows[i]),
                "BTI clustering lookup disagrees at row {i} (payload diagnostics omitted)"
            );
        }
    }
    assert!(
        iter.next_partition().unwrap().is_none(),
        "extra output partition"
    );
}

/// A process-isolated workload and deterministic allocation budgets.
pub struct Config {
    pub rows: usize,
    pub partitions: usize,
    pub fanin: usize,
    pub bytes: usize,
    pub repetitions: usize,
    pub complex: bool,
    pub purge: bool,
    pub compressed: bool,
    pub max_peak_bytes: Option<usize>,
    pub max_alloc_events: Option<usize>,
    pub max_allocated_bytes: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rows: 2048,
            partitions: 1,
            fanin: 4,
            bytes: 128,
            repetitions: 3,
            complex: false,
            purge: false,
            compressed: false,
            max_peak_bytes: None,
            max_alloc_events: None,
            max_allocated_bytes: None,
        }
    }
}

#[cfg(not(test))]
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let number =
        |i: usize, default: usize| args.get(i).map(|s| s.parse().unwrap()).unwrap_or(default);
    let rows = number(1, 2048);
    let partitions = number(2, 1);
    let fanin = number(3, 4);
    let bytes = number(4, 128);
    let repetitions = number(5, 3);
    let flags = args.get(6).map(String::as_str).unwrap_or("");
    let budget = |name| {
        std::env::var(name)
            .ok()
            .map(|s| s.parse().expect("allocation budget must be an integer"))
    };
    run(Config {
        rows,
        partitions,
        fanin,
        bytes,
        repetitions,
        complex: flags.contains("complex"),
        purge: flags.contains("purge"),
        compressed: flags.contains("lz4"),
        max_peak_bytes: budget("COMPACTION_MAX_PEAK_BYTES"),
        max_alloc_events: budget("COMPACTION_MAX_ALLOC_EVENTS"),
        max_allocated_bytes: budget("COMPACTION_MAX_ALLOCATED_BYTES"),
    });
}

/// Submit real compactions, validate every value/index, then enforce measured budgets.
/// Call only from a single-test process; allocator counters include worker threads.
pub fn run(config: Config) {
    // Configuration happens before any compaction worker thread is spawned.
    std::env::set_var("FERROSA_COMPACTION_WORKERS", "1");
    std::env::set_var("FERROSA_MAX_CONCURRENT_COMPACTIONS", "1");
    let Config {
        rows,
        partitions,
        fanin,
        bytes,
        repetitions,
        complex,
        purge,
        compressed,
        max_peak_bytes,
        max_alloc_events,
        max_allocated_bytes,
    } = config;
    assert!(rows > 0 && partitions > 0 && fanin > 0);
    let schema = schema(complex);
    let temp = tempfile::tempdir().unwrap();
    let inputs = inputs(
        temp.path(),
        [partitions, rows],
        fanin,
        bytes,
        complex,
        compressed,
        &schema,
    );
    let executor = CompactionExecutor::new();
    for repetition in 0..repetitions {
        let output_dir = temp.path().join(format!("output-{repetition}"));
        std::fs::create_dir_all(&output_dir).unwrap();
        let task = CompactionTask {
            inputs: inputs.clone(),
            output_dir,
            schema: schema.clone(),
            table_id: TableId::new("probe", "compaction"),
            purge: purge.then_some(PurgePolicy {
                gc_before: 10,
                max_purgeable_timestamp: i64::MAX,
                table_delete: DeletionTime::LIVE,
            }),
        };
        EVENTS.store(0, Ordering::Relaxed);
        BYTES.store(0, Ordering::Relaxed);
        let initial = LIVE.load(Ordering::Relaxed);
        PEAK.store(initial, Ordering::Relaxed);
        ARMED.store(true, Ordering::SeqCst);
        let start = Instant::now();
        executor.submit(task).unwrap();
        assert!(
            executor.await_result_available(Duration::from_secs(180)),
            "compaction produced no result"
        );
        let mut results = executor.poll_results();
        let elapsed = start.elapsed();
        ARMED.store(false, Ordering::SeqCst);
        let events = EVENTS.load(Ordering::Relaxed);
        let allocated = BYTES.load(Ordering::Relaxed);
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(initial);
        assert_eq!(results.len(), 1);
        let result = results.pop().unwrap();
        validate(
            &result.output,
            partitions,
            rows,
            fanin,
            bytes,
            complex,
            purge,
        );
        executor.release_task_inputs(&result.task);
        println!("{{\"correct\":true,\"rows\":{rows},\"partitions\":{partitions},\"fanin\":{fanin},\"value_bytes\":{bytes},\"complex\":{complex},\"purge\":{purge},\"compressed\":{compressed},\"repetition\":{repetition},\"alloc_events\":{events},\"allocated_bytes\":{allocated},\"peak_live_delta_bytes\":{peak},\"elapsed_ns\":{}}}", elapsed.as_nanos());
        // Optional deterministic RED/GREEN budgets are calibrated from measured
        // curves. Timing is deliberately reported without a CI wall-clock gate.
        for (name, observed, budget) in [
            ("COMPACTION_MAX_PEAK_BYTES", peak, max_peak_bytes),
            ("COMPACTION_MAX_ALLOC_EVENTS", events, max_alloc_events),
            (
                "COMPACTION_MAX_ALLOCATED_BYTES",
                allocated,
                max_allocated_bytes,
            ),
        ] {
            if let Some(budget) = budget {
                assert!(
                    observed <= budget,
                    "{name}: observed {observed} exceeds measured budget {budget}"
                );
            }
        }
    }
    executor.shutdown();
}
