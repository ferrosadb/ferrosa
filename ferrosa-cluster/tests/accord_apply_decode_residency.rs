//! TDD red guard: `EngineStorageApplier::apply_writeset` MUST NOT hold the whole
//! encoded write-set resident while it decodes it into the `Vec<BatchOp>` it
//! applies.
//!
//! Attribution (PG-ACC-03, reproduced from the raw `alloc-probe` logs): at
//! N=1 100 000 the COMMIT peak is dominated by the DECODED APPLY path — +836 MB
//! over `decoded=352 515` ops ≈ 2.4 KB per decoded op, a ratio stable across the
//! 11x range — and it is NOT the ~157+157 MB MVCC version store the inherited
//! label blamed. The decode loop consumed `&mutations`, so every entry's encoded
//! payload (~670 MB at that N) stayed pinned next to the decoded `Vec<BatchOp>`
//! until the function returned: the Apply peak held the INPUT and the OUTPUT at
//! once.
//!
//! This test proves the RESIDENCY claim, not a value: it arms a counting global
//! allocator around `apply_writeset` and asserts the peak additional heap
//! during the call stays within a bounded factor of the input size. Holding the
//! input alongside the output costs ~2x the input; consuming the input as it is
//! decoded costs ~1x. `a_retained_input_payload_trips_the_residency_guard` is
//! the NEGATIVE CONTROL: it keeps a clone of the input alive across the call and
//! asserts the SAME guard DOES trip — so the guard is measuring the input's
//! residency, not a constant.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use ferrosa_cluster::accord::apply::{ApplyMutation, EngineStorageApplier, StorageApplier};
use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{Mutation, StorageEngine, StorageEngineConfig};

// --- peak-allocation tracker (scoped to this integration-test binary only) ---
// Every test in this file measures inside `measure_peak`, which arms the tracker
// for the duration of its closure only, so the two tests cannot perturb each
// other. (Both run concurrently under the default test harness — the window is
// single-threaded and each closure runs to completion while armed.)
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
            // Clamp at zero: `measure_peak` zeroes LIVE at arm time, so a free of
            // memory allocated BEFORE the window would drive the counter negative
            // and, because PEAK is a running maximum of LIVE, suppress every later
            // allocation.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some((live - layout.size() as i64).max(0))
            });
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

/// Measure peak additional heap bytes held at once during `f`.
///
/// Serialized by `MEASURE_LOCK` (held by the caller for the whole setup+window)
/// because the tracker is a process-global allocator: a concurrently-running
/// test's allocations would otherwise pollute the window.
static MEASURE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn measure_peak<R>(f: impl FnOnce() -> R) -> (R, i64) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let out = f();
    ARMED.store(false, Ordering::SeqCst);
    (out, PEAK.load(Ordering::SeqCst))
}

const KS: &str = "apply_residency_ks";
const TABLE: &str = "apply_residency_table";
const ROW_BYTES: usize = 16 * 1024;
const N: usize = 1024;

fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TABLE.to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: Default::default(),
    }
}

fn make_engine() -> (Arc<StorageEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageEngineConfig::test_config(dir.path());
    let engine = StorageEngine::new(config, None).unwrap();
    engine.register_table(test_schema()).unwrap();
    (Arc::new(engine), dir)
}

fn accord_ts(micros: u64) -> Timestamp {
    Timestamp::synthetic(micros * 1_000)
}

fn make_key(i: usize) -> DecoratedKey {
    DecoratedKey {
        token: Token(i as i64),
        key: PartitionKey::new(format!("pk-{i:08}").into_bytes()),
    }
}

/// One `ApplyMutation` whose `data` is a serialized commit-log `Mutation`
/// carrying a `ROW_BYTES` cell — the same wire format the production applier
/// decodes. Built INSIDE the measurement window so the counting allocator sees
/// the input's residency.
fn build_writeset(cell_ts: i64) -> Vec<ApplyMutation> {
    let t = accord_ts(1_000);
    let value = vec![b'x'; ROW_BYTES];
    (0..N)
        .map(|i| {
            let row = Row {
                clustering: vec![0x00, 0x00, 0x00, 0x01],
                cells: vec![(0, CellValue::live(value.clone(), cell_ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(cell_ts),
            };
            let m = Mutation::new(
                KS.to_string(),
                TABLE.to_string(),
                make_key(i),
                vec![row],
                cell_ts,
            );
            let mut buf = vec![0u8; m.serialized_size()];
            m.serialize_into(&mut buf);
            ApplyMutation {
                data: buf,
                t,
                deps: vec![],
            }
        })
        .collect()
}

/// The peak additional heap during ONE `apply_writeset` of a `N`-entry,
/// `ROW_BYTES`-per-entry write-set, next to that write-set's total encoded size.
/// `retain_input` is the deliberate bug the negative control injects: holding a
/// clone of the input alive across the call.
fn writeset_peak(retain_input: bool) -> (i64, i64) {
    // Serialize the whole setup + measurement: the tracker is process-global.
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (engine, _dir) = make_engine();
    let applier = EngineStorageApplier::new(engine);
    let txn = TxnId::new(7, accord_ts(1_000));
    let (input_bytes, peak) = measure_peak(|| {
        let mutations = build_writeset(1_000);
        let input_bytes: i64 = mutations
            .iter()
            .map(|m| i64::try_from(m.data.len()).unwrap())
            .sum();
        if retain_input {
            // NEGATIVE CONTROL: a clone of the input (`Vec<Vec<u8>>`) held across
            // the whole call — exactly the residency the fix removes.
            let retained: Vec<Vec<u8>> = mutations.iter().map(|m| m.data.clone()).collect();
            let _ = applier.apply_writeset(txn, mutations);
            std::hint::black_box(&retained);
        } else {
            let _ = applier.apply_writeset(txn, mutations);
        }
        input_bytes
    });
    (input_bytes, peak)
}

/// The decode MUST free each entry's payload as it decodes it: holding the whole
/// input next to the whole decoded output costs ~2x the input, consuming it costs
/// ~1x. A materializing decode fails here.
#[test]
fn apply_writeset_peak_stays_within_the_input_size() {
    let (input_bytes, peak) = writeset_peak(false);
    let ratio = peak as f64 / input_bytes.max(1) as f64;
    eprintln!(
        "apply_writeset residency: N={N} row_bytes={ROW_BYTES} input={input_bytes} B, \
         peak={peak} B, ratio={ratio:.2}"
    );
    assert!(
        peak * 2 < input_bytes * 3,
        "REGRESSION: apply_writeset holds the whole encoded write-set resident while \
         decoding it — input={input_bytes} B, peak additional heap={peak} B \
         (ratio {ratio:.2}). The decode loop is pinning every entry's payload next to \
         the decoded Vec<BatchOp> instead of consuming each entry as it decodes it."
    );
}

/// NEGATIVE CONTROL: the guard above MUST be able to fail. Holding a clone of the
/// write-set across the call re-introduces exactly the residency the fix removes,
/// so the same measurement MUST exceed the bound. If this test ever stops
/// tripping, the guard has gone blind to the input and proves nothing.
#[test]
fn a_retained_input_payload_trips_the_residency_guard() {
    let (input_bytes, peak) = writeset_peak(true);
    let ratio = peak as f64 / input_bytes.max(1) as f64;
    eprintln!("negative control residency: input={input_bytes} B, peak={peak} B, ratio={ratio:.2}");
    assert!(
        peak * 2 >= input_bytes * 3,
        "the residency guard is blind: retaining a full clone of the input did NOT push \
         the measured peak past the bound (input={input_bytes} B, peak={peak} B, ratio \
         {ratio:.2}). The guard no longer measures the input's residency."
    );
}
