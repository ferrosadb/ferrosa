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
//! # What this test can — and cannot — prove
//!
//! An ABSOLUTE bound on the call's peak cannot prove the residency claim:
//! `apply_writeset` inherently holds ~2x the input at peak even when the decode
//! pins nothing, because the decoded batch (~1x, moved into `apply_batch`) then
//! co-exists with the memtable's own copy of the same rows (~1x). Measured with a
//! realistic 32 MiB commit-log segment that is 2.27x; with this file's original
//! 4 KiB test segment (`CommitLogConfig::test_config`) it was 7.70x, because a
//! 4 KiB segment can hold only one entry, so a whole batch shreds into one
//! segment file per row.
//!
//! What IS specific to the claim is the **DELTA** between a decode that pins the
//! input and one that consumes it. `measured()` records both once: the consuming
//! path (the code under test) and the pinning control. The control holds a clone
//! of every entry's payload across the call — exactly the residency the fix
//! removes — so it must sit ~1x the input ABOVE the consuming path. If
//! `apply_writeset` regressed to pinning, the consuming path would hold that same
//! clone and the delta would collapse to ~0, failing both tests.
//!
//! This test proves the RESIDENCY claim, not a value: it arms a counting global
//! allocator around `apply_writeset` and asserts the peak additional heap during
//! the consuming call is a full input-size below the pinning call's.
//! `a_retained_input_payload_trips_the_residency_guard` is the NEGATIVE CONTROL:
//! it asserts that very separation, so the guard is measuring the input's
//! residency rather than a constant.

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
// for the duration of its closure only, so the tests cannot perturb each other.
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
/// Under `CommitLogConfig::test_config`'s ~4063-byte usable segment a batch entry
/// must be smaller than that, or `apply_batch` refuses (fail-loud) and the write
/// is never persisted — so the measured window would not include the apply. 3000
/// bytes of cell leaves headroom.
const ROW_BYTES: usize = 3000;
const N: usize = 8192;
/// The commit-log segment this test runs against. `test_config`'s 4 KiB default
/// is pathological for a multi-megabyte write-set: a 4 KiB segment holds one
/// ~3 KB entry, so `apply_batch` rolls a fresh segment per row (~8192 files) and
/// the peak is dominated by that rollover (7.70x input, 144 s) rather than by the
/// decode under test. A production deployment sizes the segment to exceed the
/// largest mutation (the loadgen harness uses 32 MiB for exactly this reason), and
/// at 32 MiB the window measures the decode (2.27x input, seconds).
const SEGMENT_BYTES: usize = 32 * 1024 * 1024;

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

fn make_engine(label: &str) -> (Arc<StorageEngine>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "ferrosa-apply-residency-it-{}-{label}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut config = StorageEngineConfig::test_config(&dir);
    config.commit_log.segment_size = SEGMENT_BYTES;
    // Keep what `apply_batch` writes resident BOUNDED (`flush_threshold_bytes`)
    // so the measured peak is the decode's residency, not the memtable's.
    config.flush_threshold_bytes = 1024 * 1024;
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
    let (engine, dir) = make_engine(if retain_input { "control" } else { "positive" });
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
            applier
                .apply_writeset(txn, mutations)
                .expect("apply_writeset must persist the write-set");
            std::hint::black_box(&retained);
        } else {
            applier
                .apply_writeset(txn, mutations)
                .expect("apply_writeset must persist the write-set");
        }
        input_bytes
    });
    let _ = std::fs::remove_dir_all(&dir);
    (input_bytes, peak)
}

/// Measure BOTH paths ONCE and cache `(input_bytes, consuming_peak, pinning_peak)`.
///
/// Both tests read this so the multi-second measurement (and its two real
/// commit-log flushes) runs once per test binary. The pair is the whole evidence:
/// the pinning peak is the consuming peak plus a retained clone of the input.
fn measured() -> (i64, i64, i64) {
    static MEASURED: std::sync::OnceLock<(i64, i64, i64)> = std::sync::OnceLock::new();
    *MEASURED.get_or_init(|| {
        let (input_bytes, consuming_peak) = writeset_peak(false);
        let (_, pinning_peak) = writeset_peak(true);
        (input_bytes, consuming_peak, pinning_peak)
    })
}

/// The consuming decode MUST free each entry's payload as it decodes it: pinning
/// that same input (the control) adds ~1x its size ON TOP, so the consuming path
/// must sit a full input-size BELOW the pinning path. A materializing decode — or
/// one that regresses to it — holds the same clone and closes the gap.
#[test]
fn apply_writeset_peak_stays_within_the_input_size() {
    let (input_bytes, consuming_peak, pinning_peak) = measured();
    let resident = pinning_peak - consuming_peak;
    let input = input_bytes.max(1);
    eprintln!(
        "apply_writeset residency: N={N} row_bytes={ROW_BYTES} input={input_bytes} B, \
         consume-peak={consuming_peak} B, pin-peak={pinning_peak} B, resident-delta={resident} B"
    );
    assert!(
        resident * 10 >= input * 9,
        "REGRESSION: apply_writeset holds the whole encoded write-set resident while \
         decoding it — the consuming peak ({consuming_peak} B) is only {resident} B below the \
         pinning peak ({pinning_peak} B) against a {input_bytes} B input. Consuming each entry \
         as it decodes leaks the input, so the pinning control must sit ~a full input-size \
         above the consuming path; a gap this small means the decode is pinning too."
    );
    assert!(
        resident * 2 <= input * 3,
        "the residency guard measured a {resident} B separation for a {input_bytes} B input \
         (ratio {:.2}). A single retained clone cannot add more than ~1x the input, so the two \
         runs are not comparable.",
        resident as f64 / input as f64
    );
}

/// NEGATIVE CONTROL: the guard above MUST be able to fail. Holding a clone of the
/// write-set across the call re-introduces exactly the residency the fix removes,
/// so it MUST separate the pinning peak from the consuming peak by ~the input's
/// size. If this test ever stops seeing that separation, the guard has gone blind
/// to the input and proves nothing.
#[test]
fn a_retained_input_payload_trips_the_residency_guard() {
    let (input_bytes, consuming_peak, pinning_peak) = measured();
    let resident = pinning_peak - consuming_peak;
    let input = input_bytes.max(1);
    eprintln!(
        "negative control residency: input={input_bytes} B, consume-peak={consuming_peak} B, \
         pin-peak={pinning_peak} B, separation={resident} B, pin-ratio={:.2}",
        pinning_peak as f64 / input as f64
    );
    assert!(
        resident * 10 >= input * 9,
        "the residency guard is blind: retaining a full clone of the input did NOT separate the \
         pinning peak from the consuming peak by ~the input's size (input={input_bytes} B, \
         consume-peak={consuming_peak} B, pin-peak={pinning_peak} B, separation={resident} B). \
         The guard no longer measures the input's residency."
    );
}
