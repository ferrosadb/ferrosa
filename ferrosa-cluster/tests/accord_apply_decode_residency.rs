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

use ferrosa_cluster::accord::apply::{
    ApplyMutation, EngineStorageApplier, MutationView, StorageApplier,
};
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

// ---------------------------------------------------------------------------
// OUTPUT-residency guard: the DECODED-APPLY term
// ---------------------------------------------------------------------------
//
// The two tests above measure the INPUT's residency. They are blind to the other
// half of the same claim: `apply_writeset_views` also accumulated the whole
// decoded `Vec<BatchOp>` — one op per row, with a key/keyspace/table clone and an
// owned restamped row each — and `apply_batch` lowered that into a resident
// `Vec<Mutation>` BEFORE committing anything. The decoded OUTPUT was therefore
// resident for the entire write-set even when no input was pinned. That is the
// largest single term of the measured COMMIT peak: `apply_writeset.decoded`
// reports +495 MiB over 329 873 ops (~1.5 KB/op) on node1 at N=1 000 114, and the
// coordinator is then SIGKILLed (exit 137) once the 3.7 GB heap stacks up with the
// 542 MB spill mmap on a 4 GB node.
//
// To isolate the OUTPUT the guard drives the BORROWED entry point:
// `apply_writeset_borrowed` takes `&[MutationView<'_>]`, so the encoded payloads
// live in a buffer built BEFORE `measure_peak` arms and are never counted in the
// window. What the window sees is the DECODED batch plus the engine's own
// memtable copy — the output term, with the input held constant. A decode that
// never materializes the whole op list keeps that window near the engine floor; a
// decode that builds `Vec<BatchOp>` first must push it up by ~the whole input.

/// One encoded mutation frame per entry (raw `Mutation::serialize_into` frames, the
/// wire shape `apply_writeset_borrowed` decodes), carrying a `ROW_BYTES` cell.
fn build_payloads(cell_ts: i64) -> Vec<Vec<u8>> {
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
            buf
        })
        .collect()
}

/// Decode every payload the way the applier does — the DECODED OUTPUT a
/// materializing decode would hold resident. Used only by the negative control.
fn decode_payloads(payloads: &[Vec<u8>], t: Timestamp) -> Vec<Mutation> {
    payloads
        .iter()
        .map(|bytes| {
            let (storage_data, _) =
                ferrosa_storage::accord::decode_postgres_mvcc_mutation(bytes).unwrap();
            Mutation::deserialize_from_rebinding_list_paths(storage_data, t).unwrap()
        })
        .collect()
}

/// Peak additional heap during ONE `apply_writeset_borrowed` of an `N`-entry
/// write-set whose ENCODED payloads were built outside the window.
///
/// `hold_decoded` is the deliberate bug the negative control injects: decode the
/// whole write-set into a resident `Vec<Mutation>` and hold it across the apply —
/// exactly the decoded-OUTPUT residency the fix removes.
fn borrowed_writeset_peak(hold_decoded: bool) -> (i64, i64) {
    let _guard = MEASURE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (engine, dir) = make_engine(if hold_decoded {
        "borrowed-control"
    } else {
        "borrowed-positive"
    });
    let applier = EngineStorageApplier::new(engine);
    let txn = TxnId::new(7, accord_ts(1_000));
    let t = accord_ts(1_000);
    // Built OUTSIDE the window: the encoded input is not a term of the measurement.
    let payloads = build_payloads(1_000);
    let input_bytes: i64 = payloads
        .iter()
        .map(|bytes| i64::try_from(bytes.len()).unwrap())
        .sum();
    let views: Vec<MutationView<'_>> = payloads
        .iter()
        .map(|bytes| MutationView {
            data: bytes.as_slice(),
            t,
            deps: &[],
        })
        .collect();
    let (_, peak) = measure_peak(|| {
        if hold_decoded {
            // NEGATIVE CONTROL: a full decoded copy of the write-set held across
            // the call — the residency a materializing decode leaves behind.
            let retained = decode_payloads(&payloads, t);
            applier
                .apply_writeset_borrowed(txn, &views)
                .expect("apply_writeset_borrowed must persist the write-set");
            std::hint::black_box(&retained);
        } else {
            applier
                .apply_writeset_borrowed(txn, &views)
                .expect("apply_writeset_borrowed must persist the write-set");
        }
    });
    let _ = std::fs::remove_dir_all(&dir);
    (input_bytes, peak)
}

/// Measure BOTH borrowed paths ONCE and cache `(input_bytes, consuming_peak,
/// holding_peak)`. The pair is the evidence: a materializing decode makes the
/// holding path the consuming path plus ~a whole decoded write-set.
fn measured_borrowed() -> (i64, i64, i64) {
    static MEASURED: std::sync::OnceLock<(i64, i64, i64)> = std::sync::OnceLock::new();
    *MEASURED.get_or_init(|| {
        let (input_bytes, consuming_peak) = borrowed_writeset_peak(false);
        let (_, holding_peak) = borrowed_writeset_peak(true);
        (input_bytes, consuming_peak, holding_peak)
    })
}

/// `apply_writeset_borrowed` MUST feed the decoded rows into the atomic apply as
/// it decodes them, never accumulating the whole decoded write-set first. With the
/// input held constant (and outside the window), a materializing decode makes the
/// window hold a full extra write-set of decoded rows on top of the engine's own
/// copy; a streaming decode holds only the engine's.
#[test]
fn apply_writeset_borrowed_does_not_hold_the_decoded_batch_resident() {
    let (input_bytes, consuming_peak, holding_peak) = measured_borrowed();
    eprintln!(
        "borrowed decoded-output residency: N={N} row_bytes={ROW_BYTES} input={input_bytes} B, \
         consume-peak={consuming_peak} B ({:.2}x input), hold-peak={holding_peak} B ({:.2}x input)",
        consuming_peak as f64 / input_bytes as f64,
        holding_peak as f64 / input_bytes as f64,
    );
    assert!(
        consuming_peak * 2 <= input_bytes * 3,
        "REGRESSION: apply_writeset_borrowed holds the whole DECODED write-set resident while \
         committing it — the window (input excluded) peaked at {consuming_peak} B for a \
         {input_bytes} B write-set ({:.2}x input). Streaming decode feeds each op into the atomic \
         apply as it decodes, so the window must stay at the engine's own copy, not the whole \
         decoded set on top of it.",
        consuming_peak as f64 / input_bytes as f64
    );
}

/// NEGATIVE CONTROL: the guard above MUST be able to fail. Materializing the whole
/// decoded write-set and holding it across the call re-introduces exactly the
/// residency the fix removes, so it MUST push the window at least ~a write-set
/// above the streaming path. If this stops separating, the guard has gone blind to
/// the decoded output and proves nothing.
#[test]
fn a_materialized_decoded_batch_trips_the_residency_guard() {
    let (input_bytes, consuming_peak, holding_peak) = measured_borrowed();
    let input = input_bytes.max(1);
    eprintln!(
        "negative control (decoded output): input={input_bytes} B, consume-peak={consuming_peak} B, \
         hold-peak={holding_peak} B, separation={} B",
        holding_peak - consuming_peak
    );
    assert!(
        holding_peak * 2 > input * 3,
        "the decoded-output guard is blind: materializing the whole decoded write-set did NOT \
         push the window above 1.5x the input (input={input_bytes} B, consume-peak={consuming_peak} \
         B, hold-peak={holding_peak} B). The guard no longer measures the decoded output."
    );
}

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
