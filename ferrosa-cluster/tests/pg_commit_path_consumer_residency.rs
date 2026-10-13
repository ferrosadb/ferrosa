//! Consumer-residency and byte-identity gates for the PostgreSQL COMMIT path.
//!
//! # The defect this guards
//!
//! The read set (`ferrosa-storage/src/accord/row_read_set.rs`) and the staged write set
//! (`StagedAccordWriteSet`) are **disk-backed**: a spilled entry is a slice of a memory
//! map, and their own `resident_bytes()` stays flat in the number of entries.
//! `ferrosa-postgres/tests/pg_full_scan_memory_bound.rs` asserts exactly that — but on the
//! set's OWN accessor, at the front end, BEFORE the commit path ever sees it.
//!
//! The COMMIT path is a second consumer, and it is where the property was violated.
//! `AccordTransactionCommitter::commit_postgres` walked the staged read set
//! (`PgReadSet::for_each_row_key` — a genuine visitor) and pushed every visited row key
//! into TWO resident `Vec`s (`resolve_inputs` and `write_set`), cloning the staged routing
//! index into both instead of using it, and `drive_accord` rebuilt a third from the staged
//! writes. So the spill bought nothing at the consumer: for a transaction that read N rows
//! the commit path held ~6 resident key buffers per conflict key. On a node whose RAM is
//! FIXED at 4 GB that is the OOM.
//!
//! # Why the existing gate missed it
//!
//! A green gate over the wrong object is worse than no gate. The front-end gate measured
//! the read set's own residency (a struct field). It never measured the consumer. The
//! property held where it was measured and was violated one layer up. THIS file measures the
//! consumer: the whole `commit_postgres` call, end to end, over a real sole-replica committer.
//!
//! # The instrument
//!
//! A counting `#[global_allocator]` scoped to this integration-test binary (its own
//! process). It counts buffers of at least [`KEY_SIZED`] bytes live at once, and the peak
//! live bytes. A conflict key is a heap `Vec<u8>` of well over `KEY_SIZED` bytes, so a count
//! of live key-sized buffers IS a count of resident conflict keys — the same load-independent
//! reasoning as `tests/accord_apply_decode_streaming_budget.rs`: CPU contention changes wall
//! clock and process RSS, but not how many key buffers one call holds at once.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use ferrosa_cluster::accord::apply::{ApplyError, ApplyMutation, StorageApplier};
use ferrosa_cluster::accord::handlers::AccordState;
use ferrosa_cluster::accord::state_machine::AccordStateMachine;
use ferrosa_cluster::accord::transaction_commit::{AccordTransactionCommitter, ReplicaResolver};
use ferrosa_cluster::accord::transport::AccordTransport;
use ferrosa_common::accord::{HybridLogicalClock, Timestamp, TxnId};
use ferrosa_storage::accord::sync_writer::MockSyncWriter;
use ferrosa_storage::accord::{
    PgReadSet, RowReadSet, StagedAccordWriteSet, StagedAccordWriteSetBuilder, TransactionCommitter,
    TransactionWrite,
};
use uuid::Uuid;

/// The fewest bytes a heap buffer must be to carry ONE conflict key. Every
/// partition/row key on this path is a `Vec<u8>` of a dozen or more bytes; nothing
/// smaller can be a resident key.
const KEY_SIZED: usize = 24;

/// Row keys recorded in the SMALL transaction. Few enough to fit the read set's staging
/// buffer, so the read set itself stays fully resident (and its own residency DOES scale
/// with this N — that is the front-end gate's object, not ours).
const SMALL_ROWS: usize = 64;

/// Row keys recorded in the LARGE transaction. Far past the read set's staging buffer, so
/// the read set SPILLS and its own `resident_bytes()` is 0 — every key the consumer then
/// holds is the consumer's own, not the set's.
const LARGE_ROWS: usize = 512;

/// The read set's staging buffer for this gate. Small, so the large set spills.
const READ_SET_BUFFER_BYTES: u64 = 4096;

/// Ordinary writes in the staged write-set for the residency measurement. A couple is
/// enough to route the COMMIT through the multi-key driver; the growth we measure is the
/// read set's keys at the consumer.
const STAGED_WRITES: usize = 2;

// ── key-sized buffer probe (scoped to this integration-test binary) ──────────

struct KeyBufferProbe;

static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE_KEYS: AtomicI64 = AtomicI64::new(0);
static PEAK_KEYS: AtomicI64 = AtomicI64::new(0);
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static PEAK_BYTES: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for KeyBufferProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            let live = LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed)
                + layout.size() as i64;
            PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
            if layout.size() >= KEY_SIZED {
                let keys = LIVE_KEYS.fetch_add(1, Ordering::Relaxed) + 1;
                PEAK_KEYS.fetch_max(keys, Ordering::Relaxed);
            }
        }
        // SAFETY: delegated to the system allocator with the same layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) {
            // Clamp at zero: `measure` zeroes the counters at arm time, so a free of memory
            // allocated BEFORE the window would drive a counter negative and, because the
            // peak is a running maximum of the live value, suppress every later observation.
            let _ = LIVE_BYTES.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |b| {
                Some((b - layout.size() as i64).max(0))
            });
            if layout.size() >= KEY_SIZED {
                let _ = LIVE_KEYS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |k| {
                    Some((k - 1).max(0))
                });
            }
        }
        // SAFETY: delegated to the system allocator with the same pointer/layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: KeyBufferProbe = KeyBufferProbe;

/// What one measured commit cost the consumer.
#[derive(Clone, Copy, Debug)]
struct ConsumerSample {
    peak_keys: i64,
    peak_bytes: i64,
}

/// Run `f` with the probe armed; return its value and the peak live key buffers / bytes
/// observed during the call.
fn measure<T>(f: impl FnOnce() -> T) -> (T, ConsumerSample) {
    LIVE_KEYS.store(0, Ordering::Relaxed);
    PEAK_KEYS.store(0, Ordering::Relaxed);
    LIVE_BYTES.store(0, Ordering::Relaxed);
    PEAK_BYTES.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let out = f();
    ARMED.store(false, Ordering::Relaxed);
    (
        out,
        ConsumerSample {
            peak_keys: PEAK_KEYS.load(Ordering::Relaxed),
            peak_bytes: PEAK_BYTES.load(Ordering::Relaxed),
        },
    )
}

// ── a real sole-replica committer ────────────────────────────────────────────

/// Records every applied mutation's data, so a test can assert what the commit path
/// actually drove to the applier.
#[derive(Default)]
struct RecordingApplier {
    applied: Mutex<Vec<Vec<u8>>>,
}

impl RecordingApplier {
    fn applied_data(&self) -> Vec<Vec<u8>> {
        self.applied.lock().expect("applier mutex").clone()
    }
}

impl StorageApplier for RecordingApplier {
    fn register_postgres_mvcc_observer(
        &self,
        _observer: Arc<dyn ferrosa_storage::accord::PostgresMvccApplyObserver>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn apply(&self, _txn_id: TxnId, mutation: ApplyMutation) -> Result<(), ApplyError> {
        self.applied
            .lock()
            .expect("applier mutex")
            .push(mutation.data);
        Ok(())
    }
}

/// No reachable peers: the coordinator is the sole replica, so every send is a self-send
/// the driver never makes.
struct NoPeersTransport;

#[async_trait::async_trait]
impl AccordTransport for NoPeersTransport {
    async fn send(
        &self,
        _host: Uuid,
        _msg: ferrosa_net::message::Message,
        _lane: ferrosa_net::codec::Lane,
    ) -> ferrosa_net::error::Result<ferrosa_net::message::Message> {
        Err(ferrosa_net::error::NetError::Timeout("no peers".into()))
    }
}

fn node_id_of(u: Uuid) -> u64 {
    u64::from_be_bytes(u.as_bytes()[..8].try_into().expect("uuid 16 bytes"))
}

const HOST: Uuid = Uuid::from_u128(0xC0FFEE);

fn committer(applier: Arc<RecordingApplier>) -> AccordTransactionCommitter {
    let node_id = node_id_of(HOST);
    let clock = Arc::new(HybridLogicalClock::new(node_id, 0));
    let state: AccordState = Arc::new(parking_lot::Mutex::new(AccordStateMachine::with_applier(
        node_id,
        Arc::new(MockSyncWriter::new()),
        applier.clone(),
    )));
    let resolve: ReplicaResolver = Arc::new(move |_ks: &str, _key: &[u8]| Some(vec![HOST]));
    AccordTransactionCommitter::new(node_id, clock, Arc::new(NoPeersTransport), applier, resolve)
        .with_local_accord_state(state)
}

fn snapshot() -> Timestamp {
    Timestamp {
        epoch: 0,
        time: 1,
        seq: 0,
        node: 0,
    }
}

/// One runtime per measurement; the probe is process-global, so the tests are `serial`.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime builds")
}

fn staged_writes(n: usize) -> StagedAccordWriteSet {
    let mut builder = StagedAccordWriteSetBuilder::with_threshold_bytes(u64::MAX);
    for i in 0..n {
        builder
            .accept(TransactionWrite {
                keyspace: "ks".to_string(),
                key: format!("w{i:032}").into_bytes(),
                mutation: vec![1, 2, 3],
            })
            .expect("stage write");
    }
    builder.finish().expect("finish staging")
}

/// A read set of `n` row keys, staged through the SHARED write-set staging buffer so the
/// large case spills and the set's own residency is zero.
fn read_set_with(n: usize, tag: &str) -> PgReadSet {
    let root = std::env::temp_dir().join(format!(
        "ferrosa-commit-consumer-gate-{}-{tag}",
        std::process::id()
    ));
    let mut rows = RowReadSet::with_threshold(&root, READ_SET_BUFFER_BYTES);
    for i in 0..n {
        rows.record(format!("r{i:032}").as_bytes()).expect("record");
    }
    PgReadSet::with_row_keys(Vec::new(), rows.finish().expect("finish read set"))
}

/// One COMMIT over the real consumer: `commit_postgres` on a sole-replica committer, with
/// `n` staged read keys. Returns what the consumer held at peak, plus the read set's OWN
/// residency as the caller handed it over — the two numbers the front-end gate conflated.
fn measure_commit(n: usize, tag: &str) -> (ConsumerSample, u64, bool) {
    let rt = runtime();
    let applier = Arc::new(RecordingApplier::default());
    let committer = committer(applier);
    // Build the fixture OUTSIDE the measured window: constructing the input is itself
    // allocating work, and counting it would measure the test, not the subject.
    let writes = staged_writes(STAGED_WRITES);
    let reads = read_set_with(n, tag);
    let set_resident = reads.resident_bytes();
    let set_spilled = reads.spilled_to_disk();

    let (outcome, sample) = measure(|| {
        rt.block_on(async {
            committer
                .commit_postgres("ks", writes, reads, Vec::new(), snapshot())
                .await
        })
    });
    assert!(
        outcome.is_ok(),
        "the COMMIT must drive to a decision: {outcome:?}"
    );
    (sample, set_resident, set_spilled)
}

/// Resident key buffers per conflict key the Accord driver STRUCTURALLY holds, measured
/// on the fixed tree. This is the FLOOR this lane reaches, not a target:
///
/// 1. the driver's own `write_set` (one owned key per conflict key);
/// 2. the per-key replica index the participant resolver is built from;
/// 3. the `PreAcceptV2` wire key list;
/// 4. the local self-vote key list;
/// 5. the state machine's conflict index (one registered entry per conflict key).
///
/// Copies 1, 4 and 5 are irreducible without a change to the consensus core: the driver
/// orders conflicts on its key set, and `handle_preaccept_multi_with_snapshot` takes
/// `&[&[u8]]` and registers the transaction under EVERY key. A sixth copy — the separate
/// `resolve_inputs` collection plus the clone of the staged routing index into
/// `write_set` — was removed by this lane.
const DRIVER_REQUIRED_KEY_COPIES: i64 = 5;

/// Slack above the structurally-required copies. The measured post-fix baseline is 2593
/// buffers for 516 conflict keys (5.03 per key); the pre-fix baseline was 3113 (6.03). The
/// slack keeps a couple of protocol allocations from flaking the ratchet while leaving the
/// removed copy (one per key, ~516) far outside it.
const KEY_BUFFER_SLACK: i64 = 128;

/// The COMMIT path's consumer residency is NOT flat in the conflict-key count, and this
/// gate does not pretend otherwise.
///
/// What it DOES pin is the accumulation this lane removed: `commit_postgres` used to build
/// `resolve_inputs` and `write_set` (two resident `Vec`s) over every staged write key AND
/// every visited row key of a keyed read, and `drive_accord` rebuilt a third — on top of the
/// driver's own copies — so the spill bought nothing at the consumer, and a transaction that
/// read N rows held ~6 resident key buffers per conflict key.
///
/// The gate measures the whole `commit_postgres` call (a real sole-replica committer, the
/// real read set) and bounds the peak by the driver's required copies plus slack. It is a
/// RATCHET: re-introducing the duplicated collections the commit path used to build pushes
/// the peak to ~6 per key and fails here.
///
/// The measured baseline is 5.0 buffers per conflict key after the fix (2593 for 512 rows,
/// 354 for 64) against 6.0 before it (3113 / 426).
#[test]
#[serial_test::serial]
fn commit_path_consumer_does_not_duplicate_the_staged_conflict_key_set() {
    let (small, small_set_resident, small_set_spilled) = measure_commit(SMALL_ROWS, "small");
    let (large, large_set_resident, large_set_spilled) = measure_commit(LARGE_ROWS, "large");

    eprintln!(
        "commit consumer residency: small rows={SMALL_ROWS} peak_keys={} peak_bytes={} \
         set_resident={small_set_resident} set_spilled={small_set_spilled} | \
         large rows={LARGE_ROWS} peak_keys={} peak_bytes={} set_resident={large_set_resident} \
         set_spilled={large_set_spilled}",
        small.peak_keys, small.peak_bytes, large.peak_keys, large.peak_bytes
    );

    // CALIBRATION. The large read set must have SPILLED, so its own residency is zero and
    // every key the consumer holds is the consumer's own. Without this a too-small
    // LARGE_ROWS would pass for the wrong reason, and the two scales would not be
    // distinguishable.
    assert!(
        !small_set_spilled,
        "the small read set must fit the {READ_SET_BUFFER_BYTES}-byte buffer"
    );
    assert!(
        small_set_resident > 0,
        "the small read set must be resident, not empty"
    );
    assert!(
        large_set_spilled && large_set_resident == 0,
        "the large read set must have SPILLED (own residency 0), but resident={} spilled={}",
        large_set_resident,
        large_set_spilled
    );
    // Growth must be visible at all — a gate that cannot see the row count is blind.
    assert!(
        large.peak_keys > small.peak_keys,
        "the probe is blind: a {LARGE_ROWS}-row read set cost {} key buffers against {} for \
         {SMALL_ROWS} rows. The measurement is not tracking the conflict-key count.",
        large.peak_keys,
        small.peak_keys
    );

    // THE INVARIANT. The consumer must not hold MORE than the driver's structurally
    // required copies of the conflict-key set: a per-row-key collection rebuilt at the
    // consumer (the shape this lane removed) lands at ~6 per key and fails here.
    let conflict_keys = LARGE_ROWS as i64 + STAGED_WRITES as i64 + 2;
    let budget = DRIVER_REQUIRED_KEY_COPIES * conflict_keys + KEY_BUFFER_SLACK;
    assert!(
        large.peak_keys <= budget,
        "the COMMIT path's consumer held {} key-sized buffers live for a {LARGE_ROWS}-row read \
         set (budget {budget} = {DRIVER_REQUIRED_KEY_COPIES} per conflict key, the count the \
         Accord driver structurally requires, plus {KEY_BUFFER_SLACK} slack). A resident \
         per-row-key collection rebuilt at the consumer — the `resolve_inputs`/`write_set` \
         pair this path used to build — lands at ~6 per key. Residency is NOT flat in the row \
         count; this bounds it at the driver's own copies.",
        large.peak_keys
    );
}

/// INVARIANT: DATA PRESERVED + RESULTS COMPLETE + ORDERING. Every staged write's payload
/// reaches the storage applier byte-identical, in write order, with nothing dropped — the
/// property that makes the streaming rewrite a memory change and not a semantic one. The
/// routing index is MOVED into the driver instead of cloned, so this is also the differential
/// test for the move.
#[test]
#[serial_test::serial]
fn staged_write_payloads_reach_the_applier_byte_identical_and_in_order() {
    const N: usize = 8;
    let rt = runtime();
    let applier = Arc::new(RecordingApplier::default());
    let committer = committer(applier.clone());

    // Distinct, non-empty payloads; `writes[i]`'s payload must arrive at position i.
    let mut builder = StagedAccordWriteSetBuilder::with_threshold_bytes(u64::MAX);
    let expected: Vec<Vec<u8>> = (0..N)
        .map(|i| format!("payload-{i:02}-{}-suffix", "x".repeat(1 + i)).into_bytes())
        .collect();
    for (i, payload) in expected.iter().enumerate() {
        builder
            .accept(TransactionWrite {
                keyspace: "ks".to_string(),
                key: format!("w{i:032}").into_bytes(),
                mutation: payload.clone(),
            })
            .expect("stage write");
    }
    let writes = builder.finish().expect("finish staging");

    // A read set with a spilled row set, so the appended keys are exercised too.
    let reads = read_set_with(256, "order");
    assert!(reads.spilled_to_disk(), "the read set must have spilled");

    let outcome = rt.block_on(async {
        committer
            .commit_postgres("ks", writes, reads, Vec::new(), snapshot())
            .await
    });
    assert!(outcome.is_ok(), "the COMMIT must drive to a decision");

    // Read registrations and protocol markers carry EMPTY payloads (the applier drops them
    // before applying data mutations); the staged writes carry the real bytes.
    let applied: Vec<Vec<u8>> = applier
        .applied_data()
        .into_iter()
        .filter(|data| !data.is_empty())
        .collect();
    assert_eq!(
        applied, expected,
        "the staged write-set must reach the applier byte-identical and in write order — a \
         key or payload silently dropped or reordered by the streaming path is a data-loss \
         bug, not a memory fix"
    );
}
