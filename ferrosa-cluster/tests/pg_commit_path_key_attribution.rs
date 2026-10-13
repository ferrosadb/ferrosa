//! ATTRIBUTION of the PostgreSQL COMMIT path's resident conflict keys, by ALLOCATION SITE.
//!
//! # The question this answers
//!
//! `ferrosa-cluster/tests/pg_commit_path_consumer_residency.rs` measures that a keyed
//! transaction's COMMIT holds about five key-sized buffers per conflict key, and pins that
//! as a RATCHET. It does not say WHICH allocation site put them there, so any fix is a
//! guess: the sites it was suspected of living in were enumerated by READING, and one of
//! that list (`ferrosa-postgres/src/txn_write_set.rs`) had already been measured and found
//! to be NOT the same defect. Reading is not attribution.
//!
//! # The instrument
//!
//! Every site that can allocate a conflict key tags itself through
//! `ferrosa_common::mem_probe::{enter_site, exit_site}` (see the site constants and
//! `SITE_NAMES` there), and this binary's counting `#[global_allocator]` charges each
//! allocation of at least [`KEY_SIZED`] bytes to the tag that was current when it was made.
//! The result is a per-site table — "N key-sized buffers came from X" — not a hypothesis.
//!
//! # What these counters do and do NOT prove
//!
//! They count the ALLOCATION of a key-sized buffer. A `dealloc` carries no tag, so a site's
//! retained SHARE of the peak is not readable off this table alone; it is established by
//! REMOVING the site and re-measuring at the ratchet
//! (`pg_commit_path_consumer_residency`). Read the two together: this table says where the
//! buffers come from, the ratchet says which of them a fix can remove.
//!
//! The `untagged` bucket is the honesty check: a large remainder means a site exists that
//! the instrument does not know about, and the table must not be reported as complete.
//!
//! Gated on `alloc-probe` (see `Cargo.toml`): with the tag seam off, every `enter_site`
//! folds away and the table would read as all-untagged, which would be a fabricated zero
//! rather than a measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use ferrosa_cluster::accord::apply::{ApplyError, ApplyMutation, StorageApplier};
use ferrosa_cluster::accord::handlers::AccordState;
use ferrosa_cluster::accord::state_machine::AccordStateMachine;
use ferrosa_cluster::accord::transaction_commit::{AccordTransactionCommitter, ReplicaResolver};
use ferrosa_cluster::accord::transport::AccordTransport;
use ferrosa_common::accord::{HybridLogicalClock, Timestamp, TxnId};
use ferrosa_common::mem_probe;
use ferrosa_storage::accord::sync_writer::MockSyncWriter;
use ferrosa_storage::accord::{
    PgReadSet, RowReadSet, StagedAccordWriteSet, StagedAccordWriteSetBuilder, TransactionCommitter,
    TransactionWrite,
};
use uuid::Uuid;

/// The fewest bytes a heap buffer must be to carry ONE conflict key. Identical to the
/// ratchet's floor, so the two instruments count the same thing.
const KEY_SIZED: usize = 24;

/// Row keys recorded in the read set. Far past the read set's staging buffer, so the set
/// SPILLS and every key the commit path holds is the commit path's own.
const LARGE_ROWS: usize = 512;

/// The read set's staging buffer for this gate. Small, so the large set spills.
const READ_SET_BUFFER_BYTES: u64 = 4096;

/// Ordinary writes in the staged write-set. A couple is enough to route the COMMIT through
/// the multi-key driver.
const STAGED_WRITES: usize = 2;

/// The conflict keys the commit drives: the staged writes, the appended table keys and
/// watermarks (none here — the fixture scans and writes no tables), the visited row keys,
/// and the two protocol markers.
fn conflict_keys() -> i64 {
    LARGE_ROWS as i64 + STAGED_WRITES as i64 + 2
}

// ── site-charging probe (scoped to this integration-test binary) ────────────

struct KeyBufferProbe;

static ARMED: AtomicBool = AtomicBool::new(false);
static LIVE_KEYS: AtomicI64 = AtomicI64::new(0);
static PEAK_KEYS: AtomicI64 = AtomicI64::new(0);

unsafe impl GlobalAlloc for KeyBufferProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) && layout.size() >= KEY_SIZED {
            let live = LIVE_KEYS.fetch_add(1, Ordering::Relaxed) + 1;
            PEAK_KEYS.fetch_max(live, Ordering::Relaxed);
            // Charge the buffer to the site that is tagging itself right now. This is
            // the whole attribution: a named site, or the untagged bucket.
            mem_probe::record_site_keyed_alloc(mem_probe::current_site(), layout.size());
        }
        // SAFETY: delegated to the system allocator with the same layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(Ordering::Relaxed) && layout.size() >= KEY_SIZED {
            // Clamp at zero: `measure` zeroes the counter at arm time, so a free of memory
            // allocated BEFORE the window would drive it negative.
            let _ = LIVE_KEYS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |k| {
                Some((k - 1).max(0))
            });
        }
        // SAFETY: delegated to the system allocator with the same pointer/layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: KeyBufferProbe = KeyBufferProbe;

/// Run `f` with the probe and the site counters armed.
fn measure<T>(f: impl FnOnce() -> T) -> T {
    LIVE_KEYS.store(0, Ordering::Relaxed);
    PEAK_KEYS.store(0, Ordering::Relaxed);
    mem_probe::reset_sites();
    ARMED.store(true, Ordering::Relaxed);
    let out = f();
    ARMED.store(false, Ordering::Relaxed);
    out
}

// ── a real sole-replica committer ────────────────────────────────────────────

/// Records every applied mutation's data.
#[derive(Default)]
struct RecordingApplier {
    applied: Mutex<Vec<Vec<u8>>>,
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
/// the driver never makes, and the whole commit runs on this node.
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
fn read_set_with(n: usize) -> PgReadSet {
    let root = std::env::temp_dir().join(format!("ferrosa-commit-attr-{}", std::process::id()));
    let mut rows = RowReadSet::with_threshold(&root, READ_SET_BUFFER_BYTES);
    for i in 0..n {
        rows.record(format!("r{i:032}").as_bytes()).expect("record");
    }
    PgReadSet::with_row_keys(Vec::new(), rows.finish().expect("finish read set"))
}

/// One COMMIT over the real consumer, with every key-sized allocation charged to its site.
#[test]
#[serial_test::serial]
fn commit_path_key_buffers_are_attributed_to_their_allocation_sites() {
    assert!(
        mem_probe::installed(),
        "this binary requires the `alloc-probe` tag seam; run it with `--features alloc-probe`. \
         Without the seam every site reads as untagged, which is a fabricated zero rather than \
         a measurement."
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime builds");
    let applier = Arc::new(RecordingApplier::default());
    let committer = committer(applier);
    // Build the fixture OUTSIDE the measured window: constructing the input is itself
    // allocating work, and charging it would attribute the test, not the subject.
    let writes = staged_writes(STAGED_WRITES);
    let reads = read_set_with(LARGE_ROWS);
    assert!(reads.spilled_to_disk(), "the read set must have spilled");

    let outcome = measure(|| {
        rt.block_on(async {
            committer
                .commit_postgres("ks", writes, reads, Vec::new(), snapshot())
                .await
        })
    });
    assert!(outcome.is_ok(), "the COMMIT must drive to a decision");

    let allocs = mem_probe::site_key_allocs();
    let bytes = mem_probe::site_key_bytes();
    eprintln!(
        "key-buffer attribution for rows={LARGE_ROWS}, committed peak_keys={}:",
        PEAK_KEYS.load(Ordering::Relaxed)
    );
    eprintln!("  {:<26} {:>6} {:>10}", "site", "allocs", "bytes");
    for (i, name) in mem_probe::SITE_NAMES.iter().enumerate() {
        eprintln!("  {:<26} {:>6} {:>10}", name, allocs[i], bytes[i]);
    }

    let at = |site: usize| allocs[site];
    // Every site the commit path is KNOWN to allocate conflict keys at must actually be
    // charged, or the instrument is blind and the table says nothing.
    //
    // `order.preaccept_v2_keys` is deliberately NOT in this list: it now BORROWS the driver's
    // write-set keys and allocates ONE slice buffer, not one key buffer per conflict key. Its
    // own regression guard is below, the mirror of the self-vote guard.
    for site in [
        mem_probe::SITE_DRIVE_PER_KEY_INDEX,
        mem_probe::SITE_DRIVE_WRITE_SET,
        mem_probe::SITE_SM_CONFLICT_INDEX,
        mem_probe::SITE_APPENDED_ROW_READ_KEY,
    ] {
        assert!(
            at(site) >= LARGE_ROWS as i64,
            "site {} was charged only {} key-sized allocations for {LARGE_ROWS} rows — the \
             instrument did not observe it, so its row in the table is not evidence",
            mem_probe::SITE_NAMES[site],
            at(site)
        );
    }

    // THE FIX THIS GUARDS, already landed: the local self-vote used to clone every
    // conflict key into a `Vec<Vec<u8>>` before handing it to the state machine. It now
    // borrows them from an `Arc<[WriteSetEntry]>` the closure owns, so this site costs ONE
    // borrowed-slice buffer instead of one owned key buffer per key. This assertion is a
    // REGRESSION GUARD for that change, not the red that produced it: reverting it puts
    // this row back above `LARGE_ROWS` and the ratchet in
    // `pg_commit_path_consumer_residency.rs` fails.
    assert!(
        at(mem_probe::SITE_ORDER_SELF_VOTE_KEYS) <= 4,
        "the self-vote key list must BORROW the driver's write-set keys, not rebuild them: \
         {} key-sized allocations were charged to site {}",
        at(mem_probe::SITE_ORDER_SELF_VOTE_KEYS),
        mem_probe::SITE_NAMES[mem_probe::SITE_ORDER_SELF_VOTE_KEYS]
    );

    // THE FIX THIS GUARDS, landed by `refactor/borrow-the-commit-keys`: the PreAcceptV2 wire
    // key list used to clone every conflict key into a `Vec<Vec<u8>>` before serializing. It
    // now BORROWS the driver's keys (a `Vec<&[u8]>`) and serializes them through
    // `PreAcceptV2PayloadBorrowed`, whose frame is byte-identical. This site costs ONE
    // borrowed-slice buffer instead of one owned key buffer per key; the byte identity is
    // pinned in `accord::wire` and the ordering by the driver's own write-set order.
    assert!(
        at(mem_probe::SITE_ORDER_PREACCEPT_KEYS) <= 4,
        "the PreAcceptV2 key list must BORROW the driver's write-set keys, not clone them: \
         {} key-sized allocations were charged to site {}",
        at(mem_probe::SITE_ORDER_PREACCEPT_KEYS),
        mem_probe::SITE_NAMES[mem_probe::SITE_ORDER_PREACCEPT_KEYS]
    );

    // The honesty check. A key-sized allocation the instrument cannot name lands in the
    // untagged bucket; a large remainder means the table is missing a site and must not be
    // reported as the whole story.
    assert!(
        at(mem_probe::SITE_UNTAGGED) <= 96,
        "{} key-sized allocations were charged to NO site (budget 96): the attribution table \
         is incomplete and cannot be reported as exhaustive. The untagged bucket holds a \
         bounded set of LARGE objects — the per-key hash table, the encode buffers — not one \
         buffer per conflict key: a per-key untagged site would add at least {LARGE_ROWS} here.",
        at(mem_probe::SITE_UNTAGGED)
    );

    // The named sites must account for the conflict keys several times over — every key is
    // allocated by the driver index, the driver write-set and the index registration. A total
    // at or below one copy per key would mean a site went uncharged.
    let named: i64 = allocs[1..].iter().sum();
    assert!(
        named >= 3 * conflict_keys(),
        "the named sites were charged {named} key-sized allocations for {} conflict keys, \
         fewer than the three the path structurally allocates per key — a site is missing \
         from the table",
        conflict_keys()
    );
}
