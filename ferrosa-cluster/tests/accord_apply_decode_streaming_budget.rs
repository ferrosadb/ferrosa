//! Load-independent budget guard for the Accord apply path's decode residency.
//!
//! `tests/accord_apply_decode_residency.rs` pins the same claim with ABSOLUTE
//! resident-byte bounds (a counting allocator's peak live bytes against a multiple
//! of the input size). Those bounds measure the MACHINE as much as the code — a
//! contended runner moves the allocator's watermark and the engine's own buffers —
//! and the two input-residency tests were observed failing the per-PR
//! `Test + Coverage` job on a tree whose code had not changed. They now run nightly
//! in `nightly-fuzz.yml`'s `perf-regression` job, and `ci.yml` skips that binary by
//! name (`--skip accord_apply_decode_residency`).
//!
//! The claim those bounds are a proxy for IS load-independent, so this file asserts
//! it directly:
//!
//! **`EngineStorageApplier::apply_writeset_borrowed` does not hold the whole decoded
//! write-set resident.** It resolves the survivors by decoding each partition once,
//! records only its `(txn,key,t)` triple and drops its rows, then streams the
//! survivors into ONE atomic `write_atomic_batch` that decodes a single partition per
//! visit. A decode that accumulated the whole decoded set first (the `Vec<BatchOp>`
//! this replaced) would hold a SECOND entry-sized buffer per input entry while it
//! committed.
//!
//! # Why counting allocations is load-independent
//!
//! This guard counts how many buffers of at least [`ENTRY_SIZED`] bytes are LIVE AT
//! ONCE during the apply, on the measuring thread. Allocation COUNTS are a property
//! of the code path, not of the machine: CPU contention, a busy allocator, or another
//! job on the runner changes wall-clock time and the process' total resident bytes,
//! but not how many buffers this call holds simultaneously. It is the same reasoning
//! as `tests/reorder_buffer_drain_budget.rs`, which replaced the
//! `accord::perf_regression` wall-clock drain bound with an allocation budget.
//!
//! # Measured baseline (debug build, N = 4096, 3000-byte rows)
//!
//! streaming apply: 4101 live entry-sized buffers — the storage engine's own
//! durability copy of the rows it is writing: ONE per input entry, plus 4. Whole-set
//! decode held across the call: 8197 — a SECOND entry-sized buffer per input entry,
//! plus 5. The budget is N + [`ENTRY_BUFFER_SLACK`] (one entry-sized buffer per input
//! entry, plus ~6% slack). Do NOT raise it: one per entry is the shape, two is the
//! regression.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use ferrosa_cluster::accord::apply::{EngineStorageApplier, MutationView, StorageApplier};
use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, Token};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{Mutation, StorageEngine, StorageEngineConfig};
use serial_test::serial;

/// The fewest bytes a buffer must be to carry ONE write-set entry's decoded row
/// body. Nothing smaller than this can be a per-entry decoded payload or op, so a
/// count of live buffers at least this large IS a count of resident write-set
/// entries. The test rows are [`ROW_BYTES`] (3000), well above it.
const ENTRY_SIZED: usize = 1024;

/// Entries per measured write-set. Small enough that the guard costs well under a
/// second per apply; large enough that "one buffer per entry" is unmistakable.
const N: usize = 4096;

/// The cell size each entry's decoded row carries — what makes one entry-sized
/// buffer distinguishable from engine bookkeeping.
const ROW_BYTES: usize = 3000;

/// The commit-log segment this test runs against. A 4 KiB default is pathological
/// for a multi-megabyte write-set (one entry per segment file); sizing the segment
/// above the largest mutation is what a real deployment does.
const SEGMENT_BYTES: usize = 32 * 1024 * 1024;

/// Slack above one entry-sized buffer per input entry. The measured baseline is
/// one-per-entry plus 4; the whole-set decode adds a full second copy (N). ~6%.
const ENTRY_BUFFER_SLACK: i64 = 256;

// ── live entry-sized buffer probe (scoped to this integration-test binary) ──
//
// Thread-local on purpose: the apply runs on the calling thread, so arming the
// probe per-thread counts the DECODE's buffers and never another test's, another
// engine's, or a background flush worker's. A process-global flag was the bug in
// `reorder_buffer_drain_budget.rs` (it counted other threads' allocations).
struct EntryBufferProbe;

thread_local! {
    // `const` init: reading it never allocates, so it is safe inside `alloc`.
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static LIVE_ENTRY: Cell<i64> = const { Cell::new(0) };
    static PEAK_ENTRY: Cell<i64> = const { Cell::new(0) };
}

/// Whether the current thread is measuring. `try_with` because the allocator can
/// run during thread-local teardown, when the flag is already gone (not measuring).
fn armed() -> bool {
    ARMED.try_with(Cell::get).unwrap_or(false)
}

unsafe impl GlobalAlloc for EntryBufferProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if armed() && layout.size() >= ENTRY_SIZED {
            let live = LIVE_ENTRY.with(|c| {
                let v = c.get() + 1;
                c.set(v);
                v
            });
            PEAK_ENTRY.with(|c| c.set(c.get().max(live)));
        }
        // SAFETY: delegated to the system allocator with the same layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if armed() && layout.size() >= ENTRY_SIZED {
            // Saturating: a buffer allocated BEFORE the window can be freed inside
            // it, which would otherwise drive the live count negative and suppress
            // every later allocation.
            LIVE_ENTRY.with(|c| c.set((c.get() - 1).max(0)));
        }
        // SAFETY: delegated to the system allocator with the same pointer/layout.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: EntryBufferProbe = EntryBufferProbe;

const KS: &str = "apply_stream_budget_ks";
const TABLE: &str = "apply_stream_budget_table";

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
        "ferrosa-apply-stream-budget-{}-{label}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut config = StorageEngineConfig::test_config(&dir);
    config.commit_log.segment_size = SEGMENT_BYTES;
    // Keep the engine's own memtable residency bounded, so the count is dominated
    // by per-entry buffers rather than by the flush watermark.
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

/// One encoded commit-log mutation frame per entry — the wire shape
/// `apply_writeset_borrowed` decodes. Built OUTSIDE the measurement window, so the
/// caller's own copy of the input is never a term of the count.
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

/// Decode every payload of the write-set into a resident `Vec<Mutation>` — the
/// whole decoded write-set a materializing decode would hold. Used only by the
/// deliberate bug the negative control injects.
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

/// Peak count of entry-sized buffers live at once during ONE
/// `apply_writeset_borrowed` of the [`N`]-entry write-set.
///
/// `materialise_decoded_set` is the deliberate bug the negative control injects:
/// decode the whole write-set into a resident `Vec<Mutation>` and hold it across the
/// apply — exactly the decoded-OUTPUT residency the fix removes.
fn peak_live_entry_sized_buffers(materialise_decoded_set: bool) -> i64 {
    let (engine, dir) = make_engine(if materialise_decoded_set {
        "control"
    } else {
        "positive"
    });
    let applier = EngineStorageApplier::new(engine);
    let txn = TxnId::new(7, accord_ts(1_000));
    let t = accord_ts(1_000);
    let payloads = build_payloads(1_000);
    let views: Vec<MutationView<'_>> = payloads
        .iter()
        .map(|bytes| MutationView {
            data: bytes.as_slice(),
            t,
            deps: &[],
        })
        .collect();
    LIVE_ENTRY.with(|c| c.set(0));
    PEAK_ENTRY.with(|c| c.set(0));
    ARMED.with(|a| a.set(true));
    if materialise_decoded_set {
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
    ARMED.with(|a| a.set(false));
    let peak = PEAK_ENTRY.with(Cell::get);
    let _ = std::fs::remove_dir_all(&dir);
    peak
}

/// Measure BOTH paths ONCE and cache `(entry_count, streaming_peak, materialising_peak)`.
///
/// `streaming_peak` is the production decode; `materialising_peak` is the pinning
/// control. The pair is the whole evidence: a decode that holds the decoded write-set
/// resident sits at one extra entry-sized buffer per entry above the streaming path.
fn measured() -> (i64, i64, i64) {
    static MEASURED: std::sync::OnceLock<(i64, i64, i64)> = std::sync::OnceLock::new();
    *MEASURED.get_or_init(|| {
        let streaming = peak_live_entry_sized_buffers(false);
        let materialising = peak_live_entry_sized_buffers(true);
        (N as i64, streaming, materialising)
    })
}

/// The most entry-sized buffers the apply may hold live at once: ONE per input
/// entry, plus [`ENTRY_BUFFER_SLACK`].
fn budget(n: i64) -> i64 {
    n + ENTRY_BUFFER_SLACK
}

/// The streaming decode MUST NOT hold a second entry-sized copy of the write-set.
///
/// The storage engine's own durability copy makes the floor ONE entry-sized buffer
/// per input entry; a decode that accumulates the whole decoded set first holds a
/// SECOND, taking the live count to ~2N. The measured baseline is N + 4 (streaming)
/// against 2N + 5 (materialising). Counting live buffers is load-independent — see
/// the module docs.
#[test]
#[serial]
fn apply_writeset_borrowed_holds_at_most_one_entry_sized_buffer_per_input_entry() {
    let (n, streaming, materialising) = measured();
    let budget = budget(n);
    eprintln!(
        "apply decode streaming budget: N={n} row_bytes={ROW_BYTES} \
         entry_sized_floor={ENTRY_SIZED} B, streaming_live_max={streaming}, \
         materialising_live_max={materialising}, budget={budget}"
    );
    assert!(
        streaming <= budget,
        "REGRESSION: apply_writeset_borrowed held {streaming} entry-sized buffers live at \
         once for a {n}-entry write-set (budget {budget} = one per input entry plus slack). \
         The apply must hold only the storage engine's own copy of the rows it is writing; \
         a decode that accumulates the whole DECODED write-set before committing holds a \
         second one per entry and lands near {} — measure it with \
         peak_live_entry_sized_buffers(true).",
        materialising
    );
    assert!(
        materialising * 2 >= streaming * 3,
        "the entry-sized buffer guard is blind: materialising the whole decoded write-set \
         (the deliberate bug) raised the live buffer count only to {materialising} against \
         the streaming path's {streaming}. A whole-set decode must sit at least half again \
         above the streaming path, so the guard has stopped measuring the decoded set."
    );
}

/// NEGATIVE CONTROL: the budget above MUST be able to fail. Decoding the whole
/// write-set into a resident `Vec<Mutation>` and holding it across the apply
/// re-introduces exactly the residency the fix removes, so it MUST breach the same
/// budget the streaming path satisfies. If this stops tripping, the guard has gone
/// blind to the decoded write-set and proves nothing.
#[test]
#[serial]
fn a_materialised_decoded_write_set_trips_the_entry_sized_buffer_budget() {
    let (n, streaming, materialising) = measured();
    let budget = budget(n);
    eprintln!(
        "negative control (entry-sized buffers): N={n}, streaming_live_max={streaming}, \
         materialising_live_max={materialising}, budget={budget}"
    );
    assert!(
        materialising > budget,
        "the entry-sized buffer guard is blind: materialising the whole decoded write-set \
         did NOT push the live count above the budget (N={n}, streaming_live_max={streaming}, \
         materialising_live_max={materialising}, budget={budget}). The guard no longer \
         measures the decoded output."
    );
}
