//! Shared harness for the TWO Accord `ConflictIndex` residency guards:
//! `conflict_index_residency.rs` (a per-PR structural guard) and
//! `conflict_index_residency_absolute.rs` (a nightly absolute guard).
//!
//! ## The two measured defects (N = 1,000,112 keys, one transactional COPY)
//!
//! `single_key: HashMap<Vec<u8>, Vec<InFlightWrite>>` allocated a `Vec` of
//! **capacity 4** for every key — Rust's first-growth rule for a >1-byte element —
//! while a key almost always holds exactly ONE in-flight write. At ~112 bytes per
//! `InFlightWrite` that is ~448 bytes reserved to hold ~112: ~336 MB of pure
//! over-allocation. And `single_key_hwm: HashMap<Vec<u8>, Timestamp>` was keyed by
//! the SAME bytes, so every key's bytes lived TWICE (~105 MB of duplicated keys
//! plus a second bucket array).
//!
//! Both are invisible to a value assertion: the index returns the correct
//! conflicts and the correct HWM whether it reserves for one write or four, and
//! whether it stores the key once or twice. Only a counter sees the cost, so these
//! guards are measured with a byte-counting global allocator.
//!
//! ## How "before" is measured
//!
//! The old representation is GONE, so "before" is a stand-in built in the SAME
//! run: a `HashMap<Vec<u8>, Vec<InFlightWrite>>` plus a parallel
//! `HashMap<Vec<u8>, Timestamp>`, populated exactly as `register` + `set_commit_ts`
//! populated the old `single_key` + `single_key_hwm`, at the same constructor
//! reserve. Both the old stand-in and the new index are measured the same way, so
//! the per-key delta is a directly measured NET (it already accounts for the new
//! `KeyEntry` being wider than a bare `Vec`, which grows the bucket array a
//! little). The isolated per-factor numbers (the writes buffer alone, the HWM map
//! alone) are reported alongside so the decomposition is visible.
//!
//! ## Why bytes-live (delta), not allocation calls
//!
//! The defect is RESIDENCY — a `Vec` that reserves four slots and a map that
//! stores a key twice. A net live-heap delta (`alloc - dealloc`) across the
//! measured window reports exactly the bytes still held afterwards, which is the
//! number that has to move. It is deterministic, unlike a stopwatch.
//!
//! ## Why the two guards are SEPARATE binaries
//!
//! The counter is process-wide (`ARMED`/`LIVE`). Two counter tests in ONE binary
//! race even under a whole-body mutex: `ARMED` is global, so an allocation the
//! other test makes OUTSIDE its own window is still counted INSIDE this one's
//! window (observed: a control that must read exactly 448 B read 328 B when the
//! two shared a process). Separate integration-test binaries are separate
//! processes, so each gets its own counter; each binary then holds exactly ONE
//! test, so nothing else runs in its process.
//!
//! ## Harness rule that keeps the measurement sound
//!
//! The `ConflictIndex` / stand-in under test is built BEFORE arming; only
//! `register` / `set_commit_ts` (and the stand-in equivalents) run inside the
//! windows, so the numbers are the structure's cost, not the test's.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::mem::size_of;

use ferrosa_common::accord::{Timestamp, TxnId};
use ferrosa_storage::accord::{ConflictIndex, InFlightWrite, TxnStatus};

// ----------------------------------------------------------- counting allocator ---

/// Byte-counting global allocator; each test binary registers it as
/// `#[global_allocator]`.
pub struct CountingAlloc;
static ARMED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static LIVE: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(std::sync::atomic::Ordering::Relaxed) {
            LIVE.fetch_add(layout.size() as i64, std::sync::atomic::Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ARMED.load(std::sync::atomic::Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size() as i64, std::sync::atomic::Ordering::Relaxed);
        }
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(std::sync::atomic::Ordering::Relaxed) {
            LIVE.fetch_add(
                new_size as i64 - layout.size() as i64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        System.realloc(ptr, layout, new_size)
    }
}

/// Run `f`, returning its result and the NET live-heap bytes still held after it
/// (allocations minus frees inside the window). Only `alloc`/`dealloc`/`realloc`
/// on the `System` allocator touch atomics, so there is no reentrancy.
fn live_delta<T>(f: impl FnOnce() -> T) -> (T, i64) {
    use std::sync::atomic::Ordering::Relaxed as R;
    ARMED.store(true, R);
    let before = LIVE.load(R);
    let out = f();
    let after = LIVE.load(R);
    ARMED.store(false, R);
    (out, after - before)
}

// ------------------------------------------------------------------- test values ---

fn ts(time: u64) -> Timestamp {
    Timestamp {
        epoch: 0,
        time,
        seq: 0,
        node: 1,
    }
}

fn txn(time: u64) -> TxnId {
    TxnId(ts(time))
}

fn write() -> InFlightWrite {
    InFlightWrite {
        txn_id: txn(1),
        t0: ts(1),
        accord_ts: None,
        status: TxnStatus::PreAccepted,
    }
}

/// The exact N measured; the projection uses the live 1,000,112.
pub const N: usize = 200_000;
/// The live scale the absolute guard projects to (one transactional COPY).
pub const TARGET_KEYS: i64 = 1_000_112;
/// The constructor reserve both the new index and the old stand-in start from, so
/// their bucket arrays grow the same way inside the windows.
const RESERVE: usize = 1024;

// -------------------------------------------------------------------- measurement ---

/// Every number either guard needs, measured ONCE per [`measure`] call.
pub struct Residency {
    pub entry_size: i64,
    /// The isolated writes buffer per key: old capacity-4 `Vec` vs new
    /// capacity-1 `Vec` (a pre-sized map so only the per-key buffer is counted).
    pub value_buf_before: i64,
    pub value_buf_after: i64,
    /// The HWM map per key: old parallel map vs new in-place raise.
    pub hwm_before: i64,
    pub hwm_after: i64,
    /// The whole index per key (single_key + HWM), old stand-in vs new.
    pub whole_before: i64,
    pub whole_after: i64,
    /// `whole_before - whole_after`, and what it projects to at 1,000,112 keys.
    pub saved_per_key: i64,
    pub projected_before: i64,
    pub projected_after: i64,
    pub projected_saving: i64,
}

impl Residency {
    /// A human-readable report of every measured number (also keeps every field
    /// read, so neither test binary sees a dead field).
    pub fn report(&self, label: &str) -> String {
        format!(
            "ConflictIndex residency (measured at N={N}) [{label}]:\n  \
             value buffer / key : {after_buf} B  (was {before_buf} B = 4*{es} — a \
             capacity-1 vs capacity-4 Vec)\n  \
             HWM map / key      : {after_hwm} B  (was {before_hwm} B in a parallel \
             key->Timestamp map)\n  \
             whole index / key  : {after_whole} B  (was {before_whole} B)\n  \
             projected @N={TARGET_KEYS}: {after_mib:.0} MiB resident, was \
             {before_mib:.0} MiB → cut {saved} B/key ≈ {saved_mib:.0} MiB",
            es = self.entry_size,
            before_buf = self.value_buf_before,
            after_buf = self.value_buf_after,
            before_hwm = self.hwm_before,
            after_hwm = self.hwm_after,
            before_whole = self.whole_before,
            after_whole = self.whole_after,
            saved = self.saved_per_key,
            before_mib = self.projected_before as f64 / (1024.0 * 1024.0),
            after_mib = self.projected_after as f64 / (1024.0 * 1024.0),
            saved_mib = self.projected_saving as f64 / (1024.0 * 1024.0),
        )
    }
}

/// Measure the residency factors and assert the BEHAVIOUR is unchanged.
///
/// The behaviour assertions pass before AND after the representation change; they
/// are here so a change that quietly broke conflict detection or HWM survival
/// cannot pass by measuring a smaller (wrong) structure.
pub fn measure() -> Residency {
    let entry_size = size_of::<InFlightWrite>() as i64;
    let key_bytes = 8i64; // `u64::to_be_bytes()` keys, exact
    let per_key = |bytes: i64| bytes / N as i64;

    // ------------------------------------------------------------------
    // Negative control A — the counter SEES a capacity-4 first push.
    //
    // `Vec::new().push(x)` grows to capacity 4 for a >1-byte element. If the
    // counter cannot see it, the factor-1 guard is vacuous.
    // ------------------------------------------------------------------
    let (_v, ctrl_vec) = live_delta(|| {
        // Control A must exercise the FIRST-GROWTH path: `Vec::new()` + `push`
        // reserves capacity FOUR for an element larger than one byte, which is
        // exactly the old per-key reservation this guard rejects. `vec![x]` would
        // reserve ONE and prove nothing, so the `vec![]` lint does not apply here.
        #[allow(clippy::vec_init_then_push)]
        {
            let mut v: Vec<InFlightWrite> = Vec::new();
            v.push(write());
            v
        }
    });
    assert!(
        ctrl_vec >= 4 * entry_size,
        "a one-element Vec of {entry_size}-byte entries must allocate capacity 4 \
         ({} bytes); measured {ctrl_vec} — the allocator counter is not armed and \
         the guards are vacuous",
        4 * entry_size
    );

    // ------------------------------------------------------------------
    // Isolated value buffer. Both maps are pre-sized so only the per-key heap —
    // the writes buffer plus the key's own bytes — lands in the window.
    // ------------------------------------------------------------------
    let mut iso_after = ConflictIndex::new(N + RESERVE);
    let (_u, iso_after_bytes) = live_delta(|| {
        for i in 0..N as u64 {
            iso_after.register(&i.to_be_bytes(), write());
        }
    });
    let value_buf_after = per_key(iso_after_bytes) - key_bytes;

    let mut iso_before: HashMap<Vec<u8>, Vec<InFlightWrite>> = HashMap::with_capacity(N + RESERVE);
    let (_u, iso_before_bytes) = live_delta(|| {
        for i in 0..N as u64 {
            iso_before
                .entry(i.to_be_bytes().to_vec())
                .or_default()
                .push(write());
        }
    });
    let value_buf_before = per_key(iso_before_bytes) - key_bytes;

    // ------------------------------------------------------------------
    // Whole index, AFTER (the current `ConflictIndex`), reserve = RESERVE so its
    // bucket array grows inside the windows.
    // ------------------------------------------------------------------
    let mut after = ConflictIndex::new(RESERVE);
    let (_u, after_reg) = live_delta(|| {
        for i in 0..N as u64 {
            after.register(&i.to_be_bytes(), write());
        }
    });
    let (_u, after_hwm_bytes) = live_delta(|| {
        after.set_commit_ts(&txn(1), ts(1));
    });
    let hwm_after = per_key(after_hwm_bytes);
    let whole_after = per_key(after_reg + after_hwm_bytes);

    // ------------------------------------------------------------------
    // Behaviour is UNCHANGED — these assertions pass before AND after.
    // ------------------------------------------------------------------
    assert_eq!(
        after.len(),
        N,
        "every key of the write-set must be registered"
    );
    for i in [0usize, N / 2, N - 1] {
        assert_eq!(
            after.max_conflicting_timestamp(&(i as u64).to_be_bytes()),
            Some(ts(1)),
            "key {i} must still be found"
        );
    }
    after.mark_applied(&txn(1));
    after.gc_applied();
    assert_eq!(after.len(), 0, "applied writes are GC'd");
    assert_eq!(
        after.max_conflicting_timestamp(&0u64.to_be_bytes()),
        Some(ts(1)),
        "the committed execution timestamp must survive gc_applied as a per-key HWM"
    );
    drop(after);

    // ------------------------------------------------------------------
    // Whole index, BEFORE (stand-in for the old representation), same reserve and
    // the same population as the old `register` + `set_commit_ts`.
    // ------------------------------------------------------------------
    let mut before_sk: HashMap<Vec<u8>, Vec<InFlightWrite>> = HashMap::with_capacity(RESERVE);
    let mut before_hwm: HashMap<Vec<u8>, Timestamp> = HashMap::with_capacity(RESERVE);
    let (_u, before_reg) = live_delta(|| {
        for i in 0..N as u64 {
            before_sk
                .entry(i.to_be_bytes().to_vec())
                .or_default()
                .push(write());
        }
    });
    let (_u, before_hwm_bytes) = live_delta(|| {
        for i in 0..N as u64 {
            before_hwm.entry(i.to_be_bytes().to_vec()).or_insert(ts(1));
        }
    });
    let hwm_before = per_key(before_hwm_bytes);
    let whole_before = per_key(before_reg + before_hwm_bytes);

    // Negative control B — the counter SEES the parallel key->Timestamp map (the
    // removed `single_key_hwm`). It must cost more than one key copy per key.
    assert!(
        hwm_before > key_bytes,
        "the parallel key->Timestamp stand-in must cost more than one key copy per \
         key; measured {hwm_before} B/key vs key_bytes={key_bytes} — the ratio guard \
         would be vacuous"
    );

    let saved_per_key = whole_before - whole_after;

    Residency {
        entry_size,
        value_buf_before,
        value_buf_after,
        hwm_before,
        hwm_after,
        whole_before,
        whole_after,
        saved_per_key,
        projected_before: whole_before * TARGET_KEYS,
        projected_after: whole_after * TARGET_KEYS,
        projected_saving: saved_per_key * TARGET_KEYS,
    }
}
