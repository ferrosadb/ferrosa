//! T-034 (L6 stress): 64 concurrent `AlignedPump`s, random segment/queue-depth
//! sizes and random fault injection, run inside a 60 s wall budget. Three
//! claims, matching the write-pump test specification's L6 "Stress" row:
//!
//! 1. **Every pump returns.** Each worker reports an outcome (`Ok` or a named
//!    `Err`) over a channel; the main thread waits with an overall deadline
//!    (`Receiver::recv_timeout` against a shrinking remaining budget, never an
//!    unbounded `join()`), so a wedged pump fails the test loudly instead of
//!    hanging it.
//! 2. **No thread outlives its pump.** `pump::live_flusher_threads()` (T-034
//!    test-only instrumentation, incremented/decremented by a `Drop` guard
//!    around each flusher thread's body — see `FlusherThreadGuard` in
//!    `pump.rs`) is `0` before the run starts and `0` again once every worker
//!    has been joined and its `AlignedPump` dropped.
//! 3. **Peak allocation is bounded.** A process-wide peak-tracking allocator
//!    measures the high-water mark of live bytes during the run. It must not
//!    exceed a generous multiple of `sum over pumps of (depth + 1) * segment`
//!    — the bounded-ring rule (architecture.md) extended across many
//!    concurrently open pumps, not literally `64 * (depth+1) * segment` for a
//!    single depth/segment pair, since each of the 64 pumps independently
//!    randomizes its own `depth` and `segment`.
//!
//! Each worker uses [`StressSink`] (below), a fault-capable `SegmentSink`
//! that — unlike `test_support::FaultySink`/`RecordingSink` — never retains
//! the bytes it "writes": it folds them into a running `crc32fast::Hasher`
//! and drops them. `RecordingSink`'s `Vec<u8>` deliberately mirrors the whole
//! file so the L4 fault-matrix tests can compare exact bytes; here, with 64
//! pumps each streaming a dozen-ish segments, that would make claim 3 (a
//! tight bound on the PUMP's own memory) impossible to state meaningfully,
//! since the test double's own bookkeeping would dwarf it. This test does not
//! re-verify per-fault correctness (the dedicated `pump_sync_faulty_sink_*`/
//! `pump_async_faulty_sink_*` tests in `pump.rs` already do that byte-exactly,
//! single-pump-at-a-time, with `RecordingSink`); it exists to prove liveness
//! and memory bounds hold under concurrency and load, which those tests —
//! one pump at a time — cannot.

#![cfg(feature = "test-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::bounded;
use ferrosa_common::{Error, Result};
use ferrosa_sstable::direct::DirectMode;
use ferrosa_sstable::pump::test_support::Fault;
use ferrosa_sstable::pump::{live_flusher_threads, AlignedPump, NeverAbort, SegmentSink};

/// Tracks live (allocated - deallocated) bytes and the high-water mark seen
/// since the last [`reset_peak_to_current`] call. A process-wide allocator,
/// so — like `pump_async_alloc.rs`'s counting allocator — it sees every
/// thread in this binary; this test's single `#[test]` fn is the only test in
/// this file, so there is no cross-test contamination to guard against.
struct PeakTrackingAllocator;

static CURRENT_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

fn record_grow(n: usize) {
    let cur = CURRENT_BYTES.fetch_add(n, Ordering::Relaxed) + n;
    PEAK_BYTES.fetch_max(cur, Ordering::Relaxed);
}

fn record_shrink(n: usize) {
    CURRENT_BYTES.fetch_sub(n, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for PeakTrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_grow(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record_shrink(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size > layout.size() {
                record_grow(new_size - layout.size());
            } else {
                record_shrink(layout.size() - new_size);
            }
        }
        new_ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_grow(layout.size());
        }
        ptr
    }
}

#[global_allocator]
static GLOBAL: PeakTrackingAllocator = PeakTrackingAllocator;

fn reset_peak_to_current() -> usize {
    let cur = CURRENT_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(cur, Ordering::Relaxed);
    cur
}

fn peak_bytes() -> usize {
    PEAK_BYTES.load(Ordering::Relaxed)
}

/// A minimal fault-capable `SegmentSink` for stress testing at scale: like
/// `test_support::FaultySink`, it plays back one [`Fault`] at a chosen call
/// index (a single counter shared across every method, in call order,
/// exactly matching `FaultySink`'s contract), but it never retains the bytes
/// it "writes" — see the module doc for why.
struct StressSink {
    mode: DirectMode,
    hasher: crc32fast::Hasher,
    fault: Option<(usize, Fault)>,
    call_index: usize,
    /// Reused fault-corruption workspace, cleared and re-filled per call —
    /// bounded by one call's own size (at most one segment), never by total
    /// bytes written.
    scratch: Vec<u8>,
}

impl StressSink {
    fn new(mode: DirectMode, fault: Option<(usize, Fault)>) -> Self {
        Self {
            mode,
            hasher: crc32fast::Hasher::new(),
            fault,
            call_index: 0,
            scratch: Vec::new(),
        }
    }

    fn injected_error(kind: &str) -> Error {
        Error::Io(std::io::Error::other(format!(
            "pump_stress: injected {kind}"
        )))
    }

    /// `true`, and advances `call_index`, exactly once for the call at which
    /// this sink's scripted fault (if any) fires.
    fn fault_fires_now(&mut self) -> Option<Fault> {
        let idx = self.call_index;
        self.call_index += 1;
        match &self.fault {
            Some((at, fault)) if *at == idx => Some(fault.clone()),
            _ => None,
        }
    }

    fn write_call(&mut self, bufs: &[&[u8]]) -> Result<()> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        match self.fault_fires_now() {
            Some(Fault::Eio) => Err(Self::injected_error("EIO")),
            Some(Fault::Enospc) => Err(Self::injected_error("ENOSPC")),
            Some(Fault::Panic) => panic!("pump_stress: injected flusher panic"),
            // Corruption faults: all six feed SOMETHING other than (or a
            // mutation of) the real bytes into the hasher — this test only
            // needs "not the real bytes", not the exact corruption shape
            // `pump_sync_faulty_sink_silent_corruption_is_only_caught_by_digest_comparison`
            // and its async counterpart already prove byte-exactly.
            Some(Fault::ShortWrite(n)) => {
                let n = n.min(total);
                self.scratch.clear();
                let mut remaining = n;
                for buf in bufs {
                    if remaining == 0 {
                        break;
                    }
                    let take = remaining.min(buf.len());
                    self.scratch.extend_from_slice(&buf[..take]);
                    remaining -= take;
                }
                self.hasher.update(&self.scratch);
                Ok(())
            }
            Some(Fault::DropSilently) => {
                self.scratch.clear();
                self.scratch.resize(total, 0x5A);
                self.hasher.update(&self.scratch);
                Ok(())
            }
            Some(Fault::StaleBytes) => {
                self.scratch.clear();
                self.scratch.resize(total, 0xEE);
                self.hasher.update(&self.scratch);
                Ok(())
            }
            Some(Fault::BitFlip(byte, bit)) => {
                self.scratch.clear();
                for buf in bufs {
                    self.scratch.extend_from_slice(buf);
                }
                if let Some(b) = self.scratch.get_mut(byte) {
                    *b ^= 1 << (bit % 8);
                }
                self.hasher.update(&self.scratch);
                Ok(())
            }
            Some(Fault::WrongOffset(_)) | Some(Fault::Duplicate) => {
                // Offset-shape faults are meaningless without a real backing
                // file; stand in with zeroed bytes, which still guarantees a
                // digest mismatch (not this test's concern — see module doc).
                self.scratch.clear();
                self.scratch.resize(total, 0x00);
                self.hasher.update(&self.scratch);
                Ok(())
            }
            Some(Fault::FsyncFail) | Some(Fault::SetLenFail) | None => {
                for buf in bufs {
                    self.hasher.update(buf);
                }
                Ok(())
            }
        }
    }
}

impl SegmentSink for StressSink {
    fn pwrite(&mut self, buf: &[u8], _offset: u64) -> Result<()> {
        self.write_call(&[buf])
    }

    fn pwritev(&mut self, bufs: &[&[u8]], _offset: u64) -> Result<()> {
        self.write_call(bufs)
    }

    fn sync_data(&mut self) -> Result<()> {
        match self.fault_fires_now() {
            Some(Fault::FsyncFail) => Err(Self::injected_error("fsync failure")),
            Some(Fault::Panic) => panic!("pump_stress: injected panic at sync_data"),
            _ => Ok(()),
        }
    }

    fn set_len(&mut self, _len: u64) -> Result<()> {
        match self.fault_fires_now() {
            Some(Fault::SetLenFail) => Err(Self::injected_error("set_len failure")),
            Some(Fault::Panic) => panic!("pump_stress: injected panic at set_len"),
            _ => Ok(()),
        }
    }

    fn fadvise_dontneed(&mut self) -> Result<()> {
        let _ = self.fault_fires_now();
        Ok(())
    }

    fn mode(&self) -> DirectMode {
        self.mode
    }
}

/// One worker's fully precomputed configuration — generated up front, on the
/// main test thread, so the peak-allocation bound below can be computed from
/// the SAME numbers each worker will actually use, with no risk of two
/// independent RNG streams drifting apart.
struct WorkerPlan {
    id: usize,
    block: usize,
    segment: usize,
    depth: usize,
    total_len: usize,
    fault: Option<(usize, Fault)>,
}

/// A small, deterministic (seeded) PRNG — xorshift64* — used only to pick
/// stress parameters. Not cryptographic; reproducibility across a run (for
/// debugging a failure) is the only property that matters here.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

fn random_fault(rng: &mut Rng, block: usize) -> Fault {
    match rng.next_usize(11) {
        0 => Fault::Eio,
        1 => Fault::Enospc,
        2 => Fault::ShortWrite(block / 2),
        3 => Fault::DropSilently,
        4 => Fault::WrongOffset(-(block as i64)),
        5 => Fault::Duplicate,
        6 => Fault::StaleBytes,
        7 => Fault::BitFlip(3, 2),
        8 => Fault::Panic,
        9 => Fault::FsyncFail,
        _ => Fault::SetLenFail,
    }
}

fn plan_worker(id: usize, seed: u64) -> WorkerPlan {
    let mut rng = Rng(seed | 1); // xorshift64* requires a nonzero seed
    let block = 4096usize;
    let segment = (1 + rng.next_usize(4)) * block; // 1..=4 blocks
    let depth = 1 + rng.next_usize(4); // 1..=4
    let full_segments = 1 + rng.next_usize(12); // 1..=12 full segments
    let tail = rng.next_usize(segment); // plus a partial tail
    let total_len = full_segments * segment + tail;

    let fault = if rng.next_usize(2) == 0 {
        let call_bound = full_segments + 3; // segments + finish's own calls
        let at = rng.next_usize(call_bound);
        Some((at, random_fault(&mut rng, block)))
    } else {
        None
    };

    WorkerPlan {
        id,
        block,
        segment,
        depth,
        total_len,
        fault,
    }
}

/// Run one worker to completion: stream `plan.total_len` bytes through an
/// `AlignedPump` at `plan.depth`, in fixed-size chunks generated on the fly
/// (never a single `total_len`-sized `Vec`, so a worker's OWN input buffer
/// never dominates the peak-allocation measurement — see the module doc).
/// Returns `Ok(())` or a named `Err`; a `Fault::Panic` never propagates a
/// real panic here (`AsyncBackend::shutdown`'s `JoinHandle::join()` already
/// turns it into `Err`, per ST-15), so this function itself never panics on
/// account of an injected fault.
fn run_worker(plan: WorkerPlan) -> std::result::Result<(), String> {
    let sink = StressSink::new(DirectMode::Direct, plan.fault);
    let mut pump = AlignedPump::open_with_depth(
        Box::new(sink),
        plan.block,
        plan.segment,
        PathBuf::from(format!("stress-{}.db", plan.id)),
        plan.depth,
        Arc::new(NeverAbort::new()),
    );

    const CHUNK: usize = 577; // deliberately not a multiple of block/segment
    let mut written = 0usize;
    let mut byte = (plan.id as u8).wrapping_mul(31).wrapping_add(7);
    let mut buf = [0u8; CHUNK];
    let mut write_err: Option<ferrosa_common::Error> = None;
    while written < plan.total_len {
        let take = CHUNK.min(plan.total_len - written);
        for slot in buf.iter_mut().take(take) {
            *slot = byte;
            byte = byte.wrapping_add(1);
        }
        if let Err(e) = pump.write_all(&buf[..take]) {
            write_err = Some(e);
            break;
        }
        written += take;
    }

    match write_err {
        Some(e) => {
            drop(pump); // runs `Drop`'s own (idempotent) flusher shutdown
            Err(e.to_string())
        }
        None => pump.finish().map(|_| ()).map_err(|e| e.to_string()),
    }
}

/// L6/T-034: 64 concurrent pumps, random sizes and random faults, inside a
/// 60 s wall budget.
#[test]
fn pump_stress_64_concurrent_pumps_random_sizes_and_faults() {
    const PUMPS: usize = 64;
    const BUDGET: Duration = Duration::from_secs(60);
    // Generous but not unbounded: catches a regression that scales with
    // something other than each pump's own (depth + 1) * segment (e.g. a
    // shared buffer that grows with pump count squared, or with total bytes
    // written rather than bytes in flight).
    const OVERHEAD_FACTOR: u128 = 4;
    const PER_PUMP_FIXED_OVERHEAD_BYTES: u128 = 4096; // PathBuf, channel arrays, etc.

    assert_eq!(
        live_flusher_threads(),
        0,
        "no flusher thread should be live before the stress run starts \
         (a leftover from a prior test in this process would invalidate claim 2)"
    );

    let baseline_bytes = reset_peak_to_current() as u128;

    let plans: Vec<WorkerPlan> = (0..PUMPS)
        .map(|id| {
            let seed = 0x9E37_79B9_7F4A_7C15u64
                ^ (id as u64).wrapping_mul(0xFF51_AFD7_ED55_8CCD)
                ^ 0xC2B2_AE3D_27D4_EB4Fu64;
            plan_worker(id, seed)
        })
        .collect();

    let capacity_bound: u128 = plans
        .iter()
        .map(|p| ((p.depth + 1) * p.segment) as u128 + PER_PUMP_FIXED_OVERHEAD_BYTES)
        .sum();

    let (tx, rx) = bounded::<(usize, std::result::Result<(), String>)>(PUMPS);
    let start = Instant::now();
    let handles: Vec<_> = plans
        .into_iter()
        .map(|plan| {
            let tx = tx.clone();
            let id = plan.id;
            std::thread::spawn(move || {
                let outcome = run_worker(plan);
                let _ = tx.send((id, outcome));
            })
        })
        .collect();
    drop(tx);

    let mut reported = [false; PUMPS];
    for _ in 0..PUMPS {
        let remaining = BUDGET.saturating_sub(start.elapsed());
        let (id, outcome) = rx.recv_timeout(remaining).unwrap_or_else(|_| {
            panic!(
                "at least one of {PUMPS} pumps did not report an outcome within the \
                 {BUDGET:?} stress budget ({} already reported) — a wedged pump, not just \
                 a failed one, since a failed pump still reports",
                reported.iter().filter(|r| **r).count()
            )
        });
        // A fault-induced `Err` is an expected outcome for roughly half the
        // pumps (see `plan_worker`); what this test asserts is only that
        // EVERY pump reports SOMETHING, not that every pump succeeds.
        let _ = outcome;
        reported[id] = true;
    }
    assert!(
        reported.iter().all(|r| *r),
        "every one of {PUMPS} pumps must report exactly one outcome"
    );

    for (id, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|_| panic!("pump {id}: worker thread must not panic"));
    }

    // Claim 2: no flusher thread may outlive its pump. Every worker above
    // was joined (and its `AlignedPump` dropped inside `run_worker`, whether
    // via `finish()` or the early-return `drop(pump)`) before this check.
    assert_eq!(
        live_flusher_threads(),
        0,
        "every flusher thread must have exited once its pump was dropped — a nonzero count \
         here means at least one thread outlived its pump"
    );

    // Claim 3: peak allocation is bounded by a generous multiple of the sum
    // of each pump's own (depth + 1) * segment, not by total bytes written
    // (12 segments/pump on average) or by pump count squared.
    let peak = peak_bytes() as u128;
    let bound = baseline_bytes + capacity_bound * OVERHEAD_FACTOR;
    assert!(
        peak <= bound,
        "peak allocation during the stress run was {peak} bytes (baseline {baseline_bytes}) — \
         expected at most {bound} ({OVERHEAD_FACTOR}x the sum, across all {PUMPS} pumps, of \
         each pump's own (depth + 1) * segment plus a fixed per-pump overhead allowance); a \
         bound this loose failing means memory use is no longer proportional to segments in \
         flight (bounded-ring rule, architecture.md)"
    );
}
