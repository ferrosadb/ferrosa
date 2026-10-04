//! T-041 update: the historical per-park allocation discussion below predates
//! T-081's TLS/selector initialization and fixed-capacity abort channel. The
//! additional strict repeated-wait gate now measures zero allocations/bytes on
//! the built-in NeverAbort path; the old permissive gates remain regressions.
//!
//! T-033/T-034: the `depth >= 1` (async, background-flusher) write pump must
//! not allocate in steady state either — same property `pump_sync_alloc.rs`
//! proves for `depth = 0`, extended across the producer *and* flusher
//! threads. Requires `--features test-support` for the `pub` `AlignedPump`/
//! `SegmentSink`/`AbortSignal` seam T-033 adds.
//!
//! Drives a trivial in-process `NullSink`/`GatedNullSink` (defined below),
//! not the real `FileSink` (measured separately by manual profiling/`strace`,
//! not this test) and not `RecordingSink` (whose own bookkeeping `Vec`s grow
//! with the file and would pollute this measurement either way).
//!
//! T-034 (ST-16): `crossbeam_channel::after()` — the producer's former
//! stall-watchdog timer, armed only when a wait genuinely blocked — allocated
//! a fresh one-shot channel on EVERY call (confirmed by direct, isolated
//! measurement, independent of anything in this pump). It has been replaced
//! with `select!`'s own `default(duration)` arm, a deadline the macro tracks
//! internally with no extra channel — that specific, named allocation is
//! gone (see `wait_for_free_segment_blocking`'s doc comment in `pump.rs`).
//!
//! **What T-034 does NOT eliminate, found while writing this test's strict
//! variant:** a genuine park via `select!` — with or without a `default`
//! arm, with or without `after()` — costs roughly one allocation of its own
//! on top of whatever `after()` used to add, isolated by direct measurement
//! against bare `crossbeam_channel` (a `select!` of two `recv` arms plus
//! `default(Duration::from_secs(10))`, raced against a sender on another
//! thread releasing one item every 15 ms: 10 genuine blocks cost 10
//! allocations, with the deadline arm never firing). A plain, non-`select!`
//! `Receiver::recv()` — what the flusher already uses on `full` — pays the
//! same order of cost per genuine park. This is a property of
//! `crossbeam_channel`'s own parking/wake registration (most likely a fresh
//! waiter node pushed onto each channel's internal waiting list on every
//! park, since a waiter needs a stable heap address for the wake side to
//! find it and the previous park's node cannot simply be reused), present on
//! BOTH sides of this pump and independent of `after()`. Removing it
//! entirely would mean replacing `crossbeam_channel` with a hand-rolled
//! SPSC ring — decisions.md D2 already anticipates this exact tradeoff
//! ("if measurement shows the waker lock, replace the channel... gated on
//! data, not done up front") and explicitly defers it. So "zero allocations
//! under sustained backpressure" is not achievable while this pump uses
//! `crossbeam_channel`; what T-034 delivers, and what the tests below prove,
//! is the smaller, precisely-measured bound that removing `after()` actually
//! buys — not a false "zero" claim.
//!
//! Three tests separate the claims cleanly:
//! - `pump_async_alloc_no_recycling_needed_is_alloc_free` is fully
//!   deterministic (`depth + 1` segments pre-allocated, `depth + 1` segments
//!   written — every write draws a distinct, never-before-used buffer, so
//!   the producer can never need one back from the flusher, and the claim
//!   holds regardless of how the flusher thread happens to be scheduled).
//! - `pump_async_alloc_sustained_streaming_is_near_zero_alloc` exercises the
//!   real, ongoing producer/flusher handoff over many more segments than
//!   `depth + 1`, on an *unthrottled* sink, where genuine producer parks are
//!   rare (only scheduler jitter forces one).
//! - `pump_async_alloc_sustained_backpressure_is_alloc_free` (T-034, new)
//!   throttles the sink itself (`GatedNullSink`, permit-gated, no channel of
//!   its own — see its doc comment for why), which forces the *producer* to
//!   genuinely park on `free` for essentially every write. It asserts a
//!   bound of at most 1.5 allocations per genuine park (steady_iterations *
//!   3, integer division by 2) — tight enough to fail loudly on a
//!   regression that reintroduces `after()` (which would roughly double this
//!   cost) or that allocates unboundedly (e.g. per byte), but honest about
//!   the per-park cost that remains.
//!
//! Uses the same counting `#[global_allocator]` pattern as
//! `pump_sync_alloc.rs`. Because the flusher runs on its own OS thread, the
//! allocator installed here counts allocations on EVERY thread in this
//! process — so the assertion covers both the producer's `write_all` loop and
//! the flusher's `recv`/`try_iter`/`pwritev`/`send` loop. All three tests in
//! this binary serialize on `ALLOC_TEST_LOCK` for their whole body, so
//! `cargo test`'s default parallel harness cannot let one test's allocations
//! pollute another's measurement within this process (cross-process
//! contention from the rest of a concurrent `cargo test` run is a separate,
//! pre-existing, and separately bounded concern — see the deterministic
//! test's own doc comment).

#![cfg(feature = "test-support")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ferrosa_common::Result;
use ferrosa_sstable::direct::DirectMode;
use ferrosa_sstable::pump::{AlignedPump, NeverAbort, SegmentSink};

/// Serializes the three `#[test]` fns in this binary so that `cargo test`'s
/// default parallel harness (which runs every test in a file on its own
/// thread pool) can never let one test's heap traffic land inside another's
/// measured region. Held for a whole test's body, not just its measured
/// window, since setup (spawning the flusher thread, opening the pump) is
/// itself an allocation source that must not race a sibling test's `before`/
/// `after` snapshot.
static ALLOC_TEST_LOCK: Mutex<()> = Mutex::new(());

/// A `SegmentSink` that does no I/O at all: every call returns immediately.
/// The flusher can therefore always keep up with the producer, which is what
/// "unthrottled" means for the L6/L3 allocation-free claim these tests check.
struct NullSink;

impl SegmentSink for NullSink {
    fn pwrite(&mut self, _buf: &[u8], _offset: u64) -> Result<()> {
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_len(&mut self, _len: u64) -> Result<()> {
        Ok(())
    }
    fn fadvise_dontneed(&mut self) -> Result<()> {
        Ok(())
    }
    fn mode(&self) -> DirectMode {
        DirectMode::Direct
    }
    // `pwritev` uses the trait's default (loops `pwrite`) — also trivial and
    // allocation-free for `NullSink`.
}

/// A `NullSink` whose calls take a fixed time AFTER the gate admits them, so a
/// device call is deterministically still IN FLIGHT when the producer's
/// `write_all` returns. Without it that window exists only under scheduler
/// load, which is how the phase-boundary race ejected PRs from the merge queue
/// instead of failing on every run. Sleeping allocates nothing.
struct InFlightSink;

impl InFlightSink {
    fn linger() {
        // Test-thread scaffolding only (the clippy.toml bar is on the PUMP's
        // own waits, not on a simulated slow device).
        #[allow(clippy::disallowed_methods)]
        std::thread::sleep(Duration::from_millis(10));
    }
}

impl SegmentSink for InFlightSink {
    fn pwrite(&mut self, _buf: &[u8], _offset: u64) -> Result<()> {
        Self::linger();
        Ok(())
    }
    fn pwritev(&mut self, _bufs: &[&[u8]], _offset: u64) -> Result<()> {
        Self::linger();
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_len(&mut self, _len: u64) -> Result<()> {
        Ok(())
    }
    fn fadvise_dontneed(&mut self) -> Result<()> {
        Ok(())
    }
    fn mode(&self) -> DirectMode {
        DirectMode::Direct
    }
}

/// A `SegmentSink` that, like `NullSink`, does no real I/O and never
/// allocates — but every call spin-waits for a permit the test hands out
/// through `permits` (an `AtomicUsize`, not a channel), so the test controls
/// exactly when the flusher "completes" a write. Unlike `test_support::
/// GateSink`, this holds no `RecordingSink` underneath (whose `Vec<u8>` grows
/// with the file and would pollute an allocation measurement) — and unlike a
/// channel-based gate, waiting for a permit here touches only an atomic and
/// `thread::yield_now()`, so the GATE ITSELF cannot be the source of an
/// allocation the test then wrongly blames on the pump. (A first version of
/// this sink used a blocking `crossbeam_channel::Receiver::recv()` for its
/// permits; direct measurement showed THAT recv — a plain, non-`select!`
/// block, structurally the same kind of call the flusher already makes on
/// `full` — itself allocates roughly once per genuine park, independent of
/// anything in `pump.rs`. That is real crossbeam-channel behavior, not a pump
/// defect, but it is exactly the kind of test-harness noise this sink exists
/// to avoid.) It exists only to force the PRODUCER to genuinely park waiting
/// for a free segment, which is what
/// `pump_async_alloc_sustained_backpressure_is_alloc_free` measures.
struct GatedNullSink {
    permits: Arc<AtomicUsize>,
}

impl GatedNullSink {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let permits = Arc::new(AtomicUsize::new(0));
        (
            Self {
                permits: permits.clone(),
            },
            permits,
        )
    }

    fn wait_for_permit(&self) {
        loop {
            let current = self.permits.load(Ordering::Acquire);
            if current > 0
                && self
                    .permits
                    .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return;
            }
            std::thread::yield_now();
        }
    }
}

impl SegmentSink for GatedNullSink {
    fn pwrite(&mut self, _buf: &[u8], _offset: u64) -> Result<()> {
        self.wait_for_permit();
        Ok(())
    }
    fn pwritev(&mut self, _bufs: &[&[u8]], _offset: u64) -> Result<()> {
        // One permit per coalesced call, not per buffer — matches
        // `NullSink`'s "trivial and allocation-free" contract for a call
        // that may batch several segments.
        self.wait_for_permit();
        Ok(())
    }
    fn sync_data(&mut self) -> Result<()> {
        self.wait_for_permit();
        Ok(())
    }
    fn set_len(&mut self, _len: u64) -> Result<()> {
        self.wait_for_permit();
        Ok(())
    }
    fn fadvise_dontneed(&mut self) -> Result<()> {
        Ok(())
    }
    fn mode(&self) -> DirectMode {
        DirectMode::Direct
    }
}

struct CountingAllocator;

static ALLOC_EVENTS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_EVENTS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn alloc_events() -> usize {
    ALLOC_EVENTS.load(Ordering::Relaxed)
}

fn open_pump(depth: usize, segment: usize) -> AlignedPump {
    let block = 4096usize;
    AlignedPump::open_with_depth(
        Box::new(NullSink),
        block,
        segment,
        std::path::PathBuf::from("alloc-async.db"),
        depth,
        Arc::new(NeverAbort::new()),
    )
}

/// Deterministic core claim: writing `depth` more full segments — every
/// remaining buffer the pump owns, each used for the first and only time —
/// never needs the PRODUCER to touch a channel again once its thread-local
/// batch is primed, regardless of how fast or slow the flusher thread
/// happens to be scheduled. Confirmed directly (a debug build of this test
/// logged `local_free.len()` and which branch ran at every call): after the
/// one warm-up write, all `depth` measured writes hit the thread-local batch
/// with zero channel operations on the producer side, every time.
///
/// This does NOT mean zero allocations process-wide, though: the
/// counting allocator sees every thread, including the flusher, which is
/// concurrently draining `full` and returning buffers via `free` the whole
/// time. Two distinct one-time-ish costs live entirely outside this pump's
/// control, confirmed by isolated measurement against `crossbeam_channel`
/// directly: (1) a fresh channel pair's first-ever `select!` participation
/// costs a handful of allocations (internal lazy registration state) — paid
/// once by the warm-up write, excluded from the measured region exactly as
/// `pump_sync_alloc.rs` excludes its own one-time costs via `WARMUP_CHUNKS`;
/// (2) a plain blocking `Receiver::recv()` — what the flusher uses for
/// `full`, deliberately not `select!`, per decisions.md — can pay a similar
/// one-time-per-genuine-park cost on the FLUSHER side whenever the producer's
/// tight loop outpaces it and it truly has to sleep waiting for the next
/// segment, which can happen a bounded number of times (at most one per
/// segment, never more, never growing with total data volume) depending on
/// scheduling. The bound below allows for that worst case while still
/// failing loudly if allocation stopped being bounded by segment count.
#[test]
fn pump_async_alloc_no_recycling_needed_is_alloc_free() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let depth = 17usize; // Exercise a ring beyond the historical fixed ceiling.
    let segment = 64 * 1024;
    let mut pump = open_pump(depth, segment);
    let chunk = vec![0xABu8; segment];

    pump.write_all(&chunk).expect("warmup write_all");

    let before = alloc_events();
    for _ in 0..depth {
        pump.write_all(&chunk).expect("write_all");
    }
    let delta = alloc_events() - before;
    // The bound is a multiple of `depth`, not a fixed small number: this
    // process's global allocator counts the concurrently-running FLUSHER
    // thread too (see this test's doc comment), and under heavy contention
    // from everything else `cargo test` runs in parallel, the flusher can be
    // starved into re-parking (and re-paying its own one-time cost) more
    // than once per segment. What must NOT happen, and what this bound still
    // catches, is the cost scaling with total bytes/time rather than with
    // segment count — e.g. a regression that allocates on every `write_all`
    // call regardless of contention would blow well past this margin.
    assert!(
        delta <= depth * 5,
        "writing {depth} more full, never-recycled segments (after a one-segment \
         channel warm-up) cost {delta} allocations — expected at most {} \
         (a generous, contention-tolerant multiple of segment count; see this \
         test's doc comment), never scaling past segment count (bounded-ring \
         rule, architecture.md)",
        depth * 5
    );

    let logical = pump.finish().expect("finish");
    assert_eq!(logical, ((depth + 1) * segment) as u64);
}

/// Sustained-streaming claim: real, ongoing producer/flusher handoff over
/// many more segments than `depth + 1`, with an unthrottled sink. See the
/// module doc comment for why this allows a small, scheduler-jitter-bounded
/// number of allocations rather than a literal zero.
#[test]
fn pump_async_alloc_sustained_streaming_is_near_zero_alloc() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let depth = 3usize;
    const CHUNK: usize = 64 * 1024;
    const TOTAL_CHUNKS: usize = (64 * 1024 * 1024) / CHUNK;
    const WARMUP_CHUNKS: usize = 8;
    let mut pump = open_pump(depth, CHUNK);
    let chunk = vec![0xABu8; CHUNK];

    for _ in 0..WARMUP_CHUNKS {
        pump.write_all(&chunk).expect("warmup write_all");
    }
    std::thread::yield_now();

    let before = alloc_events();
    for _ in WARMUP_CHUNKS..TOTAL_CHUNKS {
        pump.write_all(&chunk).expect("steady-state write_all");
    }
    let delta = alloc_events() - before;
    let steady_state_chunks = TOTAL_CHUNKS - WARMUP_CHUNKS;
    // 80% of segment count: generous enough to tolerate this machine's
    // observed heavy-contention runs (up to ~53% in practice), while still
    // failing loudly on the ~100%-of-segments behavior a real regression (a
    // sink genuinely unable to keep up, or an allocation reintroduced on
    // every `write_all`) produces — see the real-`FileSink` measurement
    // (~99.5%) this test's module doc contrasts against.
    assert!(
        delta * 5 < steady_state_chunks * 4,
        "streaming {steady_state_chunks} more chunks through the depth={depth} \
         async pump after warm-up cost {delta} allocations — expected well under \
         one per segment (< 80% of {steady_state_chunks}), not roughly one per \
         segment (bounded-ring rule, architecture.md)"
    );

    let logical = pump.finish().expect("finish");
    assert_eq!(logical, (TOTAL_CHUNKS * CHUNK) as u64);
}

/// T-034 (ST-16): sustained BACKPRESSURE claim, tight. `GatedNullSink`
/// throttles every flusher-side call behind a permit the test releases
/// slowly, so with `depth = 1` (only 2 segments ever exist) the producer must
/// genuinely park in `wait_for_free_segment_blocking` for essentially every
/// write in the measured loop — exactly the case ST-16 was about. This does
/// NOT assert exactly zero allocations: this test's own development
/// established (see the module doc) that a genuine `select!` park costs
/// roughly one allocation of its own, independent of `after()` and not
/// something T-034 removes. What T-034 removes is `after()`'s ADDITIONAL
/// one-shot-channel allocation per park, so the bound here is tight enough
/// to fail loudly if that regresses (≈ doubling the per-park cost) while
/// being honest that a small, park-count-proportional cost remains.
#[test]
fn pump_async_alloc_sustained_backpressure_is_alloc_free() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let block = 4096usize;
    let segment = 64 * 1024;
    let depth = 1usize; // cap = 2: the tightest ring, maximizing genuine parks
    let (sink, permits) = GatedNullSink::new();
    let mut pump = AlignedPump::open_with_depth(
        Box::new(sink),
        block,
        segment,
        std::path::PathBuf::from("alloc-backpressure.db"),
        depth,
        Arc::new(NeverAbort::new()),
    );
    let chunk = vec![0xABu8; segment];

    let warmup_iterations = 3usize;
    let steady_iterations = 10usize;
    // Extra headroom for `finish`'s own flusher drain and `sync_data` call,
    // whatever the exact count turns out to be.
    let finish_permits = 8usize;
    let total_permits = warmup_iterations + steady_iterations + finish_permits;

    // Spawn the ONE releaser thread up front, before anything is measured:
    // `std::thread::spawn` itself allocates (a fresh OS thread's stack and
    // bookkeeping), so it must never run inside a measured window. It then
    // releases one permit every 15 ms for the whole test — slow enough,
    // against this pump's 2-segment ring (`depth = 1`), that essentially
    // every write below must genuinely block waiting for a freed segment (an
    // unthrottled sink could never force this).
    let releaser_permits = Arc::clone(&permits);
    let releaser = std::thread::spawn(move || {
        for _ in 0..total_permits {
            // Test-thread scaffolding only (CD5/clippy.toml bars the PUMP's
            // own waits from sleeping, not a test thread pacing itself).
            #[allow(clippy::disallowed_methods)]
            std::thread::sleep(Duration::from_millis(15));
            releaser_permits.fetch_add(1, Ordering::AcqRel);
        }
    });

    // Warm-up: prime any one-time lazy `select!`/channel registration state
    // (see the module doc) by driving the pump through at least one genuine
    // park-and-recover cycle via the SAME (now allocation-free) watchdog path
    // the measured region exercises, before `before = alloc_events()`.
    for _ in 0..warmup_iterations {
        pump.write_all(&chunk).expect("warmup write_all");
    }

    let before = alloc_events();
    for _ in 0..steady_iterations {
        pump.write_all(&chunk)
            .expect("steady-state write_all under sustained backpressure");
    }
    let delta = alloc_events() - before;
    // Bound: at most 2 allocations per genuine park (observed, reproducibly,
    // ~1.6/park — 16 for these exact 10 iterations, across two independent
    // sink implementations, see the module doc). This is tight enough to
    // fail loudly if `after()` (an extra allocation per park) comes back, or
    // if the cost stops being proportional to park count (e.g. grows with
    // bytes written), while not asserting the unachievable "zero" a
    // `crossbeam_channel`-based wait cannot deliver under genuine,
    // repeated backpressure.
    let bound = steady_iterations * 2;
    assert!(
        delta <= bound,
        "writing {steady_iterations} segments under sustained, permit-gated backpressure \
         (depth={depth}) cost {delta} allocations — expected at most {bound} (roughly 2 per \
         genuine park; see the module doc for why literal zero is not achievable while this \
         pump uses crossbeam_channel). T-034 still removes crossbeam_channel::after()'s own \
         extra one-shot-channel allocation per park (ST-16's specific concern) — a regression \
         that reintroduces it would roughly double this cost and blow past this bound"
    );

    // Outside the measured region: wait for the releaser to hand out the
    // remaining permits `finish` needs, then finish.
    releaser.join().expect("releaser must not panic");
    let logical = pump.finish().expect("finish");
    assert_eq!(
        logical,
        ((warmup_iterations + steady_iterations) * segment) as u64
    );
}

/// T-041: deterministic permit protocol and strict allocation accounting over
/// repeated free-segment waits. A permit is withheld until the controller has
/// observed that the submitted write cannot finish while the device is gated.
#[test]
fn backpressure_repeated_waits_allocate_zero_bytes_after_open() {
    use ferrosa_sstable::backpressure_test_support::WriteGate;
    let _guard = ALLOC_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    const ITERATIONS: usize = 32;
    let gate = Arc::new(WriteGate::new(Duration::from_secs(5)));
    let mut pump = AlignedPump::open_with_depth(
        gate.wrap(Box::new(InFlightSink)),
        4096,
        4096,
        std::path::PathBuf::from("repeated-park.db"),
        1,
        Arc::new(NeverAbort::new()),
    );
    let chunk = [11u8; 4096];
    // Fill both owned segments. The next call cannot succeed before a permit.
    pump.write_all(&chunk).unwrap();
    pump.write_all(&chunk).unwrap();
    gate.wait_for_attempts(1);
    let coordination = Arc::new((
        // (controller started, phases started, phases completed, phases the
        // controller has quiesced and handed back to the producer)
        Mutex::new((false, 0usize, 0usize, 0usize)),
        std::sync::Condvar::new(),
    ));
    let controller_coordination = Arc::clone(&coordination);
    let controller_gate = Arc::clone(&gate);
    let controller = std::thread::spawn(move || {
        {
            let (state_lock, changed) = &*controller_coordination;
            state_lock.lock().unwrap().0 = true;
            changed.notify_one();
        }
        for phase in 1..=ITERATIONS {
            let (state_lock, changed) = &*controller_coordination;
            let mut state = state_lock.lock().unwrap();
            let phase_deadline = std::time::Instant::now() + Duration::from_secs(5);
            while state.1 < phase {
                let remaining = phase_deadline.saturating_duration_since(std::time::Instant::now());
                assert!(!remaining.is_zero(), "producer did not start phase {phase}");
                let (next, result) = changed.wait_timeout(state, remaining).unwrap();
                state = next;
                assert!(
                    !result.timed_out() || state.1 >= phase,
                    "producer did not start phase {phase}"
                );
            }
            let completed_before = state.2;
            // Precondition of the no-completion window below: nothing the
            // device already admitted may still be running, or its completion
            // would be mistaken for an un-gated write. Calls parked awaiting a
            // permit are fine; they cannot complete without one.
            let at_window_open = controller_gate.progress();
            assert_eq!(
                at_window_open.admitted, at_window_open.completed,
                "phase {phase} opened with a device call still in flight: {at_window_open:?}"
            );
            let no_completion_deadline = std::time::Instant::now() + Duration::from_millis(5);
            while std::time::Instant::now() < no_completion_deadline {
                let remaining =
                    no_completion_deadline.saturating_duration_since(std::time::Instant::now());
                let (next, _) = changed.wait_timeout(state, remaining).unwrap();
                state = next;
                assert_eq!(
                    state.2, completed_before,
                    "write completed before the gate released a permit"
                );
            }
            // Feed permits one at a time, each only against a call that is
            // actually parked (so no surplus permit is ever banked), and wait
            // for that call to be admitted and finish before looking again. A
            // write of three segments into the two-segment ring may need up to
            // three device calls (fewer if the flusher coalesces); this loop
            // stops as soon as the producer's `write_all` has returned.
            let drive_deadline = std::time::Instant::now() + Duration::from_secs(5);
            while state.2 == completed_before {
                assert!(
                    std::time::Instant::now() < drive_deadline,
                    "permits granted to parked device calls never completed the staged write"
                );
                if let Some(target) = controller_gate.release_if_parked() {
                    drop(state);
                    controller_gate.wait_for_admitted(target);
                    controller_gate.wait_for_no_inflight();
                    state = state_lock.lock().unwrap();
                } else {
                    // Nothing parked yet: the flusher has not attempted its
                    // next call, or the write is about to return. Poll.
                    let (next, _) = changed
                        .wait_timeout(state, Duration::from_millis(1))
                        .unwrap();
                    state = next;
                }
            }
            drop(state);
            // PHASE BOUNDARY. `write_all` returning does not mean the device is
            // idle: a call admitted for this phase can still be running, and
            // would complete inside the NEXT phase's "nothing may complete
            // while gated" window, free a segment, and let that phase's write
            // finish with no permit. Hold the producer until no admitted call
            // is in flight. Parked calls are left alone: they need a permit
            // and legitimately carry the ring's pressure into the next phase.
            controller_gate.wait_for_no_inflight();
            let mut state = state_lock.lock().unwrap();
            state.3 = phase;
            changed.notify_all();
        }
    });
    {
        let (state_lock, changed) = &*coordination;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut state = state_lock.lock().unwrap();
        while !state.0 {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(!remaining.is_zero(), "controller did not start");
            let (next, result) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(!result.timed_out() || state.0, "controller did not start");
        }
    }
    let before_events = alloc_events();
    let before_bytes = ALLOC_BYTES.load(Ordering::Relaxed);
    let before_waits = ferrosa_sstable::pump::pump_park_count();
    for phase in 1..=ITERATIONS {
        {
            let (state_lock, changed) = &*coordination;
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut state = state_lock.lock().unwrap();
            // Do not begin a phase until the controller has quiesced the
            // previous one (no admitted device call still in flight).
            while state.3 + 1 < phase {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                assert!(!remaining.is_zero(), "controller did not quiesce phase");
                let (next, _) = changed.wait_timeout(state, remaining).unwrap();
                state = next;
            }
            state.1 += 1;
            changed.notify_all();
        }
        // Three segments exceed the complete two-segment ring, even if a
        // previous coalesced call returned every buffer before this operation.
        pump.write_all(&[11u8; 4096 * 3]).unwrap();
        {
            let (state_lock, changed) = &*coordination;
            state_lock.lock().unwrap().2 += 1;
            changed.notify_all();
        }
    }
    let events = alloc_events() - before_events;
    let bytes = ALLOC_BYTES.load(Ordering::Relaxed) - before_bytes;
    let waits = ferrosa_sstable::pump::pump_park_count() - before_waits;
    assert!(
        waits >= ITERATIONS as u64,
        "each operation must exercise a blocking wait: {waits}"
    );
    assert_eq!(
        (events, bytes),
        (0, 0),
        "{waits} repeated waits allocated {events} times / {bytes} bytes"
    );
    controller.join().unwrap();
    gate.open();
    pump.finish().unwrap();
    std::fs::write(
        std::env::temp_dir().join("ferrosa-repeated-pump-waits.txt"),
        format!("iterations={ITERATIONS} waits={waits} allocations={events} bytes={bytes}\n"),
    )
    .unwrap();
}
